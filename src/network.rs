use crate::params::RunCmd;
use crate::parse::parse_str_addr;
use libp2p::core::{PeerId, Multiaddr, ProtocolsHandler, PublicKey, Swarm};
use libp2p::core::swarm::{NetworkBehaviour, NetworkBehaviourEventProcess, NetworkBehaviourAction, PollParameters, ConnectedPoint};
use futures::Async;
use libp2p::NetworkBehaviour;
use std::{thread, cmp, time::Duration};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_timer::{Delay, clock::Clock};
use log::{debug, info, trace, warn};
use libp2p::multiaddr::Protocol;
use libp2p::kad::{Kademlia, KademliaOut};
use futures::{prelude::*, future};
use primitive_types::H256;
use std::{str::FromStr, net::{Ipv4Addr, SocketAddr}, iter};
use libp2p::identity::{Keypair, secp256k1::SecretKey};
use libp2p::identify::{Identify, IdentifyEvent, protocol::IdentifyInfo};

#[derive(NetworkBehaviour)]
pub struct Behavior<TSubstream> {
    discovery: DiscoveryBehaviour<TSubstream>,
    identify: Identify<TSubstream>,

    #[behaviour(ignore)]
    proto_version: String,
}

impl<TSubstream> Behavior<TSubstream> {
    pub fn new(
        proto_version: String,
        user_agent: String,
        local_public_key: PublicKey,
        bootnodes: Vec<(PeerId, Multiaddr)>,
    ) -> Self {
        let mut kademlia = Kademlia::new(local_public_key.clone().into_peer_id());
        for (peer_id, addr) in &bootnodes {
            kademlia.add_connected_address(peer_id, addr.clone());
        }

        let clock = Clock::new();

        let user_defined = bootnodes.clone();

        let identify = Identify::new(proto_version.clone(), user_agent, local_public_key.clone());

        Behavior {
            discovery: DiscoveryBehaviour {
                user_defined: user_defined,
                kademlia,
                next_kad_random_query: Delay::new(clock.now()),
                duration_to_next_kad: Duration::from_secs(1),
                clock,
                local_peer_id: local_public_key.into_peer_id(),
            },
            identify: identify,
            proto_version: proto_version,
        }
    }
}

pub struct DiscoveryBehaviour<TSubstream> {
    /// User-defined list of nodes and their addresses. Typically includes bootstrap nodes and
    /// reserved nodes.
    user_defined: Vec<(PeerId, Multiaddr)>,
    /// Kademlia requests and answers.
    kademlia: Kademlia<TSubstream>,
    /// Stream that fires when we need to perform the next random Kademlia query.
    next_kad_random_query: Delay,
    /// After `next_kad_random_query` triggers, the next one triggers after this duration.
    duration_to_next_kad: Duration,
    /// `Clock` instance that uses the current execution context's source of time.
    clock: Clock,
    /// Identity of our local node.
    local_peer_id: PeerId,
}

impl<TSubstream> NetworkBehaviour for DiscoveryBehaviour<TSubstream>
    where
        TSubstream: AsyncRead + AsyncWrite,
{
    type ProtocolsHandler = <Kademlia<TSubstream> as NetworkBehaviour>::ProtocolsHandler;
    type OutEvent = <Kademlia<TSubstream> as NetworkBehaviour>::OutEvent;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        NetworkBehaviour::new_handler(&mut self.kademlia)
    }

    fn addresses_of_peer(&mut self, peer_id: &PeerId) -> Vec<Multiaddr> {
        let mut list = self.user_defined.iter()
            .filter_map(|(p, a)| if p == peer_id { Some(a.clone()) } else { None })
            .collect::<Vec<_>>();
        list.extend(self.kademlia.addresses_of_peer(peer_id));
        trace!(target: "sub-libp2p", "Addresses of {:?} are {:?}", peer_id, list);
        if list.is_empty() {
            if self.kademlia.kbuckets_entries().any(|p| p == peer_id) {
                debug!(target: "sub-libp2p", "Requested dialing to {:?} (peer in k-buckets), \
					and no address was found", peer_id);
            } else {
                debug!(target: "sub-libp2p", "Requested dialing to {:?} (peer not in k-buckets), \
					and no address was found", peer_id);
            }
        }
        list
    }

    fn inject_connected(&mut self, peer_id: PeerId, endpoint: ConnectedPoint) {
        NetworkBehaviour::inject_connected(&mut self.kademlia, peer_id, endpoint)
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId, endpoint: ConnectedPoint) {
        NetworkBehaviour::inject_disconnected(&mut self.kademlia, peer_id, endpoint)
    }

    fn inject_replaced(&mut self, peer_id: PeerId, closed: ConnectedPoint, opened: ConnectedPoint) {
        NetworkBehaviour::inject_replaced(&mut self.kademlia, peer_id, closed, opened)
    }

    fn inject_node_event(
        &mut self,
        peer_id: PeerId,
        event: <Self::ProtocolsHandler as ProtocolsHandler>::OutEvent,
    ) {
        NetworkBehaviour::inject_node_event(&mut self.kademlia, peer_id, event)
    }

    fn inject_new_external_addr(&mut self, addr: &Multiaddr) {
        let new_addr = addr.clone()
            .with(Protocol::P2p(self.local_peer_id.clone().into()));
        info!(target: "sub-libp2p", "Discovered new external address for our node: {}", new_addr);
    }

    fn inject_expired_listen_addr(&mut self, addr: &Multiaddr) {
        info!(target: "sub-libp2p", "No longer listening on {}", addr);
    }

    fn poll(
        &mut self,
        params: &mut PollParameters,
    ) -> Async<
        NetworkBehaviourAction<
            <Self::ProtocolsHandler as ProtocolsHandler>::InEvent,
            Self::OutEvent,
        >,
    > {
        // Poll Kademlia.
        match self.kademlia.poll(params) {
            Async::Ready(action) => return Async::Ready(action),
            Async::NotReady => (),
        }

        // Poll the stream that fires when we need to start a random Kademlia query.
        loop {
            match self.next_kad_random_query.poll() {
                Ok(Async::NotReady) => break,
                Ok(Async::Ready(_)) => {
                    let random_peer_id = PeerId::random();
                    debug!(target: "sub-libp2p", "Libp2p <= Starting random Kademlia request for \
						{:?}", random_peer_id);
                    self.kademlia.find_node(random_peer_id);

                    // Reset the `Delay` to the next random.
                    self.next_kad_random_query.reset(self.clock.now() + self.duration_to_next_kad);
                    self.duration_to_next_kad = cmp::min(self.duration_to_next_kad * 2,
                                                         Duration::from_secs(60));
                }
                Err(err) => {
                    warn!(target: "sub-libp2p", "Kademlia query timer errored: {:?}", err);
                    break;
                }
            }
        }

        Async::NotReady
    }
}

impl<TSubstream> NetworkBehaviourEventProcess<KademliaOut> for Behavior<TSubstream> {
    fn inject_event(&mut self, out: KademliaOut) {
        match out {
            KademliaOut::Discovered { .. } => {}
            KademliaOut::KBucketAdded { peer_id, .. } => {
                info!(target: "sub-libp2p", "Should add discovered node, KBucketAdded: {}", peer_id);
            }
            KademliaOut::FindNodeResult { key, closer_peers } => {
                trace!(target: "sub-libp2p", "Libp2p => Query for {:?} yielded {:?} results",
                       key, closer_peers.len());
                if closer_peers.is_empty() {
                    warn!(target: "sub-libp2p", "Libp2p => Random Kademlia query has yielded empty \
						results");
                }
            }
            // We never start any GET_PROVIDERS query.
            KademliaOut::GetProvidersResult { .. } => ()
        }
    }
}

impl<TSubstream> NetworkBehaviourEventProcess<IdentifyEvent> for Behavior<TSubstream> {
    fn inject_event(&mut self, event: IdentifyEvent) {
        match event {
            IdentifyEvent::Identified { peer_id, mut info, .. } => {
                trace!(target: "sub-libp2p", "Identified {:?} => {:?}", peer_id, info);
                // TODO: ideally we would delay the first identification to when we open the custom
                //	protocol, so that we only report id info to the service about the nodes we
                //	care about (https://github.com/libp2p/rust-libp2p/issues/876)
                if !info.protocol_version.contains("network-sharding-demo") {
                    warn!(target: "sub-libp2p", "Connected to a non-Substrate node: {:?}", info);
                }
                if info.listen_addrs.len() > 30 {
                    warn!(target: "sub-libp2p", "Node {:?} has reported more than 30 addresses; \
						it is identified by {:?} and {:?}", peer_id, info.protocol_version,
                          info.agent_version
                    );
                    info.listen_addrs.truncate(30);
                }
                for addr in &info.listen_addrs {
                    self.discovery.kademlia.add_connected_address(&peer_id, addr.clone());
                }
                info!(target: "sub-libp2p", "Should add discovered node, Identified: {}", peer_id);
            }
            IdentifyEvent::Error { .. } => {}
            IdentifyEvent::SendBack { result: Err(ref err), ref peer_id } =>
                debug!(target: "sub-libp2p", "Error when sending back identify info \
					to {:?} => {}", peer_id, err),
            IdentifyEvent::SendBack { .. } => {}
        }
    }
}


pub fn run_network(cmd: RunCmd) {
    let mut bootnodes = Vec::new();

    // Process the bootnodes.
    for bootnode in cmd.bootnodes.iter() {
        match parse_str_addr(bootnode) {
            Ok((peer_id, addr)) => {
                bootnodes.push((peer_id, addr));
            }
            Err(_) => warn!(target: "sub-libp2p", "Not a valid bootnode address: {}", bootnode),
        }
    }

    let to_dial = bootnodes.clone();

    // Process the node key.
    let node_key: Option<SecretKey> = match cmd.node_key.map(|k| {
        H256::from_str(k.as_str()).map_err(|_err| "").and_then(|bytes| SecretKey::from_bytes(bytes).map_err(|_err| ""))
    }) {
        Some(Ok(r)) => Some(r),
        Some(Err(_e)) => None,
        None => None,
    };

    Protocol::Ip4(Ipv4Addr::new(0, 0, 0, 0));

    // Process listen_addresses
    let port = match cmd.port {
        Some(port) => port,
        None => 60001,
    };

    let listen_addresses: Vec<Multiaddr> = vec![
        iter::once(Protocol::Ip4(Ipv4Addr::new(0, 0, 0, 0)))
            .chain(iter::once(Protocol::Tcp(port)))
            .collect()
    ];

    let local_identity = node_key.map_or(Keypair::generate_secp256k1(), |k| { Keypair::Secp256k1(k.into()) });
    let local_public = local_identity.public();
    let local_peer_id = local_public.clone().into_peer_id();
    info!(target: "sub-libp2p", "Local node identity is: {}", local_peer_id.to_base58());

    let user_agent = "".to_string();
    let proto_version = "network-sharding-demo".to_string();

    let transport = libp2p::build_development_transport(local_identity);

    let behavior = Behavior::new( proto_version,user_agent, local_public, bootnodes);

    let mut swarm = libp2p::Swarm::new(transport, behavior, local_peer_id);

    for listen_address in &listen_addresses {
        ;
        if let Err(err) = Swarm::listen_on(&mut swarm, listen_address.clone()) {
            warn!(target: "sub-libp2p", "Can't listen on {} because: {:?}", listen_address, err)
        }
    }

    for (peer_id, _) in to_dial {
        Swarm::dial(&mut swarm, peer_id);
    }

    let mut listening = false;

    let thread = thread::Builder::new().name("network".to_string()).spawn(move || {
        tokio::run(future::poll_fn(move || -> Result<_, ()> {
            loop {
                match swarm.poll().expect("Error while polling swarm") {
                    Async::Ready(Some(e)) => println!("{:?}", e),
                    Async::Ready(None) | Async::NotReady => {
                        if !listening {
                            if let Some(a) = Swarm::listeners(&swarm).next() {
                                info!("Listening on {:?}", a);
                                listening = true;
                            }
                        }
                        return Ok(Async::NotReady);
                    }
                }
            }
        }));
    });

    info!("Run network successfully");
}