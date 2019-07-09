use crate::params::{self, RunCmd, BootNodesRouterCmd};
use crate::parse::parse_str_addr;
use crate::bootnodes_router::bootnodes_router_client;
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
use libp2p::floodsub::{Floodsub, FloodsubEvent};
use crate::bootnodes_router::{BootnodesRouterConf, Shard};
use std::error::Error;
use parity_codec::alloc::collections::HashMap;
use libp2p::tokio_codec::{FramedRead, LinesCodec};

const PROTOCOL_VERSION: &str = "network-sharding-demo/1.0.0";

const USERAGENT_SHARD: &str = "shard";

#[derive(NetworkBehaviour)]
pub struct Behavior<TSubstream> {
    discovery: DiscoveryBehaviour<TSubstream>,
    identify: Identify<TSubstream>,
    floodsub: Floodsub<TSubstream>,

    #[behaviour(ignore)]
    shard_num: u16,
}

impl<TSubstream> Behavior<TSubstream> {
    pub fn new(
        protocol_version: String,
        user_agent: String,
        local_public_key: PublicKey,
        bootnodes: Vec<(PeerId, Multiaddr)>,
        shard_num: u16,
    ) -> Self {
        let mut kademlia = Kademlia::new(local_public_key.clone().into_peer_id());
        for (peer_id, addr) in &bootnodes {
            kademlia.add_connected_address(peer_id, addr.clone());
        }

        let clock = Clock::new();

        let user_defined = bootnodes.clone();

        let identify = Identify::new(protocol_version, user_agent, local_public_key.clone());

        let floodsub = Floodsub::new(local_public_key.clone().into_peer_id());

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
            floodsub: floodsub,
            shard_num: shard_num,
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
                info!(target: "sub-libp2p", "KBucketAdded: {}", peer_id);
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
                if !info.protocol_version.eq(PROTOCOL_VERSION) {
                    warn!(target: "sub-libp2p", "Connected to a node with different protocol version : {:?}, will not treat as discovered", info);
                    return;
                }
                let shard_token = format!("{}/{}", USERAGENT_SHARD, self.shard_num);
                if !info.agent_version.contains(&shard_token) {
                    warn!(target: "sub-libp2p", "Connected to a node on different shard : {:?}, will not treat as discovered", info);
                    return;
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
                info!(target: "sub-libp2p", "Add discovered node, Identified: {}", peer_id);
                self.floodsub.add_node_to_partial_view(peer_id);
            }
            IdentifyEvent::Error { .. } => {}
            IdentifyEvent::SendBack { result: Err(ref err), ref peer_id } =>
                debug!(target: "sub-libp2p", "Error when sending back identify info \
					to {:?} => {}", peer_id, err),
            IdentifyEvent::SendBack { .. } => {}
        }
    }
}

impl<TSubstream> NetworkBehaviourEventProcess<FloodsubEvent> for Behavior<TSubstream> {
    // Called when `floodsub` produces an event.
    fn inject_event(&mut self, message: FloodsubEvent) {
        if let FloodsubEvent::Message(message) = message {
            info!(target: "sub-libp2p", "Received: '{:?}' from {:?}", String::from_utf8_lossy(&message.data), message.source);
        }
    }
}

fn get_bootnodes_router(cmd: &RunCmd) -> Result<BootnodesRouterConf, ()> {
    let router = &cmd.bootnodes_router;

    for one in router {
        info!("bootnodes_router: {}", one);
        let mut client = bootnodes_router_client(one.to_string());
        let result = client.bootnodes().call();

        match result {
            Ok(result) => return Ok(result),
            Err(e) => { continue; }
        }
    }

    Err(())
}

fn get_bootnodes(cmd: &RunCmd) -> Vec<(PeerId, Multiaddr)> {
    let shard_num = cmd.shard_num;

    let bootnodes_router = get_bootnodes_router(&cmd);

    let mut shards: HashMap<String, Shard>;

    let bootnodes_str = match bootnodes_router {
        Ok(result) => {
            shards = result.shards;
            let shard = shards.get(&format!("{}", shard_num));
            match shard {
                Some(shard) => &shard.bootnodes,
                None => &cmd.bootnodes,
            }
        }
        Err(e) => &cmd.bootnodes,
    };

    info!("bootnodes: {:?}", bootnodes_str);

    let mut bootnodes = Vec::new();

    // Process the bootnodes.
    for bootnode in bootnodes_str.iter() {
        match parse_str_addr(bootnode) {
            Ok((peer_id, addr)) => {
                bootnodes.push((peer_id, addr));
            }
            Err(_) => warn!(target: "sub-libp2p", "Not a valid bootnode address: {}", bootnode),
        }
    }

    bootnodes
}

pub fn run_network(cmd: RunCmd) {
    let bootnodes = get_bootnodes(&cmd);

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
    let port = match cmd.shared_params.port {
        Some(port) => port,
        None => params::DEFAULT_PORT,
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

    let user_agent = format!("{}/{}", USERAGENT_SHARD, &cmd.shard_num);
    let protocol_version = PROTOCOL_VERSION.to_string();

    info!(target: "sub-libp2p", "Local node protocol version: {}, user agent: {}", protocol_version, user_agent);

    let transport = libp2p::build_development_transport(local_identity);

    // Create a Floodsub topic
    let floodsub_topic = libp2p::floodsub::TopicBuilder::new("sync").build();

    let mut swarm = {

        let mut behavior = Behavior::new(protocol_version, user_agent, local_public, bootnodes, cmd.shard_num);

        behavior.floodsub.subscribe(floodsub_topic.clone());

        libp2p::Swarm::new(transport, behavior, local_peer_id)
    };

    for listen_address in &listen_addresses {
        ;
        if let Err(err) = Swarm::listen_on(&mut swarm, listen_address.clone()) {
            warn!(target: "sub-libp2p", "Can't listen on {} because: {:?}", listen_address, err)
        }
    }

    for (peer_id, _) in to_dial {
        Swarm::dial(&mut swarm, peer_id);
    }

    let stdin = tokio_stdin_stdout::stdin(0);
    let mut framed_stdin = FramedRead::new(stdin, LinesCodec::new());

    let mut listening = false;

    let thread = thread::Builder::new().name("network".to_string()).spawn(move || {
        tokio::run(future::poll_fn(move || -> Result<_, ()> {
            loop {
                match framed_stdin.poll().expect("Error while polling stdin") {
                    Async::Ready(Some(line)) => {
                        swarm.floodsub.publish(&floodsub_topic, line.as_bytes());
                    }
                    Async::Ready(None) => panic!("Stdin closed"),
                    Async::NotReady => break,
                };
            }

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