use crate::params::{self, RunCmd, BootNodesRouterCmd};
use crate::parse::parse_str_addr;
use crate::bootnodes_router::bootnodes_router_client;
use libp2p::core::{PeerId, Multiaddr, ProtocolsHandler, PublicKey, Swarm, Endpoint, ProtocolsHandlerEvent};
use libp2p::core::swarm::{NetworkBehaviour, NetworkBehaviourEventProcess, NetworkBehaviourAction, PollParameters, ConnectedPoint};
use futures::Async;
use libp2p::NetworkBehaviour;
use std::{thread, cmp, time::Duration};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_timer::{Delay, clock::Clock};
use log::{debug, info, trace, warn, error};
use libp2p::multiaddr::Protocol;
use libp2p::kad::{Kademlia, KademliaOut};
use futures::{prelude::*, future};
use primitive_types::H256;
use std::{str::FromStr, net::{Ipv4Addr, SocketAddr}, iter};
use libp2p::identity::{Keypair, secp256k1::SecretKey};
use libp2p::identify::{Identify, IdentifyEvent, protocol::IdentifyInfo};
use libp2p::floodsub::{Floodsub, FloodsubEvent};
use libp2p::core::protocols_handler::{SubstreamProtocol, ProtocolsHandlerUpgrErr, KeepAlive, IntoProtocolsHandler};
use libp2p::core::upgrade::{self, InboundUpgrade, OutboundUpgrade};
use crate::bootnodes_router::{BootnodesRouterConf, Shard};
use std::error::Error;
use parity_codec::alloc::collections::HashMap;
use libp2p::tokio_codec::{FramedRead, LinesCodec};
use smallvec::SmallVec;
use std::borrow::Cow;
use std::marker::PhantomData;
use std::error;
use std::fmt;
use std::io;
use std::time::Instant;
use void;
use std::mem;

const PROTOCOL_VERSION: &str = "network-sharding-demo/1.0.0";

const USERAGENT_SHARD: &str = "shard";

#[derive(NetworkBehaviour)]
pub struct Behavior<TSubstream> {
    discovery: DiscoveryBehaviour<TSubstream>,
    identify: Identify<TSubstream>,
    floodsub: Floodsub<TSubstream>,
    work: WorkBehaviour<TSubstream>,

    #[behaviour(ignore)]
    shard_num: u16,

    #[behaviour(ignore)]
    events: Vec<BehaviourOut>,
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

        let mut work = WorkBehaviour::new();

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
            work: work,
            shard_num: shard_num,
            events: Vec::new(),
        }
    }
}

pub struct WorkBehaviour<TSubstream>{

    events: SmallVec<[NetworkBehaviourAction<WorkHandlerIn, WorkOut>; 4]>,

    /// Marker to pin the generics.
    marker: PhantomData<TSubstream>,
}

impl<TSubstream> WorkBehaviour<TSubstream> {
    /// Creates a `WorkBehaviour`.
    pub fn new() -> Self {
        WorkBehaviour {
            events: SmallVec::new(),
            marker: PhantomData,
        }
    }
}

enum ProtocolState {
    /// Waiting for the behaviour to tell the handler whether it is enabled or disabled.
    Init {
        /// Deadline after which the initialization is abnormally long.
        init_deadline: Delay,
    },

    /// Handler is opening a substream in order to activate itself.
    /// If we are in this state, we haven't sent any `CustomProtocolOpen` yet.
    Opening {
        /// Deadline after which the opening is abnormally long.
        deadline: Delay,
    },

    /// Normal operating mode. Contains the substreams that are open.
    /// If we are in this state, we have sent a `CustomProtocolOpen` message to the outside.
    Normal,

    /// We are disabled. Contains substreams that are being closed.
    /// If we are in this state, either we have sent a `CustomProtocolClosed` message to the
    /// outside or we have never sent any `CustomProtocolOpen` in the first place.
    Disabled {
        /// If true, we should reactivate the handler after all the substreams in `shutdown` have
        /// been closed.
        ///
        /// Since we don't want to mix old and new substreams, we wait for all old substreams to
        /// be closed before opening any new one.
        reenable: bool,
    },

    /// In this state, we don't care about anything anymore and need to kill the connection as soon
    /// as possible.
    KillAsap,

    /// We sometimes temporarily switch to this state during processing. If we are in this state
    /// at the beginning of a method, that means something bad happend in the source code.
    Poisoned,
}

#[derive(Debug)]
pub enum WorkHandlerIn {
    /// The node should start using custom protocols. Contains whether we are the dialer or the
    /// listener of the connection.
    Enable(Endpoint),

    /// The node should stop using custom protocols.
    Disable,

    /// Sends a message through a custom protocol substream.
    SendCustomMessage {
        /// The message to send.
        message: String,
    },
}

#[derive(Debug)]
pub enum WorkHandlerOut {
    /// Opened a custom protocol with the remote.
    WorkOpen {
        /// Version of the protocol that has been opened.
        version: u8,
    },

    /// Closed a custom protocol with the remote.
    WorkClosed {
        /// Reason why the substream closed, for diagnostic purposes.
        reason: Cow<'static, str>,
    },

    /// Receives a message on a custom protocol substream.
    CustomMessage {
        /// Message that has been received.
        message: String,
    },

    /// An error has happened on the protocol level with this node.
    ProtocolError {
        /// If true the error is severe, such as a protocol violation.
        is_severe: bool,
        /// The error that happened.
        error: Box<dyn error::Error + Send + Sync>,
    },
}

#[derive(Debug)]
pub enum WorkOut {
    /// Opened a custom protocol with the remote.
    WorkOpen {
        /// Version of the protocol that has been opened.
        version: u8,
        /// Id of the node we have opened a connection with.
        peer_id: PeerId,
        /// Endpoint used for this custom protocol.
        endpoint: ConnectedPoint,
    },

    /// Closed a custom protocol with the remote.
    WorkClosed {
        /// Id of the peer we were connected to.
        peer_id: PeerId,
        /// Reason why the substream closed, for debugging purposes.
        reason: Cow<'static, str>,
    },

    /// Receives a message on a custom protocol substream.
    CustomMessage {
        /// Id of the peer the message came from.
        peer_id: PeerId,
        /// Message that has been received.
        message: String,
    },
}

#[derive(Debug)]
pub enum BehaviourOut {
    /// Opened a custom protocol with the remote.
    WorkOpen {
        /// Version of the protocol that has been opened.
        version: u8,
        /// Id of the node we have opened a connection with.
        peer_id: PeerId,
        /// Endpoint used for this custom protocol.
        endpoint: ConnectedPoint,
    },

    /// Closed a custom protocol with the remote.
    WorkClosed {
        /// Id of the peer we were connected to.
        peer_id: PeerId,
        /// Reason why the substream closed, for diagnostic purposes.
        reason: Cow<'static, str>,
    },

    /// Receives a message on a custom protocol substream.
    CustomMessage {
        /// Id of the peer the message came from.
        peer_id: PeerId,
        /// Message that has been received.
        message: String,
    },
}

impl From<WorkOut> for BehaviourOut {
    fn from(other: WorkOut) -> BehaviourOut {
        match other {
            WorkOut::WorkOpen { version, peer_id, endpoint } => {
                BehaviourOut::WorkOpen { version, peer_id, endpoint }
            }
            WorkOut::WorkClosed { peer_id, reason } => {
                BehaviourOut::WorkClosed { peer_id, reason }
            }
            WorkOut::CustomMessage { peer_id, message } => {
                BehaviourOut::CustomMessage { peer_id, message }
            }
        }
    }
}

pub struct WorkProtocolsHandlerProto<TSubstream> {

    /// Marker to pin the generic type.
    marker: PhantomData<TSubstream>,
}

impl<TSubstream> IntoProtocolsHandler for WorkProtocolsHandlerProto<TSubstream>
    where
        TSubstream: AsyncRead + AsyncWrite,
{
    type Handler = WorkProtocolsHandler<TSubstream>;

    fn into_handler(self, remote_peer_id: &PeerId) -> Self::Handler {
        WorkProtocolsHandler {
            remote_peer_id: remote_peer_id.clone(),
            state: ProtocolState::Init {
                init_deadline: Delay::new(Instant::now() + Duration::from_secs(5))
            },
            events_queue: SmallVec::new(),
            marker: self.marker,
        }
    }
}

pub struct WorkProtocolsHandler<TSubstream> {

    remote_peer_id: PeerId,

    state: ProtocolState,

    events_queue: SmallVec<[ProtocolsHandlerEvent<upgrade::DeniedUpgrade, (), WorkHandlerOut>; 16]>,

    marker: PhantomData<TSubstream>,
}

impl<TSubstream> WorkProtocolsHandlerProto<TSubstream>
    where
        TSubstream: AsyncRead + AsyncWrite,
{
    /// Builds a new `WorkProtocolsHandler`.
    pub fn new() -> Self {
        WorkProtocolsHandlerProto {
            marker: PhantomData,
        }
    }
}

#[derive(Debug)]
pub struct WorkError;

impl error::Error for WorkError {
}

impl fmt::Display for WorkError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Work error")
    }
}

impl<TSubstream> WorkProtocolsHandler<TSubstream>
    where
        TSubstream: AsyncRead + AsyncWrite,
{
    /// Enables the handler.
    fn enable(&mut self, endpoint: Endpoint) {
        self.state = match mem::replace(&mut self.state, ProtocolState::Poisoned) {
            ProtocolState::Poisoned => {
                error!(target: "sub-libp2p", "Handler with {:?} is in poisoned state",
                       self.remote_peer_id);
                ProtocolState::Poisoned
            }

            ProtocolState::Init { .. } => {

                if let Endpoint::Dialer = endpoint {
                    self.events_queue.push(ProtocolsHandlerEvent::OutboundSubstreamRequest {
                        protocol: SubstreamProtocol::new(upgrade::DeniedUpgrade{}),
                        info: (),
                    });
                }
                ProtocolState::Opening {
                    deadline: Delay::new(Instant::now() + Duration::from_secs(60))
                }

            }

            st @ ProtocolState::KillAsap => st,
            st @ ProtocolState::Opening { .. } => st,
            st @ ProtocolState::Normal { .. } => st,
            ProtocolState::Disabled { .. } => {
                ProtocolState::Disabled { reenable: true }
            }
        }
    }

    /// Disables the handler.
    fn disable(&mut self) {
        self.state = match mem::replace(&mut self.state, ProtocolState::Poisoned) {
            ProtocolState::Poisoned => {
                error!(target: "sub-libp2p", "Handler with {:?} is in poisoned state",
                       self.remote_peer_id);
                ProtocolState::Poisoned
            }

            ProtocolState::Init { .. } => {
                ProtocolState::Disabled { reenable: false }
            }

            ProtocolState::Opening { .. } | ProtocolState::Normal { .. } =>
            // At the moment, if we get disabled while things were working, we kill the entire
            // connection in order to force a reset of the state.
            // This is obviously an extremely shameful way to do things, but at the time of
            // the writing of this comment, the networking works very poorly and a solution
            // needs to be found.
                ProtocolState::KillAsap,

            ProtocolState::Disabled { .. } =>
                ProtocolState::Disabled { reenable: false },

            ProtocolState::KillAsap => ProtocolState::KillAsap,
        };
    }

    /// Polls the state for events. Optionally returns an event to produce.
    #[must_use]
    fn poll_state(&mut self)
                  -> Option<ProtocolsHandlerEvent<upgrade::DeniedUpgrade, (), WorkHandlerOut>> {
        match mem::replace(&mut self.state, ProtocolState::Poisoned) {
            ProtocolState::Poisoned => {
                error!(target: "sub-libp2p", "Handler with {:?} is in poisoned state",
                       self.remote_peer_id);
                self.state = ProtocolState::Poisoned;
                None
            }

            ProtocolState::Init { mut init_deadline } => {
                match init_deadline.poll() {
                    Ok(Async::Ready(())) => {
                        init_deadline.reset(Instant::now() + Duration::from_secs(60));
                        debug!(target: "sub-libp2p", "Handler initialization process is too long \
							with {:?}", self.remote_peer_id)
                    },
                    Ok(Async::NotReady) => {}
                    Err(_) => error!(target: "sub-libp2p", "Tokio timer has errored")
                }

                self.state = ProtocolState::Init { init_deadline };
                None
            }

            ProtocolState::Opening { mut deadline } => {
                match deadline.poll() {
                    Ok(Async::Ready(())) => {
                        deadline.reset(Instant::now() + Duration::from_secs(60));
                        let event = WorkHandlerOut::ProtocolError {
                            is_severe: true,
                            error: "Timeout when opening protocol".to_string().into(),
                        };
                        self.state = ProtocolState::Opening { deadline };
                        Some(ProtocolsHandlerEvent::Custom(event))
                    },
                    Ok(Async::NotReady) => {
                        self.state = ProtocolState::Opening { deadline };
                        None
                    },
                    Err(_) => {
                        error!(target: "sub-libp2p", "Tokio timer has errored");
                        deadline.reset(Instant::now() + Duration::from_secs(60));
                        self.state = ProtocolState::Opening { deadline };
                        None
                    },
                }
            }

            ProtocolState::Normal => {

                // This code is reached is none if and only if none of the substreams are in a ready state.
                self.state = ProtocolState::Normal;
                None
            }

            ProtocolState::Disabled { reenable } => {
                // If `reenable` is `true`, that means we should open the substreams system again
                // after all the substreams are closed.
                if reenable {
                    self.state = ProtocolState::Opening {
                        deadline: Delay::new(Instant::now() + Duration::from_secs(60))
                    };
                    Some(ProtocolsHandlerEvent::OutboundSubstreamRequest {
                        protocol: SubstreamProtocol::new(upgrade::DeniedUpgrade{}),
                        info: (),
                    })
                } else {
                    self.state = ProtocolState::Disabled { reenable };
                    None
                }
            }

            ProtocolState::KillAsap => None,
        }
    }

    /// Called by `inject_fully_negotiated_inbound` and `inject_fully_negotiated_outbound`.
    fn inject_fully_negotiated(
        &mut self,
        mut substream: void::Void
    ) {
        self.state = match mem::replace(&mut self.state, ProtocolState::Poisoned) {
            ProtocolState::Poisoned => {
                error!(target: "sub-libp2p", "Handler with {:?} is in poisoned state",
                       self.remote_peer_id);
                ProtocolState::Poisoned
            }

            ProtocolState::Init { init_deadline } => {
                ProtocolState::Init { init_deadline }
            }

            ProtocolState::Opening { .. } => {
                ProtocolState::Normal
            }

            ProtocolState::Normal => {
                ProtocolState::Normal
            }

            ProtocolState::Disabled { .. } => {
                ProtocolState::Disabled { reenable: false }
            }

            ProtocolState::KillAsap => ProtocolState::KillAsap,
        };
    }

    /// Sends a message to the remote.
    fn send_message(&mut self, message: String) {
        match self.state {
            ProtocolState::Normal => info!("Send message: {}", message),
            _ => debug!(target: "sub-libp2p", "Tried to send message over closed protocol \
				with {:?}", self.remote_peer_id)
        }
    }
}


impl<TSubstream> ProtocolsHandler for WorkProtocolsHandler<TSubstream>
    where TSubstream: AsyncRead + AsyncWrite {
    type InEvent = WorkHandlerIn;
    type OutEvent = WorkHandlerOut;
    type Substream = TSubstream;
    type Error = WorkError;
    type InboundProtocol = upgrade::DeniedUpgrade;
    type OutboundProtocol = upgrade::DeniedUpgrade;
    type OutboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol> {
        SubstreamProtocol::new(upgrade::DeniedUpgrade{})
    }

    fn inject_fully_negotiated_inbound(
        &mut self,
        proto: <Self::InboundProtocol as InboundUpgrade<TSubstream>>::Output
    ) {
        self.inject_fully_negotiated(proto);
    }

    fn inject_fully_negotiated_outbound(
        &mut self,
        proto: <Self::OutboundProtocol as OutboundUpgrade<TSubstream>>::Output,
        _: Self::OutboundOpenInfo
    ) {
        self.inject_fully_negotiated(proto);
    }

    fn inject_event(&mut self, message: WorkHandlerIn) {
        match message {
            WorkHandlerIn::Disable => self.disable(),
            WorkHandlerIn::Enable(endpoint) => self.enable(endpoint),
            WorkHandlerIn::SendCustomMessage { message } =>
                self.send_message(message),
        }
    }

    #[inline]
    fn inject_dial_upgrade_error(&mut self, _: (), err: ProtocolsHandlerUpgrErr<void::Void>) {
        let is_severe = match err {
            ProtocolsHandlerUpgrErr::Upgrade(_) => true,
            _ => false,
        };

        self.events_queue.push(ProtocolsHandlerEvent::Custom(WorkHandlerOut::ProtocolError {
            is_severe,
            error: Box::new(err),
        }));
    }

    fn connection_keep_alive(&self) -> KeepAlive {
        match self.state {
            ProtocolState::Init { .. } | ProtocolState::Opening { .. } |
            ProtocolState::Normal { .. } => KeepAlive::Yes,
            ProtocolState::Disabled { .. } | ProtocolState::Poisoned |
            ProtocolState::KillAsap => KeepAlive::No,
        }
    }

    fn poll(
        &mut self,
    ) -> Poll<
        ProtocolsHandlerEvent<Self::OutboundProtocol, Self::OutboundOpenInfo, Self::OutEvent>,
        Self::Error,
    > {
        // Flush the events queue if necessary.
        if !self.events_queue.is_empty() {
            let event = self.events_queue.remove(0);
            return Ok(Async::Ready(event))
        }

        // Kill the connection if needed.
        if let ProtocolState::KillAsap = self.state {
            return Err(WorkError);
        }

        // Process all the substreams.
        if let Some(event) = self.poll_state() {
            return Ok(Async::Ready(event))
        }

        Ok(Async::NotReady)
    }
}

impl<TSubstream> NetworkBehaviour for WorkBehaviour<TSubstream>
    where
        TSubstream: AsyncRead + AsyncWrite,
{
    type ProtocolsHandler = WorkProtocolsHandlerProto<TSubstream>;
    type OutEvent = WorkOut;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        WorkProtocolsHandlerProto::new()
    }

    fn addresses_of_peer(&mut self, _: &PeerId) -> Vec<Multiaddr> {
        Vec::new()
    }

    fn inject_connected(&mut self, peer_id: PeerId, connected_point: ConnectedPoint) {
        info!("WorkBehaviour inject_connected, peer_id: {}, connected_point: {:?}", peer_id, connected_point);
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId, endpoint: ConnectedPoint) {
        info!("WorkBehaviour inject_disconnected, peer_id: {}, endpoint: {:?}", peer_id, endpoint);
    }

    fn inject_addr_reach_failure(&mut self, peer_id: Option<&PeerId>, addr: &Multiaddr, error: &dyn error::Error) {
        trace!(target: "sub-libp2p", "Libp2p => Reach failure for {:?} through {:?}: {:?}", peer_id, addr, error);
    }

    fn inject_dial_failure(&mut self, peer_id: &PeerId) {
        info!("WorkBehaviour inject_dial_failure, peer_id: {}", peer_id);
    }

    fn inject_node_event(
        &mut self,
        source: PeerId,
        event: WorkHandlerOut,
    ) {
        info!("WorkBehaviour inject_node_event, source: {}, event: {:?}", source, event);

    }

    fn poll(
        &mut self,
        _params: &mut PollParameters,
    ) -> Async<
        NetworkBehaviourAction<
            WorkHandlerIn,
            Self::OutEvent,
        >,
    > {

        if !self.events.is_empty() {
            return Async::Ready(self.events.remove(0))
        }

        Async::NotReady
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
        info!("DiscoveryBehaviour inject_connected, peer_id: {}, endpoint: {:?}", peer_id, endpoint);
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

impl<TSubstream> NetworkBehaviourEventProcess<WorkOut> for Behavior<TSubstream> {
    fn inject_event(&mut self, event: WorkOut) {
        self.events.push(event.into());
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

    //not need
//    for (peer_id, _) in to_dial {
//        Swarm::dial(&mut swarm, peer_id);
//    }

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