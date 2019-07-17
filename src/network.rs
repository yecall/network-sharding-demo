use crate::params::{self, RunCmd};
use crate::parse::parse_str_addr;
use crate::client::{self, Client, ParseMessage, Message};
use libp2p::core::{PeerId, Multiaddr, ProtocolsHandler, PublicKey, Swarm, Endpoint, ProtocolsHandlerEvent, UpgradeInfo, Negotiated};
use libp2p::core::swarm::{NetworkBehaviour, NetworkBehaviourEventProcess, NetworkBehaviourAction, PollParameters, ConnectedPoint};
use libp2p::NetworkBehaviour;
use std::{thread, cmp, time::Duration};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_timer::{Delay, clock::Clock};
use log::{debug, info, trace, warn, error};
use libp2p::multiaddr::Protocol;
use libp2p::kad::{Kademlia, KademliaOut};
use libp2p::tokio_codec::{Framed, FramedRead, LinesCodec};
use futures::{prelude::*, future, stream, Stream, stream::Fuse};
use std::{net::Ipv4Addr, iter};
use libp2p::identity::Keypair;
use libp2p::identify::{Identify, IdentifyEvent, IdentifyInfo};
use libp2p::core::protocols_handler::{SubstreamProtocol, ProtocolsHandlerUpgrErr, KeepAlive, IntoProtocolsHandler};
use libp2p::core::upgrade::{InboundUpgrade, OutboundUpgrade};
use crate::bootnodes_router::{BootnodesRouterConf, Shard};
use parity_codec::alloc::collections::HashMap;
use smallvec::{smallvec, SmallVec};
use std::borrow::Cow;
use std::marker::PhantomData;
use std::error;
use std::fmt;
use std::io;
use std::time::Instant;
use std::mem;
use fnv::FnvHashMap;
use std::collections::VecDeque;
use unsigned_varint::codec::UviBytes;
use std::sync::Arc;
use crossbeam_channel::{self as channel, Receiver, Sender, TryRecvError};
use parking_lot::{Mutex, RwLock};


const PROTOCOL_VERSION: &str = "network-sharding-demo/1.0.0";

const USERAGENT_SHARD: &str = "shard";

#[derive(NetworkBehaviour)]
#[behaviour(out_event = "BehaviourOut", poll_method = "poll")]
pub struct Behavior<TSubstream> {
    discovery: DiscoveryBehaviour<TSubstream>,
    identify: Identify<TSubstream>,
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

        let mut work = WorkBehaviour::new();

        for (peer_id, addr) in &bootnodes {
            work.events.push(NetworkBehaviourAction::DialPeer { peer_id: peer_id.clone() });
        }

        let discovery = DiscoveryBehaviour {
            user_defined: user_defined,
            kademlia,
            next_kad_random_query: Delay::new(clock.now()),
            duration_to_next_kad: Duration::from_secs(1),
            clock,
            local_peer_id: local_public_key.into_peer_id(),
        };

        Behavior {
            discovery: discovery,
            identify: identify,
            work: work,
            shard_num: shard_num,
            events: Vec::new(),
        }
    }

    pub fn broadcast_custom_message(&mut self, data: String) {
        let peer_ids = self.get_peer_ids();

        for peer_id in peer_ids {
            self.send_custom_message(&peer_id, data.clone());
        }
    }

    pub fn get_peer_ids(&self) -> Vec<PeerId> {
        let mut peer_ids: Vec<PeerId> = Vec::new();

        for (peer_id, _) in &self.work.peers {
            peer_ids.push(peer_id.clone());
        }

        peer_ids
    }

    #[inline]
    pub fn send_custom_message(&mut self, target: &PeerId, data: String) {
        self.work.send_packet(target, data)
    }
}

pub struct WorkBehaviour<TSubstream> {
    protocol: RegisteredProtocol,

    events: SmallVec<[NetworkBehaviourAction<WorkHandlerIn, WorkOut>; 4]>,

    peers: FnvHashMap<PeerId, ()>,

    /// Marker to pin the generics.
    marker: PhantomData<TSubstream>,
}

impl<TSubstream> WorkBehaviour<TSubstream> {
    /// Creates a `WorkBehaviour`.
    pub fn new() -> Self {
        WorkBehaviour {
            protocol: RegisteredProtocol { id: "/work".to_string() },
            events: SmallVec::new(),
            peers: FnvHashMap::default(),
            marker: PhantomData,
        }
    }

    pub fn send_packet(&mut self, target: &PeerId, message: String) {
        info!("WorkBehaviour send_packet, target: {}, message: {}", target, message);
        trace!(target: "sub-libp2p", "External API => Packet for {:?}", target);
        trace!(target: "sub-libp2p", "Handler({:?}) <= Packet", target);
        self.events.push(NetworkBehaviourAction::SendEvent {
            peer_id: target.clone(),
            event: WorkHandlerIn::SendCustomMessage {
                message,
            },
        });
    }

    pub fn add_discovered_node(&mut self, peer_id: &PeerId) {
        info!("WorkBehaviour add_discovered_node, peer_id: {}", peer_id);
        self.peers.insert(peer_id.clone(), ());
        info!("WorkBehaviour add_discovered_node, peers count: {}", self.peers.len());
    }

    pub fn disconnect_node(&mut self, peer_id: &PeerId) {
        self.events.push(NetworkBehaviourAction::SendEvent {
            peer_id: peer_id.clone(),
            event: WorkHandlerIn::Disable,
        });
        info!("WorkBehaviour disconnect_node, peer_id: {}", peer_id);
    }
}

enum ProtocolState<TSubstream> {
    /// Waiting for the behaviour to tell the handler whether it is enabled or disabled.
    Init {
        /// List of substreams opened by the remote but that haven't been processed yet.
        substreams: SmallVec<[RegisteredProtocolSubstream<TSubstream>; 6]>,
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
    Normal {
        /// The substreams where bidirectional communications happen.
        substreams: SmallVec<[RegisteredProtocolSubstream<TSubstream>; 4]>,
        /// Contains substreams which are being shut down.
        shutdown: SmallVec<[RegisteredProtocolSubstream<TSubstream>; 4]>,
    },

    /// We are disabled. Contains substreams that are being closed.
    /// If we are in this state, either we have sent a `CustomProtocolClosed` message to the
    /// outside or we have never sent any `CustomProtocolOpen` in the first place.
    Disabled {
        /// List of substreams to shut down.
        shutdown: SmallVec<[RegisteredProtocolSubstream<TSubstream>; 6]>,

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
    WorkOpen,

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

    /// A substream to the remote is clogged. The send buffer is very large, and we should print
	/// a diagnostic message and/or avoid sending more data.
    Clogged {
        /// Copy of the messages that are within the buffer, for further diagnostic.
        messages: Vec<String>,
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

    /// We have obtained debug information from a peer.
    Identified {
        /// Id of the peer that has been identified.
        peer_id: PeerId,
        /// Information about the peer.
        info: IdentifyInfo,
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
    protocol: RegisteredProtocol,

    /// Marker to pin the generic type.
    marker: PhantomData<TSubstream>,
}


impl<TSubstream> WorkProtocolsHandlerProto<TSubstream>
    where
        TSubstream: AsyncRead + AsyncWrite,
{
    /// Builds a new `WorkProtocolsHandler`.
    pub fn new(protocol: RegisteredProtocol) -> Self {
        WorkProtocolsHandlerProto {
            protocol: protocol,
            marker: PhantomData,
        }
    }
}

impl<TSubstream> IntoProtocolsHandler for WorkProtocolsHandlerProto<TSubstream>
    where
        TSubstream: AsyncRead + AsyncWrite,
{
    type Handler = WorkProtocolsHandler<TSubstream>;

    fn into_handler(self, remote_peer_id: &PeerId) -> Self::Handler {
        info!("into WorkProtocolsHandler, remote_peer_id={}", remote_peer_id);
        WorkProtocolsHandler {
            protocol: self.protocol,
            remote_peer_id: remote_peer_id.clone(),
            state: ProtocolState::Init {
                substreams: SmallVec::new(),
                init_deadline: Delay::new(Instant::now() + Duration::from_secs(5)),
            },
            events_queue: SmallVec::new(),
            marker: self.marker,
        }
    }
}

pub struct WorkProtocolsHandler<TSubstream> {
    protocol: RegisteredProtocol,

    remote_peer_id: PeerId,

    state: ProtocolState<TSubstream>,

    events_queue: SmallVec<[ProtocolsHandlerEvent<RegisteredProtocol, (), WorkHandlerOut>; 16]>,

    marker: PhantomData<TSubstream>,
}

#[derive(Debug)]
pub struct WorkError;

impl error::Error for WorkError {}

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
        info!("WorkProtocolsHandler enable");
        self.state = match mem::replace(&mut self.state, ProtocolState::Poisoned) {
            ProtocolState::Poisoned => {
                error!(target: "sub-libp2p", "Handler with {:?} is in poisoned state",
                       self.remote_peer_id);
                ProtocolState::Poisoned
            }

            ProtocolState::Init { substreams: incoming, .. } => {
                if incoming.is_empty() {
                    if let Endpoint::Dialer = endpoint {
                        self.events_queue.push(ProtocolsHandlerEvent::OutboundSubstreamRequest {
                            protocol: SubstreamProtocol::new(self.protocol.clone()),
                            info: (),
                        });
                    }
                    ProtocolState::Opening {
                        deadline: Delay::new(Instant::now() + Duration::from_secs(60))
                    }
                } else {
                    let event = WorkHandlerOut::WorkOpen;
                    self.events_queue.push(ProtocolsHandlerEvent::Custom(event));
                    ProtocolState::Normal {
                        substreams: incoming.into_iter().collect(),
                        shutdown: SmallVec::new(),
                    }
                }
            }

            st @ ProtocolState::KillAsap => st,
            st @ ProtocolState::Opening { .. } => st,
            st @ ProtocolState::Normal { .. } => st,
            ProtocolState::Disabled { shutdown, .. } => {
                ProtocolState::Disabled { shutdown, reenable: true }
            }
        }
    }

    /// Disables the handler.
    fn disable(&mut self) {
        info!("WorkProtocolsHandler disable");
        self.state = match mem::replace(&mut self.state, ProtocolState::Poisoned) {
            ProtocolState::Poisoned => {
                error!(target: "sub-libp2p", "Handler with {:?} is in poisoned state",
                       self.remote_peer_id);
                ProtocolState::Poisoned
            }

            ProtocolState::Init { substreams: mut shutdown, .. } => {
                for s in &mut shutdown {
                    s.shutdown();
                }
                ProtocolState::Disabled { shutdown, reenable: false }
            }

            ProtocolState::Opening { .. } | ProtocolState::Normal { .. } =>
            // At the moment, if we get disabled while things were working, we kill the entire
            // connection in order to force a reset of the state.
            // This is obviously an extremely shameful way to do things, but at the time of
            // the writing of this comment, the networking works very poorly and a solution
            // needs to be found.
                ProtocolState::KillAsap,

            ProtocolState::Disabled { shutdown, .. } =>
                ProtocolState::Disabled { shutdown, reenable: false },

            ProtocolState::KillAsap => ProtocolState::KillAsap,
        };
    }

    /// Polls the state for events. Optionally returns an event to produce.
    #[must_use]
    fn poll_state(&mut self)
                  -> Option<ProtocolsHandlerEvent<RegisteredProtocol, (), WorkHandlerOut>> {
        match mem::replace(&mut self.state, ProtocolState::Poisoned) {
            ProtocolState::Poisoned => {
                error!(target: "sub-libp2p", "Handler with {:?} is in poisoned state",
                       self.remote_peer_id);
                self.state = ProtocolState::Poisoned;
                None
            }

            ProtocolState::Init { substreams, mut init_deadline } => {
                match init_deadline.poll() {
                    Ok(Async::Ready(())) => {
                        init_deadline.reset(Instant::now() + Duration::from_secs(60));
                        debug!(target: "sub-libp2p", "Handler initialization process is too long \
							with {:?}", self.remote_peer_id)
                    }
                    Ok(Async::NotReady) => {}
                    Err(_) => error!(target: "sub-libp2p", "Tokio timer has errored")
                }

                self.state = ProtocolState::Init { substreams, init_deadline };
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
                    }
                    Ok(Async::NotReady) => {
                        self.state = ProtocolState::Opening { deadline };
                        None
                    }
                    Err(_) => {
                        error!(target: "sub-libp2p", "Tokio timer has errored");
                        deadline.reset(Instant::now() + Duration::from_secs(60));
                        self.state = ProtocolState::Opening { deadline };
                        None
                    }
                }
            }

            ProtocolState::Normal { mut substreams, mut shutdown } => {
                for n in (0..substreams.len()).rev() {
                    let mut substream = substreams.swap_remove(n);
                    match substream.poll() {
                        Ok(Async::NotReady) => substreams.push(substream),
                        Ok(Async::Ready(Some(RegisteredProtocolEvent::Message(message)))) => {
                            let event = WorkHandlerOut::CustomMessage {
                                message
                            };
                            substreams.push(substream);
                            self.state = ProtocolState::Normal { substreams, shutdown };
                            return Some(ProtocolsHandlerEvent::Custom(event));
                        }
                        Ok(Async::Ready(Some(RegisteredProtocolEvent::Clogged { messages }))) => {
                            let event = WorkHandlerOut::Clogged {
                                messages,
                            };
                            substreams.push(substream);
                            self.state = ProtocolState::Normal { substreams, shutdown };
                            return Some(ProtocolsHandlerEvent::Custom(event));
                        }
                        Ok(Async::Ready(None)) => {
                            shutdown.push(substream);
                            if substreams.is_empty() {
                                let event = WorkHandlerOut::WorkClosed {
                                    reason: "All substreams have been closed by the remote".into(),
                                };
                                self.state = ProtocolState::Disabled {
                                    shutdown: shutdown.into_iter().collect(),
                                    reenable: true,
                                };
                                return Some(ProtocolsHandlerEvent::Custom(event));
                            }
                        }
                        Err(err) => {
                            if substreams.is_empty() {
                                let event = WorkHandlerOut::WorkClosed {
                                    reason: format!("Error on the last substream: {:?}", err).into(),
                                };
                                self.state = ProtocolState::Disabled {
                                    shutdown: shutdown.into_iter().collect(),
                                    reenable: true,
                                };
                                return Some(ProtocolsHandlerEvent::Custom(event));
                            } else {
                                debug!(target: "sub-libp2p", "Error on extra substream: {:?}", err);
                            }
                        }
                    }
                }

                // This code is reached is none if and only if none of the substreams are in a ready state.
                self.state = ProtocolState::Normal { substreams, shutdown };
                None
            }

            ProtocolState::Disabled { mut shutdown, reenable } => {
                shutdown_list(&mut shutdown);
                // If `reenable` is `true`, that means we should open the substreams system again
                // after all the substreams are closed.
                if reenable && shutdown.is_empty() {
                    self.state = ProtocolState::Opening {
                        deadline: Delay::new(Instant::now() + Duration::from_secs(60))
                    };
                    Some(ProtocolsHandlerEvent::OutboundSubstreamRequest {
                        protocol: SubstreamProtocol::new(self.protocol.clone()),
                        info: (),
                    })
                } else {
                    self.state = ProtocolState::Disabled { shutdown, reenable };
                    None
                }
            }

            ProtocolState::KillAsap => None,
        }
    }

    /// Called by `inject_fully_negotiated_inbound` and `inject_fully_negotiated_outbound`.
    fn inject_fully_negotiated(
        &mut self,
        mut substream: RegisteredProtocolSubstream<TSubstream>,
    ) {
        self.state = match mem::replace(&mut self.state, ProtocolState::Poisoned) {
            ProtocolState::Poisoned => {
                error!(target: "sub-libp2p", "Handler with {:?} is in poisoned state",
                       self.remote_peer_id);
                ProtocolState::Poisoned
            }

            ProtocolState::Init { mut substreams, init_deadline } => {
                if substream.endpoint() == Endpoint::Dialer {
                    error!(target: "sub-libp2p", "Opened dialing substream with {:?} before \
						initialization", self.remote_peer_id);
                }
                substreams.push(substream);
                ProtocolState::Init { substreams, init_deadline }
            }

            ProtocolState::Opening { .. } => {
                //info!("inject_fully_negotiated Opening, substream");
                let event = WorkHandlerOut::WorkOpen;
                self.events_queue.push(ProtocolsHandlerEvent::Custom(event));
                ProtocolState::Normal {
                    substreams: smallvec![substream],
                    shutdown: SmallVec::new(),
                }
            }

            ProtocolState::Normal { substreams: mut existing, shutdown } => {
                existing.push(substream);
                ProtocolState::Normal { substreams: existing, shutdown }
            }

            ProtocolState::Disabled { mut shutdown, .. } => {
                substream.shutdown();
                shutdown.push(substream);
                ProtocolState::Disabled { shutdown, reenable: false }
            }

            ProtocolState::KillAsap => ProtocolState::KillAsap,
        };
    }

    /// Sends a message to the remote.
    fn send_message(&mut self, message: String) {
        //info!("WorkProtocolsHandler send_message");
        match self.state {
            ProtocolState::Normal { ref mut substreams, .. } =>
                {
                    //info!("WorkProtocolsHandler send_message normal");
                    substreams[0].send_message(message);
                }
            _ => info!(target: "sub-libp2p", "Tried to send message over closed protocol \
				with {:?}", self.remote_peer_id)
        }
    }
}

fn shutdown_list<TSubstream>
(list: &mut SmallVec<impl smallvec::Array<Item=RegisteredProtocolSubstream<TSubstream>>>)
    where TSubstream: AsyncRead + AsyncWrite {
    'outer: for n in (0..list.len()).rev() {
        let mut substream = list.swap_remove(n);
        loop {
            match substream.poll() {
                Ok(Async::Ready(Some(_))) => {}
                Ok(Async::NotReady) => break,
                Err(_) | Ok(Async::Ready(None)) => continue 'outer,
            }
        }
        list.push(substream);
    }
}


impl<TSubstream> ProtocolsHandler for WorkProtocolsHandler<TSubstream>
    where TSubstream: AsyncRead + AsyncWrite {
    type InEvent = WorkHandlerIn;
    type OutEvent = WorkHandlerOut;
    type Substream = TSubstream;
    type Error = WorkError;
    type InboundProtocol = RegisteredProtocol;
    type OutboundProtocol = RegisteredProtocol;
    type OutboundOpenInfo = ();

    fn listen_protocol(&self) -> SubstreamProtocol<Self::InboundProtocol> {
        SubstreamProtocol::new(self.protocol.clone())
    }

    fn inject_fully_negotiated_inbound(
        &mut self,
        proto: <Self::InboundProtocol as InboundUpgrade<TSubstream>>::Output,
    ) {
        info!("WorkProtocolsHandler inject_fully_negotiated_inbound");
        self.inject_fully_negotiated(proto);
    }

    fn inject_fully_negotiated_outbound(
        &mut self,
        proto: <Self::OutboundProtocol as OutboundUpgrade<TSubstream>>::Output,
        _: Self::OutboundOpenInfo,
    ) {
        info!("WorkProtocolsHandler inject_fully_negotiated_outbound");
        self.inject_fully_negotiated(proto);
    }

    fn inject_event(&mut self, message: WorkHandlerIn) {
        info!("WorkProtocolsHandler inject_event, message={:?}", message);
        match message {
            WorkHandlerIn::Disable => self.disable(),
            WorkHandlerIn::Enable(endpoint) => self.enable(endpoint),
            WorkHandlerIn::SendCustomMessage { message } =>
                self.send_message(message),
        }
    }

    #[inline]
    fn inject_dial_upgrade_error(&mut self, _: (), err: ProtocolsHandlerUpgrErr<io::Error>) {
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
            //info!("WorkProtocolsHandler poll events_queue");
            return Ok(Async::Ready(event));
        }

        // Kill the connection if needed.
        if let ProtocolState::KillAsap = self.state {
            return Err(WorkError);
        }

        // Process all the substreams.
        if let Some(event) = self.poll_state() {
            //info!("WorkProtocolsHandler poll_state");
            return Ok(Async::Ready(event));
        }

        Ok(Async::NotReady)
    }
}

pub struct RegisteredProtocol {
    id: String,
}

impl RegisteredProtocol {
    /// Creates a new `RegisteredProtocol`. The `custom_data` parameter will be
    /// passed inside the `RegisteredProtocolOutput`.
    pub fn new(protocol: String)
               -> Self {
        RegisteredProtocol {
            id: protocol,
        }
    }

    /// Returns the ID of the protocol.
    pub fn id(&self) -> String {
        self.id.clone()
    }
}

impl Clone for RegisteredProtocol {
    fn clone(&self) -> Self {
        RegisteredProtocol {
            id: self.id.clone(),
        }
    }
}

impl UpgradeInfo for RegisteredProtocol {
    type Info = String;
    type InfoIter = iter::Once<Self::Info>;

    #[inline]
    fn protocol_info(&self) -> Self::InfoIter {
        // Report each version as an individual protocol.
        iter::once(self.id.clone())
    }
}

impl<TSubstream> InboundUpgrade<TSubstream> for RegisteredProtocol
    where TSubstream: AsyncRead + AsyncWrite,
{
    type Output = RegisteredProtocolSubstream<TSubstream>;
    type Future = future::FutureResult<Self::Output, io::Error>;
    type Error = io::Error;

    fn upgrade_inbound(
        self,
        socket: Negotiated<TSubstream>,
        info: Self::Info,
    ) -> Self::Future {
        let framed: Framed<Negotiated<TSubstream>, UviBytes<Vec<u8>>> = {
            let mut codec = UviBytes::default();
            codec.set_max_len(16 * 1024 * 1024);        // 16 MiB hard limit for packets.
            Framed::new(socket, codec)
        };

        let mut inner: Fuse<Framed<Negotiated<TSubstream>, UviBytes<Vec<u8>>>> = framed.fuse();

        future::ok(RegisteredProtocolSubstream {
            is_closing: false,
            endpoint: Endpoint::Listener,
            send_queue: VecDeque::new(),
            requires_poll_complete: false,
            inner: inner,
            protocol_id: self.id,
            clogged_fuse: false,
        })
    }
}

impl<TSubstream> OutboundUpgrade<TSubstream> for RegisteredProtocol
    where TSubstream: AsyncRead + AsyncWrite,
{
    type Output = <Self as InboundUpgrade<TSubstream>>::Output;
    type Future = <Self as InboundUpgrade<TSubstream>>::Future;
    type Error = <Self as InboundUpgrade<TSubstream>>::Error;

    fn upgrade_outbound(
        self,
        socket: Negotiated<TSubstream>,
        info: Self::Info,
    ) -> Self::Future {
        let framed: Framed<Negotiated<TSubstream>, UviBytes<Vec<u8>>> = Framed::new(socket, UviBytes::default());

        let mut inner: Fuse<Framed<Negotiated<TSubstream>, UviBytes<Vec<u8>>>> = framed.fuse();

        future::ok(RegisteredProtocolSubstream {
            is_closing: false,
            endpoint: Endpoint::Dialer,
            send_queue: VecDeque::new(),
            requires_poll_complete: false,
            inner: inner,
            protocol_id: self.id,
            clogged_fuse: false,
        })
    }
}

pub struct RegisteredProtocolSubstream<TSubstream> {
    /// If true, we are in the process of closing the sink.
    is_closing: bool,
    /// Whether the local node opened this substream (dialer), or we received this substream from
    /// the remote (listener).
    endpoint: Endpoint,
    /// Buffer of packets to send.
    send_queue: VecDeque<Vec<u8>>,
    /// If true, we should call `poll_complete` on the inner sink.
    requires_poll_complete: bool,
    /// The underlying substream.
    inner: stream::Fuse<Framed<Negotiated<TSubstream>, UviBytes<Vec<u8>>>>,
    /// Id of the protocol.
    protocol_id: String,
    /// If true, we have sent a "remote is clogged" event recently and shouldn't send another one
    /// unless the buffer empties then fills itself again.
    clogged_fuse: bool,
}

impl<TSubstream> RegisteredProtocolSubstream<TSubstream> {
    /// Returns the protocol id.
    #[inline]
    pub fn protocol_id(&self) -> &str {
        &self.protocol_id
    }

    /// Returns whether the local node opened this substream (dialer), or we received this
    /// substream from the remote (listener).
    pub fn endpoint(&self) -> Endpoint {
        self.endpoint
    }

    /// Starts a graceful shutdown process on this substream.
    ///
    /// Note that "graceful" means that we sent a closing message. We don't wait for any
    /// confirmation from the remote.
    ///
    /// After calling this, the stream is guaranteed to finish soon-ish.
    pub fn shutdown(&mut self) {
        self.is_closing = true;
        self.send_queue.clear();
    }

    /// Sends a message to the substream.
    pub fn send_message(&mut self, data: String) {
        //info!("RegisteredProtocolSubstream send_message, data={}", data);
        if self.is_closing {
            return;
        }

        self.send_queue.push_back(data.into_bytes());
    }
}

#[derive(Debug, Clone)]
pub enum RegisteredProtocolEvent {
    /// Received a message from the remote.
    Message(String),

    /// Diagnostic event indicating that the connection is clogged and we should avoid sending too
    /// many messages to it.
    Clogged {
        /// Copy of the messages that are within the buffer, for further diagnostic.
        messages: Vec<String>,
    },
}

impl<TSubstream> Stream for RegisteredProtocolSubstream<TSubstream>
    where TSubstream: AsyncRead + AsyncWrite {
    type Item = RegisteredProtocolEvent;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // Flushing the local queue.
        while let Some(packet) = self.send_queue.pop_front() {
            match self.inner.start_send(packet)? {
                AsyncSink::NotReady(packet) => {
                    self.send_queue.push_front(packet);
                    break;
                }
                AsyncSink::Ready => self.requires_poll_complete = true,
            }
        }

        // If we are closing, close as soon as the Sink is closed.
        if self.is_closing {
            return Ok(self.inner.close()?.map(|()| None));
        }

        // Indicating that the remote is clogged if that's the case.
        if self.send_queue.len() >= 2048 {
            if !self.clogged_fuse {
                // Note: this fuse is important not just for preventing us from flooding the logs;
                // 	if you remove the fuse, then we will always return early from this function and
                //	thus never read any message from the network.
                self.clogged_fuse = true;
                return Ok(Async::Ready(Some(RegisteredProtocolEvent::Clogged {
                    messages: self.send_queue.iter()
                        .map(|m| String::from_utf8(m.to_vec()))
                        .filter_map(Result::ok)
                        .collect(),
                })));
            }
        } else {
            self.clogged_fuse = false;
        }

        // Flushing if necessary.
        if self.requires_poll_complete {
            if let Async::Ready(()) = self.inner.poll_complete()? {
                self.requires_poll_complete = false;
            }
        }

        // Receiving incoming packets.
        // Note that `inner` is wrapped in a `Fuse`, therefore we can poll it forever.
        match self.inner.poll()? {
            Async::Ready(Some(data)) => {
                //log::info!("-----inner.poll Ready Some");
                let message = String::from_utf8(data.to_vec())
                    .map_err(|_| {
                        warn!(target: "sub-libp2p", "Couldn't decode packet sent by the remote: {:?}", data);
                        io::ErrorKind::InvalidData
                    })?;
                Ok(Async::Ready(Some(RegisteredProtocolEvent::Message(message))))
            }
            Async::Ready(None) => {
                //log::info!("-----inner.poll Ready None");
                if !self.requires_poll_complete && self.send_queue.is_empty() {
                    Ok(Async::Ready(None))
                } else {
                    Ok(Async::NotReady)
                }
            }
            Async::NotReady => {
                //log::info!("-----inner.poll NotReady");
                Ok(Async::NotReady)
            }
        }
    }
}

impl<TSubstream> NetworkBehaviour for WorkBehaviour<TSubstream>
    where
        TSubstream: AsyncRead + AsyncWrite,
{
    type ProtocolsHandler = WorkProtocolsHandlerProto<TSubstream>;
    type OutEvent = WorkOut;

    fn new_handler(&mut self) -> Self::ProtocolsHandler {
        WorkProtocolsHandlerProto::new(self.protocol.clone())
    }

    fn addresses_of_peer(&mut self, _: &PeerId) -> Vec<Multiaddr> {
        Vec::new()
    }

    fn inject_connected(&mut self, peer_id: PeerId, connected_point: ConnectedPoint) {
        info!("WorkBehaviour inject_connected, peer_id: {}, connected_point: {:?}", peer_id, connected_point);

        self.events.push(NetworkBehaviourAction::SendEvent {
            peer_id: peer_id,
            event: WorkHandlerIn::Enable(connected_point.clone().into()),
        });
    }

    fn inject_disconnected(&mut self, peer_id: &PeerId, endpoint: ConnectedPoint) {
        info!("WorkBehaviour inject_disconnected, peer_id: {}, endpoint: {:?}", peer_id, endpoint);
        self.peers.remove(peer_id);
        info!("WorkBehaviour inject_disconnected, peers count: {}", self.peers.len());

        self.events.push(NetworkBehaviourAction::SendEvent {
            peer_id: peer_id.clone(),
            event: WorkHandlerIn::Disable,
        });
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

        match event {
            WorkHandlerOut::CustomMessage { message } => {
                let message = message.parse_message();
                match message {
                    Some(message) => match message {
                        t @ Message::Transaction{..} => {
                            info!("!!! Got a transaction: {:?}", t);
                        },
                        t @ Message::Block{..} => {
                            info!("!!! Got a block: {:?}", t);
                        },
                        t @ Message::BlockHead{..} => {
                            info!("!!! Got a block head: {:?}", t);
                        }
                    }
                    None => {}
                }
            }
            _ => {}
        }
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
            //info!("WorkBehaviour events not empty");
            return Async::Ready(self.events.remove(0));
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
                    self.work.disconnect_node(&peer_id);
                    return;
                }
                let shard_token = format!("{}/{}", USERAGENT_SHARD, self.shard_num);
                if !info.agent_version.contains(&shard_token) {
                    warn!(target: "sub-libp2p", "Connected to a node on different shard : {:?}, will not treat as discovered", info);
                    self.work.disconnect_node(&peer_id);
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
                self.work.add_discovered_node(&peer_id);
                self.events.push(BehaviourOut::Identified { peer_id: peer_id.clone(), info: info });
                info!(target: "sub-libp2p", "Identified: {}", peer_id);
            }
            IdentifyEvent::Error { .. } => {}
            IdentifyEvent::SendBack { result: Err(ref err), ref peer_id } =>
                debug!(target: "sub-libp2p", "Error when sending back identify info \
					to {:?} => {}", peer_id, err),
            IdentifyEvent::SendBack { .. } => {}
        }
    }
}

impl<TSubstream> NetworkBehaviourEventProcess<WorkOut> for Behavior<TSubstream> {
    fn inject_event(&mut self, event: WorkOut) {
        info!("WorkOut inject_event, event: {:?}", event);
        self.events.push(event.into());
    }
}

impl<TSubstream> Behavior<TSubstream> {
    fn poll<TEv>(&mut self) -> Async<NetworkBehaviourAction<TEv, BehaviourOut>> {
        if !self.events.is_empty() {
            return Async::Ready(NetworkBehaviourAction::GenerateEvent(self.events.remove(0)));
        }

        Async::NotReady
    }
}

pub struct Network {
    cmd: RunCmd,
    local_identity: Keypair,
    bootnodes_router_conf: Option<BootnodesRouterConf>,
    client: Arc<Client>,
}

impl Network {
    pub fn new(cmd: RunCmd,
               local_identity: Keypair,
               bootnodes_router_conf: Option<BootnodesRouterConf>,
               client: Arc<Client>) -> Self {
        Network {
            cmd,
            local_identity,
            bootnodes_router_conf,
            client,
        }
    }

    pub fn run(&self) {
        let bootnodes = self.get_bootnodes();

        let port = self.get_port();

        let listen_addresses: Vec<Multiaddr> = vec![
            iter::once(Protocol::Ip4(Ipv4Addr::new(0, 0, 0, 0)))
                .chain(iter::once(Protocol::Tcp(port)))
                .collect()
        ];

        let local_public = self.local_identity.public();
        let local_peer_id = local_public.clone().into_peer_id();
        info!(target: "sub-libp2p", "Local node identity is: {}", local_peer_id.to_base58());

        let user_agent = format!("{}/{}", USERAGENT_SHARD, &self.cmd.shard_num);
        let protocol_version = PROTOCOL_VERSION.to_string();

        info!(target: "sub-libp2p", "Local node protocol version: {}, user agent: {}", protocol_version, user_agent);

        let transport = libp2p::build_development_transport(self.local_identity.clone());

        let mut swarm = {
            let mut behavior = Behavior::new(protocol_version, user_agent, local_public, bootnodes, self.cmd.shard_num);

            libp2p::Swarm::new(transport, behavior, local_peer_id)
        };

        for listen_address in &listen_addresses {
            if let Err(err) = Swarm::listen_on(&mut swarm, listen_address.clone()) {
                warn!(target: "sub-libp2p", "Can't listen on {} because: {:?}", listen_address, err)
            }
        }

        let stdin_receiver = self.client.stdin_receiver.clone();
        let stdin_notify = self.client.stdin_notify.clone();

        let foreign_sender = self.client.foreign_sender.clone();
        let foreign_notify = self.client.foreign_notify.clone();

        let swarm_ref = Arc::new(Mutex::new(swarm));

        let swarm_clone = swarm_ref.clone();

        let thread = thread::Builder::new().name("stdin_channel".to_string()).spawn(move || {
            tokio::run(future::poll_fn(move || -> Result<_, ()> {
                loop {
                    stdin_notify.register();
                    match stdin_receiver.try_recv() {
                        Ok(msg) => {
                            swarm_clone.lock().broadcast_custom_message(msg);

                            foreign_sender.send(msg);
                            foreign_notify.notify();
                        }
                        Err(_) => {
                            return Ok(Async::NotReady);
                        }
                    }
                }
                Ok(Async::NotReady)
            }));
        });

        let mut listening = false;

        let thread = thread::Builder::new().name("network".to_string()).spawn(move || {
            tokio::run(future::poll_fn(move || -> Result<_, ()> {
                loop {
                    let mut s = swarm_ref.lock();
                    match s.poll().expect("Error while polling swarm") {
                        Async::Ready(Some(e)) => info!("swarm poll: {:?}", e),
                        Async::Ready(None) | Async::NotReady => {
                            if !listening {
                                if let Some(a) = Swarm::listeners(&s).next() {
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

    fn get_port(&self) -> u16 {
        let port = match self.cmd.shared_params.port {
            Some(port) => port,
            None => params::DEFAULT_PORT,
        };

        port
    }

    fn get_bootnodes(&self) -> Vec<(PeerId, Multiaddr)> {
        let shard_num = self.cmd.shard_num;

        let bootnodes_router_conf = (&self.bootnodes_router_conf).clone();

        let mut shards: HashMap<String, Shard>;

        let bootnodes_str = match bootnodes_router_conf {
            Some(result) => {
                shards = result.shards;
                let shard = shards.get(&format!("{}", shard_num));
                match shard {
                    Some(shard) => &shard.native,
                    None => &self.cmd.bootnodes,
                }
            }
            None => &self.cmd.bootnodes,
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
}