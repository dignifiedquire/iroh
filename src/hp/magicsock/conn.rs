use std::{
    collections::{hash_map, HashMap, HashSet, VecDeque},
    io::{self, IoSliceMut},
    mem::MaybeUninit,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    ops::Deref,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering},
        Arc,
    },
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

use anyhow::{bail, Context as _, Result};
use backoff::backoff::Backoff;
use bytes::{Bytes, BytesMut};
use futures::{future::BoxFuture, Stream, StreamExt};
use quinn::AsyncUdpSocket;
use rand::{seq::SliceRandom, Rng, SeedableRng};
use tokio::{
    sync::{self, Mutex},
    task::JoinHandle,
    time,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, trace, warn};

use crate::{
    hp::{
        cfg::{self, DERP_MAGIC_IP},
        derp::{self, client::PacketSplitIter, DerpMap, DerpRegion},
        disco, key, netcheck, netmap, portmapper, stun,
    },
    inc,
    metrics::magicsock::MagicsockMetrics,
    net::ip::LocalAddresses,
    record,
};

use super::{
    endpoint::{Options as EndpointOptions, PeerMap},
    rebinding_conn::RebindingUdpConn,
    DERP_CLEAN_STALE_INTERVAL, DERP_INACTIVE_CLEANUP_TIME, ENDPOINTS_FRESH_ENOUGH_DURATION,
    HEARTBEAT_INTERVAL,
};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum CurrentPortFate {
    Keep,
    Drop,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum Network {
    Ipv4,
    Ipv6,
}

impl From<IpAddr> for Network {
    fn from(value: IpAddr) -> Self {
        match value {
            IpAddr::V4(_) => Self::Ipv4,
            IpAddr::V6(_) => Self::Ipv6,
        }
    }
}

impl Network {
    pub(super) fn default_addr(&self) -> IpAddr {
        match self {
            Self::Ipv4 => Ipv4Addr::UNSPECIFIED.into(),
            Self::Ipv6 => Ipv6Addr::UNSPECIFIED.into(),
        }
    }
}

impl From<Network> for socket2::Domain {
    fn from(value: Network) -> Self {
        match value {
            Network::Ipv4 => socket2::Domain::IPV4,
            Network::Ipv6 => socket2::Domain::IPV6,
        }
    }
}

/// Contains options for `Conn::listen`.
#[derive(derive_more::Debug)]
pub struct Options {
    /// The port to listen on.
    /// Zero means to pick one automatically.
    pub port: u16,

    /// Optionally provides a func to be called when endpoints change.
    #[allow(clippy::type_complexity)]
    #[debug("on_endpoints: Option<Box<..>>")]
    pub on_endpoints: Option<Box<dyn Fn(&[cfg::Endpoint]) + Send + Sync + 'static>>,

    /// Optionally provides a func to be called when a connection is made to a DERP server.
    #[debug("on_derp_active: Option<Box<..>>")]
    pub on_derp_active: Option<Box<dyn Fn() + Send + Sync + 'static>>,

    /// A callback that provides a `cfg::NetInfo` when discovered network conditions change.
    #[debug("on_net_info: Option<Box<..>>")]
    pub on_net_info: Option<Box<dyn Fn(cfg::NetInfo) + Send + Sync + 'static>>,

    /// Private key for this node.
    pub private_key: key::node::SecretKey,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            port: 0,
            on_endpoints: None,
            on_derp_active: None,
            on_net_info: None,
            private_key: key::node::SecretKey::generate(),
        }
    }
}

/// Iroh connectivity layer.
///
/// This is responsible for routing packets to peers based on peer IDs, it will initially
/// route packets via a derper relay and transparently try and establish a peer-to-peer
/// connection and upgrade to it.  It will also keep looking for better connections as the
/// network details of both endpoints change.
///
/// It is usually only necessary to use a single [`Conn`] instance in an application, it
/// means any QUIC endpoints on top will be sharing as much information about peers as
/// possible.
#[derive(Clone, Debug)]
pub struct Conn {
    inner: Arc<Inner>,
    // None when closed
    actor_task: Arc<Mutex<Option<JoinHandle<()>>>>,
}

impl Deref for Conn {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[derive(derive_more::Debug)]
pub struct Inner {
    actor_sender: flume::Sender<ActorMessage>,
    /// Sends network messages.
    network_sender: flume::Sender<Vec<quinn_udp::Transmit>>,
    pub(super) name: String,
    #[allow(clippy::type_complexity)]
    #[debug("on_endpoints: Option<Box<..>>")]
    on_endpoints: Option<Box<dyn Fn(&[cfg::Endpoint]) + Send + Sync + 'static>>,
    #[debug("on_derp_active: Option<Box<..>>")]
    on_derp_active: Option<Box<dyn Fn() + Send + Sync + 'static>>,
    /// A callback that provides a `cfg::NetInfo` when discovered network conditions change.
    #[debug("on_net_info: Option<Box<..>>")]
    on_net_info: Option<Box<dyn Fn(cfg::NetInfo) + Send + Sync + 'static>>,

    /// Used for receiving DERP messages.
    network_recv_ch: flume::Receiver<NetworkReadResult>,
    /// Stores wakers, to be called when derp_recv_ch receives new data.
    network_recv_wakers: std::sync::Mutex<Option<Waker>>,
    network_send_wakers: std::sync::Mutex<Option<Waker>>,

    public_key: key::node::PublicKey,

    /// Preferred port from `Options::port`; 0 means auto.
    port: AtomicU16,

    /// Close is in progress (or done)
    closing: AtomicBool,
    /// Close was called.
    closed: AtomicBool,
}

impl Inner {
    async fn get_derp_region(&self, region: usize) -> Option<DerpRegion> {
        let (s, r) = sync::oneshot::channel();
        self.actor_sender
            .send_async(ActorMessage::GetDerpRegion(region, s))
            .await
            .ok()?;
        r.await.ok()?
    }

    fn is_closing(&self) -> bool {
        self.closing.load(Ordering::Relaxed)
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }
}

#[derive(Debug)]
struct EndpointUpdateState {
    /// If running, set to the task handle of the update.
    running: sync::watch::Sender<Option<&'static str>>,
    want_update: Option<&'static str>,
}

impl EndpointUpdateState {
    fn new() -> Self {
        let (running, _) = sync::watch::channel(None);
        EndpointUpdateState {
            running,
            want_update: None,
        }
    }

    /// Returns `true` if an update is currently in progress.
    fn is_running(&self) -> bool {
        self.running.borrow().is_some()
    }
}

impl Conn {
    /// Creates a magic `Conn` listening on `opts.port`.
    /// As the set of possible endpoints for a Conn changes, the callback opts.EndpointsFunc is called.
    #[instrument(skip_all, fields(name))]
    pub async fn new(opts: Options) -> Result<Self> {
        let name = format!(
            "magic-{}",
            hex::encode(&opts.private_key.public_key().as_ref()[..8])
        );
        let port_mapper = portmapper::Client::new(); // TODO: pass self.on_port_map_changed

        let Options {
            port,
            on_endpoints,
            on_derp_active,
            on_net_info,
            private_key,
        } = opts;

        let (network_recv_ch_sender, network_recv_ch_receiver) = flume::bounded(128);

        let (pconn4, pconn6) = bind(port).await?;
        let port = pconn4.port();
        port_mapper.set_local_port(port).await;

        let net_checker = netcheck::Client::new(Some(port_mapper.clone())).await?;
        let (actor_sender, actor_receiver) = flume::bounded(128);
        let (network_sender, network_receiver) = flume::bounded(128);

        let inner = Arc::new(Inner {
            name,
            on_endpoints,
            on_derp_active,
            on_net_info,
            port: AtomicU16::new(port),
            public_key: private_key.public_key(),
            closing: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            network_recv_ch: network_recv_ch_receiver,
            network_recv_wakers: std::sync::Mutex::new(None),
            network_send_wakers: std::sync::Mutex::new(None),
            actor_sender: actor_sender.clone(),
            network_sender,
        });

        let conn = inner.clone();
        let derp_task = tokio::task::spawn(async move {
            let actor = Actor {
                msg_receiver: actor_receiver,
                msg_sender: actor_sender,
                network_receiver,
                conn,
                net_map: None,
                derp_recv_sender: network_recv_ch_sender,
                active_derp: HashMap::default(),
                derp_route: HashMap::new(),
                endpoints_update_state: EndpointUpdateState::new(),
                last_endpoints: Vec::new(),
                last_endpoints_time: None,
                on_endpoint_refreshed: HashMap::new(),
                periodic_re_stun_timer: new_re_stun_timer(),
                net_info_last: None,
                disco_info: HashMap::new(),
                peer_map: Default::default(),
                port_mapper,
                net_checker,
                enable_stun_packets: false,
                pconn4,
                pconn6,
                udp_state: quinn_udp::UdpState::default(),
                derp_map: Default::default(),
                private_key,
                my_derp: 0,
                ipv6_reported: Arc::new(AtomicBool::new(false)),
                no_v4_send: false,
            };

            if let Err(err) = actor.run().await {
                warn!("derp handler errored: {:?}", err);
            }
        });

        let c = Conn {
            inner,
            actor_task: Arc::new(Mutex::new(Some(derp_task))),
        };

        Ok(c)
    }

    pub async fn tracked_endpoints(&self) -> Result<Vec<key::node::PublicKey>> {
        let (s, r) = sync::oneshot::channel();
        self.actor_sender
            .send_async(ActorMessage::TrackedEndpoints(s))
            .await?;
        let res = r.await?;
        Ok(res)
    }

    pub async fn local_endpoints(&self) -> Result<Vec<cfg::Endpoint>> {
        let (s, r) = sync::oneshot::channel();
        self.actor_sender
            .send_async(ActorMessage::LocalEndpoints(s))
            .await?;
        let res = r.await?;
        Ok(res)
    }

    pub async fn local_addr(&self) -> Result<(SocketAddr, Option<SocketAddr>)> {
        let (s, r) = flume::bounded(1);
        self.actor_sender
            .send_async(ActorMessage::GetLocalAddr(s))
            .await?;
        let (v4, v6) = r.recv_async().await?;
        if let Some(v6) = v6 {
            return Ok((v4?, Some(v6?)));
        }
        Ok((v4?, None))
    }

    /// Triggers an address discovery. The provided why string is for debug logging only.
    #[instrument(skip_all, fields(self.name = %self.name))]
    pub async fn re_stun(&self, why: &'static str) {
        self.actor_sender
            .send_async(ActorMessage::ReStun(why))
            .await
            .unwrap();
    }

    /// Returns the [`SocketAddr`] which can be used by the QUIC layer to dial this peer.
    ///
    /// Note this is a user-facing API and does not wrap the [`SocketAddr`] in a
    /// `QuicMappedAddr` as we do internally.
    pub async fn get_mapping_addr(&self, node_key: &key::node::PublicKey) -> Option<SocketAddr> {
        let (s, r) = tokio::sync::oneshot::channel();
        if self
            .actor_sender
            .send_async(ActorMessage::GetMappingAddr(node_key.clone(), s))
            .await
            .is_ok()
        {
            return r.await.ok().flatten().map(|m| m.0);
        }
        None
    }

    // TODO
    // /// Handles a "ping" CLI query.
    // #[instrument(skip_all, fields(self.name = %self.name))]
    // pub async fn ping<F>(&self, peer: cfg::Node, mut res: cfg::PingResult, cb: F)
    // where
    //     F: Fn(cfg::PingResult) -> BoxFuture<'static, ()> + Send + Sync + 'static,
    // {
    //     res.node_ip = peer.addresses.get(0).copied();
    //     res.node_name = match peer.name.as_ref().and_then(|n| n.split('.').next()) {
    //         Some(name) => {
    //             // prefer DNS name
    //             Some(name.to_string())
    //         }
    //         None => {
    //             // else hostname
    //             Some(peer.hostinfo.hostname.clone())
    //         }
    //     };
    //     let ep = self
    //         .peer_map
    //         .read()
    //         .await
    //         .endpoint_for_node_key(&peer.key)
    //         .cloned();
    //     match ep {
    //         Some(ep) => {
    //             ep.cli_ping(res, cb).await;
    //         }
    //         None => {
    //             res.err = Some("unknown peer".to_string());
    //             cb(res);
    //         }
    //     }
    // }

    /// Sets the connection's preferred local port.
    #[instrument(skip_all, fields(self.name = %self.name))]
    pub async fn set_preferred_port(&self, port: u16) {
        let (s, r) = sync::oneshot::channel();
        self.actor_sender
            .send_async(ActorMessage::SetPreferredPort(port, s))
            .await
            .unwrap();
        r.await.unwrap();
    }

    /// Controls which (if any) DERP servers are used. A `None` value means to disable DERP; it's disabled by default.
    #[instrument(skip_all, fields(self.name = %self.name))]
    pub async fn set_derp_map(&self, dm: Option<derp::DerpMap>) -> Result<()> {
        let (s, r) = sync::oneshot::channel();
        self.actor_sender
            .send_async(ActorMessage::SetDerpMap(dm, s))
            .await?;
        r.await?;
        Ok(())
    }

    /// Called when the control client gets a new network map from the control server.
    /// It should not use the DerpMap field of NetworkMap; that's
    /// conditionally sent to set_derp_map instead.
    #[instrument(skip_all, fields(self.name = %self.name))]
    pub async fn set_network_map(&self, nm: netmap::NetworkMap) -> Result<()> {
        let (s, r) = sync::oneshot::channel();
        self.actor_sender
            .send_async(ActorMessage::SetNetworkMap(nm, s))
            .await?;
        r.await?;
        Ok(())
    }

    /// Closes the connection.
    ///
    /// Only the first close does anything. Any later closes return nil.
    #[instrument(skip_all, fields(self.name = %self.name))]
    pub async fn close(&self) -> Result<()> {
        if self.is_closed() {
            return Ok(());
        }
        self.actor_sender.send_async(ActorMessage::Shutdown).await?;

        self.closing.store(true, Ordering::Relaxed);

        self.closed.store(true, Ordering::SeqCst);
        // c.connCtxCancel()

        if let Some(task) = self.actor_task.lock().await.take() {
            task.await?;
        }

        Ok(())
    }

    #[instrument(skip_all, fields(self.name = %self.name))]
    async fn on_port_map_changed(&self) {
        self.re_stun("portmap-changed").await;
    }

    /// Closes and re-binds the UDP sockets and resets the DERP connection.
    /// It should be followed by a call to ReSTUN.
    #[instrument(skip_all, fields(self.name = %self.name))]
    pub async fn rebind_all(&self) {
        let (s, r) = sync::oneshot::channel();
        self.actor_sender
            .send_async(ActorMessage::RebindAll(s))
            .await
            .unwrap();
        r.await.unwrap();
    }
}

/// A route entry for a public key, saying that a certain peer should be available at DERP
/// node derpID, as long as the current connection for that derpID is dc. (but dc should not be
/// used to write directly; it's owned by the read/write loops)
#[derive(Debug)]
struct DerpRoute {
    derp_id: u16,
    dc: derp::http::Client, // don't use directly; see comment above
}

/// The info and state for the DiscoKey in the Conn.discoInfo map key.
///
/// Note that a DiscoKey does not necessarily map to exactly one
/// node. In the case of shared nodes and users switching accounts, two
/// nodes in the NetMap may legitimately have the same DiscoKey.  As
/// such, no fields in here should be considered node-specific.
pub(super) struct DiscoInfo {
    pub(super) node_key: key::node::PublicKey,
    /// The precomputed key for communication with the peer that has the `node_key` used to
    /// look up this `DiscoInfo` in Conn.discoInfo.
    /// Not modified once initialized.
    shared_key: key::node::SharedSecret,

    /// Tthe src of a ping for `node_key`.
    last_ping_from: Option<SocketAddr>,

    /// The last time of a ping for `node_key`.
    last_ping_time: Option<Instant>,
}

/// Reports whether x and y represent the same set of endpoints. The order doesn't matter.
fn endpoint_sets_equal(xs: &[cfg::Endpoint], ys: &[cfg::Endpoint]) -> bool {
    if xs.is_empty() && ys.is_empty() {
        return true;
    }
    if xs.len() == ys.len() {
        let mut order_matches = true;
        for (i, x) in xs.iter().enumerate() {
            if x != &ys[i] {
                order_matches = false;
                break;
            }
        }
        if order_matches {
            return true;
        }
    }
    let mut m: HashMap<&cfg::Endpoint, usize> = HashMap::new();
    for x in xs {
        *m.entry(x).or_default() |= 1;
    }
    for y in ys {
        *m.entry(y).or_default() |= 2;
    }

    m.values().all(|v| *v == 3)
}

impl AsyncUdpSocket for Conn {
    #[instrument(skip_all, fields(self.name = %self.name))]
    fn poll_send(
        &self,
        _udp_state: &quinn_udp::UdpState,
        cx: &mut Context,
        transmits: &[quinn_udp::Transmit],
    ) -> Poll<io::Result<usize>> {
        let bytes_total: usize = transmits.iter().map(|t| t.contents.len()).sum();
        record!(MagicsockMetrics::SendData, bytes_total as _);

        if self.is_closed() {
            record!(MagicsockMetrics::SendDataNetworkDown, bytes_total as _);
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "connection closed",
            )));
        }

        let mut n = 0;
        if transmits.is_empty() {
            return Poll::Ready(Ok(n));
        }

        // Split up transmits by destination, as the rest of the code assumes single dest.
        let groups = TransmitIter::new(transmits);
        for group in groups {
            if self.network_sender.is_full() {
                // TODO: add counter?
                self.network_send_wakers
                    .lock()
                    .unwrap()
                    .replace(cx.waker().clone());
                break;
            }
            if self.network_sender.is_disconnected() {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "connection closed",
                )));
            }

            n += group.len();
            self.network_sender.try_send(group).expect("just checked");
        }
        if n > 0 {
            return Poll::Ready(Ok(n));
        }

        Poll::Pending
    }

    #[instrument(skip_all, fields(self.name = %self.name))]
    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [io::IoSliceMut<'_>],
        metas: &mut [quinn_udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        // FIXME: currently ipv4 load results in ipv6 traffic being ignored
        debug_assert_eq!(bufs.len(), metas.len(), "non matching bufs & metas");
        if self.is_closed() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "connection closed",
            )));
        }

        let mut num_msgs = 0;
        for (buf_out, meta_out) in bufs.iter_mut().zip(metas.iter_mut()) {
            match self.network_recv_ch.try_recv() {
                Err(flume::TryRecvError::Empty) => {
                    self.network_recv_wakers
                        .lock()
                        .unwrap()
                        .replace(cx.waker().clone());
                    break;
                }
                Err(flume::TryRecvError::Disconnected) => {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::NotConnected,
                        "connection closed",
                    )));
                }
                Ok(dm) => {
                    if self.is_closed() {
                        break;
                    }

                    match dm {
                        NetworkReadResult::Error(err) => {
                            return Poll::Ready(Err(err));
                        }
                        NetworkReadResult::Ok {
                            bytes,
                            meta,
                            source,
                        } => {
                            buf_out[..bytes.len()].copy_from_slice(&bytes);
                            *meta_out = meta;

                            match source {
                                NetworkSource::Derp => {
                                    record!(MagicsockMetrics::RecvDataDerp, bytes.len() as _);
                                }
                                NetworkSource::Ipv4 => {
                                    record!(MagicsockMetrics::RecvDataIPv4, bytes.len() as _);
                                }
                                NetworkSource::Ipv6 => {
                                    record!(MagicsockMetrics::RecvDataIPv6, bytes.len() as _);
                                }
                            }
                            trace!(
                                "[QUINN] <- {} ({}b) ({}) ({:?}, {:?})",
                                meta_out.addr,
                                meta_out.len,
                                self.name,
                                meta_out.dst_ip,
                                source
                            );
                        }
                    }

                    num_msgs += 1;
                }
            }
        }
        debug!(
            "poll_recv: received: {} - requested: {} - msg_buffer: {}",
            num_msgs,
            bufs.len(),
            self.network_recv_ch.len(),
        );

        // If we have any msgs to report, they are in the first `num_msgs_total` slots
        if num_msgs > 0 {
            info!("received {} msgs", num_msgs);
            return Poll::Ready(Ok(num_msgs));
        }

        Poll::Pending
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        let (s, r) = flume::bounded(1);
        self.actor_sender
            .send(ActorMessage::GetLocalAddr(s))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        let (v4, v6) = r
            .recv()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
        // Default to v6 to pretend v6 for quinn in all cases (needs more thought)
        if let Some(v6) = v6 {
            return v6;
        }
        v4
    }
}

#[derive(Debug)]
enum NetworkReadResult {
    Error(io::Error),
    Ok {
        source: NetworkSource,
        meta: quinn_udp::RecvMeta,
        bytes: Bytes,
    },
}

#[derive(Debug)]
enum NetworkSource {
    Ipv4,
    Ipv6,
    Derp,
}

#[derive(derive_more::Debug)]
struct DerpReadResult {
    region_id: u16,
    src: key::node::PublicKey,
    /// packet data
    #[debug(skip)]
    buf: Bytes,
}

/// Contains fields for an active DERP connection.
#[derive(Debug)]
struct ActiveDerp {
    c: derp::http::Client,
    cancel: CancellationToken,
    /// The time of the last request for its write
    /// channel (currently even if there was no write).
    last_write: Instant,
    create_time: Instant,
}

/// Simple DropGuard for decrementing a Waitgroup.
struct WgGuard(wg::AsyncWaitGroup);
impl Drop for WgGuard {
    fn drop(&mut self) {
        self.0.done();
    }
}

#[derive(Debug)]
pub(super) enum ActorMessage {
    GetLocalAddr(flume::Sender<(io::Result<SocketAddr>, Option<io::Result<SocketAddr>>)>),
    SetDerpMap(Option<DerpMap>, sync::oneshot::Sender<()>),
    GetDerpRegion(usize, sync::oneshot::Sender<Option<DerpRegion>>),
    TrackedEndpoints(sync::oneshot::Sender<Vec<key::node::PublicKey>>),
    LocalEndpoints(sync::oneshot::Sender<Vec<cfg::Endpoint>>),
    GetMappingAddr(
        key::node::PublicKey,
        sync::oneshot::Sender<Option<QuicMappedAddr>>,
    ),
    SetPreferredPort(u16, sync::oneshot::Sender<()>),
    RebindAll(sync::oneshot::Sender<()>),
    Shutdown,
    CloseOrReconnect(u16, &'static str),
    ReStun(&'static str),
    Connected(ReaderState),
    EnqueueCallMeMaybe {
        derp_addr: SocketAddr,
        endpoint_id: usize,
    },
    SendDiscoMessage {
        dst: SocketAddr,
        dst_key: key::node::PublicKey,
        msg: disco::Message,
    },
    SetNetworkMap(netmap::NetworkMap, sync::oneshot::Sender<()>),
}

struct Actor {
    conn: Arc<Inner>,
    net_map: Option<netmap::NetworkMap>,
    msg_receiver: flume::Receiver<ActorMessage>,
    msg_sender: flume::Sender<ActorMessage>,
    network_receiver: flume::Receiver<Vec<quinn_udp::Transmit>>,
    /// Channel to send received derp messages on, for processing.
    derp_recv_sender: flume::Sender<NetworkReadResult>,
    /// DERP regionID -> connection to a node in that region
    active_derp: HashMap<u16, ActiveDerp>,
    /// Contains optional alternate routes to use as an optimization instead of
    /// contacting a peer via their home DERP connection.  If they sent us a message
    /// on a different DERP connection (which should really only be on our DERP
    /// home connection, or what was once our home), then we remember that route here to optimistically
    /// use instead of creating a new DERP connection back to their home.
    derp_route: HashMap<key::node::PublicKey, DerpRoute>,
    /// Indicates the update endpoint state.
    endpoints_update_state: EndpointUpdateState,
    /// Records the endpoints found during the previous
    /// endpoint discovery. It's used to avoid duplicate endpoint change notifications.
    last_endpoints: Vec<cfg::Endpoint>,

    /// The last time the endpoints were updated, even if there was no change.
    last_endpoints_time: Option<Instant>,

    /// Functions to run (in their own tasks) when endpoints are refreshed.
    on_endpoint_refreshed:
        HashMap<usize, Box<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static>>,
    /// When set, is an AfterFunc timer that will call Conn::do_periodic_stun.
    periodic_re_stun_timer: time::Interval,
    /// The `NetInfo` provided in the last call to `net_info_func`. It's used to deduplicate calls to netInfoFunc.
    net_info_last: Option<cfg::NetInfo>,
    /// The state for an active DiscoKey.
    disco_info: HashMap<key::node::PublicKey, DiscoInfo>,
    /// Tracks the networkmap node entity for each peer discovery key.
    peer_map: PeerMap,

    // The underlying UDP sockets used to send/rcv packets.
    pconn4: RebindingUdpConn,
    pconn6: Option<RebindingUdpConn>,
    udp_state: quinn_udp::UdpState,

    /// The prober that discovers local network conditions, including the closest DERP relay and NAT mappings.
    net_checker: netcheck::Client,

    enable_stun_packets: bool,

    /// The NAT-PMP/PCP/UPnP prober/client, for requesting port mappings from NAT devices.
    port_mapper: portmapper::Client,

    /// Whether IPv4 UDP is known to be unable to transmit
    /// at all. This could happen if the socket is in an invalid state
    /// (as can happen on darwin after a network link status change).
    no_v4_send: bool,

    /// Private key for this node
    private_key: key::node::SecretKey,

    /// If the last netcheck report, reports IPv6 to be available.
    ipv6_reported: Arc<AtomicBool>,

    /// None (or zero regions/nodes) means DERP is disabled.
    /// Tracked outside to avoid deadlock issues (replaces atomic acess from go)
    derp_map: Option<DerpMap>,

    /// Nearest DERP region ID; 0 means none/unknown.
    my_derp: u16,
}

impl Actor {
    #[instrument(level = "error", skip_all, fields(self.name = %self.conn.name))]
    async fn run(mut self) -> Result<()> {
        let mut cleanup_timer = time::interval_at(
            time::Instant::now() + DERP_CLEAN_STALE_INTERVAL,
            DERP_CLEAN_STALE_INTERVAL,
        );
        let mut endpoint_heartbeat_timer = time::interval(HEARTBEAT_INTERVAL);
        let mut endpoints_update_receiver = self.endpoints_update_state.running.subscribe();
        let mut recvs = futures::stream::FuturesUnordered::new();

        let mut ip_stream = IpStream::new(
            &self.udp_state,
            self.conn.clone(),
            self.pconn4.clone(),
            self.pconn6.clone(),
        );
        loop {
            tokio::select! {
                transmits = self.network_receiver.recv_async() => {
                    debug!("tick: network send");
                    self.send_network(transmits?).await;
                    debug!("tick: msg");
                    {
                        // Wake up the send waker if one is waiting for space in the channel
                        let mut wakers = self.conn.network_send_wakers.lock().unwrap();
                        if let Some(waker) = wakers.take() {
                            waker.wake();
                        }
                    }
                }
                msg = self.msg_receiver.recv_async() => {
                    match msg? {
                        ActorMessage::GetLocalAddr(s) => {
                            let addr = self.local_addr();
                            let _ = s.send(addr);
                        }
                        ActorMessage::SetDerpMap(dm, s) => {
                            self.set_derp_map(dm).await;
                            let _ = s.send(());
                        }
                        ActorMessage::GetDerpRegion(region_id, s) => {
                            let res = match self.derp_map {
                                None => None,
                                Some(ref derp_map) => {
                                    derp_map.regions.get(&region_id).cloned()
                                }
                            };
                            let _ = s.send(res);
                        }
                        ActorMessage::TrackedEndpoints(s) => {
                            let eps: Vec<_> = self.peer_map.endpoints().filter_map(|(_, ep)| ep.public_key.clone()).collect();
                            let _ = s.send(eps);
                        }
                        ActorMessage::LocalEndpoints(s) => {
                            let eps: Vec<_> = self.last_endpoints.clone();
                            let _ = s.send(eps);
                        }
                        ActorMessage::GetMappingAddr(node_key, s) => {
                            let res = self.peer_map
                                .endpoint_for_node_key(&node_key)
                                .map(|ep| ep.quic_mapped_addr);
                            let _ = s.send(res);
                        }
                        ActorMessage::Shutdown => {
                            for (_, ep) in self.peer_map.endpoints_mut() {
                                ep.stop_and_reset();
                            }
                            self.close_all_derp("conn-close").await;
                            self.port_mapper.close();

                            // Ignore errors from pconnN
                            // They will frequently have been closed already by a call to connBind.Close.
                            if let Some(ref conn) = self.pconn6 {
                                conn.close().await.ok();
                            }
                            self.pconn4.close().await.ok();

                            return Ok(());
                        }
                        ActorMessage::CloseOrReconnect(rid, reason) => {
                            self.close_or_reconnect_derp(rid, reason).await;
                        }
                        ActorMessage::ReStun(reason) => {
                            self.re_stun(reason).await;
                        }
                        ActorMessage::Connected(rs) => {
                            recvs.push(rs.recv())
                        }
                        ActorMessage::EnqueueCallMeMaybe {
                            derp_addr,
                            endpoint_id,
                        } => {
                            self.enqueue_call_me_maybe(derp_addr, endpoint_id).await;
                        }
                        ActorMessage::RebindAll(s) => {
                            self.rebind_all().await;
                            let _ = s.send(());
                        }
                        ActorMessage::SetPreferredPort(port, s) => {
                            self.set_preferred_port(port).await;
                            let _ = s.send(());
                        }
                        ActorMessage::SendDiscoMessage {
                            dst, dst_key, msg,
                        } => {
                            let _res = self.send_disco_message(dst, dst_key, msg).await;
                        }
                        ActorMessage::SetNetworkMap(nm, s) => {
                            self.set_network_map(nm);
                            s.send(()).unwrap();
                        }
                    }
                }
                Some((rs, result, action)) = recvs.next() => {
                    debug!("tick: recvs: {:?}, {:?}", result, action);
                    match action {
                        ReadAction::None => {},
                        ReadAction::AddPeerRoute { peers, region, derp_client } => {
                            for peer in peers {
                                self.add_derp_peer_route(peer, region, derp_client.clone());
                            }
                        },
                        ReadAction::RemovePeerRoute { peers, region, derp_client } => {
                            for peer in peers {
                                self.remove_derp_peer_route(peer, region, &derp_client);
                            }
                        }
                    }
                    match result {
                        ReadResult::Break => {
                            // drop client
                            continue;
                        }
                        ReadResult::Continue => {
                            recvs.push(rs.recv())
                        }
                        ReadResult::Yield(read_result) => {
                            let passthroughs = self.process_derp_read_result(read_result).await;
                            for passthrough in passthroughs {
                                self.derp_recv_sender.send_async(passthrough).await.expect("missing recv sender");
                                let mut wakers = self.conn.network_recv_wakers.lock().unwrap();
                                if let Some(waker) = wakers.take() {
                                    waker.wake();
                                }
                            }
                            recvs.push(rs.recv());

                        }
                    }
                }
                Some(ip_msgs) = ip_stream.next() => {
                    match ip_msgs {
                        Ok((bytes, network, mut meta)) => {
                            if self.receive_ip(&bytes, &mut meta, network).await {
                                match network {
                                    Network::Ipv4 => {
                                        let _ = self.derp_recv_sender.send_async(NetworkReadResult::Ok {
                                            source: NetworkSource::Ipv4,
                                            bytes,
                                            meta,
                                        }).await;
                                    }
                                    Network::Ipv6 => {
                                        let _ = self.derp_recv_sender.send_async(NetworkReadResult::Ok {
                                            source: NetworkSource::Ipv6,
                                            bytes,
                                            meta,
                                        }).await;
                                    }
                                }
                                let mut wakers = self.conn.network_recv_wakers.lock().unwrap();
                                if let Some(waker) = wakers.take() {
                                    waker.wake();
                                }
                            }
                        }
                        Err(err) => {
                            let _ = self.derp_recv_sender.send_async(NetworkReadResult::Error(err)).await;
                            let mut wakers = self.conn.network_recv_wakers.lock().unwrap();
                            while let Some(waker) = wakers.take() {
                                waker.wake();
                            }
                        }
                    }
                }
                tick = self.periodic_re_stun_timer.tick() => {
                    debug!("tick: re_stun {:?}", tick);
                    self.re_stun("periodic").await;
                }
                _ = endpoint_heartbeat_timer.tick() => {
                    debug!("tick: endpoint heartbeat {} endpoints", self.peer_map.node_count());
                    // TODO: this might trigger too many packets at once, pace this
                    for (_, ep) in self.peer_map.endpoints_mut() {
                        ep.stayin_alive().await;
                    }
                }
                _ = endpoints_update_receiver.changed() => {
                    let reason = *endpoints_update_receiver.borrow();
                    debug!("tick: endpoints update receiver {:?}", reason);
                    if let Some(reason) = reason {
                        self.update_endpoints(reason).await;
                    }
                }
                _ = cleanup_timer.tick() => {
                    debug!("tick: cleanup");
                    self.clean_stale_derp().await;
                }
                else => {
                    debug!("tick: other");
                }
            }
        }
    }

    /// Decides if a received UDP packet should be passed to the above QUIC layer or not.
    ///
    /// This also modifies the [`quinn_udp::RecvMeta`] for the packet to set the addresses
    /// to those that the QUIC layer should see.  E.g. the remote address will be set to the
    /// [`QuicMappedAddr`] instead of the actual remote.
    ///
    /// Returns `false` if this is an internal packet and it should not be reported.
    async fn receive_ip(
        &mut self,
        bytes: &Bytes,
        meta: &mut quinn_udp::RecvMeta,
        network: Network,
    ) -> bool {
        debug!(
            "received data {} from {} on {:?}",
            meta.len, meta.addr, network
        );

        if stun::is(bytes) {
            debug!("received STUN message {}", bytes.len());
            self.on_stun_receive(bytes, meta.addr).await;
            return false;
        }
        if self.handle_disco_message(bytes, meta.addr, None).await {
            debug!("received DISCO message {}", bytes.len());
            return false;
        }
        match self.peer_map.endpoint_for_ip_port(&meta.addr) {
            None => {
                info!("no peer_map state found for {}", meta.addr);

                let id = self.peer_map.insert_endpoint(EndpointOptions {
                    conn_sender: self.conn.actor_sender.clone(),
                    conn_public_key: self.conn.public_key.clone(),
                    public_key: None,
                    derp_addr: None,
                });
                self.peer_map.set_endpoint_for_ip_port(&meta.addr, id);

                let ep = self.peer_map.by_id_mut(&id).expect("inserted");
                ep.maybe_add_best_addr(meta.addr);
                meta.addr = ep.quic_mapped_addr.0;
            }
            Some(ep) => {
                debug!("peer_map state found for {}", meta.addr);

                meta.addr = ep.quic_mapped_addr.0;
            }
        }

        // Normalize local_ip
        meta.dst_ip = self.normalized_local_addr().ok().map(|addr| addr.ip());

        // ep.noteRecvActivity();
        // if stats := c.stats.Load(); stats != nil {
        //     stats.UpdateRxPhysical(ep.nodeAddr, ipp, len(b));
        // }

        debug!("received passthrough message {}", bytes.len());

        true
    }

    fn normalized_local_addr(&self) -> io::Result<SocketAddr> {
        let (v4, v6) = self.local_addr();
        if let Some(v6) = v6 {
            return v6;
        }
        v4
    }

    fn local_addr(&self) -> (io::Result<SocketAddr>, Option<io::Result<SocketAddr>>) {
        // TODO: think more about this
        // needs to pretend ipv6 always as the fake addrs are ipv6
        let mut ipv6_addr = None;
        if let Some(ref conn) = self.pconn6 {
            ipv6_addr = Some(conn.local_addr());
        }
        let ipv4_addr = self.pconn4.local_addr();

        (ipv4_addr, ipv6_addr)
    }

    async fn on_stun_receive(&self, packet: &[u8], addr: SocketAddr) {
        if self.enable_stun_packets {
            let packet = packet.to_vec();
            self.net_checker.receive_stun_packet(&packet, addr).await;
        }
    }

    async fn process_derp_read_result(&mut self, dm: DerpReadResult) -> Vec<NetworkReadResult> {
        debug!("process_derp_read {} bytes", dm.buf.len());
        if dm.buf.is_empty() {
            return Vec::new();
        }
        let region_id = dm.region_id;

        let ipp = SocketAddr::new(DERP_MAGIC_IP, region_id);

        if self
            .handle_disco_message(&dm.buf, ipp, Some(dm.src.clone()))
            .await
        {
            // Message was internal, do not bubble up.
            debug!("processed internal disco message from {:?}", dm.src);
            return Vec::new();
        }

        let ep_quic_mapped_addr = match self.peer_map.endpoint_for_node_key(&dm.src) {
            Some(ep) => ep.quic_mapped_addr,
            None => {
                info!(
                    "no peer_map state found for {:?} in: {:#?}",
                    dm.src, self.peer_map
                );
                let id = self.peer_map.insert_endpoint(EndpointOptions {
                    conn_sender: self.conn.actor_sender.clone(),
                    conn_public_key: self.conn.public_key.clone(),
                    public_key: Some(dm.src),
                    derp_addr: Some(ipp),
                });
                self.peer_map.set_endpoint_for_ip_port(&ipp, id);
                let ep = self.peer_map.by_id_mut(&id).expect("inserted");
                ep.quic_mapped_addr
            }
        };

        // the derp packet is made up of multiple udp packets, prefixed by a u16 be length prefix
        //
        // split the packet into these parts
        let parts = PacketSplitIter::new(dm.buf);
        // Normalize local_ip
        let dst_ip = self.normalized_local_addr().ok().map(|addr| addr.ip());
        parts
            .map(|part| match part {
                Ok(part) => {
                    let meta = quinn_udp::RecvMeta {
                        len: part.len(),
                        stride: part.len(),
                        addr: ep_quic_mapped_addr.0,
                        dst_ip,
                        ecn: None,
                    };
                    NetworkReadResult::Ok {
                        source: NetworkSource::Derp,
                        bytes: part,
                        meta,
                    }
                }
                Err(e) => NetworkReadResult::Error(e),
            })
            .collect::<Vec<_>>()
    }

    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn clean_stale_derp(&mut self) {
        debug!("cleanup {} derps", self.active_derp.len());
        let now = Instant::now();

        let mut to_close = Vec::new();
        for (i, ad) in &self.active_derp {
            if *i == self.my_derp {
                continue;
            }
            if ad.last_write.duration_since(now) > DERP_INACTIVE_CLEANUP_TIME {
                to_close.push(*i);
            }
        }

        let dirty = !to_close.is_empty();
        debug!(
            "closing {}/{} derps",
            to_close.len(),
            self.active_derp.len()
        );
        for i in to_close {
            self.close_derp(i, "idle").await;
        }
        if dirty {
            self.log_active_derp();
        }
    }

    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn close_all_derp(&mut self, why: &'static str) {
        if self.active_derp.is_empty() {
            return;
        }
        // Need to collect to avoid double borrow
        let regions: Vec<_> = self.active_derp.keys().copied().collect();
        for region in regions {
            self.close_derp(region, why).await;
        }
        self.log_active_derp();
    }

    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn close_derp(&mut self, region_id: u16, why: &'static str) {
        if let Some(ad) = self.active_derp.remove(&region_id) {
            debug!(
                "closing connection to derp-{} ({:?}), age {}s",
                region_id,
                why,
                ad.create_time.elapsed().as_secs()
            );

            let ActiveDerp { c, cancel, .. } = ad;
            c.close().await;
            cancel.cancel();

            inc!(MagicsockMetrics::NumDerpConnsRemoved);
        }
    }

    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn connect(
        &mut self,
        region_id: u16,
        peer: Option<&key::node::PublicKey>,
    ) -> derp::http::Client {
        // See if we have a connection open to that DERP node ID
        // first. If so, might as well use it. (It's a little
        // arbitrary whether we use this one vs. the reverse route
        // below when we have both.)

        if let Some(ad) = self.active_derp.get_mut(&region_id) {
            ad.last_write = Instant::now();
            return ad.c.clone();
        }

        // If we don't have an open connection to the peer's home DERP
        // node, see if we have an open connection to a DERP node
        // where we'd heard from that peer already. For instance,
        // perhaps peer's home is Frankfurt, but they dialed our home DERP
        // node in SF to reach us, so we can reply to them using our
        // SF connection rather than dialing Frankfurt.
        if let Some(peer) = peer {
            if let Some(r) = self.derp_route.get(peer) {
                if let Some(ad) = self.active_derp.get_mut(&r.derp_id) {
                    if ad.c == r.dc {
                        ad.last_write = Instant::now();
                        return ad.c.clone();
                    }
                }
            }
        }

        let why = if let Some(peer) = peer {
            format!("{:?}", peer)
        } else {
            "home-keep-alive".to_string()
        };
        info!("adding connection to derp-{} for {}", region_id, why);

        let my_derp = self.my_derp;

        // Note that derp::http.new_region_client does not dial the server
        // (it doesn't block) so it is safe to do under the state lock.

        let conn1 = self.conn.clone();
        let ipv6_reported = self.ipv6_reported.clone();
        let dc = derp::http::ClientBuilder::new()
            .address_family_selector(move || {
                let ipv6_reported = ipv6_reported.clone();
                Box::pin(async move { ipv6_reported.load(Ordering::Relaxed) })
            })
            .can_ack_pings(true)
            .is_preferred(my_derp == region_id)
            .new_region(self.private_key.clone(), move || {
                let conn = conn1.clone();
                Box::pin(async move {
                    // Warning: it is not legal to acquire
                    // magicsock.Conn.mu from this callback.
                    // It's run from derp::http::Client.connect (via Send, etc)
                    // and the lock ordering rules are that magicsock.Conn.mu
                    // must be acquired before derp::http.Client.mu

                    if conn.is_closing() {
                        // We're closing anyway; return to stop dialing.
                        return None;
                    }

                    // Need to load the derp map without aquiring the lock
                    conn.get_derp_region(usize::from(region_id)).await
                })
            });

        let cancel = CancellationToken::new();
        let ad = ActiveDerp {
            c: dc.clone(),
            cancel: cancel.clone(),
            last_write: Instant::now(),
            create_time: Instant::now(),
        };
        self.active_derp.insert(region_id, ad);

        inc!(MagicsockMetrics::NumDerpConnsAdded);
        self.log_active_derp();

        let d = dc.clone();
        let c = self.conn.clone();
        let msg_sender = self.msg_sender.clone();

        // Needs to be done in a different task, to avoid deadlocking.
        tokio::task::spawn(async move {
            // Make sure we can establish a connection.
            if let Err(err) = d.connect().await {
                // TODO: what to do?
                warn!("failed to connect to derp server: {:?}", err);
            }

            if let Some(ref f) = c.on_derp_active {
                // TODO: spawn
                f();
            }

            let rs = ReaderState::new(region_id, cancel, d);
            msg_sender
                .send_async(ActorMessage::Connected(rs))
                .await
                .unwrap();
        });

        dc
    }

    async fn send_network(&mut self, transmits: Vec<quinn_udp::Transmit>) {
        trace!(
            "sending:\n{}",
            transmits
                .iter()
                .map(|t| format!(
                    "  dest: {}, src: {:?}, content_len: {}\n",
                    QuicMappedAddr(t.destination),
                    t.src_ip,
                    t.contents.len()
                ))
                .collect::<String>()
        );

        if transmits.is_empty() {
            return;
        }
        let current_destination = &transmits[0].destination;
        debug_assert!(
            transmits
                .iter()
                .all(|t| &t.destination == current_destination),
            "mixed destinations"
        );
        let current_destination = QuicMappedAddr(*current_destination);

        match self
            .peer_map
            .endpoint_for_quic_mapped_addr_mut(&current_destination)
        {
            Some(ep) => {
                let public_key = ep.public_key();
                match ep.get_send_addrs().await {
                    Ok((Some(udp_addr), Some(derp_addr))) => {
                        let res = if let Some(public_key) = public_key {
                            let res = self.send_raw(udp_addr, transmits.clone()).await;
                            self.send_derp(
                                derp_addr.port(),
                                public_key,
                                transmits.into_iter().map(|t| t.contents).collect(),
                            )
                            .await;
                            res
                        } else {
                            self.send_raw(udp_addr, transmits).await
                        };
                        if let Err(err) = res {
                            warn!("failed to send UDP: {:?}", err);
                        }
                    }
                    Ok((None, Some(derp_addr))) => {
                        if let Some(public_key) = ep.public_key() {
                            self.send_derp(
                                derp_addr.port(),
                                public_key,
                                transmits.into_iter().map(|t| t.contents).collect(),
                            )
                            .await;
                        } else {
                            warn!("no public key for endpoint available, and only DERP address");
                        }
                    }
                    Ok((Some(udp_addr), None)) => {
                        if let Err(err) = self.send_raw(udp_addr, transmits).await {
                            warn!("failed to send UDP: {:?}", err);
                        }
                    }
                    Ok((None, None)) => {
                        warn!("no UDP or DERP addr")
                    }
                    Err(err) => {
                        warn!(
                            "failed to send messages to {}: {:?}",
                            current_destination, err
                        );
                    }
                }
            }
            None => {
                // TODO: This should not be possible.  We should have errored during the
                // AsyncUdpSocket::poll_send call.
                error!(addr=%current_destination, "no endpoint for mapped address");
            }
        }
    }

    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn send_derp(&mut self, port: u16, peer: key::node::PublicKey, contents: Vec<Bytes>) {
        debug!("sending derp region: {} - {:?}", port, peer);
        let region_id = port;

        if self.derp_map.is_none() {
            warn!("DERP is disabled");
            return;
        }
        if !self
            .derp_map
            .as_ref()
            .unwrap()
            .regions
            .contains_key(&usize::from(region_id))
        {
            warn!("unknown region id {}", region_id);
            return;
        }

        let derp_client = self.connect(region_id, Some(&peer)).await;
        for content in &contents {
            trace!("[DERP] -> {} ({}b) {:?}", region_id, content.len(), peer);
        }

        match derp_client.send(peer.clone(), contents).await {
            Ok(_) => {
                inc!(MagicsockMetrics::SendDerp);
            }
            Err(err) => {
                warn!("derp.send: failed {:?}", err);
                inc!(MagicsockMetrics::SendDerpError);
            }
        }
    }

    /// Removes a DERP route entry previously added by add_derp_peer_route.
    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    fn remove_derp_peer_route(
        &mut self,
        peer: key::node::PublicKey,
        derp_id: u16,
        dc: &derp::http::Client,
    ) {
        if let hash_map::Entry::Occupied(r) = self.derp_route.entry(peer) {
            if r.get().derp_id == derp_id && &r.get().dc == dc {
                r.remove();
            }
        }
    }

    /// Adds a DERP route entry, noting that peer was seen on DERP node `derp_id`, at least on the
    /// connection identified by `dc`.
    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    fn add_derp_peer_route(
        &mut self,
        peer: key::node::PublicKey,
        derp_id: u16,
        dc: derp::http::Client,
    ) {
        self.derp_route.insert(peer, DerpRoute { derp_id, dc });
    }

    /// Called in response to a rebind, closes all DERP connections that don't have a local address in okay_local_ips
    /// and pings all those that do.
    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn maybe_close_derps_on_rebind(&mut self, okay_local_ips: &[IpAddr]) {
        let mut tasks = Vec::new();
        for (region_id, ad) in &self.active_derp {
            let la = match ad.c.local_addr().await {
                None => {
                    tasks.push((*region_id, "rebind-no-localaddr"));
                    continue;
                }
                Some(la) => la,
            };

            if !okay_local_ips.contains(&la.ip()) {
                tasks.push((*region_id, "rebind-default-route-change"));
                continue;
            }

            let dc = ad.c.clone();
            let region_id = *region_id;
            let msg_sender = self.msg_sender.clone();
            tokio::task::spawn(time::timeout(Duration::from_secs(3), async move {
                if let Err(_err) = dc.ping().await {
                    msg_sender
                        .send_async(ActorMessage::CloseOrReconnect(
                            region_id,
                            "rebind-ping-fail",
                        ))
                        .await
                        .unwrap();
                    return;
                }
                debug!("post-rebind ping of DERP region {} okay", region_id);
            }));
        }
        for (region_id, why) in tasks {
            self.close_or_reconnect_derp(region_id, why).await;
        }

        self.log_active_derp();
    }

    /// Closes the DERP connection to the provided `region_id` and starts reconnecting it if it's
    /// our current home DERP.
    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn close_or_reconnect_derp(&mut self, region_id: u16, why: &'static str) {
        self.close_derp(region_id, why).await;
        if self.my_derp == region_id {
            self.connect(region_id, None).await;
        }
    }

    /// Triggers an address discovery. The provided why string is for debug logging only.
    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn re_stun(&mut self, why: &'static str) {
        inc!(MagicsockMetrics::ReStunCalls);

        if self.endpoints_update_state.is_running() {
            if Some(why) != self.endpoints_update_state.want_update {
                debug!(
                    "re_stun({:?}): endpoint update active, need another later: {:?}",
                    self.endpoints_update_state.want_update, why
                );
                self.endpoints_update_state.want_update.replace(why);
            }
        } else {
            debug!("re_stun({}): started", why);
            self.endpoints_update_state
                .running
                .send(Some(why))
                .expect("update state not to go away");
        }
    }

    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn update_endpoints(&mut self, why: &'static str) {
        inc!(MagicsockMetrics::UpdateEndpoints);

        debug!("starting endpoint update ({})", why);
        if self.no_v4_send && !self.conn.is_closed() {
            warn!(
                "last netcheck reported send error. Rebinding. (no_v4_send: {} conn closed: {})",
                self.no_v4_send,
                self.conn.is_closed()
            );
            self.rebind_all().await;
        }

        match self.determine_endpoints().await {
            Ok(endpoints) => {
                if self.set_endpoints(&endpoints).await {
                    log_endpoint_change(&endpoints);
                    if let Some(ref cb) = self.conn.on_endpoints {
                        cb(&endpoints[..]);
                    }
                }
            }
            Err(err) => {
                info!("endpoint update ({}) failed: {:#?}", why, err);
                // TODO(crawshaw): are there any conditions under which
                // we should trigger a retry based on the error here?
            }
        }

        let new_why = self.endpoints_update_state.want_update.take();
        if !self.conn.is_closed() {
            if let Some(new_why) = new_why {
                debug!("endpoint update: needed new ({})", new_why);
                self.endpoints_update_state
                    .running
                    .send(Some(new_why))
                    .expect("sender not go away");
                return;
            }
            self.periodic_re_stun_timer = new_re_stun_timer();
        }

        self.endpoints_update_state
            .running
            .send(None)
            .expect("sender not go away");

        debug!("endpoint update done ({})", why);
    }

    /// Returns the machine's endpoint addresses. It does a STUN lookup (via netcheck)
    /// to determine its public address.
    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn determine_endpoints(&mut self) -> Result<Vec<cfg::Endpoint>> {
        let mut portmap_ext = self
            .port_mapper
            .get_cached_mapping_or_start_creating_one()
            .await;
        let nr = self.update_net_info().await.context("update_net_info")?;

        // endpoint -> how it was found
        let mut already = HashMap::new();
        // unique endpoints
        let mut eps = Vec::new();

        macro_rules! add_addr {
            ($already:expr, $eps:expr, $ipp:expr, $et:expr) => {
                if !$already.contains_key(&$ipp) {
                    $already.insert($ipp, $et);
                    $eps.push(cfg::Endpoint {
                        addr: $ipp,
                        typ: $et,
                    });
                }
            };
        }

        // If we didn't have a portmap earlier, maybe it's done by now.
        if portmap_ext.is_none() {
            portmap_ext = self
                .port_mapper
                .get_cached_mapping_or_start_creating_one()
                .await;
        }
        if let Some(portmap_ext) = portmap_ext {
            add_addr!(already, eps, portmap_ext, cfg::EndpointType::Portmapped);
            self.set_net_info_have_port_map().await;
        }

        if let Some(global_v4) = nr.global_v4 {
            add_addr!(already, eps, global_v4, cfg::EndpointType::Stun);

            // If they're behind a hard NAT and are using a fixed
            // port locally, assume they might've added a static
            // port mapping on their router to the same explicit
            // port that we are running with. Worst case it's an invalid candidate mapping.
            let port = self.conn.port.load(Ordering::Relaxed);
            if nr.mapping_varies_by_dest_ip.unwrap_or_default() && port != 0 {
                let mut addr = global_v4;
                addr.set_port(port);
                add_addr!(already, eps, addr, cfg::EndpointType::Stun4LocalPort);
            }
        }
        if let Some(global_v6) = nr.global_v6 {
            add_addr!(already, eps, global_v6, cfg::EndpointType::Stun);
        }

        self.ignore_stun_packets().await;

        let local_addr_v4 = self.pconn4.local_addr().ok();
        let local_addr_v6 = self.pconn6.as_ref().and_then(|c| c.local_addr().ok());

        let is_unspecified_v4 = local_addr_v4
            .map(|a| a.ip().is_unspecified())
            .unwrap_or(false);
        let is_unspecified_v6 = local_addr_v6
            .map(|a| a.ip().is_unspecified())
            .unwrap_or(false);

        let LocalAddresses {
            regular: mut ips,
            loopback,
        } = LocalAddresses::new();

        if is_unspecified_v4 || is_unspecified_v6 {
            if ips.is_empty() && eps.is_empty() {
                // Only include loopback addresses if we have no
                // interfaces at all to use as endpoints and don't
                // have a public IPv4 or IPv6 address. This allows
                // for localhost testing when you're on a plane and
                // offline, for example.
                ips = loopback;
            }
            let v4_port = local_addr_v4.and_then(|addr| {
                if addr.ip().is_unspecified() {
                    Some(addr.port())
                } else {
                    None
                }
            });

            let v6_port = local_addr_v6.and_then(|addr| {
                if addr.ip().is_unspecified() {
                    Some(addr.port())
                } else {
                    None
                }
            });

            for ip in ips {
                match ip {
                    IpAddr::V4(_) => {
                        if let Some(port) = v4_port {
                            add_addr!(
                                already,
                                eps,
                                SocketAddr::new(ip, port),
                                cfg::EndpointType::Local
                            );
                        }
                    }
                    IpAddr::V6(_) => {
                        if let Some(port) = v6_port {
                            add_addr!(
                                already,
                                eps,
                                SocketAddr::new(ip, port),
                                cfg::EndpointType::Local
                            );
                        }
                    }
                }
            }
        }

        if !is_unspecified_v4 {
            // Our local endpoint is bound to a particular address.
            // Do not offer addresses on other local interfaces.
            add_addr!(
                already,
                eps,
                local_addr_v4.unwrap(),
                cfg::EndpointType::Local
            );
        }

        if !is_unspecified_v6 {
            // Our local endpoint is bound to a particular address.
            // Do not offer addresses on other local interfaces.
            add_addr!(
                already,
                eps,
                local_addr_v6.unwrap(),
                cfg::EndpointType::Local
            );
        }

        // Note: the endpoints are intentionally returned in priority order,
        // from "farthest but most reliable" to "closest but least
        // reliable." Addresses returned from STUN should be globally
        // addressable, but might go farther on the network than necessary.
        // Local interface addresses might have lower latency, but not be
        // globally addressable.
        //
        // The STUN address(es) are always first.
        // Despite this sorting, clients are not relying on this sorting for decisions;

        Ok(eps)
    }

    /// Updates `NetInfo.HavePortMap` to true.
    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn set_net_info_have_port_map(&mut self) {
        if let Some(ref mut net_info_last) = self.net_info_last {
            if net_info_last.have_port_map {
                // No change.
                return;
            }
            net_info_last.have_port_map = true;
            let net_info = net_info_last.clone();
            self.call_net_info_callback_locked(net_info);
        }
    }

    /// Calls the NetInfo callback (if previously
    /// registered with SetNetInfoCallback) if ni has substantially changed
    /// since the last state.
    ///
    /// callNetInfoCallback takes ownership of ni.
    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn call_net_info_callback(&mut self, ni: cfg::NetInfo) {
        if let Some(ref net_info_last) = self.net_info_last {
            if ni.basically_equal(net_info_last) {
                return;
            }
        }

        self.call_net_info_callback_locked(ni);
    }

    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    fn call_net_info_callback_locked(&mut self, ni: cfg::NetInfo) {
        self.net_info_last = Some(ni.clone());
        if let Some(ref on_net_info) = self.conn.on_net_info {
            debug!("net_info update: {:?}", ni);
            on_net_info(ni);
        }
    }

    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn update_net_info(&mut self) -> Result<Arc<netcheck::Report>> {
        if self.derp_map.is_none() {
            debug!("skipping netcheck, no Derp Map");
            return Ok(Default::default());
        }

        self.enable_stun_packets = true;
        let dm = self.derp_map.as_ref().unwrap();
        let net_checker = &mut self.net_checker;
        let pconn4 = Some(self.pconn4.as_socket());
        let pconn6 = self.pconn6.as_ref().map(|p| p.as_socket());

        let report = time::timeout(Duration::from_secs(10), async move {
            net_checker.get_report(dm, pconn4, pconn6).await
        })
        .await??;
        self.ipv6_reported.store(report.ipv6, Ordering::Relaxed);
        let r = &report;
        debug!(
            "setting no_v4_send {} -> {}",
            self.no_v4_send, !r.ipv4_can_send
        );
        self.no_v4_send = !r.ipv4_can_send;

        let mut ni = cfg::NetInfo {
            derp_latency: Default::default(),
            mapping_varies_by_dest_ip: r.mapping_varies_by_dest_ip,
            hair_pinning: r.hair_pinning,
            upnp: r.upnp,
            pmp: r.pmp,
            pcp: r.pcp,
            have_port_map: self.port_mapper.have_mapping(),
            working_ipv6: Some(r.ipv6),
            os_has_ipv6: Some(r.os_has_ipv6),
            working_udp: Some(r.udp),
            working_icm_pv4: Some(r.icmpv4),
            preferred_derp: r.preferred_derp,
            link_type: None,
        };
        for (rid, d) in &r.region_v4_latency {
            ni.derp_latency
                .insert(format!("{}-v4", rid), d.as_secs_f64());
        }
        for (rid, d) in &r.region_v6_latency {
            ni.derp_latency
                .insert(format!("{}-v6", rid), d.as_secs_f64());
        }

        if ni.preferred_derp == 0 {
            // Perhaps UDP is blocked. Pick a deterministic but arbitrary one.
            ni.preferred_derp = self.pick_derp_fallback().await;
        }
        if !self.set_nearest_derp(ni.preferred_derp.try_into()?).await {
            ni.preferred_derp = 0;
        }

        // TODO: set link type
        self.call_net_info_callback(ni).await;
        self.ignore_stun_packets().await;

        Ok(report)
    }

    /// Returns a non-zero but deterministic DERP node to
    /// connect to. This is only used if netcheck couldn't find the nearest one
    /// For instance, if UDP is blocked and thus STUN latency checks aren't working
    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn pick_derp_fallback(&self) -> usize {
        if self.derp_map.is_none() {
            return 0;
        }
        let ids = self
            .derp_map
            .as_ref()
            .map(|d| d.region_ids())
            .unwrap_or_default();
        if ids.is_empty() {
            // No DERP regions in map.
            return 0;
        }

        // TODO: figure out which DERP region most of our peers are using,
        // and use that region as our fallback.
        //
        // If we already had selected something in the past and it has any
        // peers, we want to stay on it. If there are no peers at all,
        // stay on whatever DERP we previously picked. If we need to pick
        // one and have no peer info, pick a region randomly.
        //
        // We used to do the above for legacy clients, but never updated it for disco.

        let my_derp = self.my_derp;
        if my_derp > 0 {
            return my_derp.into();
        }

        let mut rng = rand::rngs::StdRng::seed_from_u64(0);
        *ids.choose(&mut rng).unwrap()
    }

    /// Sets a STUN packet processing func that does nothing.
    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn ignore_stun_packets(&mut self) {
        self.enable_stun_packets = false;
    }

    /// Records the new endpoints, reporting whether they're changed.
    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn set_endpoints(&mut self, endpoints: &[cfg::Endpoint]) -> bool {
        self.last_endpoints_time = Some(Instant::now());
        for (_de, f) in self.on_endpoint_refreshed.drain() {
            tokio::task::spawn(async move {
                f();
            });
        }

        if endpoint_sets_equal(endpoints, &self.last_endpoints) {
            return false;
        }
        self.last_endpoints.clear();
        self.last_endpoints.extend_from_slice(endpoints);

        true
    }

    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn enqueue_call_me_maybe(&mut self, derp_addr: SocketAddr, endpoint_id: usize) {
        let endpoint = self.peer_map.by_id(&endpoint_id);
        if endpoint.is_none() {
            warn!(
                "enqueue_call_me_maybe with invalid endpoint_id called: {} - {}",
                derp_addr, endpoint_id
            );
            return;
        }
        let endpoint = endpoint.unwrap();
        if self.last_endpoints_time.is_none()
            || self.last_endpoints_time.as_ref().unwrap().elapsed()
                > ENDPOINTS_FRESH_ENOUGH_DURATION
        {
            info!(
                "want call-me-maybe but endpoints stale; restunning ({:?})",
                self.last_endpoints_time
            );

            let msg_sender = self.msg_sender.clone();
            self.on_endpoint_refreshed.insert(
                endpoint_id,
                Box::new(move || {
                    let msg_sender = msg_sender.clone();
                    Box::pin(async move {
                        info!("STUN done; sending call-me-maybe",);
                        msg_sender
                            .send_async(ActorMessage::EnqueueCallMeMaybe {
                                derp_addr,
                                endpoint_id,
                            })
                            .await
                            .unwrap();
                    })
                }),
            );

            self.msg_sender
                .send_async(ActorMessage::ReStun("refresh-for-peering"))
                .await
                .unwrap();
        } else if let Some(public_key) = endpoint.public_key() {
            let eps: Vec<_> = self.last_endpoints.iter().map(|ep| ep.addr).collect();
            let msg = disco::Message::CallMeMaybe(disco::CallMeMaybe { my_number: eps });

            let msg_sender = self.msg_sender.clone();
            tokio::task::spawn(async move {
                if let Err(err) = msg_sender
                    .send_async(ActorMessage::SendDiscoMessage {
                        dst: derp_addr,
                        dst_key: public_key,
                        msg,
                    })
                    .await
                {
                    warn!("failed to send disco message to {}: {:?}", derp_addr, err);
                }
            });
        }
    }

    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn rebind_all(&mut self) {
        inc!(MagicsockMetrics::RebindCalls);
        if let Err(err) = self.rebind(CurrentPortFate::Keep).await {
            debug!("{:?}", err);
            return;
        }

        let ifs = Default::default(); // TODO: load actual interfaces from the monitor
        self.maybe_close_derps_on_rebind(ifs).await;
        self.reset_endpoint_states();
    }

    /// Resets the preferred address for all peers.
    /// This is called when connectivity changes enough that we no longer trust the old routes.
    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    fn reset_endpoint_states(&mut self) {
        for (_, ep) in self.peer_map.endpoints_mut() {
            ep.note_connectivity_change();
        }
    }

    /// Closes and re-binds the UDP sockets.
    /// We consider it successful if we manage to bind the IPv4 socket.
    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn rebind(&mut self, cur_port_fate: CurrentPortFate) -> Result<()> {
        if let Some(ref mut conn) = self.pconn6 {
            let port = conn.port();
            trace!("IPv6 rebind {} {:?}", port, cur_port_fate);
            // If we were not able to bind ipv6 at program start, dont retry
            if let Err(err) = conn.rebind(port, Network::Ipv6, cur_port_fate).await {
                info!("rebind ignoring IPv6 bind failure: {:?}", err);
            }
        }

        let port = self.local_port_v4();
        self.pconn4
            .rebind(port, Network::Ipv4, cur_port_fate)
            .await
            .context("rebind IPv4 failed")?;

        // reread, as it might have changed
        let port = self.local_port_v4();
        self.port_mapper.set_local_port(port).await;

        Ok(())
    }

    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    pub async fn set_preferred_port(&mut self, port: u16) {
        let existing_port = self.conn.port.swap(port, Ordering::Relaxed);
        if existing_port == port {
            return;
        }

        if let Err(err) = self.rebind(CurrentPortFate::Drop).await {
            warn!("failed to rebind: {:?}", err);
            return;
        }
        self.reset_endpoint_states();
    }

    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn set_nearest_derp(&mut self, derp_num: u16) -> bool {
        if self.derp_map.is_none() {
            self.my_derp = 0;
            return false;
        }
        let my_derp = self.my_derp;
        if derp_num == my_derp {
            // No change.
            return true;
        }
        if my_derp != 0 && derp_num != 0 {
            inc!(MagicsockMetrics::DerpHomeChange);
        }
        self.my_derp = derp_num;

        // On change, notify all currently connected DERP servers and
        // start connecting to our home DERP if we are not already.
        match self
            .derp_map
            .as_ref()
            .expect("already checked")
            .regions
            .get(&usize::from(derp_num))
        {
            Some(dr) => {
                info!("home is now derp-{} ({})", derp_num, dr.region_code);
            }
            None => {
                warn!("derp_map.regions[{}] is empty", derp_num);
            }
        }

        let my_derp = self.my_derp;
        futures::future::join_all(self.active_derp.iter().map(|(i, ad)| async move {
            let b = *i == my_derp;
            ad.c.note_preferred(b).await;
        }))
        .await;

        self.connect(derp_num, None).await;
        true
    }

    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn send_disco_message(
        &mut self,
        dst: SocketAddr,
        dst_key: key::node::PublicKey,
        msg: disco::Message,
    ) -> Result<bool> {
        debug!("sending disco message to {}: {:?}", dst, msg);
        if self.conn.is_closed() {
            bail!("connection closed");
        }
        let di = get_disco_info(&mut self.disco_info, &self.private_key, &dst_key);
        let seal = di.shared_key.seal(&msg.as_bytes());

        let is_derp = dst.ip() == DERP_MAGIC_IP;
        if is_derp {
            inc!(MagicsockMetrics::SendDiscoDerp);
        } else {
            inc!(MagicsockMetrics::SendDiscoUdp);
        }

        let pkt = disco::encode_message(&self.conn.public_key, seal);
        let sent = self.send_addr(dst, Some(&dst_key), pkt.into()).await;
        match sent {
            Ok(0) => {
                // Can't send. (e.g. no IPv6 locally)
                warn!("disco: failed to send {:?} to {}", msg, dst);
                Ok(false)
            }
            Ok(_n) => {
                debug!("disco: sent message to {}", dst);
                if is_derp {
                    inc!(MagicsockMetrics::SentDiscoDerp);
                } else {
                    inc!(MagicsockMetrics::SentDiscoUdp);
                }
                match msg {
                    disco::Message::Ping(_) => {
                        inc!(MagicsockMetrics::SentDiscoPing);
                    }
                    disco::Message::Pong(_) => {
                        inc!(MagicsockMetrics::SentDiscoPong);
                    }
                    disco::Message::CallMeMaybe(_) => {
                        inc!(MagicsockMetrics::SentDiscoCallMeMaybe);
                    }
                }
                Ok(true)
            }
            Err(err) => {
                warn!("disco: failed to send {:?} to {}: {:?}", msg, dst, err);
                Err(err.into())
            }
        }
    }

    /// Sends either to UDP or DERP, depending on the IP.
    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn send_addr(
        &mut self,
        addr: SocketAddr,
        pub_key: Option<&key::node::PublicKey>,
        pkt: Bytes,
    ) -> io::Result<usize> {
        if addr.ip() != DERP_MAGIC_IP {
            let transmits = vec![quinn_udp::Transmit {
                destination: addr,
                contents: pkt,
                ecn: None,
                segment_size: None,
                src_ip: None, // TODO
            }];
            return self.send_raw(addr, transmits).await;
        }

        match pub_key {
            None => Err(io::Error::new(
                io::ErrorKind::Other,
                "missing pub key for derp route",
            )),
            Some(pub_key) => {
                self.send_derp(addr.port(), pub_key.clone(), vec![pkt])
                    .await;
                Ok(1)
            }
        }
    }

    /// Handles a discovery message and reports whether `msg`f was a Tailscale inter-node discovery message.
    ///
    /// A discovery message has the form:
    ///
    ///   - magic             [6]byte
    ///   - senderDiscoPubKey [32]byte
    ///   - nonce             [24]byte
    ///   - naclbox of payload (see disco package for inner payload format)
    ///
    /// For messages received over DERP, the src.ip() will be DERP_MAGIC_IP (with src.port() being the region ID) and the
    /// derp_node_src will be the node key it was received from at the DERP layer. derp_node_src is None when received over UDP.
    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn handle_disco_message(
        &mut self,
        msg: &[u8],
        src: SocketAddr,
        derp_node_src: Option<key::node::PublicKey>,
    ) -> bool {
        debug!("handle_disco_message start {} - {:?}", src, derp_node_src);
        let source = disco::source_and_box(msg);
        if source.is_none() {
            return false;
        }

        let (source, sealed_box) = source.unwrap();

        if self.conn.is_closed() {
            return true;
        }

        let sender = key::node::PublicKey::from(source);
        if self.peer_map.endpoint_for_node_key(&sender).is_none() {
            let mut abort = true;
            // Disco Ping from seen endpoint without node key
            if let Some(ep) = self.peer_map.endpoint_for_ip_port_mut(&src) {
                if ep.public_key().is_none() {
                    debug!("disco: inserting {:?} - {}", sender, src);
                    ep.set_public_key(sender.clone());
                    let id = ep.id;
                    self.peer_map.store_node_key_mapping(id, sender.clone());
                    abort = false;
                }
            }
            if abort {
                inc!(MagicsockMetrics::RecvDiscoBadPeer);
                debug!(
                    "disco: ignoring disco-looking frame, don't know endpoint for {:?}",
                    sender
                );
                return true;
            }
        }

        // We're now reasonably sure we're expecting communication from
        // this peer, do the heavy crypto lifting to see what they want.

        let di = get_disco_info(&mut self.disco_info, &self.private_key, &sender);
        let payload = di.shared_key.open(sealed_box);
        if payload.is_err() {
            // This might be have been intended for a previous
            // disco key.  When we restart we get a new disco key
            // and old packets might've still been in flight (or
            // scheduled). This is particularly the case for LANs
            // or non-NATed endpoints.
            // Don't log in normal case. Pass on to wireguard, in case
            // it's actually a wireguard packet (super unlikely, but).
            debug!(
                "disco: [{:?}] failed to open box from {:?} (wrong rcpt?) {:?}",
                self.conn.public_key, sender, payload,
            );
            inc!(MagicsockMetrics::RecvDiscoBadKey);
            return true;
        }
        let payload = payload.unwrap();
        let dm = disco::Message::from_bytes(&payload);
        debug!("disco: disco.parse = {:?}", dm);

        if dm.is_err() {
            // Couldn't parse it, but it was inside a correctly
            // signed box, so just ignore it, assuming it's from a
            // newer version of Tailscale that we don't
            // understand. Not even worth logging about, lest it
            // be too spammy for old clients.

            inc!(MagicsockMetrics::RecvDiscoBadParse);
            return true;
        }

        let dm = dm.unwrap();
        let is_derp = src.ip() == DERP_MAGIC_IP;
        if is_derp {
            inc!(MagicsockMetrics::RecvDiscoDerp);
        } else {
            inc!(MagicsockMetrics::RecvDiscoUdp);
        }

        debug!("got disco message: {:?}", dm);
        match dm {
            disco::Message::Ping(ping) => {
                inc!(MagicsockMetrics::RecvDiscoPing);
                self.handle_ping(ping, &sender, src, derp_node_src).await;
                true
            }
            disco::Message::Pong(pong) => {
                inc!(MagicsockMetrics::RecvDiscoPong);
                // There might be multiple nodes for the sender's DiscoKey.
                // Ask each to handle it, stopping once one reports that
                // the Pong's TxID was theirs.
                if let Some(ep) = self.peer_map.endpoint_for_node_key_mut(&sender) {
                    let (_, insert) = ep
                        .handle_pong_conn(&self.conn.public_key, &pong, di, src)
                        .await;
                    if let Some((src, key)) = insert {
                        self.peer_map.set_node_key_for_ip_port(&src, &key);
                    }
                }
                true
            }
            disco::Message::CallMeMaybe(cm) => {
                inc!(MagicsockMetrics::RecvDiscoCallMeMaybe);
                if !is_derp || derp_node_src.is_none() {
                    // CallMeMaybe messages should only come via DERP.
                    debug!("[unexpected] CallMeMaybe packets should only come via DERP");
                    return true;
                }
                let node_key = derp_node_src.unwrap();
                match self.peer_map.endpoint_for_node_key_mut(&node_key) {
                    None => {
                        inc!(MagicsockMetrics::RecvDiscoCallMeMaybeBadDisco);
                        debug!(
                            "disco: ignoring CallMeMaybe from {:?}; {:?} is unknown",
                            sender, node_key,
                        );
                    }
                    Some(ep) => {
                        info!(
                            "disco: {:?}<-{:?} ({:?})  got call-me-maybe, {} endpoints",
                            self.conn.public_key,
                            ep.public_key(),
                            src,
                            cm.my_number.len()
                        );
                        ep.handle_call_me_maybe(cm).await;
                    }
                }
                true
            }
        }
    }

    /// di is the DiscoInfo of the source of the ping.
    /// derp_node_src is non-zero if the ping arrived via DERP.
    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn handle_ping(
        &mut self,
        dm: disco::Ping,
        sender: &key::node::PublicKey,
        src: SocketAddr,
        derp_node_src: Option<key::node::PublicKey>,
    ) {
        let di = get_disco_info(&mut self.disco_info, &self.private_key, sender);
        let likely_heart_beat = Some(src) == di.last_ping_from
            && di
                .last_ping_time
                .map(|s| s.elapsed() < Duration::from_secs(5))
                .unwrap_or_default();
        di.last_ping_from.replace(src);
        di.last_ping_time.replace(Instant::now());
        let is_derp = src.ip() == DERP_MAGIC_IP;

        // If we can figure out with certainty which node key this disco
        // message is for, eagerly update our IP<>node and disco<>node
        // mappings to make p2p path discovery faster in simple
        // cases. Without this, disco would still work, but would be
        // reliant on DERP call-me-maybe to establish the disco<>node
        // mapping, and on subsequent disco handlePongLocked to establish the IP<>disco mapping.
        let mut unambigous_node_key = None;
        {
            // Attempt to look up an unambiguous mapping from a DiscoKey `dk` (which sent ping dm) to a NodeKey.
            // `None` if not unamabigous.
            if let Some(ref src) = derp_node_src {
                if self.peer_map.endpoint_for_node_key(src).is_some() {
                    unambigous_node_key = Some(src.clone());
                }
            } else if let Some(_ep) = self.peer_map.endpoint_for_node_key(&dm.node_key) {
                unambigous_node_key = Some(dm.node_key.clone());
            }
        }

        if let Some(nk) = unambigous_node_key {
            if !is_derp {
                self.peer_map.set_node_key_for_ip_port(&src, &nk);
            }
        }

        // If we got a ping over DERP, then derp_node_src is non-zero and we reply
        // over DERP (in which case ip_dst is also a DERP address).
        // But if the ping was over UDP (ip_dst is not a DERP address), then dst_key
        // will be zero here, but that's fine: send_disco_message only requires
        // a dstKey if the dst ip:port is DERP.
        if is_derp {
            assert!(derp_node_src.is_some());
        } else {
            assert!(derp_node_src.is_none());
        }
        let mut dst_key = derp_node_src.clone();

        // Remember this route if not present.
        let mut num_nodes = 0;
        let mut dup = false;
        if let Some(ref dst_key) = dst_key {
            if let Some(ep) = self.peer_map.endpoint_for_node_key_mut(dst_key) {
                if ep.add_candidate_endpoint(src, dm.tx_id) {
                    debug!("disco: ping got duplicate endpoint {} - {}", src, dm.tx_id);
                    return;
                }
                num_nodes = 1;
            }
        } else {
            if let Some(ep) = self.peer_map.endpoint_for_node_key_mut(&di.node_key) {
                if ep.add_candidate_endpoint(src, dm.tx_id) {
                    dup = true;
                } else {
                    num_nodes += 1;
                    if num_nodes == 1 && dst_key.is_none() {
                        dst_key.replace(di.node_key.clone());
                    }
                }
            }
            if dup {
                debug!("disco: ping got duplicate endpoint {} - {}", src, dm.tx_id);
                return;
            }
            if num_nodes > 1 {
                // Zero it out if it's ambiguous, so send_disco_message logging isn't confusing.
                dst_key = None;
            }
        }

        if num_nodes == 0 {
            warn!(
                "[unexpected] got disco ping from {:?}/{:?} for node not in peers",
                src, derp_node_src
            );
            return;
        }

        if !likely_heart_beat {
            let ping_node_src_str = if num_nodes > 1 {
                "[one-of-multi]".to_string()
            } else {
                format!("{:?}", dst_key)
            };
            info!(
                "disco: {:?}<-{:?} ({:?}, {:?})  got ping tx={:?}",
                self.conn.public_key, di.node_key, ping_node_src_str, src, dm.tx_id
            );
        }

        let ip_dst = src;
        let pong = disco::Message::Pong(disco::Pong {
            tx_id: dm.tx_id,
            src,
        });
        let dst_key = dst_key.unwrap_or_else(|| di.node_key.clone());
        if let Err(err) = self.send_disco_message(ip_dst, dst_key, pong).await {
            warn!("failed to send disco message to {}: {:?}", ip_dst, err);
        }
    }

    async fn set_derp_map(&mut self, dm: Option<DerpMap>) {
        if self.derp_map == dm {
            return;
        }

        debug!("setting new derp map: {:?}", dm);

        let old = std::mem::replace(&mut self.derp_map, dm);
        if self.derp_map.is_none() {
            self.close_all_derp("derp-disabled").await;
            return;
        }

        // Reconnect any DERP region that changed definitions.
        if let Some(old) = old {
            let mut changes = false;
            for (rid, old_def) in old.regions {
                if let Some(new_def) = self.derp_map.as_ref().unwrap().regions.get(&rid) {
                    if &old_def == new_def {
                        continue;
                    }
                }
                changes = true;
                if u16::try_from(rid).expect("region too large") == self.my_derp {
                    self.my_derp = 0;
                }
                self.close_derp(
                    rid.try_into().expect("region too large"),
                    "derp-region-redefined",
                )
                .await;
            }
            if changes {
                self.log_active_derp();
            }
        }

        self.re_stun("derp-map-update").await;
    }

    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    fn set_network_map(&mut self, nm: netmap::NetworkMap) {
        if self.conn.is_closed() {
            return;
        }

        // Update self.net_map regardless, before the following early return.
        let prior_netmap = self.net_map.replace(nm);

        if prior_netmap.is_some()
            && prior_netmap.as_ref().unwrap().peers == self.net_map.as_ref().unwrap().peers
        {
            // The rest of this function is all adjusting state for peers that have
            // changed. But if the set of peers is equal no need to do anything else.
            return;
        }

        info!(
            "got updated network map; {} peers",
            self.net_map.as_ref().unwrap().peers.len()
        );

        // Try a pass of just inserting missing endpoints. If the set of nodes is the same,
        // this is an efficient alloc-free update. If the set of nodes is different, we'll
        // remove moribund nodes in the next step below.
        for n in &self.net_map.as_ref().unwrap().peers {
            if self.peer_map.endpoint_for_node_key(&n.key).is_none() {
                info!(
                    "inserting endpoint {:?} - {:?} {:#?} {:#?}",
                    self.conn.public_key,
                    n.key.clone(),
                    self.peer_map,
                    n,
                );
                self.peer_map.insert_endpoint(EndpointOptions {
                    conn_sender: self.conn.actor_sender.clone(),
                    conn_public_key: self.conn.public_key.clone(),
                    public_key: Some(n.key.clone()),
                    derp_addr: n.derp,
                });
            }

            if let Some(ep) = self.peer_map.endpoint_for_node_key_mut(&n.key) {
                ep.update_from_node(n);
                let id = ep.id;
                for endpoint in &n.endpoints {
                    self.peer_map.set_endpoint_for_ip_port(endpoint, id);
                }
            }
        }

        // If the set of nodes changed since the last set_network_map, the
        // insert loop just above made self.peer_map contain the union of the
        // old and new peers - which will be larger than the set from the
        // current netmap. If that happens, go through the allocful
        // deletion path to clean up moribund nodes.
        if self.peer_map.node_count() != self.net_map.as_ref().unwrap().peers.len() {
            let keep: HashSet<_> = self
                .net_map
                .as_ref()
                .unwrap()
                .peers
                .iter()
                .map(|n| n.key.clone())
                .collect();

            let mut to_delete = Vec::new();
            for (id, ep) in self.peer_map.endpoints() {
                if let Some(ref public_key) = ep.public_key() {
                    if !keep.contains(public_key) {
                        to_delete.push(*id);
                    }
                }
            }

            for id in to_delete {
                self.peer_map.delete_endpoint(id);
            }
        }
    }

    /// Returns the current IPv4 listener's port number.
    fn local_port_v4(&self) -> u16 {
        self.pconn4.port()
    }

    #[instrument(skip_all, fields(self.name = %self.conn.name))]
    async fn send_raw(
        &self,
        addr: SocketAddr,
        mut transmits: Vec<quinn_udp::Transmit>,
    ) -> io::Result<usize> {
        debug!("send_raw: {} packets", transmits.len());

        if addr.is_ipv6() && self.pconn6.is_none() {
            return Err(io::Error::new(io::ErrorKind::Other, "no IPv6 connection"));
        }

        let conn = if addr.is_ipv6() {
            self.pconn6.as_ref().unwrap()
        } else {
            &self.pconn4
        };

        if transmits.iter().any(|t| t.destination != addr) {
            for t in &mut transmits {
                t.destination = addr;
            }
        }
        let sum =
            futures::future::poll_fn(|cx| conn.poll_send(&self.udp_state, cx, &transmits)).await?;

        debug!("sent {} packets to {}", sum, addr);
        debug_assert!(
            sum <= transmits.len(),
            "too many msgs {} > {}",
            sum,
            transmits.len()
        );

        Ok(sum)
    }

    fn log_active_derp(&self) {
        let now = Instant::now();
        debug!("{} active derp conns{}", self.active_derp.len(), {
            let mut s = String::new();
            if !self.active_derp.is_empty() {
                s += ":";
                for (node, ad) in self.active_derp_sorted() {
                    s += &format!(
                        " derp-{}=cr{},wr{}",
                        node,
                        now.duration_since(ad.create_time).as_secs(),
                        now.duration_since(ad.last_write).as_secs()
                    );
                }
            }
            s
        });
    }

    fn active_derp_sorted(&self) -> impl Iterator<Item = (u16, &'_ ActiveDerp)> + '_ {
        let mut ids: Vec<_> = self.active_derp.keys().copied().collect();
        ids.sort();

        ids.into_iter()
            .map(|id| (id, self.active_derp.get(&id).unwrap()))
    }
}

struct IpStream {
    conn: Arc<Inner>,
    pconn4: RebindingUdpConn,
    pconn6: Option<RebindingUdpConn>,
    recv_buf: Box<[u8]>,
    out_buffer: VecDeque<(Bytes, Network, quinn_udp::RecvMeta)>,
}

impl IpStream {
    fn new(
        udp_state: &quinn_udp::UdpState,
        conn: Arc<Inner>,
        pconn4: RebindingUdpConn,
        pconn6: Option<RebindingUdpConn>,
    ) -> Self {
        // 1480 MTU size based on default from quinn
        let target_recv_buf_len = 1480 * udp_state.gro_segments() * quinn_udp::BATCH_SIZE;
        let recv_buf = vec![0u8; target_recv_buf_len];

        Self {
            recv_buf: recv_buf.into(),
            conn,
            pconn4,
            pconn6,
            out_buffer: VecDeque::new(),
        }
    }
}

impl Stream for IpStream {
    type Item = io::Result<(Bytes, Network, quinn_udp::RecvMeta)>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.conn.is_closed() {
            return Poll::Ready(None);
        }
        if let Some(res) = self.out_buffer.pop_front() {
            return Poll::Ready(Some(Ok(res)));
        }

        let mut metas = [quinn_udp::RecvMeta::default(); quinn_udp::BATCH_SIZE];
        let mut iovs = MaybeUninit::<[IoSliceMut; quinn_udp::BATCH_SIZE]>::uninit();
        let chunk_size = self.recv_buf.len() / quinn_udp::BATCH_SIZE;
        self.recv_buf
            .chunks_mut(chunk_size)
            .enumerate()
            .for_each(|(i, buf)| unsafe {
                iovs.as_mut_ptr()
                    .cast::<IoSliceMut>()
                    .add(i)
                    .write(IoSliceMut::new(buf));
            });
        let mut iovs = unsafe { iovs.assume_init() };

        if let Some(ref pconn6) = self.pconn6 {
            match pconn6.poll_recv(cx, &mut iovs, &mut metas) {
                Poll::Pending => {}
                Poll::Ready(Ok(msgs)) => {
                    for (mut meta, buf) in metas.into_iter().zip(iovs.iter()).take(msgs) {
                        let mut data: BytesMut = buf[0..meta.len].into();
                        let stride = meta.stride;
                        while !data.is_empty() {
                            let buf = data.split_to(stride.min(data.len())).freeze();
                            // set stride to len, as we are cutting it into pieces here
                            meta.len = buf.len();
                            meta.stride = buf.len();
                            self.out_buffer.push_back((buf, Network::Ipv6, meta));
                        }
                    }
                    if let Some(res) = self.out_buffer.pop_front() {
                        return Poll::Ready(Some(Ok(res)));
                    }
                }
                Poll::Ready(Err(err)) => {
                    return Poll::Ready(Some(Err(err)));
                }
            }
        }

        match self.pconn4.poll_recv(cx, &mut iovs, &mut metas) {
            Poll::Pending => {}
            Poll::Ready(Ok(msgs)) => {
                for (mut meta, buf) in metas.into_iter().zip(iovs.iter()).take(msgs) {
                    let mut data: BytesMut = buf[0..meta.len].into();
                    let stride = meta.stride;
                    while !data.is_empty() {
                        let buf = data.split_to(stride.min(data.len())).freeze();
                        // set stride to len, as we are cutting it into pieces here
                        meta.len = buf.len();
                        meta.stride = buf.len();
                        self.out_buffer.push_back((buf, Network::Ipv6, meta));
                    }
                }
                if let Some(res) = self.out_buffer.pop_front() {
                    return Poll::Ready(Some(Ok(res)));
                }
            }
            Poll::Ready(Err(err)) => {
                return Poll::Ready(Some(Err(err)));
            }
        }

        Poll::Pending
    }
}

/// Returns the previous or new DiscoInfo for `k`.
fn get_disco_info<'a>(
    disco_info: &'a mut HashMap<key::node::PublicKey, DiscoInfo>,
    node_private: &key::node::SecretKey,
    k: &key::node::PublicKey,
) -> &'a mut DiscoInfo {
    if !disco_info.contains_key(k) {
        let shared_key = node_private.shared(k);
        disco_info.insert(
            k.clone(),
            DiscoInfo {
                node_key: k.clone(),
                shared_key,
                last_ping_from: None,
                last_ping_time: None,
            },
        );
    }

    disco_info.get_mut(k).unwrap()
}

fn new_re_stun_timer() -> time::Interval {
    // Pick a random duration between 20 and 26 seconds (just under 30s,
    // a common UDP NAT timeout on Linux,etc)
    let mut rng = rand::thread_rng();
    let d: Duration = rng.gen_range(Duration::from_secs(20)..=Duration::from_secs(26));
    debug!("scheduling periodic_stun to run in {}s", d.as_secs());
    time::interval_at(time::Instant::now() + d, d)
}

/// Initial connection setup.
async fn bind(port: u16) -> Result<(RebindingUdpConn, Option<RebindingUdpConn>)> {
    let pconn6 = match RebindingUdpConn::bind(port, Network::Ipv6).await {
        Ok(conn) => Some(conn),
        Err(err) => {
            info!("rebind ignoring IPv6 bind failure: {:?}", err);
            None
        }
    };

    let pconn4 = RebindingUdpConn::bind(port, Network::Ipv4)
        .await
        .context("rebind IPv4 failed")?;

    Ok((pconn4, pconn6))
}

fn log_endpoint_change(endpoints: &[cfg::Endpoint]) {
    debug!("endpoints changed: {}", {
        let mut s = String::new();
        for (i, ep) in endpoints.iter().enumerate() {
            if i > 0 {
                s += ", ";
            }
            s += &format!("{} ({})", ep.addr, ep.typ);
        }
        s
    });
}

/// Manages reading state for a single derp connection.
#[derive(Debug)]
pub(super) struct ReaderState {
    region: u16,
    derp_client: derp::http::Client,
    /// The set of senders we know are present on this connection, based on
    /// messages we've received from the server.
    peer_present: HashSet<key::node::PublicKey>,
    backoff: backoff::exponential::ExponentialBackoff<backoff::SystemClock>,
    last_packet_time: Option<Instant>,
    last_packet_src: Option<key::node::PublicKey>,
    cancel: CancellationToken,
}

#[derive(Debug)]
enum ReadResult {
    Yield(DerpReadResult),
    Break,
    Continue,
}

#[derive(Debug)]
enum ReadAction {
    None,
    RemovePeerRoute {
        peers: Vec<key::node::PublicKey>,
        region: u16,
        derp_client: derp::http::Client,
    },
    AddPeerRoute {
        peers: Vec<key::node::PublicKey>,
        region: u16,
        derp_client: derp::http::Client,
    },
}

impl ReaderState {
    fn new(region: u16, cancel: CancellationToken, derp_client: derp::http::Client) -> Self {
        ReaderState {
            region,
            derp_client,
            cancel,
            peer_present: HashSet::new(),
            backoff: backoff::exponential::ExponentialBackoffBuilder::new()
                .with_initial_interval(Duration::from_millis(10))
                .with_max_interval(Duration::from_secs(5))
                .build(),
            last_packet_time: None,
            last_packet_src: None,
        }
    }

    async fn recv(mut self) -> (Self, ReadResult, ReadAction) {
        let msg = tokio::select! {
            msg = self.derp_client.recv_detail() => {
                msg
            }
            _ = self.cancel.cancelled() => {
                return (self, ReadResult::Break, ReadAction::None);
            }
        };
        debug!("derp.recv(derp-{}) received: {:?}", self.region, msg);

        match msg {
            Err(err) => {
                debug!(
                    "[{:?}] derp.recv(derp-{}): {:?}",
                    self.derp_client, self.region, err
                );

                // Forget that all these peers have routes.
                let peers = self.peer_present.drain().collect();
                let action = ReadAction::RemovePeerRoute {
                    peers,
                    region: self.region,
                    derp_client: self.derp_client.clone(),
                };

                if matches!(
                    err,
                    derp::http::ClientError::Closed | derp::http::ClientError::IPDisabled
                ) {
                    // drop client
                    return (self, ReadResult::Break, action);
                }

                // If our DERP connection broke, it might be because our network
                // conditions changed. Start that check.
                // TODO:
                // self.re_stun("derp-recv-error").await;

                // Back off a bit before reconnecting.
                match self.backoff.next_backoff() {
                    Some(t) => {
                        debug!("backoff sleep: {}ms", t.as_millis());
                        time::sleep(t).await;
                        (self, ReadResult::Continue, action)
                    }
                    None => (self, ReadResult::Break, action),
                }
            }
            Ok((msg, conn_gen)) => {
                // reset
                self.backoff.reset();
                let now = Instant::now();
                if self.last_packet_time.is_none()
                    || self.last_packet_time.as_ref().unwrap().elapsed() > Duration::from_secs(5)
                {
                    self.last_packet_time = Some(now);
                }

                match msg {
                    derp::ReceivedMessage::ServerInfo { .. } => {
                        info!("derp-{} connected; connGen={}", self.region, conn_gen);
                        (self, ReadResult::Continue, ReadAction::None)
                    }
                    derp::ReceivedMessage::ReceivedPacket { source, data } => {
                        trace!("[DERP] <- {} ({}b)", self.region, data.len());
                        // If this is a new sender we hadn't seen before, remember it and
                        // register a route for this peer.
                        let action = if self.last_packet_src.is_none()
                            || &source != self.last_packet_src.as_ref().unwrap()
                        {
                            // avoid map lookup w/ high throughput single peer
                            self.last_packet_src = Some(source.clone());
                            let mut peers = Vec::new();
                            if !self.peer_present.contains(&source) {
                                self.peer_present.insert(source.clone());
                                peers.push(source.clone());
                            }
                            ReadAction::AddPeerRoute {
                                peers,
                                region: self.region,
                                derp_client: self.derp_client.clone(),
                            }
                        } else {
                            ReadAction::None
                        };

                        let res = DerpReadResult {
                            region_id: self.region,
                            src: source,
                            buf: data,
                        };
                        (self, ReadResult::Yield(res), action)
                    }
                    derp::ReceivedMessage::Ping(data) => {
                        // Best effort reply to the ping.
                        let dc = self.derp_client.clone();
                        tokio::task::spawn(async move {
                            if let Err(err) = dc.send_pong(data).await {
                                info!("derp-{} send_pong error: {:?}", self.region, err);
                            }
                        });
                        (self, ReadResult::Continue, ReadAction::None)
                    }
                    derp::ReceivedMessage::Health { .. } => {
                        // health.SetDERPRegionHealth(regionID, m.Problem);
                        (self, ReadResult::Continue, ReadAction::None)
                    }
                    derp::ReceivedMessage::PeerGone(key) => {
                        let read_action = ReadAction::RemovePeerRoute {
                            peers: vec![key],
                            region: self.region,
                            derp_client: self.derp_client.clone(),
                        };

                        (self, ReadResult::Continue, read_action)
                    }
                    _ => {
                        // Ignore.
                        (self, ReadResult::Continue, ReadAction::None)
                    }
                }
            }
        }
    }
}

/// A simple iterator to group [`Transmit`]s by destination.
struct TransmitIter<'a> {
    transmits: &'a [quinn_udp::Transmit],
    offset: usize,
}

impl<'a> TransmitIter<'a> {
    fn new(transmits: &'a [quinn_udp::Transmit]) -> Self {
        TransmitIter {
            transmits,
            offset: 0,
        }
    }
}

impl Iterator for TransmitIter<'_> {
    type Item = Vec<quinn_udp::Transmit>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset == self.transmits.len() {
            return None;
        }
        let current_dest = &self.transmits[self.offset].destination;
        let mut end = self.offset;
        for t in &self.transmits[self.offset..] {
            if current_dest != &t.destination {
                break;
            }
            end += 1;
        }

        let out = self.transmits[self.offset..end].to_vec();
        self.offset = end;
        Some(out)
    }
}

/// The fake address used by the QUIC layer to address a peer.
///
/// You can consider this as nothing more than a lookup key for a peer the [`Conn`] knows
/// about.
///
/// [`Conn`] can reach a peer by several real socket addresses, or maybe even via the derper
/// relay.  The QUIC layer however needs to address a peer by a stable [`SocketAddr`] so
/// that normal socket APIs can function.  Thus when a new peer is introduced to a [`Conn`]
/// it is given a new fake address.  This is the type of that address.
///
/// It is but a newtype.  And in our QUIC-facing socket APIs like [`AsyncUdpSocket`] it
/// comes in as the inner [`SocketAddr`], in those interfaces we have to be careful to do
/// the conversion to this type.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub(crate) struct QuicMappedAddr(SocketAddr);

/// Counter to always generate unique addresses for [`QuicMappedAddr`].
static ADDR_COUNTER: AtomicU64 = AtomicU64::new(0);

impl QuicMappedAddr {
    /// The Prefix/L of our Unique Local Addresses.
    const ADDR_PREFIXL: u8 = 0xfd;
    /// The Global ID used in our Unique Local Addresses.
    const ADDR_GLOBAL_ID: [u8; 5] = [21, 7, 10, 81, 11];
    /// The Subnet ID used in our Unique Local Addresses.
    const ADDR_SUBNET: [u8; 2] = [0; 2];

    /// Generates a globally unique fake UDP address.
    ///
    /// This generates and IPv6 Unique Local Address according to RFC 4193.
    pub(crate) fn generate() -> Self {
        let mut addr = [0u8; 16];
        addr[0] = Self::ADDR_PREFIXL;
        addr[1..6].copy_from_slice(&Self::ADDR_GLOBAL_ID);
        addr[6..8].copy_from_slice(&Self::ADDR_SUBNET);

        let counter = ADDR_COUNTER.fetch_add(1, Ordering::Relaxed);
        addr[8..16].copy_from_slice(&counter.to_be_bytes());

        Self(SocketAddr::new(IpAddr::V6(Ipv6Addr::from(addr)), 12345))
    }
}

impl std::fmt::Display for QuicMappedAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "QuicMappedAddr({})", self.0)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Context;
    use hyper::server::conn::Http;
    use rand::RngCore;
    use std::net::Ipv4Addr;
    use tokio::{net, sync, task::JoinSet};
    use tracing::{debug_span, Instrument};
    use tracing_subscriber::{prelude::*, EnvFilter};

    use super::*;
    use crate::{
        hp::{
            derp::{DerpNode, DerpRegion, UseIpv4, UseIpv6},
            hostinfo::Hostinfo,
        },
        tls,
    };

    fn make_transmit(destination: SocketAddr) -> quinn_udp::Transmit {
        quinn_udp::Transmit {
            destination,
            ecn: None,
            contents: destination.to_string().into(),
            segment_size: None,
            src_ip: None,
        }
    }

    #[test]
    fn test_transmit_iter() {
        let transmits = vec![
            make_transmit(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1)),
            make_transmit(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 2)),
            make_transmit(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 2)),
            make_transmit(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1)),
            make_transmit(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 3)),
            make_transmit(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 3)),
        ];

        let groups: Vec<_> = TransmitIter::new(&transmits).collect();
        dbg!(&groups);
        assert_eq!(groups.len(), 4);
        assert_eq!(groups[0].len(), 1);
        assert_eq!(groups[1].len(), 2);
        assert_eq!(groups[2].len(), 1);
        assert_eq!(groups[3].len(), 2);

        let transmits = vec![
            make_transmit(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1)),
            make_transmit(SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1)),
        ];

        let groups: Vec<_> = TransmitIter::new(&transmits).collect();
        dbg!(&groups);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 2);
    }

    async fn pick_port() -> u16 {
        let conn = net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        conn.local_addr().unwrap().port()
    }

    /// Returns a new Conn.
    async fn new_test_conn() -> Conn {
        let port = pick_port().await;
        Conn::new(Options {
            port,
            ..Default::default()
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn test_rebind_stress() {
        let c = new_test_conn().await;

        let (cancel, mut cancel_r) = sync::oneshot::channel();

        let conn = c.clone();
        let t = tokio::task::spawn(async move {
            let mut buff = vec![0u8; 1500];
            let mut buffs = [io::IoSliceMut::new(&mut buff)];
            let mut meta = [quinn_udp::RecvMeta::default()];
            loop {
                tokio::select! {
                    _ = &mut cancel_r => {
                        return anyhow::Ok(());
                    }
                    res = futures::future::poll_fn(|cx| conn.poll_recv(cx, &mut buffs, &mut meta)) => {
                        res?;
                    }
                }
            }
        });

        let conn = c.clone();
        let t1 = tokio::task::spawn(async move {
            for _i in 0..2000 {
                conn.rebind_all().await;
            }
        });

        let conn = c.clone();
        let t2 = tokio::task::spawn(async move {
            for _i in 0..2000 {
                conn.rebind_all().await;
            }
        });

        t1.await.unwrap();
        t2.await.unwrap();

        cancel.send(()).unwrap();

        c.close().await.unwrap();
        t.await.unwrap().unwrap();
    }

    struct Devices {
        stun_ip: IpAddr,
    }

    async fn run_derp_and_stun(stun_ip: IpAddr) -> Result<(DerpMap, impl FnOnce())> {
        // TODO: pass a mesh_key?
        let derp_server: derp::Server<
            net::tcp::OwnedReadHalf,
            net::tcp::OwnedWriteHalf,
            derp::http::Client,
        > = derp::Server::new(key::node::SecretKey::generate(), None);

        let http_listener = net::TcpListener::bind("127.0.0.1:0").await?;
        let http_addr = http_listener.local_addr()?;
        println!("DERP listening on {:?}", http_addr);

        let (derp_shutdown, mut rx) = sync::oneshot::channel::<()>();

        // TODO: TLS
        // httpsrv.StartTLS()

        tokio::task::spawn(async move {
            let derp_client_handler = derp_server.client_conn_handler(Default::default());

            loop {
                tokio::select! {
                    biased;
                    _ = &mut rx => {
                        derp_server.close().await;
                        return Ok::<_, anyhow::Error>(());
                    }

                    conn = http_listener.accept() => {
                        trace!("accepted derp connection");
                        let (stream, _) = conn?;
                        let derp_client_handler = derp_client_handler.clone();
                        tokio::task::spawn(async move {
                            if let Err(err) = Http::new()
                                .serve_connection(stream, derp_client_handler)
                                .with_upgrades()
                                .await
                            {
                                eprintln!("Failed to serve connection: {:?}", err);
                            }
                        });
                    }
                }
            }
        });

        let (stun_addr, _, stun_cleanup) = stun::test::serve(stun_ip).await?;
        let m = DerpMap {
            regions: [(
                1,
                DerpRegion {
                    region_id: 1,
                    region_code: "test".into(),
                    nodes: vec![DerpNode {
                        name: "t1".into(),
                        region_id: 1,
                        host_name: "test-node.invalid".into(),
                        stun_only: false,
                        stun_port: stun_addr.port(),
                        ipv4: UseIpv4::Some("127.0.0.1".parse().unwrap()),
                        ipv6: UseIpv6::None,

                        derp_port: http_addr.port(),
                        stun_test_ip: Some(stun_addr.ip()),
                    }],
                    avoid: false,
                },
            )]
            .into_iter()
            .collect(),
        };

        let cleanup = || {
            println!("CLEANUP");
            stun_cleanup.send(()).unwrap();
            derp_shutdown.send(()).unwrap();
        };

        Ok((m, cleanup))
    }

    /// Magicsock plus wrappers for sending packets
    #[derive(Clone)]
    struct MagicStack {
        ep_ch: flume::Receiver<Vec<cfg::Endpoint>>,
        key: key::node::SecretKey,
        conn: Conn,
        quic_ep: quinn::Endpoint,
    }

    impl MagicStack {
        async fn new(derp_map: DerpMap) -> Result<Self> {
            let (on_derp_s, mut on_derp_r) = sync::mpsc::channel(8);
            let (ep_s, ep_r) = flume::bounded(16);
            let opts = Options {
                on_endpoints: Some(Box::new(move |eps: &[cfg::Endpoint]| {
                    let _ = ep_s.send(eps.to_vec());
                })),
                on_derp_active: Some(Box::new(move || {
                    on_derp_s.try_send(()).ok();
                })),
                ..Default::default()
            };
            let key = opts.private_key.clone();
            let conn = Conn::new(opts).await?;
            conn.set_derp_map(Some(derp_map)).await?;

            tokio::time::timeout(Duration::from_secs(10), on_derp_r.recv())
                .await
                .context("wait for derp connection")?;

            let tls_server_config =
                tls::make_server_config(&key.clone().into(), vec![tls::P2P_ALPN.to_vec()], false)?;
            let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(tls_server_config));
            let mut transport_config = quinn::TransportConfig::default();
            transport_config.keep_alive_interval(Some(Duration::from_secs(5)));
            transport_config.max_idle_timeout(Some(Duration::from_secs(10).try_into().unwrap()));
            server_config.transport_config(Arc::new(transport_config));
            let mut quic_ep = quinn::Endpoint::new_with_abstract_socket(
                quinn::EndpointConfig::default(),
                Some(server_config),
                conn.clone(),
                Arc::new(quinn::TokioRuntime),
            )?;

            let tls_client_config = tls::make_client_config(
                &key.clone().into(),
                None,
                vec![tls::P2P_ALPN.to_vec()],
                false,
            )?;
            let mut client_config = quinn::ClientConfig::new(Arc::new(tls_client_config));
            let mut transport_config = quinn::TransportConfig::default();
            transport_config.max_idle_timeout(Some(Duration::from_secs(10).try_into().unwrap()));
            client_config.transport_config(Arc::new(transport_config));
            quic_ep.set_default_client_config(client_config);

            Ok(Self {
                ep_ch: ep_r,
                key,
                conn,
                quic_ep,
            })
        }

        async fn tracked_endpoints(&self) -> Vec<key::node::PublicKey> {
            self.conn.tracked_endpoints().await.unwrap_or_default()
        }

        fn public(&self) -> key::node::PublicKey {
            self.key.public_key()
        }
    }

    /// Monitors endpoint changes and plumbs things together.
    async fn mesh_stacks(stacks: Vec<MagicStack>) -> Result<impl FnOnce()> {
        // Serialize all reconfigurations globally, just to keep things simpler.
        let eps = Arc::new(Mutex::new(vec![Vec::new(); stacks.len()]));

        async fn build_netmap(
            eps: &[Vec<cfg::Endpoint>],
            ms: &[MagicStack],
            my_idx: usize,
        ) -> netmap::NetworkMap {
            let mut peers = Vec::new();

            for (i, peer) in ms.iter().enumerate() {
                if i == my_idx {
                    continue;
                }
                if eps[i].is_empty() {
                    continue;
                }

                let addresses = vec![Ipv4Addr::new(1, 0, 0, (i + 1) as u8).into()];
                peers.push(cfg::Node {
                    addresses: addresses.clone(),
                    name: Some(format!("node{}", i + 1)),
                    key: peer.key.public_key(),
                    endpoints: eps[i].iter().map(|ep| ep.addr).collect(),
                    derp: Some(SocketAddr::new(DERP_MAGIC_IP, 1)),
                    created: Instant::now(),
                    hostinfo: Hostinfo::default(),
                    keep_alive: false,
                    expired: false,
                    online: None,
                    last_seen: None,
                });
            }

            netmap::NetworkMap { peers }
        }

        async fn update_eps(
            eps: Arc<Mutex<Vec<Vec<cfg::Endpoint>>>>,
            ms: &[MagicStack],
            my_idx: usize,
            new_eps: Vec<cfg::Endpoint>,
        ) {
            let eps = &mut *eps.lock().await;
            eps[my_idx] = new_eps;

            for (i, m) in ms.iter().enumerate() {
                let nm = build_netmap(eps, ms, i).await;
                let _ = m.conn.set_network_map(nm).await;
            }
        }

        let mut tasks = JoinSet::new();

        for (my_idx, m) in stacks.iter().enumerate() {
            let m = m.clone();
            let eps = eps.clone();
            let stacks = stacks.clone();
            tasks.spawn(async move {
                loop {
                    tokio::select! {
                        res = m.ep_ch.recv_async() => match res {
                            Ok(new_eps) => {
                                debug!("conn{} endpoints update: {:?}", my_idx + 1, new_eps);
                                update_eps(eps.clone(), &stacks, my_idx, new_eps).await;
                            }
                            Err(err) => {
                                warn!("err: {:?}", err);
                                break;
                            }
                        }
                    }
                }
            });
        }

        Ok(move || {
            tasks.abort_all();
        })
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_two_devices_roundtrip_quinn_magic() -> Result<()> {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
            .with(EnvFilter::from_default_env())
            .try_init()
            .ok();

        let devices = Devices {
            stun_ip: "127.0.0.1".parse()?,
        };

        let (derp_map, cleanup) = run_derp_and_stun(devices.stun_ip).await?;

        let m1 = MagicStack::new(derp_map.clone()).await?;
        let m2 = MagicStack::new(derp_map.clone()).await?;

        let cleanup_mesh = mesh_stacks(vec![m1.clone(), m2.clone()]).await?;

        // Wait for magicsock to be told about peers from mesh_stacks.
        let m1t = m1.clone();
        let m2t = m2.clone();
        time::timeout(Duration::from_secs(10), async move {
            loop {
                let ab = m1t.tracked_endpoints().await.contains(&m2t.public());
                let ba = m2t.tracked_endpoints().await.contains(&m1t.public());
                if ab && ba {
                    break;
                }
            }
        })
        .await
        .context("failed to connect peers")?;

        // msg from  m2 -> m1
        macro_rules! roundtrip {
            ($a:expr, $b:expr, $msg:expr) => {
                let a = $a.clone();
                let b = $b.clone();
                let a_name = stringify!($a);
                let b_name = stringify!($b);
                println!("{} -> {} ({} bytes)", a_name, b_name, $msg.len());
                println!("[{}] {:?}", a_name, a.conn.local_addr().await);
                println!("[{}] {:?}", b_name, b.conn.local_addr().await);

                let a_addr = b.conn.get_mapping_addr(&a.public()).await.unwrap();
                let b_addr = a.conn.get_mapping_addr(&b.public()).await.unwrap();

                println!("{}: {}, {}: {}", a_name, a_addr, b_name, b_addr);

                let b_span = debug_span!("receiver", b_name, %b_addr);
                let b_task = tokio::task::spawn(
                    async move {
                        println!("[{}] accepting conn", b_name);
                        let conn = b.quic_ep.accept().await.expect("no conn");

                        println!("[{}] connecting", b_name);
                        let conn = conn
                            .await
                            .with_context(|| format!("[{}] connecting", b_name))?;
                        println!("[{}] accepting bi", b_name);
                        let (mut send_bi, mut recv_bi) = conn
                            .accept_bi()
                            .await
                            .with_context(|| format!("[{}] accepting bi", b_name))?;

                        println!("[{}] reading", b_name);
                        let val = recv_bi
                            .read_to_end(usize::MAX)
                            .await
                            .with_context(|| format!("[{}] reading to end", b_name))?;

                        println!("[{}] replying", b_name);
                        for chunk in val.chunks(12) {
                            send_bi
                                .write_all(chunk)
                                .await
                                .with_context(|| format!("[{}] sending chunk", b_name))?;
                        }

                        println!("[{}] finishing", b_name);
                        send_bi
                            .finish()
                            .await
                            .with_context(|| format!("[{}] finishing", b_name))?;

                        let stats = conn.stats();
                        println!("[{}] stats: {:#?}", a_name, stats);
                        assert_eq!(stats.path.lost_packets, 0, "[{}] should not loose any packets", b_name);

                        println!("[{}] close", b_name);
                        conn.close(0u32.into(), b"done");
                        println!("[{}] closed", b_name);

                        Ok::<_, anyhow::Error>(())
                    }
                    .instrument(b_span),
                );

                let a_span = debug_span!("sender", a_name, %a_addr);
                async move {
                    println!("[{}] connecting to {}", a_name, b_addr);
                    let conn = a
                        .quic_ep
                        .connect(b_addr, "localhost")?
                        .await
                        .with_context(|| format!("[{}] connect", a_name))?;

                    println!("[{}] opening bi", a_name);
                    let (mut send_bi, mut recv_bi) = conn
                        .open_bi()
                        .await
                        .with_context(|| format!("[{}] open bi", a_name))?;

                    println!("[{}] writing message", a_name);
                    send_bi
                        .write_all(&$msg[..])
                        .await
                        .with_context(|| format!("[{}] write all", a_name))?;

                    println!("[{}] finishing", a_name);
                    send_bi
                        .finish()
                        .await
                        .with_context(|| format!("[{}] finish", a_name))?;

                    println!("[{}] reading_to_end", a_name);
                    let val = recv_bi
                        .read_to_end(usize::MAX)
                        .await
                        .with_context(|| format!("[{}]", a_name))?;
                    anyhow::ensure!(
                        val == $msg,
                        "expected {}, got {}",
                        hex::encode($msg),
                        hex::encode(val)
                    );

                    let stats = conn.stats();
                    println!("[{}] stats: {:#?}", a_name, stats);
                    assert_eq!(stats.path.lost_packets, 0, "[{}] should not loose any packets", a_name);

                    println!("[{}] close", a_name);
                    conn.close(0u32.into(), b"done");
                    println!("[{}] wait idle", a_name);
                    a.quic_ep.wait_idle().await;
                    println!("[{}] waiting for channel", a_name);
                    b_task.await??;
                    Ok(())
                }
                .instrument(a_span)
                .await?;
            };
        }

        for i in 0..10 {
            println!("-- round {}", i + 1);
            roundtrip!(m1, m2, b"hello m1");
            roundtrip!(m2, m1, b"hello m2");

            println!("-- larger data");
            let mut data = vec![0u8; 10 * 1024];
            rand::thread_rng().fill_bytes(&mut data);
            roundtrip!(m1, m2, data);

            let mut data = vec![0u8; 10 * 1024];
            rand::thread_rng().fill_bytes(&mut data);
            roundtrip!(m2, m1, data);
        }

        println!("cleaning up");
        cleanup();
        cleanup_mesh();
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_two_devices_setup_teardown() -> Result<()> {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
            .with(EnvFilter::from_default_env())
            .try_init()
            .ok();

        let devices = Devices {
            stun_ip: "127.0.0.1".parse()?,
        };

        for _ in 0..10 {
            let (derp_map, cleanup) = run_derp_and_stun(devices.stun_ip).await?;
            println!("setting up magic stack");
            let m1 = MagicStack::new(derp_map.clone()).await?;
            let m2 = MagicStack::new(derp_map.clone()).await?;

            let cleanup_mesh = mesh_stacks(vec![m1.clone(), m2.clone()]).await?;

            // Wait for magicsock to be told about peers from mesh_stacks.
            println!("waiting for connection");
            let m1t = m1.clone();
            let m2t = m2.clone();
            time::timeout(Duration::from_secs(10), async move {
                loop {
                    let ab = m1t.tracked_endpoints().await.contains(&m2t.public());
                    let ba = m2t.tracked_endpoints().await.contains(&m1t.public());
                    if ab && ba {
                        break;
                    }
                }
            })
            .await
            .context("failed to connect peers")?;

            println!("closing endpoints");
            m1.quic_ep.close(0u32.into(), b"done");
            m2.quic_ep.close(0u32.into(), b"done");

            println!("closing connection");
            m1.conn.close().await?;
            m2.conn.close().await?;
            assert!(m1.conn.is_closed());
            assert!(m2.conn.is_closed());

            println!("cleaning up");
            cleanup();
            cleanup_mesh();
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_two_devices_roundtrip_quinn_raw() -> Result<()> {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
            .with(EnvFilter::from_default_env())
            .try_init()
            .ok();

        let make_conn = |addr: SocketAddr| -> anyhow::Result<quinn::Endpoint> {
            let key = key::node::SecretKey::generate();
            let conn = std::net::UdpSocket::bind(addr)?;

            let tls_server_config =
                tls::make_server_config(&key.clone().into(), vec![tls::P2P_ALPN.to_vec()], false)?;
            let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(tls_server_config));
            let mut transport_config = quinn::TransportConfig::default();
            transport_config.keep_alive_interval(Some(Duration::from_secs(5)));
            transport_config.max_idle_timeout(Some(Duration::from_secs(10).try_into().unwrap()));
            server_config.transport_config(Arc::new(transport_config));
            let mut quic_ep = quinn::Endpoint::new(
                quinn::EndpointConfig::default(),
                Some(server_config),
                conn,
                Arc::new(quinn::TokioRuntime),
            )?;

            let tls_client_config =
                tls::make_client_config(&key.into(), None, vec![tls::P2P_ALPN.to_vec()], false)?;
            let mut client_config = quinn::ClientConfig::new(Arc::new(tls_client_config));
            let mut transport_config = quinn::TransportConfig::default();
            transport_config.max_idle_timeout(Some(Duration::from_secs(10).try_into().unwrap()));
            client_config.transport_config(Arc::new(transport_config));
            quic_ep.set_default_client_config(client_config);

            Ok(quic_ep)
        };

        let m1 = make_conn("127.0.0.1:8770".parse().unwrap())?;
        let m2 = make_conn("127.0.0.1:8771".parse().unwrap())?;

        // msg from  a -> b
        macro_rules! roundtrip {
            ($a:expr, $b:expr, $msg:expr) => {
                let a = $a.clone();
                let b = $b.clone();
                let a_name = stringify!($a);
                let b_name = stringify!($b);
                println!("{} -> {} ({} bytes)", a_name, b_name, $msg.len());

                let a_addr = a.local_addr()?;
                let b_addr = b.local_addr()?;

                println!("{}: {}, {}: {}", a_name, a_addr, b_name, b_addr);

                let b_task = tokio::task::spawn(async move {
                    println!("[{}] accepting conn", b_name);
                    let conn = b.accept().await.expect("no conn");
                    println!("[{}] connecting", b_name);
                    let conn = conn
                        .await
                        .with_context(|| format!("[{}] connecting", b_name))?;
                    println!("[{}] accepting bi", b_name);
                    let (mut send_bi, mut recv_bi) = conn
                        .accept_bi()
                        .await
                        .with_context(|| format!("[{}] accepting bi", b_name))?;

                    println!("[{}] reading", b_name);
                    let val = recv_bi
                        .read_to_end(usize::MAX)
                        .await
                        .with_context(|| format!("[{}] reading to end", b_name))?;
                    println!("[{}] finishing", b_name);
                    send_bi
                        .finish()
                        .await
                        .with_context(|| format!("[{}] finishing", b_name))?;

                    println!("[{}] close", b_name);
                    conn.close(0u32.into(), b"done");
                    println!("[{}] closed", b_name);

                    Ok::<_, anyhow::Error>(val)
                });

                println!("[{}] connecting to {}", a_name, b_addr);
                let conn = a
                    .connect(b_addr, "localhost")?
                    .await
                    .with_context(|| format!("[{}] connect", a_name))?;

                println!("[{}] opening bi", a_name);
                let (mut send_bi, mut recv_bi) = conn
                    .open_bi()
                    .await
                    .with_context(|| format!("[{}] open bi", a_name))?;
                println!("[{}] writing message", a_name);
                send_bi
                    .write_all(&$msg[..])
                    .await
                    .with_context(|| format!("[{}] write all", a_name))?;

                println!("[{}] finishing", a_name);
                send_bi
                    .finish()
                    .await
                    .with_context(|| format!("[{}] finish", a_name))?;

                println!("[{}] reading_to_end", a_name);
                let _ = recv_bi
                    .read_to_end(usize::MAX)
                    .await
                    .with_context(|| format!("[{}]", a_name))?;
                println!("[{}] close", a_name);
                conn.close(0u32.into(), b"done");
                println!("[{}] wait idle", a_name);
                a.wait_idle().await;

                drop(send_bi);

                // make sure the right values arrived
                println!("[{}] waiting for channel", a_name);
                let val = b_task.await??;
                anyhow::ensure!(
                    val == $msg,
                    "expected {}, got {}",
                    hex::encode($msg),
                    hex::encode(val)
                );
            };
        }

        for i in 0..10 {
            println!("-- round {}", i + 1);
            roundtrip!(m1, m2, b"hello m1");
            roundtrip!(m2, m1, b"hello m2");

            println!("-- larger data");

            let mut data = vec![0u8; 10 * 1024];
            rand::thread_rng().fill_bytes(&mut data);
            roundtrip!(m1, m2, data);
            roundtrip!(m2, m1, data);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_two_devices_roundtrip_quinn_rebinding_conn() -> Result<()> {
        tracing_subscriber::registry()
            .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
            .with(EnvFilter::from_default_env())
            .try_init()
            .ok();

        async fn make_conn(addr: SocketAddr) -> anyhow::Result<quinn::Endpoint> {
            let key = key::node::SecretKey::generate();
            let conn = RebindingUdpConn::bind(addr.port(), addr.ip().into()).await?;

            let tls_server_config =
                tls::make_server_config(&key.clone().into(), vec![tls::P2P_ALPN.to_vec()], false)?;
            let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(tls_server_config));
            let mut transport_config = quinn::TransportConfig::default();
            transport_config.keep_alive_interval(Some(Duration::from_secs(5)));
            transport_config.max_idle_timeout(Some(Duration::from_secs(10).try_into().unwrap()));
            server_config.transport_config(Arc::new(transport_config));
            let mut quic_ep = quinn::Endpoint::new_with_abstract_socket(
                quinn::EndpointConfig::default(),
                Some(server_config),
                conn,
                Arc::new(quinn::TokioRuntime),
            )?;

            let tls_client_config = tls::make_client_config(
                &key.clone().into(),
                None,
                vec![tls::P2P_ALPN.to_vec()],
                false,
            )?;
            let mut client_config = quinn::ClientConfig::new(Arc::new(tls_client_config));
            let mut transport_config = quinn::TransportConfig::default();
            transport_config.max_idle_timeout(Some(Duration::from_secs(10).try_into().unwrap()));
            client_config.transport_config(Arc::new(transport_config));
            quic_ep.set_default_client_config(client_config);

            Ok(quic_ep)
        }

        let m1 = make_conn("127.0.0.1:7770".parse().unwrap()).await?;
        let m2 = make_conn("127.0.0.1:7771".parse().unwrap()).await?;

        // msg from  a -> b
        macro_rules! roundtrip {
            ($a:expr, $b:expr, $msg:expr) => {
                let a = $a.clone();
                let b = $b.clone();
                let a_name = stringify!($a);
                let b_name = stringify!($b);
                println!("{} -> {} ({} bytes)", a_name, b_name, $msg.len());

                let a_addr: SocketAddr = format!("127.0.0.1:{}", a.local_addr()?.port())
                    .parse()
                    .unwrap();
                let b_addr: SocketAddr = format!("127.0.0.1:{}", b.local_addr()?.port())
                    .parse()
                    .unwrap();

                println!("{}: {}, {}: {}", a_name, a_addr, b_name, b_addr);

                let b_task = tokio::task::spawn(async move {
                    println!("[{}] accepting conn", b_name);
                    let conn = b.accept().await.expect("no conn");
                    println!("[{}] connecting", b_name);
                    let conn = conn
                        .await
                        .with_context(|| format!("[{}] connecting", b_name))?;
                    println!("[{}] accepting bi", b_name);
                    let (mut send_bi, mut recv_bi) = conn
                        .accept_bi()
                        .await
                        .with_context(|| format!("[{}] accepting bi", b_name))?;

                    println!("[{}] reading", b_name);
                    let val = recv_bi
                        .read_to_end(usize::MAX)
                        .await
                        .with_context(|| format!("[{}] reading to end", b_name))?;
                    println!("[{}] finishing", b_name);
                    send_bi
                        .finish()
                        .await
                        .with_context(|| format!("[{}] finishing", b_name))?;

                    println!("[{}] close", b_name);
                    conn.close(0u32.into(), b"done");
                    println!("[{}] closed", b_name);

                    Ok::<_, anyhow::Error>(val)
                });

                println!("[{}] connecting to {}", a_name, b_addr);
                let conn = a
                    .connect(b_addr, "localhost")?
                    .await
                    .with_context(|| format!("[{}] connect", a_name))?;

                println!("[{}] opening bi", a_name);
                let (mut send_bi, mut recv_bi) = conn
                    .open_bi()
                    .await
                    .with_context(|| format!("[{}] open bi", a_name))?;
                println!("[{}] writing message", a_name);
                send_bi
                    .write_all(&$msg[..])
                    .await
                    .with_context(|| format!("[{}] write all", a_name))?;

                println!("[{}] finishing", a_name);
                send_bi
                    .finish()
                    .await
                    .with_context(|| format!("[{}] finish", a_name))?;

                println!("[{}] reading_to_end", a_name);
                let _ = recv_bi
                    .read_to_end(usize::MAX)
                    .await
                    .with_context(|| format!("[{}]", a_name))?;
                println!("[{}] close", a_name);
                conn.close(0u32.into(), b"done");
                println!("[{}] wait idle", a_name);
                a.wait_idle().await;

                drop(send_bi);

                // make sure the right values arrived
                println!("[{}] waiting for channel", a_name);
                let val = b_task.await??;
                anyhow::ensure!(
                    val == $msg,
                    "expected {}, got {}",
                    hex::encode($msg),
                    hex::encode(val)
                );
            };
        }

        for i in 0..10 {
            println!("-- round {}", i + 1);
            roundtrip!(m1, m2, b"hello m1");
            roundtrip!(m2, m1, b"hello m2");

            println!("-- larger data");

            let mut data = vec![0u8; 10 * 1024];
            rand::thread_rng().fill_bytes(&mut data);
            roundtrip!(m1, m2, data);
            roundtrip!(m2, m1, data);
        }

        Ok(())
    }
}
