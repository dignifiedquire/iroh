use std::{
    collections::HashMap,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context};
use bytes::{Bytes, BytesMut};
use iroh_net::{
    endpoint::{get_peer_id, MagicEndpoint},
    tls::PeerId,
};
use rand::rngs::StdRng;
use rand_core::SeedableRng;
use tokio::{
    sync::{broadcast, mpsc, oneshot},
    task::JoinHandle,
};
use tracing::{debug, warn};

use self::util::{read_message, write_message, DialQueue, Timers};
use crate::proto::{gossipswarm, topicswarm, Config, TopicId, TopicSwarm};

mod util;

/// ALPN protocol for the gossipswarm
pub const GOSSIP_ALPN: &[u8] = b"n0/iroh-gossipswarm/0";
/// Maximum message size is limited to 1024 bytes for now.
pub(crate) const MAX_MESSAGE_SIZE: usize = 1024;
/// Channel capacity for topic subscription broadcast channels (one per topic)
const SUBSCRIBE_ALL_CAP: usize = 64;
/// Channel capacity for all subscription broadcast channels (single)
const SUBSCRIBE_TOPIC_CAP: usize = 64;
/// Channel capacity for the send queue (one per connection)
const SEND_QUEUE_CAP: usize = 64;
/// Channel capacity for the ToActor message queue (single)
const TO_ACTOR_CAP: usize = 64;
/// Channel capacity for the InEvent message queue (single)
const IN_EVENT_CAP: usize = 1024;
/// Timeout after which [GossipHandle::join] returns an error if no peer connections
/// could be established for the topic
const JOIN_TOPIC_TIMEOUT: Duration = Duration::from_secs(10);

pub type InEvent = topicswarm::InEvent<PeerId>;
pub type OutEvent = topicswarm::OutEvent<PeerId>;
pub type Event = gossipswarm::Event<PeerId>;
pub type Command = gossipswarm::Command<PeerId>;
pub type Timer = topicswarm::Timer<PeerId>;
pub type ProtoMessage = topicswarm::Message<PeerId>;

/// Publish and subscribe on gossiping topics.
///
/// Each topic is a separate broadcast tree with separate memberships.
///
/// A topic has to be joined before you can publish or subscribe on the topic. 
/// To join the swarm for a topic, you have to know the [PeerId] of at least one peer that also joined the topic.
///
/// Messages published on the swarm will be delivered to all peers that joined the swarm for that
/// topic. You will also be relaying (gossiping) messages published by other peers.
///
/// With the default settings, the gossipswarm will maintain up to 5 peer connections per topic.
///
/// While the GossipHandle is created from a [MagicEndpoint], it does not accept connections
/// itself. You should run an accept loop on the MagicEndpoint yourself, check the ALPN protocol of incoming
/// connections, and if the ALPN protocol equals [GOSSIP_ALPN], forward the connection to the
/// gossip actor through [Self::handle_connection].
///
/// The gossip actor will, however, initiate new connections to other peers by itself.
#[derive(Clone)]
pub struct GossipHandle {
    to_actor_tx: mpsc::Sender<ToActor>,
    _actor_handle: Arc<JoinHandle<anyhow::Result<()>>>,
}

impl GossipHandle {
    /// Spawn a gossip actor and get a handle for it
    pub fn from_endpoint(endpoint: MagicEndpoint, config: Config) -> Self {
        let peer_id = endpoint.peer_id();
        let dialer = DialQueue::new(endpoint);
        Self::new(dialer, peer_id, config)
    }

    fn new(
        dial_machine: DialQueue,
        peer_id: PeerId,
        // accept_policy: AcceptTopicPolicy,
        config: Config,
    ) -> Self {
        let state = TopicSwarm::new(peer_id, config, rand::rngs::StdRng::from_entropy());
        let (to_actor_tx, to_actor_rx) = mpsc::channel(TO_ACTOR_CAP);
        let (in_event_tx, in_event_rx) = mpsc::channel(IN_EVENT_CAP);
        let actor = GossipActor {
            state,
            // accept_policy: AcceptTopicPolicy::AcceptAll,
            to_actor_rx,
            in_event_rx,
            in_event_tx,
            conns: Default::default(),
            conn_send_tx: Default::default(),
            subscribers_topic: Default::default(),
            subscribers_all: None,
            dial_queue: dial_machine,
            pending_sends: Default::default(),
            timers: Timers::new(),
        };
        let actor_handle = tokio::spawn(async move {
            if let Err(err) = actor.run().await {
                warn!("GossipActor closed with error: {err:?}");
                Err(err)
            } else {
                Ok(())
            }
        });
        Self {
            _actor_handle: Arc::new(actor_handle),
            to_actor_tx,
        }
    }

    /// Join a topic
    ///
    /// Waits for at least one successfull peer connection for the topic, and returns
    /// an error if no connection is established before a timeout is reached.
    pub async fn join(&self, topic: TopicId, known_peers: Vec<PeerId>) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.send(ToActor::Join(topic, known_peers, tx)).await?;
        rx.await?
    }

    /// Broadcast a message on a topic
    ///
    /// Does not join the topic automatically, so you have to call [Self::join] yourself
    /// for messages to be broadcast to peers.
    pub async fn broadcast(&self, topic: TopicId, message: Bytes) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.send(ToActor::Broadcast(topic, message, tx)).await?;
        rx.await??;
        Ok(())
    }

    /// Subscribe to messages and event notifications for a topic.
    ///
    /// Does not join the topic automatically, so you have to call [Self::join] yourself
    /// to actually receive messages.
    pub async fn subscribe(&self, topic: TopicId) -> anyhow::Result<broadcast::Receiver<Event>> {
        let (tx, rx) = oneshot::channel();
        self.send(ToActor::Subscribe(topic, tx)).await?;
        let res = rx.await.map_err(|_| anyhow!("subscribe_tx dropped"))??;
        Ok(res)
    }

    /// Subscribe to all events published on topics that you joined.
    pub async fn subscribe_all(&self) -> anyhow::Result<broadcast::Receiver<(TopicId, Event)>> {
        let (tx, rx) = oneshot::channel();
        self.send(ToActor::SubscribeAll(tx)).await?;
        let res = rx.await.map_err(|_| anyhow!("subscribe_tx dropped"))??;
        Ok(res)
    }

    /// Pass an incoming [quinn::Connection] to the gossip actor.
    ///
    /// Make sure to check the ALPN protocol yourself before passing the connection.
    pub async fn handle_connection(&self, mut conn: quinn::Connection) -> anyhow::Result<()> {
        let peer_id = get_peer_id(&mut conn).await?;
        self.send(ToActor::ConnIncoming(peer_id, ConnOrigin::Accept, conn))
            .await?;
        Ok(())
    }

    async fn send(&self, event: ToActor) -> anyhow::Result<()> {
        self.to_actor_tx
            .send(event)
            .await
            .map_err(|_| anyhow!("gossip actor dropped"))
    }
}


/// Whether a connection is initiated by us (Dial) or by the remote peer (Accept)
#[derive(Debug)]
enum ConnOrigin {
    Accept,
    Dial,
}

/// Input messages for the [GossipActor]
enum ToActor {
    ConnIncoming(PeerId, ConnOrigin, quinn::Connection),
    Join(TopicId, Vec<PeerId>, oneshot::Sender<anyhow::Result<()>>),
    Broadcast(TopicId, Bytes, oneshot::Sender<anyhow::Result<()>>),
    Subscribe(
        TopicId,
        oneshot::Sender<anyhow::Result<broadcast::Receiver<Event>>>,
    ),
    SubscribeAll(oneshot::Sender<anyhow::Result<broadcast::Receiver<(TopicId, Event)>>>),
}

impl fmt::Debug for ToActor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ToActor::ConnIncoming(peer_id, origin, _conn) => {
                write!(f, "ConnIncoming({peer_id:?}, {origin:?})")
            }
            ToActor::Join(topic, peers, _reply) => write!(f, "Join({topic:?}, {peers:?})"),
            ToActor::Broadcast(topic, message, _reply) => {
                write!(f, "Broadcast({topic:?}, bytes<{}>)", message.len())
            }
            ToActor::Subscribe(topic, _reply) => write!(f, "Subscribe({topic:?})"),
            ToActor::SubscribeAll(_reply) => write!(f, "SubscribeAll"),
        }
    }
}

struct GossipActor {
    state: TopicSwarm<PeerId, StdRng>,
    // accept_policy: AcceptTopicPolicy,
    to_actor_rx: mpsc::Receiver<ToActor>,
    in_event_rx: mpsc::Receiver<InEvent>,
    in_event_tx: mpsc::Sender<InEvent>,
    conns: HashMap<PeerId, quinn::Connection>,
    conn_send_tx: HashMap<PeerId, mpsc::Sender<ProtoMessage>>,
    subscribers_topic: HashMap<TopicId, broadcast::Sender<Event>>,
    subscribers_all: Option<broadcast::Sender<(TopicId, Event)>>,
    dial_queue: DialQueue,
    pending_sends: HashMap<PeerId, Vec<ProtoMessage>>,
    timers: Timers<Timer>,
}

impl GossipActor {
    pub async fn run(mut self) -> anyhow::Result<()> {
        let me = *self.state.endpoint();
        loop {
            tokio::select! {
                biased;
                msg = self.to_actor_rx.recv() => {
                    match msg {
                        Some(msg) => self.handle_to_actor_msg(msg, Instant::now()).await?,
                        None => {
                            debug!("all GossipHandles dropped, close gossip actor");
                            break;
                        }
                    }
                },
                (peer_id, res) = self.dial_queue.next() => {
                    match res {
                        Ok(conn) => {
                            debug!(me = ?me, peer = ?peer_id, "dial successfull");
                            self.handle_to_actor_msg(ToActor::ConnIncoming(peer_id, ConnOrigin::Dial, conn), Instant::now()).await.context("dialer.next -> conn -> handle_to_actor_msg")?;
                        }
                        Err(err) => {
                            // TODO: Remove pending messages?
                            warn!(me = ?me, peer = ?peer_id, "dial failed: {err}");
                            self.handle_in_event(InEvent::PeerDisconnected(peer_id), Instant::now()).await.context(format!("dialer.next -> err {err} -> handle_in_event PeerDisconnected{peer_id:?}"))?
                        }
                    }
                }
                event = self.in_event_rx.recv() => {
                    match event {
                        Some(event) => {
                            self.handle_in_event(event, Instant::now()).await.context("in_event_rx.recv -> handle_in_event")?;
                        }
                        None => unreachable!()
                    }
                }
                drain = self.timers.wait_and_drain() => {
                    let now = Instant::now();
                    for (_instant, timer) in drain {
                        self.handle_in_event(InEvent::TimerExpired(timer), now).await.context("timers.drain_expired -> handle_in_event")?;
                    }
                    self.timers.reset();
                }

            }
        }
        Ok(())
    }

    async fn handle_to_actor_msg(&mut self, msg: ToActor, now: Instant) -> anyhow::Result<()> {
        let me = *self.state.endpoint();
        debug!(me = ?me, "handle to_actor  {msg:?}");
        match msg {
            ToActor::ConnIncoming(peer_id, origin, conn) => {
                self.conns.insert(peer_id, conn.clone());
                self.dial_queue.abort_dial(&peer_id);
                let (send_tx, send_rx) = mpsc::channel(SEND_QUEUE_CAP);
                self.conn_send_tx.insert(peer_id, send_tx.clone());

                // spawn a task for this connection
                let in_event_tx = self.in_event_tx.clone();
                tokio::spawn(async move {
                    debug!(me = ?me, peer = ?peer_id, "connection established, start loop");
                    let res = run_conn_loop(peer_id, conn, origin, send_rx, &in_event_tx).await;
                    match res {
                        Ok(()) => {
                            debug!(me = ?me, peer = ?peer_id, "connection closed without error")
                        }
                        Err(err) => {
                            debug!(me = ?me, peer = ?peer_id, "connection closed with error {err:?}")
                        }
                    }
                    in_event_tx
                        .send(InEvent::PeerDisconnected(peer_id))
                        .await
                        .ok();
                });

                // Forward queued pending sends
                if let Some(send_queue) = self.pending_sends.remove(&peer_id) {
                    for msg in send_queue {
                        send_tx.send(msg).await?;
                    }
                }
            }
            ToActor::Join(topic_id, peers, reply) => {
                for peer_id in peers.iter() {
                    self.handle_in_event(InEvent::Command(topic_id, Command::Join(*peer_id)), now)
                        .await?;
                }
                if self.state.has_active_peers(&topic_id) {
                    // If the active_view contains at least one peer, return now
                    reply.send(Ok(())).ok();
                } else {
                    // Otherwise, wait for any peer to come up as neighbor.
                    let sub = self.subscribe(topic_id);
                    tokio::spawn(async move {
                        let res = wait_for_neighbor_up(sub, JOIN_TOPIC_TIMEOUT).await;
                        reply.send(res).ok();
                    });
                }
            }
            ToActor::Broadcast(topic_id, message, reply) => {
                self.handle_in_event(InEvent::Command(topic_id, Command::Broadcast(message)), now)
                    .await?;
                reply.send(Ok(())).ok();
            }
            ToActor::Subscribe(topic_id, reply) => {
                let rx = self.subscribe(topic_id);
                reply.send(Ok(rx)).ok();
            }
            ToActor::SubscribeAll(reply) => {
                let rx = self.subscribe_all();
                reply.send(Ok(rx)).ok();
            }
        };
        Ok(())
    }

    async fn handle_in_event(&mut self, event: InEvent, now: Instant) -> anyhow::Result<()> {
        let me = *self.state.endpoint();
        debug!(me = ?me, "handle in_event  {event:?}");
        let out = self.state.handle(event, now);
        for event in out {
            debug!(me = ?me, "handle out_event {event:?}");
            match event {
                OutEvent::SendMessage(peer_id, message) => {
                    if let Some(send) = self.conn_send_tx.get(&peer_id) {
                        send.send(message)
                            .await
                            .with_context(|| format!("conn_send[{peer_id:?}] dropped"))?;
                    } else {
                        debug!(me = ?me, peer = ?peer_id, "dial");
                        self.dial_queue.queue_dial(peer_id, GOSSIP_ALPN);
                        // TODO: Enforce max length
                        self.pending_sends.entry(peer_id).or_default().push(message);
                    }
                }
                OutEvent::EmitEvent(topic_id, event) => {
                    if let Some(sender) = self.subscribers_all.as_mut() {
                        if let Err(_event) = sender.send((topic_id, event.clone())) {
                            self.subscribers_all = None;
                        }
                    }
                    if let Some(sender) = self.subscribers_topic.get(&topic_id) {
                        // Only error case is that all [broadcast::Receivers] have been dropped.
                        // If so, remove the sender as well.
                        if let Err(_event) = sender.send(event) {
                            self.subscribers_topic.remove(&topic_id);
                        }
                    }
                }
                OutEvent::ScheduleTimer(delay, timer) => {
                    self.timers.insert(now + delay, timer);
                }
                OutEvent::DisconnectPeer(peer) => {
                    if let Some(conn) = self.conns.remove(&peer) {
                        conn.close(0u8.into(), b"close from disconnect");
                    }
                    self.conn_send_tx.remove(&peer);
                    self.pending_sends.remove(&peer);
                    self.dial_queue.abort_dial(&peer);
                }
            }
        }
        Ok(())
    }


    fn subscribe_all(&mut self) -> broadcast::Receiver<(TopicId, Event)> {
        if let Some(tx) = self.subscribers_all.as_mut() {
            tx.subscribe()
        } else {
            let (tx, rx) = broadcast::channel(SUBSCRIBE_ALL_CAP);
            self.subscribers_all = Some(tx);
            rx
        }
    }

    fn subscribe(&mut self, topic_id: TopicId) -> broadcast::Receiver<Event> {
        if let Some(tx) = self.subscribers_topic.get(&topic_id) {
            tx.subscribe()
        } else {
            let (tx, rx) = broadcast::channel(SUBSCRIBE_TOPIC_CAP);
            self.subscribers_topic.insert(topic_id, tx);
            rx
        }
    }
}

async fn wait_for_neighbor_up(
    mut sub: broadcast::Receiver<Event>,
    timeout: Duration,
) -> anyhow::Result<()> {
    let timeout = tokio::time::sleep(timeout);
    tokio::pin!(timeout);
    loop {
        tokio::select! {
            biased;
            _ = &mut timeout => break Err(anyhow!("Failed to join swarm: Timeout expired with no connections")),
            event = sub.recv() => {
                match event {
                    Ok(Event::NeighborUp(_neighbor)) =>  break Ok(()),
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {},
                    Err(broadcast::error::RecvError::Closed) => break Err(anyhow!("Failed to join swarm: Gossip actor dropped")),
                }
            }
        }
    }
}

async fn run_conn_loop(
    from: PeerId,
    conn: quinn::Connection,
    origin: ConnOrigin,
    mut send_rx: mpsc::Receiver<ProtoMessage>,
    in_event_tx: &mpsc::Sender<InEvent>,
) -> anyhow::Result<()> {
    let (mut send, mut recv) = match origin {
        ConnOrigin::Accept => conn.accept_bi().await?,
        ConnOrigin::Dial => conn.open_bi().await?,
    };
    let mut send_buf = BytesMut::new();
    let mut recv_buf = BytesMut::new();
    loop {
        tokio::select! {
            biased;
            msg = send_rx.recv() => {
                match msg {
                    None => break,
                    Some(msg) =>  write_message(&mut send, &mut send_buf, &msg).await?,
                }
            }

            msg = read_message(&mut recv, &mut recv_buf) => {
                let msg = msg?;
                match msg {
                    None => break,
                    Some(msg) => in_event_tx.send(InEvent::RecvMessage(from, msg)).await?
                }
            }
        }
    }
    Ok(())
}

// pub enum AcceptTopicPolicy {
//     AcceptAll,
//     AcceptNone,
//     Filter(FilterTopicFn),
// }
// pub type FilterTopicFn = Box<dyn Fn(PeerId, TopicId) -> BoxFuture<'static, bool> + Send + 'static>;

#[cfg(test)]
mod test {
    use std::net::SocketAddr;

    use iroh_net::{endpoint::MagicEndpoint, hp::derp::DerpMap, tls::Keypair};
    use tokio::spawn;
    use tokio_util::sync::CancellationToken;
    use tracing::info;

    use super::*;

    async fn create_endpoint(derp_map: DerpMap) -> anyhow::Result<MagicEndpoint> {
        MagicEndpoint::bind(
            Keypair::generate(),
            SocketAddr::new([127, 0, 0, 1].into(), 0),
            vec![GOSSIP_ALPN.to_vec()],
            None,
            Some(derp_map),
            false,
        )
        .await
    }

    async fn endpoint_loop(
        endpoint: MagicEndpoint,
        gossip: GossipHandle,
        cancel: CancellationToken,
    ) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                conn = endpoint.accept() => match conn {
                    None => break,
                    Some(conn) => gossip.handle_connection(conn.await?).await?
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn gossip_net_smoke() {
        util::setup_logging();
        let (derp_map, cleanup) = util::run_derp_and_stun([127, 0, 0, 1].into())
            .await
            .unwrap();

        let ep1 = create_endpoint(derp_map.clone()).await.unwrap();
        let ep2 = create_endpoint(derp_map.clone()).await.unwrap();
        let ep3 = create_endpoint(derp_map.clone()).await.unwrap();

        let go1 = GossipHandle::from_endpoint(ep1.clone(), Default::default());
        let go2 = GossipHandle::from_endpoint(ep2.clone(), Default::default());
        let go3 = GossipHandle::from_endpoint(ep3.clone(), Default::default());
        debug!("peer1 {:?}", ep1.peer_id());
        debug!("peer2 {:?}", ep2.peer_id());
        debug!("peer3 {:?}", ep3.peer_id());
        let pi1 = ep1.peer_id();

        let cancel = CancellationToken::new();
        let mut tasks = vec![];
        tasks.push(spawn(endpoint_loop(ep1, go1.clone(), cancel.clone())));
        tasks.push(spawn(endpoint_loop(ep2, go3.clone(), cancel.clone())));
        tasks.push(spawn(endpoint_loop(ep3, go2.clone(), cancel.clone())));

        let topic: TopicId = blake3::hash(b"foobar").into();
        go2.join(topic, vec![pi1]).await.unwrap();
        go3.join(topic, vec![pi1]).await.unwrap();
        go1.join(topic, vec![]).await.unwrap();

        let pub1 = spawn(async move {
            for i in 0..10 {
                let message = format!("hi{}", i);
                info!("go1 broadcast: {message:?}");
                go1.broadcast(topic, message.into_bytes().into())
                    .await
                    .unwrap();
                tokio::time::sleep(Duration::from_micros(1)).await;
            }
        });

        let sub2 = spawn(async move {
            let mut stream = go2.subscribe(topic).await.unwrap();
            let mut recv = vec![];
            loop {
                let ev = stream.recv().await.unwrap();
                info!("go2 event: {ev:?}");
                match ev {
                    Event::Received(msg) => recv.push(msg),
                    _ => {}
                }
                if recv.len() == 10 {
                    return recv;
                }
            }
        });

        let sub3 = spawn(async move {
            let mut stream = go3.subscribe(topic).await.unwrap();
            let mut recv = vec![];
            loop {
                let ev = stream.recv().await.unwrap();
                info!("go3 event: {ev:?}");
                match ev {
                    Event::Received(msg) => recv.push(msg),
                    _ => {}
                }
                if recv.len() == 10 {
                    return recv;
                }
            }
        });

        pub1.await.unwrap();
        let recv2 = sub2.await.unwrap();
        let recv3 = sub3.await.unwrap();

        let expected: Vec<Bytes> = (0..10)
            .map(|i| Bytes::from(format!("hi{i}").into_bytes()))
            .collect();
        assert_eq!(recv2, expected);
        assert_eq!(recv3, expected);

        cancel.cancel();
        for t in tasks {
            t.await.unwrap().unwrap();
        }
        cleanup().await;
    }

    // This is copied from iroh-net/src/hp/magicsock/conn.rs
    // TODO: Move into a public test_utils module in iroh-net?
    mod util {
        use std::net::{IpAddr, SocketAddr};

        use anyhow::Result;
        use futures::future::BoxFuture;
        use iroh_net::hp::{
            derp::{DerpMap, DerpNode, DerpRegion, UseIpv4, UseIpv6},
            stun::{is, parse_binding_request, response},
        };
        use tokio::sync::oneshot;
        use tracing::debug;
        use tracing_subscriber::{prelude::*, EnvFilter};

        pub fn setup_logging() {
            tracing_subscriber::registry()
                .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
                .with(EnvFilter::from_default_env())
                .try_init()
                .ok();
        }

        pub async fn run_derp_and_stun(
            stun_ip: IpAddr,
        ) -> Result<(DerpMap, impl FnOnce() -> BoxFuture<'static, ()>)> {
            // TODO: pass a mesh_key?

            let server_key = iroh_net::hp::key::node::SecretKey::generate();
            // let tls_config = iroh_net::hp::derp::http::make_tls_config();
            let server =
                iroh_net::hp::derp::http::ServerBuilder::new("127.0.0.1:0".parse().unwrap())
                    .secret_key(Some(server_key))
                    // .tls_config(Some(tls_config))
                    .tls_config(None)
                    .spawn()
                    .await?;

            let https_addr = server.addr();
            println!("DERP listening on {:?}", https_addr);

            let (stun_addr, stun_cleanup) = serve(stun_ip).await?;
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

                            derp_port: https_addr.port(),
                            stun_test_ip: Some(stun_addr.ip()),
                        }],
                        avoid: false,
                    },
                )]
                .into_iter()
                .collect(),
            };

            let cleanup = move || {
                Box::pin(async move {
                    println!("CLEANUP");
                    stun_cleanup.send(()).unwrap();
                    server.shutdown().await;
                }) as BoxFuture<'static, ()>
            };

            Ok((m, cleanup))
        }

        /// Sets up a simple STUN server.
        ///
        /// The returned [`oneshot::Sender`] can be used to stop the server, either drop it or
        /// send a message on it.
        pub async fn serve(ip: IpAddr) -> anyhow::Result<(SocketAddr, oneshot::Sender<()>)> {
            let pc = tokio::net::UdpSocket::bind((ip, 0)).await?;
            let mut addr = pc.local_addr()?;
            match addr.ip() {
                IpAddr::V4(ip) => {
                    if ip.octets() == [0, 0, 0, 0] {
                        addr.set_ip("127.0.0.1".parse().unwrap());
                    }
                }
                _ => unreachable!("using ipv4"),
            }

            println!("listening on {}", addr);
            let (s, r) = oneshot::channel();
            tokio::task::spawn(async move {
                run_stun(pc, r).await;
            });

            Ok((addr, s))
        }

        async fn run_stun(pc: tokio::net::UdpSocket, mut done: oneshot::Receiver<()>) {
            let mut buf = vec![0u8; 64 << 10];
            loop {
                debug!("stun read loop");
                tokio::select! {
                    _ = &mut done => {
                        debug!("shutting down");
                        break;
                    }
                    res = pc.recv_from(&mut buf) => match res {
                        Ok((n, addr)) => {
                            debug!("stun read packet {}bytes from {}", n, addr);
                            let pkt = &buf[..n];
                            if !is(pkt) {
                                debug!("stun received non STUN pkt");
                                continue;
                            }
                            if let Ok(txid) = parse_binding_request(pkt) {
                                debug!("stun received binding request");
                                let res = response(txid, addr);
                                if let Err(err) = pc.send_to(&res, addr).await {
                                    eprintln!("STUN server write failed: {:?}", err);
                                }
                            }
                        }
                        Err(err) => {
                            eprintln!("failed to read: {:?}", err);
                        }
                    }
                }
            }
        }
    }
}
