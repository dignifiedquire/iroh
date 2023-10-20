use std::{
    collections::{HashMap, HashSet},
    net::{IpAddr, SocketAddr},
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};

use backoff::backoff::Backoff;
use bytes::{Bytes, BytesMut};
use iroh_metrics::{inc, inc_by};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time,
};
use tracing::{debug, info, trace, warn};

use crate::{
    derp::{self, http::ClientError, MAX_PACKET_SIZE},
    key::{PublicKey, PUBLIC_KEY_LENGTH},
};

use super::{ActorMessage, Inner};
use super::{DerpContents, Metrics as MagicsockMetrics};

/// How long a non-home DERP connection needs to be idle (last written to) before we close it.
const DERP_INACTIVE_CLEANUP_TIME: Duration = Duration::from_secs(60);

/// How often `clean_stale_derp` runs when there are potentially-stale DERP connections to close.
const DERP_CLEAN_STALE_INTERVAL: Duration = Duration::from_secs(15);

pub(super) enum DerpActorMessage {
    Send {
        region_id: u16,
        contents: DerpContents,
        peer: PublicKey,
    },
    Connect {
        region_id: u16,
        peer: Option<PublicKey>,
    },
    NotePreferred(u16),
    MaybeCloseDerpsOnRebind(Vec<IpAddr>),
    Shutdown,
}

/// Contains fields for an active DERP connection.
#[derive(Debug)]
struct ActiveDerp {
    c: derp::http::Client,
    /// The time of the last request for its write
    /// channel (currently even if there was no write).
    last_write: Instant,
    reader: ReaderState,
    inbox: mpsc::Receiver<ActiveDerpMessage>,
    msg_sender: mpsc::Sender<ActorMessage>,
    /// Contains optional alternate routes to use as an optimization instead of
    /// contacting a peer via their home DERP connection. If they sent us a message
    /// on this DERP connection (which should really only be on our DERP
    /// home connection, or what was once our home), then we remember that route here to optimistically
    /// use instead of creating a new DERP connection back to their home.
    derp_routes: Vec<PublicKey>,
}

#[derive(Debug)]
enum ActiveDerpMessage {
    GetLastWrite(oneshot::Sender<Instant>),
    Ping(oneshot::Sender<Result<Duration, ClientError>>),
    GetLocalAddr(oneshot::Sender<Option<SocketAddr>>),
    GetPeerRoute(PublicKey, oneshot::Sender<Option<derp::http::Client>>),
    GetClient(oneshot::Sender<derp::http::Client>),
    NotePreferred(bool),
    Shutdown,
}

impl ActiveDerp {
    async fn run(mut self) {
        if let Err(err) = self.c.connect().await {
            // TODO: what to do?
            warn!("failed to connect to derp server: {:?}", err);
        }

        loop {
            tokio::select! {
                Some(msg) = self.inbox.recv() => {
                    match msg {
                        ActiveDerpMessage::GetLastWrite(r) => {
                            r.send(self.last_write).ok();
                        }
                        ActiveDerpMessage::Ping(r) => {
                            r.send(self.c.ping().await).ok();
                        }
                        ActiveDerpMessage::GetLocalAddr(r) => {
                            r.send(self.c.local_addr().await).ok();
                        }
                        ActiveDerpMessage::GetClient(r) => {
                            self.last_write = Instant::now();
                            r.send(self.c.clone()).ok();
                        }
                        ActiveDerpMessage::NotePreferred(is_preferred) => {
                            self.c.note_preferred(is_preferred).await;
                        }
                        ActiveDerpMessage::GetPeerRoute(peer, r) => {
                            let res = if self.derp_routes.contains(&peer) {
                                Some(self.c.clone())
                            } else {
                                None
                            };
                            r.send(res).ok();
                        }
                        ActiveDerpMessage::Shutdown => {
                            self.c.close().await.ok();
                            break;
                        }
                    }
                }
                (read_result, read_action) = self.reader.recv() => {
                    match read_action {
                        ReadAction::None => {},
                        ReadAction::AddPeerRoute { peer,  } => {
                            self.derp_routes.push(peer);
                        },
                        ReadAction::RemovePeerRoutes { peers, } => {
                            self.derp_routes.retain(|peer| {
                                !peers.contains(&peer)
                            });
                        }
                    }
                    match read_result {
                        ReadResult::Break => {
                            self.c.close().await.ok();
                            break;
                        }
                        ReadResult::Continue => {},
                        ReadResult::Yield(read_result) => {
                            self.msg_sender.send(ActorMessage::ReceiveDerp(read_result)).await.ok();
                        }
                    }
                }
                else => {
                    break;
                }
            }
        }
    }
}

pub(super) struct DerpActor {
    conn: Arc<Inner>,
    /// DERP regionID -> connection to a node in that region
    active_derp: HashMap<u16, (mpsc::Sender<ActiveDerpMessage>, JoinHandle<()>)>,
    msg_sender: mpsc::Sender<ActorMessage>,
}

impl DerpActor {
    pub(super) fn new(conn: Arc<Inner>, msg_sender: mpsc::Sender<ActorMessage>) -> Self {
        DerpActor {
            conn,
            active_derp: HashMap::default(),
            msg_sender,
        }
    }

    pub(super) async fn run(mut self, mut receiver: mpsc::Receiver<DerpActorMessage>) {
        let mut cleanup_timer = time::interval_at(
            time::Instant::now() + DERP_CLEAN_STALE_INTERVAL,
            DERP_CLEAN_STALE_INTERVAL,
        );

        loop {
            tokio::select! {
                Some(msg) = receiver.recv() => {
                    match msg {
                        DerpActorMessage::Send { region_id, contents, peer } => {
                            self.send_derp(region_id, contents, peer).await;
                        }
                        DerpActorMessage::Connect { region_id, peer } => {
                            self.connect_derp(region_id, peer.as_ref()).await;
                        }
                        DerpActorMessage::NotePreferred(my_derp) => {
                            self.note_preferred(my_derp).await;
                        }
                        DerpActorMessage::MaybeCloseDerpsOnRebind(ifs) => {
                            self.maybe_close_derps_on_rebind(&ifs).await;
                        }
                        DerpActorMessage::Shutdown => {
                            debug!("shutting down");
                            self.close_all_derp("conn-close").await;
                            break;
                        }
                    }
                }
                _ = cleanup_timer.tick() => {
                    trace!("tick: cleanup");
                    self.clean_stale_derp().await;
                }
                else => {
                    trace!("shutting down derp recv loop");
                    break;
                }
            }
        }
    }

    async fn note_preferred(&self, my_num: u16) {
        futures::future::join_all(
            self.active_derp
                .iter()
                .map(|(region_id, (s, _))| async move {
                    let is_preferred = *region_id == my_num;
                    s.send(ActiveDerpMessage::NotePreferred(is_preferred))
                        .await
                        .ok()
                }),
        )
        .await;
    }

    async fn send_derp(&mut self, region_id: u16, contents: DerpContents, peer: PublicKey) {
        debug!(region_id, ?peer, "sending derp");
        if !self.conn.derp_map.contains_region(region_id) {
            warn!("unknown region id {}", region_id);
            return;
        }

        // Derp Send
        let derp_client = self.connect_derp(region_id, Some(&peer)).await;
        for content in &contents {
            trace!("[DERP] -> {} ({}b) {:?}", region_id, content.len(), peer);
        }
        let total_bytes = contents.iter().map(|c| c.len() as u64).sum::<u64>();

        const PAYLAOD_SIZE: usize = MAX_PACKET_SIZE - PUBLIC_KEY_LENGTH;

        // Split into multiple packets if needed.
        // In almost all cases this will be a single packet.
        // But we have no guarantee that the total size of the contents including
        // length prefix will be smaller than the payload size.
        for packet in PacketizeIter::<_, PAYLAOD_SIZE>::new(contents) {
            match derp_client.send(peer, packet).await {
                Ok(_) => {
                    inc_by!(MagicsockMetrics, send_derp, total_bytes);
                }
                Err(err) => {
                    warn!("derp.send: failed {:?}", err);
                    inc!(MagicsockMetrics, send_derp_error);
                }
            }
        }

        // Wake up the send waker if one is waiting for space in the channel
        let mut wakers = self.conn.network_send_wakers.lock().unwrap();
        if let Some(waker) = wakers.take() {
            waker.wake();
        }
    }

    /// Returns `true`if the message was sent successfully.
    async fn send_to_active(&mut self, region: u16, msg: ActiveDerpMessage) -> bool {
        match self.active_derp.get(&region) {
            Some((s, _)) => match s.send(msg).await {
                Ok(_) => true,
                Err(mpsc::error::SendError(_)) => {
                    self.close_derp(region, "sender-closed").await;
                    false
                }
            },
            None => false,
        }
    }

    /// Connect to the given derp region.
    async fn connect_derp(
        &mut self,
        region_id: u16,
        peer: Option<&PublicKey>,
    ) -> derp::http::Client {
        // See if we have a connection open to that DERP node ID first. If so, might as
        // well use it. (It's a little arbitrary whether we use this one vs. the reverse route
        // below when we have both.)

        {
            let (os, or) = oneshot::channel();
            if self
                .send_to_active(region_id, ActiveDerpMessage::GetClient(os))
                .await
            {
                if let Ok(client) = or.await {
                    return client;
                }
            }
        }

        // If we don't have an open connection to the peer's home DERP
        // node, see if we have an open connection to a DERP node
        // where we'd heard from that peer already. For instance,
        // perhaps peer's home is Frankfurt, but they dialed our home DERP
        // node in SF to reach us, so we can reply to them using our
        // SF connection rather than dialing Frankfurt.
        if let Some(peer) = peer {
            for region_id in self
                .active_derp
                .keys()
                .copied()
                .collect::<Vec<_>>()
                .into_iter()
            {
                let (os, or) = oneshot::channel();
                if self
                    .send_to_active(region_id, ActiveDerpMessage::GetPeerRoute(*peer, os))
                    .await
                {
                    if let Ok(Some(client)) = or.await {
                        return client;
                    }
                }
            }
        }

        let why = if let Some(peer) = peer {
            format!("{peer:?}")
        } else {
            "home-keep-alive".to_string()
        };
        info!("adding connection to derp-{region_id} for {why}");

        let my_derp = self.conn.my_derp();
        let conn1 = self.conn.clone();
        let ipv6_reported = self.conn.ipv6_reported.clone();

        // building a client does not dial
        let (dc, dc_receiver) = derp::http::ClientBuilder::new()
            .address_family_selector(move || {
                let ipv6_reported = ipv6_reported.clone();
                Box::pin(async move { ipv6_reported.load(Ordering::Relaxed) })
            })
            .can_ack_pings(true)
            .is_preferred(my_derp == region_id)
            .get_region(move || {
                let conn = conn1.clone();
                Box::pin(async move {
                    if conn.is_closing() {
                        // We're closing anyway; return to stop dialing.
                        return None;
                    }
                    conn.get_derp_region(region_id).await
                })
            })
            .build(self.conn.secret_key.clone())
            .expect("will only fail is a `get_region` callback is not supplied");

        let (s, r) = mpsc::channel(64);

        let c = dc.clone();
        let msg_sender = self.msg_sender.clone();
        let handle = tokio::task::spawn(async move {
            let ad = ActiveDerp {
                c: c.clone(),
                last_write: Instant::now(),
                reader: ReaderState::new(region_id, c, dc_receiver),
                inbox: r,
                msg_sender,
                derp_routes: Default::default(),
            };

            ad.run().await;
        });

        // Insert, to make sure we do not attempt to double connect.
        self.active_derp.insert(region_id, (s, handle));

        inc!(MagicsockMetrics, num_derp_conns_added);

        self.log_active_derp();

        if let Some(ref f) = self.conn.on_derp_active {
            // TODO: spawn
            f();
        }

        dc
    }

    /// Called in response to a rebind, closes all DERP connections that don't have a local address in okay_local_ips
    /// and pings all those that do.
    async fn maybe_close_derps_on_rebind(&mut self, okay_local_ips: &[IpAddr]) {
        let mut tasks = Vec::new();
        for region_id in self
            .active_derp
            .keys()
            .copied()
            .collect::<Vec<_>>()
            .into_iter()
        {
            let (os, or) = oneshot::channel();
            let la = if self
                .send_to_active(region_id, ActiveDerpMessage::GetLocalAddr(os))
                .await
            {
                match or.await {
                    Ok(None) | Err(_) => {
                        tasks.push((region_id, "rebind-no-localaddr"));
                        continue;
                    }
                    Ok(Some(la)) => la,
                }
            } else {
                tasks.push((region_id, "rebind-no-localaddr"));
                continue;
            };

            if !okay_local_ips.contains(&la.ip()) {
                tasks.push((region_id, "rebind-default-route-change"));
                continue;
            }

            // TODO: this currently will block, need to figure out how to avoid that
            let ping_success = time::timeout(Duration::from_secs(3), async {
                let (os, or) = oneshot::channel();
                if self
                    .send_to_active(region_id, ActiveDerpMessage::Ping(os))
                    .await
                {
                    or.await.is_ok()
                } else {
                    false
                }
            })
            .await
            .unwrap_or(false);

            if !ping_success {
                tasks.push((region_id, "rebind-ping-fail"));
                return;
            }
            debug!("post-rebind ping of DERP region {} okay", region_id);
        }

        for (region_id, why) in tasks {
            self.close_or_reconnect_derp(region_id, why).await;
        }

        self.log_active_derp();
    }

    /// Closes the DERP connection to the provided `region_id` and starts reconnecting it if it's
    /// our current home DERP.
    async fn close_or_reconnect_derp(&mut self, region_id: u16, why: &'static str) {
        self.close_derp(region_id, why).await;
        if self.conn.my_derp() == region_id {
            self.connect_derp(region_id, None).await;
        }
    }

    async fn clean_stale_derp(&mut self) {
        debug!("cleanup {} derps", self.active_derp.len());
        let now = Instant::now();

        let mut to_close = Vec::new();
        for (i, (s, _)) in &self.active_derp {
            if *i == self.conn.my_derp() {
                continue;
            }
            let (os, or) = oneshot::channel();
            match s.send(ActiveDerpMessage::GetLastWrite(os)).await {
                Ok(_) => match or.await {
                    Ok(last_write) => {
                        if last_write.duration_since(now) > DERP_INACTIVE_CLEANUP_TIME {
                            to_close.push(*i);
                        }
                    }
                    Err(_) => {
                        to_close.push(*i);
                    }
                },
                Err(_) => {
                    to_close.push(*i);
                }
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

    async fn close_derp(&mut self, region_id: u16, why: &'static str) {
        if let Some((s, t)) = self.active_derp.remove(&region_id) {
            debug!("closing connection to derp-{} ({:?})", region_id, why,);

            s.send(ActiveDerpMessage::Shutdown).await.ok();
            t.await.ok(); // ensure the task is shutdown

            inc!(MagicsockMetrics, num_derp_conns_removed);
        }
    }

    fn log_active_derp(&self) {
        debug!("{} active derp conns{}", self.active_derp.len(), {
            let mut s = String::new();
            if !self.active_derp.is_empty() {
                s += ":";
                for node in self.active_derp_sorted() {
                    s += &format!(" derp-{}", node,);
                }
            }
            s
        });
    }

    fn active_derp_sorted(&self) -> impl Iterator<Item = u16> {
        let mut ids: Vec<_> = self.active_derp.keys().copied().collect();
        ids.sort();

        ids.into_iter()
    }
}

#[derive(derive_more::Debug)]
pub(super) struct DerpReadResult {
    pub(super) region_id: u16,
    pub(super) src: PublicKey,
    /// packet data
    #[debug(skip)]
    pub(super) buf: Bytes,
}

/// Manages reading state for a single derp connection.
#[derive(Debug)]
struct ReaderState {
    region: u16,
    derp_client: derp::http::Client,
    derp_client_receiver: derp::http::ClientReceiver,
    /// The set of senders we know are present on this connection, based on
    /// messages we've received from the server.
    peer_present: HashSet<PublicKey>,
    backoff: backoff::exponential::ExponentialBackoff<backoff::SystemClock>,
    last_packet_time: Option<Instant>,
    last_packet_src: Option<PublicKey>,
}

#[derive(Debug)]
pub(super) enum ReadResult {
    Yield(DerpReadResult),
    Break,
    Continue,
}

#[derive(Debug)]
pub(super) enum ReadAction {
    None,
    RemovePeerRoutes { peers: Vec<PublicKey> },
    AddPeerRoute { peer: PublicKey },
}

impl ReaderState {
    fn new(
        region: u16,
        derp_client: derp::http::Client,
        derp_client_receiver: derp::http::ClientReceiver,
    ) -> Self {
        ReaderState {
            region,
            derp_client,
            derp_client_receiver,
            peer_present: HashSet::new(),
            backoff: backoff::exponential::ExponentialBackoffBuilder::new()
                .with_initial_interval(Duration::from_millis(10))
                .with_max_interval(Duration::from_secs(5))
                .build(),
            last_packet_time: None,
            last_packet_src: None,
        }
    }

    async fn recv(&mut self) -> (ReadResult, ReadAction) {
        let msg = self.derp_client_receiver.recv().await;
        trace!(region_id=%self.region, ?msg, "derp.recv received");

        match msg {
            None => (ReadResult::Continue, ReadAction::None),
            Some(Err(err)) => {
                debug!(
                    "[{:?}] derp.recv(derp-{}): {:?}",
                    self.derp_client, self.region, err
                );

                // Forget that all these peers have routes.
                let peers = self.peer_present.drain().collect();
                let action = ReadAction::RemovePeerRoutes { peers };

                if matches!(
                    err,
                    derp::http::ClientError::Closed | derp::http::ClientError::IPDisabled
                ) {
                    // drop client
                    return (ReadResult::Break, action);
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
                        (ReadResult::Continue, action)
                    }
                    None => (ReadResult::Break, action),
                }
            }
            Some(Ok((msg, conn_gen))) => {
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
                        (ReadResult::Continue, ReadAction::None)
                    }
                    derp::ReceivedMessage::ReceivedPacket { source, data } => {
                        trace!("[DERP] <- {} ({}b)", self.region, data.len());
                        // If this is a new sender we hadn't seen before, remember it and
                        // register a route for this peer.
                        let action = if self.last_packet_src.is_none()
                            || &source != self.last_packet_src.as_ref().unwrap()
                        {
                            // avoid map lookup w/ high throughput single peer
                            self.last_packet_src = Some(source);
                            if !self.peer_present.contains(&source) {
                                self.peer_present.insert(source);
                                ReadAction::AddPeerRoute { peer: source }
                            } else {
                                ReadAction::None
                            }
                        } else {
                            ReadAction::None
                        };

                        let res = DerpReadResult {
                            region_id: self.region,
                            src: source,
                            buf: data,
                        };
                        (ReadResult::Yield(res), action)
                    }
                    derp::ReceivedMessage::Ping(data) => {
                        // Best effort reply to the ping.
                        let dc = self.derp_client.clone();
                        let region = self.region;
                        tokio::task::spawn(async move {
                            if let Err(err) = dc.send_pong(data).await {
                                info!("derp-{} send_pong error: {:?}", region, err);
                            }
                        });
                        (ReadResult::Continue, ReadAction::None)
                    }
                    derp::ReceivedMessage::Health { .. } => {
                        (ReadResult::Continue, ReadAction::None)
                    }
                    derp::ReceivedMessage::PeerGone(key) => {
                        let read_action = ReadAction::RemovePeerRoutes { peers: vec![key] };

                        (ReadResult::Continue, read_action)
                    }
                    _ => {
                        // Ignore.
                        (ReadResult::Continue, ReadAction::None)
                    }
                }
            }
        }
    }
}

/// Combines blobs into packets of at most MAX_PACKET_SIZE.
///
/// Each item in a packet has a little-endian 2-byte length prefix.
pub(super) struct PacketizeIter<I: Iterator, const N: usize> {
    iter: std::iter::Peekable<I>,
    buffer: BytesMut,
}

impl<I: Iterator, const N: usize> PacketizeIter<I, N> {
    /// Create a new new PacketizeIter from something that can be turned into an
    /// iterator of slices, like a `Vec<Bytes>`.
    pub(super) fn new(iter: impl IntoIterator<IntoIter = I>) -> Self {
        Self {
            iter: iter.into_iter().peekable(),
            buffer: BytesMut::with_capacity(N),
        }
    }
}

impl<I: Iterator, const N: usize> Iterator for PacketizeIter<I, N>
where
    I::Item: AsRef<[u8]>,
{
    type Item = Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        use bytes::BufMut;
        while let Some(next_bytes) = self.iter.peek() {
            let next_bytes = next_bytes.as_ref();
            assert!(next_bytes.len() + 2 <= N);
            let next_length: u16 = next_bytes.len().try_into().expect("items < 64k size");
            if self.buffer.len() + next_bytes.len() + 2 > N {
                break;
            }
            self.buffer.put_u16_le(next_length);
            self.buffer.put_slice(next_bytes);
            self.iter.next();
        }
        if !self.buffer.is_empty() {
            Some(self.buffer.split().freeze())
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_packetize_iter() {
        let empty_vec: Vec<Bytes> = Vec::new();
        let mut iter = PacketizeIter::<_, MAX_PACKET_SIZE>::new(empty_vec);
        assert_eq!(None, iter.next());

        let single_vec = vec!["Hello"];
        let iter = PacketizeIter::<_, MAX_PACKET_SIZE>::new(single_vec);
        let result = iter.collect::<Vec<_>>();
        assert_eq!(1, result.len());
        assert_eq!(&[5, 0, b'H', b'e', b'l', b'l', b'o'], &result[0][..]);

        let spacer = vec![0u8; MAX_PACKET_SIZE - 10];
        let multiple_vec = vec![&b"Hello"[..], &spacer, &b"World"[..]];
        let iter = PacketizeIter::<_, MAX_PACKET_SIZE>::new(multiple_vec);
        let result = iter.collect::<Vec<_>>();
        assert_eq!(2, result.len());
        assert_eq!(&[5, 0, b'H', b'e', b'l', b'l', b'o'], &result[0][..7]);
        assert_eq!(&[5, 0, b'W', b'o', b'r', b'l', b'd'], &result[1][..]);
    }
}
