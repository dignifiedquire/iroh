//! Handle downloading blobs and collections concurrently and from peers.
//!
//! The [`Service`] interacts with four main components to this end.
//! - [`Dialer`]: Used to queue opening connections to peers we need to perform downloads.
//! - [`AvailabilityRegistry`]: Where the downloader obtains information about peers that could be
//!   used to perform a download.
//! - [`Store`]: Where data is stored.
//! - [`CollectionParser`]: Used by the [`GetRequest`] associated logic to identify blobs encoding
//!   collections.
//!
//! Once a download request is received, the logic is as follows:
//! 1. The [`AvailabilityRegistry`] is queried for peers. From these peers some are selected
//!    prioritizing connected peers with lower number of active requests. If no useful peer is
//!    connected, or useful connected peers have no capacity to perform the request, a connection
//!    attempt is started using the [`Dialer`].
//! 2. The download is queued for processing at a later time. Downloads are not performed right
//!    away. Instead, they are initially delayed to allow the peer to obtain the data itself, and
//!    to wait for the new connection to be established if necessary.
//! 3. Once a request is ready to be sent after a delay (initial or for a retry), the preferred
//!    peer is used if available. The request is now considered active.
//!
//! Concurrency is limited in different ways:
//! - *Total number of active request:* This is a way to prevent a self DoS by overwhelming our own
//!   bandwidth capacity. This is a best effort heuristic since it doesn't take into account how
//!   much data we are actually requesting or receiving.
//! - *Total number of connected peers:* Peer connections are kept for a longer time than they are
//!   strictly needed since it's likely they will be useful soon again.
//! - *Requests per peer*: to avoid overwhelming peers with requests, the number of concurrent
//!   requests to a single peer is also limited.

#![allow(clippy::all, unused, missing_docs)]

use std::collections::{hash_map::Entry, HashMap};

use futures::{
    future::{BoxFuture, LocalBoxFuture},
    stream::FuturesUnordered,
    FutureExt, StreamExt, TryFutureExt,
};
use iroh_bytes::{
    baomap::{range_collections::RangeSet2, Store},
    collection::CollectionParser,
    protocol::{RangeSpec, RangeSpecSeq},
    util::progress::IgnoreProgressSender,
    Hash,
};
use iroh_gossip::net::util::Dialer;
use iroh_net::key::PublicKey;
use tokio::sync::{mpsc, oneshot};
use tokio_util::{sync::CancellationToken, time::delay_queue};
use tracing::{debug, error, info, trace, warn};

use self::get::FailureAction;

mod get;
mod test;

/// Delay added to a request when it's first received.
const INITIAL_REQUEST_DELAY: std::time::Duration = std::time::Duration::from_millis(500);
/// Number of retries initially assigned to a request.
const INITIAL_RETRY_COUNT: u8 = 4;

/// Download identifier.
// Mainly for readability.
pub type Id = u64;

pub trait AvailabilityRegistry {
    type CandidateIter<'a>: Iterator<Item = &'a PublicKey>
    where
        Self: 'a;
    fn get_candidates(&self, hash: &Hash) -> Self::CandidateIter<'_>;
}

/// Concurrency limits for the [`Service`].
#[derive(Debug)]
pub struct ConcurrencyLimits {
    /// Maximum number of requests the service performs concurrently.
    max_concurrent_requests: usize,
    /// Maximum number of requests performed by a single peer concurrently.
    max_concurrent_requests_per_peer: usize,
    /// Maximum number of open connections the service maintains.
    max_open_connections: usize,
}

impl ConcurrencyLimits {
    fn at_requests_capacity(&self, active_requests: usize) -> bool {
        active_requests >= self.max_concurrent_requests
    }

    fn peer_at_request_capacity(&self, active_peer_requests: usize) -> bool {
        active_peer_requests >= self.max_concurrent_requests_per_peer
    }

    fn at_connections_capacity(&self, active_connections: usize) -> bool {
        active_connections >= self.max_open_connections
    }
}

/// Download requests the [`Downloader`] handles.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub enum DownloadKind {
    /// Download a single blob entirely.
    Blob {
        /// Blob to be downloaded.
        hash: Hash,
    },
    /// Download a collection entirely.
    Collection {
        /// Blob to be downloaded.
        hash: Hash,
    },
}

impl DownloadKind {
    /// Get the requested hash.
    const fn hash(&self) -> &Hash {
        match self {
            DownloadKind::Blob { hash } | DownloadKind::Collection { hash } => hash,
        }
    }

    /// Get the ranges this download is requesting.
    fn ranges(&self) -> RangeSpecSeq {
        match self {
            DownloadKind::Blob { .. } => RangeSpecSeq::from_ranges([RangeSet2::all()]),
            DownloadKind::Collection { hash } => RangeSpecSeq::all(),
        }
    }
}

// TODO(@divma): do we care about failure reason? do we care about success data reporting?
type DownloadResult = Result<(), ()>;

/// Handle to interact with a download request.
#[derive(Debug)]
pub struct DownloadHandle {
    /// Id used to identify the request in the [`Downloader`].
    id: u64,
    /// Kind of download.
    kind: DownloadKind,
    /// Receiver to retrieve the return value of this download.
    receiver: oneshot::Receiver<DownloadResult>,
}

impl std::future::Future for DownloadHandle {
    type Output = DownloadResult;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        use std::task::Poll::*;
        // make it easier on holders of the handle to poll the result, removing the receiver error
        // from the middle
        match self.receiver.poll_unpin(cx) {
            Ready(Ok(result)) => Ready(result),
            Ready(Err(_recv_err)) => Ready(Err(())),
            Pending => Pending,
        }
    }
}

#[derive(Debug)]
pub struct Downloader;

#[derive(derive_more::Debug)]
struct DownloadInfo {
    /// Kind of download.
    kind: DownloadKind,
    /// How many times can this request be attempted again before declaring it failed.
    remaining_retries: u8,
    /// Oneshot to return the download result back to the requester.
    #[debug(skip)]
    sender: oneshot::Sender<DownloadResult>,
}

enum Message {
    Queue {
        kind: DownloadKind,
        id: Id,
        sender: oneshot::Sender<DownloadResult>,
    },
    Cancel {
        id: Id,
        kind: DownloadKind,
    },
}

/// Information about a request being processed.
#[derive(derive_more::Debug)]
struct ActiveRequestInfo {
    /// Ids of intents associated with this request.
    #[debug("{:?}", intents.keys().collect::<Vec<_>>())]
    intents: HashMap<Id, oneshot::Sender<DownloadResult>>,
    /// How many times can this request be retried.
    remaining_retries: u8,
    /// Token used to cancel the future doing the request.
    #[debug(skip)]
    cancellation: CancellationToken,
    /// Peer doing this request attempt.
    peer: PublicKey,
}

/// Information about a request that has not started.
#[derive(derive_more::Debug)]
struct PendingRequestInfo {
    /// Ids of intents associated with this request.
    #[debug("{:?}", intents.keys().collect::<Vec<_>>())]
    intents: HashMap<Id, oneshot::Sender<DownloadResult>>,
    /// How many times can this request be retried.
    remaining_retries: u8,
    /// Key to manage the delay associated with this scheduled request.
    #[debug(skip)]
    delay_key: delay_queue::Key,
    /// If this attempt was scheduled with a known potential peer, this is stored here to
    /// prevent another query to the [`AvailabilityRegistry`].
    next_peer: Option<PublicKey>,
}

/// State of the connection to this peer.
#[derive(derive_more::Debug)]
struct ConnectionInfo {
    /// Connection to this peer.
    ///
    /// If this peer was deemed unusable by a request, this will be set to `None`. As a
    /// consequence, when evaluating peers for a download, this peer will not be considered.
    /// Since peers are kept for a longer time that they are strictly necessary, this acts as a
    /// temporary ban.
    #[debug(skip)]
    connection: Option<quinn::Connection>,
    /// Number of active requests this peer is performing for us.
    active_requests: usize,
}

impl ConnectionInfo {
    fn new(connection: quinn::Connection) -> ConnectionInfo {
        ConnectionInfo {
            connection: Some(connection),
            active_requests: 0,
        }
    }
}

/// Type of future that performs a download request.
type DownloadFut = LocalBoxFuture<'static, (DownloadKind, Result<(), FailureAction>)>;

#[derive(Debug)]
struct Service<S, C, R> {
    /// The store to which data is downloaded.
    store: S,
    /// Parser to identify blobs encoding collections.
    collection_parser: C,
    /// Registry to query for peers that we believe have the data we are looking for.
    availabiliy_registry: R,
    /// Dialer to get connections for required peers.
    dialer: Dialer,
    /// Limits to concurrent tasks handled by the service.
    concurrency_limits: ConcurrencyLimits,
    /// Channel to receive messages from the service's handle.
    msg_rx: mpsc::Receiver<Message>,
    /// Peers available to use and their relevant information.
    peers: HashMap<PublicKey, ConnectionInfo>,
    /// Requests performed for download intents. Two download requests can produce the same
    /// request. This map allows deduplication of efforts.
    current_requests: HashMap<DownloadKind, ActiveRequestInfo>,
    /// Requests scheduled to be downloaded at a later time.
    scheduled_requests: HashMap<DownloadKind, PendingRequestInfo>,
    /// Queue of scheduled requests.
    scheduled_request_queue: delay_queue::DelayQueue<DownloadKind>,
    /// Downloads underway.
    in_progress_downloads: FuturesUnordered<DownloadFut>,
}

impl<S: Store, C: CollectionParser, R: AvailabilityRegistry> Service<S, C, R> {
    fn new(
        store: S,
        collection_parser: C,
        availabiliy_registry: R,
        endpoint: iroh_net::MagicEndpoint,
        concurrency_limits: ConcurrencyLimits,
        msg_rx: mpsc::Receiver<Message>,
    ) -> Self {
        let dialer = Dialer::new(endpoint);
        Service {
            store,
            collection_parser,
            availabiliy_registry,
            dialer,
            concurrency_limits,
            msg_rx,
            peers: HashMap::default(),
            current_requests: HashMap::default(),
            scheduled_requests: HashMap::default(),
            scheduled_request_queue: delay_queue::DelayQueue::default(),
            in_progress_downloads: FuturesUnordered::default(),
        }
    }

    /// Main loop for the service.
    async fn run(mut self) {
        loop {
            // check if we have capacity to dequeue another scheduled request
            let at_capacity = self
                .concurrency_limits
                .at_requests_capacity(self.in_progress_downloads.len());

            tokio::select! {
                (peer, conn_result) = self.dialer.next() => {
                    trace!("tick: connection ready");
                    self.on_connection_ready(peer, conn_result);
                }
                maybe_msg = self.msg_rx.recv() => {
                    trace!("tick: message received");
                    match maybe_msg {
                        Some(msg) => self.handle_message(msg),
                        None => return self.shutdown().await,
                    }
                }
                Some((kind, result)) = self.in_progress_downloads.next() => {
                    trace!("tick: download completed");
                    self.on_download_completed(kind, result);
                }
                Some(expired) = self.scheduled_request_queue.next(), if !at_capacity => {
                    trace!("tick: scheduled request ready");
                    let kind = expired.into_inner();
                    let request_info = self.scheduled_requests.remove(&kind).expect("is registered");
                    self.on_scheduled_request_ready(kind, request_info);
                }
            }
        }
    }

    /// Handle receiving a [`Message`].
    fn handle_message(&mut self, msg: Message) {
        match msg {
            Message::Queue { kind, id, sender } => self.handle_queue_new_download(kind, id, sender),
            Message::Cancel { id, kind } => self.handle_cancel_download(id, kind),
        }
    }

    /// Handle a [`Message::Queue`].
    ///
    /// If this intent maps to a request that already exists, it will be registered with it. If the
    /// request is new it will be scheduled.
    fn handle_queue_new_download(
        &mut self,
        kind: DownloadKind,
        id: Id,
        sender: oneshot::Sender<DownloadResult>,
    ) {
        if let Some(info) = self.current_requests.get_mut(&kind) {
            // this intent maps to a download that already exists, simply register it
            info.intents.insert(id, sender);
            // increasing the retries by one accounts for multiple intents for the same request in
            // a conservative way
            info.remaining_retries += 1;
            return trace!(?kind, ?info, "intent registered with active request");
        }

        let needs_peer = self
            .scheduled_requests
            .get(&kind)
            .map(|info| info.next_peer.is_none())
            .unwrap_or(true);

        let next_peer = needs_peer
            .then(|| self.get_best_candidate(kind.hash()))
            .flatten();

        // if we are here this request is not active, check if it needs to be scheduled
        match self.scheduled_requests.get_mut(&kind) {
            Some(info) => {
                info.intents.insert(id, sender);
                // pre-emptively get a peer if we don't already have one
                if info.next_peer.is_none() {
                    info.next_peer = next_peer
                }
                trace!(?kind, ?info, "intent registered with scheduled request");
            }
            None => {
                let intents = HashMap::from([(id, sender)]);
                self.schedule_request(kind, INITIAL_RETRY_COUNT, next_peer, intents)
            }
        }
    }

    /// Gets the best candidate for a download.
    ///
    /// Peers are selected prioritizing those with an open connection and with capacity for another
    /// request, followed by peers we are currently dialing with capacity for another request.
    /// Lastly, peers not connected and not dialing are considered.
    ///
    /// If the selected candidate is not connected and we have capacity for another connection, a
    /// dial is queued.
    fn get_best_candidate(&mut self, hash: &Hash) -> Option<PublicKey> {
        /// Model the state of peers found in the candidates
        #[derive(PartialEq, Eq, Clone, Copy)]
        enum PeerState {
            Dialing,
            Connected(usize),
            NotConnected,
        }

        impl Ord for PeerState {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                // define the order of preference between candidates as follows:
                // - prefer connected peers to dialing ones
                // - prefer dialing peers to not connected ones
                // - prefer peers with less active requests when connected
                use std::cmp::Ordering::*;
                match (self, other) {
                    (PeerState::Dialing, PeerState::Dialing) => Equal,
                    (PeerState::Dialing, PeerState::Connected(_)) => Less,
                    (PeerState::Dialing, PeerState::NotConnected) => Greater,
                    (PeerState::NotConnected, PeerState::Dialing) => Less,
                    (PeerState::NotConnected, PeerState::Connected(_)) => Less,
                    (PeerState::NotConnected, PeerState::NotConnected) => Equal,
                    (PeerState::Connected(_), PeerState::Dialing) => Greater,
                    (PeerState::Connected(_), PeerState::NotConnected) => Greater,
                    (PeerState::Connected(a), PeerState::Connected(b)) => {
                        if a < b {
                            Greater
                        } else if a > b {
                            Less
                        } else {
                            Equal
                        }
                    }
                }
            }
        }

        impl PartialOrd for PeerState {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }

        // first collect suitable candidates
        let mut candidates = self
            .availabiliy_registry
            .get_candidates(hash)
            .filter_map(|peer| {
                if let Some(info) = self.peers.get(peer) {
                    if info.connection.is_none() {
                        // peer is temporarily banned, filter them out
                        return None;
                    }
                    let req_count = info.active_requests;
                    // filter out peers at capacity
                    let has_capacity = !self.concurrency_limits.peer_at_request_capacity(req_count);
                    has_capacity.then_some((peer, PeerState::Connected(req_count)))
                } else if self.dialer.is_pending(peer) {
                    Some((peer, PeerState::Dialing))
                } else {
                    Some((peer, PeerState::NotConnected))
                }
            })
            .collect::<Vec<_>>();

        candidates.sort_unstable_by_key(|peer_and_state| peer_and_state.1 /* state */);

        // this is our best peer, check if we need to dial it
        let (peer, state) = candidates.pop()?;

        if let PeerState::NotConnected = state {
            let total_conns = self.peers.len() + self.dialer.pending_count();
            if !self.concurrency_limits.at_connections_capacity(total_conns) {
                // peer is not connected, not dialing and concurrency limits allow another connection
                debug!(%peer, "dialing peer");
                self.dialer.queue_dial(*peer, &iroh_bytes::protocol::ALPN);
                Some(*peer)
            } else {
                trace!(%peer, "required peer not dialed to maintain concurrency limits");
                None
            }
        } else {
            Some(*peer)
        }
    }

    /// Cancels the download request.
    ///
    /// This removes the registered download intent and, depending on its state, it will either
    /// remove it from the scheduled requests, or cancel the future.
    fn handle_cancel_download(&mut self, id: Id, kind: DownloadKind) {
        if let Entry::Occupied(mut occupied_entry) = self.current_requests.entry(kind.clone()) {
            // remove the intent from the associated request
            let intents = &mut occupied_entry.get_mut().intents;
            intents.remove(&id);
            // if this was the last intent associated with the request cancel it
            if intents.is_empty() {
                occupied_entry.remove().cancellation.cancel();
            }
        } else if let Entry::Occupied(mut occupied_entry) = self.scheduled_requests.entry(kind) {
            // remove the intent from the associated request
            let intents = &mut occupied_entry.get_mut().intents;
            intents.remove(&id);
            // if this was the last intent associated with the request remove it from the schedule
            // queue
            if intents.is_empty() {
                let delay_key = occupied_entry.remove().delay_key;
                self.scheduled_request_queue.remove(&delay_key);
            }
        }
    }

    /// Handle receiving a new connection.
    fn on_connection_ready(&mut self, peer: PublicKey, result: anyhow::Result<quinn::Connection>) {
        match result {
            Ok(connection) => {
                trace!(%peer, "connected to peer");
                self.peers.insert(peer, ConnectionInfo::new(connection));
            }
            Err(err) => {
                debug!(%peer, %err, "connection to peer failed")
            }
        }
    }

    fn on_download_completed(&mut self, kind: DownloadKind, result: Result<(), FailureAction>) {
        // first remove the request
        let info = self
            .current_requests
            .remove(&kind)
            .expect("request was active");

        // update the active requests for this peer
        let ActiveRequestInfo {
            intents,
            peer,
            mut remaining_retries,
            ..
        } = info;

        let peer_info = self.peers.get_mut(&peer).expect("peer is connected");
        peer_info.active_requests -= 1;

        match result {
            Ok(()) => {
                debug!(%peer, ?kind, "download compleated");
                for sender in intents.into_values() {
                    let _ = sender.send(Ok(()));
                }
            }
            Err(FailureAction::AbortRequest(reason)) => {
                debug!(%peer, ?kind, %reason, "aborting request");
                for sender in intents.into_values() {
                    let _ = sender.send(Err(()));
                }
            }
            Err(FailureAction::DropPeer(reason)) => {
                debug!(%peer, ?kind, %reason, "dropping peer after failed request");
                self.peers.remove(&peer);
                // TODO(@divma): should we keep a small set of peers that should not be used for a
                // bit? so that we don't retry the same peer after disconnecting them
            }
            Err(FailureAction::RetryLater(reason)) => {
                // check if the download can be retried
                if remaining_retries > 0 {
                    debug!(%peer, ?kind, %reason, "download attempt failed");
                    remaining_retries -= 1;
                    let next_peer = self.get_best_candidate(kind.hash());
                    self.schedule_request(kind, remaining_retries, next_peer, intents);
                } else {
                    debug!(%peer, ?kind, %reason, "download failed");
                    for sender in intents.into_values() {
                        let _ = sender.send(Err(()));
                    }
                }
            }
        }
    }

    /// A scheduled request is ready to be processed.
    ///
    /// The peer that was initially selected is used if possible. Otherwise we try to get a new
    /// peer
    fn on_scheduled_request_ready(&mut self, kind: DownloadKind, info: PendingRequestInfo) {
        let PendingRequestInfo {
            intents,
            mut remaining_retries,
            next_peer,
            ..
        } = info;

        // first try with the peer that was initially assigned
        if let Some((peer, conn)) = next_peer.and_then(|peer| {
            self.get_peer_connection_for_download(&peer)
                .map(|conn| (peer, conn))
        }) {
            return self.start_download(kind, peer, conn, remaining_retries, intents);
        }

        // we either didn't have a peer or the peer is busy or dialing. In any case try to get
        // another peer
        let next_peer = match self.get_best_candidate(kind.hash()) {
            None => None,
            Some(peer) => {
                // optimistically check if the peer could do the request right away
                match self.get_peer_connection_for_download(&peer) {
                    Some(conn) => {
                        return self.start_download(kind, peer, conn, remaining_retries, intents)
                    }
                    None => Some(peer),
                }
            }
        };

        // we tried to get a peer to perform this request but didn't get one, so now this attempt
        // is failed
        if remaining_retries > 0 {
            remaining_retries -= 1;
            self.schedule_request(kind, remaining_retries, next_peer, intents);
        } else {
            // request can't be retried
            for sender in intents.into_values() {
                let _ = sender.send(Err(()));
            }
            return debug!(?kind, "download ran out of attempts");
        }
    }

    /// Start downloading from the given peer.
    fn start_download(
        &mut self,
        kind: DownloadKind,
        peer: PublicKey,
        conn: quinn::Connection,
        remaining_retries: u8,
        intents: HashMap<Id, oneshot::Sender<DownloadResult>>,
    ) {
        debug!(%peer, ?kind, "starting download");
        let cancellation = CancellationToken::new();
        let info = ActiveRequestInfo {
            intents,
            remaining_retries,
            cancellation,
            peer,
        };
        let cancellation = info.cancellation.clone();
        self.current_requests.insert(kind.clone(), info);

        let store = self.store.clone();
        let collection_parser = self.collection_parser.clone();

        let fut = async move {
            let get = match kind {
                DownloadKind::Blob { hash } => {
                    get::get(&store, &collection_parser, conn, hash, false)
                }
                DownloadKind::Collection { hash } => {
                    get::get(&store, &collection_parser, conn, hash, true)
                }
            };

            // TODO(@divma): timeout?
            let res = tokio::select! {
                _ = cancellation.cancelled() => Err(FailureAction::AbortRequest(anyhow::anyhow!("cancelled"))),
                res = get => res
            };

            // TODO: use stats for metrics
            (kind, res.and_then(|_stats| Ok(())))
        };
        self.in_progress_downloads.push(fut.boxed_local());
    }

    /// Schedule a request for later processing.
    fn schedule_request(
        &mut self,
        kind: DownloadKind,
        remaining_retries: u8,
        next_peer: Option<PublicKey>,
        intents: HashMap<Id, oneshot::Sender<DownloadResult>>,
    ) {
        // this is simply INITIAL_REQUEST_DELAY * attempt_num where attempt_num (as an ordinal
        // number) is maxed at INITIAL_RETRY_COUNT
        let delay = INITIAL_REQUEST_DELAY
            * (INITIAL_RETRY_COUNT.saturating_sub(remaining_retries) as u32 + 1);
        let delay_key = self.scheduled_request_queue.insert(kind.clone(), delay);

        let info = PendingRequestInfo {
            intents,
            remaining_retries,
            delay_key,
            next_peer,
        };
        debug!(?kind, ?info, "request scheduled");
        self.scheduled_requests.insert(kind, info);
    }

    /// Gets the [`quinn::Connection`] for a peer if it's connected an has capacity for another
    /// request. In this case, the count of active requests for the peer is incremented.
    fn get_peer_connection_for_download(&mut self, peer: &PublicKey) -> Option<quinn::Connection> {
        let info = self.peers.get_mut(peer)?;
        let connection = info.connection.as_ref()?;
        // check if the peer can be sent another request
        if !self
            .concurrency_limits
            .peer_at_request_capacity(info.active_requests)
        {
            info.active_requests += 1;
            Some(connection.clone())
        } else {
            None
        }
    }

    async fn shutdown(mut self) {
        debug!("shutting down");
        // TODO(@divma): how to make sure the download futures end gracefully?
    }
}
