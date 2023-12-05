//! Handle downloading blobs and collections concurrently and from nodes.
//!
//! The [`Downloader`] interacts with four main components to this end.
//! - [`Dialer`]: Used to queue opening connections to nodes we need to perform downloads.
//! - [`ProviderMap`]: Where the downloader obtains information about nodes that could be
//!   used to perform a download.
//! - [`Store`]: Where data is stored.
//!
//! Once a download request is received, the logic is as follows:
//! 1. The [`ProviderMap`] is queried for nodes. From these nodes some are selected
//!    prioritizing connected nodes with lower number of active requests. If no useful node is
//!    connected, or useful connected nodes have no capacity to perform the request, a connection
//!    attempt is started using the [`Dialer`].
//! 2. The download is queued for processing at a later time. Downloads are not performed right
//!    away. Instead, they are initially delayed to allow the node to obtain the data itself, and
//!    to wait for the new connection to be established if necessary.
//! 3. Once a request is ready to be sent after a delay (initial or for a retry), the preferred
//!    node is used if available. The request is now considered active.
//!
//! Concurrency is limited in different ways:
//! - *Total number of active request:* This is a way to prevent a self DoS by overwhelming our own
//!   bandwidth capacity. This is a best effort heuristic since it doesn't take into account how
//!   much data we are actually requesting or receiving.
//! - *Total number of connected nodes:* Peer connections are kept for a longer time than they are
//!   strictly needed since it's likely they will be useful soon again.
//! - *Requests per node*: to avoid overwhelming nodes with requests, the number of concurrent
//!   requests to a single node is also limited.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use futures::{future::LocalBoxFuture, FutureExt, StreamExt};
use iroh_bytes::{store::Store, TempTag};
use iroh_gossip::net::util::Timers;
use iroh_net::{MagicEndpoint, NodeId};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
};
use tokio_util::{sync::CancellationToken, task::LocalPoolHandle};
use tracing::{debug, error_span, trace, warn, Instrument};

mod get;
mod invariants;
mod state;
mod test;

use self::state::{
    ConcurrencyLimits, InEvent, IntentId, OutEvent, State, Timer, Transfer, TransferId,
};
pub use self::state::{Group, NodeHints, Resource, ResourceHints, ResourceKind};

/// Number of retries initially assigned to a request.
const INITIAL_RETRY_COUNT: u8 = 4;
/// Duration for which we keep nodes connected after they were last useful to us.
const IDLE_PEER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// Capacity of the channel used to comunicate between the [`Downloader`] and the [`Service`].
const SERVICE_CHANNEL_CAPACITY: usize = 128;

/// Download identifier.
// Mainly for readability.
pub type Id = u64;

/// Trait modeling a dialer. This allows for IO-less testing.
pub trait Dialer:
    futures::Stream<Item = (NodeId, anyhow::Result<Self::Connection>)> + Unpin
{
    /// Type of connections returned by the Dialer.
    type Connection: Clone;
    /// Dial a node.
    fn queue_dial(&mut self, node_id: NodeId);
    /// Get the number of dialing nodes.
    fn pending_count(&self) -> usize;
    /// Check if a node is being dialed.
    fn is_pending(&self, node: &NodeId) -> bool;
}

/// Signals what should be done with the request when it fails.
#[derive(Debug)]
pub enum FailureAction {
    /// An error ocurred that prevents the request from being retried at all.
    AbortRequest(anyhow::Error),
    /// An error occurred that suggests the node should not be used in general.
    DropPeer(anyhow::Error),
    /// An error occurred in which neither the node nor the request are at fault.
    RetryLater(anyhow::Error),
    /// The peer doesn't have the requested content
    NotFound,
    /// We cancelled the transfer.
    Cancelled,
}

/// Future of a get request.
type GetFut = LocalBoxFuture<'static, Result<TempTag, FailureAction>>;

/// Trait modelling performing a single request over a connection. This allows for IO-less testing.
pub trait Getter {
    /// Type of connections the Getter requires to perform a download.
    type Connection;
    /// Return a future that performs the download using the given connection.
    fn get(&mut self, resource: Resource, conn: Self::Connection) -> GetFut;
}

// For readability. In the future we might care about some data reporting on a successful download
// or kind of failure in the error case.
type DownloadResult = Result<TempTag, DownloadError>;

/// Error returned when a resource could not be downloaded.
#[derive(Debug, Clone, thiserror::Error)]
pub enum DownloadError {
    /// Failed to download from any provider
    #[error("Failed to download resource")]
    DownloadFailed,
    /// Failed to receive response from service.
    #[error("Failed to receive response from download service")]
    ActorDied,
}

/// Handle to interact with a download request.
#[derive(Debug)]
pub struct DownloadHandle {
    /// Id used to identify the request in the [`Downloader`].
    id: Id,
    /// Kind of download.
    resource: Resource,
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
            Ready(Err(_recv_err)) => Ready(Err(DownloadError::ActorDied)),
            Pending => Pending,
        }
    }
}

/// Handle for the download services.
#[derive(Clone, Debug)]
pub struct Downloader {
    /// Next id to use for a download intent.
    next_id: Arc<AtomicU64>,
    /// Channel to communicate with the service.
    msg_tx: mpsc::Sender<Message>,
}

impl Downloader {
    /// Create a new Downloader.
    pub fn new<S>(store: S, endpoint: MagicEndpoint, rt: LocalPoolHandle) -> Self
    where
        S: Store,
    {
        let me = endpoint.node_id().fmt_short();
        let (msg_tx, msg_rx) = mpsc::channel(SERVICE_CHANNEL_CAPACITY);
        let dialer = iroh_gossip::net::util::Dialer::new(endpoint);

        let create_future = move || {
            let concurrency_limits = ConcurrencyLimits::default();
            let getter = get::IoGetter { store };

            let service = Service::new(getter, dialer, concurrency_limits, msg_rx);

            service.run().instrument(error_span!("downloader", %me))
        };
        rt.spawn_pinned(create_future);
        Self {
            next_id: Arc::new(AtomicU64::new(0)),
            msg_tx,
        }
    }

    /// Queue a download.
    pub async fn queue(&mut self, resource: Resource, hints: ResourceHints) -> DownloadHandle {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);

        let (sender, receiver) = oneshot::channel();
        let handle = DownloadHandle {
            id,
            resource,
            receiver,
        };
        let msg = Message::AddResource {
            resource,
            id,
            sender,
            hints,
        };
        // if this fails polling the handle will fail as well since the sender side of the oneshot
        // will be dropped
        if let Err(send_err) = self.msg_tx.send(msg).await {
            let msg = send_err.0;
            debug!(?msg, "download not sent");
        }
        handle
    }

    /// Cancel a download.
    // NOTE: receiving the handle ensures an intent can't be cancelled twice
    pub async fn cancel(&mut self, handle: DownloadHandle) {
        let DownloadHandle {
            id,
            resource,
            receiver: _,
        } = handle;
        let msg = Message::Cancel { id, resource };
        if let Err(send_err) = self.msg_tx.send(msg).await {
            let msg = send_err.0;
            debug!(?msg, "cancel not sent");
        }
    }

    /// Declare that certains nodes can be used to download a hash.
    pub async fn add_node(&mut self, node: NodeId, hints: NodeHints) {
        let msg = Message::AddNode { node, hints };
        if let Err(send_err) = self.msg_tx.send(msg).await {
            let msg = send_err.0;
            debug!(?msg, "nodes have not been sent")
        }
    }
}

/// Messages the service can receive.
#[derive(derive_more::Debug)]
enum Message {
    /// Queue a download intent.
    AddResource {
        resource: Resource,
        id: Id,
        #[debug(skip)]
        sender: oneshot::Sender<DownloadResult>,
        hints: ResourceHints,
    },
    AddNode {
        node: NodeId,
        hints: NodeHints,
    },
    /// Cancel an intent. The associated request will be cancelled when the last intent is
    /// cancelled.
    Cancel {
        id: Id,
        resource: Resource,
    },
}

/// Information about a request being processed.
#[derive(derive_more::Debug, Default)]
struct ActiveRequestInfo {
    /// Ids of intents associated with this request.
    #[debug("{:?}", intents.keys().collect::<Vec<_>>())]
    intents: HashMap<Id, oneshot::Sender<DownloadResult>>,
}

/// Type that is returned from a download request.
type DownloadRes = (TransferId, Result<TempTag, FailureAction>);

#[derive(Debug)]
struct Service<G: Getter, D: Dialer> {
    /// The getter performs individual requests.
    getter: G,
    /// Dialer to get connections for required nodes.
    dialer: D,
    /// Channel to receive messages from the service's handle.
    msg_rx: mpsc::Receiver<Message>,
    /// Active connections
    conns: HashMap<NodeId, D::Connection>,
    /// Requests performed for download intents. Two download requests can produce the same
    /// request. This map allows deduplication of efforts.
    current_requests: HashMap<Resource, ActiveRequestInfo>,
    active_transfers: HashMap<TransferId, ActiveTransfer>,
    /// Downloads underway.
    active_transfers_futs: JoinSet<DownloadRes>,
    /// State
    state: State,
    /// Timers
    timers: Timers<Timer>,
}

#[derive(Debug)]
struct ActiveTransfer {
    transfer: Transfer,
    cancel: CancellationToken,
}

impl<G: Getter<Connection = D::Connection>, D: Dialer> Service<G, D> {
    fn new(
        getter: G,
        dialer: D,
        concurrency_limits: ConcurrencyLimits,
        msg_rx: mpsc::Receiver<Message>,
    ) -> Self {
        Service {
            getter,
            dialer,
            msg_rx,
            current_requests: HashMap::default(),
            active_transfers: HashMap::default(),
            active_transfers_futs: Default::default(),
            state: State::new(concurrency_limits),
            timers: Default::default(),
            conns: Default::default(),
        }
    }

    /// Main loop for the service.
    async fn run(mut self) {
        loop {
            tokio::select! {
                Some((node, conn_result)) = self.dialer.next() => {
                    trace!("tick: connection ready");
                    self.on_connection_ready(node, conn_result);
                }
                maybe_msg = self.msg_rx.recv() => {
                    trace!(msg=?maybe_msg, "tick: message received");
                    match maybe_msg {
                        Some(msg) => self.handle_message(msg),
                        None => return self.shutdown().await,
                    }
                }
                Some(res) = self.active_transfers_futs.join_next() => {
                    match res {
                        Ok((transfer_id, result)) => {
                            trace!("tick: download completed");
                            self.on_download_completed(transfer_id, result);
                        }
                        Err(e) => {
                            warn!("download issue: {:?}", e);
                        }
                    }
                }
                drain = self.timers.wait_and_drain() => {
                    trace!("tick: timer ready");
                    for (_instant, timer) in drain {
                        self.state.handle(InEvent::TimerExpired { timer });
                    }
                }
            }

            self.perform_actions();
        }
    }

    /// Handle receiving a [`Message`].
    fn handle_message(&mut self, msg: Message) {
        match msg {
            Message::AddResource {
                resource,
                hints,
                id,
                sender,
            } => {
                self.state.handle(InEvent::AddResource {
                    resource,
                    hints,
                    intent: IntentId(id),
                });
                let info = self.current_requests.entry(resource).or_default();
                info.intents.insert(id, sender);
            }
            Message::AddNode { node, hints } => {
                self.state.handle(InEvent::AddNode { node, hints });
            }
            Message::Cancel { id, resource } => {
                self.state.handle(InEvent::CancelIntent {
                    resource,
                    intent: IntentId(id),
                });
            }
        }
    }

    fn perform_actions(&mut self) {
        let actions = self.state.events();
        // TODO: Can we avoid the alloc? We have a mutable borrow on state...
        let actions: Vec<_> = actions.collect();
        for action in actions.into_iter() {
            debug!("perform action: {action:?}");
            match action {
                OutEvent::StartTransfer(transfer) => self.start_download(transfer),
                OutEvent::StartConnect(node) => self.dialer.queue_dial(node),
                OutEvent::RegisterTimer(duration, timer) => self.timers.insert(
                    Instant::now()
                        .checked_add(duration)
                        .expect("duration is too long"),
                    timer,
                ),
                OutEvent::DropConnection(node) => {
                    let _ = self.conns.remove(&node);
                }
                OutEvent::CancelTransfer(id) => {
                    self.on_cancel(id);
                }
                OutEvent::ResourceFailed(resource) => {
                    self.finalize_intents(&resource, Err(DownloadError::DownloadFailed))
                }
            }
        }
    }

    fn on_cancel(&mut self, transfer_id: TransferId) {
        let Some(transfer) = self.active_transfers.get(&transfer_id) else {
            warn!(?transfer_id, "cancelled download not in current_requests");
            debug_assert!(false, "cancelled download not in current_requests");
            return;
        };
        transfer.cancel.cancel();
    }

    /// Handle receiving a new connection.
    fn on_connection_ready(&mut self, node: NodeId, result: anyhow::Result<D::Connection>) {
        match result {
            Ok(connection) => {
                trace!(%node, "connected to node");
                self.conns.insert(node, connection);
                self.state.handle(InEvent::NodeConnected { node });
            }
            Err(err) => {
                debug!(%node, %err, "connection to node failed");
                self.state.handle(InEvent::NodeFailed { node });
            }
        }
    }

    fn on_download_completed(&mut self, id: TransferId, result: Result<TempTag, FailureAction>) {
        // first remove the request
        let Some(transfer) = self.active_transfers.remove(&id) else {
            warn!(?id, ?result, "finished transfer not in active_transfer");
            debug_assert!(false, "finished transfer not in active_transfers");
            return;
        };

        match result {
            Ok(temp_tag) => {
                // The transfer is finished, finalize and remove.
                self.state.handle(InEvent::TransferReady { id });
                self.finalize_intents(&transfer.transfer.resource, Ok(temp_tag));
            }
            Err(failure) => {
                // The transfer failed. Inform state but do not remove yet because there's still a
                // possiblity for it to succeed. It will be removed in on_cancel.
                self.state.handle(InEvent::TransferFailed { id, failure });
            }
        }
    }

    fn finalize_intents(&mut self, resource: &Resource, res: DownloadResult) {
        let Some(info) = self.current_requests.remove(&resource) else {
            warn!(?resource, ?res, "finished transfer has no intent info");
            debug_assert!(false, "finished transfer has no intent info");
            return;
        };
        for sender in info.intents.into_values() {
            let _ = sender.send(res.clone());
        }
    }

    /// Start downloading from the given node.
    fn start_download(&mut self, transfer: Transfer) {
        let Transfer { id, resource, node } = transfer;
        debug!(?id, node = %node.fmt_short(), ?resource, "starting download");
        let cancellation = CancellationToken::new();
        let Some(conn) = self.conns.get(&node) else {
            warn!(?transfer, "starting download while node not connected");
            return;
        };
        let state = ActiveTransfer {
            transfer,
            cancel: cancellation.clone(),
        };
        let get_fut = self.getter.get(resource, conn.clone());
        let fut = async move {
            // NOTE: it's an open question if we should do timeouts at this point. Considerations from @Frando:
            // > at this stage we do not know the size of the download, so the timeout would have
            // > to be so large that it won't be useful for non-huge downloads. At the same time,
            // > this means that a super slow node would block a download from succeeding for a long
            // > time, while faster nodes could be readily available.
            // As a conclusion, timeouts should be added only after downloads are known to be bounded
            let res = tokio::select! {
                _ = cancellation.cancelled() => Err(FailureAction::Cancelled),
                res = get_fut => res
            };

            (id, res)
        };
        self.active_transfers.insert(id, state);
        self.active_transfers_futs.spawn_local(fut);
    }

    #[allow(clippy::unused_async)]
    async fn shutdown(self) {
        debug!("shutting down");
        // TODO(@divma): how to make sure the download futures end gracefully?
    }
}

impl Dialer for iroh_gossip::net::util::Dialer {
    type Connection = quinn::Connection;

    fn queue_dial(&mut self, node_id: NodeId) {
        self.queue_dial(node_id, &iroh_bytes::protocol::ALPN)
    }

    fn pending_count(&self) -> usize {
        self.pending_count()
    }

    fn is_pending(&self, node: &NodeId) -> bool {
        self.is_pending(node)
    }
}
