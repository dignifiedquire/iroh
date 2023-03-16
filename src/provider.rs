//! Provider API
//!
//! A provider is a server that serves content-addressed data (blobs or collections).
//! To create a provider, create a database using [`create_collection`], then build a
//! provider using [`Builder`] and spawn it using [`Builder::spawn`].
//!
//! You can monitor what is happening in the provider using [`Provider::subscribe`].
//!
//! To shut down the provider, call [`Provider::shutdown`].
use std::fmt::{self, Display};
use std::future::Future;
use std::io::{BufReader, Read};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::RwLock;
use std::task::Poll;
use std::time::Duration;
use std::{collections::HashMap, sync::Arc};

use abao::encode::SliceExtractor;
use anyhow::{ensure, Context, Result};
use bytes::{Bytes, BytesMut};
use futures::future;
use futures::Stream;
use postcard::experimental::max_size::MaxSize;
use quic_rpc::server::RpcChannel;
use quic_rpc::transport::flume::FlumeConnection;
use quic_rpc::transport::misc::DummyServerEndpoint;
use quic_rpc::{RpcClient, RpcServer, ServiceConnection, ServiceEndpoint};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::broadcast;
use tokio::task::{JoinError, JoinHandle};
use tokio_util::io::SyncIoBridge;
use tokio_util::sync::CancellationToken;
use tracing::{debug, debug_span, warn};
use tracing_futures::Instrument;

use crate::blobs::{Blob, Collection};
use crate::protocol::{
    read_lp, write_lp, AuthToken, Closed, Handshake, Request, Res, Response, VERSION,
};
use crate::rpc_protocol::{
    IdRequest, IdResponse, ListRequest, ListResponse, ProvideRequest, ProvideResponse,
    ProvideResponseEntry, ProviderRequest, ProviderResponse, ProviderService, ShutdownRequest,
    VersionRequest, VersionResponse, WatchRequest, WatchResponse,
};
use crate::tls::{self, Keypair, PeerId};
use crate::util::{self, Hash};

const MAX_CONNECTIONS: u32 = 1024;
const MAX_STREAMS: u64 = 10;
const HEALTH_POLL_WAIT: Duration = Duration::from_secs(1);

/// Database containing content-addressed data (blobs or collections).
#[derive(Debug, Clone, Default)]
pub struct Database(Arc<RwLock<HashMap<Hash, BlobOrCollection>>>);

impl Database {
    fn get(&self, key: &Hash) -> Option<BlobOrCollection> {
        self.0.read().unwrap().get(key).cloned()
    }

    fn union_with(&self, db: HashMap<Hash, BlobOrCollection>) {
        let mut inner = self.0.write().unwrap();
        for (k, v) in db {
            inner.entry(k).or_insert(v);
        }
    }

    /// Iterate over all blobs in the database.
    pub fn blobs(&self) -> impl Iterator<Item = (Hash, PathBuf, u64)> + 'static {
        let items = self
            .0
            .read()
            .unwrap()
            .iter()
            .filter_map(|(k, v)| match v {
                BlobOrCollection::Blob(data) => Some((k, data)),
                BlobOrCollection::Collection(_) => None,
            })
            .map(|(k, data)| (*k, data.path.clone(), data.size))
            .collect::<Vec<_>>();
        // todo: make this a proper lazy iterator at some point
        // e.g. by using an immutable map or a real database that supports snapshots.
        items.into_iter()
    }
}

/// Builder for the [`Provider`].
///
/// You must supply a database which can be created using [`create_collection`], everything else is
/// optional.  Finally you can create and run the provider by calling [`Builder::spawn`].
///
/// The returned [`Provider`] is awaitable to know when it finishes.  It can be terminated
/// using [`Provider::shutdown`].
#[derive(Debug)]
pub struct Builder<E: ServiceEndpoint<ProviderService> = DummyServerEndpoint> {
    bind_addr: SocketAddr,
    keypair: Keypair,
    auth_token: AuthToken,
    rpc_endpoint: E,
    db: Database,
    keylog: bool,
}

#[derive(Debug, Clone)]
pub(crate) enum BlobOrCollection {
    Blob(Data),
    Collection((Bytes, Bytes)),
}

impl Builder {
    /// Creates a new builder for [`Provider`] using the given [`Database`].
    pub fn with_db(db: Database) -> Self {
        Self {
            bind_addr: "127.0.0.1:4433".parse().unwrap(),
            keypair: Keypair::generate(),
            auth_token: AuthToken::generate(),
            rpc_endpoint: Default::default(),
            db,
            keylog: false,
        }
    }
}

impl<E: ServiceEndpoint<ProviderService>> Builder<E> {
    ///
    pub fn rpc_endpoint<E2: ServiceEndpoint<ProviderService>>(self, value: E2) -> Builder<E2> {
        Builder {
            bind_addr: self.bind_addr,
            keypair: self.keypair,
            auth_token: self.auth_token,
            db: self.db,
            keylog: self.keylog,
            rpc_endpoint: value,
        }
    }

    /// Binds the provider service to a different socket.
    ///
    /// By default it binds to `127.0.0.1:4433`.
    pub fn bind_addr(mut self, addr: SocketAddr) -> Self {
        self.bind_addr = addr;
        self
    }

    /// Uses the given [`Keypair`] for the [`PeerId`] instead of a newly generated one.
    pub fn keypair(mut self, keypair: Keypair) -> Self {
        self.keypair = keypair;
        self
    }

    /// Uses the given [`AuthToken`] instead of a newly generated one.
    pub fn auth_token(mut self, auth_token: AuthToken) -> Self {
        self.auth_token = auth_token;
        self
    }

    /// Whether to log the SSL pre-master key.
    ///
    /// If `true` and the `SSLKEYLOGFILE` environment variable is the path to a file this
    /// file will be used to log the SSL pre-master key.  This is useful to inspect captured
    /// traffic.
    pub fn keylog(mut self, keylog: bool) -> Self {
        self.keylog = keylog;
        self
    }

    /// Spawns the [`Provider`] in a tokio task.
    ///
    /// This will create the underlying network server and spawn a tokio task accepting
    /// connections.  The returned [`Provider`] can be used to control the task as well as
    /// get information about it.
    pub fn spawn(self) -> Result<Provider> {
        let tls_server_config = tls::make_server_config(
            &self.keypair,
            vec![crate::tls::P2P_ALPN.to_vec()],
            self.keylog,
        )?;
        let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(tls_server_config));
        let mut transport_config = quinn::TransportConfig::default();
        transport_config
            .max_concurrent_bidi_streams(MAX_STREAMS.try_into()?)
            .max_concurrent_uni_streams(0u32.into());

        server_config
            .transport_config(Arc::new(transport_config))
            .concurrent_connections(MAX_CONNECTIONS);

        let endpoint = quinn::Endpoint::server(server_config, self.bind_addr)?;
        let listen_addr = endpoint.local_addr().unwrap();
        let (events_sender, _events_receiver) = broadcast::channel(8);
        let events = events_sender.clone();
        let cancel_token = CancellationToken::new();
        tracing::debug!("rpc listening on: {:?}", self.rpc_endpoint.local_addr());
        let (internal_rpc, controller) = quic_rpc::transport::flume::connection(1);
        let task = {
            let cancel_token = cancel_token.clone();
            let handler = ProviderHandles {
                peer_id: self.keypair.public().into(),
                db: self.db,
                auth_token: self.auth_token,
                shutdown: cancel_token,
                listen_addr,
            };
            tokio::spawn(async move {
                Self::run(
                    endpoint,
                    events_sender,
                    handler,
                    self.rpc_endpoint,
                    internal_rpc,
                )
                .await
            })
        };

        Ok(Provider {
            listen_addr,
            keypair: self.keypair,
            auth_token: self.auth_token,
            task,
            events,
            controller,
            cancel_token,
        })
    }

    async fn run(
        server: quinn::Endpoint,
        events: broadcast::Sender<Event>,
        handler: ProviderHandles,
        rpc: E,
        internal_rpc: impl ServiceEndpoint<ProviderService>,
    ) {
        let rpc = RpcServer::new(rpc);
        let internal_rpc = RpcServer::new(internal_rpc);
        if let Ok(addr) = server.local_addr() {
            debug!("listening at: {addr}");
        }
        let cancel_token = handler.shutdown.clone();
        loop {
            tokio::select! {
                biased;
                _ = cancel_token.cancelled() => break,
                // handle rpc requests. This will do nothing if rpc is not configured, since
                // accept is just a pending future.
                request = rpc.accept() => {
                    match request {
                        Ok((msg, chan)) => {
                            handle_rpc_request(msg, chan, &handler);
                        }
                        Err(e) => {
                            tracing::info!("rpc request error: {:?}", e);
                        }
                    }
                },
                // handle internal rpc requests.
                request = internal_rpc.accept() => {
                    match request {
                        Ok((msg, chan)) => {
                            handle_rpc_request(msg, chan, &handler);
                        }
                        Err(_) => {
                            tracing::info!("last controller dropped, shutting down");
                            break;
                        }
                    }
                },
                // handle incoming p2p connections
                Some(connecting) = server.accept() => {
                    let db = handler.db.clone();
                    let events = events.clone();
                    let auth_token = handler.auth_token;
                    tokio::spawn(handle_connection(connecting, db, auth_token, events));
                }
                else => break,
            }
        }

        // Closing the Endpoint is the equivalent of calling Connection::close on all
        // connections: Operations will immediately fail with
        // ConnectionError::LocallyClosed.  All streams are interrupted, this is not
        // graceful.
        let error_code = Closed::ProviderTerminating;
        server.close(error_code.into(), error_code.reason());
    }
}

/// A server which implements the iroh provider.
///
/// Clients can connect to this server and requests hashes from it.
///
/// The only way to create this is by using the [`Builder::spawn`].  [`Provider::builder`]
/// is a shorthand to create a suitable [`Builder`].
///
/// This runs a tokio task which can be aborted and joined if desired.  To join the task
/// await the [`Provider`] struct directly, it will complete when the task completes.  If
/// this is dropped the provider task is not stopped but keeps running.
#[derive(Debug)]
pub struct Provider {
    listen_addr: SocketAddr,
    keypair: Keypair,
    auth_token: AuthToken,
    task: JoinHandle<()>,
    events: broadcast::Sender<Event>,
    cancel_token: CancellationToken,
    controller: FlumeConnection<ProviderResponse, ProviderRequest>,
}

/// Events emitted by the [`Provider`] informing about the current status.
#[derive(Debug, Clone)]
pub enum Event {
    /// A new client connected to the provider.
    ClientConnected {
        /// An unique connection id.
        connection_id: u64,
    },
    /// A request was received from a client.
    RequestReceived {
        /// An unique connection id.
        connection_id: u64,
        /// An identifier uniquely identifying this transfer request.
        request_id: u64,
        /// The hash for which the client wants to receive data.
        hash: Hash,
    },
    /// A request was completed and the data was sent to the client.
    TransferCompleted {
        /// An unique connection id.
        connection_id: u64,
        /// An identifier uniquely identifying this transfer request.
        request_id: u64,
    },
    /// A request was aborted because the client disconnected.
    TransferAborted {
        /// The quic connection id.
        connection_id: u64,
        /// An identifier uniquely identifying this request.
        request_id: u64,
    },
}

impl Provider {
    /// Returns a new builder for the [`Provider`].
    ///
    /// Once the done with the builder call [`Builder::spawn`] to create the provider.
    pub fn builder(db: Database) -> Builder {
        Builder::with_db(db)
    }

    /// Returns the address on which the server is listening for connections.
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    /// Returns the [`PeerId`] of the provider.
    pub fn peer_id(&self) -> PeerId {
        self.keypair.public().into()
    }

    /// Returns the [`AuthToken`] needed to connect to the provider.
    pub fn auth_token(&self) -> AuthToken {
        self.auth_token
    }

    /// Subscribe to [`Event`]s emitted from the provider, informing about connections and
    /// progress.
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.events.subscribe()
    }

    /// Returns a handle that can be used to do RPC calls to the provider internally.
    pub fn controller(
        &self,
    ) -> RpcClient<ProviderService, impl ServiceConnection<ProviderService>> {
        RpcClient::new(self.controller.clone())
    }

    /// Return a single token containing everything needed to get a hash.
    ///
    /// See [`Ticket`] for more details of how it can be used.
    pub fn ticket(&self, hash: Hash) -> Ticket {
        // TODO: Verify that the hash exists in the db?
        Ticket {
            hash,
            peer: self.peer_id(),
            addr: self.listen_addr,
            token: self.auth_token,
        }
    }

    /// Aborts the provider.
    ///
    /// This does not gracefully terminate currently: all connections are closed and
    /// anything in-transit is lost.  The task will stop running and awaiting this
    /// [`Provider`] will complete.
    ///
    /// The shutdown behaviour will become more graceful in the future.
    pub fn shutdown(&self) {
        self.cancel_token.cancel();
    }

    /// Returns a token that can be used to cancel the provider.
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }
}

/// The future completes when the spawned tokio task finishes.
impl Future for Provider {
    type Output = Result<(), JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.task).poll(cx)
    }
}

#[derive(Debug, Clone)]
struct ProviderHandles {
    /// database handle
    pub db: Database,
    /// cancellation token
    pub shutdown: CancellationToken,
    /// peer id
    pub peer_id: PeerId,
    /// auth token
    pub auth_token: AuthToken,
    /// listen address
    pub listen_addr: SocketAddr,
}

impl ProviderHandles {
    fn list(self, _msg: ListRequest) -> impl Stream<Item = ListResponse> + Send + 'static {
        let items = self
            .db
            .blobs()
            .map(|(hash, path, size)| ListResponse { hash, path, size });
        futures::stream::iter(items)
    }
    async fn provide(self, msg: ProvideRequest) -> anyhow::Result<ProvideResponse> {
        let path = msg.path;
        let data_sources: Vec<DataSource> = if path.is_dir() {
            let mut paths = Vec::new();
            let mut iter = tokio::fs::read_dir(&path).await?;
            while let Some(el) = iter.next_entry().await? {
                if el.path().is_file() {
                    paths.push(el.path().into());
                }
            }
            paths
        } else if path.is_file() {
            vec![path.into()]
        } else {
            anyhow::bail!("path must be either a Directory or a File");
        };
        // create the collection
        // todo: provide feedback for progress
        let (db, entries, hash) = create_collection_inner(data_sources).await?;
        self.db.union_with(db);

        Ok(ProvideResponse { hash, entries })
    }
    async fn version(self, _: VersionRequest) -> VersionResponse {
        VersionResponse {
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
    async fn id(self, _: IdRequest) -> IdResponse {
        IdResponse {
            peer_id: Box::new(self.peer_id),
            auth_token: Box::new(self.auth_token),
            listen_addr: Box::new(self.listen_addr),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
    async fn shutdown(self, request: ShutdownRequest) {
        if request.force {
            tracing::info!("hard shutdown requested");
            std::process::exit(0);
        } else {
            // trigger a graceful shutdown
            tracing::info!("graceful shutdown requested");
            self.shutdown.cancel();
        }
    }
    fn watch(self, _: WatchRequest) -> impl Stream<Item = WatchResponse> {
        futures::stream::unfold((), |()| async move {
            tokio::time::sleep(HEALTH_POLL_WAIT).await;
            Some((
                WatchResponse {
                    version: env!("CARGO_PKG_VERSION").to_string(),
                },
                (),
            ))
        })
    }
}

fn handle_rpc_request<C: ServiceEndpoint<ProviderService>>(
    msg: ProviderRequest,
    chan: RpcChannel<ProviderService, C>,
    handler: &ProviderHandles,
) {
    let handler = handler.clone();
    tokio::spawn(async move {
        use ProviderRequest::*;
        match msg {
            List(msg) => {
                chan.server_streaming(msg, handler, ProviderHandles::list)
                    .await
            }
            Provide(msg) => {
                chan.rpc_map_err(msg, handler, ProviderHandles::provide)
                    .await
            }
            Watch(msg) => {
                chan.server_streaming(msg, handler, ProviderHandles::watch)
                    .await
            }
            Version(msg) => chan.rpc(msg, handler, ProviderHandles::version).await,
            Id(msg) => chan.rpc(msg, handler, ProviderHandles::id).await,
            Shutdown(msg) => chan.rpc(msg, handler, ProviderHandles::shutdown).await,
        }
    });
}

async fn handle_connection(
    connecting: quinn::Connecting,
    db: Database,
    auth_token: AuthToken,
    events: broadcast::Sender<Event>,
) {
    let remote_addr = connecting.remote_address();
    let connection = match connecting.await {
        Ok(conn) => conn,
        Err(err) => {
            warn!(%remote_addr, "Error connecting: {err:#}");
            return;
        }
    };
    let connection_id = connection.stable_id() as u64;
    let span = debug_span!("connection", connection_id, %remote_addr);
    async move {
        while let Ok(stream) = connection.accept_bi().await {
            let span = debug_span!("stream", stream_id = %stream.0.id());
            events.send(Event::ClientConnected { connection_id }).ok();
            let db = db.clone();
            let events = events.clone();
            tokio::spawn(
                async move {
                    if let Err(err) =
                        handle_stream(db, auth_token, connection_id, stream, events).await
                    {
                        warn!("error: {err:#?}",);
                    }
                }
                .instrument(span),
            );
        }
    }
    .instrument(span)
    .await
}

/// Read and decode the handshake.
///
/// Will fail if there is an error while reading, there is a token mismatch, or no valid
/// handshake was received.
///
/// When successful, the reader is still useable after this function and the buffer will be
/// drained of any handshake data.
async fn read_handshake<R: AsyncRead + Unpin>(
    mut reader: R,
    buffer: &mut BytesMut,
    token: AuthToken,
) -> Result<()> {
    let payload = read_lp(&mut reader, buffer)
        .await?
        .context("no valid handshake received")?;
    let handshake: Handshake = postcard::from_bytes(&payload)?;
    ensure!(
        handshake.version == VERSION,
        "expected version {} but got {}",
        VERSION,
        handshake.version
    );
    ensure!(handshake.token == token, "AuthToken mismatch");
    Ok(())
}

/// Read the request from the getter.
///
/// Will fail if there is an error while reading, if the reader
/// contains more data than the Request, or if no valid request is sent.
///
/// When successful, the buffer is empty after this function call.
async fn read_request(mut reader: quinn::RecvStream, buffer: &mut BytesMut) -> Result<Request> {
    let payload = read_lp(&mut reader, buffer)
        .await?
        .context("No request received")?;
    let request: Request = postcard::from_bytes(&payload)?;
    ensure!(
        reader.read_chunk(8, false).await?.is_none(),
        "Extra data past request"
    );
    Ok(request)
}

/// Transfers the collection & blob data.
///
/// First, it transfers the collection data & its associated outboard encoding data. Then it sequentially transfers each individual blob data & its associated outboard
/// encoding data.
///
/// Will fail if there is an error writing to the getter or reading from
/// the database.
///
/// If a blob from the collection cannot be found in the database, the transfer will gracefully
/// close the writer, and return with `Ok(SentStatus::NotFound)`.
///
/// If the transfer does _not_ end in error, the buffer will be empty and the writer is gracefully closed.
async fn transfer_collection(
    // Database from which to fetch blobs.
    db: &Database,
    // Quinn stream.
    mut writer: quinn::SendStream,
    // Buffer used when writing to writer.
    buffer: &mut BytesMut,
    // The bao outboard encoded data.
    outboard: &Bytes,
    // The actual blob data.
    data: &Bytes,
) -> Result<SentStatus> {
    // We only respond to requests for collections, not individual blobs
    let mut extractor = SliceExtractor::new_outboard(
        std::io::Cursor::new(&data[..]),
        std::io::Cursor::new(&outboard[..]),
        0,
        data.len() as u64,
    );
    let encoded_size: usize = abao::encode::encoded_size(data.len() as u64)
        .try_into()
        .unwrap();
    let mut encoded = Vec::with_capacity(encoded_size);
    extractor.read_to_end(&mut encoded)?;

    let c: Collection = postcard::from_bytes(data)?;

    // TODO: we should check if the blobs referenced in this container
    // actually exist in this provider before returning `FoundCollection`
    write_response(
        &mut writer,
        buffer,
        Res::FoundCollection {
            total_blobs_size: c.total_blobs_size,
        },
    )
    .await?;

    let mut data = BytesMut::from(&encoded[..]);
    writer.write_buf(&mut data).await?;
    for (i, blob) in c.blobs.iter().enumerate() {
        debug!("writing blob {}/{}", i, c.blobs.len());
        let (status, writer1) = send_blob(db.clone(), blob.hash, writer, buffer).await?;
        writer = writer1;
        if SentStatus::NotFound == status {
            writer.finish().await?;
            return Ok(status);
        }
    }

    writer.finish().await?;
    Ok(SentStatus::Sent)
}

fn notify_transfer_aborted(events: broadcast::Sender<Event>, connection_id: u64, request_id: u64) {
    let _ = events.send(Event::TransferAborted {
        connection_id,
        request_id,
    });
}

async fn handle_stream(
    db: Database,
    token: AuthToken,
    connection_id: u64,
    (mut writer, mut reader): (quinn::SendStream, quinn::RecvStream),
    events: broadcast::Sender<Event>,
) -> Result<()> {
    let mut out_buffer = BytesMut::with_capacity(1024);
    let mut in_buffer = BytesMut::with_capacity(1024);

    // The stream ID index is used to identify this request.  Requests only arrive in
    // bi-directional RecvStreams initiated by the client, so this uniquely identifies them.
    let request_id = reader.id().index();

    // 1. Read Handshake
    debug!("reading handshake");
    if let Err(e) = read_handshake(&mut reader, &mut in_buffer, token).await {
        notify_transfer_aborted(events, connection_id, request_id);
        return Err(e);
    }

    // 2. Decode the request.
    debug!("reading request");
    let request = match read_request(reader, &mut in_buffer).await {
        Ok(r) => r,
        Err(e) => {
            notify_transfer_aborted(events, connection_id, request_id);
            return Err(e);
        }
    };

    let hash = request.name;
    debug!("got request for ({hash})");
    let _ = events.send(Event::RequestReceived {
        connection_id,
        hash,
        request_id,
    });

    // 4. Attempt to find hash
    let (outboard, data) = match db.get(&hash) {
        // We only respond to requests for collections, not individual blobs
        Some(BlobOrCollection::Collection(d)) => d,
        _ => {
            debug!("not found {}", hash);
            notify_transfer_aborted(events, connection_id, request_id);
            write_response(&mut writer, &mut out_buffer, Res::NotFound).await?;
            writer.finish().await?;

            return Ok(());
        }
    };

    // 5. Transfer data!
    match transfer_collection(&db, writer, &mut out_buffer, &outboard, &data).await {
        Ok(SentStatus::Sent) => {
            let _ = events.send(Event::TransferCompleted {
                connection_id,
                request_id,
            });
        }
        Ok(SentStatus::NotFound) => {
            notify_transfer_aborted(events, connection_id, request_id);
        }
        Err(e) => {
            notify_transfer_aborted(events, connection_id, request_id);
            return Err(e);
        }
    }

    debug!("finished response");
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SentStatus {
    Sent,
    NotFound,
}

async fn send_blob<W: AsyncWrite + Unpin + Send + 'static>(
    db: Database,
    name: Hash,
    mut writer: W,
    buffer: &mut BytesMut,
) -> Result<(SentStatus, W)> {
    match db.get(&name) {
        Some(BlobOrCollection::Blob(Data {
            outboard,
            path,
            size,
        })) => {
            write_response(&mut writer, buffer, Res::Found).await?;
            // need to thread the writer though the spawn_blocking, since
            // taking a reference does not work. spawn_blocking requires
            // 'static lifetime.
            writer = tokio::task::spawn_blocking(move || {
                let file_reader = std::fs::File::open(&path)?;
                let outboard_reader = std::io::Cursor::new(outboard);
                let mut wrapper = SyncIoBridge::new(&mut writer);
                let mut slice_extractor = abao::encode::SliceExtractor::new_outboard(
                    file_reader,
                    outboard_reader,
                    0,
                    size,
                );
                let _copied = std::io::copy(&mut slice_extractor, &mut wrapper)?;
                std::io::Result::Ok(writer)
            })
            .await??;
            Ok((SentStatus::Sent, writer))
        }
        _ => {
            write_response(&mut writer, buffer, Res::NotFound).await?;
            Ok((SentStatus::NotFound, writer))
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Data {
    /// Outboard data from bao.
    outboard: Bytes,
    /// Path to the original data, which must not change while in use.
    ///
    /// Note that when adding multiple files with the same content, only one of them
    /// will get added to the store. So the path is not that useful for information.
    /// It is just a place to look for the data correspoding to the hash and outboard.
    path: PathBuf,
    /// Size of the original data.
    size: u64,
}

/// A data source
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum DataSource {
    /// A blob of data originating from the filesystem. The name of the blob is derived from
    /// the filename.
    File(PathBuf),
    /// NamedFile is treated the same as [`DataSource::File`], except you can pass in a custom
    /// name. Passing in the empty string will explicitly _not_ persist the filename.
    NamedFile {
        /// Path to the file
        path: PathBuf,
        /// Custom name
        name: String,
    },
}

impl DataSource {
    /// Creates a new [`DataSource`] from a [`PathBuf`].
    pub fn new(path: PathBuf) -> Self {
        DataSource::File(path)
    }
    /// Creates a new [`DataSource`] from a [`PathBuf`] and a custom name.
    pub fn with_name(path: PathBuf, name: String) -> Self {
        DataSource::NamedFile { path, name }
    }
}

impl From<PathBuf> for DataSource {
    fn from(value: PathBuf) -> Self {
        DataSource::new(value)
    }
}

impl From<&std::path::Path> for DataSource {
    fn from(value: &std::path::Path) -> Self {
        DataSource::new(value.to_path_buf())
    }
}

/// Synchronously compute the outboard of a file, and return hash and outboard.
///
/// It is assumed that the file is not modified while this is running.
///
/// If it is modified while or after this is running, the outboard will be
/// invalid, so any attempt to compute a slice from it will fail.
///
/// If the size of the file is changed while this is running, an error will be
/// returned.
///
/// path and name are returned with the result to provide context
fn compute_outboard(
    path: PathBuf,
    name: Option<String>,
) -> anyhow::Result<(PathBuf, Option<String>, Hash, Vec<u8>)> {
    ensure!(
        path.is_file(),
        "can only transfer blob data: {}",
        path.display()
    );
    let file = std::fs::File::open(&path)?;
    let len = file.metadata()?.len();
    // compute outboard size so we can pre-allocate the buffer.
    //
    // outboard is ~1/16 of data size, so this will fail for really large files
    // on really small devices. E.g. you want to transfer a 1TB file from a pi4 with 1gb ram.
    //
    // The way to solve this would be to have larger blocks than the blake3 chunk size of 1024.
    // I think we really want to keep the outboard in memory for simplicity.
    let outboard_size = usize::try_from(abao::encode::outboard_size(len))
        .context("outboard too large to fit in memory")?;
    let mut outboard = Vec::with_capacity(outboard_size);

    // copy the file into the encoder. Data will be skipped by the encoder in outboard mode.
    let outboard_cursor = std::io::Cursor::new(&mut outboard);
    let mut encoder = abao::encode::Encoder::new_outboard(outboard_cursor);

    let mut reader = BufReader::new(file);
    // the length we have actually written, should be the same as the length of the file.
    let len2 = std::io::copy(&mut reader, &mut encoder)?;
    // this can fail if the file was appended to during encoding.
    ensure!(len == len2, "file changed during encoding");
    // this flips the outboard encoding from post-order to pre-order
    let hash = encoder.finalize()?;

    Ok((path, name, hash.into(), outboard))
}

/// Creates a database of blobs (stored in outboard storage) and Collections, stored in memory.
/// Returns a the hash of the collection created by the given list of DataSources
pub async fn create_collection(data_sources: Vec<DataSource>) -> Result<(Database, Hash)> {
    let (db, _, hash) = create_collection_inner(data_sources).await?;
    Ok((Database(Arc::new(RwLock::new(db))), hash))
}

/// The actual implementation of create_collection, except for the wrapping into arc and mutex to make
/// a public Database.
async fn create_collection_inner(
    data_sources: Vec<DataSource>,
) -> Result<(
    HashMap<Hash, BlobOrCollection>,
    Vec<ProvideResponseEntry>,
    Hash,
)> {
    // +1 is for the collection itself
    let mut db = HashMap::with_capacity(data_sources.len() + 1);
    let mut blobs = Vec::with_capacity(data_sources.len());
    let mut total_blobs_size: u64 = 0;

    // compute outboards in parallel, using tokio's blocking thread pool
    let outboards = data_sources.into_iter().map(|data| {
        let (path, name) = match data {
            DataSource::File(path) => (path, None),
            DataSource::NamedFile { path, name } => (path, Some(name)),
        };
        tokio::task::spawn_blocking(move || compute_outboard(path, name))
    });
    // wait for completion and collect results
    let outboards = future::join_all(outboards)
        .await
        .into_iter()
        .collect::<Result<Result<Vec<_>, _>, _>>()??;

    // compute information about the collection
    let entries = outboards
        .iter()
        .map(|(path, name, hash, outboard)| {
            let name = name
                .as_ref()
                .cloned()
                .unwrap_or_else(|| path.display().to_string());
            ProvideResponseEntry {
                name,
                hash: *hash,
                size: u64::from_le_bytes(outboard[..8].try_into().unwrap()),
            }
        })
        .collect();

    // insert outboards into the database and build collection
    for (path, name, hash, outboard) in outboards {
        debug_assert!(outboard.len() >= 8, "outboard must at least contain size");
        let size = u64::from_le_bytes(outboard[..8].try_into().unwrap());
        db.insert(
            hash,
            BlobOrCollection::Blob(Data {
                outboard: Bytes::from(outboard),
                path: path.clone(),
                size,
            }),
        );
        total_blobs_size += size;
        // if the given name is `None`, use the filename from the given path as the name
        let name = name.unwrap_or_else(|| {
            path.file_name()
                .and_then(|s| s.to_str())
                .unwrap_or_default()
                .to_string()
        });
        blobs.push(Blob { name, hash });
    }

    let c = Collection {
        name: "collection".to_string(),
        blobs,
        total_blobs_size,
    };

    let data = postcard::to_stdvec(&c).context("blob encoding")?;
    let (outboard, hash) = abao::encode::outboard(&data);
    let hash = Hash::from(hash);
    db.insert(
        hash,
        BlobOrCollection::Collection((Bytes::from(outboard), Bytes::from(data.to_vec()))),
    );

    Ok((db, entries, hash))
}

async fn write_response<W: AsyncWrite + Unpin>(
    mut writer: W,
    buffer: &mut BytesMut,
    res: Res,
) -> Result<()> {
    let response = Response { data: res };

    // TODO: do not transfer blob data as part of the responses
    if buffer.len() < Response::POSTCARD_MAX_SIZE {
        buffer.resize(Response::POSTCARD_MAX_SIZE, 0u8);
    }
    let used = postcard::to_slice(&response, buffer)?;

    write_lp(&mut writer, used).await?;

    debug!("written response of length {}", used.len());
    Ok(())
}

/// A token containing everything to get a file from the provider.
///
/// It is a single item which can be easily serialized and deserialized.  The [`Display`]
/// and [`FromStr`] implementations serialize to base64.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Ticket {
    /// The hash to retrieve.
    pub hash: Hash,
    /// The peer ID identifying the provider.
    pub peer: PeerId,
    /// The socket address the provider is listening on.
    pub addr: SocketAddr,
    /// The authentication token with permission to retrieve the hash.
    pub token: AuthToken,
}

impl Ticket {
    /// Deserializes from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let slf = postcard::from_bytes(bytes)?;
        Ok(slf)
    }

    /// Serializes to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("postcard::to_stdvec is infallible")
    }
}

/// Serializes to base64.
impl Display for Ticket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let encoded = self.to_bytes();
        write!(f, "{}", util::encode(encoded))
    }
}

/// Deserializes from base64.
impl FromStr for Ticket {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = util::decode(s)?;
        let slf = Self::from_bytes(&bytes)?;
        Ok(slf)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use testdir::testdir;

    use super::*;

    #[test]
    fn test_ticket_base64_roundtrip() {
        let (_encoded, hash) = abao::encode::encode(b"hi there");
        let hash = Hash::from(hash);
        let peer = PeerId::from(Keypair::generate().public());
        let addr = SocketAddr::from_str("127.0.0.1:1234").unwrap();
        let token = AuthToken::generate();
        let ticket = Ticket {
            hash,
            peer,
            addr,
            token,
        };
        let base64 = ticket.to_string();
        println!("Ticket: {base64}");
        println!("{} bytes", base64.len());

        let ticket2: Ticket = base64.parse().unwrap();
        assert_eq!(ticket2, ticket);
    }

    #[tokio::test]
    async fn test_create_collection() -> Result<()> {
        let dir: PathBuf = testdir!();
        let mut expect_blobs = vec![];
        let (_, hash) = abao::encode::outboard(vec![]);
        let hash = Hash::from(hash);

        // DataSource::File
        let foo = dir.join("foo");
        tokio::fs::write(&foo, vec![]).await?;
        let foo = DataSource::new(foo);
        expect_blobs.push(Blob {
            name: "foo".to_string(),
            hash,
        });

        // DataSource::NamedFile
        let bar = dir.join("bar");
        tokio::fs::write(&bar, vec![]).await?;
        let bar = DataSource::with_name(bar, "bat".to_string());
        expect_blobs.push(Blob {
            name: "bat".to_string(),
            hash,
        });

        // DataSource::NamedFile, empty string name
        let baz = dir.join("baz");
        tokio::fs::write(&baz, vec![]).await?;
        let baz = DataSource::with_name(baz, "".to_string());
        expect_blobs.push(Blob {
            name: "".to_string(),
            hash,
        });

        let expect_collection = Collection {
            name: "collection".to_string(),
            blobs: expect_blobs,
            total_blobs_size: 0,
        };

        let (db, hash) = create_collection(vec![foo, bar, baz]).await?;

        let collection = {
            let c = db.get(&hash).unwrap();
            if let BlobOrCollection::Collection((_, data)) = c {
                Collection::from_bytes(&data)?
            } else {
                panic!("expected hash to correspond with a `Collection`, found `Blob` instead");
            }
        };

        assert_eq!(expect_collection, collection);

        Ok(())
    }
}

/// Create a [`quinn::ServerConfig`] with the given keypair and limits.
pub fn make_server_config(
    keypair: &Keypair,
    max_streams: u64,
    max_connections: u32,
    alpn_protocols: Vec<Vec<u8>>,
) -> anyhow::Result<quinn::ServerConfig> {
    let tls_server_config = tls::make_server_config(keypair, alpn_protocols, false)?;
    let mut server_config = quinn::ServerConfig::with_crypto(Arc::new(tls_server_config));
    let mut transport_config = quinn::TransportConfig::default();
    transport_config
        .max_concurrent_bidi_streams(max_streams.try_into()?)
        .max_concurrent_uni_streams(0u32.into());

    server_config
        .transport_config(Arc::new(transport_config))
        .concurrent_connections(max_connections);
    Ok(server_config)
}
