//! This defines the RPC protocol used for communication between a CLI and an iroh node.
//!
//! RPC using the [`quic-rpc`](https://docs.rs/quic-rpc) crate.
//!
//! This file contains request messages, response messages and definitions of
//! the interaction pattern. Some requests like version and shutdown have a single
//! response, while others like provide have a stream of responses.
//!
//! Note that this is subject to change. The RPC protocol is not yet stable.
use std::{collections::HashMap, fmt, net::SocketAddr, path::PathBuf, str::FromStr};

use bytes::Bytes;
use derive_more::{From, TryInto};
use iroh_bytes::{protocol::RequestToken, provider::ShareProgress, Hash};
use iroh_gossip::proto::util::base32;
use iroh_net::key::PublicKey;

use iroh_sync::{
    store::GetFilter,
    sync::{AuthorId, NamespaceId, SignedEntry},
};
use quic_rpc::{
    message::{Msg, RpcMsg, ServerStreaming, ServerStreamingMsg},
    Service,
};
use serde::{Deserialize, Serialize};

pub use iroh_bytes::{baomap::ValidateProgress, provider::ProvideProgress, util::RpcResult};

use crate::sync_engine::{LiveEvent, LiveStatus, PeerSource};

/// A 32-byte key or token
pub type KeyBytes = [u8; 32];

/// A request to the node to provide the data at the given path
///
/// Will produce a stream of [`ProvideProgress`] messages.
#[derive(Debug, Serialize, Deserialize)]
pub struct ProvideRequest {
    /// The path to the data to provide.
    ///
    /// This should be an absolute path valid for the file system on which
    /// the node runs. Usually the cli will run on the same machine as the
    /// node, so this should be an absolute path on the cli machine.
    pub path: PathBuf,
    /// True if the provider can assume that the data will not change, so it
    /// can be shared in place.
    pub in_place: bool,
}

impl Msg<ProviderService> for ProvideRequest {
    type Pattern = ServerStreaming;
}

impl ServerStreamingMsg<ProviderService> for ProvideRequest {
    type Response = ProvideProgress;
}

/// A request to the node to download and share the data specified by the hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShareRequest {
    /// This mandatory field contains the hash of the data to download and share.
    pub hash: Hash,
    /// If this flag is true, the hash is assumed to be a collection and all
    /// children are downloaded and shared as well.
    pub recursive: bool,
    /// This mandatory field specifies the peer to download the data from.
    pub peer: PublicKey,
    /// This vec contains possible candidate addresses of the peer.
    pub addrs: Vec<SocketAddr>,
    /// This optional field contains a request token that can be used to authorize
    /// the download request.
    pub token: Option<RequestToken>,
    /// This optional field contains the derp region to use for contacting the peer
    /// over the DERP protocol.
    pub derp_region: Option<u16>,
    /// This optional field contains the path to store the data to. If it is not
    /// set, the data is dumped to stdout.
    pub out: Option<String>,
    /// If this flag is true, the data is shared in place, i.e. it is moved to the
    /// out path instead of being copied. The database itself contains only a
    /// reference to the out path of the file.
    ///
    /// If the data is modified in the location specified by the out path,
    /// download attempts for the associated hash will fail.
    ///
    /// This flag is only relevant if the out path is set.
    pub in_place: bool,
}

impl Msg<ProviderService> for ShareRequest {
    type Pattern = ServerStreaming;
}

impl ServerStreamingMsg<ProviderService> for ShareRequest {
    type Response = ShareProgress;
}

/// A request to the node to validate the integrity of all provided data
#[derive(Debug, Serialize, Deserialize)]
pub struct ValidateRequest {
    /// If true, remove invalid data
    pub repair: bool,
}

impl Msg<ProviderService> for ValidateRequest {
    type Pattern = ServerStreaming;
}

impl ServerStreamingMsg<ProviderService> for ValidateRequest {
    type Response = ValidateProgress;
}

/// List all blobs, including collections
#[derive(Debug, Serialize, Deserialize)]
pub struct ListBlobsRequest;

/// A response to a list blobs request
#[derive(Debug, Serialize, Deserialize)]
pub struct ListBlobsResponse {
    /// Location of the blob
    pub path: String,
    /// The hash of the blob
    pub hash: Hash,
    /// The size of the blob
    pub size: u64,
}

impl Msg<ProviderService> for ListBlobsRequest {
    type Pattern = ServerStreaming;
}

impl ServerStreamingMsg<ProviderService> for ListBlobsRequest {
    type Response = ListBlobsResponse;
}

/// List all blobs, including collections
#[derive(Debug, Serialize, Deserialize)]
pub struct ListIncompleteBlobsRequest;

/// A response to a list blobs request
#[derive(Debug, Serialize, Deserialize)]
pub struct ListIncompleteBlobsResponse {
    /// The size we got
    pub size: u64,
    /// The size we expect
    pub expected_size: u64,
    /// The hash of the blob
    pub hash: Hash,
}

impl Msg<ProviderService> for ListIncompleteBlobsRequest {
    type Pattern = ServerStreaming;
}

impl ServerStreamingMsg<ProviderService> for ListIncompleteBlobsRequest {
    type Response = ListIncompleteBlobsResponse;
}

/// List all collections
///
/// Lists all collections that have been explicitly added to the database.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListCollectionsRequest;

/// A response to a list collections request
#[derive(Debug, Serialize, Deserialize)]
pub struct ListCollectionsResponse {
    /// Hash of the collection
    pub hash: Hash,
    /// Number of children in the collection
    ///
    /// This is an optional field, because the data is not always available.
    pub total_blobs_count: Option<u64>,
    /// Total size of the raw data referred to by all links
    ///
    /// This is an optional field, because the data is not always available.
    pub total_blobs_size: Option<u64>,
}

impl Msg<ProviderService> for ListCollectionsRequest {
    type Pattern = ServerStreaming;
}

impl ServerStreamingMsg<ProviderService> for ListCollectionsRequest {
    type Response = ListCollectionsResponse;
}

/// A request to get the version of the node
#[derive(Serialize, Deserialize, Debug)]
pub struct VersionRequest;

impl RpcMsg<ProviderService> for VersionRequest {
    type Response = VersionResponse;
}

/// A request to shutdown the node
#[derive(Serialize, Deserialize, Debug)]
pub struct ShutdownRequest {
    /// Force shutdown
    pub force: bool,
}

impl RpcMsg<ProviderService> for ShutdownRequest {
    type Response = ();
}

/// A request to get information about the identity of the node
///
/// See [`StatusResponse`] for the response.
#[derive(Serialize, Deserialize, Debug)]
pub struct StatusRequest;

impl RpcMsg<ProviderService> for StatusRequest {
    type Response = StatusResponse;
}

/// The response to a version request
#[derive(Serialize, Deserialize, Debug)]
pub struct StatusResponse {
    /// The peer id of the node
    pub peer_id: Box<PublicKey>,
    /// The addresses of the node
    pub listen_addrs: Vec<SocketAddr>,
    /// The version of the node
    pub version: String,
}

/// A request to watch for the node status
#[derive(Serialize, Deserialize, Debug)]
pub struct WatchRequest;

impl Msg<ProviderService> for WatchRequest {
    type Pattern = ServerStreaming;
}

impl ServerStreamingMsg<ProviderService> for WatchRequest {
    type Response = WatchResponse;
}

/// The response to a watch request
#[derive(Serialize, Deserialize, Debug)]
pub struct WatchResponse {
    /// The version of the node
    pub version: String,
}

/// The response to a version request
#[derive(Serialize, Deserialize, Debug)]
pub struct VersionResponse {
    /// The version of the node
    pub version: String,
}

// author

/// List document authors for which we have a secret key.
#[derive(Serialize, Deserialize, Debug)]
pub struct AuthorListRequest {}

impl Msg<ProviderService> for AuthorListRequest {
    type Pattern = ServerStreaming;
}

impl ServerStreamingMsg<ProviderService> for AuthorListRequest {
    type Response = RpcResult<AuthorListResponse>;
}

/// Response for [`AuthorListRequest`]
#[derive(Serialize, Deserialize, Debug)]
pub struct AuthorListResponse {
    /// The author id
    pub author_id: AuthorId,
}

/// Create a new document author.
#[derive(Serialize, Deserialize, Debug)]
pub struct AuthorCreateRequest;

impl RpcMsg<ProviderService> for AuthorCreateRequest {
    type Response = RpcResult<AuthorCreateResponse>;
}

/// Response for [`AuthorCreateRequest`]
#[derive(Serialize, Deserialize, Debug)]
pub struct AuthorCreateResponse {
    /// The id of the created author
    pub author_id: AuthorId,
}

/// Import author from secret key
#[derive(Serialize, Deserialize, Debug)]
pub struct AuthorImportRequest {
    /// The secret key for the author
    pub key: KeyBytes,
}

impl RpcMsg<ProviderService> for AuthorImportRequest {
    type Response = RpcResult<AuthorImportResponse>;
}

/// Response to [`AuthorImportRequest`]
#[derive(Serialize, Deserialize, Debug)]
pub struct AuthorImportResponse {
    /// The author id of the imported author
    pub author_id: AuthorId,
}

/// Intended capability for document share tickets
#[derive(Serialize, Deserialize, Debug, Clone)]
#[cfg_attr(feature = "cli", derive(clap::ValueEnum))]
pub enum ShareMode {
    /// Read-only access
    Read,
    /// Write access
    Write,
}

/// Subscribe to events for a document.
#[derive(Serialize, Deserialize, Debug)]
pub struct DocSubscribeRequest {
    /// The document id
    pub doc_id: NamespaceId,
}

impl Msg<ProviderService> for DocSubscribeRequest {
    type Pattern = ServerStreaming;
}

impl ServerStreamingMsg<ProviderService> for DocSubscribeRequest {
    type Response = RpcResult<DocSubscribeResponse>;
}

/// Response to [`DocSubscribeRequest`]
#[derive(Serialize, Deserialize, Debug)]
pub struct DocSubscribeResponse {
    /// The event that occured on the document
    pub event: LiveEvent,
}

/// List all documents
#[derive(Serialize, Deserialize, Debug)]
pub struct DocListRequest {}

impl Msg<ProviderService> for DocListRequest {
    type Pattern = ServerStreaming;
}

impl ServerStreamingMsg<ProviderService> for DocListRequest {
    type Response = RpcResult<DocListResponse>;
}

/// Response to [`DocListRequest`]
#[derive(Serialize, Deserialize, Debug)]
pub struct DocListResponse {
    /// The document id
    pub id: NamespaceId,
}

/// Create a new document
#[derive(Serialize, Deserialize, Debug)]
pub struct DocCreateRequest {}

impl RpcMsg<ProviderService> for DocCreateRequest {
    type Response = RpcResult<DocCreateResponse>;
}

/// Response to [`DocCreateRequest`]
#[derive(Serialize, Deserialize, Debug)]
pub struct DocCreateResponse {
    /// The document id
    pub id: NamespaceId,
}

/// Contains both a key (either secret or public) to a document, and a list of peers to join.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DocTicket {
    /// either a public or private key
    pub key: KeyBytes,
    /// a list of peers
    pub peers: Vec<PeerSource>,
}
impl DocTicket {
    /// Create a new doc ticket
    pub fn new(key: KeyBytes, peers: Vec<PeerSource>) -> Self {
        Self { key, peers }
    }
    /// Serialize the ticket to a byte array.
    pub fn to_bytes(&self) -> anyhow::Result<Vec<u8>> {
        let bytes = postcard::to_stdvec(&self)?;
        Ok(bytes)
    }
    /// Parse ticket from a byte array.
    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        let slf = postcard::from_bytes(bytes)?;
        Ok(slf)
    }
}
impl FromStr for DocTicket {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_bytes(&base32::parse_vec(s)?)
    }
}
impl fmt::Display for DocTicket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}",
            base32::fmt(self.to_bytes().expect("failed to serialize"))
        )
    }
}

/// Import a document from a ticket.
#[derive(Serialize, Deserialize, Debug)]
pub struct DocImportRequest(pub DocTicket);

impl RpcMsg<ProviderService> for DocImportRequest {
    type Response = RpcResult<DocImportResponse>;
}

/// Response to [`DocImportRequest`]
#[derive(Serialize, Deserialize, Debug)]
pub struct DocImportResponse {
    /// the document id
    pub doc_id: NamespaceId,
}

/// Share a document with peers over a ticket.
#[derive(Serialize, Deserialize, Debug)]
pub struct DocShareRequest {
    /// The document id
    pub doc_id: NamespaceId,
    /// Whether to share read or write access to the document
    pub mode: ShareMode,
}

impl RpcMsg<ProviderService> for DocShareRequest {
    type Response = RpcResult<DocShareResponse>;
}

/// The response to [`DocShareRequest`]
#[derive(Serialize, Deserialize, Debug)]
pub struct DocShareResponse(pub DocTicket);

/// Get info on a document
#[derive(Serialize, Deserialize, Debug)]
pub struct DocInfoRequest {
    /// The document id
    pub doc_id: NamespaceId,
}

impl RpcMsg<ProviderService> for DocInfoRequest {
    type Response = RpcResult<DocInfoResponse>;
}

/// Response to [`DocInfoRequest`]
// TODO: actually provide info
#[derive(Serialize, Deserialize, Debug)]
pub struct DocInfoResponse {
    /// Live sync status
    pub status: LiveStatus,
}

/// Start to sync a doc with peers.
#[derive(Serialize, Deserialize, Debug)]
pub struct DocStartSyncRequest {
    /// The document id
    pub doc_id: NamespaceId,
    /// List of peers to join
    pub peers: Vec<PeerSource>,
}

impl RpcMsg<ProviderService> for DocStartSyncRequest {
    type Response = RpcResult<DocStartSyncResponse>;
}

/// Response to [`DocStartSyncRequest`]
#[derive(Serialize, Deserialize, Debug)]
pub struct DocStartSyncResponse {}

/// Stop the live sync for a doc.
#[derive(Serialize, Deserialize, Debug)]
pub struct DocStopSyncRequest {
    /// The document id
    pub doc_id: NamespaceId,
}

impl RpcMsg<ProviderService> for DocStopSyncRequest {
    type Response = RpcResult<DocStopSyncResponse>;
}

/// Response to [`DocStopSyncRequest`]
#[derive(Serialize, Deserialize, Debug)]
pub struct DocStopSyncResponse {}

/// Set an entry in a document
#[derive(Serialize, Deserialize, Debug)]
pub struct DocSetRequest {
    /// The document id
    pub doc_id: NamespaceId,
    /// Author of this entry.
    pub author_id: AuthorId,
    /// Key of this entry.
    pub key: Vec<u8>,
    /// Value of this entry.
    // TODO: Allow to provide the hash directly
    // TODO: Add a way to provide content as stream
    pub value: Vec<u8>,
}

impl RpcMsg<ProviderService> for DocSetRequest {
    type Response = RpcResult<DocSetResponse>;
}

/// Response to [`DocSetRequest`]
#[derive(Serialize, Deserialize, Debug)]
pub struct DocSetResponse {
    /// The newly-created entry.
    pub entry: SignedEntry,
}

/// Get entries from a document
#[derive(Serialize, Deserialize, Debug)]
pub struct DocGetRequest {
    /// The document id
    pub doc_id: NamespaceId,
    /// Filter entries by this [`GetFilter`]
    pub filter: GetFilter,
}

impl Msg<ProviderService> for DocGetRequest {
    type Pattern = ServerStreaming;
}

impl ServerStreamingMsg<ProviderService> for DocGetRequest {
    type Response = RpcResult<DocGetResponse>;
}

/// Response to [`DocGetRequest`]
#[derive(Serialize, Deserialize, Debug)]
pub struct DocGetResponse {
    /// The document entry
    pub entry: SignedEntry,
}

/// Get the bytes for a hash
#[derive(Serialize, Deserialize, Debug)]
pub struct BytesGetRequest {
    /// Hash to get bytes for
    pub hash: Hash,
}

impl RpcMsg<ProviderService> for BytesGetRequest {
    type Response = RpcResult<BytesGetResponse>;
}

/// Response to [`BytesGetRequest`]
#[derive(Serialize, Deserialize, Debug)]
pub struct BytesGetResponse {
    /// The blob data
    pub data: Bytes,
}

/// Get stats for the running Iroh node
#[derive(Serialize, Deserialize, Debug)]
pub struct StatsGetRequest {}

impl RpcMsg<ProviderService> for StatsGetRequest {
    type Response = RpcResult<StatsGetResponse>;
}

/// Counter stats
#[derive(Serialize, Deserialize, Debug)]
pub struct CounterStats {
    /// The counter value
    pub value: u64,
    /// The counter description
    pub description: String,
}

/// Response to [`StatsGetRequest`]
#[derive(Serialize, Deserialize, Debug)]
pub struct StatsGetResponse {
    /// Map of statistics
    pub stats: HashMap<String, CounterStats>,
}

/// The RPC service for the iroh provider process.
#[derive(Debug, Clone)]
pub struct ProviderService;

/// The request enum, listing all possible requests.
#[allow(missing_docs)]
#[derive(Debug, Serialize, Deserialize, From, TryInto)]
pub enum ProviderRequest {
    Watch(WatchRequest),
    Version(VersionRequest),
    ListBlobs(ListBlobsRequest),
    ListIncompleteBlobs(ListIncompleteBlobsRequest),
    ListCollections(ListCollectionsRequest),
    Provide(ProvideRequest),
    Share(ShareRequest),
    Status(StatusRequest),
    Shutdown(ShutdownRequest),
    Validate(ValidateRequest),

    // TODO: I see I changed naming convention here but at least to me it becomes easier to parse
    // with the subject in front if there's many commands
    AuthorList(AuthorListRequest),
    AuthorCreate(AuthorCreateRequest),
    AuthorImport(AuthorImportRequest),

    DocInfo(DocInfoRequest),
    DocList(DocListRequest),
    DocCreate(DocCreateRequest),
    DocImport(DocImportRequest),
    DocSet(DocSetRequest),
    DocGet(DocGetRequest),
    DocStartSync(DocStartSyncRequest),
    DocStopSync(DocStopSyncRequest),
    DocShare(DocShareRequest),
    DocSubscribe(DocSubscribeRequest),

    BytesGet(BytesGetRequest),

    Stats(StatsGetRequest),
}

/// The response enum, listing all possible responses.
#[allow(missing_docs, clippy::large_enum_variant)]
#[derive(Debug, Serialize, Deserialize, From, TryInto)]
pub enum ProviderResponse {
    Watch(WatchResponse),
    Version(VersionResponse),
    ListBlobs(ListBlobsResponse),
    ListIncompleteBlobs(ListIncompleteBlobsResponse),
    ListCollections(ListCollectionsResponse),
    Provide(ProvideProgress),
    Share(ShareProgress),
    Status(StatusResponse),
    Validate(ValidateProgress),
    Shutdown(()),

    AuthorList(RpcResult<AuthorListResponse>),
    AuthorCreate(RpcResult<AuthorCreateResponse>),
    AuthorImport(RpcResult<AuthorImportResponse>),

    DocInfo(RpcResult<DocInfoResponse>),
    DocList(RpcResult<DocListResponse>),
    DocCreate(RpcResult<DocCreateResponse>),
    DocImport(RpcResult<DocImportResponse>),
    DocSet(RpcResult<DocSetResponse>),
    DocGet(RpcResult<DocGetResponse>),
    DocShare(RpcResult<DocShareResponse>),
    DocStartSync(RpcResult<DocStartSyncResponse>),
    DocStopSync(RpcResult<DocStopSyncResponse>),
    DocSubscribe(RpcResult<DocSubscribeResponse>),

    BytesGet(RpcResult<BytesGetResponse>),

    Stats(RpcResult<StatsGetResponse>),
}

impl Service for ProviderService {
    type Req = ProviderRequest;
    type Res = ProviderResponse;
}
