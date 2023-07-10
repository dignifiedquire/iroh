//! The client side API
//!
//! To get data, create a connection using the `dial` function or use any quinn
//! connection that was obtained in another way.
//!
//! Create a request describing the data you want to get.
//!
//! Then create a state machine using [get_response_machine::start] and
//! drive it to completion by calling next on each state.
//!
//! For some states you have to provide additional arguments when calling next,
//! or you can choose to finish early.
use std::error::Error;
use std::fmt::{self, Debug};
use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bao_tree::io::error::DecodeError;
use bao_tree::io::DecodeResponseItem;
use bao_tree::outboard::PreOrderMemOutboard;
use bao_tree::{ByteNum, ChunkNum};
use bytes::BytesMut;
use iroh_net::tls::Keypair;
use iroh_net::{hp::derp::DerpMap, tls::PeerId};
use quinn::RecvStream;
use range_collections::RangeSet2;
use std::path::{Path, PathBuf};
use tracing::{debug, error};

pub use crate::util::Hash;

use crate::blobs::Collection;
use crate::protocol::{write_lp, AnyGetRequest, RangeSpecSeq};
use crate::tokio_util::{TrackingReader, TrackingWriter};
use crate::util::pathbuf_from_name;
use crate::IROH_BLOCK_SIZE;

/// Options for the client
#[derive(Clone, Debug)]
pub struct Options {
    /// The keypair of the node
    pub keypair: Keypair,
    /// The addresses to connect to
    pub addrs: Vec<SocketAddr>,
    /// The peer id to dial
    pub peer_id: PeerId,
    /// Whether to log the SSL keys when `SSLKEYLOGFILE` environment variable is set
    pub keylog: bool,
    /// The configuration of the derp services
    pub derp_map: Option<DerpMap>,
}

/// Stats about the transfer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stats {
    /// The number of bytes written
    pub bytes_written: u64,
    /// The number of bytes read
    pub bytes_read: u64,
    /// The time it took to transfer the data
    pub elapsed: Duration,
}

impl Stats {
    /// Transfer rate in megabits per second
    pub fn mbits(&self) -> f64 {
        let data_len_bit = self.bytes_read * 8;
        data_len_bit as f64 / (1000. * 1000.) / self.elapsed.as_secs_f64()
    }
}

/// Finite state machine for get responses
///
#[doc = include_str!("../docs/img/get_machine.drawio.svg")]
pub mod get_response_machine {
    use std::result;

    use crate::{
        protocol::{read_lp, GetRequest, NonEmptyRequestRangeSpecIter},
        tokio_util::ConcatenateSliceWriter,
    };

    use super::*;

    use bao_tree::io::{
        fsm::{ResponseDecoderReading, ResponseDecoderReadingNext, ResponseDecoderStart},
        Leaf, Parent,
    };
    use derive_more::From;
    use iroh_io::AsyncSliceWriter;

    self_cell::self_cell! {
        struct RangesIterInner {
            owner: RangeSpecSeq,
            #[covariant]
            dependent: NonEmptyRequestRangeSpecIter,
        }
    }

    /// The entry point of the get response machine
    pub fn start(connection: quinn::Connection, request: AnyGetRequest) -> AtInitial {
        AtInitial::new(connection, request)
    }

    /// Owned iterator for the ranges in a request
    ///
    /// We need an owned iterator for a fsm style API, otherwise we would have
    /// to drag a lifetime around every single state.
    struct RangesIter(RangesIterInner);

    impl fmt::Debug for RangesIter {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("RangesIter").finish()
        }
    }

    impl RangesIter {
        pub fn new(owner: RangeSpecSeq) -> Self {
            Self(RangesIterInner::new(owner, |owner| owner.iter_non_empty()))
        }
    }

    impl Iterator for RangesIter {
        type Item = (u64, RangeSet2<ChunkNum>);

        fn next(&mut self) -> Option<Self::Item> {
            self.0.with_dependent_mut(|_owner, iter| {
                iter.next()
                    .map(|(offset, ranges)| (offset, ranges.to_chunk_ranges()))
            })
        }
    }

    /// Initial state of the get response machine
    #[derive(Debug)]
    pub struct AtInitial {
        connection: quinn::Connection,
        request: AnyGetRequest,
    }

    impl AtInitial {
        /// Create a new get response
        ///
        /// `connection` is an existing connection
        /// `request` is the request to be sent
        pub fn new(connection: quinn::Connection, request: AnyGetRequest) -> Self {
            Self {
                connection,
                request,
            }
        }

        /// Initiate a new bidi stream to use for the get response
        pub async fn next(self) -> Result<AtConnected, quinn::ConnectionError> {
            let start = Instant::now();
            let (writer, reader) = self.connection.open_bi().await?;
            let reader = TrackingReader::new(reader);
            let writer = TrackingWriter::new(writer);
            Ok(AtConnected {
                start,
                reader,
                writer,
                request: self.request,
            })
        }
    }

    /// State of the get response machine after the handshake has been sent
    #[derive(Debug)]
    pub struct AtConnected {
        start: Instant,
        reader: TrackingReader<quinn::RecvStream>,
        writer: TrackingWriter<quinn::SendStream>,
        request: AnyGetRequest,
    }

    /// Possible next states after the handshake has been sent
    #[derive(Debug, From)]
    pub enum ConnectedNext {
        /// First response is either a collection or a single blob
        StartRoot(AtStartRoot),
        /// First response is a child
        StartChild(AtStartChild),
        /// Request is empty
        Closing(AtClosing),
    }

    impl AtConnected {
        /// Send the request and move to the next state
        ///
        /// The next state will be either `StartRoot` or `StartChild` depending on whether
        /// the request requests part of the collection or not.
        ///
        /// If the request is empty, this can also move directly to `Finished`.
        pub async fn next(self) -> Result<ConnectedNext, GetResponseError> {
            let Self {
                start,
                mut reader,
                mut writer,
                request,
            } = self;
            // 1. Send Request
            {
                debug!("sending request");
                // wrap the get request in a request so we can serialize it
                let request_bytes = postcard::to_stdvec(&request)?;
                write_lp(&mut writer, &request_bytes).await?;
            }

            // 2. Finish writing before expecting a response
            let (mut writer, bytes_written) = writer.into_parts();
            writer.finish().await?;

            // 3. Turn a possible custom request into a get request
            let request = match request {
                AnyGetRequest::Get(get_request) => {
                    // we already have a get request, just return it
                    get_request
                }
                AnyGetRequest::CustomGet(_) => {
                    // we sent a custom request, so we need the actual GetRequest from the response
                    let mut buffer = BytesMut::new();
                    let response = read_lp(&mut reader, &mut buffer)
                        .await?
                        .context("unexpected EOF when reading response to custom get request")?;
                    postcard::from_bytes::<GetRequest>(&response).context(
                        "unable to deserialize response to custom get request as get request",
                    )?
                }
            };
            let hash = request.hash;
            let ranges_iter = RangesIter::new(request.ranges);
            // this is in a box so we don't have to memcpy it on every state transition
            let mut misc = Box::new(Misc {
                start,
                bytes_written,
                ranges_iter,
            });
            Ok(match misc.ranges_iter.next() {
                Some((offset, ranges)) => {
                    if offset == 0 {
                        AtStartRoot {
                            reader,
                            ranges,
                            misc,
                            hash,
                        }
                        .into()
                    } else {
                        AtStartChild {
                            reader,
                            ranges,
                            misc,
                            child_offset: offset - 1,
                        }
                        .into()
                    }
                }
                None => AtClosing::new(misc, reader).into(),
            })
        }
    }

    /// State of the get response when we start reading a collection
    #[derive(Debug)]
    pub struct AtStartRoot {
        ranges: RangeSet2<ChunkNum>,
        reader: TrackingReader<quinn::RecvStream>,
        misc: Box<Misc>,
        hash: Hash,
    }

    /// State of the get response when we start reading a child
    #[derive(Debug)]
    pub struct AtStartChild {
        ranges: RangeSet2<ChunkNum>,
        reader: TrackingReader<quinn::RecvStream>,
        misc: Box<Misc>,
        child_offset: u64,
    }

    impl AtStartChild {
        /// The offset of the child we are currently reading
        ///
        /// This must be used to determine the hash needed to call next.
        /// If this is larger than the number of children in the collection,
        /// you can call finish to stop reading the response.
        pub fn child_offset(&self) -> u64 {
            self.child_offset
        }

        /// The ranges we have requested for the child
        pub fn ranges(&self) -> &RangeSet2<ChunkNum> {
            &self.ranges
        }

        /// Go into the next state, reading the header
        ///
        /// This requires passing in the hash of the child for validation
        pub fn next(self, hash: Hash) -> AtBlobHeader {
            let stream = ResponseDecoderStart::<TrackingReader<RecvStream>>::new(
                hash.into(),
                self.ranges,
                IROH_BLOCK_SIZE,
                self.reader,
            );
            AtBlobHeader {
                stream,
                misc: self.misc,
            }
        }

        /// Finish the get response without reading further
        ///
        /// This is used if you know that there are no more children from having
        /// read the collection, or when you want to stop reading the response
        /// early.
        pub fn finish(self) -> AtClosing {
            AtClosing::new(self.misc, self.reader)
        }
    }

    impl AtStartRoot {
        /// The ranges we have requested for the child
        pub fn ranges(&self) -> &RangeSet2<ChunkNum> {
            &self.ranges
        }

        /// Go into the next state, reading the header
        ///
        /// For the collection we already know the hash, since it was part of the request
        pub fn next(self) -> AtBlobHeader {
            let stream = ResponseDecoderStart::new(
                self.hash.into(),
                self.ranges,
                IROH_BLOCK_SIZE,
                self.reader,
            );
            AtBlobHeader {
                stream,
                misc: self.misc,
            }
        }

        /// Finish the get response without reading further
        pub fn finish(self) -> AtClosing {
            AtClosing::new(self.misc, self.reader)
        }
    }

    /// State before reading a size header
    #[derive(Debug)]
    pub struct AtBlobHeader {
        stream: ResponseDecoderStart<TrackingReader<RecvStream>>,
        misc: Box<Misc>,
    }

    impl AtBlobHeader {
        /// Read the size header, returning it and going into the `Content` state.
        pub async fn next(self) -> Result<(AtBlobContent, u64), std::io::Error> {
            let (stream, size) = self.stream.next().await?;
            Ok((
                AtBlobContent {
                    stream,
                    misc: self.misc,
                },
                size,
            ))
        }

        /// Drain the response and throw away the result
        pub async fn drain(self) -> result::Result<AtEndBlob, DecodeError> {
            let (mut content, _size) = self.next().await?;
            loop {
                match content.next().await {
                    BlobContentNext::More((content1, Ok(_))) => {
                        content = content1;
                    }
                    BlobContentNext::More((_, Err(e))) => {
                        return Err(e);
                    }
                    BlobContentNext::Done(end) => {
                        return Ok(end);
                    }
                }
            }
        }

        /// Concatenate the entire response into a vec
        ///
        /// For a request that does not request the complete blob, this will just
        /// concatenate the ranges that were requested.
        pub async fn concatenate_into_vec(
            self,
        ) -> result::Result<(AtEndBlob, Vec<u8>), DecodeError> {
            let (curr, size) = self.next().await?;
            let res = Vec::with_capacity(size as usize);
            let mut writer = ConcatenateSliceWriter::new(res);
            let res = curr.write_all(&mut writer).await?;
            Ok((res, writer.into_inner()))
        }

        /// Write the entire blob to a slice writer
        pub async fn write_all<D: AsyncSliceWriter>(
            self,
            data: &mut D,
        ) -> result::Result<AtEndBlob, DecodeError> {
            self.write_all_with_outboard::<D, D>(&mut None, data).await
        }

        /// Write the entire blob to a slice writer, optionally also writing
        /// an outboard.
        pub async fn write_all_with_outboard<D, O>(
            self,
            outboard: &mut Option<O>,
            data: &mut D,
        ) -> result::Result<AtEndBlob, DecodeError>
        where
            D: AsyncSliceWriter,
            O: AsyncSliceWriter,
        {
            let (content, size) = self.next().await?;
            if let Some(o) = outboard.as_mut() {
                o.write_at(0, &size.to_le_bytes()).await?;
            }
            content.write_all_with_outboard(outboard, data).await
        }
    }

    /// State while we are reading content
    #[derive(Debug)]
    pub struct AtBlobContent {
        stream: ResponseDecoderReading<TrackingReader<RecvStream>>,
        misc: Box<Misc>,
    }

    /// Bao content item
    #[derive(Debug)]
    pub enum BaoContentItem {
        /// A parent node
        Parent(Parent),
        /// A leaf node containing data
        Leaf(Leaf),
    }

    impl TryFrom<DecodeResponseItem> for BaoContentItem {
        type Error = bao_tree::io::Header;

        fn try_from(item: DecodeResponseItem) -> result::Result<Self, Self::Error> {
            match item {
                DecodeResponseItem::Parent(p) => Ok(BaoContentItem::Parent(p)),
                DecodeResponseItem::Leaf(l) => Ok(BaoContentItem::Leaf(l)),
                DecodeResponseItem::Header(h) => Err(h),
            }
        }
    }

    /// The next state after reading a content item
    #[derive(Debug, From)]
    pub enum BlobContentNext {
        /// We expect more content
        More((AtBlobContent, result::Result<BaoContentItem, DecodeError>)),
        /// We are done with this blob
        Done(AtEndBlob),
    }

    impl AtBlobContent {
        /// Read the next item, either content, an error, or the end of the blob
        pub async fn next(self) -> BlobContentNext {
            match self.stream.next().await {
                ResponseDecoderReadingNext::More((stream, res)) => {
                    let next = Self { stream, ..self };
                    let res = res.map(|x| x.try_into().unwrap());
                    (next, res).into()
                }
                ResponseDecoderReadingNext::Done(stream) => AtEndBlob {
                    stream,
                    misc: self.misc,
                }
                .into(),
            }
        }

        /// Write the entire blob to a slice writer
        pub async fn write_all<D: AsyncSliceWriter>(
            self,
            data: &mut D,
        ) -> result::Result<AtEndBlob, DecodeError> {
            self.write_all_with_outboard::<D, D>(&mut None, data).await
        }

        /// Write the entire blob to a slice writer, optionally also writing
        /// an outboard.
        pub async fn write_all_with_outboard<D, O>(
            self,
            outboard: &mut Option<O>,
            data: &mut D,
        ) -> result::Result<AtEndBlob, DecodeError>
        where
            D: AsyncSliceWriter,
            O: AsyncSliceWriter,
        {
            let mut content = self;
            loop {
                match content.next().await {
                    BlobContentNext::More((content1, item)) => {
                        content = content1;
                        match item? {
                            BaoContentItem::Parent(parent) => {
                                if let Some(outboard) = outboard.as_mut() {
                                    let offset = parent.node.post_order_offset() * 64 + 8;
                                    let (l_hash, r_hash) = parent.pair;
                                    outboard.write_at(offset, l_hash.as_bytes()).await?;
                                    outboard.write_at(offset + 32, r_hash.as_bytes()).await?;
                                }
                            }
                            BaoContentItem::Leaf(leaf) => {
                                data.write_bytes_at(leaf.offset.0, leaf.data).await?;
                            }
                        }
                    }
                    BlobContentNext::Done(end) => {
                        return Ok(end);
                    }
                }
            }
        }
    }

    /// State after we have read all the content for a blob
    #[derive(Debug)]
    pub struct AtEndBlob {
        stream: TrackingReader<RecvStream>,
        misc: Box<Misc>,
    }

    /// The next state after the end of a blob
    #[derive(Debug, From)]
    pub enum EndBlobNext {
        /// Response is expected to have more children
        MoreChildren(AtStartChild),
        /// No more children expected
        Closing(AtClosing),
    }

    impl AtEndBlob {
        /// Read the next child, or finish
        pub fn next(mut self) -> EndBlobNext {
            if let Some((offset, ranges)) = self.misc.ranges_iter.next() {
                AtStartChild {
                    reader: self.stream,
                    child_offset: offset - 1,
                    ranges,
                    misc: self.misc,
                }
                .into()
            } else {
                AtClosing::new(self.misc, self.stream).into()
            }
        }
    }

    /// State when finishing the get response
    #[derive(Debug)]
    pub struct AtClosing {
        misc: Box<Misc>,
        reader: TrackingReader<RecvStream>,
    }

    impl AtClosing {
        fn new(misc: Box<Misc>, reader: TrackingReader<RecvStream>) -> Self {
            Self { misc, reader }
        }

        /// Finish the get response, returning statistics
        pub async fn next(self) -> result::Result<Stats, std::io::Error> {
            // Shut down the stream
            let (mut reader, bytes_read) = self.reader.into_parts();
            if let Some(chunk) = reader.read_chunk(8, false).await? {
                reader.stop(0u8.into()).ok();
                error!("Received unexpected data from the provider: {chunk:?}");
            }
            Ok(Stats {
                elapsed: self.misc.start.elapsed(),
                bytes_written: self.misc.bytes_written,
                bytes_read,
            })
        }
    }

    /// Stuff we need to hold on to while going through the machine states
    #[derive(Debug)]
    struct Misc {
        /// start time for statistics
        start: Instant,
        /// bytes written for statistics
        bytes_written: u64,
        /// iterator over the ranges of the collection and the children
        ranges_iter: RangesIter,
    }
}

/// Create a new endpoint and dial a peer, returning the connection
///
/// Note that this will create an entirely new endpoint, so it should be only
/// used for short lived connections. If you want to connect to multiple peers,
/// it is preferable to create an endpoint and use `connect` on the endpoint.
pub async fn dial(opts: Options) -> anyhow::Result<quinn::Connection> {
    let endpoint = iroh_net::MagicEndpoint::builder()
        .keypair(opts.keypair)
        .derp_map(opts.derp_map)
        .keylog(opts.keylog)
        .bind(0)
        .await?;
    endpoint
        .connect(opts.peer_id, &crate::protocol::ALPN, &opts.addrs)
        .await
        .context("failed to connect to provider")
}

/// Error when processing a response
#[derive(thiserror::Error, Debug)]
pub enum GetResponseError {
    /// Error when opening a stream
    #[error("connection: {0}")]
    Connection(#[from] quinn::ConnectionError),
    /// Error when writing the handshake or request to the stream
    #[error("write: {0}")]
    Write(#[from] quinn::WriteError),
    /// Error when reading from the stream
    #[error("read: {0}")]
    Read(#[from] quinn::ReadError),
    /// Error when decoding, e.g. hash mismatch
    #[error("decode: {0}")]
    Decode(bao_tree::io::error::DecodeError),
    /// A generic error
    #[error("generic: {0}")]
    Generic(anyhow::Error),
}

impl From<postcard::Error> for GetResponseError {
    fn from(cause: postcard::Error) -> Self {
        Self::Generic(cause.into())
    }
}

impl From<bao_tree::io::error::DecodeError> for GetResponseError {
    fn from(cause: bao_tree::io::error::DecodeError) -> Self {
        match cause {
            bao_tree::io::error::DecodeError::Io(cause) => {
                // try to downcast to specific quinn errors
                if let Some(source) = cause.source() {
                    if let Some(error) = source.downcast_ref::<quinn::ConnectionError>() {
                        return Self::Connection(error.clone());
                    }
                    if let Some(error) = source.downcast_ref::<quinn::ReadError>() {
                        return Self::Read(error.clone());
                    }
                    if let Some(error) = source.downcast_ref::<quinn::WriteError>() {
                        return Self::Write(error.clone());
                    }
                }
                Self::Generic(cause.into())
            }
            _ => Self::Decode(cause),
        }
    }
}

impl From<anyhow::Error> for GetResponseError {
    fn from(cause: anyhow::Error) -> Self {
        Self::Generic(cause)
    }
}

/// Get missing range for a single file, given a temp and target directory
///
/// This will check missing ranges from the outboard, but for the data file itself
/// just use the length of the file.
pub fn get_missing_range(
    hash: &Hash,
    name: &str,
    temp_dir: &Path,
    target_dir: &Path,
) -> std::io::Result<RangeSet2<ChunkNum>> {
    if target_dir.exists() && !temp_dir.exists() {
        // target directory exists yet does not contain the temp dir
        // refuse to continue
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Target directory exists but does not contain temp directory",
        ));
    }
    let range = get_missing_range_impl(hash, name, temp_dir, target_dir)?;
    Ok(range)
}

/// Get missing range for a single file
fn get_missing_range_impl(
    hash: &Hash,
    name: &str,
    temp_dir: &Path,
    target_dir: &Path,
) -> std::io::Result<RangeSet2<ChunkNum>> {
    let paths = FilePaths::new(hash, name, temp_dir, target_dir);
    Ok(if paths.is_final() {
        tracing::debug!("Found final file: {:?}", paths.target);
        // we assume that the file is correct
        RangeSet2::empty()
    } else if paths.is_incomplete() {
        tracing::debug!("Found incomplete file: {:?}", paths.temp);
        // we got incomplete data
        let outboard = std::fs::read(&paths.outboard)?;
        let outboard = PreOrderMemOutboard::new((*hash).into(), IROH_BLOCK_SIZE, outboard);
        match outboard {
            Ok(outboard) => {
                // compute set of valid ranges from the outboard and the file
                //
                // We assume that the file is correct and does not contain holes.
                // Otherwise, we would have to rehash the file.
                //
                // Do a quick check of the outboard in case something went wrong when writing.
                let mut valid = bao_tree::io::sync::valid_ranges(&outboard)?;
                let valid_from_file =
                    RangeSet2::from(..ByteNum(paths.temp.metadata()?.len()).full_chunks());
                tracing::debug!("valid_from_file: {:?}", valid_from_file);
                tracing::debug!("valid_from_outboard: {:?}", valid);
                valid &= valid_from_file;
                RangeSet2::all().difference(&valid)
            }
            Err(cause) => {
                tracing::debug!("Outboard damaged, assuming missing {cause:?}");
                // the outboard is invalid, so we assume that the file is missing
                RangeSet2::all()
            }
        }
    } else {
        tracing::debug!("Found missing file: {:?}", paths.target);
        // we don't know anything about this file, so we assume it's missing
        RangeSet2::all()
    })
}

/// Given a target directory and a temp directory, get a set of ranges that we are missing
///
/// Assumes that the temp directory contains at least the data for the collection.
/// Also assumes that partial data files do not contain gaps.
pub fn get_missing_ranges(
    hash: Hash,
    target_dir: &Path,
    temp_dir: &Path,
) -> anyhow::Result<(RangeSpecSeq, Option<Collection>)> {
    if target_dir.exists() && !temp_dir.exists() {
        // target directory exists yet does not contain the temp dir
        // refuse to continue
        anyhow::bail!("Target directory exists but does not contain temp directory");
    }
    // try to load the collection from the temp directory
    //
    // if the collection can not be deserialized, we treat it as if it does not exist
    let collection = load_collection(temp_dir, hash).ok().flatten();
    let collection = match collection {
        Some(collection) => collection,
        None => return Ok((RangeSpecSeq::all(), None)),
    };
    let mut ranges = collection
        .blobs()
        .iter()
        .map(|blob| get_missing_range_impl(&blob.hash, blob.name.as_str(), temp_dir, target_dir))
        .collect::<std::io::Result<Vec<_>>>()?;
    ranges
        .iter()
        .zip(collection.blobs())
        .for_each(|(ranges, blob)| {
            if ranges.is_empty() {
                tracing::debug!("{} is complete", blob.name);
            } else if ranges.is_all() {
                tracing::debug!("{} is missing", blob.name);
            } else {
                tracing::debug!("{} is partial {:?}", blob.name, ranges);
            }
        });
    // make room for the collection at offset 0
    // if we get here, we already have the collection, so we don't need to ask for it again.
    ranges.insert(0, RangeSet2::empty());
    Ok((RangeSpecSeq::new(ranges), Some(collection)))
}

#[derive(Debug)]
struct FilePaths {
    target: PathBuf,
    temp: PathBuf,
    outboard: PathBuf,
}

impl FilePaths {
    fn new(hash: &Hash, name: &str, temp_dir: &Path, target_dir: &Path) -> Self {
        let target = target_dir.join(pathbuf_from_name(name));
        let hash = blake3::Hash::from(*hash).to_hex();
        let temp = temp_dir.join(format!("{hash}.data.part"));
        let outboard = temp_dir.join(format!("{hash}.outboard.part"));
        Self {
            target,
            temp,
            outboard,
        }
    }

    fn is_final(&self) -> bool {
        self.target.exists()
    }

    fn is_incomplete(&self) -> bool {
        self.temp.exists() && self.outboard.exists()
    }
}

/// get data path for a hash
pub fn get_data_path(data_path: &Path, hash: Hash) -> PathBuf {
    let hash = blake3::Hash::from(hash).to_hex();
    data_path.join(format!("{hash}.data"))
}

/// Load a collection from a data path
fn load_collection(data_path: &Path, hash: Hash) -> anyhow::Result<Option<Collection>> {
    let collection_path = get_data_path(data_path, hash);
    Ok(if collection_path.exists() {
        let collection = std::fs::read(&collection_path)?;
        let collection = Collection::from_bytes(&collection)?;
        Some(collection)
    } else {
        None
    })
}
