use std::{
    fmt::Debug,
    path::{Path, PathBuf},
    pin::Pin,
};

use anyhow::{anyhow, ensure, Result};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use cid::{multihash::MultihashDigest, Cid};
use futures::{stream::LocalBoxStream, Stream, StreamExt};
use iroh_rpc_client::Client;
use prost::Message;
use tokio::io::AsyncRead;

use crate::{
    balanced_tree::TreeBuilder,
    chunker::{self, Chunker, DEFAULT_CHUNK_SIZE_LIMIT},
    codecs::Codec,
    unixfs::{dag_pb, unixfs_pb, DataType, Node, UnixfsNode},
};

// CID_LEN is the length of a cid
const CID_LEN: usize = 64;

// the PBNode.Data field may contain some data, this buffer is to ensure that
// the contents of that data field + the size of the encoded protobuf links
// will not be larger than the chunk size
const BUFFER: usize = 1024;

#[derive(Debug, PartialEq)]
enum DirectoryType {
    Basic,
    // TODO: writing hamt sharding not yet implemented
    Hamt,
}

/// Entry is the kind of entry in a directory can be either a file or a
/// folder (if recursive directories are allowed)
#[derive(Debug)]
enum Entry {
    File(File),
    Directory(Directory),
}

/// Construct a UnixFS directory.
#[derive(Debug)]
pub struct DirectoryBuilder {
    name: Option<String>,
    entries: Vec<Entry>,
    typ: DirectoryType,
    // estimated_size is used to compare the size of the directory
    // to the chunk size.
    // When the number of links in a directory get so large that the
    // links themselves no longer fit into one chunk
    // we must use HAMT sharding to represent that directory.
    // max size that the directory can be before it should be created
    // as a HAMT, rather than a Basic directory
    estimated_size: usize,
    chunker: Chunker,
}

impl Default for DirectoryBuilder {
    fn default() -> Self {
        Self {
            name: None,
            entries: Default::default(),
            typ: DirectoryType::Basic,
            estimated_size: 0,
            chunker: Chunker::fixed_size(),
        }
    }
}

impl DirectoryBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = Some(name.into());
        self
    }

    pub fn add_dir(&mut self, dir: Directory) -> &mut Self {
        let name = dir.name();
        self.estimated_size += self.estimated_size + name.len() + CID_LEN;
        if self.typ == DirectoryType::Basic
            && self.estimated_size + BUFFER > self.chunker.chunk_size()
        {
            self.typ = DirectoryType::Hamt;
        }
        self.entries.push(Entry::Directory(dir));
        self
    }

    pub fn add_file(&mut self, file: File) -> &mut Self {
        let name = file.name();
        self.estimated_size += self.estimated_size + name.len() + CID_LEN;
        if self.typ == DirectoryType::Basic
            && self.estimated_size + BUFFER > self.chunker.chunk_size()
        {
            self.typ = DirectoryType::Hamt;
        }
        self.entries.push(Entry::File(file));
        self
    }

    pub async fn build(self) -> Result<Directory> {
        if self.typ == DirectoryType::Hamt {
            anyhow::bail!("too many links to fit into one chunk, must be encoded as a HAMT. However, HAMT creation has not yet been implemented.");
        }
        let DirectoryBuilder { name, entries, .. } = self;

        let name = name.unwrap_or_default();

        Ok(Directory { name, entries })
    }
}

/// Representation of a constructed Directory.
#[derive(Debug)]
pub struct Directory {
    name: String,
    entries: Vec<Entry>,
}

impl Directory {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub async fn encode_root(self) -> Result<(Cid, Bytes)> {
        let mut current = None;
        let parts = self.encode();
        tokio::pin!(parts);

        while let Some(part) = parts.next().await {
            current = Some(part);
        }

        current.expect("must not be empty")
    }

    pub fn encode<'a>(self) -> LocalBoxStream<'a, Result<(Cid, Bytes)>> {
        async_stream::try_stream! {
            let mut links = Vec::new();
            for entry in self.entries {
                let (name, root) = match entry {
                    Entry::File(file) => {
                        let name = file.name();
                        let parts = file.encode();
                        tokio::pin!(parts);
                        let mut root = None;
                        while let Some(part) = parts.next().await {
                            let (cid, bytes) = part?;
                            root = Some((cid, bytes.clone()));
                            yield (cid, bytes);
                        }
                         (name, root)
                    }
                    Entry::Directory(dir) => {
                        let name = dir.name.clone();
                        let parts = dir.encode();
                        tokio::pin!(parts);
                        let mut root = None;
                        while let Some(part) = parts.next().await {
                            let (cid, bytes) = part?;
                            root = Some((cid, bytes.clone()));
                            yield (cid, bytes);
                        }
                         (name, root)
                    }
                };
                let (cid, bytes) = root.expect("file must not be empty");
                links.push(dag_pb::PbLink {
                    hash: Some(cid.to_bytes()),
                    name: Some(name),
                    tsize: Some(bytes.len() as u64),
                });

            }

            // directory itself comes last
            let inner = unixfs_pb::Data {
                r#type: DataType::Directory as i32,
                ..Default::default()
            };
            let outer = encode_unixfs_pb(&inner, links)?;

            let node = UnixfsNode::Directory(Node { outer, inner });
            yield node.encode()?;
        }
        .boxed_local()
    }
}

/// Constructs a UnixFS file.
pub struct FileBuilder {
    name: Option<String>,
    content: Option<Pin<Box<dyn AsyncRead>>>,
    chunker: Chunker,
    path: Option<PathBuf>,
}

impl Debug for FileBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let content = if self.content.is_some() {
            "Some(Box<AsyncRead>)"
        } else {
            "None"
        };

        f.debug_struct("FileBuilder")
            .field("name", &self.name)
            .field("content", &content)
            .field("chunker", &self.chunker)
            .finish()
    }
}

impl Default for FileBuilder {
    fn default() -> Self {
        Self {
            name: None,
            content: None,
            chunker: chunker::Chunker::fixed_size(),
            path: None,
        }
    }
}

/// FileBuilder separates uses a reader or bytes to chunk the data into raw unixfs nodes
impl FileBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_path(&mut self, path: PathBuf) -> &mut Self {
        self.path = Some(path);
        self
    }

    pub fn name(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = Some(name.into());
        self
    }

    pub fn content_bytes<B: Into<Bytes>>(&mut self, content: B) -> &mut Self {
        let bytes = content.into();
        self.content = Some(Box::pin(std::io::Cursor::new(bytes)));
        self
    }

    pub fn content_reader<T: tokio::io::AsyncRead + 'static>(&mut self, content: T) -> &mut Self {
        self.content = Some(Box::pin(content));
        self
    }

    pub async fn build(self) -> Result<File> {
        // encodes files as raw

        let name = self.name.ok_or_else(|| anyhow!("missing name"))?;
        let reader = self.content.ok_or_else(|| anyhow!("missing content"))?;

        Ok(File {
            name,
            nodes: Box::pin(self.chunker.chunks(reader)),
            tree_builder: TreeBuilder::balanced_tree(),
        })
    }
}
/// Representation of a constructed File.
pub struct File {
    name: String,
    nodes: Pin<Box<dyn Stream<Item = std::io::Result<BytesMut>>>>,
    tree_builder: TreeBuilder,
}

impl Debug for File {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("File")
            .field("name", &self.name)
            .field("nodes", &"Stream<Item = Result<UnixfsNode>>")
            .finish()
    }
}

/// A File that has been encoded into serialized UnixFS.
#[derive(Debug)]
pub enum EncodedFile {
    Raw(Bytes),
    Chunked { root: Bytes, leaves: Vec<Bytes> },
}

impl EncodedFile {
    pub fn root(&self) -> &Bytes {
        match self {
            EncodedFile::Raw(r) => r,
            EncodedFile::Chunked { root, .. } => root,
        }
    }

    pub fn root_cid(&self) -> Cid {
        let root = self.root();
        let hash = cid::multihash::Code::Sha2_256.digest(root);
        let codec = match self {
            EncodedFile::Raw(_) => Codec::Raw,
            EncodedFile::Chunked { .. } => Codec::Sha2256,
        };

        Cid::new_v1(codec as _, hash)
    }

    pub fn leave_cids(&self) -> Option<Vec<Cid>> {
        match self {
            EncodedFile::Raw(_) => None,
            EncodedFile::Chunked { leaves, .. } => {
                let cids = leaves
                    .iter()
                    .map(|l| Cid::new_v1(Codec::Raw as _, cid::multihash::Code::Sha2_256.digest(l)))
                    .collect();
                Some(cids)
            }
        }
    }
}

impl File {
    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub async fn encode_root(self) -> Result<(Cid, Bytes)> {
        let mut current = None;
        let parts = self.encode();
        tokio::pin!(parts);

        while let Some(part) = parts.next().await {
            current = Some(part);
        }

        current.expect("must not be empty")
    }

    pub fn encode(self) -> impl Stream<Item = Result<(Cid, Bytes)>> {
        self.tree_builder.stream_tree(self.nodes)
    }
}

pub(crate) fn encode_unixfs_pb(
    inner: &unixfs_pb::Data,
    links: Vec<dag_pb::PbLink>,
) -> Result<dag_pb::PbNode> {
    let data = inner.encode_to_vec();
    ensure!(
        data.len() <= DEFAULT_CHUNK_SIZE_LIMIT,
        "node is too large: {} bytes",
        data.len()
    );

    Ok(dag_pb::PbNode {
        links,
        data: Some(data.into()),
    })
}

#[async_trait]
pub trait Store {
    async fn put(&self, cid: Cid, blob: Bytes, links: Vec<Cid>) -> Result<()>;
}

#[async_trait]
impl Store for &Client {
    async fn put(&self, cid: Cid, blob: Bytes, links: Vec<Cid>) -> Result<()> {
        self.try_store()?.put(cid, blob, links).await
    }
}

#[async_trait]
impl Store for &tokio::sync::Mutex<std::collections::HashMap<Cid, Bytes>> {
    async fn put(&self, cid: Cid, blob: Bytes, _links: Vec<Cid>) -> Result<()> {
        self.lock().await.insert(cid, blob);
        Ok(())
    }
}

/// Adds a single file.
/// - storing the content using `rpc.store`
/// - returns the root Cid
/// - wraps into a UnixFs directory to preserve the filename
pub async fn add_file<S: Store>(path: &Path, rpc: Option<S>) -> Result<Cid> {
    ensure!(path.is_file(), "provided path was not a file");

    // wrap file in dir to preserve file name
    let mut dir = DirectoryBuilder::new();
    dir.name("");
    let mut file = FileBuilder::new();
    file.name(
        path.file_name()
            .and_then(|s| s.to_str())
            .unwrap_or_default(),
    );
    let f = tokio::fs::File::open(path).await?;
    let buf = tokio::io::BufReader::new(f);
    file.content_reader(buf);
    let file = file.build().await?;
    dir.add_file(file);

    let dir = dir.build().await?;

    // encode and store
    let mut root = None;
    let parts = dir.encode();
    tokio::pin!(parts);

    while let Some(part) = parts.next().await {
        let (cid, bytes) = part?;
        if let Some(ref rpc) = rpc {
            rpc.put(cid, bytes, vec![]).await?;
        }
        root = Some(cid);
    }

    Ok(root.expect("missing root"))
}

/// Adds a directory.
/// - storing the content using `rpc.store`
/// - returns the root Cid
/// - wraps into a UnixFs directory to preserve the filename
pub async fn add_dir<S: Store>(path: &Path, rpc: Option<S>, _recursive: bool) -> Result<Cid> {
    ensure!(path.is_dir(), "provided path was not a directory");

    // wrap dir in dir to preserve file name
    let mut wrap = DirectoryBuilder::new();
    wrap.name("");
    let mut dir = DirectoryBuilder::new();
    dir.name(
        path.file_name()
            .and_then(|s| s.to_str())
            .unwrap_or_default(),
    );

    // iterate through dir
    // if also a dir, check if recursive == false, if so, error
    //
    let dir = dir.build().await?;
    wrap.add_dir(dir);

    let wrap = wrap.build().await?;

    // encode and store
    let mut root = None;
    let parts = wrap.encode();
    tokio::pin!(parts);

    while let Some(part) = parts.next().await {
        let (cid, bytes) = part?;
        if let Some(ref rpc) = rpc {
            rpc.put(cid, bytes, vec![]).await?;
        }
        root = Some(cid);
    }

    Ok(root.expect("missing root"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use futures::TryStreamExt;

    #[tokio::test]
    async fn test_builder_basics() -> Result<()> {
        // Create a directory
        let mut dir = DirectoryBuilder::new();
        dir.name("foo");

        // Add a file
        let mut bar = FileBuilder::new();
        bar.name("bar.txt").content_bytes(b"bar".to_vec());
        let bar = bar.build().await?;
        let bar_encoded: Vec<_> = {
            let mut bar = FileBuilder::new();
            bar.name("bar.txt").content_bytes(b"bar".to_vec());
            let bar = bar.build().await?;
            bar.encode().try_collect().await?
        };
        assert_eq!(bar_encoded.len(), 1);

        // Add a file
        let mut baz = FileBuilder::new();
        baz.name("baz.txt").content_bytes(b"baz".to_vec());
        let baz = baz.build().await?;
        let baz_encoded: Vec<_> = {
            let mut baz = FileBuilder::new();
            baz.name("baz.txt").content_bytes(b"baz".to_vec());
            let baz = baz.build().await?;
            baz.encode().try_collect().await?
        };
        assert_eq!(baz_encoded.len(), 1);

        dir.add_file(bar).add_file(baz);

        let dir = dir.build().await?;

        let (cid_dir, dir_encoded) = dir.encode_root().await?;
        let decoded_dir = UnixfsNode::decode(&cid_dir, dir_encoded)?;

        let links = decoded_dir.links().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(links[0].name.unwrap(), "bar.txt");
        assert_eq!(links[0].cid, bar_encoded[0].0);
        assert_eq!(links[1].name.unwrap(), "baz.txt");
        assert_eq!(links[1].cid, baz_encoded[0].0);

        // TODO: check content
        // TODO: add nested directory

        Ok(())
    }

    #[tokio::test]
    async fn test_builder_stream_small() -> Result<()> {
        // Create a directory
        let mut dir = DirectoryBuilder::new();
        dir.name("foo");

        // Add a file
        let mut bar = FileBuilder::new();
        let bar_reader = std::io::Cursor::new(b"bar");
        bar.name("bar.txt").content_reader(bar_reader);
        let bar = bar.build().await?;
        let bar_encoded: Vec<_> = {
            let mut bar = FileBuilder::new();
            let bar_reader = std::io::Cursor::new(b"bar");
            bar.name("bar.txt").content_reader(bar_reader);
            let bar = bar.build().await?;
            bar.encode().try_collect().await?
        };
        assert_eq!(bar_encoded.len(), 1);

        // Add a file
        let mut baz = FileBuilder::new();
        let baz_reader = std::io::Cursor::new(b"bazz");
        baz.name("baz.txt").content_reader(baz_reader);
        let baz = baz.build().await?;
        let baz_encoded: Vec<_> = {
            let mut baz = FileBuilder::new();
            let baz_reader = std::io::Cursor::new(b"bazz");
            baz.name("baz.txt").content_reader(baz_reader);
            let baz = baz.build().await?;
            baz.encode().try_collect().await?
        };
        assert_eq!(baz_encoded.len(), 1);

        dir.add_file(bar).add_file(baz);

        let dir = dir.build().await?;

        let (cid_dir, dir_encoded) = dir.encode_root().await?;
        let decoded_dir = UnixfsNode::decode(&cid_dir, dir_encoded)?;

        let links = decoded_dir.links().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(links[0].name.unwrap(), "bar.txt");
        assert_eq!(links[0].cid, bar_encoded[0].0);
        assert_eq!(links[1].name.unwrap(), "baz.txt");
        assert_eq!(links[1].cid, baz_encoded[0].0);

        // TODO: check content
        // TODO: add nested directory

        Ok(())
    }

    #[tokio::test]
    async fn test_builder_stream_large() -> Result<()> {
        // Create a directory
        let mut dir = DirectoryBuilder::new();
        dir.name("foo");

        // Add a file
        let mut bar = FileBuilder::new();
        let bar_reader = std::io::Cursor::new(vec![1u8; 1024 * 1024]);
        bar.name("bar.txt").content_reader(bar_reader);
        let bar = bar.build().await?;
        let bar_encoded: Vec<_> = {
            let mut bar = FileBuilder::new();
            let bar_reader = std::io::Cursor::new(vec![1u8; 1024 * 1024]);
            bar.name("bar.txt").content_reader(bar_reader);
            let bar = bar.build().await?;
            bar.encode().try_collect().await?
        };
        assert_eq!(bar_encoded.len(), 5);

        // Add a file
        let mut baz = FileBuilder::new();
        let mut baz_content = Vec::with_capacity(1024 * 1024 * 2);
        for i in 0..2 {
            for _ in 0..(1024 * 1024) {
                baz_content.push(i);
            }
        }

        let baz_reader = std::io::Cursor::new(baz_content.clone());
        baz.name("baz.txt").content_reader(baz_reader);
        let baz = baz.build().await?;
        let baz_encoded: Vec<_> = {
            let mut baz = FileBuilder::new();
            let baz_reader = std::io::Cursor::new(baz_content);
            baz.name("baz.txt").content_reader(baz_reader);
            let baz = baz.build().await?;
            baz.encode().try_collect().await?
        };
        assert_eq!(baz_encoded.len(), 9);

        dir.add_file(bar).add_file(baz);

        let dir = dir.build().await?;

        let (cid_dir, dir_encoded) = dir.encode_root().await?;
        let decoded_dir = UnixfsNode::decode(&cid_dir, dir_encoded)?;

        let links = decoded_dir.links().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(links[0].name.unwrap(), "bar.txt");
        assert_eq!(links[0].cid, bar_encoded[4].0);
        assert_eq!(links[1].name.unwrap(), "baz.txt");
        assert_eq!(links[1].cid, baz_encoded[8].0);

        for (i, encoded) in baz_encoded.iter().enumerate() {
            let node = UnixfsNode::decode(&encoded.0, encoded.1.clone())?;
            if i == 8 {
                assert_eq!(node.typ(), Some(DataType::File));
                assert_eq!(node.links().count(), 8);
            } else {
                assert_eq!(node.typ(), None); // raw leaves
                assert_eq!(node.size(), Some(1024 * 256));
                assert_eq!(node.links().count(), 0);
            }
        }

        // TODO: check content
        // TODO: add nested directory

        Ok(())
    }

    fn test_chunk_stream(num_chunks: usize) -> impl Stream<Item = std::io::Result<BytesMut>> {
        futures::stream::iter((0..num_chunks).map(|n| Ok(BytesMut::from(&n.to_be_bytes()[..]))))
    }

    #[tokio::test]
    async fn test_chunk_size_overflow() -> Result<()> {
        let chunker = Chunker::FixedSize { chunk_size: 1200 };
        let mut builder = DirectoryBuilder::new();
        builder.chunker = chunker;

        // add one file
        let nodes = Box::pin(test_chunk_stream(1));
        let file = File {
            name: "foo.bar".into(),
            nodes,
            tree_builder: TreeBuilder::balanced_tree(),
        };
        builder.add_file(file);
        assert_eq!(DirectoryType::Basic, builder.typ);

        // add second file. this should cause us to try and convert
        // the directory to a hamt
        let nodes = Box::pin(test_chunk_stream(1));
        let file = File {
            name: "foo.bar".into(),
            nodes,
            tree_builder: TreeBuilder::balanced_tree(),
        };
        builder.add_file(file);
        assert_eq!(DirectoryType::Hamt, builder.typ);

        if (builder.build().await).is_ok() {
            panic!("expected builder to error when attempting to build a hamt directory")
        }

        Ok(())
    }
}
