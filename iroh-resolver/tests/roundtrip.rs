use anyhow::Result;
use async_recursion::async_recursion;
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use iroh_metrics::resolver::OutMetrics;
use iroh_unixfs::{
    balanced_tree::DEFAULT_DEGREE,
    builder::{Directory, DirectoryBuilder, FileBuilder, SymlinkBuilder},
    chunker::DEFAULT_CHUNKS_SIZE,
    content_loader::ContentLoader,
    ResponseClip,
};
use proptest::prelude::*;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::collections::BTreeMap;
use tokio::io::AsyncReadExt;

use iroh_resolver::resolver::{read_to_vec, stream_to_resolver, Out, Resolver};

#[derive(Debug, Clone, PartialEq, Eq)]
enum TestDirEntry {
    File(Bytes),
    Directory(TestDir),
}
type TestDir = BTreeMap<String, TestDirEntry>;

/// builds an unixfs directory out of a TestDir
#[async_recursion(?Send)]
async fn build_directory(name: &str, dir: &TestDir) -> Result<Directory> {
    let mut builder = DirectoryBuilder::new();
    builder.name(name);
    for (name, entry) in dir {
        match entry {
            TestDirEntry::File(content) => {
                let file = FileBuilder::new()
                    .name(name)
                    .content_bytes(content.to_vec())
                    .build()
                    .await?;
                builder.add_file(file);
            }
            TestDirEntry::Directory(dir) => {
                let dir = build_directory(name, dir).await?;
                builder.add_dir(dir)?;
            }
        }
    }
    builder.build()
}

/// builds a TestDir out of a stream of blocks and a resolver
async fn build_testdir(
    stream: impl Stream<Item = Result<(iroh_resolver::resolver::Path, Out)>>,
    resolver: Resolver<impl ContentLoader + Unpin>,
) -> Result<TestDir> {
    tokio::pin!(stream);

    /// recursively create directories for a path
    fn mkdir(dir: &mut TestDir, path: &[String]) -> Result<()> {
        if let Some((first, rest)) = path.split_first() {
            if let TestDirEntry::Directory(child) = dir
                .entry(first.clone())
                .or_insert_with(|| TestDirEntry::Directory(Default::default()))
            {
                mkdir(child, rest)?;
            } else {
                anyhow::bail!("not a directory");
            }
        }
        Ok(())
    }

    /// create a file in a directory hierarchy
    fn mkfile(dir: &mut TestDir, path: &[String], data: Bytes) -> Result<()> {
        if let Some((first, rest)) = path.split_first() {
            if rest.is_empty() {
                dir.insert(first.clone(), TestDirEntry::File(data));
            } else if let TestDirEntry::Directory(child) = dir
                .entry(first.clone())
                .or_insert_with(|| TestDirEntry::Directory(Default::default()))
            {
                mkfile(child, rest, data)?;
            } else {
                anyhow::bail!("not a directory");
            }
        }
        Ok(())
    }

    let reference = stream
        .try_fold(TestDir::default(), move |mut agg, (path, item)| {
            let resolver = resolver.clone();
            async move {
                if item.is_dir() {
                    mkdir(&mut agg, path.tail())?;
                } else {
                    let reader = item.pretty(
                        resolver.clone(),
                        OutMetrics::default(),
                        ResponseClip::NoClip,
                    )?;
                    let data = read_to_vec(reader).await?;
                    mkfile(&mut agg, path.tail(), data.into())?;
                }
                Ok(agg)
            }
        })
        .await?;
    Ok(reference)
}

/// a roundtrip test that converts a dir to an unixfs DAG and back
async fn dir_roundtrip_test(dir: TestDir) -> Result<bool> {
    let directory = build_directory("", &dir).await?;
    let stream = directory.encode();
    let (root, resolver) = stream_to_resolver(stream).await?;
    let stream =
        resolver.resolve_recursive_with_paths(iroh_resolver::resolver::Path::from_cid(root));
    let reference = build_testdir(stream, resolver).await?;
    Ok(dir == reference)
}

/// sync version of dir_roundtrip_test for use in proptest
fn dir_roundtrip_test_sync(dir: TestDir) -> bool {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(dir_roundtrip_test(dir))
        .unwrap()
}

/// a roundtrip test that converts a file to an unixfs DAG and back
async fn file_roundtrip_test(data: Bytes, chunk_size: usize, degree: usize) -> Result<bool> {
    let file = FileBuilder::new()
        .name("file.bin")
        .fixed_chunker(chunk_size)
        .degree(degree)
        .content_bytes(data.clone())
        .build()
        .await?;
    let stream = file.encode().await?;
    let (root, resolver) = stream_to_resolver(stream).await?;
    let out = resolver
        .resolve(iroh_resolver::resolver::Path::from_cid(root))
        .await?;
    let t = read_to_vec(out.pretty(resolver, OutMetrics::default(), ResponseClip::NoClip)?).await?;
    println!("{}", data.len());
    Ok(t == data)
}

async fn large_dir_roundtrip(n: usize) -> Result<()> {
    let mut builder = DirectoryBuilder::new();
    for i in 0..n {
        let file = FileBuilder::new()
            .name(format!("file_{}", i))
            .content_bytes(Bytes::from(""))
            .build()
            .await?;
        builder.add_file(file);
    }
    let dir = builder.build()?;
    let blocks = dir.encode();
    tokio::pin!(blocks);
    let (root, resolver) = stream_to_resolver(blocks).await?;
    let stream =
        resolver.resolve_recursive_with_paths(iroh_resolver::resolver::Path::from_cid(root));
    tokio::pin!(stream);
    let mut i = 0;
    while let Some(item) = stream.next().await {
        if let Ok((path, out)) = item {
            if !out.is_dir() {
                assert_eq!(path.tail(), &[format!("file_{}", i)]);
                i += 1;
            }
        }
    }
    assert_eq!(i, n);
    Ok(())
}

/// a roundtrip test that converts a symlink to a unixfs DAG and back
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_large_dir_roundtrip() {
    large_dir_roundtrip(2048).await.unwrap();
    // uncomment once we got hamt support
    // large_dir_roundtrip(6001).await.unwrap();
}

/// a roundtrip test that converts a symlink to a unixfs DAG and back
#[tokio::test]
async fn symlink_roundtrip_test() -> Result<()> {
    let mut builder = SymlinkBuilder::new("foo");
    let target = "../../bar.txt";
    builder.target(target);
    let sym = builder.build().await?;
    let block = sym.encode()?;
    let stream = async_stream::try_stream! {
        yield block;
    };
    let (root, resolver) = stream_to_resolver(stream).await?;
    let out = resolver
        .resolve(iroh_resolver::resolver::Path::from_cid(root))
        .await?;
    let mut reader = out.pretty(resolver, OutMetrics::default(), ResponseClip::NoClip)?;
    let mut t = String::new();
    reader.read_to_string(&mut t).await?;
    println!("{}", t);
    assert_eq!(target, t);
    Ok(())
}

/// sync version of file_roundtrip_test for use in proptest
fn file_roundtrip_test_sync(data: Bytes, chunk_size: usize, degree: usize) -> bool {
    let f = file_roundtrip_test(data, chunk_size, degree);
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(f)
        .unwrap()
}

fn arb_test_dir() -> impl Strategy<Value = TestDir> {
    // create an arbitrary nested directory structure
    fn arb_dir_entry() -> impl Strategy<Value = TestDirEntry> {
        let leaf = any::<Vec<u8>>().prop_map(|x| TestDirEntry::File(Bytes::from(x)));
        leaf.prop_recursive(3, 64, 10, |inner| {
            prop::collection::btree_map(".*", inner, 0..10).prop_map(TestDirEntry::Directory)
        })
    }
    prop::collection::btree_map(".*", arb_dir_entry(), 0..10)
}

fn arb_degree() -> impl Strategy<Value = usize> {
    // use either the smallest possible degree for complex tree structures, or the default value for realism
    prop_oneof![Just(2), Just(DEFAULT_DEGREE)]
}

fn arb_chunk_size() -> impl Strategy<Value = usize> {
    // use either the smallest possible chunk size for complex tree structures, or the default value for realism
    prop_oneof![Just(1), Just(DEFAULT_CHUNKS_SIZE)]
}

proptest! {
    #[test]
    fn test_file_roundtrip(data in proptest::collection::vec(any::<u8>(), 0usize..1024), chunk_size in arb_chunk_size(), degree in arb_degree()) {
        assert!(file_roundtrip_test_sync(data.into(), chunk_size, degree));
    }

    #[test]
    fn test_dir_roundtrip(data in arb_test_dir()) {
        assert!(dir_roundtrip_test_sync(data));
    }
}

#[tokio::test]
async fn test_builder_roundtrip_complex_tree_1() -> Result<()> {
    // fill with random data so we get distinct cids for all blocks
    let mut rng = ChaCha8Rng::from_seed([0; 32]);
    let mut data = vec![0u8; 1024 * 128];
    rng.fill(data.as_mut_slice());
    assert!(file_roundtrip_test(data.into(), 1024, 4).await?);
    Ok(())
}

#[tokio::test]
async fn test_builder_roundtrip_128m() -> Result<()> {
    // fill with random data so we get distinct cids for all blocks
    let mut rng = ChaCha8Rng::from_seed([0; 32]);
    let mut data = vec![0u8; 128 * 1024 * 1024];
    rng.fill(data.as_mut_slice());
    assert!(file_roundtrip_test(data.into(), DEFAULT_CHUNKS_SIZE, DEFAULT_DEGREE).await?);
    Ok(())
}
