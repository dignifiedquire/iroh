//! Traits for async local IO, and implementations for local files, http resources and memory.
//! This crate provides two core traits: [AsyncSliceReader] and [AsyncSliceWriter].
//!
//! Memory implementations for Bytes and BytesMut are provided by default, file and http are
//! behind features.
//!
//! Futures for the two core traits are not required to be [Send](std::marker::Send).
//!
//! This allows implementing these traits for types that are not thread safe, such as many
//! embedded databases.
//!
//! It also allows for omitting thread synchronization primitives. E.g. you could use an
//! `Rc<RefCell<BytesMut>>` instead of a much more costly `Arc<Mutex<BytesMut>>`.
//!
//! The downside is that you have to use a local executor, e.g.
//! [LocalPoolHandle](https://docs.rs/tokio-util/latest/tokio_util/task/struct.LocalPoolHandle.html),
//! if you want to do async IO in a background task.
//!
//! A good blog post providing a rationale for these decisions is
//! [Local Async Executors and Why They Should be the Default](https://maciej.codes/2022-06-09-local-async.html).
//!
//! All futures returned by this trait must be polled to completion, otherwise
//! all subsequent calls will produce an io error.
//!
//! So it is fine to e.g. limit a call to read_at with a timeout, but once a future
//! is dropped without being polled to completion, the reader is not useable anymore.
//!
//! All methods, even those that do not modify the underlying resource, take a mutable
//! reference to self. This is to enforce linear access to the underlying resource.
//!
//! In general it is assumed that readers are cheap, so in case of an error you can
//! always get a new reader. Also, if you need concurrent access to the same resource,
//! create multiple readers.
use bytes::Bytes;
use futures::Future;
use pin_project::pin_project;
use std::{io, task};

/// A trait to abstract async reading from different resource.
///
/// This trait does not require the notion of a current position, but instead
/// requires explicitly passing the offset to read_at. In addition to the ability
/// to read at an arbitrary offset, it also provides the ability to get the
/// length of the resource.
///
/// This is similar to the io interface of sqlite.
/// See xRead, xFileSize in <https://www.sqlite.org/c3ref/io_methods.html>
#[allow(clippy::len_without_is_empty)]
pub trait AsyncSliceReader {
    /// The future returned by read_at
    type ReadAtFuture<'a>: Future<Output = io::Result<Bytes>> + 'a
    where
        Self: 'a;
    /// Read the entire buffer at the given position.
    ///
    /// Will return at most `len` bytes, but may return fewer if the resource is smaller.
    /// If the range is completely covered by the resource, will return exactly `len` bytes.
    /// If the range is not covered at all by the resource, will return an empty buffer.
    /// It will never return an io error independent of the range as long as the underlying
    /// resource is valid.
    #[must_use = "io futures must be polled to completion"]
    fn read_at(&mut self, offset: u64, len: usize) -> Self::ReadAtFuture<'_>;

    /// The future returned by len
    type LenFuture<'a>: Future<Output = io::Result<u64>> + 'a
    where
        Self: 'a;
    /// Get the length of the resource
    #[must_use = "io futures must be polled to completion"]
    fn len(&mut self) -> Self::LenFuture<'_>;
}

/// A trait to abstract async writing to different resources.
///
/// This trait does not require the notion of a current position, but instead
/// requires explicitly passing the offset to write_at and write_array_at.
/// In addition to the ability to write at an arbitrary offset, it also provides
/// the ability to set the length of the resource.
///
/// This is similar to the io interface of sqlite.
/// See xWrite in <https://www.sqlite.org/c3ref/io_methods.html>
pub trait AsyncSliceWriter: Sized {
    /// The future returned by write_at
    type WriteAtFuture<'a>: Future<Output = io::Result<()>> + 'a
    where
        Self: 'a;
    /// Write the entire Bytes at the given position.
    ///
    /// When writing with the end of the range after the end of the blob, the blob will be extended.
    /// When writing with the start of the range after the end of the blob, the gap will be filled with zeros.
    #[must_use = "io futures must be polled to completion"]
    fn write_at(&mut self, offset: u64, data: Bytes) -> Self::WriteAtFuture<'_>;

    /// The future returned by write_slice_at
    type WriteArrayAtFuture<'a>: Future<Output = io::Result<()>> + 'a
    where
        Self: 'a;
    /// Write the entire fixed size array at the given position.
    /// This is useful to write a small number of bytes without allocating a [bytes::Bytes].
    ///
    /// Except for taking a fixed size array instead of a [bytes::Bytes], this is equivalent to [AsyncSliceWriter::write_at].
    #[must_use = "io futures must be polled to completion"]
    fn write_array_at<const N: usize>(
        &mut self,
        offset: u64,
        data: [u8; N],
    ) -> Self::WriteArrayAtFuture<'_>;

    /// The future returned by set_len
    type SetLenFuture<'a>: Future<Output = io::Result<()>> + 'a
    where
        Self: 'a;
    /// Set the length of the underlying storage.
    #[must_use = "io futures must be polled to completion"]
    fn set_len(&mut self, len: u64) -> Self::SetLenFuture<'_>;

    /// The future returned by sync
    type SyncFuture<'a>: Future<Output = io::Result<()>> + 'a
    where
        Self: 'a;
    /// Sync any buffers to the underlying storage.
    #[must_use = "io futures must be polled to completion"]
    fn sync(&mut self) -> Self::SyncFuture<'_>;
}

#[cfg(any(feature = "tokio-io", feature = "http"))]
/// A macro to create a newtype wrapper for a future, to hide the underlying type.
macro_rules! newtype_future {
    ($(#[$outer:meta])* $name:ident, $inner:ty, $output:ty) => {
        #[repr(transparent)]
        #[pin_project::pin_project]
        #[must_use]
        $(#[$outer])*
        pub struct $name<'a>(#[pin] $inner);

        impl<'a> futures::Future for $name<'a> {
            type Output = $output;
            fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
                self.project().0.poll(cx)
            }
        }
    };
}

#[cfg(feature = "tokio-io")]
mod tokio_io;
#[cfg(feature = "tokio-io")]
pub use tokio_io::*;

#[cfg(feature = "http")]
mod http;
#[cfg(feature = "http")]
pub use http::*;

/// implementations for [AsyncSliceReader] and [AsyncSliceWriter] for [bytes::Bytes] and [bytes::BytesMut]
mod mem;

/// Either type to be used with [AsyncSliceReader] and [AsyncSliceWriter].
///
/// This also implements [futures::Future]
#[derive(Debug)]
#[pin_project(project = EitherProj)]
#[must_use]
pub enum Either<L, R> {
    Left(#[pin] L),
    Right(#[pin] R),
}

impl<L, R> AsyncSliceReader for Either<L, R>
where
    L: AsyncSliceReader + 'static,
    R: AsyncSliceReader + 'static,
{
    type ReadAtFuture<'a> = Either<L::ReadAtFuture<'a>, R::ReadAtFuture<'a>>;
    fn read_at(&mut self, offset: u64, len: usize) -> Self::ReadAtFuture<'_> {
        match self {
            Either::Left(l) => Either::Left(l.read_at(offset, len)),
            Either::Right(r) => Either::Right(r.read_at(offset, len)),
        }
    }

    type LenFuture<'a> = Either<L::LenFuture<'a>, R::LenFuture<'a>>;
    fn len(&mut self) -> Self::LenFuture<'_> {
        match self {
            Either::Left(l) => Either::Left(l.len()),
            Either::Right(r) => Either::Right(r.len()),
        }
    }
}

impl<L, R> AsyncSliceWriter for Either<L, R>
where
    L: AsyncSliceWriter + 'static,
    R: AsyncSliceWriter + 'static,
{
    type WriteAtFuture<'a> = Either<L::WriteAtFuture<'a>, R::WriteAtFuture<'a>>;
    fn write_at(&mut self, offset: u64, data: Bytes) -> Self::WriteAtFuture<'_> {
        match self {
            Either::Left(l) => Either::Left(l.write_at(offset, data)),
            Either::Right(r) => Either::Right(r.write_at(offset, data)),
        }
    }

    type WriteArrayAtFuture<'a> = Either<L::WriteArrayAtFuture<'a>, R::WriteArrayAtFuture<'a>>;
    fn write_array_at<const N: usize>(
        &mut self,
        offset: u64,
        data: [u8; N],
    ) -> Self::WriteArrayAtFuture<'_> {
        match self {
            Either::Left(l) => Either::Left(l.write_array_at(offset, data)),
            Either::Right(r) => Either::Right(r.write_array_at(offset, data)),
        }
    }

    type SyncFuture<'a> = Either<L::SyncFuture<'a>, R::SyncFuture<'a>>;
    fn sync(&mut self) -> Self::SyncFuture<'_> {
        match self {
            Either::Left(l) => Either::Left(l.sync()),
            Either::Right(r) => Either::Right(r.sync()),
        }
    }

    type SetLenFuture<'a> = Either<L::SetLenFuture<'a>, R::SetLenFuture<'a>>;
    fn set_len(&mut self, len: u64) -> Self::SetLenFuture<'_> {
        match self {
            Either::Left(l) => Either::Left(l.set_len(len)),
            Either::Right(r) => Either::Right(r.set_len(len)),
        }
    }
}

impl<T, R: Future<Output = T>, L: Future<Output = T>> Future for Either<L, R> {
    type Output = T;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut task::Context<'_>,
    ) -> task::Poll<Self::Output> {
        match self.project() {
            EitherProj::Left(l) => l.poll(cx),
            EitherProj::Right(r) => r.poll(cx),
        }
    }
}

#[cfg(test)]
mod tests {

    use crate::mem::limited_range;

    use super::*;
    use bytes::BytesMut;
    use proptest::prelude::*;
    use std::io;

    #[cfg(feature = "tokio-io")]
    use std::io::Write;

    /// mutable style read smoke test, expects a resource containing 0..100u8
    async fn read_mut_smoke(mut file: impl AsyncSliceReader) -> io::Result<()> {
        let expected = (0..100u8).collect::<Vec<_>>();

        // read the whole file
        let res = file.read_at(0, usize::MAX).await?;
        assert_eq!(res, expected);

        let res = file.len().await?;
        assert_eq!(res, 100);

        // read 3 bytes at offset 0
        let res = file.read_at(0, 3).await?;
        assert_eq!(res, vec![0, 1, 2]);

        // read 10 bytes at offset 95 (near the end of the file)
        let res = file.read_at(95, 10).await?;
        assert_eq!(res, vec![95, 96, 97, 98, 99]);

        // read 10 bytes at offset 110 (after the end of the file)
        let res = file.read_at(110, 10).await?;
        assert_eq!(res, vec![]);

        Ok(())
    }

    /// mutable style write smoke test, expects an empty resource
    async fn write_mut_smoke<F: AsyncSliceWriter, C: Fn(&F) -> Vec<u8>>(
        mut file: F,
        contents: C,
    ) -> io::Result<()> {
        // write 3 bytes at offset 0
        file.write_at(0, vec![0, 1, 2].into()).await?;
        assert_eq!(contents(&file), &[0, 1, 2]);

        // write 3 bytes at offset 5
        file.write_at(5, vec![0, 1, 2].into()).await?;
        assert_eq!(contents(&file), &[0, 1, 2, 0, 0, 0, 1, 2]);

        // write a u16 at offset 8
        file.write_array_at(8, 1u16.to_le_bytes()).await?;
        assert_eq!(contents(&file), &[0, 1, 2, 0, 0, 0, 1, 2, 1, 0]);

        // truncate to 0
        file.set_len(0).await?;
        assert_eq!(contents(&file).len(), 0);

        Ok(())
    }

    #[cfg(feature = "tokio-io")]
    /// Tests the various ways to read from a std::fs::File
    #[tokio::test]
    async fn file_reading_smoke() -> io::Result<()> {
        // create a file with 100 bytes
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&(0..100u8).collect::<Vec<_>>()).unwrap();
        read_mut_smoke(FileAdapter::from_std(file)).await?;
        Ok(())
    }

    /// Tests the various ways to read from a bytes::Bytes
    #[tokio::test]
    async fn bytes_reading_smoke() -> io::Result<()> {
        let bytes: Bytes = (0..100u8).collect::<Vec<_>>().into();
        read_mut_smoke(bytes).await?;

        Ok(())
    }

    /// Tests the various ways to read from a bytes::BytesMut
    #[tokio::test]
    async fn bytes_mut_reading_smoke() -> io::Result<()> {
        let mut bytes: BytesMut = BytesMut::new();
        bytes.extend(0..100u8);

        read_mut_smoke(bytes).await?;

        Ok(())
    }

    fn bytes_mut_contents(bytes: &BytesMut) -> Vec<u8> {
        bytes.to_vec()
    }

    #[cfg(feature = "tokio-io")]
    #[tokio::test]
    async fn async_slice_writer_smoke() -> io::Result<()> {
        let file = tempfile::tempfile().unwrap();
        write_mut_smoke(FileAdapter::from_std(file), |x| x.read_contents()).await?;

        Ok(())
    }

    #[tokio::test]
    async fn bytes_mut_writing_smoke() -> io::Result<()> {
        let bytes: BytesMut = BytesMut::new();

        write_mut_smoke(bytes, |x| x.as_ref().to_vec()).await?;

        Ok(())
    }

    fn random_slice(offset: u64, size: usize) -> impl Strategy<Value = (u64, Vec<u8>)> {
        (0..offset, 0..size).prop_map(|(offset, size)| {
            let data = (0..size).map(|x| x as u8).collect::<Vec<_>>();
            (offset, data)
        })
    }

    fn random_write_op(offset: u64, size: usize) -> impl Strategy<Value = WriteOp> {
        prop_oneof![
            20 => random_slice(offset, size).prop_map(|(offset, data)| WriteOp::Write(offset, data)),
            1 => (0..(offset + size as u64)).prop_map(WriteOp::SetLen),
            1 => Just(WriteOp::Sync),
        ]
    }

    fn random_write_ops(offset: u64, size: usize, n: usize) -> impl Strategy<Value = Vec<WriteOp>> {
        prop::collection::vec(random_write_op(offset, size), n)
    }

    fn random_read_ops(offset: u64, size: usize, n: usize) -> impl Strategy<Value = Vec<ReadOp>> {
        prop::collection::vec(random_read_op(offset, size), n)
    }

    fn sequential_offset(mag: usize) -> impl Strategy<Value = isize> {
        prop_oneof![
            20 => Just(0),
            1 => (0..mag).prop_map(|x| x as isize),
            1 => (0..mag).prop_map(|x| -(x as isize)),
        ]
    }

    fn random_read_op(offset: u64, size: usize) -> impl Strategy<Value = ReadOp> {
        prop_oneof![
            20 => (0..offset, 0..size).prop_map(|(offset, len)| ReadOp::ReadAt(offset, len)),
            1 => (sequential_offset(1024), 0..size).prop_map(|(offset, len)| ReadOp::ReadSequential(offset, len)),
            1 => Just(ReadOp::Len),
        ]
    }

    #[derive(Debug, Clone)]
    enum ReadOp {
        // read the data at the given offset
        ReadAt(u64, usize),
        // read the data relative to the previous position
        ReadSequential(isize, usize),
        // ask for the length of the file
        Len,
    }

    #[derive(Debug, Clone)]
    enum WriteOp {
        // write the data at the given offset
        Write(u64, Vec<u8>),
        // set the length of the file
        SetLen(u64),
        // sync the file
        Sync,
    }

    /// reference implementation for a vector of bytes
    fn apply_op(file: &mut Vec<u8>, op: &WriteOp) {
        match op {
            WriteOp::Write(offset, data) => {
                // an empty write is a no-op and does not resize the file
                if data.is_empty() {
                    return;
                }
                let end = offset.saturating_add(data.len() as u64);
                let start = usize::try_from(*offset).unwrap();
                let end = usize::try_from(end).unwrap();
                if end > file.len() {
                    file.resize(end, 0);
                }
                file[start..end].copy_from_slice(data);
            }
            WriteOp::SetLen(offset) => {
                let offset = usize::try_from(*offset).unwrap_or(usize::MAX);
                file.resize(offset, 0);
            }
            WriteOp::Sync => {}
        }
    }

    fn async_test<F: Future>(f: F) -> F::Output {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(f)
    }

    async fn write_op_test<W: AsyncSliceWriter, C: Fn(&W) -> Vec<u8>>(
        ops: Vec<WriteOp>,
        mut bytes: W,
        content: C,
    ) -> io::Result<()> {
        let mut reference = Vec::new();
        for op in ops {
            apply_op(&mut reference, &op);
            match op {
                WriteOp::Write(offset, data) => {
                    AsyncSliceWriter::write_at(&mut bytes, offset, data.into()).await?;
                }
                WriteOp::SetLen(offset) => {
                    AsyncSliceWriter::set_len(&mut bytes, offset).await?;
                }
                WriteOp::Sync => {
                    AsyncSliceWriter::sync(&mut bytes).await?;
                }
            }
            assert_eq!(content(&bytes), reference.as_slice());
        }
        io::Result::Ok(())
    }

    async fn read_op_test<R: AsyncSliceReader>(
        ops: Vec<ReadOp>,
        mut file: R,
        actual: &[u8],
    ) -> io::Result<()> {
        let mut current = 0u64;
        for op in ops {
            match op {
                ReadOp::ReadAt(offset, len) => {
                    let data = AsyncSliceReader::read_at(&mut file, offset, len).await?;
                    current = offset.checked_add(len as u64).unwrap();
                    assert_eq!(&data, &actual[limited_range(offset, len, actual.len())]);
                }
                ReadOp::ReadSequential(offset, len) => {
                    let offset = if offset >= 0 {
                        current.saturating_add(offset as u64)
                    } else {
                        current.saturating_sub((-offset) as u64)
                    };
                    let data = AsyncSliceReader::read_at(&mut file, offset, len).await?;
                    current = offset.checked_add(len as u64).unwrap();
                    assert_eq!(&data, &actual[limited_range(offset, len, actual.len())]);
                }
                ReadOp::Len => {
                    let len = AsyncSliceReader::len(&mut file).await?;
                    assert_eq!(len, actual.len() as u64);
                }
            }
        }
        io::Result::Ok(())
    }

    // #[cfg(feature = "http")]
    // #[tokio::test]
    // async fn test_http_range() -> io::Result<()> {
    //     let url = reqwest::Url::parse("https://ipfs.io/ipfs/bafybeiaj2dgwpi6bsisyf4wq7yvj4lqvpbmlmztm35hqyqqjihybnden24/image").unwrap();
    //     let mut resource = HttpAdapter::new(url).await?;
    //     let buf = resource.read_at(0, 100).await?;
    //     println!("buf: {:?}", buf);
    //     let buf = resource.read_at(1000000, 100).await?;
    //     println!("buf: {:?}", buf);
    //     Ok(())
    // }

    #[cfg(feature = "http")]
    #[tokio::test]
    async fn http_smoke() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.txt");
        let mut file = std::fs::File::create(&path).unwrap();
        file.write_all(b"hello world").unwrap();
        let filter = warp::fs::file(path);
        let server = warp::serve(filter);
        let (addr, server) = server.bind_ephemeral(([127, 0, 0, 1], 0));
        let url = format!("http://{}", addr);
        println!("serving from {}", url);
        let url = reqwest::Url::parse(&url).unwrap();
        let server = tokio::spawn(server);
        let mut reader = HttpAdapter::new(url).await.unwrap();
        let len = reader.len().await.unwrap();
        assert_eq!(len, 11);
        println!("len: {:?}", reader);
        let part = reader.read_at(0, 11).await.unwrap();
        assert_eq!(part.as_ref(), b"hello world");
        let part = reader.read_at(6, 5).await.unwrap();
        assert_eq!(part.as_ref(), b"world");
        let part = reader.read_at(6, 10).await.unwrap();
        assert_eq!(part.as_ref(), b"world");
        let part = reader.read_at(100, 10).await.unwrap();
        assert_eq!(part.as_ref(), b"");
        server.abort();
    }

    proptest! {

        #[test]
        fn bytes_write(ops in random_write_ops(1024, 1024, 10)) {
            async_test(write_op_test(ops, BytesMut::new(), bytes_mut_contents)).unwrap();
        }

        #[cfg(feature = "tokio-io")]
        #[test]
        fn file_write(ops in random_write_ops(1024, 1024, 10)) {
            let file = tempfile::tempfile().unwrap();
            async_test(write_op_test(ops, FileAdapter::from_std(file), |x| x.read_contents())).unwrap();
        }

        #[test]
        fn bytes_read(data in proptest::collection::vec(any::<u8>(), 0..1024), ops in random_read_ops(1024, 1024, 2)) {
            async_test(read_op_test(ops, Bytes::from(data.clone()), &data)).unwrap();
        }

        #[cfg(feature = "tokio-io")]
        #[test]
        fn file_read(data in proptest::collection::vec(any::<u8>(), 0..1024), ops in random_read_ops(1024, 1024, 2)) {
            let mut file = tempfile::tempfile().unwrap();
            file.write_all(&data).unwrap();
            async_test(read_op_test(ops, FileAdapter::from_std(file), &data)).unwrap();
        }

        #[cfg(feature = "http")]
        #[test]
        fn http_read(data in proptest::collection::vec(any::<u8>(), 0..10), ops in random_read_ops(10, 10, 2)) {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("test.txt");
            let mut file = std::fs::File::create(&path).unwrap();
            file.write_all(&data).unwrap();
            let filter = warp::fs::file(path);
            async_test(async move {
                // create a test server. this has to happen in a tokio runtime
                let server = warp::serve(filter);
                // bind to a random port
                let (addr, server) = server.bind_ephemeral(([127, 0, 0, 1], 0));
                // spawn the server in a background task
                let server = tokio::spawn(server);
                // create a resource from the server
                let url = reqwest::Url::parse(&format!("http://{}", addr)).unwrap();
                let file = HttpAdapter::new(url).await.unwrap();
                // run the test
                read_op_test(ops, file, &data).await.unwrap();
                // stop the server
                server.abort();
            });
        }
    }
}
