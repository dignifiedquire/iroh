use std::{
    fs::OpenOptions,
    io::{self, Write},
    path::Path,
    time::{Duration, Instant},
};

/// A reader that calls a callback with the number of bytes read after each read.
pub(crate) struct ProgressReader<R, F: Fn(u64) -> io::Result<()>> {
    inner: R,
    offset: u64,
    cb: F,
}

impl<R: io::Read, F: Fn(u64) -> io::Result<()>> ProgressReader<R, F> {
    pub fn new(inner: R, cb: F) -> Self {
        Self {
            inner,
            offset: 0,
            cb,
        }
    }
}

impl<R: io::Read, F: Fn(u64) -> io::Result<()>> io::Read for ProgressReader<R, F> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.offset += read as u64;
        (self.cb)(self.offset)?;
        Ok(read)
    }
}

/// overwrite a file with the given data.
///
/// This is almost like `std::fs::write`, but it does not truncate the file.
///
/// So if you overwrite a file with less data than it had before, the file will
/// still have the same size as before.
///
/// Also, if you overwrite a file with the same data as it had before, the
/// file will be unchanged even if the overwrite operation is interrupted.
pub fn overwrite_and_sync(path: &Path, data: &[u8]) -> io::Result<std::fs::File> {
    tracing::trace!(
        "overwriting file {} with {} bytes",
        path.display(),
        data.len()
    );
    // std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    // tracing::error!("{}", path.parent().unwrap().display());
    // tracing::error!("{}", path.parent().unwrap().metadata().unwrap().is_dir());
    let mut file = OpenOptions::new().write(true).create(true).open(path)?;
    file.write_all(data)?;
    // todo: figure out if it is safe to not sync here
    file.sync_all()?;
    Ok(file)
}

/// Read a file into memory and then delete it.
pub fn read_and_remove(path: &Path) -> io::Result<Vec<u8>> {
    let data = std::fs::read(path)?;
    // todo: should we fail here or just log a warning?
    // remove could fail e.g. on windows if the file is still open
    std::fs::remove_file(path)?;
    Ok(data)
}

/// A wrapper for a flume receiver that allows peeking at the next message.
#[derive(Debug)]
pub(super) struct PeekableFlumeReceiver<T> {
    msg: Option<T>,
    recv: flume::Receiver<T>,
}

#[allow(dead_code)]
impl<T> PeekableFlumeReceiver<T> {
    pub fn new(recv: flume::Receiver<T>) -> Self {
        Self { msg: None, recv }
    }

    /// Peek at the next message.
    ///
    /// Will block if there are no messages.
    /// Returns None only if there are no more messages (sender is dropped).
    pub fn peek(&mut self) -> Option<&T> {
        if self.msg.is_none() {
            self.msg = self.recv.recv().ok();
        }
        self.msg.as_ref()
    }

    /// Receive the next message.
    ///
    /// Will block if there are no messages.
    /// Returns None only if there are no more messages (sender is dropped).
    pub fn recv(&mut self) -> Option<T> {
        if let Some(msg) = self.msg.take() {
            return Some(msg);
        }
        self.recv.recv().ok()
    }

    /// Try to peek at the next message.
    ///
    /// Will not block.
    /// Returns None if reading would block, or if there are no more messages (sender is dropped).
    pub fn try_peek(&mut self) -> Option<&T> {
        if self.msg.is_none() {
            self.msg = self.recv.try_recv().ok();
        }
        self.msg.as_ref()
    }

    /// Try to receive the next message.
    ///
    /// Will not block.
    /// Returns None if reading would block, or if there are no more messages (sender is dropped).
    pub fn try_recv(&mut self) -> Option<T> {
        if let Some(msg) = self.msg.take() {
            return Some(msg);
        }
        self.recv.try_recv().ok()
    }

    pub fn recv_timeout(&mut self, timeout: std::time::Duration) -> Option<T> {
        if let Some(msg) = self.msg.take() {
            return Some(msg);
        }
        self.recv.recv_timeout(timeout).ok()
    }

    /// Create an iterator that pulls messages from the receiver for at most
    /// `count` messages or `max_duration` time.
    pub fn batch_iter(&mut self, count: usize, max_duration: Duration) -> BatchIter<T> {
        BatchIter::new(self, count, max_duration)
    }

    /// Push back a message. This will only work if there is room for it.
    /// Otherwise, it will fail and return the message.
    pub fn push_back(&mut self, msg: T) -> std::result::Result<(), T> {
        if self.msg.is_none() {
            self.msg = Some(msg);
            Ok(())
        } else {
            Err(msg)
        }
    }
}

pub(super) struct BatchIter<'a, T> {
    recv: &'a mut PeekableFlumeReceiver<T>,
    start: Instant,
    remaining: usize,
    max_duration: Duration,
}

impl<'a, T> BatchIter<'a, T> {
    fn new(recv: &'a mut PeekableFlumeReceiver<T>, count: usize, max_duration: Duration) -> Self {
        Self {
            recv,
            start: Instant::now(),
            remaining: count,
            max_duration,
        }
    }
}

impl<'a, T> Iterator for BatchIter<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let elapsed = self.start.elapsed();
        if elapsed >= self.max_duration {
            return None;
        }
        let remaining_time = self.max_duration - elapsed;
        self.remaining -= 1;
        self.recv.recv_timeout(remaining_time)
    }
}
