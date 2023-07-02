use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use futures::FutureExt;

/// A join handle that owns the task it is running, and aborts it when dropped.
#[derive(Debug, derive_more::Deref)]
pub(crate) struct AbortingJoinHandle<T>(tokio::task::JoinHandle<T>);

impl<T> From<tokio::task::JoinHandle<T>> for AbortingJoinHandle<T> {
    fn from(handle: tokio::task::JoinHandle<T>) -> Self {
        Self(handle)
    }
}

impl<T> Future for AbortingJoinHandle<T> {
    type Output = std::result::Result<T, tokio::task::JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(cx)
    }
}

impl<T> Drop for AbortingJoinHandle<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Holds a handle to a task and aborts it on drop.
///
/// See [`tokio::task::AbortHandle`].
#[derive(derive_more::Debug)]
pub struct CancelOnDrop {
    task_name: &'static str,
    #[debug(skip)]
    handle: tokio::task::AbortHandle,
}

impl CancelOnDrop {
    pub fn new(task_name: &'static str, handle: tokio::task::AbortHandle) -> Self {
        CancelOnDrop { task_name, handle }
    }
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.handle.abort();
        tracing::debug!("{} completed", self.task_name);
    }
}

/// Resolves to pending if the inner is `None`.
#[derive(Debug)]
pub struct MaybeFuture<T> {
    pub inner: Option<T>,
}

// NOTE: explicit implementation to bypass derive's unnecessary bounds
impl<T> Default for MaybeFuture<T> {
    fn default() -> Self {
        MaybeFuture { inner: None }
    }
}

impl<T: Future + Unpin> Future for MaybeFuture<T> {
    type Output = T::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.inner {
            Some(ref mut t) => Pin::new(t).poll(cx),
            None => Poll::Pending,
        }
    }
}
