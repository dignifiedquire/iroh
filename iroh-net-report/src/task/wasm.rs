use std::{
    cell::RefCell,
    fmt::Debug,
    future::{Future, IntoFuture},
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

use futures_lite::stream::StreamExt;
use wasm_bindgen::prelude::wasm_bindgen;

pub struct JoinSet<T> {
    handles: futures_buffered::FuturesUnordered<JoinHandle<T>>,
    // We need to keep a second list of JoinHandles so we can access them for cancellation
    to_cancel: Vec<JoinHandle<T>>,
}

impl<T> Default for JoinSet<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> JoinSet<T> {
    pub fn new() -> Self {
        Self {
            handles: futures_buffered::FuturesUnordered::new(),
            to_cancel: Vec::new(),
        }
    }

    pub fn spawn(&mut self, fut: impl IntoFuture<Output = T> + 'static)
    where
        T: 'static,
    {
        let handle = JoinHandle::new();
        let handle_for_spawn = JoinHandle {
            task: handle.task.clone(),
        };
        let handle_for_cancel = JoinHandle {
            task: handle.task.clone(),
        };

        wasm_bindgen_futures::spawn_local(SpawnFuture {
            handle: handle_for_spawn,
            fut: fut.into_future(),
        });

        self.handles.push(handle);
        self.to_cancel.push(handle_for_cancel);
    }

    pub fn abort_all(&self) {
        self.to_cancel.iter().for_each(JoinHandle::abort);
    }

    pub async fn join_next(&mut self) -> Option<Result<T, JoinError>> {
        futures_lite::future::poll_fn(|cx| {
            let ret = self.handles.poll_next(cx);
            match &ret {
                Poll::Pending => tracing::info!("polled JoinSet::join_next (pending)"),
                Poll::Ready(None) => tracing::info!("polled JoinSet::join_next: None"),
                Poll::Ready(Some(Ok(_))) => {
                    tracing::info!("polled JoinSet::join_next: Some(Ok(_))")
                }
                Poll::Ready(Some(Err(e))) => {
                    tracing::info!("polled JoinSet::join_next: Some(Err({e:?}))")
                }
            }
            // clean up handles that are either cancelled or have finished
            self.to_cancel.retain(JoinHandle::is_running);
            ret
        })
        .await
    }

    pub fn is_empty(&self) -> bool {
        self.handles.is_empty()
    }

    pub fn len(&self) -> usize {
        self.handles.len()
    }
}

impl<T> Drop for JoinSet<T> {
    fn drop(&mut self) {
        self.abort_all()
    }
}

pub struct JoinHandle<T> {
    task: Rc<RefCell<Task<T>>>,
}

struct Task<T> {
    cancelled: bool,
    completed: bool,
    waker_handler: Option<Waker>,
    waker_spawn_fn: Option<Waker>,
    result: Option<T>,
}

impl<T> Task<T> {
    fn cancel(&mut self) {
        if !self.cancelled {
            self.cancelled = true;
            self.wake();
        }
    }

    fn complete(&mut self, value: T) {
        self.result = Some(value);
        self.completed = true;
        self.wake();
    }

    fn wake(&mut self) {
        if let Some(waker) = self.waker_handler.take() {
            waker.wake();
        }
        if let Some(waker) = self.waker_spawn_fn.take() {
            waker.wake();
        }
    }

    fn register_handler(&mut self, cx: &mut Context<'_>) {
        match self.waker_handler {
            // clone_from can be marginally faster in some cases
            Some(ref mut waker) => waker.clone_from(cx.waker()),
            None => self.waker_handler = Some(cx.waker().clone()),
        }
    }

    fn register_spawn_fn(&mut self, cx: &mut Context<'_>) {
        match self.waker_spawn_fn {
            // clone_from can be marginally faster in some cases
            Some(ref mut waker) => waker.clone_from(cx.waker()),
            None => self.waker_spawn_fn = Some(cx.waker().clone()),
        }
    }
}

impl<T> JoinHandle<T> {
    fn new() -> Self {
        Self {
            task: Rc::new(RefCell::new(Task {
                cancelled: false,
                completed: false,
                waker_handler: None,
                waker_spawn_fn: None,
                result: None,
            })),
        }
    }

    pub fn abort(&self) {
        self.task.borrow_mut().cancel();
    }

    fn is_running(&self) -> bool {
        let task = self.task.borrow();
        !task.cancelled && !task.completed
    }
}

#[derive(derive_more::Display, Debug, Clone, Copy)]
pub enum JoinError {
    #[display("task was cancelled")]
    Cancelled,
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        tracing::info!("JoinHandle::poll");
        let mut task = self.task.borrow_mut();
        if task.cancelled {
            tracing::info!("JoinHandle::poll: cancelled");
            return Poll::Ready(Err(JoinError::Cancelled));
        }

        if let Some(result) = task.result.take() {
            tracing::info!("JoinHandle::poll: Ready(Ok(_))");
            return Poll::Ready(Ok(result));
        }

        tracing::info!("JoinHandle::poll: Pending");
        task.register_handler(cx);
        Poll::Pending
    }
}

#[pin_project::pin_project]
pub struct SpawnFuture<Fut: Future<Output = T>, T> {
    handle: JoinHandle<T>,
    #[pin]
    fut: Fut,
}

impl<Fut: Future<Output = T>, T> Future for SpawnFuture<Fut, T> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let mut task = this.handle.task.borrow_mut();

        if task.cancelled {
            return Poll::Ready(());
        }

        match this.fut.poll(cx) {
            Poll::Ready(value) => {
                tracing::info!("waking up");
                task.complete(value);
                Poll::Ready(())
            }
            Poll::Pending => {
                task.register_spawn_fn(cx);
                Poll::Pending
            }
        }
    }
}

#[pin_project::pin_project(PinnedDrop)]
#[derive(derive_more::Debug)]
#[debug("AbortOnDropHandle")]
pub struct AbortOnDropHandle<T>(#[pin] JoinHandle<T>);

#[pin_project::pinned_drop]
impl<T> PinnedDrop for AbortOnDropHandle<T> {
    fn drop(self: Pin<&mut Self>) {
        self.0.abort();
    }
}

impl<T> Future for AbortOnDropHandle<T> {
    type Output = <JoinHandle<T> as Future>::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().0.poll(cx)
    }
}

impl<T> AbortOnDropHandle<T> {
    pub fn new(task: JoinHandle<T>) -> Self {
        Self(task)
    }
}

pub fn spawn<T: 'static>(fut: impl IntoFuture<Output = T> + 'static) -> JoinHandle<T> {
    let handle = JoinHandle::new();

    wasm_bindgen_futures::spawn_local(SpawnFuture {
        handle: JoinHandle {
            task: handle.task.clone(),
        },
        fut: fut.into_future(),
    });

    handle
}
