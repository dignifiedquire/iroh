use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use futures::Future;

use crate::error::RpcError;
use crate::stream::StreamConfig;

pub struct Namespace<T> {
    name: String,
    handlers: HashMap<String, BoxedHandler<T>>,
}

impl<T> Namespace<T> {
    pub fn new(name: String) -> Self {
        Self {
            name,
            handlers: Default::default(),
        }
    }

    pub fn with_method<S: Into<String>, H>(mut self, method: S, handler: H) -> Self
    where
        H: Factory<T> + Send + Sync + 'static,
        T: Send + Sync + 'static,
    {
        self.handlers
            .insert(method.into(), Handler::new(handler).into());
        self
    }

    pub async fn handle(
        &mut self,
        method: String,
        state: State<T>,
        stream: Option<StreamConfig>,
        params: Vec<u8>,
    ) -> Result<Vec<u8>, RpcError> {
        let handler = match self.handlers.get(&method) {
            Some(h) => &h.0,
            None => return Err(RpcError::MethodNotFound(method)),
        };
        handler(state, stream, params).await
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }
}
pub struct State<T>(pub Arc<T>);

impl<T> State<T> {
    pub fn new(t: T) -> Self {
        State(Arc::new(t))
    }
}

impl<T> std::ops::Deref for State<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

#[async_trait::async_trait]
pub trait Factory<T> {
    async fn handle(
        &self,
        state: State<T>,
        stream_cfg: Option<StreamConfig>,
        param: Vec<u8>,
    ) -> Result<Vec<u8>, RpcError>;
}

pub struct Handler<T, F: Factory<T>> {
    hnd: F,
    _t: PhantomData<T>,
}

impl<T, F: Factory<T>> Handler<T, F> {
    fn new(hnd: F) -> Self {
        Handler {
            hnd,
            _t: PhantomData,
        }
    }
}

#[async_trait::async_trait]
impl<FN, I, T> Factory<T> for FN
where
    I: Future<Output = Result<Vec<u8>, RpcError>> + Send + 'static,
    FN: Fn(State<T>, Option<StreamConfig>, Vec<u8>) -> I + Sync,
    T: Send + Sync + 'static,
{
    async fn handle(
        &self,
        state: State<T>,
        stream_id: Option<StreamConfig>,
        param: Vec<u8>,
    ) -> Result<Vec<u8>, RpcError> {
        (self)(state, stream_id, param).await
    }
}

// TODO: refactor for less complexity
#[allow(clippy::type_complexity)]
pub struct BoxedHandler<T>(
    Box<
        dyn Fn(
                State<T>,
                Option<StreamConfig>,
                Vec<u8>,
            )
                -> std::pin::Pin<Box<dyn Future<Output = Result<Vec<u8>, RpcError>> + Send>>
            + Send
            + Sync,
    >,
);

impl<T, F> From<Handler<T, F>> for BoxedHandler<T>
where
    T: Send + Sync + 'static,
    F: Factory<T> + Send + Sync + 'static,
{
    fn from(t: Handler<T, F>) -> BoxedHandler<T> {
        let hnd = Arc::new(t.hnd);

        let inner = move |state: State<T>, stream: Option<StreamConfig>, params: Vec<u8>| {
            let hnd = Arc::clone(&hnd);
            Box::pin(async move {
                let out = { hnd.handle(state, stream, params).await? };
                Ok(out)
            })
                as std::pin::Pin<Box<dyn Future<Output = Result<Vec<u8>, RpcError>> + Send>>
        };
        BoxedHandler(Box::new(inner))
    }
}
