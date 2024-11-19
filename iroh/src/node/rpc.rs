use std::{fmt::Debug, sync::Arc, time::Duration};

use anyhow::Result;
use iroh_blobs::{
    net_protocol::Blobs as BlobsProtocol, store::Store as BaoStore,
    util::local_pool::LocalPoolHandle,
};
use iroh_docs::net::DOCS_ALPN;
use iroh_gossip::net::{Gossip, GOSSIP_ALPN};
use iroh_node_util::rpc::server::Node;
use iroh_router::Router;
use quic_rpc::server::{RpcChannel, RpcServerError};
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

use super::IrohServerEndpoint;
use crate::{
    node::NodeInner,
    rpc_protocol::{Request, RpcService},
};

pub(crate) type RpcError = serde_error::Error;
pub(crate) type RpcResult<T> = Result<T, RpcError>;

#[derive(Debug, Clone)]
pub(crate) struct Handler<D> {
    pub(crate) inner: Arc<NodeInner<D>>,
    pub(crate) router: Router,
}

impl<D> Handler<D> {
    pub fn new(inner: Arc<NodeInner<D>>, router: Router) -> Self {
        Self { inner, router }
    }
}

impl<D: BaoStore> Handler<D> {
    fn blobs(&self) -> Arc<BlobsProtocol<D>> {
        self.router
            .get_protocol::<BlobsProtocol<D>>(iroh_blobs::protocol::ALPN)
            .expect("missing blobs")
    }

    pub(crate) fn spawn_rpc_request(
        inner: Arc<NodeInner<D>>,
        join_set: &mut JoinSet<anyhow::Result<()>>,
        accepting: quic_rpc::server::Accepting<RpcService, IrohServerEndpoint>,
        router: Router,
    ) {
        let handler = Self::new(inner, router);
        join_set.spawn(async move {
            let (msg, chan) = accepting.read_first().await?;
            if let Err(err) = handler.handle_rpc_request(msg, chan).await {
                warn!("rpc request handler error: {err:?}");
            }
            Ok(())
        });
    }

    async fn handle_node_request(
        self,
        msg: iroh_node_util::rpc::proto::Request,
        chan: RpcChannel<RpcService, IrohServerEndpoint>,
    ) -> Result<(), RpcServerError<IrohServerEndpoint>> {
        debug!("handling node request: {msg:?}");
        let node = Node::new(
            self.inner.endpoint.clone(),
            self.inner.cancel_token.clone(),
            self.inner.rpc_addr.clone(),
        );
        node.handle_rpc_request(msg, chan.map().boxed())
            .await
            .map_err(|e| e.errors_into())
    }

    async fn handle_blobs_request(
        self,
        msg: iroh_blobs::rpc::proto::Request,
        chan: RpcChannel<iroh_blobs::rpc::proto::RpcService>,
    ) -> Result<(), RpcServerError<IrohServerEndpoint>> {
        self.blobs()
            .handle_rpc_request(msg, chan)
            .await
            .map_err(|e| e.errors_into())
    }

    async fn handle_gossip_request(
        self,
        msg: iroh_gossip::RpcRequest,
        chan: RpcChannel<RpcService, IrohServerEndpoint>,
    ) -> Result<(), RpcServerError<IrohServerEndpoint>> {
        let gossip = self
            .router
            .get_protocol::<Gossip>(GOSSIP_ALPN)
            .expect("missing gossip");
        let chan = chan.map::<iroh_gossip::RpcService>();
        gossip
            .as_ref()
            .clone()
            .handle_rpc_request(msg, chan)
            .await
            .map_err(|e| e.errors_into())
    }

    async fn handle_docs_request(
        self,
        msg: iroh_docs::rpc::proto::Request,
        chan: RpcChannel<RpcService, IrohServerEndpoint>,
    ) -> Result<(), RpcServerError<IrohServerEndpoint>> {
        if let Some(docs) = self
            .router
            .get_protocol::<iroh_docs::engine::Engine<D>>(DOCS_ALPN)
        {
            let chan = chan.map::<iroh_docs::rpc::proto::RpcService>();
            docs.as_ref()
                .clone()
                .handle_rpc_request(msg, chan)
                .await
                .map_err(|e| e.errors_into())
        } else {
            Err(RpcServerError::SendError(anyhow::anyhow!(
                "Docs is not enabled"
            )))
        }
    }

    pub(crate) async fn handle_rpc_request(
        self,
        msg: Request,
        chan: RpcChannel<RpcService, IrohServerEndpoint>,
    ) -> Result<(), RpcServerError<IrohServerEndpoint>> {
        use Request::*;
        debug!("handling rpc request: {msg}");
        match msg {
            Node(msg) => self.handle_node_request(msg, chan).await,
            BlobsAndTags(msg) => self.handle_blobs_request(msg, chan.map().boxed()).await,
            Docs(msg) => self.handle_docs_request(msg, chan).await,
            Gossip(msg) => self.handle_gossip_request(msg, chan).await,
        }
    }

    fn local_pool_handle(&self) -> LocalPoolHandle {
        self.inner.local_pool_handle.clone()
    }
}
