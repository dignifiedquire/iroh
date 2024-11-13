use std::collections::BTreeMap;

use anyhow::Result;
use quic_rpc::RpcClient;

use super::net::NodeStatus;
use crate::rpc::proto::{node::*, RpcService};

#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct Client {
    pub(super) rpc: RpcClient<RpcService>,
}

impl Client {
    /// Shuts down the node.
    ///
    /// If `force` is true, the node will be shut down instantly without
    /// waiting for things to stop gracefully.
    pub async fn shutdown(&self, force: bool) -> Result<()> {
        self.rpc.rpc(ShutdownRequest { force }).await?;
        Ok(())
    }

    /// Fetches statistics of the running node.
    pub async fn stats(&self) -> Result<BTreeMap<String, CounterStats>> {
        let res = self.rpc.rpc(StatsRequest {}).await??;
        Ok(res.stats)
    }

    /// Fetches status information about this node.
    pub async fn status(&self) -> Result<NodeStatus> {
        let response = self.rpc.rpc(StatusRequest).await??;
        Ok(response)
    }
}
