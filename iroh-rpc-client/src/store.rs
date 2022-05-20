use std::io::Cursor;
use std::net::SocketAddr;

use anyhow::{Context, Result};
use bytes::Bytes;
use cid::Cid;
use iroh_rpc_types::store::{self, GetLinksRequest, GetRequest, PutRequest};

#[derive(Debug, Clone)]
pub struct StoreClient(store::store_client::StoreClient<tonic::transport::Channel>);

impl StoreClient {
    pub async fn new(addr: SocketAddr) -> Result<Self> {
        let conn = tonic::transport::Endpoint::new(format!("http://{}", addr))?
            .keep_alive_while_idle(true)
            .connect_lazy();

        let client = store::store_client::StoreClient::new(conn);

        Ok(StoreClient(client))
    }

    pub async fn put(&self, cid: Cid, blob: Bytes, links: Vec<Cid>) -> Result<()> {
        let req = iroh_metrics::req::trace_tonic_req(PutRequest {
            cid: cid.to_bytes(),
            blob,
            links: links.iter().map(|l| l.to_bytes()).collect(),
        });
        self.0.clone().put(req).await?;
        Ok(())
    }

    pub async fn get(&self, cid: Cid) -> Result<Option<Bytes>> {
        let req = iroh_metrics::req::trace_tonic_req(GetRequest {
            cid: cid.to_bytes(),
        });
        let res = self.0.clone().get(req).await?;
        Ok(res.into_inner().data)
    }

    pub async fn get_links(&self, cid: Cid) -> Result<Option<Vec<Cid>>> {
        let req = iroh_metrics::req::trace_tonic_req(GetLinksRequest {
            cid: cid.to_bytes(),
        });
        let links = self.0.clone().get_links(req).await?.into_inner().links;
        if links.is_empty() {
            Ok(None)
        } else {
            let links: Result<Vec<Cid>> = links
                .iter()
                .map(|l| Cid::read_bytes(Cursor::new(l)).context(format!("invalid cid: {:?}", l)))
                .collect();
            Ok(Some(links?))
        }
    }
}
