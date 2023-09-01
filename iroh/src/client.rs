//! Client to an iroh node. Is generic over the connection (in-memory or RPC).
//!
//! TODO: Contains only iroh sync related methods. Add other methods.

use std::collections::HashMap;
use std::result::Result as StdResult;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use iroh_bytes::Hash;
use iroh_net::{key::PublicKey, magic_endpoint::ConnectionInfo};
use iroh_sync::store::GetFilter;
use iroh_sync::sync::{AuthorId, NamespaceId};
use iroh_sync::Entry;
use quic_rpc::{RpcClient, ServiceConnection};

use crate::rpc_protocol::{
    AuthorCreateRequest, AuthorListRequest, BytesGetRequest, ConnectionInfoRequest,
    ConnectionInfoResponse, ConnectionsRequest, CounterStats, DocCreateRequest, DocGetOneRequest,
    DocGetRequest, DocImportRequest, DocInfoRequest, DocListRequest, DocSetRequest,
    DocShareRequest, DocStartSyncRequest, DocStopSyncRequest, DocSubscribeRequest, DocTicket,
    ProviderService, ShareMode, StatsGetRequest,
};
use crate::sync_engine::{LiveEvent, LiveStatus, PeerSource};

pub mod mem;
#[cfg(feature = "cli")]
pub mod quic;

/// Iroh client
#[derive(Debug, Clone)]
pub struct Iroh<C> {
    rpc: RpcClient<ProviderService, C>,
}

impl<C> Iroh<C>
where
    C: ServiceConnection<ProviderService>,
{
    /// Create a new high-level client to a Iroh node from the low-level RPC client.
    pub fn new(rpc: RpcClient<ProviderService, C>) -> Self {
        Self { rpc }
    }

    /// Create a new document author.
    pub async fn create_author(&self) -> Result<AuthorId> {
        let res = self.rpc.rpc(AuthorCreateRequest).await??;
        Ok(res.author_id)
    }

    /// List document authors for which we have a secret key.
    pub async fn list_authors(&self) -> Result<impl Stream<Item = Result<AuthorId>>> {
        let stream = self.rpc.server_streaming(AuthorListRequest {}).await?;
        Ok(flatten(stream).map_ok(|res| res.author_id))
    }

    /// Create a new document.
    pub async fn create_doc(&self) -> Result<Doc<C>> {
        let res = self.rpc.rpc(DocCreateRequest {}).await??;
        let doc = Doc {
            id: res.id,
            rpc: self.rpc.clone(),
        };
        Ok(doc)
    }

    /// Import a document from a ticket and join all peers in the ticket.
    pub async fn import_doc(&self, ticket: DocTicket) -> Result<Doc<C>> {
        let res = self.rpc.rpc(DocImportRequest(ticket)).await??;
        let doc = Doc {
            id: res.doc_id,
            rpc: self.rpc.clone(),
        };
        Ok(doc)
    }

    /// List all documents.
    pub async fn list_docs(&self) -> Result<impl Stream<Item = Result<NamespaceId>>> {
        let stream = self.rpc.server_streaming(DocListRequest {}).await?;
        Ok(flatten(stream).map_ok(|res| res.id))
    }

    /// Get a [`Doc`] client for a single document. Return an error if the document cannot be found.
    pub async fn get_doc(&self, id: NamespaceId) -> Result<Doc<C>> {
        match self.try_get_doc(id).await? {
            Some(doc) => Ok(doc),
            None => Err(anyhow!("Document not found")),
        }
    }

    /// Get a [`Doc`] client for a single document. Return None if the document cannot be found.
    pub async fn try_get_doc(&self, id: NamespaceId) -> Result<Option<Doc<C>>> {
        if let Err(_err) = self.rpc.rpc(DocInfoRequest { doc_id: id }).await? {
            return Ok(None);
        }
        let doc = Doc {
            id,
            rpc: self.rpc.clone(),
        };
        Ok(Some(doc))
    }

    /// Get the bytes for a hash.
    ///
    /// Note: This reads the full blob into memory.
    // TODO: add get_reader for streaming gets
    pub async fn get_bytes(&self, hash: Hash) -> Result<Bytes> {
        let res = self.rpc.rpc(BytesGetRequest { hash }).await??;
        Ok(res.data)
    }

    /// Get statistics of the running node.
    pub async fn stats(&self) -> Result<HashMap<String, CounterStats>> {
        let res = self.rpc.rpc(StatsGetRequest {}).await??;
        Ok(res.stats)
    }

    /// Get information about the different connections we have made
    pub async fn connections(&self) -> Result<impl Stream<Item = Result<ConnectionInfo>>> {
        let stream = self.rpc.server_streaming(ConnectionsRequest {}).await?;
        Ok(flatten(stream).map_ok(|res| res.conn_info))
    }

    /// Get connection information about a node
    pub async fn connection_info(&self, node_id: PublicKey) -> Result<Option<ConnectionInfo>> {
        let ConnectionInfoResponse { conn_info } =
            self.rpc.rpc(ConnectionInfoRequest { node_id }).await??;
        Ok(conn_info)
    }
}

/// Document handle
#[derive(Debug, Clone)]
pub struct Doc<C> {
    id: NamespaceId,
    rpc: RpcClient<ProviderService, C>,
}

impl<C> Doc<C>
where
    C: ServiceConnection<ProviderService>,
{
    /// Get the document id of this doc.
    pub fn id(&self) -> NamespaceId {
        self.id
    }

    /// Set the content of a key to a byte array.
    pub async fn set_bytes(
        &self,
        author_id: AuthorId,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<Hash> {
        let res = self
            .rpc
            .rpc(DocSetRequest {
                doc_id: self.id,
                author_id,
                key,
                value,
            })
            .await??;
        Ok(res.entry.content_hash())
    }

    /// Get the contents of an entry as a byte array.
    // TODO: add get_content_reader
    pub async fn get_content_bytes(&self, hash: Hash) -> Result<Bytes> {
        let bytes = self.rpc.rpc(BytesGetRequest { hash }).await??;
        Ok(bytes.data)
    }

    /// Get the latest entry for a key and author.
    pub async fn get_one(&self, author: AuthorId, key: Vec<u8>) -> Result<Option<Entry>> {
        let res = self
            .rpc
            .rpc(DocGetOneRequest {
                author,
                key,
                doc_id: self.id,
            })
            .await??;
        Ok(res.entry.map(|entry| entry.into()))
    }

    /// Get entries.
    pub async fn get(&self, filter: GetFilter) -> Result<impl Stream<Item = Result<Entry>>> {
        let stream = self
            .rpc
            .server_streaming(DocGetRequest {
                doc_id: self.id,
                filter,
            })
            .await?;
        Ok(flatten(stream).map_ok(|res| res.entry.into()))
    }

    /// Share this document with peers over a ticket.
    pub async fn share(&self, mode: ShareMode) -> anyhow::Result<DocTicket> {
        let res = self
            .rpc
            .rpc(DocShareRequest {
                doc_id: self.id,
                mode,
            })
            .await??;
        Ok(res.0)
    }

    /// Start to sync this document with a list of peers.
    pub async fn start_sync(&self, peers: Vec<PeerSource>) -> Result<()> {
        let _res = self
            .rpc
            .rpc(DocStartSyncRequest {
                doc_id: self.id,
                peers,
            })
            .await??;
        Ok(())
    }

    /// Stop the live sync for this document.
    pub async fn stop_sync(&self) -> Result<()> {
        let _res = self
            .rpc
            .rpc(DocStopSyncRequest { doc_id: self.id })
            .await??;
        Ok(())
    }

    /// Subscribe to events for this document.
    pub async fn subscribe(&self) -> anyhow::Result<impl Stream<Item = anyhow::Result<LiveEvent>>> {
        let stream = self
            .rpc
            .server_streaming(DocSubscribeRequest { doc_id: self.id })
            .await?;
        Ok(flatten(stream).map_ok(|res| res.event).map_err(Into::into))
    }

    /// Get status info for this document
    pub async fn status(&self) -> anyhow::Result<LiveStatus> {
        let res = self.rpc.rpc(DocInfoRequest { doc_id: self.id }).await??;
        Ok(res.status)
    }
}

fn flatten<T, E1, E2>(
    s: impl Stream<Item = StdResult<StdResult<T, E1>, E2>>,
) -> impl Stream<Item = Result<T>>
where
    E1: std::error::Error + Send + Sync + 'static,
    E2: std::error::Error + Send + Sync + 'static,
{
    s.map(|res| match res {
        Ok(Ok(res)) => Ok(res),
        Ok(Err(err)) => Err(err.into()),
        Err(err) => Err(err.into()),
    })
}
