//! Handlers and actors to for live syncing [`iroh_sync`] replicas.
//!
//! [`iroh_sync::Replica`] is also called documents here.

use anyhow::anyhow;
use iroh_bytes::{store::Store as BaoStore, util::runtime::Handle};
use iroh_gossip::net::Gossip;
use iroh_net::{MagicEndpoint, PeerAddr};
use iroh_sync::{
    store::Store,
    sync::{Author, AuthorId, NamespaceId, Replica},
};

use crate::downloader::Downloader;

mod live;
pub mod rpc;

pub use iroh_sync::net::SYNC_ALPN;
pub use live::*;

/// The SyncEngine contains the [`LiveSync`] handle, and keeps a copy of the store and endpoint.
///
/// The RPC methods dealing with documents and sync operate on the `SyncEngine`, with method
/// implementations in [rpc].
#[derive(Debug, Clone)]
pub struct SyncEngine<S: Store> {
    pub(crate) rt: Handle,
    pub(crate) store: S,
    pub(crate) endpoint: MagicEndpoint,
    pub(crate) live: LiveSync<S>,
}

impl<S: Store> SyncEngine<S> {
    /// Start the sync engine.
    ///
    /// This will spawn a background task for the [`LiveSync`]. When documents are added to the
    /// engine with [`Self::start_sync`], then new entries inserted locally will be sent to peers
    /// through iroh-gossip.
    ///
    /// The engine will also register for [`Replica::subscribe`] events to download content for new
    /// entries from peers.
    pub fn spawn<B: BaoStore>(
        rt: Handle,
        endpoint: MagicEndpoint,
        gossip: Gossip,
        store: S,
        bao_store: B,
        downloader: Downloader,
    ) -> Self {
        let live = LiveSync::spawn(
            rt.clone(),
            endpoint.clone(),
            store.clone(),
            gossip,
            bao_store,
            downloader,
        );
        Self {
            live,
            store,
            rt,
            endpoint,
        }
    }

    /// Start to sync a document.
    ///
    /// If `peers` is non-empty, it will both do an initial set-reconciliation sync with each peer,
    /// and join an iroh-gossip swarm with these peers to receive and broadcast document updates.
    pub async fn start_sync(
        &self,
        namespace: NamespaceId,
        peers: Vec<PeerAddr>,
    ) -> anyhow::Result<()> {
        self.live.start_sync(namespace, peers).await
    }

    /// Stop syncing a document.
    pub async fn leave(&self, namespace: NamespaceId, force_close: bool) -> anyhow::Result<()> {
        self.live.leave(namespace, force_close).await
    }

    /// Shutdown the sync engine.
    pub async fn shutdown(&self) -> anyhow::Result<()> {
        self.live.shutdown().await
    }

    /// Get a [`Replica`] from the store, returning an error if the replica does not exist.
    pub fn get_replica(&self, id: &NamespaceId) -> anyhow::Result<Replica<S::Instance>> {
        self.store
            .open_replica(id)?
            .ok_or_else(|| anyhow!("doc not found"))
    }

    /// Get an [`Author`] from the store, returning an error if the replica does not exist.
    pub fn get_author(&self, id: &AuthorId) -> anyhow::Result<Author> {
        self.store
            .get_author(id)?
            .ok_or_else(|| anyhow!("author not found"))
    }

    /// Handle an incoming iroh-sync connection.
    pub async fn handle_connection(&self, conn: quinn::Connecting) -> anyhow::Result<()> {
        self.live.handle_connection(conn).await
    }
}
