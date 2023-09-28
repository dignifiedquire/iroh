//! Network implementation of the iroh-sync protocol

use std::future::Future;

use iroh_net::{key::PublicKey, magic_endpoint::get_peer_id, MagicEndpoint, PeerAddr};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::{
    net::codec::{run_alice, BobState},
    store,
    sync::AsyncReplica as Replica,
    NamespaceId, SyncProgress,
};

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;
#[cfg(feature = "metrics")]
use iroh_metrics::inc;

/// The ALPN identifier for the iroh-sync protocol
pub const SYNC_ALPN: &[u8] = b"/iroh-sync/1";

mod codec;

/// Connect to a peer and sync a replica
///
/// With `cancel` you may abort the request during the connection phase. After having connected to
/// the peer, `cancel` does not have any effect anymore.
pub async fn connect_and_sync<S: store::Store>(
    endpoint: &MagicEndpoint,
    doc: &Replica<S::Instance>,
    peer: PeerAddr,
    cancel: CancellationToken,
) -> Result<SyncFinished, ConnectError> {
    let peer_id = peer.peer_id;
    debug!(?peer_id, "sync[dial]: connect");
    let namespace = doc.namespace();
    let connection = tokio::select! {
        biased;
        _  = cancel.cancelled() => return Err(ConnectError::Cancelled),
        res = endpoint.connect(peer, SYNC_ALPN) => {
            res.map_err(ConnectError::connect)?
        }
    };
    debug!(?peer_id, ?namespace, "sync[dial]: connected");
    let (mut send_stream, mut recv_stream) =
        connection.open_bi().await.map_err(ConnectError::connect)?;
    let res = run_alice::<S, _, _>(&mut send_stream, &mut recv_stream, doc, peer_id).await;

    send_stream.finish().await.map_err(ConnectError::close)?;
    recv_stream
        .read_to_end(0)
        .await
        .map_err(ConnectError::close)?;

    #[cfg(feature = "metrics")]
    if res.is_ok() {
        inc!(Metrics, sync_via_connect_success);
    } else {
        inc!(Metrics, sync_via_connect_failure);
    }

    debug!(?peer_id, ?namespace, ?res, "sync[dial]: done");
    let progress = res?;

    let res = SyncFinished {
        namespace,
        peer: peer_id,
        progress,
    };

    Ok(res)
}

/// What to do with incoming sync requests
pub type AcceptOutcome<S> = Result<Replica<<S as store::Store>::Instance>, AbortReason>;

/// Handle an iroh-sync connection and sync all shared documents in the replica store.
pub async fn handle_connection<S, F, Fut>(
    connecting: quinn::Connecting,
    accept_cb: F,
) -> Result<SyncFinished, AcceptError>
where
    S: store::Store,
    F: Fn(NamespaceId, PublicKey) -> Fut,
    Fut: Future<Output = anyhow::Result<AcceptOutcome<S>>>,
{
    let connection = connecting.await.map_err(AcceptError::connect)?;
    let peer = get_peer_id(&connection)
        .await
        .map_err(AcceptError::connect)?;
    let (mut send_stream, mut recv_stream) = connection
        .accept_bi()
        .await
        .map_err(|e| AcceptError::open(peer, e))?;
    debug!(?peer, "sync[accept]: handle");

    let mut state = BobState::<S>::new(peer);
    let res = state
        .run(&mut send_stream, &mut recv_stream, accept_cb)
        .await;

    #[cfg(feature = "metrics")]
    if res.is_ok() {
        inc!(Metrics, sync_via_accept_success);
    } else {
        inc!(Metrics, sync_via_accept_failure);
    }

    let namespace = state.namespace();
    let progress = state.into_progress();

    send_stream
        .finish()
        .await
        .map_err(|error| AcceptError::close(peer, namespace, error))?;
    recv_stream
        .read_to_end(0)
        .await
        .map_err(|error| AcceptError::close(peer, namespace, error))?;

    let namespace = res?;

    debug!(?peer, ?namespace, "sync[accept]: done");

    let res = SyncFinished {
        namespace,
        progress,
        peer,
    };

    Ok(res)
}

///
pub struct SyncFinished {
    ///
    pub namespace: NamespaceId,
    ///
    pub peer: PublicKey,
    ///
    pub progress: SyncProgress,
}

/// Errors that may occur on handling incoming sync connections.
#[derive(thiserror::Error, Debug)]
#[allow(missing_docs)]
pub enum AcceptError {
    /// Failed to establish connection
    #[error("Failed to establish connection")]
    Connect {
        #[source]
        error: anyhow::Error,
    },
    /// Failed to open replica
    #[error("Failed to open replica with {peer:?}")]
    Open {
        peer: PublicKey,
        #[source]
        error: anyhow::Error,
    },
    /// We aborted the sync request.
    #[error("Aborted sync of {namespace:?} with {peer:?}: {reason:?}")]
    Abort {
        peer: PublicKey,
        namespace: NamespaceId,
        reason: AbortReason,
    },
    /// Failed to run sync
    #[error("Failed to sync {namespace:?} with {peer:?}")]
    Sync {
        peer: PublicKey,
        namespace: Option<NamespaceId>,
        #[source]
        error: anyhow::Error,
    },
    /// Failed to close
    #[error("Failed to close {namespace:?} with {peer:?}")]
    Close {
        peer: PublicKey,
        namespace: Option<NamespaceId>,
        #[source]
        error: anyhow::Error,
    },
}

/// Errors that may occur on outgoing sync requests.
#[derive(thiserror::Error, Debug)]
#[allow(missing_docs)]
pub enum ConnectError {
    /// Failed to establish connection
    #[error("Failed to establish connection")]
    Connect {
        #[source]
        error: anyhow::Error,
    },
    /// The remote peer aborted the sync request.
    #[error("Remote peer aborted sync: {0:?}")]
    RemoteAbort(AbortReason),
    /// We cancelled the operation
    #[error("Cancelled")]
    Cancelled,
    /// Failed to run sync
    #[error("Failed to sync")]
    Sync {
        #[source]
        error: anyhow::Error,
    },
    /// Failed to close
    #[error("Failed to close connection1")]
    Close {
        #[source]
        error: anyhow::Error,
    },
}

/// Reason why we aborted an incoming sync request.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AbortReason {
    /// Namespace is not avaiable.
    NotAvailable,
    /// We are already syncing this namespace.
    AlreadySyncing,
}

impl AcceptError {
    fn connect(error: impl Into<anyhow::Error>) -> Self {
        Self::Connect {
            error: error.into(),
        }
    }
    fn open(peer: PublicKey, error: impl Into<anyhow::Error>) -> Self {
        Self::Open {
            peer,
            error: error.into(),
        }
    }
    pub(crate) fn sync(
        peer: PublicKey,
        namespace: Option<NamespaceId>,
        error: impl Into<anyhow::Error>,
    ) -> Self {
        Self::Sync {
            peer,
            namespace,
            error: error.into(),
        }
    }
    fn close(
        peer: PublicKey,
        namespace: Option<NamespaceId>,
        error: impl Into<anyhow::Error>,
    ) -> Self {
        Self::Close {
            peer,
            namespace,
            error: error.into(),
        }
    }
    /// Get the peer's node ID (if available)
    pub fn peer(&self) -> Option<PublicKey> {
        match self {
            AcceptError::Connect { .. } => None,
            AcceptError::Open { peer, .. } => Some(*peer),
            AcceptError::Sync { peer, .. } => Some(*peer),
            AcceptError::Close { peer, .. } => Some(*peer),
            AcceptError::Abort { peer, .. } => Some(*peer),
        }
    }

    /// Get the namespace (if available)
    pub fn namespace(&self) -> Option<NamespaceId> {
        match self {
            AcceptError::Connect { .. } => None,
            AcceptError::Open { .. } => None,
            AcceptError::Sync { namespace, .. } => namespace.to_owned(),
            AcceptError::Close { namespace, .. } => namespace.to_owned(),
            AcceptError::Abort { namespace, .. } => Some(*namespace),
        }
    }
}

impl ConnectError {
    fn connect(error: impl Into<anyhow::Error>) -> Self {
        Self::Connect {
            error: error.into(),
        }
    }
    fn close(error: impl Into<anyhow::Error>) -> Self {
        Self::Close {
            error: error.into(),
        }
    }
    pub(crate) fn sync(error: impl Into<anyhow::Error>) -> Self {
        Self::Sync {
            error: error.into(),
        }
    }
    pub(crate) fn remote_abort(reason: AbortReason) -> Self {
        Self::RemoteAbort(reason)
    }
}
