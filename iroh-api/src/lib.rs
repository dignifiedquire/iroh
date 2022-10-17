mod api;
mod api_ext;
mod config;
mod p2p;
mod size;

#[cfg(feature = "testing")]
pub use crate::api::MockApi;
pub use crate::api::{Api, Iroh, OutType};
pub use crate::api_ext::ApiExt;
#[cfg(feature = "testing")]
pub use crate::p2p::MockP2p;
pub use crate::p2p::P2p as P2pApi;
pub use crate::p2p::{Lookup, PeerIdOrAddr};
pub use bytes::Bytes;
pub use cid::Cid;
pub use iroh_resolver::resolver::Path as IpfsPath;
pub use iroh_rpc_client::{ServiceStatus, StatusRow, StatusTable};
pub use libp2p::gossipsub::MessageId;
pub use libp2p::{Multiaddr, PeerId};
// TODO(faassen) we don't want to expose this but perhaps move it, or
// instead use it to also construct a Directory
pub use crate::size::size_stream;
