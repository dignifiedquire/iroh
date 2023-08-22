//! Send data over the internet.
#![deny(missing_docs, rustdoc::broken_intra_doc_links)]

pub use iroh_bytes as bytes;
pub use iroh_net as net;

pub mod baomap;
#[cfg(feature = "iroh-collection")]
pub mod collection;
pub mod dial;
pub mod get;
pub mod node;
pub mod rpc_protocol;
pub mod util;
pub mod downloader;

/// Expose metrics module
#[cfg(feature = "metrics")]
pub mod metrics;
