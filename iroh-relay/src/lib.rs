//! Iroh's relay is a feature within [iroh](https://github.com/n0-computer/iroh), a peer-to-peer
//! networking system designed to facilitate direct, encrypted connections between devices. Iroh
//! aims to simplify decentralized communication by automatically handling connections through
//! "relays" when direct connections aren't immediately possible. The relay server helps establish
//! connections by temporarily routing encrypted traffic until a direct, P2P connection is
//! feasible. Once this direct path is set up, the relay server steps back, and the data flows
//! directly between devices. This approach allows Iroh to maintain a secure, low-latency
//! connection, even in challenging network situations.
//!
//! This crate provides a complete setup for creating and interacting with iroh relays, including:
//! - [`protos::relay`]: The protocol used to communicate between relay servers and clients. It's a
//!   revised version of the Designated Encrypted Relay for Packets (DERP) protocol written by
//!   Tailscale.
//! - [`server`]: A fully-fledged iroh-relay server over HTTP or HTTPS. Optionally will also
//!   expose a stun endpoint and metrics.
//! - [`client`]: A client for establishing connections to the relay.
//! - *Server Binary*: A CLI for running your own relay server. It can be configured to also offer
//!   STUN support and expose metrics.
// Based on tailscale/derp/derp.go

#![cfg_attr(iroh_docsrs, feature(doc_cfg))]
#![deny(missing_docs, rustdoc::broken_intra_doc_links)]

pub mod client;
pub mod defaults;
pub mod http;
pub mod protos;
#[cfg(feature = "server")]
#[cfg_attr(iroh_docsrs, doc(cfg(feature = "server")))]
pub mod server;

#[cfg(test)]
mod dns;

pub use iroh_base::node_addr::RelayUrl;
pub use protos::relay::MAX_PACKET_SIZE;

pub use self::client::{
    conn::{Conn as RelayConn, ReceivedMessage},
    Client as HttpClient, ClientBuilder as HttpClientBuilder, ClientError as HttpClientError,
    ClientReceiver as HttpClientReceiver,
};
