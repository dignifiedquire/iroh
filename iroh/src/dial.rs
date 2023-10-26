//! The ticket type for the provider.
//!
//! This is in it's own module to enforce the invariant that you can not construct a ticket
//! with an empty address list.

use std::fmt::{self, Display};
use std::str::FromStr;

use anyhow::{ensure, Context, Result};
use iroh_bytes::protocol::RequestToken;
use iroh_bytes::{BlobFormat, Hash};
use iroh_net::derp::{DerpMap, DerpMode};
use iroh_net::key::SecretKey;
use iroh_net::PeerAddr;
use serde::{Deserialize, Serialize};

/// Options for the client
#[derive(Clone, Debug)]
pub struct Options {
    /// The secret key of the node
    pub secret_key: SecretKey,
    /// The peer to connect to.
    pub peer: PeerAddr,
    /// Whether to log the SSL keys when `SSLKEYLOGFILE` environment variable is set
    pub keylog: bool,
    /// The configuration of the derp services
    pub derp_map: Option<DerpMap>,
}

/// Create a new endpoint and dial a peer, returning the connection
///
/// Note that this will create an entirely new endpoint, so it should be only
/// used for short lived connections. If you want to connect to multiple peers,
/// it is preferable to create an endpoint and use `connect` on the endpoint.
pub async fn dial(opts: Options) -> anyhow::Result<quinn::Connection> {
    let endpoint = iroh_net::MagicEndpoint::builder()
        .secret_key(opts.secret_key)
        .keylog(opts.keylog);
    let derp_mode = match opts.derp_map {
        Some(derp_map) => DerpMode::Custom(derp_map),
        None => DerpMode::Default,
    };
    let endpoint = endpoint.derp_mode(derp_mode);
    let endpoint = endpoint.bind(0).await?;
    endpoint
        .connect(opts.peer, &iroh_bytes::protocol::ALPN)
        .await
        .context("failed to connect to provider")
}

/// A token containing everything to get a file from the provider.
///
/// It is a single item which can be easily serialized and deserialized.  The [`Display`]
/// and [`FromStr`] implementations serialize to base32.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ticket {
    /// The provider to get a file from.
    peer: PeerAddr,
    /// The format of the blob.
    format: BlobFormat,
    /// The hash to retrieve.
    hash: Hash,
    /// Optional Request token.
    token: Option<RequestToken>,
}

impl Ticket {
    /// Creates a new ticket.
    pub fn new(
        peer: PeerAddr,
        hash: Hash,
        format: BlobFormat,
        token: Option<RequestToken>,
    ) -> Result<Self> {
        ensure!(
            !peer.info.direct_addresses.is_empty(),
            "addrs list can not be empty"
        );
        Ok(Self {
            hash,
            format,
            peer,
            token,
        })
    }

    /// Deserializes from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let slf: Ticket = postcard::from_bytes(bytes)?;
        ensure!(
            !slf.peer.info.direct_addresses.is_empty(),
            "Invalid address list in ticket"
        );
        Ok(slf)
    }

    /// Serializes to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        postcard::to_stdvec(self).expect("postcard::to_stdvec is infallible")
    }

    /// The hash of the item this ticket can retrieve.
    pub fn hash(&self) -> Hash {
        self.hash
    }

    /// The [`PeerAddr`] of the provider for this ticket.
    pub fn node_addr(&self) -> &PeerAddr {
        &self.peer
    }

    /// The [`RequestToken`] for this ticket.
    pub fn token(&self) -> Option<&RequestToken> {
        self.token.as_ref()
    }

    /// The [`BlobFormat`] for this ticket.
    pub fn format(&self) -> BlobFormat {
        self.format
    }

    /// Set the [`RequestToken`] for this ticket.
    pub fn with_token(self, token: Option<RequestToken>) -> Self {
        Self { token, ..self }
    }

    /// True if the ticket is for a collection and should retrieve all blobs in it.
    pub fn recursive(&self) -> bool {
        self.format.is_hash_seq()
    }

    /// Get the contents of the ticket, consuming it.
    pub fn into_parts(self) -> (PeerAddr, Hash, BlobFormat, Option<RequestToken>) {
        let Ticket {
            peer,
            hash,
            format,
            token,
        } = self;
        (peer, hash, format, token)
    }

    /// Convert this ticket into a [`Options`], adding the given secret key.
    pub fn as_get_options(&self, secret_key: SecretKey, derp_map: Option<DerpMap>) -> Options {
        Options {
            peer: self.peer.clone(),
            secret_key,
            keylog: true,
            derp_map,
        }
    }
}

/// Serializes to base32.
impl Display for Ticket {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let encoded = self.to_bytes();
        let mut text = data_encoding::BASE32_NOPAD.encode(&encoded);
        text.make_ascii_lowercase();
        write!(f, "{text}")
    }
}

/// Deserializes from base32.
impl FromStr for Ticket {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = data_encoding::BASE32_NOPAD.decode(s.to_ascii_uppercase().as_bytes())?;
        let slf = Self::from_bytes(&bytes)?;
        Ok(slf)
    }
}

impl Serialize for Ticket {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        if serializer.is_human_readable() {
            serializer.serialize_str(&self.to_string())
        } else {
            let Ticket {
                peer,
                format,
                hash,
                token,
            } = self;
            (peer, format, hash, token).serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for Ticket {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        if deserializer.is_human_readable() {
            let s = String::deserialize(deserializer)?;
            Self::from_str(&s).map_err(serde::de::Error::custom)
        } else {
            let (peer, format, hash, token) = Deserialize::deserialize(deserializer)?;
            Self::new(peer, hash, format, token).map_err(serde::de::Error::custom)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use bao_tree::blake3;

    use super::*;

    fn make_ticket() -> Ticket {
        let hash = blake3::hash(b"hi there");
        let hash = Hash::from(hash);
        let peer = SecretKey::generate().public();
        let addr = SocketAddr::from_str("127.0.0.1:1234").unwrap();
        let token = RequestToken::new(vec![1, 2, 3, 4, 5, 6]).unwrap();
        let derp_region = Some(0);
        Ticket {
            hash,
            peer: PeerAddr::from_parts(peer, derp_region, vec![addr]),
            token: Some(token),
            format: BlobFormat::HashSeq,
        }
    }

    #[test]
    fn test_ticket_postcard() {
        let ticket = make_ticket();
        let bytes = postcard::to_stdvec(&ticket).unwrap();
        let ticket2: Ticket = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(ticket2, ticket);
    }

    #[test]
    fn test_ticket_json() {
        let ticket = make_ticket();
        let json = serde_json::to_string(&ticket).unwrap();
        let ticket2: Ticket = serde_json::from_str(&json).unwrap();
        assert_eq!(ticket2, ticket);
    }

    #[test]
    fn test_ticket_base32_roundtrip() {
        let ticket = make_ticket();
        let base32 = ticket.to_string();
        println!("Ticket: {base32}");
        println!("{} bytes", base32.len());

        let ticket2: Ticket = base32.parse().unwrap();
        assert_eq!(ticket2, ticket);
    }
}
