//! The ticket type for the provider.
//!
//! This is in it's own module to enforce the invariant that you can not construct a ticket
//! with an empty address list.

use std::fmt::{self, Display};
use std::net::SocketAddr;
use std::str::FromStr;

use anyhow::{ensure, Result};
use iroh_net::tls::{Keypair, PeerId};
use serde::{Deserialize, Serialize};

use crate::protocol::RequestToken;
use crate::{get, Hash};

/// A token containing everything to get a file from the provider.
///
/// It is a single item which can be easily serialized and deserialized.  The [`Display`]
/// and [`FromStr`] implementations serialize to base32.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Ticket {
    /// The hash to retrieve.
    hash: Hash,
    /// The peer ID identifying the provider.
    peer: PeerId,
    /// Optional Request token.
    token: Option<RequestToken>,
    /// The socket addresses the provider is listening on.
    ///
    /// This will never be empty.
    addrs: Vec<SocketAddr>,
}

impl Ticket {
    pub fn new(
        hash: Hash,
        peer: PeerId,
        addrs: Vec<SocketAddr>,
        token: Option<RequestToken>,
    ) -> Result<Self> {
        ensure!(!addrs.is_empty(), "addrs list can not be empty");
        Ok(Self {
            hash,
            peer,
            addrs,
            token,
        })
    }

    /// Deserializes from bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let slf: Ticket = postcard::from_bytes(bytes)?;
        ensure!(!slf.addrs.is_empty(), "Invalid address list in ticket");
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

    /// The [`PeerId`] of the provider for this ticket.
    pub fn peer(&self) -> PeerId {
        self.peer
    }

    /// The [`RequestToken`] for this ticket.
    pub fn token(&self) -> Option<&RequestToken> {
        self.token.as_ref()
    }

    /// The addresses on which the provider can be reached.
    ///
    /// This is guaranteed to be non-empty.
    pub fn addrs(&self) -> &[SocketAddr] {
        &self.addrs
    }

    /// Get the contents of the ticket, consuming it.
    pub fn into_parts(self) -> (Hash, PeerId, Vec<SocketAddr>, Option<RequestToken>) {
        let Ticket {
            hash,
            peer,
            token,
            addrs,
        } = self;
        (hash, peer, addrs, token)
    }

    pub fn get_options(&self, keypair: Keypair) -> get::Options {
        get::Options {
            peer_id: self.peer,
            addrs: self.addrs.clone(),
            keypair,
            keylog: true,
            derp_map: None,
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

#[cfg(test)]
mod tests {
    use iroh_net::tls::Keypair;

    use super::*;

    #[test]
    fn test_ticket_base32_roundtrip() {
        let hash = blake3::hash(b"hi there");
        let hash = Hash::from(hash);
        let peer = PeerId::from(Keypair::generate().public());
        let addr = SocketAddr::from_str("127.0.0.1:1234").unwrap();
        let token = RequestToken::new(vec![1, 2, 3, 4, 5, 6]).unwrap();
        let ticket = Ticket {
            hash,
            peer,
            addrs: vec![addr],
            token: Some(token),
        };
        let base32 = ticket.to_string();
        println!("Ticket: {base32}");
        println!("{} bytes", base32.len());

        let ticket2: Ticket = base32.parse().unwrap();
        assert_eq!(ticket2, ticket);
    }
}
