//! This module manages the different tickets Iroh has.

pub mod blob;
pub mod doc;

const PREFIX_SEPARATOR: char = ':';

/// Kind of ticket.
#[derive(Debug, strum::EnumString, strum::Display, PartialEq, Eq, Clone, Copy)]
#[strum(serialize_all = "snake_case")]
pub enum Kind {
    /// A blob ticket.
    Blob,
    /// A document ticket.
    Doc,
    /// A ticket for an Iroh node.
    Node,
}

impl Kind {
    /// Parse the ticket prefix to obtain the [`Kind`] and remainig string.
    pub fn parse_prefix(s: &str) -> Option<Result<(Self, &str), Error>> {
        let (prefix, rest) = s.split_once(PREFIX_SEPARATOR)?;
        match prefix.parse() {
            Ok(kind) => Some(Ok((kind, rest))),
            Err(e) => Some(Err(e.into())),
        }
    }
}

/// An error deserializing an iroh ticket.
#[derive(Debug, derive_more::Display, thiserror::Error)]
pub enum Error {
    /// Found a ticket of the wrong [`Kind`].
    #[display("expected a {expected} ticket but found {found}")]
    WrongKind {
        /// Expected [`Kind`] of ticket.
        expected: Kind,
        /// Found [`Kind`] of ticket.
        found: Kind,
    },
    /// It appears to be a ticket but the prefix is not a known one.
    #[display("unrecogized ticket prefix")]
    UnrecognizedKind(#[from] strum::ParseError),
    /// This does not appear to be a ticket.
    #[display("not a {expected} ticket")]
    MissingKind {
        /// Prefix that is missing.
        expected: Kind,
    },
    /// This looks like a ticket, but postcard deserialization failed.
    #[display("deserialization failed: {_0}")]
    Postcard(#[from] postcard::Error),
    /// This looks like a ticket, but base32 decoding failed.
    #[display("decoding failed: {_0}")]
    Encoding(#[from] data_encoding::DecodeError),
    /// Verification of the deserialized bytes failed.
    #[display("verification failed: {_0}")]
    Verify(&'static str),
}

trait IrohTicket: serde::Serialize + for<'de> serde::Deserialize<'de> {
    /// Kind of Iroh ticket.
    const KIND: Kind;

    /// Serialize to postcard bytes.
    fn to_bytes(&self) -> Vec<u8> {
        postcard::to_stdvec(&self).expect("postcard::to_stdvec is infallible")
    }

    /// Deserialize from postcard bytes.
    fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let ticket: Self = postcard::from_bytes(bytes)?;
        ticket.verify().map_err(Error::Verify)?;
        Ok(ticket)
    }

    /// Verify this ticket.
    fn verify(&self) -> Result<(), &'static str> {
        Ok(())
    }

    /// Serialize to string.
    fn serialize(&self) -> String {
        let mut out = Self::KIND.to_string();
        out.push(PREFIX_SEPARATOR);
        let bytes = self.to_bytes();
        data_encoding::BASE32_NOPAD.encode_append(&bytes, &mut out);
        out.make_ascii_lowercase();
        out
    }

    /// Deserialize from a string.
    fn deserialize(str: &str) -> Result<Self, Error> {
        let expected = Self::KIND;
        let (found, bytes) = Kind::parse_prefix(str).ok_or(Error::MissingKind { expected })??;
        if expected != found {
            return Err(Error::WrongKind { expected, found });
        }
        let bytes = bytes.to_ascii_uppercase();
        let bytes = data_encoding::BASE32_NOPAD.decode(bytes.as_bytes())?;
        let ticket = Self::from_bytes(&bytes)?;
        Ok(ticket)
    }
}
