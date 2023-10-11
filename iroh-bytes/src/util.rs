//! Utility functions and types.
use anyhow::Result;
use bao_tree::blake3;
use bytes::Bytes;
use derive_more::{Debug, Display, From, Into};
use postcard::experimental::max_size::MaxSize;
use serde::{
    de::{self, SeqAccess},
    ser::SerializeTuple,
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::{borrow::Borrow, fmt, result, str::FromStr, time::SystemTime};
use thiserror::Error;
pub mod io;
pub mod progress;
pub mod runtime;

/// A format identifier
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default, Debug)]
pub enum BlobFormat {
    /// Raw blob
    #[default]
    Raw,
    /// A sequence of BLAKE3 hashes
    HashSeq,
}

impl From<BlobFormat> for u64 {
    fn from(value: BlobFormat) -> Self {
        match value {
            BlobFormat::Raw => 0,
            BlobFormat::HashSeq => 1,
        }
    }
}

impl BlobFormat {
    /// Is raw format
    pub const fn is_raw(&self) -> bool {
        matches!(self, BlobFormat::Raw)
    }

    /// Is hash seq format
    pub const fn is_hash_seq(&self) -> bool {
        matches!(self, BlobFormat::HashSeq)
    }
}

/// A tag
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, From, Into)]
pub struct Tag(pub Bytes);

impl Borrow<[u8]> for Tag {
    fn borrow(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<String> for Tag {
    fn from(value: String) -> Self {
        Self(Bytes::from(value))
    }
}

impl From<&str> for Tag {
    fn from(value: &str) -> Self {
        Self(Bytes::from(value.to_owned()))
    }
}

impl Display for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.0.as_ref();
        match std::str::from_utf8(bytes) {
            Ok(s) => write!(f, "\"{}\"", s),
            Err(_) => write!(f, "{}", hex::encode(bytes)),
        }
    }
}

impl Debug for Tag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Tag").field(&DD(self)).finish()
    }
}

impl Tag {
    /// Create a new tag that does not exist yet.
    pub fn auto(time: SystemTime, exists: impl Fn(&[u8]) -> bool) -> Self {
        let now = chrono::DateTime::<chrono::Utc>::from(time);
        let mut i = 0;
        loop {
            let mut text = format!("auto-{}", now.format("%Y-%m-%dT%H:%M:%S%.3fZ"));
            if i != 0 {
                text.push_str(&format!("-{}", i));
            }
            if !exists(text.as_bytes()) {
                return Self::from(text);
            }
            i += 1;
        }
    }
}

/// Option for commands that allow setting a tag
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SetTagOption {
    /// A tag will be automatically generated
    Auto,
    /// The tag is explicitly named
    Named(Tag),
}

/// A hash and format pair
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct HashAndFormat {
    /// The hash
    pub hash: Hash,
    /// The format
    pub format: BlobFormat,
}

impl HashAndFormat {
    /// Create a new hash and format pair, using the default (raw) format.
    pub fn raw(hash: Hash) -> Self {
        Self {
            hash,
            format: BlobFormat::Raw,
        }
    }

    /// Create a new hash and format pair, using the collection format.
    pub fn hash_seq(hash: Hash) -> Self {
        Self {
            hash,
            format: BlobFormat::HashSeq,
        }
    }
}

/// Hash type used throughout.
#[derive(PartialEq, Eq, Copy, Clone, Hash)]
pub struct Hash(blake3::Hash);

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Hash").field(&DD(self.to_hex())).finish()
    }
}

struct DD<T: fmt::Display>(T);

impl<T: fmt::Display> fmt::Debug for DD<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl Hash {
    /// The hash for the empty byte range (`b""`).
    pub const EMPTY: Hash = Hash::from_bytes([
        175, 19, 73, 185, 245, 249, 161, 166, 160, 64, 77, 234, 54, 220, 201, 73, 155, 203, 37,
        201, 173, 193, 18, 183, 204, 154, 147, 202, 228, 31, 50, 98,
    ]);

    /// Calculate the hash of the provide bytes.
    pub fn new(buf: impl AsRef<[u8]>) -> Self {
        let val = blake3::hash(buf.as_ref());
        Hash(val)
    }

    /// Bytes of the hash.
    pub fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }

    /// Create a `Hash` from its raw bytes representation.
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(blake3::Hash::from_bytes(bytes))
    }

    /// Get the cid as bytes.
    pub fn as_cid_bytes(&self) -> [u8; 36] {
        let hash = self.0.as_bytes();
        let mut res = [0u8; 36];
        res[0..4].copy_from_slice(&CID_PREFIX);
        res[4..36].copy_from_slice(hash);
        res
    }

    /// Try to create a blake3 cid from cid bytes.
    ///
    /// This will only work if the prefix is the following:
    /// - version 1
    /// - raw codec
    /// - blake3 hash function
    /// - 32 byte hash size
    pub fn from_cid_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(
            bytes.len() == 36,
            "invalid cid length, expected 36, got {}",
            bytes.len()
        );
        anyhow::ensure!(bytes[0..4] == CID_PREFIX, "invalid cid prefix");
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&bytes[4..36]);
        Ok(Self::from(hash))
    }

    /// Convert the hash to a hex string.
    pub fn to_hex(&self) -> String {
        self.0.to_hex().to_string()
    }
}

impl AsRef<[u8]> for Hash {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl From<Hash> for blake3::Hash {
    fn from(value: Hash) -> Self {
        value.0
    }
}

impl From<blake3::Hash> for Hash {
    fn from(value: blake3::Hash) -> Self {
        Hash(value)
    }
}

impl From<[u8; 32]> for Hash {
    fn from(value: [u8; 32]) -> Self {
        Hash(blake3::Hash::from(value))
    }
}

impl From<Hash> for [u8; 32] {
    fn from(value: Hash) -> Self {
        *value.as_bytes()
    }
}

impl From<&[u8; 32]> for Hash {
    fn from(value: &[u8; 32]) -> Self {
        Hash(blake3::Hash::from(*value))
    }
}

impl PartialOrd for Hash {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.0.as_bytes().cmp(other.0.as_bytes()))
    }
}

impl Ord for Hash {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.as_bytes().cmp(other.0.as_bytes())
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // result will be 58 bytes plus prefix
        let mut res = [b'b'; 59];
        // write the encoded bytes
        data_encoding::BASE32_NOPAD.encode_mut(&self.as_cid_bytes(), &mut res[1..]);
        // convert to string, this is guaranteed to succeed
        let t = std::str::from_utf8_mut(res.as_mut()).unwrap();
        // hack since data_encoding doesn't have BASE32LOWER_NOPAD as a const
        t.make_ascii_lowercase();
        // write the str, no allocations
        f.write_str(t)
    }
}

impl FromStr for Hash {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let sb = s.as_bytes();
        if sb.len() == 64 {
            // this is most likely a hex encoded cid
            // try to decode it as hex
            let mut bytes = [0u8; 32];
            if hex::decode_to_slice(sb, &mut bytes).is_ok() {
                return Ok(Self::from(bytes));
            }
        }
        if sb.len() == 59 && sb[0] == b'b' {
            // this is a base32 encoded cid, we can decode it directly
            let mut t = [0u8; 58];
            t.copy_from_slice(&sb[1..]);
            // hack since data_encoding doesn't have BASE32LOWER_NOPAD as a const
            std::str::from_utf8_mut(t.as_mut())
                .unwrap()
                .make_ascii_uppercase();
            // decode the bytes
            let mut res = [0u8; 36];
            data_encoding::BASE32_NOPAD
                .decode_mut(&t, &mut res)
                .map_err(|_e| anyhow::anyhow!("invalid base32"))?;
            // convert to cid, this will check the prefix
            Self::from_cid_bytes(&res)
        } else {
            // if we want to support all the weird multibase prefixes, we have no choice
            // but to use the multibase crate
            let (_base, bytes) = multibase::decode(s)?;
            Self::from_cid_bytes(bytes.as_ref())
        }
    }
}

impl Serialize for Hash {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Fixed-length structures, including arrays, are supported in Serde as tuples
        // See: https://serde.rs/impl-serialize.html#serializing-a-tuple
        let mut s = serializer.serialize_tuple(32)?;
        for item in self.0.as_bytes() {
            s.serialize_element(item)?;
        }
        s.end()
    }
}

impl<'de> Deserialize<'de> for Hash {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_tuple(32, HashVisitor)
    }
}

struct HashVisitor;

impl<'de> de::Visitor<'de> for HashVisitor {
    type Value = Hash;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "an array of 32 bytes containing hash data")
    }

    /// Process a sequence into an array
    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut arr = [0u8; 32];
        let mut i = 0;
        while let Some(val) = seq.next_element()? {
            arr[i] = val;
            i += 1;
            if i > 32 {
                return Err(de::Error::invalid_length(i, &self));
            }
        }

        Ok(Hash::from(arr))
    }
}

impl MaxSize for Hash {
    const POSTCARD_MAX_SIZE: usize = 32;
}

const CID_PREFIX: [u8; 4] = [
    0x01, // version
    0x55, // raw codec
    0x1e, // hash function, blake3
    0x20, // hash size, 32 bytes
];

/// A serializable error type for use in RPC responses.
#[derive(Serialize, Deserialize, Debug, Error)]
pub struct RpcError(serde_error::Error);

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl From<anyhow::Error> for RpcError {
    fn from(e: anyhow::Error) -> Self {
        RpcError(serde_error::Error::new(&*e))
    }
}

impl From<std::io::Error> for RpcError {
    fn from(e: std::io::Error) -> Self {
        RpcError(serde_error::Error::new(&e))
    }
}

/// A serializable result type for use in RPC responses.
#[allow(dead_code)]
pub type RpcResult<T> = result::Result<T, RpcError>;

/// A non-sendable marker type
#[derive(Debug)]
pub(crate) struct NonSend {
    _marker: std::marker::PhantomData<std::rc::Rc<()>>,
}

impl NonSend {
    /// Create a new non-sendable marker.
    #[allow(dead_code)]
    pub const fn new() -> Self {
        Self {
            _marker: std::marker::PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use iroh_test::{assert_eq_hex, hexdump::parse_hexdump};

    use super::*;

    use serde_test::{assert_tokens, Token};

    #[test]
    fn test_display_parse_roundtrip() {
        for i in 0..100 {
            let hash: Hash = blake3::hash(&[i]).into();
            let text = hash.to_string();
            let hash1 = text.parse::<Hash>().unwrap();
            assert_eq!(hash, hash1);

            let text = hash.to_hex();
            let hash1 = Hash::from_str(&text).unwrap();
            assert_eq!(hash, hash1);
        }
    }

    #[test]
    fn test_hash() {
        let data = b"hello world";
        let hash = Hash::new(data);

        let encoded = hash.to_string();
        assert_eq!(encoded.parse::<Hash>().unwrap(), hash);
    }

    #[test]
    fn test_empty_hash() {
        let hash = Hash::new(b"");
        assert_eq!(hash, Hash::EMPTY);
    }

    #[test]
    fn hash_wire_format() {
        let hash = Hash::from([0xab; 32]);
        let serialized = postcard::to_stdvec(&hash).unwrap();
        let expected = parse_hexdump(r"
            ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab ab # hash
        ").unwrap();
        assert_eq_hex!(serialized, expected);
    }

    #[test]
    fn test_hash_serde() {
        let hash = Hash::new("hello");

        // Hashes are serialized as 32 tuples
        let mut tokens = Vec::new();
        tokens.push(Token::Tuple { len: 32 });
        for byte in hash.as_bytes() {
            tokens.push(Token::U8(*byte));
        }
        tokens.push(Token::TupleEnd);
        assert_eq!(tokens.len(), 34);

        assert_tokens(&hash, &tokens);
    }

    #[test]
    fn test_hash_postcard() {
        let hash = Hash::new("hello");
        let ser = postcard::to_stdvec(&hash).unwrap();
        let de = postcard::from_bytes(&ser).unwrap();
        assert_eq!(hash, de);

        assert_eq!(ser.len(), 32);
    }
}
