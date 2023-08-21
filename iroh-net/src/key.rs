//! Cryptographic key handling for `iroh-net`.

mod encryption;

use std::{
    fmt::{Debug, Display},
    str::FromStr,
};

pub use ed25519_dalek::{Signature, PUBLIC_KEY_LENGTH};
use ed25519_dalek::{SignatureError, SigningKey, VerifyingKey};
use once_cell::sync::OnceCell;
use serde::{Deserialize, Serialize};
use ssh_key::LineEnding;

pub use self::encryption::SharedSecret;
use self::encryption::{public_ed_box, secret_ed_box};

/// A public key.
#[derive(Clone, Copy, Hash, PartialEq, Eq)]
pub struct PublicKey {
    public: VerifyingKey,
    /// Cached version of `crypto_box::PublicKey` matching `public`.
    /// Stored as raw array, as `crypto_box::PublicKey` is not `Copy`.
    public_crypto_box: [u8; 32],
}

impl Serialize for PublicKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.public.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for PublicKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let public = VerifyingKey::deserialize(deserializer)?;
        Ok(public.into())
    }
}

impl PublicKey {
    /// Get this public key as a byte array.
    pub fn as_bytes(&self) -> &[u8; 32] {
        self.public.as_bytes()
    }

    fn public_crypto_box(&self) -> crypto_box::PublicKey {
        crypto_box::PublicKey::from_bytes(self.public_crypto_box)
    }

    /// Verify a signature on a message with this secret key's public key.
    ///
    /// # Return
    ///
    /// Returns `Ok(())` if the signature is valid, and `Err` otherwise.
    pub fn verify(&self, message: &[u8], signature: &Signature) -> Result<(), SignatureError> {
        self.public.verify_strict(message, signature)
    }
}

impl TryFrom<&[u8]> for PublicKey {
    type Error = SignatureError;

    #[inline]
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let public = VerifyingKey::try_from(bytes)?;
        Ok(public.into())
    }
}

impl AsRef<[u8]> for PublicKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<VerifyingKey> for PublicKey {
    fn from(public: VerifyingKey) -> Self {
        let public_crypto_box = public_ed_box(&public).to_bytes();
        PublicKey {
            public,
            public_crypto_box,
        }
    }
}

impl Debug for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut text = data_encoding::BASE32_NOPAD.encode(self.as_bytes());
        text.make_ascii_lowercase();
        write!(f, "PublicKey({text})")
    }
}

impl Display for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut text = data_encoding::BASE32_NOPAD.encode(self.as_bytes());
        text.make_ascii_lowercase();
        write!(f, "{text}")
    }
}

/// A secret key.
#[derive(Clone)]
pub struct SecretKey {
    secret: SigningKey,
    secret_crypto_box: OnceCell<crypto_box::SecretKey>,
}

impl Debug for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut text = data_encoding::BASE32_NOPAD.encode(&self.to_bytes());
        text.make_ascii_lowercase();
        write!(f, "SecretKey({text})")
    }
}

impl Display for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut text = data_encoding::BASE32_NOPAD.encode(&self.to_bytes());
        text.make_ascii_lowercase();
        write!(f, "{text}")
    }
}

impl Serialize for SecretKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.secret.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SecretKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let secret = SigningKey::deserialize(deserializer)?;
        Ok(secret.into())
    }
}

impl SecretKey {
    /// The public key of this [`SecretKey`].
    pub fn public(&self) -> PublicKey {
        self.secret.verifying_key().into()
    }

    /// Generate a new [`SecretKey`].
    pub fn generate() -> Self {
        let mut rng = rand::rngs::OsRng;
        let secret = SigningKey::generate(&mut rng);

        Self {
            secret,
            secret_crypto_box: OnceCell::default(),
        }
    }

    /// Serialise this key to OpenSSH format.
    pub fn to_openssh(&self) -> ssh_key::Result<zeroize::Zeroizing<String>> {
        let ckey = ssh_key::private::Ed25519Keypair {
            public: self.secret.verifying_key().into(),
            private: self.secret.clone().into(),
        };
        ssh_key::private::PrivateKey::from(ckey).to_openssh(LineEnding::default())
    }

    /// Deserialise this key from OpenSSH format.
    pub fn try_from_openssh<T: AsRef<[u8]>>(data: T) -> anyhow::Result<Self> {
        let ser_key = ssh_key::private::PrivateKey::from_openssh(data)?;
        match ser_key.key_data() {
            ssh_key::private::KeypairData::Ed25519(kp) => Ok(SecretKey {
                secret: kp.private.clone().into(),
                secret_crypto_box: OnceCell::default(),
            }),
            _ => anyhow::bail!("invalid key format"),
        }
    }

    /// Sign the given message and return a digital signature
    pub fn sign(&self, msg: &[u8]) -> Signature {
        use ed25519_dalek::Signer;

        self.secret.sign(msg)
    }

    /// Convert this to the bytes representing the secret part.
    /// The public part can always be recovered.
    pub fn to_bytes(&self) -> [u8; 32] {
        self.secret.to_bytes()
    }

    fn secret_crypto_box(&self) -> &crypto_box::SecretKey {
        self.secret_crypto_box
            .get_or_init(|| secret_ed_box(&self.secret))
    }
}

impl From<SigningKey> for SecretKey {
    fn from(secret: SigningKey) -> Self {
        SecretKey {
            secret,
            secret_crypto_box: OnceCell::default(),
        }
    }
}

impl From<[u8; 32]> for SecretKey {
    fn from(value: [u8; 32]) -> Self {
        let secret = SigningKey::from(value);
        secret.into()
    }
}

impl TryFrom<&[u8]> for SecretKey {
    type Error = SignatureError;

    #[inline]
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let secret = SigningKey::try_from(bytes)?;
        Ok(secret.into())
    }
}

// TODO: probably needs a version field
/// An identifier for networked peers.
///
/// Each network node has a cryptographic identifier which can be used to make sure you are
/// connecting to the right peer.
///
/// # `Display` and `FromStr`
///
/// The [`PeerId`] implements both `Display` and `FromStr` which can be used to
/// (de)serialise to human-readable and relatively safely transferrable strings.
#[derive(Clone, PartialEq, Eq, Copy, Serialize, Deserialize, Hash)]
pub struct PeerId(PublicKey);

impl PeerId {
    /// Get this peer id as a byte array.
    pub fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_bytes()
    }
}

impl TryFrom<&[u8]> for PeerId {
    type Error = SignatureError;

    #[inline]
    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        let public = PublicKey::try_from(bytes)?;
        Ok(public.into())
    }
}

impl From<PublicKey> for PeerId {
    fn from(key: PublicKey) -> Self {
        PeerId(key)
    }
}

impl From<PeerId> for PublicKey {
    fn from(key: PeerId) -> Self {
        key.0
    }
}

impl Debug for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut text = data_encoding::BASE32_NOPAD.encode(self.0.as_bytes());
        text.make_ascii_lowercase();
        write!(f, "PeerId({text})")
    }
}

/// Serialises the [`PeerId`] to base32.
///
/// [`FromStr`] is capable of deserialising this format.
impl Display for PeerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut text = data_encoding::BASE32_NOPAD.encode(self.0.as_bytes());
        text.make_ascii_lowercase();
        write!(f, "{text}")
    }
}

/// Error when deserialising a [`PeerId`].
#[derive(thiserror::Error, Debug)]
pub enum PeerIdError {
    /// Error when decoding the base32.
    #[error("decoding: {0}")]
    Base32(#[from] data_encoding::DecodeError),
    /// Error when decoding the public key.
    #[error("key: {0}")]
    Key(#[from] ed25519_dalek::SignatureError),
    /// Invalid length of the id.
    #[error("decoding size")]
    DecodingSize,
}

/// Deserialises the [`PeerId`] from it's base32 encoding.
///
/// [`Display`] is capable of serialising this format.
impl FromStr for PeerId {
    type Err = PeerIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bytes = data_encoding::BASE32_NOPAD.decode(s.to_ascii_uppercase().as_bytes())?;
        let key = PublicKey::try_from(&bytes[..])?;
        Ok(PeerId(key))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_secret_key_openssh_roundtrip() {
        let kp = SecretKey::generate();
        let ser = kp.to_openssh().unwrap();
        let de = SecretKey::try_from_openssh(&ser).unwrap();
        assert_eq!(kp.to_bytes(), de.to_bytes());
    }
}
