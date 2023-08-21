//! The private and public keys of a node.

use std::fmt::Display;
use std::{fmt::Debug, hash::Hash};

use anyhow::{anyhow, ensure, Context, Result};
use serde::{Deserialize, Serialize};

pub use crypto_box::KEY_SIZE;

pub(crate) const PUBLIC_KEY_LENGTH: usize = KEY_SIZE;
pub(crate) const SECRET_KEY_LENGTH: usize = KEY_SIZE;
pub(crate) const NONCE_LEN: usize = 24;

/// Public key of a node.
#[derive(Clone, Eq)]
pub struct PublicKey(crypto_box::PublicKey);

impl From<&crate::tls::PublicKey> for PublicKey {
    fn from(value: &crate::tls::PublicKey) -> Self {
        let key: ed25519_dalek::VerifyingKey =(*value).into();
        PublicKey(crypto_box::PublicKey::from(key.to_montgomery()))
    }
}

impl Debug for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PublicKey({})", hex::encode(self.0.as_bytes()))
    }
}

impl PublicKey {
    /// The number of hex characters to show in [`PublicKey::short_hex`].
    const SHORT_HEX_LENGTH: usize = 8;

    /// Return a short hex-formatted string of this key.
    ///
    /// This is useful for displaying in logs etc.
    pub fn short_hex(&self) -> String {
        let bytes = &self.0.as_bytes()[..Self::SHORT_HEX_LENGTH];
        hex::encode(bytes)
    }
}

/// Uses the [`PublicKey::short_hex`] to represent the key.
impl Display for PublicKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PublicKey({}..)", self.short_hex())
    }
}

impl AsRef<[u8]> for PublicKey {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl Hash for PublicKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.as_bytes().hash(state)
    }
}

impl std::cmp::PartialEq for PublicKey {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl From<crypto_box::PublicKey> for PublicKey {
    fn from(key: crypto_box::PublicKey) -> Self {
        Self(key)
    }
}

impl From<[u8; PUBLIC_KEY_LENGTH]> for PublicKey {
    fn from(value: [u8; PUBLIC_KEY_LENGTH]) -> Self {
        Self(crypto_box::PublicKey::from(value))
    }
}

impl TryFrom<&[u8]> for PublicKey {
    type Error = anyhow::Error;
    fn try_from(value: &[u8]) -> std::result::Result<Self, Self::Error> {
        let value =
            <[u8; PUBLIC_KEY_LENGTH]>::try_from(value).context("TryFrom slice to PublicKey")?;
        Ok(PublicKey::from(value))
    }
}

impl PublicKey {
    /// Borrow the public key as bytes.
    pub fn as_bytes(&self) -> &[u8; PUBLIC_KEY_LENGTH] {
        self.0.as_bytes()
    }

    /// Whether the public key is zero.
    pub fn is_zero(&self) -> bool {
        self.0.as_bytes() == &[0u8; PUBLIC_KEY_LENGTH]
    }
}

/// The private key of a node.
#[derive(Clone)]
pub struct SecretKey(crypto_box::SecretKey);

impl From<&crate::tls::SecretKey> for SecretKey {
    fn from(key: &crate::tls::SecretKey) -> Self {
        SecretKey(crypto_box::SecretKey::from(key.to_scalar()))
    }
}

impl Serialize for SecretKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::ser::Serializer,
    {
        serdect::array::serialize_hex_upper_or_bin(&self.0.to_bytes(), serializer)
    }
}

impl<'de> Deserialize<'de> for SecretKey {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        let mut bytes = [0u8; KEY_SIZE];
        serdect::array::deserialize_hex_or_bin(&mut bytes, deserializer)?;
        Ok(SecretKey::from(bytes))
    }
}

impl SecretKey {
    /// Generate a random [SecretKey].
    pub fn generate() -> Self {
        Self(crypto_box::SecretKey::generate(&mut rand::rngs::OsRng))
    }

    /// Get the [PublicKey] that corresponds to this [SecretKey].
    pub fn public_key(&self) -> PublicKey {
        self.0.public_key().into()
    }

    /// Serialize the [SecretKey] to bytes.
    pub fn to_bytes(&self) -> [u8; 32] {
        self.0.to_bytes()
    }

    /// Returns the shared key for communication between this key and `other`.
    pub fn shared(&self, other: &PublicKey) -> SharedSecret {
        let boxx = self.shared_secret(other);
        SharedSecret(boxx)
    }

    fn shared_secret(&self, other: &PublicKey) -> crypto_box::ChaChaBox {
        crypto_box::ChaChaBox::new(&other.0, &self.0)
    }

    /// Creates a shared secret between the [SecretKey] and the given [PublicKey], and seals the
    /// provided cleartext.
    pub fn seal_to(&self, other: &PublicKey, cleartext: &[u8]) -> Vec<u8> {
        use crypto_box::aead::{Aead, AeadCore, OsRng};

        let shared_secret = self.shared_secret(other);
        let nonce = crypto_box::ChaChaBox::generate_nonce(&mut OsRng);
        let ciphertext = shared_secret
            .encrypt(&nonce, cleartext)
            .expect("encryption failed");

        let mut res = nonce.to_vec();
        res.extend(ciphertext);
        res
    }

    /// Creates a shared secret between the [SecretKey] and the given [PublicKey], and opens the
    /// `seal`, returning the cleartext.
    pub fn open_from(&self, other: &PublicKey, seal: &[u8]) -> Result<Vec<u8>> {
        let shared_secret = self.shared_secret(other);

        use crypto_box::aead::Aead;
        ensure!(seal.len() > NONCE_LEN, "too short");

        let (nonce, ciphertext) = seal.split_at(NONCE_LEN);
        let nonce: [u8; NONCE_LEN] = nonce.try_into().unwrap();
        let cleartext = shared_secret
            .decrypt(&nonce.into(), ciphertext)
            .map_err(|e| anyhow!("decryption failed: {:?}", e))?;

        Ok(cleartext)
    }
}

impl Debug for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecretKey({})", hex::encode(self.0.to_bytes()))
    }
}

impl Hash for SecretKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_bytes().hash(state)
    }
}

impl From<crypto_box::SecretKey> for SecretKey {
    fn from(key: crypto_box::SecretKey) -> Self {
        Self(key)
    }
}

impl From<[u8; SECRET_KEY_LENGTH]> for SecretKey {
    fn from(value: [u8; SECRET_KEY_LENGTH]) -> Self {
        Self(crypto_box::SecretKey::from(value))
    }
}

/// Shared Secret.
pub struct SharedSecret(crypto_box::ChaChaBox);

impl Debug for SharedSecret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SharedSecret(crypto_box::ChaChaBox)")
    }
}

impl SharedSecret {
    /// Seals the provided cleartext.
    pub fn seal(&self, cleartext: &[u8]) -> Vec<u8> {
        use crypto_box::aead::{Aead, AeadCore, OsRng};

        let nonce = crypto_box::ChaChaBox::generate_nonce(&mut OsRng);
        let ciphertext = self
            .0
            .encrypt(&nonce, cleartext)
            .expect("encryption failed");

        let mut res = nonce.to_vec();
        res.extend(ciphertext);
        res
    }

    /// Opens the ciphertext, which must have been created using `Self::seal`, and returns the cleartext.
    pub fn open(&self, seal: &[u8]) -> Result<Vec<u8>> {
        use crypto_box::aead::Aead;
        ensure!(seal.len() > NONCE_LEN, "too short");

        let (nonce, ciphertext) = seal.split_at(NONCE_LEN);
        let nonce: [u8; NONCE_LEN] = nonce.try_into().unwrap();
        let plaintext = self
            .0
            .decrypt(&nonce.into(), ciphertext)
            .map_err(|e| anyhow!("decryption failed: {:?}", e))?;

        Ok(plaintext)
    }
}

/// Extension trait for dealing with encryption.
pub trait EncryptExt {
    /// Creates a shared secret between [Self] and the given [crate::tls::PublicKey], and seals the
    /// provided cleartext.
    fn seal_to(&self, other: &crate::tls::PublicKey, cleartext: &[u8]) -> Vec<u8>;
    /// Creates a shared secret between [Self] and the given [crate::tls::PublicKey], and opens the
    /// `seal`, returning the cleartext.
    fn open_from(&self, other: &crate::tls::PublicKey, seal: &[u8]) -> Result<Vec<u8>>;
    /// Returns the shared key for communication between this key and `other`.
    fn shared(&self, other: &crate::tls::PublicKey) -> SharedSecret;
}


impl EncryptExt for crate::tls::Keypair {
    fn seal_to(&self, other: &crate::tls::PublicKey, cleartext: &[u8]) -> Vec<u8> {
        let secret_key: SecretKey = self.secret().into();
        let public_key: PublicKey = other.into();
        secret_key.seal_to(&public_key, cleartext)
    }

    fn open_from(&self, other: &crate::tls::PublicKey, seal: &[u8]) -> Result<Vec<u8>> {
        let secret_key: SecretKey = self.secret().into();
        let public_key: PublicKey = other.into();
        secret_key.open_from(&public_key, seal)
    }

    fn shared(&self, other: &crate::tls::PublicKey) -> SharedSecret {
        let secret_key: SecretKey = self.secret().into();
        let public_key: PublicKey = other.into();
        secret_key.shared(&public_key)        
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_seal_open_roundtrip_regular() {
        let key_a = SecretKey::generate();
        let key_b = SecretKey::generate();

        seal_open_roundtrip(&key_a, &key_b);
        seal_open_roundtrip(&key_b, &key_a);
    }

    #[test]
    fn test_seal_open_roundtrip_converted() {
        let key_a = crate::tls::Keypair::generate();
        let key_b = crate::tls::Keypair::generate();
        let key_a: SecretKey = key_a.secret().into();
        let key_b: SecretKey = key_b.secret().into();

        seal_open_roundtrip(&key_a, &key_b);
        seal_open_roundtrip(&key_b, &key_a);
        seal_open_roundtrip(&key_a, &key_a);
    }

    fn seal_open_roundtrip(key_a: &SecretKey, key_b: &SecretKey) {
        let msg = b"super secret message!!!!";
        let sealed_message = key_a.seal_to(&key_b.public_key(), msg);
        let decrypted_message = key_b
            .open_from(&key_a.public_key(), &sealed_message)
            .unwrap();
        assert_eq!(&msg[..], &decrypted_message);

        let shared_a = key_a.shared(&key_b.public_key());
        let sealed_message = shared_a.seal(msg);
        let shared_b = key_b.shared(&key_a.public_key());
        let decrypted_message = shared_b
            .open(&sealed_message)
            .unwrap();
        assert_eq!(&msg[..], &decrypted_message);

    }

    #[test]
    fn test_roundtrip_public_key() {
        let key = SecretKey::generate();
        let public_bytes = *key.public_key().as_bytes();
        let public_key_back = PublicKey::from(public_bytes);
        assert_eq!(key.public_key(), public_key_back);
    }

    #[test]
    fn test_same_public_key_api() {
        let key = crate::tls::Keypair::generate();
        let public_key1: PublicKey = (&key.public()).into();
        let public_key2: PublicKey = SecretKey::from(key.secret()).public_key();

        assert_eq!(public_key1, public_key2);
    }

    #[test]
    fn test_same_public_key_low_level() {
        let mut rng = rand::thread_rng();
        let key = ed25519_dalek::SigningKey::generate(&mut rng);
        let public_key1 = {
            let m = key.verifying_key().to_montgomery();
            crypto_box::PublicKey::from(m)
        };

        let public_key2 = {
            let s = key.to_scalar();
            let cs = crypto_box::SecretKey::from(s);
            cs.public_key()
        };

        assert_eq!(public_key1, public_key2);
    }
}
