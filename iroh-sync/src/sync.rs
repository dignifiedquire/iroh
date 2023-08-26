//! API for iroh-sync replicas

// Names and concepts are roughly based on Willows design at the moment:
//
// https://hackmd.io/DTtck8QOQm6tZaQBBtTf7w
//
// This is going to change!

use std::{fmt::Debug, sync::Arc, time::SystemTime};

#[cfg(feature = "metrics")]
use crate::metrics::Metrics;
#[cfg(feature = "metrics")]
use iroh_metrics::{inc, inc_by};

use parking_lot::{Mutex, RwLock};

use ed25519_dalek::{Signature, SignatureError};
use iroh_bytes::Hash;
use serde::{Deserialize, Serialize};

use crate::ranger::{self, AsFingerprint, Fingerprint, Peer, RangeKey};

pub use crate::keys::*;

/// Protocol message for the set reconciliation protocol.
///
/// Can be serialized to bytes with [serde] to transfer between peers.
pub type ProtocolMessage = crate::ranger::Message<RecordIdentifier, SignedEntry>;

/// Byte represenation of a `PeerId` from `iroh-net`.
// TODO: PeerId is in iroh-net which iroh-sync doesn't depend on. Add iroh-common crate with `PeerId`.
pub type PeerIdBytes = [u8; 32];

/// Whether an entry was inserted locally or by a remote peer.
#[derive(Debug, Clone)]
pub enum InsertOrigin {
    /// The entry was inserted locally.
    Local,
    /// The entry was received from the remote peer identified by [`PeerIdBytes`].
    Sync(PeerIdBytes),
}

/// Local representation of a mutable, synchronizable key-value store.
#[derive(derive_more::Debug, Clone)]
pub struct Replica<S: ranger::Store<RecordIdentifier, SignedEntry>> {
    inner: Arc<RwLock<InnerReplica<S>>>,
    on_insert_sender: flume::Sender<(InsertOrigin, SignedEntry)>,
    #[allow(clippy::type_complexity)]
    on_insert_receiver: Arc<Mutex<Option<flume::Receiver<(InsertOrigin, SignedEntry)>>>>,
}

#[derive(derive_more::Debug)]
struct InnerReplica<S: ranger::Store<RecordIdentifier, SignedEntry>> {
    namespace: Namespace,
    peer: Peer<RecordIdentifier, SignedEntry, S>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ReplicaData {
    entries: Vec<SignedEntry>,
    namespace: Namespace,
}

impl<S: ranger::Store<RecordIdentifier, SignedEntry>> Replica<S> {
    /// Create a new replica.
    // TODO: make read only replicas possible
    pub fn new(namespace: Namespace, store: S) -> Self {
        let (s, r) = flume::bounded(16); // TODO: should this be configurable?
        Replica {
            inner: Arc::new(RwLock::new(InnerReplica {
                namespace,
                peer: Peer::from_store(store),
            })),
            on_insert_sender: s,
            on_insert_receiver: Arc::new(Mutex::new(Some(r))),
        }
    }

    /// Subscribe to insert events.
    ///
    /// Only one subscription can be active at a time. If a previous subscription was created, this
    /// will return `None`.
    // TODO: Allow to clear a previous subscription?
    pub fn subscribe(&self) -> Option<flume::Receiver<(InsertOrigin, SignedEntry)>> {
        self.on_insert_receiver.lock().take()
    }

    /// Insert a new record at the given key.
    ///
    /// The entry will by signed by the provided `author`.
    /// The `len` must be the byte length of the data identified by `hash`.
    pub fn insert(
        &self,
        key: impl AsRef<[u8]>,
        author: &Author,
        hash: Hash,
        len: u64,
    ) -> Result<(), S::Error> {
        let mut inner = self.inner.write();

        let id = RecordIdentifier::new(key, inner.namespace.id(), author.id());
        let record = Record::from_hash(hash, len);

        // Store signed entries
        let entry = Entry::new(id.clone(), record);
        let signed_entry = entry.sign(&inner.namespace, author);
        inner.peer.put(id, signed_entry.clone())?;
        drop(inner);

        self.on_insert_sender
            .send((InsertOrigin::Local, signed_entry))
            .ok();

        #[cfg(feature = "metrics")]
        {
            inc!(Metrics, new_entries_local);
            inc_by!(Metrics, new_entries_local_size, len);
        }

        Ok(())
    }

    /// Hashes the given data and inserts it.
    ///
    /// This does not store the content, just the record of it.
    /// Returns the calculated hash.
    pub fn hash_and_insert(
        &self,
        key: impl AsRef<[u8]>,
        author: &Author,
        data: impl AsRef<[u8]>,
    ) -> Result<Hash, S::Error> {
        let len = data.as_ref().len() as u64;
        let hash = Hash::new(data);
        self.insert(key, author, hash, len)?;
        Ok(hash)
    }

    /// Get the identifier for an entry in this replica.
    pub fn id(&self, key: impl AsRef<[u8]>, author: &Author) -> RecordIdentifier {
        let inner = self.inner.read();
        RecordIdentifier::new(key, inner.namespace.id(), author.id())
    }

    /// Insert an entry into this replica which was received from a remote peer.
    ///
    /// This will verify both the namespace and author signatures of the entry, emit an `on_insert`
    /// event, and insert the entry into the replica store.
    pub fn insert_remote_entry(
        &self,
        entry: SignedEntry,
        received_from: PeerIdBytes,
    ) -> anyhow::Result<()> {
        entry.verify()?;
        let mut inner = self.inner.write();
        let id = entry.entry.id.clone();
        inner.peer.put(id, entry.clone()).map_err(Into::into)?;
        drop(inner);
        self.on_insert_sender
            .send((InsertOrigin::Sync(received_from), entry.clone()))
            .ok();

        #[cfg(feature = "metrics")]
        {
            inc!(Metrics, new_entries_remote);
            inc_by!(Metrics, new_entries_remote_size, entry.content_len());
        }

        Ok(())
    }

    /// Create the initial message for the set reconciliation flow with a remote peer.
    pub fn sync_initial_message(
        &self,
    ) -> Result<crate::ranger::Message<RecordIdentifier, SignedEntry>, S::Error> {
        self.inner.read().peer.initial_message()
    }

    /// Process a set reconciliation message from a remote peer.
    ///
    /// Returns the next message to be sent to the peer, if any.
    pub fn sync_process_message(
        &self,
        message: crate::ranger::Message<RecordIdentifier, SignedEntry>,
        from_peer: PeerIdBytes,
    ) -> Result<Option<crate::ranger::Message<RecordIdentifier, SignedEntry>>, S::Error> {
        let reply = self
            .inner
            .write()
            .peer
            .process_message(message, |_key, entry| {
                self.on_insert_sender
                    .send((InsertOrigin::Sync(from_peer), entry))
                    .ok();
            })?;

        Ok(reply)
    }

    /// Get the namespace identifier for this [`Replica`].
    pub fn namespace(&self) -> NamespaceId {
        self.inner.read().namespace.id()
    }

    /// Get the byte represenation of the [`Namespace`] key for this replica.
    // TODO: Why return [u8; 32] and not `Namespace` here?
    pub fn secret_key(&self) -> [u8; 32] {
        self.inner.read().namespace.to_bytes()
    }
}

/// A signed entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedEntry {
    signature: EntrySignature,
    entry: Entry,
}

impl SignedEntry {
    pub(crate) fn new(signature: EntrySignature, entry: Entry) -> Self {
        SignedEntry { signature, entry }
    }

    /// Create a new signed entry by signing an entry with the `namespace` and `author`.
    pub fn from_entry(entry: Entry, namespace: &Namespace, author: &Author) -> Self {
        let signature = EntrySignature::from_entry(&entry, namespace, author);
        SignedEntry { signature, entry }
    }

    /// Verify the signatures on this entry.
    pub fn verify(&self) -> Result<(), SignatureError> {
        self.signature
            .verify(&self.entry, &self.entry.id.namespace, &self.entry.id.author)
    }

    /// Get the signature.
    pub fn signature(&self) -> &EntrySignature {
        &self.signature
    }

    /// Get the [`Entry`].
    pub fn entry(&self) -> &Entry {
        &self.entry
    }

    /// Get the content [`struct@Hash`] of the entry.
    pub fn content_hash(&self) -> &Hash {
        self.entry().record().content_hash()
    }

    /// Get the content length of the entry.
    pub fn content_len(&self) -> u64 {
        self.entry().record().content_len()
    }

    /// Get the [`AuthorId`] of the entry.
    pub fn author(&self) -> AuthorId {
        self.entry().id().author()
    }

    /// Get the key of the entry.
    pub fn key(&self) -> &[u8] {
        self.entry().id().key()
    }
}

/// Signature over an entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntrySignature {
    author_signature: Signature,
    namespace_signature: Signature,
}

impl EntrySignature {
    /// Create a new signature by signing an entry with the `namespace` and `author`.
    pub fn from_entry(entry: &Entry, namespace: &Namespace, author: &Author) -> Self {
        // TODO: this should probably include a namespace prefix
        // namespace in the cryptographic sense.
        let bytes = entry.to_vec();
        let namespace_signature = namespace.sign(&bytes);
        let author_signature = author.sign(&bytes);

        EntrySignature {
            author_signature,
            namespace_signature,
        }
    }

    /// Verify that this signature was created by signing the `entry` with the
    /// secret keys of the specified `author` and `namespace`.
    pub fn verify(
        &self,
        entry: &Entry,
        namespace: &NamespaceId,
        author: &AuthorId,
    ) -> Result<(), SignatureError> {
        let bytes = entry.to_vec();
        namespace.verify(&bytes, &self.namespace_signature)?;
        author.verify(&bytes, &self.author_signature)?;

        Ok(())
    }

    pub(crate) fn from_parts(namespace_sig: &[u8; 64], author_sig: &[u8; 64]) -> Self {
        let namespace_signature = Signature::from_bytes(namespace_sig);
        let author_signature = Signature::from_bytes(author_sig);

        EntrySignature {
            author_signature,
            namespace_signature,
        }
    }

    pub(crate) fn author_signature(&self) -> &Signature {
        &self.author_signature
    }

    pub(crate) fn namespace_signature(&self) -> &Signature {
        &self.namespace_signature
    }
}

/// A single entry in a [`Replica`]
///
/// An entry is identified by a key, its [`Author`], and the [`Replica`]'s
/// [`Namespace`]. Its value is the [32-byte BLAKE3 hash](iroh_bytes::Hash)
/// of the entry's content data, the size of this content data, and a timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    id: RecordIdentifier,
    record: Record,
}

impl Entry {
    /// Create a new entry
    pub fn new(id: RecordIdentifier, record: Record) -> Self {
        Entry { id, record }
    }

    /// Get the [`RecordIdentifier`] for this entry.
    pub fn id(&self) -> &RecordIdentifier {
        &self.id
    }

    /// Get the [`NamespaceId`] of this entry.
    pub fn namespace(&self) -> NamespaceId {
        self.id.namespace()
    }

    /// Get the [`Record`] contained in this entry.
    pub fn record(&self) -> &Record {
        &self.record
    }

    /// Serialize this entry into its canonical byte representation used for signing.
    pub fn into_vec(&self, out: &mut Vec<u8>) {
        self.id.as_bytes(out);
        self.record.as_bytes(out);
    }

    /// Serialize this entry into a new vector with its canonical byte representation.
    pub fn to_vec(&self) -> Vec<u8> {
        let mut out = Vec::new();
        self.into_vec(&mut out);
        out
    }

    /// Sign this entry with a [`Namespace`] and [`Author`].
    pub fn sign(self, namespace: &Namespace, author: &Author) -> SignedEntry {
        SignedEntry::from_entry(self, namespace, author)
    }
}

/// The indentifier of a record.
#[derive(Default, Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RecordIdentifier {
    /// The key of the record.
    key: Vec<u8>,
    /// The [`NamespaceId`] of the namespace this record belongs to.
    namespace: NamespaceId,
    /// The [`AuthorId`] of the author that wrote this record.
    author: AuthorId,
}

impl AsFingerprint for RecordIdentifier {
    fn as_fingerprint(&self) -> crate::ranger::Fingerprint {
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.namespace.as_bytes());
        hasher.update(self.author.as_bytes());
        hasher.update(&self.key);
        Fingerprint(hasher.finalize().into())
    }
}

impl RangeKey for RecordIdentifier {
    fn contains(&self, range: &crate::ranger::Range<Self>) -> bool {
        use crate::ranger::contains;

        let key_range = range.clone().map(|x, y| (x.key, y.key));
        let namespace_range = range.clone().map(|x, y| (x.namespace, y.namespace));
        let author_range = range.clone().map(|x, y| (x.author, y.author));

        contains(&self.key, &key_range)
            && contains(&self.namespace, &namespace_range)
            && contains(&self.author, &author_range)
    }
}

impl RecordIdentifier {
    /// Create a new [`RecordIdentifier`].
    pub fn new(key: impl AsRef<[u8]>, namespace: NamespaceId, author: AuthorId) -> Self {
        RecordIdentifier {
            key: key.as_ref().to_vec(),
            namespace,
            author,
        }
    }

    pub(crate) fn from_parts(
        key: &[u8],
        namespace: &[u8; 32],
        author: &[u8; 32],
    ) -> anyhow::Result<Self> {
        Ok(RecordIdentifier {
            key: key.to_vec(),
            namespace: NamespaceId::from_bytes(namespace)?,
            author: AuthorId::from_bytes(author)?,
        })
    }

    /// Serialize this [`RecordIdentifier`] into a mutable byte array.
    pub(crate) fn as_bytes(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(self.namespace.as_bytes());
        out.extend_from_slice(self.author.as_bytes());
        out.extend_from_slice(&self.key);
    }

    /// Get the key of this record.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Get the [`NamespaceId`] of this record.
    pub fn namespace(&self) -> NamespaceId {
        self.namespace
    }

    pub(crate) fn namespace_bytes(&self) -> &[u8; 32] {
        self.namespace.as_bytes()
    }

    /// Get the [`AuthorId`] of this record.
    pub fn author(&self) -> AuthorId {
        self.author
    }

    pub(crate) fn author_bytes(&self) -> &[u8; 32] {
        self.author.as_bytes()
    }
}

/// The data part of an entry in a [`Replica`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    /// Record creation timestamp. Counted as micros since the Unix epoch.
    timestamp: u64,
    /// Length of the data referenced by `hash`.
    len: u64,
    /// Hash of the content data.
    hash: Hash,
}

impl Record {
    /// Create a new record.
    pub fn new(timestamp: u64, len: u64, hash: Hash) -> Self {
        Record {
            timestamp,
            len,
            hash,
        }
    }

    /// Get the timestamp of this record.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Get the length of the data addressed by this record's content hash.
    pub fn content_len(&self) -> u64 {
        self.len
    }

    /// Get the [`struct@Hash`] of the content data of this record.
    pub fn content_hash(&self) -> &Hash {
        &self.hash
    }

    /// Create a new record with a timestamp of the current system date.
    pub fn from_hash(hash: Hash, len: u64) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("time drift")
            .as_micros() as u64;
        Self::new(timestamp, len, hash)
    }

    // TODO: remove
    #[cfg(test)]
    pub(crate) fn from_data(data: impl AsRef<[u8]>, namespace: NamespaceId) -> Self {
        // Salted hash
        // TODO: do we actually want this?
        // TODO: this should probably use a namespace prefix if used
        let mut hasher = blake3::Hasher::new();
        hasher.update(namespace.as_bytes());
        hasher.update(data.as_ref());
        let hash = hasher.finalize();
        Self::from_hash(hash.into(), data.as_ref().len() as u64)
    }

    /// Serialize this record into a mutable byte array.
    pub(crate) fn as_bytes(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.timestamp.to_be_bytes());
        out.extend_from_slice(&self.len.to_be_bytes());
        out.extend_from_slice(self.hash.as_ref());
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;

    use crate::{
        ranger::Range,
        store::{self, GetFilter},
    };

    use super::*;

    #[test]
    fn test_basics_memory() -> Result<()> {
        let store = store::memory::Store::default();
        test_basics(store)?;

        Ok(())
    }

    #[cfg(feature = "fs-store")]
    #[test]
    fn test_basics_fs() -> Result<()> {
        let dbfile = tempfile::NamedTempFile::new()?;
        let store = store::fs::Store::new(dbfile.path())?;
        test_basics(store)?;
        Ok(())
    }

    fn test_basics<S: store::Store>(store: S) -> Result<()> {
        let mut rng = rand::thread_rng();
        let alice = Author::new(&mut rng);
        let bob = Author::new(&mut rng);
        let myspace = Namespace::new(&mut rng);

        let record_id = RecordIdentifier::new("/my/key", myspace.id(), alice.id());
        let record = Record::from_data(b"this is my cool data", myspace.id());
        let entry = Entry::new(record_id, record);
        let signed_entry = entry.sign(&myspace, &alice);
        signed_entry.verify().expect("failed to verify");

        let my_replica = store.new_replica(myspace)?;
        for i in 0..10 {
            my_replica
                .hash_and_insert(format!("/{i}"), &alice, format!("{i}: hello from alice"))
                .map_err(Into::into)?;
        }

        for i in 0..10 {
            let res = store
                .get_latest_by_key_and_author(my_replica.namespace(), alice.id(), format!("/{i}"))?
                .unwrap();
            let len = format!("{i}: hello from alice").as_bytes().len() as u64;
            assert_eq!(res.entry().record().content_len(), len);
            res.verify()?;
        }

        // Test multiple records for the same key
        my_replica
            .hash_and_insert("/cool/path", &alice, "round 1")
            .map_err(Into::into)?;
        let _entry = store
            .get_latest_by_key_and_author(my_replica.namespace(), alice.id(), "/cool/path")?
            .unwrap();
        // Second
        my_replica
            .hash_and_insert("/cool/path", &alice, "round 2")
            .map_err(Into::into)?;
        let _entry = store
            .get_latest_by_key_and_author(my_replica.namespace(), alice.id(), "/cool/path")?
            .unwrap();

        // Get All by author
        let entries: Vec<_> = store
            .get(
                my_replica.namespace(),
                GetFilter::all()
                    .with_author(alice.id())
                    .with_key("/cool/path"),
            )?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 2);

        // Get All by key
        let entries: Vec<_> = store
            .get(
                my_replica.namespace(),
                GetFilter::all().with_key(b"/cool/path"),
            )?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 2);

        // Get latest by key
        let entries: Vec<_> = store
            .get(
                my_replica.namespace(),
                GetFilter::latest().with_key(b"/cool/path"),
            )?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 1);

        // Get latest by prefix
        let entries: Vec<_> = store
            .get(
                my_replica.namespace(),
                GetFilter::latest().with_prefix(b"/cool"),
            )?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 1);

        // Get All
        let entries: Vec<_> = store
            .get(my_replica.namespace(), GetFilter::all())?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 12);

        // Get All latest
        let entries: Vec<_> = store
            .get(my_replica.namespace(), GetFilter::latest())?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 11);

        // insert record from different author
        let _entry = my_replica
            .hash_and_insert("/cool/path", &bob, "bob round 1")
            .map_err(Into::into)?;

        // Get All by author
        let entries: Vec<_> = store
            .get(
                my_replica.namespace(),
                GetFilter::all()
                    .with_author(alice.id())
                    .with_key("/cool/path"),
            )?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 2);

        let entries: Vec<_> = store
            .get(
                my_replica.namespace(),
                GetFilter::all()
                    .with_author(bob.id())
                    .with_key("/cool/path"),
            )?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 1);

        // Get All by key
        let entries: Vec<_> = store
            .get(
                my_replica.namespace(),
                GetFilter::all().with_key(b"/cool/path"),
            )?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 3);

        // Get latest by key
        let entries: Vec<_> = store
            .get(
                my_replica.namespace(),
                GetFilter::latest().with_key(b"/cool/path"),
            )?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 2);

        // Get latest by prefix
        let entries: Vec<_> = store
            .get(
                my_replica.namespace(),
                GetFilter::latest().with_prefix(b"/cool"),
            )?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 2);

        // Get all by prefix
        let entries: Vec<_> = store
            .get(
                my_replica.namespace(),
                GetFilter::all().with_prefix(b"/cool"),
            )?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 3);

        // Get All
        let entries: Vec<_> = store
            .get(my_replica.namespace(), GetFilter::all())?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 13);

        // Get All latest
        let entries: Vec<_> = store
            .get(my_replica.namespace(), GetFilter::latest())?
            .collect::<Result<_>>()?;
        assert_eq!(entries.len(), 12);

        Ok(())
    }

    #[test]
    fn test_multikey() {
        let mut rng = rand::thread_rng();

        let k = ["a", "c", "z"];

        let mut n: Vec<_> = (0..3).map(|_| Namespace::new(&mut rng)).collect();
        n.sort_by_key(|n| n.id());

        let mut a: Vec<_> = (0..3).map(|_| Author::new(&mut rng)).collect();
        a.sort_by_key(|a| a.id());

        // Just key
        {
            let ri0 = RecordIdentifier::new(k[0], n[0].id(), a[0].id());
            let ri1 = RecordIdentifier::new(k[1], n[0].id(), a[0].id());
            let ri2 = RecordIdentifier::new(k[2], n[0].id(), a[0].id());

            let range = Range::new(ri0.clone(), ri2.clone());
            assert!(ri0.contains(&range), "start");
            assert!(ri1.contains(&range), "inside");
            assert!(!ri2.contains(&range), "end");
        }

        // Just namespace
        {
            let ri0 = RecordIdentifier::new(k[0], n[0].id(), a[0].id());
            let ri1 = RecordIdentifier::new(k[0], n[1].id(), a[0].id());
            let ri2 = RecordIdentifier::new(k[0], n[2].id(), a[0].id());

            let range = Range::new(ri0.clone(), ri2.clone());
            assert!(ri0.contains(&range), "start");
            assert!(ri1.contains(&range), "inside");
            assert!(!ri2.contains(&range), "end");
        }

        // Just author
        {
            let ri0 = RecordIdentifier::new(k[0], n[0].id(), a[0].id());
            let ri1 = RecordIdentifier::new(k[0], n[0].id(), a[1].id());
            let ri2 = RecordIdentifier::new(k[0], n[0].id(), a[2].id());

            let range = Range::new(ri0.clone(), ri2.clone());
            assert!(ri0.contains(&range), "start");
            assert!(ri1.contains(&range), "inside");
            assert!(!ri2.contains(&range), "end");
        }

        // Just key and namespace
        {
            let ri0 = RecordIdentifier::new(k[0], n[0].id(), a[0].id());
            let ri1 = RecordIdentifier::new(k[1], n[1].id(), a[0].id());
            let ri2 = RecordIdentifier::new(k[2], n[2].id(), a[0].id());

            let range = Range::new(ri0.clone(), ri2.clone());
            assert!(ri0.contains(&range), "start");
            assert!(ri1.contains(&range), "inside");
            assert!(!ri2.contains(&range), "end");
        }
    }

    #[test]
    fn test_replica_sync_memory() -> Result<()> {
        let alice_store = store::memory::Store::default();
        let bob_store = store::memory::Store::default();

        test_replica_sync(alice_store, bob_store)?;
        Ok(())
    }

    #[cfg(feature = "fs-store")]
    #[test]
    fn test_replica_sync_fs() -> Result<()> {
        let alice_dbfile = tempfile::NamedTempFile::new()?;
        let alice_store = store::fs::Store::new(alice_dbfile.path())?;
        let bob_dbfile = tempfile::NamedTempFile::new()?;
        let bob_store = store::fs::Store::new(bob_dbfile.path())?;
        test_replica_sync(alice_store, bob_store)?;

        Ok(())
    }

    fn test_replica_sync<S: store::Store>(alice_store: S, bob_store: S) -> Result<()> {
        let alice_set = ["ape", "eel", "fox", "gnu"];
        let bob_set = ["bee", "cat", "doe", "eel", "fox", "hog"];

        let mut rng = rand::thread_rng();
        let author = Author::new(&mut rng);
        let myspace = Namespace::new(&mut rng);
        let alice = alice_store.new_replica(myspace.clone())?;
        for el in &alice_set {
            alice
                .hash_and_insert(el, &author, el.as_bytes())
                .map_err(Into::into)?;
        }

        let bob = bob_store.new_replica(myspace)?;
        for el in &bob_set {
            bob.hash_and_insert(el, &author, el.as_bytes())
                .map_err(Into::into)?;
        }

        sync(
            &author,
            &alice,
            &alice_store,
            &bob,
            &bob_store,
            &alice_set,
            &bob_set,
        )?;
        Ok(())
    }

    fn sync<S: store::Store>(
        author: &Author,
        alice: &Replica<S::Instance>,
        alice_store: &S,
        bob: &Replica<S::Instance>,
        bob_store: &S,
        alice_set: &[&str],
        bob_set: &[&str],
    ) -> Result<()> {
        let alice_peer_id = [1u8; 32];
        let bob_peer_id = [2u8; 32];
        // Sync alice - bob
        let mut next_to_bob = Some(alice.sync_initial_message().map_err(Into::into)?);
        let mut rounds = 0;
        while let Some(msg) = next_to_bob.take() {
            assert!(rounds < 100, "too many rounds");
            rounds += 1;
            println!("round {}", rounds);
            if let Some(msg) = bob
                .sync_process_message(msg, alice_peer_id)
                .map_err(Into::into)?
            {
                next_to_bob = alice
                    .sync_process_message(msg, bob_peer_id)
                    .map_err(Into::into)?;
            }
        }

        // Check result
        for el in alice_set {
            alice_store.get_latest_by_key_and_author(alice.namespace(), author.id(), el)?;
            bob_store.get_latest_by_key_and_author(bob.namespace(), author.id(), el)?;
        }

        for el in bob_set {
            alice_store.get_latest_by_key_and_author(alice.namespace(), author.id(), el)?;
            bob_store.get_latest_by_key_and_author(bob.namespace(), author.id(), el)?;
        }
        Ok(())
    }
}
