//! Utility functions and types.
use bytes::Bytes;
use derive_more::{Debug, Display, From, Into};
use serde::{Deserialize, Serialize};
use std::{borrow::Borrow, fmt, sync::Arc, time::SystemTime};

use crate::{BlobFormat, Hash, HashAndFormat};

pub mod io;
pub mod progress;

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

struct DD<T: fmt::Display>(T);

impl<T: fmt::Display> fmt::Debug for DD<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
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

/// A trait for things that can track liveness of blobs and collections.
///
/// This trait works together with [TempTag] to keep track of the liveness of a
/// blob or collection.
///
/// It is important to include the format in the liveness tracking, since
/// protecting a collection means protecting the blob and all its children,
/// whereas protecting a raw blob only protects the blob itself.
pub trait LivenessTracker: std::fmt::Debug + Send + Sync + 'static {
    /// Called on clone
    fn on_clone(&self, inner: &HashAndFormat);
    /// Called on drop
    fn on_drop(&self, inner: &HashAndFormat);
}

/// A hash and format pair that is protected from garbage collection.
///
/// If format is raw, this will protect just the blob
/// If format is collection, this will protect the collection and all blobs in it
#[derive(Debug)]
pub struct TempTag {
    /// The hash and format we are pinning
    inner: HashAndFormat,
    /// liveness tracker
    liveness: Option<Arc<dyn LivenessTracker>>,
}

impl TempTag {
    /// Create a new temp tag for the given hash and format
    ///
    /// This should only be used by store implementations.
    ///
    /// The caller is responsible for increasing the refcount on creation and to
    /// make sure that temp tags that are created between a mark phase and a sweep
    /// phase are protected.
    pub fn new(inner: HashAndFormat, liveness: Option<Arc<dyn LivenessTracker>>) -> Self {
        if let Some(liveness) = liveness.as_ref() {
            liveness.on_clone(&inner);
        }
        Self { inner, liveness }
    }

    /// The hash of the pinned item
    pub fn inner(&self) -> &HashAndFormat {
        &self.inner
    }

    /// The hash of the pinned item
    pub fn hash(&self) -> &Hash {
        &self.inner.hash
    }

    /// The format of the pinned item
    pub fn format(&self) -> BlobFormat {
        self.inner.format
    }

    /// Keep the item alive until the end of the process
    pub fn leak(mut self) {
        // set the liveness tracker to None, so that the refcount is not decreased
        // during drop. This means that the refcount will never reach 0 and the
        // item will not be gced until the end of the process.
        self.liveness = None;
    }
}

impl Clone for TempTag {
    fn clone(&self) -> Self {
        Self::new(self.inner, self.liveness.clone())
    }
}

impl Drop for TempTag {
    fn drop(&mut self) {
        if let Some(liveness) = self.liveness.as_ref() {
            liveness.on_drop(&self.inner);
        }
    }
}

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
