//! A latch that lets a failure which repeats on every request log once per
//! cause.
//!
//! A key that has failed stays latched until the caller clears it, so a
//! cause that recovers and fails again logs again. The latch holds at most
//! [`MAX_KEYS`] keys, because some keys come from the client; past that, a
//! new key doesn't log.

use std::borrow::Borrow;
use std::hash::Hash;

use dashmap::DashMap;

const MAX_KEYS: usize = 1024;

pub(crate) struct WarnOnce<K: Eq + Hash>(DashMap<K, ()>);

impl<K: Eq + Hash> Default for WarnOnce<K> {
    fn default() -> Self {
        Self(DashMap::new())
    }
}

impl<K: Eq + Hash> WarnOnce<K> {
    /// Latch `key`. True when the key was not latched yet, so the caller
    /// should log.
    pub(crate) fn first<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ToOwned<Owned = K> + ?Sized,
    {
        if self.0.contains_key(key) || self.0.len() >= MAX_KEYS {
            return false;
        }
        self.0.insert(key.to_owned(), ()).is_none()
    }

    /// Unlatch `key` after it succeeds. Takes only a read lock while the key
    /// is not latched, which is the common case on the request path.
    pub(crate) fn clear<Q>(&self, key: &Q)
    where
        K: Borrow<Q>,
        Q: Eq + Hash + ?Sized,
    {
        if self.0.contains_key(key) {
            self.0.remove(key);
        }
    }
}

#[cfg(test)]
mod tests;
