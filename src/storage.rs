//! Cache storage trait and an unbounded in-memory implementation.
//!
//! The trait defines the persistence boundary for a cache: lookup,
//! insert, and invalidate, all keyed by [`CacheKey`] (URL + method).
//! Multiple entries may live under one key, distinguished by the
//! response's `Vary` signature; the handler iterates and matches via
//! [`CachePolicy::before_request`].

use crate::CachePolicy;
use std::{
    collections::HashMap,
    fmt::{self, Debug, Display, Formatter},
    sync::RwLock,
};
use trillium_client::{Method, Url};

/// Cache lookup key. Two responses share a key when they share method
/// + URL; `Vary` distinguishes variants within the entry list.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct CacheKey {
    method: Method,
    url: Url,
}

impl Display for CacheKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.method, self.url)
    }
}

impl Debug for CacheKey {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_tuple("CacheKey")
            .field(&format_args!("{self}"))
            .finish()
    }
}

impl CacheKey {
    /// Construct a cache key.
    pub fn new(method: Method, url: Url) -> Self {
        Self { method, url }
    }

    /// Method this key was constructed with.
    pub fn method(&self) -> Method {
        self.method
    }

    /// URL this key was constructed with.
    pub fn url(&self) -> &Url {
        &self.url
    }
}

/// One stored response: the [`CachePolicy`] (headers + freshness
/// state) plus the response body bytes.
#[derive(Debug, Clone)]
pub struct CacheEntry {
    policy: CachePolicy,
    body: Vec<u8>,
}

impl CacheEntry {
    /// Construct a cache entry.
    pub fn new(policy: CachePolicy, body: Vec<u8>) -> Self {
        Self { policy, body }
    }

    /// The stored policy.
    pub fn policy(&self) -> &CachePolicy {
        &self.policy
    }

    /// The stored response body.
    pub fn body(&self) -> &[u8] {
        &self.body
    }

    /// Decompose this entry into its policy and body. Used by the
    /// handler when serving a hit — the body bytes flow into the
    /// response and the policy informs whether the entry should be
    /// re-stored after a revalidation.
    pub fn into_parts(self) -> (CachePolicy, Vec<u8>) {
        (self.policy, self.body)
    }
}

/// Storage backend for cached responses.
///
/// Each key may carry multiple entries distinguished by the response's
/// `Vary` signature; the handler iterates and matches via
/// [`CachePolicy::before_request`].
pub trait CacheStorage: Debug + Send + Sync + 'static {
    /// Fetch all entries stored under `key`. Returns an empty vec when
    /// the key has no entries.
    fn get(&self, key: &CacheKey) -> impl Future<Output = Vec<CacheEntry>> + Send;

    /// Insert (or replace) an entry under `key`. If an existing entry
    /// has the same `Vary` signature, it is replaced; otherwise the
    /// new entry is appended.
    fn put(&self, key: CacheKey, entry: CacheEntry) -> impl Future<Output = ()> + Send;

    /// Remove all entries stored under `key`. Used by the handler on
    /// unsafe-method invalidation (RFC 9111 §4.4).
    fn invalidate(&self, key: &CacheKey) -> impl Future<Output = ()> + Send;
}

/// Unbounded in-memory cache storage backed by a [`HashMap`].
///
/// Useful for tests, conformance smoke-testing, and as a starting
/// point before a real backend ships.
///
/// **Memory grows without bound.** Put requests with the same `(URL,
/// method, Vary)` triple replace older entries, but distinct Vary
/// signatures and distinct keys accumulate forever. For real workloads
/// ship a size-aware backend; this one will OOM eventually under
/// sustained traffic.
#[derive(Debug, Default)]
pub struct InMemoryStorage {
    entries: RwLock<HashMap<CacheKey, Vec<CacheEntry>>>,
}

impl InMemoryStorage {
    /// Construct an empty in-memory storage.
    pub fn new() -> Self {
        Self::default()
    }

    /// Total number of stored entries across all keys. Useful for
    /// assertions in tests.
    pub fn len(&self) -> usize {
        self.entries.read().unwrap().values().map(Vec::len).sum()
    }

    /// `true` if no entries are stored.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl CacheStorage for InMemoryStorage {
    async fn get(&self, key: &CacheKey) -> Vec<CacheEntry> {
        self.entries
            .read()
            .unwrap()
            .get(key)
            .cloned()
            .unwrap_or_default()
    }

    async fn put(&self, key: CacheKey, entry: CacheEntry) {
        let mut map = self.entries.write().unwrap();
        let bucket = map.entry(key).or_default();
        let new_snapshot = entry.policy.vary_snapshot.clone();
        bucket.retain(|e| e.policy.vary_snapshot != new_snapshot);
        bucket.push(entry);
    }

    async fn invalidate(&self, key: &CacheKey) {
        self.entries.write().unwrap().remove(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::*;
    use std::time::SystemTime;
    use trillium_client::{Conn, KnownHeaderName::*, Status};
    use trillium_testing::{TestResult, harness, test};

    fn key() -> CacheKey {
        CacheKey::new(Method::Get, "http://example.com/".parse().unwrap())
    }

    fn entry_from(conn: &Conn, body: &[u8]) -> CacheEntry {
        CacheEntry::new(
            CachePolicy::new(conn, SystemTime::now(), private_cache()),
            body.to_vec(),
        )
    }

    #[test(harness)]
    async fn get_missing_key_returns_empty() -> TestResult {
        let storage = InMemoryStorage::new();
        assert!(storage.get(&key()).await.is_empty());
        Ok(())
    }

    #[test(harness)]
    async fn put_then_get_returns_entry() -> TestResult {
        let storage = InMemoryStorage::new();
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600")],
        );
        storage.put(key(), entry_from(&conn, b"hello")).await;
        let result = storage.get(&key()).await;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].body(), b"hello");
        Ok(())
    }

    #[test(harness)]
    async fn put_with_same_vary_replaces() -> TestResult {
        let storage = InMemoryStorage::new();
        let conn = exchange(
            Method::Get,
            &[(AcceptEncoding, "gzip")],
            Status::Ok,
            &[(CacheControl, "max-age=600"), (Vary, "Accept-Encoding")],
        );
        storage.put(key(), entry_from(&conn, b"v1")).await;
        storage.put(key(), entry_from(&conn, b"v2")).await;
        let result = storage.get(&key()).await;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].body(), b"v2");
        Ok(())
    }

    #[test(harness)]
    async fn put_with_different_vary_appends() -> TestResult {
        let storage = InMemoryStorage::new();
        let gzip = exchange(
            Method::Get,
            &[(AcceptEncoding, "gzip")],
            Status::Ok,
            &[(CacheControl, "max-age=600"), (Vary, "Accept-Encoding")],
        );
        let br = exchange(
            Method::Get,
            &[(AcceptEncoding, "br")],
            Status::Ok,
            &[(CacheControl, "max-age=600"), (Vary, "Accept-Encoding")],
        );
        storage.put(key(), entry_from(&gzip, b"gz")).await;
        storage.put(key(), entry_from(&br, b"br")).await;
        let result = storage.get(&key()).await;
        assert_eq!(result.len(), 2);
        Ok(())
    }

    #[test(harness)]
    async fn invalidate_removes_all_entries_for_key() -> TestResult {
        let storage = InMemoryStorage::new();
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600")],
        );
        storage.put(key(), entry_from(&conn, b"x")).await;
        assert_eq!(storage.len(), 1);
        storage.invalidate(&key()).await;
        assert!(storage.get(&key()).await.is_empty());
        assert!(storage.is_empty());
        Ok(())
    }

    #[test(harness)]
    async fn invalidate_does_not_touch_other_keys() -> TestResult {
        let storage = InMemoryStorage::new();
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600")],
        );
        let key_a = CacheKey::new(Method::Get, "http://a.example/".parse().unwrap());
        let key_b = CacheKey::new(Method::Get, "http://b.example/".parse().unwrap());
        storage.put(key_a.clone(), entry_from(&conn, b"a")).await;
        storage.put(key_b.clone(), entry_from(&conn, b"b")).await;
        storage.invalidate(&key_a).await;
        assert!(storage.get(&key_a).await.is_empty());
        assert_eq!(storage.get(&key_b).await.len(), 1);
        Ok(())
    }
}
