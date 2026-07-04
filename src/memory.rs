//! In-memory [`CacheStorage`].

use crate::{CacheKey, CachePolicy, CacheStorage, PutHandle, StoredEntry};
use futures_lite::{AsyncRead, AsyncWrite};
use moka::{future::Cache, ops::compute::Op};
use std::{
    fmt::{self, Debug, Formatter},
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use trillium_http::{Body, BodySource, Headers};

const DEFAULT_MAX_CAPACITY_BYTES: u64 = 256 * 1024 * 1024;

// All variants stored under one CacheKey. Cheap to clone (Arc); held as the moka
// value type so eviction operates per-CacheKey.
type Bucket = Arc<[Variant]>;

#[derive(Clone)]
struct Variant {
    policy: Arc<CachePolicy>,
    body: Arc<[u8]>,
    trailers: Option<Headers>,
}

impl Debug for Variant {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Variant")
            .field("body_len", &self.body.len())
            .field("has_trailers", &self.trailers.is_some())
            .finish_non_exhaustive()
    }
}

/// Bounded in-memory cache storage.
///
/// Suitable for production reverse-proxy and client-side caching: byte-aware size cap,
/// scan-resistant admission, and concurrent reads and writes on distinct keys without
/// contention.
///
/// Defaults to a 256 MiB byte cap; override with
/// [`with_max_capacity_bytes`][Self::with_max_capacity_bytes],
/// [`unbounded`][Self::unbounded],
/// [`with_time_to_idle`][Self::with_time_to_idle], and
/// [`with_time_to_live`][Self::with_time_to_live]. Each setter
/// discards any previously inserted entries; configure at
/// construction, before the storage is populated or shared.
///
/// `Clone` is cheap — clones share the same backing storage.
///
/// # Granularity
///
/// Eviction is coarse: the unit is one [`CacheKey`] (method + URL), and all `Vary` variants
/// stored under that key live and die together during eviction. In typical traffic patterns
/// variants of the same URL are hot or cold together (a single `Accept-Encoding` is usually
/// dominant, etc.), so the cost is bounded — at worst we keep a few cold variants resident
/// alongside one hot variant. This is correct per RFC 9111; the only consequence is slightly
/// less efficient use of memory than per-variant eviction would give.
///
/// # Sizing
///
/// The byte cap is enforced over stored *body* bytes only (the dominant cost); headers and
/// other metadata are not counted. The per-response cap on [`Cache::with_max_cacheable_size`]
/// interacts independently — that one bounds how large any single response may be; the storage
/// cap bounds total resident size across the cache.
///
/// [`Cache::with_max_cacheable_size`]: crate::Cache::with_max_cacheable_size
#[derive(Clone)]
pub struct InMemoryStorage {
    cache: Cache<CacheKey, Bucket>,
    max_capacity_bytes: Option<u64>,
    time_to_idle: Option<Duration>,
    time_to_live: Option<Duration>,
}

impl Debug for InMemoryStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryStorage")
            .field("entry_count", &self.cache.entry_count())
            .field("weighted_size", &self.cache.weighted_size())
            .field("max_capacity_bytes", &self.max_capacity_bytes)
            .field("time_to_idle", &self.time_to_idle)
            .field("time_to_live", &self.time_to_live)
            .finish_non_exhaustive()
    }
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStorage {
    /// Construct an in-memory storage with default settings: a
    /// 256 MiB byte cap, no idle eviction, no TTL.
    pub fn new() -> Self {
        Self {
            cache: build_cache(Some(DEFAULT_MAX_CAPACITY_BYTES), None, None),
            max_capacity_bytes: Some(DEFAULT_MAX_CAPACITY_BYTES),
            time_to_idle: None,
            time_to_live: None,
        }
    }

    /// Set the maximum total stored body size, in bytes. Entries are
    /// evicted when inserts would exceed this cap. Defaults to
    /// 256 MiB.
    pub fn with_max_capacity_bytes(mut self, bytes: u64) -> Self {
        self.max_capacity_bytes = Some(bytes);
        self.rebuild();
        self
    }

    /// Remove the size cap. The cache grows without bound. Useful in
    /// tests and short-lived processes; production deployments should
    /// prefer the default capped configuration.
    pub fn unbounded(mut self) -> Self {
        self.max_capacity_bytes = None;
        self.rebuild();
        self
    }

    /// Evict entries that have not been read in this duration. Off by
    /// default.
    pub fn with_time_to_idle(mut self, duration: Duration) -> Self {
        self.time_to_idle = Some(duration);
        self.rebuild();
        self
    }

    /// Evict entries this duration after their last insert,
    /// regardless of access. Off by default.
    ///
    /// Note: this is independent of RFC 9111 freshness — a stored
    /// entry may be evicted by TTL while still within its
    /// `max-age`/`s-maxage` window, or remain past it (the
    /// [`CachePolicy`] handles freshness on read).
    pub fn with_time_to_live(mut self, duration: Duration) -> Self {
        self.time_to_live = Some(duration);
        self.rebuild();
        self
    }

    /// Approximate count of stored [`CacheKey`]s. Each key may hold
    /// multiple `Vary` variants. Eventually consistent — call
    /// [`run_pending_tasks`][Self::run_pending_tasks] first for a
    /// settled value (useful in tests).
    pub fn entry_count(&self) -> u64 {
        self.cache.entry_count()
    }

    /// Approximate total weighted size (sum of stored body bytes
    /// across all entries). Eventually consistent — call
    /// [`run_pending_tasks`][Self::run_pending_tasks] first for a
    /// settled value.
    pub fn weighted_size(&self) -> u64 {
        self.cache.weighted_size()
    }

    /// Flush pending eviction/insertion bookkeeping. Call before
    /// reading [`entry_count`][Self::entry_count] or
    /// [`weighted_size`][Self::weighted_size] when an exact value
    /// matters.
    pub async fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks().await;
    }

    // moka::future::Cache has no resize/set-capacity API — configuration is
    // fixed at build time. Each setter rebuilds the backing cache.
    fn rebuild(&mut self) {
        self.cache = build_cache(
            self.max_capacity_bytes,
            self.time_to_idle,
            self.time_to_live,
        );
    }
}

fn build_cache(
    max_capacity_bytes: Option<u64>,
    time_to_idle: Option<Duration>,
    time_to_live: Option<Duration>,
) -> Cache<CacheKey, Bucket> {
    let mut builder = Cache::<CacheKey, Bucket>::builder().weigher(weigh_bucket);
    if let Some(cap) = max_capacity_bytes {
        builder = builder.max_capacity(cap);
    }
    if let Some(tti) = time_to_idle {
        builder = builder.time_to_idle(tti);
    }
    if let Some(ttl) = time_to_live {
        builder = builder.time_to_live(ttl);
    }
    builder.build()
}

fn weigh_bucket(_key: &CacheKey, bucket: &Bucket) -> u32 {
    let total: u64 = bucket.iter().map(|v| v.body.len() as u64).sum();
    u32::try_from(total).unwrap_or(u32::MAX)
}

/// In-memory [`StoredEntry`]. Cheap to clone — fields are `Arc`-shared
/// with the backing cache.
#[derive(Clone)]
pub struct InMemoryEntry {
    variant: Variant,
    cache: Cache<CacheKey, Bucket>,
    key: CacheKey,
}

impl Debug for InMemoryEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryEntry")
            .field("key", &self.key)
            .field("variant", &self.variant)
            .finish_non_exhaustive()
    }
}

impl StoredEntry for InMemoryEntry {
    fn policy(&self) -> &CachePolicy {
        &self.variant.policy
    }

    async fn refresh_policy(&mut self, new_policy: CachePolicy) -> io::Result<()> {
        let new_arc = Arc::new(new_policy);
        // Update the local view first so an immediately-following policy() call sees the new
        // value even if the cache update below has nothing to write back (e.g. the entry was
        // already evicted).
        self.variant.policy = Arc::clone(&new_arc);

        self.cache
            .entry(self.key.clone())
            .and_compute_with(|maybe_entry| async move {
                let Some(entry) = maybe_entry else {
                    return Op::Nop;
                };
                let bucket = entry.into_value();
                let mut updated = false;
                let new_variants: Vec<Variant> = bucket
                    .iter()
                    .map(|v| {
                        if !updated && v.policy.same_variant_as(&new_arc) {
                            updated = true;
                            Variant {
                                policy: Arc::clone(&new_arc),
                                body: Arc::clone(&v.body),
                                trailers: v.trailers.clone(),
                            }
                        } else {
                            v.clone()
                        }
                    })
                    .collect();
                if updated {
                    Op::Put(Arc::from(new_variants.into_boxed_slice()))
                } else {
                    Op::Nop
                }
            })
            .await;
        Ok(())
    }

    async fn open(self) -> io::Result<Body> {
        let Variant { body, trailers, .. } = self.variant;
        let len = u64::try_from(body.len()).ok();
        let source = ReplayBodySource {
            body,
            position: 0,
            trailers,
        };
        Ok(Body::new_with_trailers(source, len))
    }
}

// BodySource over a shared Arc<[u8]>. No copy on open; reads slice through the Arc.
struct ReplayBodySource {
    body: Arc<[u8]>,
    position: usize,
    trailers: Option<Headers>,
}

impl AsyncRead for ReplayBodySource {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let remaining = self.body.len() - self.position;
        let n = remaining.min(buf.len());
        if n > 0 {
            buf[..n].copy_from_slice(&self.body[self.position..self.position + n]);
            self.position += n;
        }
        Poll::Ready(Ok(n))
    }
}

impl BodySource for ReplayBodySource {
    fn trailers(self: Pin<&mut Self>) -> Option<Headers> {
        self.get_mut().trailers.take()
    }
}

impl CacheStorage for InMemoryStorage {
    type PutHandle = InMemoryPutHandle;
    type StoredEntry = InMemoryEntry;

    async fn get(&self, key: &CacheKey) -> Vec<Self::StoredEntry> {
        let Some(bucket) = self.cache.get(key).await else {
            return Vec::new();
        };
        bucket
            .iter()
            .map(|variant| InMemoryEntry {
                variant: variant.clone(),
                cache: self.cache.clone(),
                key: key.clone(),
            })
            .collect()
    }

    async fn put(&self, key: CacheKey, policy: CachePolicy) -> io::Result<Self::PutHandle> {
        Ok(InMemoryPutHandle {
            cache: self.cache.clone(),
            key,
            policy,
            buffer: Vec::new(),
        })
    }

    async fn invalidate(&self, key: &CacheKey) {
        self.cache.invalidate(key).await;
    }
}

/// Streaming [`PutHandle`] for [`InMemoryStorage`].
///
/// Buffers writes internally; [`finalize`][Self::finalize] commits the
/// buffered bytes and any trailers to the cache atomically. Drop
/// without finalize discards the buffered bytes.
#[derive(Debug)]
pub struct InMemoryPutHandle {
    cache: Cache<CacheKey, Bucket>,
    key: CacheKey,
    policy: CachePolicy,
    buffer: Vec<u8>,
}

impl AsyncWrite for InMemoryPutHandle {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.buffer.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl PutHandle for InMemoryPutHandle {
    async fn finalize(self, trailers: Option<Headers>) -> io::Result<()> {
        let Self {
            cache,
            key,
            policy,
            buffer,
        } = self;
        let new_variant = Variant {
            policy: Arc::new(policy),
            body: Arc::from(buffer.into_boxed_slice()),
            trailers,
        };

        cache
            .entry(key)
            .and_upsert_with(|maybe_entry| async move {
                let mut variants: Vec<Variant> = match maybe_entry {
                    Some(entry) => entry.into_value().to_vec(),
                    None => Vec::new(),
                };
                variants.retain(|v| !v.policy.same_variant_as(&new_variant.policy));
                variants.push(new_variant);
                Arc::from(variants.into_boxed_slice())
            })
            .await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::*;
    use futures_lite::{AsyncReadExt, AsyncWriteExt};
    use std::time::SystemTime;
    use trillium_client::Conn;
    use trillium_http::{KnownHeaderName::*, Method, Status};
    use trillium_testing::{TestResult, harness, test};

    fn key() -> CacheKey {
        CacheKey::new(Method::Get, "http://example.com/".parse().unwrap())
    }

    async fn store(storage: &InMemoryStorage, conn: &Conn, body: &[u8]) {
        let policy = policy_from(conn, SystemTime::now(), private_cache());
        let mut handle = storage.put(key(), policy).await.unwrap();
        handle.write_all(body).await.unwrap();
        handle.finalize(None).await.unwrap();
    }

    async fn read_body(entry: InMemoryEntry) -> Vec<u8> {
        let mut body = entry.open().await.unwrap();
        let mut buf = Vec::new();
        body.read_to_end(&mut buf).await.unwrap();
        buf
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
        store(&storage, &conn, b"hello").await;
        let result = storage.get(&key()).await;
        assert_eq!(result.len(), 1);
        assert_eq!(read_body(result[0].clone()).await, b"hello");
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
        store(&storage, &conn, b"v1").await;
        store(&storage, &conn, b"v2").await;
        let result = storage.get(&key()).await;
        assert_eq!(result.len(), 1);
        assert_eq!(read_body(result[0].clone()).await, b"v2");
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
        store(&storage, &gzip, b"gz").await;
        store(&storage, &br, b"br").await;
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
        store(&storage, &conn, b"x").await;
        storage.run_pending_tasks().await;
        assert_eq!(storage.entry_count(), 1);
        storage.invalidate(&key()).await;
        assert!(storage.get(&key()).await.is_empty());
        storage.run_pending_tasks().await;
        assert_eq!(storage.entry_count(), 0);
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
        {
            let policy_a = policy_from(&conn, SystemTime::now(), private_cache());
            let mut h = storage.put(key_a.clone(), policy_a).await.unwrap();
            h.write_all(b"a").await.unwrap();
            h.finalize(None).await.unwrap();
        }
        {
            let policy_b = policy_from(&conn, SystemTime::now(), private_cache());
            let mut h = storage.put(key_b.clone(), policy_b).await.unwrap();
            h.write_all(b"b").await.unwrap();
            h.finalize(None).await.unwrap();
        }
        storage.invalidate(&key_a).await;
        assert!(storage.get(&key_a).await.is_empty());
        assert_eq!(storage.get(&key_b).await.len(), 1);
        Ok(())
    }

    #[test(harness)]
    async fn drop_put_handle_without_finalize_discards() -> TestResult {
        let storage = InMemoryStorage::new();
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600")],
        );
        let policy = policy_from(&conn, SystemTime::now(), private_cache());
        let mut handle = storage.put(key(), policy).await.unwrap();
        handle.write_all(b"partial").await.unwrap();
        drop(handle);
        assert!(storage.get(&key()).await.is_empty());
        Ok(())
    }

    #[test(harness)]
    async fn refresh_policy_updates_storage() -> TestResult {
        let storage = InMemoryStorage::new();
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600")],
        );
        store(&storage, &conn, b"body").await;

        let mut entries = storage.get(&key()).await;
        let original_time = entries[0].policy().response_time;
        let refreshed = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=1200")],
        );
        let new_policy = policy_from(
            &refreshed,
            original_time + Duration::from_secs(100),
            private_cache(),
        );
        entries[0].refresh_policy(new_policy).await.unwrap();

        let fresh = storage.get(&key()).await;
        assert_eq!(fresh.len(), 1);
        assert_ne!(fresh[0].policy().response_time, original_time);
        Ok(())
    }

    // Size-bounded: insert past the cap and verify that the cache stays within bounds.
    #[test(harness)]
    async fn size_cap_evicts_old_entries() -> TestResult {
        // Cap at 1 KiB; insert several 600-byte responses under distinct URLs.
        let storage = InMemoryStorage::new().with_max_capacity_bytes(1024);
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600")],
        );
        let body = vec![b'x'; 600];
        for i in 0..10 {
            let key = CacheKey::new(
                Method::Get,
                format!("http://example.com/{i}").parse().unwrap(),
            );
            let policy = policy_from(&conn, SystemTime::now(), private_cache());
            let mut h = storage.put(key, policy).await.unwrap();
            h.write_all(&body).await.unwrap();
            h.finalize(None).await.unwrap();
        }
        storage.run_pending_tasks().await;
        assert!(
            storage.weighted_size() <= 1024,
            "weighted size {} should be within cap of 1024",
            storage.weighted_size()
        );
        Ok(())
    }
}
