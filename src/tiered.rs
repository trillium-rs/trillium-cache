//! Tiered [`CacheStorage`] composing a fast hot tier over a durable cold tier.
//!
//! [`TieredStorage`] layers two backends: a `Hot` tier serving the working set from fast
//! storage and a `Cold` tier holding the larger, durable set. It is itself a [`CacheStorage`],
//! so it drops in wherever a single backend would go — the headline pairing is an
//! [`InMemoryStorage`] hot tier over a [`FileSystemStorage`] cold tier, but any two backends
//! compose.
//!
//! [`InMemoryStorage`]: crate::InMemoryStorage
//! [`FileSystemStorage`]: crate::FileSystemStorage
//!
//! ## Runtime
//!
//! The write path finishes asynchronously (see below), so a [`TieredStorage`] is constructed
//! with the [`Runtime`] it spawns that background work on — and it must be the runtime actually
//! driving the process, or the flush never makes progress. Construct the adapter for your
//! runtime directly (for example `trillium_smol::SmolRuntime::default()` or
//! `trillium_tokio::TokioRuntime::default()`); on a client you can instead take it from the
//! connector with `client.connector().runtime()`. The `tiered_cache` example wires this up
//! end to end.
//!
//! ## Read path
//!
//! [`get`] consults the hot tier first and, on a hit, serves from it alone. On a hot miss it
//! reads the cold tier; opening a cold entry *promotes* it, streaming the body to the reader
//! and into the hot tier at once (the same teeing used on the origin→user+storage path), so
//! the working set migrates into fast storage as it is served. A hot tier emptied by a restart
//! repopulates from cold as entries are read.
//!
//! The hot-first lookup assumes the hot tier evicts a whole [`CacheKey`] at once — all `Vary`
//! variants of a URL together — so a hot hit implies the full variant set for that key is
//! present. [`InMemoryStorage`] satisfies this. A hot tier that evicts individual variants
//! could leave siblings only in cold and hide them behind a hot hit; pair `TieredStorage` with
//! a whole-key-eviction hot tier.
//!
//! ## Write path
//!
//! [`put`] writes the body into the hot tier as it streams, then finalizing the entry spawns a
//! background task that copies it into the cold tier — a write-back. The hot tier is populated
//! synchronously; cold durability follows shortly after, off the request path. A crash in that
//! window loses the not-yet-flushed entry, which for a cache means only an extra origin fetch.
//! Because cold ends up holding every stored entry, evicting from hot only drops a fast-path
//! copy — the entry stays served from cold and re-promotes on its next read.
//!
//! ## Policy refresh
//!
//! A 304 revalidation refreshes the policy on whichever tier served the entry. After a hot
//! eviction a request may fall through to a cold copy carrying the pre-refresh policy and
//! revalidate once more; the content served is always correct.
//!
//! [`get`]: CacheStorage::get
//! [`put`]: CacheStorage::put

use crate::{CacheKey, CachePolicy, CacheStorage, PutHandle, StoredEntry, tee::TeeingReader};
use futures_lite::{AsyncReadExt, AsyncWrite, AsyncWriteExt};
use std::{
    fmt::{self, Debug, Formatter},
    io,
    pin::Pin,
    task::{Context, Poll},
};
use trillium_http::{Body, Headers};
use trillium_server_common::{Runtime, RuntimeTrait};

/// Two-tier cache storage: a fast hot tier over a durable cold tier.
///
/// See the [module documentation][self] for the read, write, and eviction behavior.
///
/// `Clone` is available when both tiers are `Clone`, and shares their backing storage.
pub struct TieredStorage<Hot, Cold> {
    hot: Hot,
    cold: Cold,
    runtime: Runtime,
}

impl<Hot, Cold> TieredStorage<Hot, Cold> {
    /// Compose `hot` and `cold` into a tiered storage, spawning background write-back onto
    /// `runtime`. Lookups and promotions favor `hot`; every stored entry is flushed through to
    /// `cold`.
    ///
    /// Pass the runtime the surrounding server or client already runs on.
    pub fn new(hot: Hot, cold: Cold, runtime: impl RuntimeTrait) -> Self {
        Self {
            hot,
            cold,
            runtime: runtime.into(),
        }
    }

    /// Borrow the hot tier.
    pub fn hot(&self) -> &Hot {
        &self.hot
    }

    /// Borrow the cold tier.
    pub fn cold(&self) -> &Cold {
        &self.cold
    }
}

impl<Hot: Clone, Cold: Clone> Clone for TieredStorage<Hot, Cold> {
    fn clone(&self) -> Self {
        Self {
            hot: self.hot.clone(),
            cold: self.cold.clone(),
            runtime: self.runtime.clone(),
        }
    }
}

impl<Hot: CacheStorage, Cold: CacheStorage> Debug for TieredStorage<Hot, Cold> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TieredStorage")
            .field("hot", &self.hot)
            .field("cold", &self.cold)
            .finish_non_exhaustive()
    }
}

impl<Hot, Cold> CacheStorage for TieredStorage<Hot, Cold>
where
    Hot: CacheStorage + Clone,
    Cold: CacheStorage + Clone,
{
    type StoredEntry = TieredEntry<Hot, Cold>;
    type PutHandle = TieredPutHandle<Hot, Cold>;

    async fn get(&self, key: &CacheKey) -> Vec<Self::StoredEntry> {
        let hot = self.hot.get(key).await;
        if !hot.is_empty() {
            return hot.into_iter().map(TieredEntry::Hot).collect();
        }
        self.cold
            .get(key)
            .await
            .into_iter()
            .map(|entry| TieredEntry::Cold {
                entry,
                hot: self.hot.clone(),
                key: key.clone(),
            })
            .collect()
    }

    async fn put(&self, key: CacheKey, policy: CachePolicy) -> io::Result<Self::PutHandle> {
        let hot = self.hot.put(key.clone(), policy.clone()).await?;
        Ok(TieredPutHandle {
            hot,
            hot_store: self.hot.clone(),
            cold: self.cold.clone(),
            runtime: self.runtime.clone(),
            key,
            policy,
        })
    }

    async fn invalidate(&self, key: &CacheKey) {
        self.hot.invalidate(key).await;
        self.cold.invalidate(key).await;
    }
}

/// One stored response from a [`TieredStorage`], held in either tier.
///
/// A cold-tier entry carries a handle to the hot tier and its key so that
/// [`open`][StoredEntry::open] can promote it — streaming the body to the reader and into the
/// hot tier at once.
pub enum TieredEntry<Hot: CacheStorage, Cold: CacheStorage> {
    /// An entry served from the hot tier.
    Hot(Hot::StoredEntry),
    /// An entry served from the cold tier, promoted into hot on open.
    Cold {
        /// The cold-tier entry.
        entry: Cold::StoredEntry,
        /// Hot tier to promote into.
        hot: Hot,
        /// Key the entry is stored under.
        key: CacheKey,
    },
}

impl<Hot, Cold> Clone for TieredEntry<Hot, Cold>
where
    Hot: CacheStorage + Clone,
    Cold: CacheStorage,
{
    fn clone(&self) -> Self {
        match self {
            Self::Hot(entry) => Self::Hot(entry.clone()),
            Self::Cold { entry, hot, key } => Self::Cold {
                entry: entry.clone(),
                hot: hot.clone(),
                key: key.clone(),
            },
        }
    }
}

impl<Hot: CacheStorage, Cold: CacheStorage> Debug for TieredEntry<Hot, Cold> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Hot(entry) => f.debug_tuple("Hot").field(entry).finish(),
            Self::Cold { entry, key, .. } => f
                .debug_struct("Cold")
                .field("entry", entry)
                .field("key", key)
                .finish_non_exhaustive(),
        }
    }
}

impl<Hot, Cold> StoredEntry for TieredEntry<Hot, Cold>
where
    Hot: CacheStorage + Clone,
    Cold: CacheStorage,
{
    fn policy(&self) -> &CachePolicy {
        match self {
            Self::Hot(entry) => entry.policy(),
            Self::Cold { entry, .. } => entry.policy(),
        }
    }

    async fn refresh_policy(&mut self, new_policy: CachePolicy) -> io::Result<()> {
        match self {
            Self::Hot(entry) => entry.refresh_policy(new_policy).await,
            Self::Cold { entry, .. } => entry.refresh_policy(new_policy).await,
        }
    }

    async fn open(self) -> io::Result<Body> {
        match self {
            Self::Hot(entry) => entry.open().await,
            Self::Cold { entry, hot, key } => {
                let policy = entry.policy().clone();
                let cold_body = entry.open().await?;
                let len = cold_body.len();
                match hot.put(key, policy).await {
                    Ok(put_handle) => {
                        let tee = TeeingReader::new(cold_body, put_handle, u64::MAX);
                        Ok(Body::new_with_trailers(tee, len))
                    }
                    Err(e) => {
                        log::warn!("cache: promotion put failed: {e}, serving cold entry only");
                        Ok(cold_body)
                    }
                }
            }
        }
    }
}

/// Streaming [`PutHandle`] for [`TieredStorage`].
///
/// Body bytes stream into the hot tier; [`finalize`][PutHandle::finalize] commits the hot entry
/// and spawns a background task that copies it into the cold tier. Dropping without finalizing
/// aborts the hot write, and nothing reaches either tier.
pub struct TieredPutHandle<Hot: CacheStorage, Cold: CacheStorage> {
    hot: Hot::PutHandle,
    hot_store: Hot,
    cold: Cold,
    runtime: Runtime,
    key: CacheKey,
    policy: CachePolicy,
}

impl<Hot: CacheStorage, Cold: CacheStorage> Debug for TieredPutHandle<Hot, Cold> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TieredPutHandle")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

// Only the hot `PutHandle` is ever polled through a pin (and `PutHandle: Unpin`); the storage
// handles and metadata are plain data, moved but never pin-projected. So the composite is
// `Unpin` regardless of whether the tier types are.
impl<Hot: CacheStorage, Cold: CacheStorage> Unpin for TieredPutHandle<Hot, Cold> {}

impl<Hot: CacheStorage, Cold: CacheStorage> AsyncWrite for TieredPutHandle<Hot, Cold> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().hot).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().hot).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().hot).poll_close(cx)
    }
}

impl<Hot, Cold> PutHandle for TieredPutHandle<Hot, Cold>
where
    Hot: CacheStorage + Clone,
    Cold: CacheStorage,
{
    async fn finalize(self, trailers: Option<Headers>) -> io::Result<()> {
        let Self {
            hot,
            hot_store,
            cold,
            runtime,
            key,
            policy,
        } = self;
        hot.finalize(trailers).await?;

        let log_key = key.clone();
        let _detached = runtime.spawn(async move {
            if let Err(e) = flush_to_cold(hot_store, cold, key, policy).await {
                log::warn!("cache: tiered background flush to cold failed for {log_key}: {e}");
            }
        });
        Ok(())
    }
}

// Copy the just-committed hot entry into the cold tier. Reads the entry back from hot (cheap
// when hot is in-memory) and streams it into a cold `put`, carrying over any trailers the hot
// body surfaces. A hot eviction between finalize and flush leaves nothing to copy — the entry
// is simply not yet durable in cold, which a later read re-promotes and re-flushes.
async fn flush_to_cold<Hot, Cold>(
    hot_store: Hot,
    cold: Cold,
    key: CacheKey,
    policy: CachePolicy,
) -> io::Result<()>
where
    Hot: CacheStorage,
    Cold: CacheStorage,
{
    let Some(entry) = hot_store
        .get(&key)
        .await
        .into_iter()
        .find(|entry| entry.policy().same_variant_as(&policy))
    else {
        return Ok(());
    };

    let mut body = entry.open().await?;
    let mut put = cold.put(key, policy).await?;
    let mut buf = [0u8; 8192];
    loop {
        let n = body.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        put.write_all(&buf[..n]).await?;
    }
    let trailers = body.trailers();
    put.finalize(trailers).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{InMemoryStorage, test_helpers::*};
    use std::time::{Duration, SystemTime};
    use trillium_http::{KnownHeaderName::*, Method, Status};
    use trillium_testing::{TestResult, harness, runtime, test};

    fn key() -> CacheKey {
        CacheKey::new(Method::Get, "http://example.com/".parse().unwrap())
    }

    fn tiered() -> TieredStorage<InMemoryStorage, InMemoryStorage> {
        TieredStorage::new(InMemoryStorage::new(), InMemoryStorage::new(), runtime())
    }

    async fn store_into(storage: &impl CacheStorage, key: CacheKey, body: &[u8]) {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600")],
        );
        let policy = policy_from(&conn, SystemTime::now(), private_cache());
        let mut handle = storage.put(key, policy).await.unwrap();
        handle.write_all(body).await.unwrap();
        handle.finalize(None).await.unwrap();
    }

    async fn read_body(entry: impl StoredEntry) -> Vec<u8> {
        let mut body = entry.open().await.unwrap();
        let mut buf = Vec::new();
        body.read_to_end(&mut buf).await.unwrap();
        buf
    }

    // Write-back finalizes cold on a spawned task; poll until it lands (or give up).
    async fn cold_settles<Hot, Cold>(
        storage: &TieredStorage<Hot, Cold>,
        key: &CacheKey,
    ) -> Vec<Cold::StoredEntry>
    where
        Hot: CacheStorage + Clone,
        Cold: CacheStorage + Clone,
    {
        for _ in 0..200 {
            let entries = storage.cold().get(key).await;
            if !entries.is_empty() {
                return entries;
            }
            storage.runtime.delay(Duration::from_millis(5)).await;
        }
        panic!("cold tier never populated");
    }

    #[test(harness)]
    async fn hot_populated_synchronously_cold_written_back() -> TestResult {
        let storage = tiered();
        store_into(&storage, key(), b"hello").await;

        // Hot is populated before finalize returns.
        let entries = storage.get(&key()).await;
        assert_eq!(entries.len(), 1);
        assert!(matches!(entries[0], TieredEntry::Hot(_)));
        assert_eq!(read_body(entries[0].clone()).await, b"hello");

        // Cold catches up on the background task.
        let cold = cold_settles(&storage, &key()).await;
        assert_eq!(cold.len(), 1);
        assert_eq!(read_body(cold[0].clone()).await, b"hello");
        Ok(())
    }

    #[test(harness)]
    async fn cold_hit_promotes_into_hot() -> TestResult {
        let storage = tiered();
        // Seed cold directly so hot starts empty — the post-restart / post-eviction shape.
        store_into(storage.cold(), key(), b"promoted").await;
        assert!(storage.hot().get(&key()).await.is_empty());

        let entries = storage.get(&key()).await;
        assert_eq!(entries.len(), 1);
        assert!(matches!(entries[0], TieredEntry::Cold { .. }));
        // Opening the cold entry streams it through into hot.
        assert_eq!(read_body(entries[0].clone()).await, b"promoted");

        let hot = storage.hot().get(&key()).await;
        assert_eq!(hot.len(), 1);
        assert_eq!(read_body(hot[0].clone()).await, b"promoted");
        Ok(())
    }

    #[test(harness)]
    async fn invalidate_clears_both_tiers() -> TestResult {
        let storage = tiered();
        store_into(&storage, key(), b"x").await;
        cold_settles(&storage, &key()).await;
        storage.invalidate(&key()).await;
        assert!(storage.get(&key()).await.is_empty());
        assert!(storage.hot().get(&key()).await.is_empty());
        assert!(storage.cold().get(&key()).await.is_empty());
        Ok(())
    }

    #[test(harness)]
    async fn drop_put_handle_without_finalize_stores_nothing() -> TestResult {
        let storage = tiered();
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
        assert!(storage.hot().get(&key()).await.is_empty());
        assert!(storage.cold().get(&key()).await.is_empty());
        Ok(())
    }

    // The headline pairing: memory hot tier over a filesystem cold tier. A cold copy survives a
    // fresh hot tier (the post-restart shape) and re-promotes into memory on read.
    #[cfg(feature = "fs")]
    #[test(harness)]
    async fn memory_over_filesystem_promotes_from_disk() -> TestResult {
        use crate::FileSystemStorage;

        let dir = tempfile::tempdir().unwrap();
        {
            let storage = TieredStorage::new(
                InMemoryStorage::new(),
                FileSystemStorage::new(dir.path()),
                runtime(),
            );
            store_into(&storage, key(), b"on-disk").await;
            cold_settles(&storage, &key()).await;
        }

        // A fresh instance over the same directory: hot is empty, cold holds the entry on disk.
        let reopened = TieredStorage::new(
            InMemoryStorage::new(),
            FileSystemStorage::new(dir.path()),
            runtime(),
        );
        assert!(reopened.hot().get(&key()).await.is_empty());

        let entries = reopened.get(&key()).await;
        assert_eq!(entries.len(), 1);
        assert!(matches!(entries[0], TieredEntry::Cold { .. }));
        assert_eq!(read_body(entries[0].clone()).await, b"on-disk");

        // Promotion pulled it into memory.
        let hot = reopened.hot().get(&key()).await;
        assert_eq!(hot.len(), 1);
        assert_eq!(read_body(hot[0].clone()).await, b"on-disk");
        Ok(())
    }
}
