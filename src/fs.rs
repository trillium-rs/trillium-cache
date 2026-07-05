//! Filesystem-backed [`CacheStorage`].

use crate::{
    CacheKey, CachePolicy, CacheStorage, PutHandle, StoredEntry, fs_shims, policy::PolicyRepr,
};
use futures_lite::{AsyncRead, AsyncWrite, AsyncWriteExt};
use moka::{notification::RemovalCause, sync::Cache};
use sha2::{Digest, Sha256};
use std::{
    fmt::{self, Debug, Formatter, Write as _},
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use trillium_http::{Body, BodySource, Headers};

const META_SUFFIX: &str = ".meta";
const BODY_SUFFIX: &str = ".body";

// Disk caches are cheap to grow relative to memory, so the default ceiling is larger than
// `InMemoryStorage`'s.
const DEFAULT_MAX_CAPACITY_BYTES: u64 = 1024 * 1024 * 1024;

// Disambiguates concurrent temporary files under one directory. Process-local; on-disk
// temporaries from a previous run are never read (only committed files are).
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Filesystem-backed cache storage rooted at a directory.
///
/// Persists cached responses under a root directory so they survive process restarts. Each
/// response is two files: a `<hash>.meta` sidecar holding the [`CachePolicy`] and any trailers
/// as an rkyv-encoded binary blob, and a `<hash>.body` holding the raw body bytes and nothing
/// else. Bodies stream in and out — [`put`] writes to a temporary file the caller feeds
/// incrementally, and [`open`] streams the stored body back without loading it into memory. The
/// metadata is not human-readable; it is optimized for compact, fast loading rather than
/// inspection.
///
/// Defaults to a 1 GiB byte cap; override with
/// [`with_max_capacity_bytes`][Self::with_max_capacity_bytes] or remove it with
/// [`unbounded`][Self::unbounded]. Optional time-based eviction is available through
/// [`with_time_to_idle`][Self::with_time_to_idle] and
/// [`with_time_to_live`][Self::with_time_to_live] (off by default).
///
/// `Clone` is cheap — clones share the same root and capacity index, and see each other's
/// writes.
///
/// # Layout
///
/// Entries live at `<root>/<key-hash>/<variant-hash>.{meta,body}`. The key hash is a SHA-256
/// of the request method and URL; the variant hash is a SHA-256 of the `Vary` signature, so
/// the multiple variants of one URL are sibling files in the same directory and [`get`]
/// enumerates them by reading that directory. Writing a variant that already exists replaces
/// it.
///
/// # Durability
///
/// Writes commit by renaming a fully-written temporary file into place, and the `.meta` is
/// written last — a reader treats it as the commit marker, so a half-written or abandoned entry
/// (a [`PutHandle`] dropped without [`finalize`]) is never visible to [`get`].
///
/// # Capacity
///
/// A byte cap (1 GiB by default) bounds the total stored body size. When a write would push
/// the total past the cap, least-recently-used variants are evicted — their `.meta` and
/// `.body` files deleted — until the cache fits. The cap counts body bytes only, per variant,
/// matching the granularity of the on-disk layout. Reads count as use, so a frequently-served
/// variant outlives idle ones. Override with [`with_max_capacity_bytes`] or remove the cap with
/// [`unbounded`].
///
/// The cap is tracked in an in-memory index built by scanning the root at construction, so it
/// survives restarts (recency resets to whatever order the scan encounters). A directory that
/// grew past the current cap under an older, unbounded configuration is trimmed to fit on the
/// next construction.
///
/// # Expiry
///
/// Beyond the size cap, entries can be evicted on a timer:
/// [`with_time_to_idle`][Self::with_time_to_idle] drops variants not read within a duration,
/// [`with_time_to_live`][Self::with_time_to_live] drops them a duration after they are stored.
/// Both delete the variant's files on eviction, just like size eviction. This is best-effort
/// space reclamation rather than a hard read gate — [`get`][CacheStorage::get] enumerates the
/// files on disk, so a just-expired variant may still be served in the brief window before its
/// files are deleted. It is never a correctness hazard: RFC 9111 freshness is enforced by the
/// [`Cache`](crate::Cache) handler from the stored [`CachePolicy`], independent of this
/// storage-level expiry. Both clocks are seeded at construction, so a reopened directory times
/// each entry from the reopen, not from its pre-restart history.
///
/// # Runtime
///
/// Filesystem access goes through the runtime selected by the `smol`, `tokio`, or `async-std`
/// feature. Enabling `fs` without one of those compiles but panics on use.
///
/// [`put`]: CacheStorage::put
/// [`get`]: CacheStorage::get
/// [`open`]: StoredEntry::open
/// [`finalize`]: PutHandle::finalize
/// [`with_max_capacity_bytes`]: FileSystemStorage::with_max_capacity_bytes
/// [`unbounded`]: FileSystemStorage::unbounded
#[derive(Clone)]
pub struct FileSystemStorage {
    root: Arc<PathBuf>,
    index: Cache<VariantId, u64>,
    max_capacity_bytes: Option<u64>,
    time_to_idle: Option<Duration>,
    time_to_live: Option<Duration>,
}

impl Debug for FileSystemStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileSystemStorage")
            .field("root", &self.root)
            .field("weighted_size", &self.index.weighted_size())
            .field("max_capacity_bytes", &self.max_capacity_bytes)
            .field("time_to_idle", &self.time_to_idle)
            .field("time_to_live", &self.time_to_live)
            .finish()
    }
}

impl FileSystemStorage {
    /// Construct a storage rooted at `root` with a 1 GiB byte cap. The directory is created
    /// on demand as entries are written; it need not exist yet. If it exists, it is scanned
    /// to seed the capacity index, so previously stored entries count against the cap.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        let root = Arc::new(root.into());
        let max_capacity_bytes = Some(DEFAULT_MAX_CAPACITY_BYTES);
        let index = build_index(Arc::clone(&root), max_capacity_bytes, None, None);
        scan_root(&root, &index);
        Self {
            root,
            index,
            max_capacity_bytes,
            time_to_idle: None,
            time_to_live: None,
        }
    }

    /// Set the maximum total stored body size, in bytes. Least-recently-used variants are
    /// evicted — their files deleted — when a write would exceed this cap. Defaults to
    /// 1 GiB. Re-scans the root, so a directory already over the new cap is trimmed to fit.
    pub fn with_max_capacity_bytes(mut self, bytes: u64) -> Self {
        self.max_capacity_bytes = Some(bytes);
        self.rebuild();
        self
    }

    /// Remove the size cap. Stored bytes grow without bound. Useful in tests and short-lived
    /// processes; a cache living on shared disk should prefer the default capped
    /// configuration.
    pub fn unbounded(mut self) -> Self {
        self.max_capacity_bytes = None;
        self.rebuild();
        self
    }

    /// Evict entries that have not been read in this duration, deleting their files. Off by
    /// default.
    ///
    /// This is best-effort space reclamation, not a read gate: [`get`](CacheStorage::get)
    /// enumerates the files on disk rather than the expiry index, so a just-expired variant may
    /// still be served in the window before the eviction is processed and its files deleted. It
    /// never serves *stale* content — RFC 9111 freshness is enforced by the
    /// [`Cache`](crate::Cache) handler from the stored [`CachePolicy`], independent of this
    /// storage-level expiry. (The in-memory backend's idle eviction, by contrast, `get`
    /// observes as a hard miss.)
    ///
    /// The idle clock is seeded at construction: a reopened directory counts idle time from the
    /// reopen, not from each entry's last read before the restart.
    pub fn with_time_to_idle(mut self, duration: Duration) -> Self {
        self.time_to_idle = Some(duration);
        self.rebuild();
        self
    }

    /// Evict entries this duration after their last insert regardless of access, deleting their
    /// files. Off by default.
    ///
    /// Best-effort like [`with_time_to_idle`](Self::with_time_to_idle): a just-expired variant
    /// may be served until its files are deleted, but never past RFC 9111 freshness, which the
    /// [`Cache`](crate::Cache) handler enforces separately. This TTL is independent of that
    /// freshness — an entry may be evicted while still fresh, or linger briefly past it.
    ///
    /// The clock is seeded at construction, so a reopened directory counts each entry's TTL
    /// from the reopen rather than its original store time.
    pub fn with_time_to_live(mut self, duration: Duration) -> Self {
        self.time_to_live = Some(duration);
        self.rebuild();
        self
    }

    /// Approximate total stored body size, in bytes, currently counted against the cap.
    /// Eventually consistent — call [`run_pending_tasks`][Self::run_pending_tasks] first for
    /// a settled value.
    pub fn weighted_size(&self) -> u64 {
        self.index.weighted_size()
    }

    /// Approximate count of stored variants. Eventually consistent — call
    /// [`run_pending_tasks`][Self::run_pending_tasks] first for a settled value.
    pub fn entry_count(&self) -> u64 {
        self.index.entry_count()
    }

    /// Flush pending eviction bookkeeping, including deletion of files for evicted variants.
    /// Call before reading [`weighted_size`][Self::weighted_size] or
    /// [`entry_count`][Self::entry_count] when an exact value matters.
    pub async fn run_pending_tasks(&self) {
        self.index.run_pending_tasks();
    }

    // The capacity index has no resize API; rebuilding it and re-scanning the root applies a
    // new cap while preserving on-disk entries (unlike the in-memory backend, disk data
    // survives a reconfigure).
    fn rebuild(&mut self) {
        self.index = build_index(
            Arc::clone(&self.root),
            self.max_capacity_bytes,
            self.time_to_idle,
            self.time_to_live,
        );
        scan_root(&self.root, &self.index);
    }
}

// Identity of one stored variant, sufficient to reconstruct its `.meta`/`.body` paths under
// a known root. Keys the capacity index.
#[derive(Clone, Hash, PartialEq, Eq)]
struct VariantId {
    key_hash: String,
    variant_hash: String,
}

// Build the capacity index. The eviction listener deletes a variant's files when moka
// evicts it for size or expiry; replacement and explicit invalidation are handled at their
// call sites, so the listener ignores those causes.
fn build_index(
    root: Arc<PathBuf>,
    max_capacity_bytes: Option<u64>,
    time_to_idle: Option<Duration>,
    time_to_live: Option<Duration>,
) -> Cache<VariantId, u64> {
    let mut builder = Cache::<VariantId, u64>::builder()
        .weigher(|_key, &body_len| u32::try_from(body_len).unwrap_or(u32::MAX))
        .eviction_listener(move |id: Arc<VariantId>, _body_len, cause: RemovalCause| {
            if cause.was_evicted() {
                let dir = root.join(&id.key_hash);
                let _ = std::fs::remove_file(dir.join(format!("{}{META_SUFFIX}", id.variant_hash)));
                let _ = std::fs::remove_file(dir.join(format!("{}{BODY_SUFFIX}", id.variant_hash)));
            }
        });
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

// Seed the index from the root, counting each committed variant's body length against the
// cap. Runs on the calling thread with blocking IO — a one-time construction cost — and
// forces eviction so an over-cap directory is trimmed before the storage is used.
fn scan_root(root: &Path, index: &Cache<VariantId, u64>) {
    let Ok(key_dirs) = std::fs::read_dir(root) else {
        return;
    };
    for key_entry in key_dirs.flatten() {
        let key_dir = key_entry.path();
        let Some(key_hash) = file_stem_string(&key_dir) else {
            continue;
        };
        let Ok(files) = std::fs::read_dir(&key_dir) else {
            continue;
        };
        for file in files.flatten() {
            let path = file.path();
            let Some(variant_hash) = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.strip_suffix(META_SUFFIX))
                .map(str::to_string)
            else {
                continue;
            };
            let body = key_dir.join(format!("{variant_hash}{BODY_SUFFIX}"));
            let Ok(metadata) = std::fs::metadata(&body) else {
                continue;
            };
            index.insert(
                VariantId {
                    key_hash: key_hash.clone(),
                    variant_hash,
                },
                metadata.len(),
            );
        }
    }
    index.run_pending_tasks();
}

fn file_stem_string(path: &Path) -> Option<String> {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(str::to_string)
}

// The rkyv-encoded sidecar written alongside each body. `PolicyRepr` recomputes the derived
// cache-control fields on load, so only the directly-captured policy fields are stored.
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
struct StoredMeta {
    policy: PolicyRepr,
    trailers: Option<Headers>,
}

impl CacheStorage for FileSystemStorage {
    type PutHandle = FsPutHandle;
    type StoredEntry = FsStoredEntry;

    async fn get(&self, key: &CacheKey) -> Vec<Self::StoredEntry> {
        let key_hash = key_hash(key);
        let dir = self.root.join(&key_hash);
        let Ok(paths) = fs_shims::read_dir_paths(&dir).await else {
            return Vec::new();
        };

        let mut entries = Vec::new();
        for path in paths {
            let Some(variant_hash) = path
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| name.strip_suffix(META_SUFFIX))
                .map(str::to_string)
            else {
                continue;
            };
            let Ok(bytes) = fs_shims::read(&path).await else {
                continue;
            };
            let Ok(meta) = deserialize_meta(&bytes) else {
                continue;
            };
            // Count the lookup as use so a frequently-served variant survives eviction.
            self.index.get(&VariantId {
                key_hash: key_hash.clone(),
                variant_hash: variant_hash.clone(),
            });
            entries.push(FsStoredEntry {
                meta_path: path,
                body_path: dir.join(format!("{variant_hash}{BODY_SUFFIX}")),
                policy: meta.policy.into(),
                trailers: meta.trailers,
            });
        }
        entries
    }

    async fn put(&self, key: CacheKey, policy: CachePolicy) -> io::Result<Self::PutHandle> {
        let key_hash = key_hash(&key);
        let dir = self.root.join(&key_hash);
        fs_shims::create_dir_all(&dir).await?;

        let variant_hash = variant_hash(&policy);
        let n = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let body_tmp = dir.join(format!("{variant_hash}{BODY_SUFFIX}.tmp.{n}"));
        let writer = fs_shims::create(&body_tmp).await?;

        Ok(FsPutHandle {
            writer,
            body_tmp,
            body_final: dir.join(format!("{variant_hash}{BODY_SUFFIX}")),
            meta_tmp: dir.join(format!("{variant_hash}{META_SUFFIX}.tmp.{n}")),
            meta_final: dir.join(format!("{variant_hash}{META_SUFFIX}")),
            policy,
            index: self.index.clone(),
            variant_id: VariantId {
                key_hash,
                variant_hash,
            },
            written: 0,
            committed: false,
        })
    }

    async fn invalidate(&self, key: &CacheKey) {
        let key_hash = key_hash(key);
        let dir = self.root.join(&key_hash);
        // Prune the index before removing files; the whole directory goes at once, so the
        // per-variant eviction listener would be redundant (it skips explicit removals).
        if let Ok(paths) = fs_shims::read_dir_paths(&dir).await {
            for path in paths {
                if let Some(variant_hash) = path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .and_then(|name| name.strip_suffix(META_SUFFIX))
                {
                    self.index.invalidate(&VariantId {
                        key_hash: key_hash.clone(),
                        variant_hash: variant_hash.to_string(),
                    });
                }
            }
        }
        let _ = fs_shims::remove_dir_all(&dir).await;
    }
}

/// Streaming [`PutHandle`] for [`FileSystemStorage`].
///
/// Body bytes are written to a temporary file as they arrive; [`finalize`][Self::finalize]
/// renames the body into place and writes the metadata sidecar. Dropping without finalizing
/// removes the temporary body and stores nothing.
pub struct FsPutHandle {
    writer: fs_shims::Writer,
    body_tmp: PathBuf,
    body_final: PathBuf,
    meta_tmp: PathBuf,
    meta_final: PathBuf,
    policy: CachePolicy,
    index: Cache<VariantId, u64>,
    variant_id: VariantId,
    written: u64,
    committed: bool,
}

impl Debug for FsPutHandle {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FsPutHandle")
            .field("body_final", &self.body_final)
            .finish_non_exhaustive()
    }
}

impl AsyncWrite for FsPutHandle {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.writer).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = &poll {
            this.written += *n as u64;
        }
        poll
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().writer).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().writer).poll_close(cx)
    }
}

impl PutHandle for FsPutHandle {
    async fn finalize(mut self, trailers: Option<Headers>) -> io::Result<()> {
        self.writer.close().await?;
        fs_shims::rename(&self.body_tmp, &self.body_final).await?;

        let meta = StoredMeta {
            policy: PolicyRepr::from(&self.policy),
            trailers,
        };
        let bytes = serialize_meta(&meta)?;
        fs_shims::write(&self.meta_tmp, &bytes).await?;
        fs_shims::rename(&self.meta_tmp, &self.meta_final).await?;

        // Account the committed body against the cap. Re-inserting the same variant replaces
        // its prior weight; the eviction listener ignores the replacement.
        self.index.insert(self.variant_id.clone(), self.written);

        self.committed = true;
        Ok(())
    }
}

impl Drop for FsPutHandle {
    fn drop(&mut self) {
        if !self.committed {
            let _ = std::fs::remove_file(&self.body_tmp);
        }
    }
}

/// One stored response returned by [`FileSystemStorage::get`].
///
/// Holds the metadata; the body stays on disk until [`open`][Self::open] streams it. `Clone`
/// copies the metadata and re-opens the body file on demand.
#[derive(Clone)]
pub struct FsStoredEntry {
    meta_path: PathBuf,
    body_path: PathBuf,
    policy: CachePolicy,
    trailers: Option<Headers>,
}

impl Debug for FsStoredEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FsStoredEntry")
            .field("body_path", &self.body_path)
            .field("has_trailers", &self.trailers.is_some())
            .finish_non_exhaustive()
    }
}

impl StoredEntry for FsStoredEntry {
    fn policy(&self) -> &CachePolicy {
        &self.policy
    }

    async fn refresh_policy(&mut self, new_policy: CachePolicy) -> io::Result<()> {
        let meta = StoredMeta {
            policy: PolicyRepr::from(&new_policy),
            trailers: self.trailers.clone(),
        };
        let bytes = serialize_meta(&meta)?;
        let tmp = temp_sibling(&self.meta_path);
        fs_shims::write(&tmp, &bytes).await?;
        fs_shims::rename(&tmp, &self.meta_path).await?;

        self.policy = new_policy;
        Ok(())
    }

    async fn open(self) -> io::Result<Body> {
        let len = fs_shims::metadata_len(&self.body_path).await?;
        let reader = fs_shims::open(&self.body_path).await?;
        let source = FsBodySource {
            reader,
            trailers: self.trailers,
        };
        Ok(Body::new_with_trailers(source, Some(len)))
    }
}

// BodySource over a stored body file. Reads stream straight from the file; trailers surface
// after EOF.
struct FsBodySource {
    reader: fs_shims::Reader,
    trailers: Option<Headers>,
}

impl AsyncRead for FsBodySource {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().reader).poll_read(cx, buf)
    }
}

impl BodySource for FsBodySource {
    fn trailers(self: Pin<&mut Self>) -> Option<Headers> {
        self.get_mut().trailers.take()
    }
}

fn hash_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    finalize_hex(hasher)
}

fn key_hash(key: &CacheKey) -> String {
    hash_hex(key.to_string().as_bytes())
}

fn variant_hash(policy: &CachePolicy) -> String {
    let mut hasher = Sha256::new();
    for (name, value) in &policy.vary_snapshot {
        hasher.update(name.as_bytes());
        hasher.update([0]);
        match value {
            Some(value) => {
                hasher.update([1]);
                hasher.update(value.as_bytes());
            }
            None => hasher.update([0]),
        }
        hasher.update([0]);
    }
    finalize_hex(hasher)
}

fn finalize_hex(hasher: Sha256) -> String {
    let digest = hasher.finalize();
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(out, "{byte:02x}").expect("writing to a String cannot fail");
    }
    out
}

// A unique sibling temp path for atomically rewriting `path`.
fn temp_sibling(path: &Path) -> PathBuf {
    let n = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut name = path.as_os_str().to_owned();
    name.push(format!(".tmp.{n}"));
    PathBuf::from(name)
}

fn serialize_meta(meta: &StoredMeta) -> io::Result<rkyv::util::AlignedVec> {
    rkyv::to_bytes::<rkyv::rancor::Error>(meta)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

fn deserialize_meta(bytes: &[u8]) -> io::Result<StoredMeta> {
    // A disk read lands in a buffer aligned only to 1, but rkyv's validated access requires
    // the archived root to be aligned; copy into an `AlignedVec` before decoding.
    let mut aligned = rkyv::util::AlignedVec::<16>::new();
    aligned.extend_from_slice(bytes);
    rkyv::from_bytes::<StoredMeta, rkyv::rancor::Error>(&aligned)
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::*;
    use futures_lite::{AsyncReadExt, AsyncWriteExt};
    use std::time::{Duration, SystemTime};
    use tempfile::TempDir;
    use trillium_client::Conn;
    use trillium_http::{KnownHeaderName::*, Method, Status};
    use trillium_testing::{TestResult, harness, test};

    fn key() -> CacheKey {
        CacheKey::new(Method::Get, "http://example.com/".parse().unwrap())
    }

    fn new_storage() -> (TempDir, FileSystemStorage) {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileSystemStorage::new(dir.path());
        (dir, storage)
    }

    async fn store_at(storage: &FileSystemStorage, url: &str, body: &[u8]) {
        let key = CacheKey::new(Method::Get, url.parse().unwrap());
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

    async fn store(storage: &FileSystemStorage, key: CacheKey, conn: &Conn, body: &[u8]) {
        let policy = policy_from(conn, SystemTime::now(), private_cache());
        let mut handle = storage.put(key, policy).await.unwrap();
        handle.write_all(body).await.unwrap();
        handle.finalize(None).await.unwrap();
    }

    async fn read_body(entry: FsStoredEntry) -> Vec<u8> {
        let mut body = entry.open().await.unwrap();
        let mut buf = Vec::new();
        body.read_to_end(&mut buf).await.unwrap();
        buf
    }

    #[test(harness)]
    async fn get_missing_key_returns_empty() -> TestResult {
        let (_dir, storage) = new_storage();
        assert!(storage.get(&key()).await.is_empty());
        Ok(())
    }

    #[test(harness)]
    async fn put_then_get_round_trips_through_disk() -> TestResult {
        let (_dir, storage) = new_storage();
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600")],
        );
        store(&storage, key(), &conn, b"hello").await;
        let result = storage.get(&key()).await;
        assert_eq!(result.len(), 1);
        assert_eq!(read_body(result[0].clone()).await, b"hello");
        Ok(())
    }

    #[test(harness)]
    async fn put_with_same_vary_replaces() -> TestResult {
        let (_dir, storage) = new_storage();
        let conn = exchange(
            Method::Get,
            &[(AcceptEncoding, "gzip")],
            Status::Ok,
            &[(CacheControl, "max-age=600"), (Vary, "Accept-Encoding")],
        );
        store(&storage, key(), &conn, b"v1").await;
        store(&storage, key(), &conn, b"v2").await;
        let result = storage.get(&key()).await;
        assert_eq!(result.len(), 1);
        assert_eq!(read_body(result[0].clone()).await, b"v2");
        Ok(())
    }

    #[test(harness)]
    async fn put_with_different_vary_appends() -> TestResult {
        let (_dir, storage) = new_storage();
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
        store(&storage, key(), &gzip, b"gz").await;
        store(&storage, key(), &br, b"br").await;
        assert_eq!(storage.get(&key()).await.len(), 2);
        Ok(())
    }

    #[test(harness)]
    async fn invalidate_removes_all_entries_for_key() -> TestResult {
        let (_dir, storage) = new_storage();
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600")],
        );
        store(&storage, key(), &conn, b"x").await;
        storage.invalidate(&key()).await;
        assert!(storage.get(&key()).await.is_empty());
        Ok(())
    }

    #[test(harness)]
    async fn invalidate_does_not_touch_other_keys() -> TestResult {
        let (_dir, storage) = new_storage();
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600")],
        );
        let key_a = CacheKey::new(Method::Get, "http://a.example/".parse().unwrap());
        let key_b = CacheKey::new(Method::Get, "http://b.example/".parse().unwrap());
        store(&storage, key_a.clone(), &conn, b"a").await;
        store(&storage, key_b.clone(), &conn, b"b").await;
        storage.invalidate(&key_a).await;
        assert!(storage.get(&key_a).await.is_empty());
        assert_eq!(storage.get(&key_b).await.len(), 1);
        Ok(())
    }

    #[test(harness)]
    async fn drop_put_handle_without_finalize_discards() -> TestResult {
        let (_dir, storage) = new_storage();
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
    async fn refresh_policy_updates_meta_and_keeps_body() -> TestResult {
        let (_dir, storage) = new_storage();
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600")],
        );
        store(&storage, key(), &conn, b"body").await;

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
        assert_eq!(read_body(fresh[0].clone()).await, b"body");
        Ok(())
    }

    #[test(harness)]
    async fn trailers_round_trip() -> TestResult {
        let (_dir, storage) = new_storage();
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600")],
        );
        let policy = policy_from(&conn, SystemTime::now(), private_cache());
        let mut handle = storage.put(key(), policy).await.unwrap();
        handle.write_all(b"data").await.unwrap();
        let mut trailers = Headers::new();
        trailers.insert("x-checksum", "abc123");
        handle.finalize(Some(trailers)).await.unwrap();

        let entry = storage.get(&key()).await.remove(0);
        let mut body = entry.open().await.unwrap();
        let mut buf = Vec::new();
        body.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf, b"data");
        let trailers = body
            .trailers()
            .expect("stored trailers should surface after EOF");
        assert_eq!(trailers.get_str("x-checksum"), Some("abc123"));
        Ok(())
    }

    #[test(harness)]
    async fn persists_across_new_storage_on_same_root() -> TestResult {
        let dir = tempfile::tempdir().unwrap();
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600")],
        );
        {
            let storage = FileSystemStorage::new(dir.path());
            store(&storage, key(), &conn, b"persisted").await;
        }

        // A brand-new storage over the same directory sees the prior instance's entry.
        let reopened = FileSystemStorage::new(dir.path());
        let result = reopened.get(&key()).await;
        assert_eq!(result.len(), 1);
        assert_eq!(read_body(result[0].clone()).await, b"persisted");
        Ok(())
    }

    #[test(harness)]
    async fn size_cap_evicts_and_deletes_files() -> TestResult {
        // Cap at 1 KiB; write ten 600-byte bodies under distinct URLs.
        let dir = tempfile::tempdir().unwrap();
        let storage = FileSystemStorage::new(dir.path()).with_max_capacity_bytes(1024);
        let body = vec![b'x'; 600];
        for i in 0..10 {
            store_at(&storage, &format!("http://example.com/{i}"), &body).await;
        }
        storage.run_pending_tasks().await;
        assert!(
            storage.weighted_size() <= 1024,
            "weighted size {} should be within cap of 1024",
            storage.weighted_size()
        );

        // A fresh unbounded scan of the same root reflects only the files still on disk, so
        // the low total proves evicted variants' files were actually deleted, not just
        // forgotten by the index.
        let reopened = FileSystemStorage::new(dir.path()).unbounded();
        assert!(
            reopened.weighted_size() <= 1024,
            "on-disk bytes {} should be within cap of 1024",
            reopened.weighted_size()
        );
        Ok(())
    }

    #[test(harness)]
    async fn rebuild_scan_trims_over_cap_directory() -> TestResult {
        let dir = tempfile::tempdir().unwrap();
        let body = vec![b'x'; 600];
        {
            let unbounded = FileSystemStorage::new(dir.path()).unbounded();
            for i in 0..10 {
                store_at(&unbounded, &format!("http://example.com/{i}"), &body).await;
            }
            unbounded.run_pending_tasks().await;
            assert_eq!(unbounded.entry_count(), 10);
        }

        // Reopening with a cap trims the pre-existing directory to fit during construction.
        let capped = FileSystemStorage::new(dir.path()).with_max_capacity_bytes(1024);
        assert!(
            capped.weighted_size() <= 1024,
            "weighted size {} should be within cap of 1024",
            capped.weighted_size()
        );
        Ok(())
    }

    #[test(harness)]
    async fn unbounded_keeps_all_entries() -> TestResult {
        let dir = tempfile::tempdir().unwrap();
        let storage = FileSystemStorage::new(dir.path()).unbounded();
        let body = vec![b'x'; 600];
        for i in 0..10 {
            store_at(&storage, &format!("http://example.com/{i}"), &body).await;
        }
        storage.run_pending_tasks().await;
        assert_eq!(storage.entry_count(), 10);
        assert_eq!(storage.weighted_size(), 6000);
        Ok(())
    }

    #[test(harness)]
    async fn replacing_a_variant_does_not_double_count() -> TestResult {
        let (_dir, storage) = new_storage();
        store_at(&storage, "http://example.com/", &vec![b'x'; 600]).await;
        store_at(&storage, "http://example.com/", &vec![b'y'; 300]).await;
        storage.run_pending_tasks().await;
        assert_eq!(storage.entry_count(), 1);
        assert_eq!(storage.weighted_size(), 300);
        Ok(())
    }

    // Generous margin (>2x the TTL) over the real clock keeps these timing tests robust under
    // loaded CI; blocking sleeps are fine in a test and advance moka's Instant-based expiry.
    #[test(harness)]
    async fn time_to_live_evicts_and_deletes_files() -> TestResult {
        let dir = tempfile::tempdir().unwrap();
        let storage =
            FileSystemStorage::new(dir.path()).with_time_to_live(Duration::from_millis(50));
        store_at(&storage, "http://example.com/", b"x").await;
        storage.run_pending_tasks().await;
        assert_eq!(storage.entry_count(), 1);

        std::thread::sleep(Duration::from_millis(120));
        storage.run_pending_tasks().await;
        assert_eq!(storage.entry_count(), 0);

        // A fresh scan of the same root proves the eviction listener deleted the files, rather
        // than the index merely forgetting them.
        let reopened = FileSystemStorage::new(dir.path()).unbounded();
        assert_eq!(reopened.entry_count(), 0);
        Ok(())
    }

    #[test(harness)]
    async fn time_to_idle_evicts_unread_entries() -> TestResult {
        let dir = tempfile::tempdir().unwrap();
        let storage =
            FileSystemStorage::new(dir.path()).with_time_to_idle(Duration::from_millis(50));
        store_at(&storage, "http://example.com/", b"x").await;
        storage.run_pending_tasks().await;
        assert_eq!(storage.entry_count(), 1);

        // No reads, so the entry sits idle past its TTI and is evicted with its files.
        std::thread::sleep(Duration::from_millis(120));
        storage.run_pending_tasks().await;
        assert_eq!(storage.entry_count(), 0);
        let reopened = FileSystemStorage::new(dir.path()).unbounded();
        assert_eq!(reopened.entry_count(), 0);
        Ok(())
    }
}
