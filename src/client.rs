//! Client-side cache handler.
//!
//! [`Cache`] wires [`CacheStorage`] + [`CachePolicy`] onto a `trillium-client` request
//! lifecycle. Feature-gated behind `client`.
//!
//! ## Position in the handler chain
//!
//! Add `Cache` *last* in the handler tuple:
//!
//! ```ignore
//! client.with_handler((Logger::new(), Cache::new(storage)));
//! ```
//!
//! Reasons:
//! - `run` runs in declared order; the cache should be the last `run` so it can short-circuit the
//!   network with a fresh hit.
//! - `after_response` runs in reverse declared order; the cache should be the first
//!   `after_response` so it can read the response body and replace it with a streaming tee before
//!   any other handler reads the (one-shot) network body.
//!
//! ## Streaming
//!
//! On miss, the cache installs a streaming tee between the origin response body and the
//! user — bytes flow to storage and the user concurrently. Trailers propagate to both.
//! The cap on stored body size is enforced mid-stream; if exceeded, the cache write is
//! aborted and the remainder of the body passes through unmodified.

use crate::{
    CacheKey, CacheOptions, CachePolicy, CacheStorage, PutHandle, StoredEntry,
    tee::TeeingReader,
    validation::{AfterResponse, BeforeRequest},
};
use futures_lite::{AsyncReadExt, AsyncWriteExt};
use std::{sync::Arc, time::SystemTime};
use trillium_client::{
    Body, Client, ClientHandler, Conn, ConnExt, Headers, KnownHeaderName, Method, ResponseBody,
    Result, Url,
};

const DEFAULT_MAX_CACHEABLE_SIZE: u64 = 16 * 1024 * 1024;

/// Cache handler. Mount on a [`trillium_client::Client`] together with
/// a [`CacheStorage`] backend.
///
/// `Cache` is cheap to `Clone`: storage is held in an `Arc`, so clones
/// share the same backend.
#[derive(Debug)]
pub struct Cache<S: CacheStorage> {
    storage: Arc<S>,
    options: CacheOptions,
    max_cacheable_size: u64,
}

impl<S: CacheStorage> Clone for Cache<S> {
    fn clone(&self) -> Self {
        Self {
            storage: Arc::clone(&self.storage),
            options: self.options,
            max_cacheable_size: self.max_cacheable_size,
        }
    }
}

impl<S: CacheStorage> Cache<S> {
    /// Construct a cache handler with default options
    /// ([`CacheOptions::default`]) and a 16 MiB body-size cap.
    pub fn new(storage: S) -> Self {
        Self {
            storage: Arc::new(storage),
            options: CacheOptions::default(),
            max_cacheable_size: DEFAULT_MAX_CACHEABLE_SIZE,
        }
    }

    /// Replace the cache options.
    pub fn with_options(mut self, options: CacheOptions) -> Self {
        self.options = options;
        self
    }

    /// Mark this cache as a *shared cache* (proxy/CDN). Equivalent to
    /// `with_options` with `shared: true`.
    pub fn shared(mut self) -> Self {
        self.options.shared = true;
        self
    }

    /// Set the cap on response body bytes the cache will store.
    /// Responses larger than this pass through but are not stored. If
    /// the cap is exceeded mid-stream, the cache write is aborted and
    /// the remainder of the body passes through unmodified.
    pub fn with_max_cacheable_size(mut self, max: u64) -> Self {
        self.max_cacheable_size = max;
        self
    }

    /// Borrow the storage backend.
    pub fn storage(&self) -> &S {
        &self.storage
    }
}

// State stashed in the conn's typeset by `run` for `after_response` to
// pick up.
enum CacheCtx<E: StoredEntry> {
    /// Cache hit — `run` already populated a synthetic response and
    /// halted. `after_response` is a no-op.
    Hit,
    /// Stored entry was stale and a conditional revalidation request
    /// has been spliced onto the conn. `after_response` reconciles the
    /// origin's reply (304 vs 200) with the stored entry.
    Revalidation { stored: E, key: CacheKey },
    /// Cache miss — no stored entry matched. If the response is
    /// storable, `after_response` will install a streaming tee.
    Miss { key: CacheKey },
    /// Unsafe method (POST/PUT/DELETE/...). On a non-error response,
    /// `after_response` invalidates the target URI per RFC 9111 §4.4.
    Unsafe { url: Url },
}

impl<E: StoredEntry> std::fmt::Debug for CacheCtx<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hit => f.write_str("Hit"),
            Self::Revalidation { key, .. } => f
                .debug_struct("Revalidation")
                .field("key", key)
                .finish_non_exhaustive(),
            Self::Miss { key } => f.debug_struct("Miss").field("key", key).finish(),
            Self::Unsafe { url } => f.debug_struct("Unsafe").field("url", url).finish(),
        }
    }
}

impl<S: CacheStorage> ClientHandler for Cache<S> {
    async fn run(&self, conn: &mut Conn) -> Result<()> {
        let method = conn.method();
        let key = CacheKey::new(method, conn.url().clone());
        log::trace!("cache: run {method} {}", conn.url());

        // RFC 9111 §4.4: don't read from cache for unsafe methods;
        // possibly invalidate after the round-trip.
        if !method.is_safe() {
            log::trace!("cache: unsafe method {method}, bypassing cache read");
            conn.insert_state(CacheCtx::<S::StoredEntry>::Unsafe {
                url: conn.url().clone(),
            });
            return Ok(());
        }

        let now = SystemTime::now();
        let entries = self.storage.get(&key).await;
        log::trace!("cache: {} stored candidate(s) for {key}", entries.len());

        for entry in entries {
            match entry.policy().before_request(conn.request_headers(), now) {
                BeforeRequest::Fresh(cached) => {
                    log::trace!("cache: hit (fresh) for {key}, serving cached response");
                    *conn.response_headers_mut() = cached.headers;
                    let body = match entry.open().await {
                        Ok(b) => b,
                        Err(e) => {
                            log::warn!(
                                "cache: open for hit failed for {key}: {e}, passing through"
                            );
                            // Reset the override; let the network round-trip happen.
                            return Ok(());
                        }
                    };
                    conn.set_status(cached.status)
                        .set_response_body(body)
                        .halt()
                        .insert_state(CacheCtx::<S::StoredEntry>::Hit);
                    return Ok(());
                }

                BeforeRequest::NotModified(cached) => {
                    log::trace!("cache: hit (fresh, conditional matches) for {key}, serving 304");
                    *conn.response_headers_mut() = cached.headers;
                    conn.set_status(cached.status)
                        .set_response_body(b"" as &[u8])
                        .halt()
                        .insert_state(CacheCtx::<S::StoredEntry>::Hit);
                    return Ok(());
                }

                BeforeRequest::Stale {
                    request_headers,
                    matches: true,
                } => {
                    // RFC 9111 §4.2.4 stale-while-revalidate: if the
                    // entry is within its SWR window, serve it
                    // immediately and revalidate in the background.
                    if entry.policy().is_swr_eligible(now) {
                        log::trace!(
                            "cache: stale-while-revalidate for {key}, serving stale + spawning \
                             background revalidation"
                        );
                        let entry_for_bg = entry.clone();
                        self.spawn_background_revalidation(
                            conn,
                            entry_for_bg,
                            key.clone(),
                            request_headers,
                        );
                        match self.serve_stale(conn, entry, now).await {
                            Ok(()) => {
                                conn.halt();
                                conn.insert_state(CacheCtx::<S::StoredEntry>::Hit);
                            }
                            Err(e) => {
                                log::warn!(
                                    "cache: open for stale serve failed for {key}: {e}, passing \
                                     through"
                                );
                            }
                        }
                        return Ok(());
                    }
                    // Otherwise fall through to synchronous revalidation.
                    log::trace!("cache: stale for {key}, sending conditional revalidation request");
                    *conn.request_headers_mut() = request_headers;
                    conn.insert_state(CacheCtx::Revalidation { stored: entry, key });
                    return Ok(());
                }

                BeforeRequest::Stale { matches: false, .. } => {
                    log::trace!("cache: candidate vary-mismatch for {key}, trying next");
                    continue;
                }
            }
        }

        log::trace!("cache: miss for {key}, forwarding to origin");
        conn.insert_state(CacheCtx::<S::StoredEntry>::Miss { key });
        Ok(())
    }

    async fn after_response(&self, conn: &mut Conn) -> Result<()> {
        let Some(ctx) = conn.take_state::<CacheCtx<S::StoredEntry>>() else {
            log::trace!("cache: after_response with no CacheCtx, nothing to do");
            return Ok(());
        };

        // RFC 9111 §4.2.4 / RFC 5861 stale-if-error: if revalidation hit a transport-level
        // failure or a 5xx, and the stored entry is SIE-eligible, serve it instead.
        if let CacheCtx::Revalidation { ref stored, .. } = ctx {
            let now = SystemTime::now();
            let origin_failed =
                conn.error().is_some() || conn.status().is_some_and(|s| s.is_server_error());
            if origin_failed && stored.policy().is_sie_eligible(now) {
                log::trace!(
                    "cache: stale-if-error recovery for {} (origin error/{:?}), serving stale",
                    conn.url(),
                    conn.status()
                );
                if let Err(e) = self.serve_stale(conn, stored.clone(), now).await {
                    log::warn!(
                        "cache: open for stale serve failed for {}: {e}, propagating error",
                        conn.url()
                    );
                    return Ok(());
                }
                conn.take_error();
                return Ok(());
            }
        }

        if conn.status().is_none() {
            log::trace!(
                "cache: transport error with no SIE recovery for {}, propagating",
                conn.url()
            );
            return Ok(());
        }

        match ctx {
            CacheCtx::Hit => {
                log::trace!("cache: hit confirmed in after_response for {}", conn.url());
                Ok(())
            }
            CacheCtx::Revalidation { stored, key } => {
                self.handle_revalidation(conn, stored, key).await
            }
            CacheCtx::Miss { key } => self.handle_miss(conn, key).await,
            CacheCtx::Unsafe { url } => {
                let status = conn.status().expect("checked above");
                if status.is_success() || status.is_redirection() {
                    log::trace!(
                        "cache: unsafe method {} → {}, invalidating GET and HEAD entries for {url}",
                        conn.method(),
                        status
                    );
                    self.invalidate_url(&url).await;

                    for header in [KnownHeaderName::Location, KnownHeaderName::ContentLocation] {
                        let Some(value) = conn.response_headers().get_str(header) else {
                            continue;
                        };
                        let Ok(target) = url.join(value) else {
                            log::trace!(
                                "cache: unsafe method secondary invalidation: {header} value \
                                 {value:?} did not resolve, skipping"
                            );
                            continue;
                        };
                        if target.host_str() != url.host_str() {
                            log::trace!(
                                "cache: unsafe method secondary invalidation: {header} target \
                                 {target} differs in host from request URL, skipping (§4.4 DoS \
                                 guard)"
                            );
                            continue;
                        }
                        log::trace!(
                            "cache: unsafe method secondary invalidation via {header}: {target}"
                        );
                        self.invalidate_url(&target).await;
                    }
                } else {
                    log::trace!(
                        "cache: unsafe method {} → {} for {url}, no invalidation",
                        conn.method(),
                        status
                    );
                }
                Ok(())
            }
        }
    }
}

impl<S: CacheStorage> Cache<S> {
    // §4.4: invalidate any stored entries for this URI under the methods
    // we'd ever cache (GET and HEAD).
    async fn invalidate_url(&self, url: &Url) {
        self.storage
            .invalidate(&CacheKey::new(Method::Get, url.clone()))
            .await;
        self.storage
            .invalidate(&CacheKey::new(Method::Head, url.clone()))
            .await;
    }

    // RFC 9111 §4.2.4 / RFC 5861: apply a stored stale entry to the
    // conn as the served response. Used by both stale-while-revalidate
    // and stale-if-error paths.
    async fn serve_stale(
        &self,
        conn: &mut Conn,
        stored: S::StoredEntry,
        now: SystemTime,
    ) -> std::io::Result<()> {
        let cached = stored.policy().cached_response(now);
        let body = stored.open().await?;
        conn.set_status(cached.status);
        *conn.response_headers_mut() = cached.headers;
        conn.set_response_body(body);
        Ok(())
    }

    // RFC 9111 §4.2.4: spawn a background revalidation so the user gets
    // an immediate stale response while the cache refreshes.
    //
    // We share the runtime + connector + pool with the user's client
    // (cloning `conn.client()` is cheap — the underlying pools are
    // Arc-shared). The bypass client has its handler stack replaced
    // with `()` so the cache handler doesn't recurse on itself.
    fn spawn_background_revalidation(
        &self,
        conn: &Conn,
        stored: S::StoredEntry,
        key: CacheKey,
        request_headers: Headers,
    ) {
        let runtime = conn.client().connector().runtime();
        let bypass_client = conn.client().clone().with_handler(());
        let cache = self.clone();
        let method = conn.method();
        let url = conn.url().clone();
        log::trace!("cache: spawning background revalidation for {key}");

        let _detached = runtime.spawn(async move {
            cache
                .background_revalidation(bypass_client, method, url, request_headers, stored, key)
                .await;
        });
    }

    async fn background_revalidation(
        self,
        client: Client,
        method: Method,
        url: Url,
        request_headers: Headers,
        mut stored: S::StoredEntry,
        key: CacheKey,
    ) {
        let mut new_conn = client.build_conn(method, url);
        *new_conn.request_headers_mut() = request_headers;

        if let Err(e) = (&mut new_conn).await {
            log::trace!(
                "cache: background revalidation transport error for {key} ({e}), leaving stored \
                 entry"
            );
            return;
        }

        let now = SystemTime::now();
        let new_status = new_conn
            .status()
            .expect("background revalidation: response not yet received");
        match stored.policy().after_response(
            new_conn.request_headers(),
            new_status,
            new_conn.response_headers(),
            now,
        ) {
            AfterResponse::NotModified(new_policy, _) => {
                log::trace!("cache: background revalidation 304 for {key}, refreshing entry");
                if let Err(e) = stored.refresh_policy(new_policy).await {
                    log::warn!("cache: background refresh_policy failed for {key}: {e}");
                }
            }
            AfterResponse::Modified => {
                let new_request_method = new_conn.method();
                let new_request_headers = new_conn.request_headers().clone();
                let new_response_headers = new_conn.response_headers().clone();
                if !CachePolicy::is_storable(
                    new_request_method,
                    &new_request_headers,
                    new_status,
                    &new_response_headers,
                    &self.options,
                ) {
                    log::trace!(
                        "cache: background revalidation 200 for {key}, response not storable, \
                         dropping"
                    );
                    return;
                }
                let new_policy = CachePolicy::new(
                    new_request_method,
                    &new_request_headers,
                    new_status,
                    new_response_headers,
                    now,
                    self.options,
                );
                let put_handle = match self.storage.put(key.clone(), new_policy).await {
                    Ok(h) => h,
                    Err(e) => {
                        log::warn!(
                            "cache: background put({key}) failed: {e}, leaving stored entry"
                        );
                        return;
                    }
                };
                let Some(body) = new_conn.take_response_body() else {
                    log::trace!(
                        "cache: background revalidation 200 for {key}, no body, leaving stored \
                         entry"
                    );
                    return;
                };
                if let Err(e) = copy_into_storage(body, put_handle, self.max_cacheable_size).await {
                    log::warn!(
                        "cache: background copy into storage failed for {key}: {e}, leaving \
                         stored entry"
                    );
                }
            }
        }
    }

    async fn handle_revalidation(
        &self,
        conn: &mut Conn,
        mut stored: S::StoredEntry,
        key: CacheKey,
    ) -> Result<()> {
        let now = SystemTime::now();
        let new_status = conn.status().expect("checked above");
        match stored.policy().after_response(
            conn.request_headers(),
            new_status,
            conn.response_headers(),
            now,
        ) {
            AfterResponse::NotModified(new_policy, cached_response) => {
                log::trace!(
                    "cache: revalidation 304 for {key}, reusing stored body and refreshing entry"
                );
                if let Err(e) = stored.refresh_policy(new_policy).await {
                    log::warn!("cache: refresh_policy failed for {key}: {e}");
                }
                let body = match stored.open().await {
                    Ok(b) => b,
                    Err(e) => {
                        log::warn!("cache: open after 304 failed for {key}: {e}, passing through");
                        return Ok(());
                    }
                };
                conn.set_status(cached_response.status);
                *conn.response_headers_mut() = cached_response.headers;
                conn.set_response_body(body);
                Ok(())
            }
            AfterResponse::Modified => {
                // Drop the stored entry; treat as a fresh miss against the same key. The new
                // entry replaces any stored variant with the same Vary signature.
                drop(stored);
                self.handle_miss(conn, key).await
            }
        }
    }

    async fn handle_miss(&self, conn: &mut Conn, key: CacheKey) -> Result<()> {
        let status = conn.status().expect("checked above");
        if !CachePolicy::is_storable(
            conn.method(),
            conn.request_headers(),
            status,
            conn.response_headers(),
            &self.options,
        ) {
            log::trace!("cache: miss for {key}, response not storable, passing through");
            return Ok(());
        }

        // Skip the put entirely when content-length is known and already over cap.
        if let Some(len) = conn
            .response_headers()
            .get_str(KnownHeaderName::ContentLength)
            .and_then(|s| s.parse::<u64>().ok())
            && len > self.max_cacheable_size
        {
            log::trace!(
                "cache: miss for {key}, body {len} > max {}, not caching",
                self.max_cacheable_size
            );
            return Ok(());
        }

        let policy = CachePolicy::new(
            conn.method(),
            conn.request_headers(),
            status,
            conn.response_headers().clone(),
            SystemTime::now(),
            self.options,
        );
        let put_handle = match self.storage.put(key.clone(), policy).await {
            Ok(h) => h,
            Err(e) => {
                log::warn!("cache: put({key}) failed: {e}, passing through");
                return Ok(());
            }
        };

        let Some(response_body) = conn.take_response_body() else {
            log::trace!("cache: miss for {key}, no body, passing through");
            return Ok(());
        };
        let len = response_body.content_length();
        // Strip wire-format chunked framing so the tee stores the decoded body. The outer
        // body re-frames for the downstream when `len` is None.
        let upstream = Body::new_with_trailers(response_body, len).without_chunked_framing();
        log::trace!("cache: miss for {key}, streaming through tee");
        let tee = TeeingReader::new(upstream, put_handle, self.max_cacheable_size);
        conn.set_response_body(Body::new_with_trailers(tee, len));
        Ok(())
    }
}

// Copy a response body into a put handle, finalizing on EOF with whatever trailers the body
// exposes. Used by background revalidation, where there's no concurrent user reader; the cap
// is enforced by aborting when exceeded.
async fn copy_into_storage<P: PutHandle>(
    body: ResponseBody<'static>,
    mut put: P,
    cap: u64,
) -> std::io::Result<()> {
    let len = body.content_length();
    // Strip wire-format chunked framing so storage gets the decoded body, not chunk bytes.
    let mut body = Body::new_with_trailers(body, len).without_chunked_framing();
    let mut buf = [0u8; 8192];
    let mut total: u64 = 0;
    loop {
        let n = body.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        total = total.saturating_add(n as u64);
        if total > cap {
            // Drop put_handle without finalizing — storage gets nothing.
            drop(put);
            log::trace!("cache: background copy exceeded cap {cap}, aborting cache write");
            return Ok(());
        }
        put.write_all(&buf[..n]).await?;
    }
    let trailers = body.trailers();
    put.finalize(trailers).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryStorage;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use trillium::{Conn as ServerConn, Handler as ServerHandler, KnownHeaderName, Status};
    use trillium_client::Client;
    use trillium_testing::{ServerConnector, TestResult, harness, test};

    #[derive(Debug, Clone)]
    struct CountingServer {
        counter: Arc<AtomicUsize>,
        cache_control: &'static str,
        etag: Option<&'static str>,
    }

    impl CountingServer {
        fn new(cache_control: &'static str) -> Self {
            Self {
                counter: Arc::new(AtomicUsize::new(0)),
                cache_control,
                etag: None,
            }
        }

        fn with_etag(mut self, etag: &'static str) -> Self {
            self.etag = Some(etag);
            self
        }
    }

    impl ServerHandler for CountingServer {
        async fn run(&self, conn: ServerConn) -> ServerConn {
            let n = self.counter.fetch_add(1, Ordering::SeqCst);

            if let Some(etag) = self.etag {
                if conn.request_headers().get_str(KnownHeaderName::IfNoneMatch) == Some(etag) {
                    return conn
                        .with_status(Status::NotModified)
                        .with_response_header(KnownHeaderName::Etag, etag)
                        .halt();
                }
            }

            let mut conn = conn
                .with_response_header(KnownHeaderName::CacheControl, self.cache_control)
                .ok(format!("body-{n}"));
            if let Some(etag) = self.etag {
                conn.response_headers_mut()
                    .insert(KnownHeaderName::Etag, etag);
            }
            conn
        }
    }

    fn cache_client(server: CountingServer) -> (Client, Arc<AtomicUsize>) {
        let counter = server.counter.clone();
        let client = Client::new(ServerConnector::new(server))
            .with_handler(Cache::new(InMemoryStorage::new()));
        (client, counter)
    }

    #[test(harness)]
    async fn first_request_misses_subsequent_request_hits() -> TestResult {
        let (client, counter) = cache_client(CountingServer::new("max-age=600"));

        let mut r1 = client.get("http://example.com/x").await?;
        assert_eq!(r1.status(), Some(Status::Ok));
        assert_eq!(r1.response_body().read_string().await?, "body-0");

        let mut r2 = client.get("http://example.com/x").await?;
        assert_eq!(r2.status(), Some(Status::Ok));
        assert_eq!(r2.response_body().read_string().await?, "body-0");
        assert_eq!(counter.load(Ordering::SeqCst), 1, "server only hit once");
        Ok(())
    }

    #[test(harness)]
    async fn different_urls_dont_collide() -> TestResult {
        let (client, counter) = cache_client(CountingServer::new("max-age=600"));

        let mut r1 = client.get("http://example.com/a").await?;
        let mut r2 = client.get("http://example.com/b").await?;
        assert_eq!(r1.response_body().read_string().await?, "body-0");
        assert_eq!(r2.response_body().read_string().await?, "body-1");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[test(harness)]
    async fn no_store_response_is_not_cached() -> TestResult {
        let (client, counter) = cache_client(CountingServer::new("no-store"));

        let mut r1 = client.get("http://example.com/x").await?;
        assert_eq!(r1.response_body().read_string().await?, "body-0");

        let mut r2 = client.get("http://example.com/x").await?;
        assert_eq!(r2.response_body().read_string().await?, "body-1");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[test(harness)]
    async fn post_invalidates_existing_entry() -> TestResult {
        let (client, counter) = cache_client(CountingServer::new("max-age=600"));

        let mut r1 = client.get("http://example.com/x").await?;
        assert_eq!(r1.response_body().read_string().await?, "body-0");

        let _ = client.post("http://example.com/x").await?;

        let mut r3 = client.get("http://example.com/x").await?;
        assert_eq!(r3.response_body().read_string().await?, "body-2");
        assert_eq!(counter.load(Ordering::SeqCst), 3);
        Ok(())
    }

    #[test(harness)]
    async fn post_invalidates_location_and_content_location_targets() -> TestResult {
        #[derive(Debug, Clone, Default)]
        struct LclServer(Arc<AtomicUsize>);
        impl ServerHandler for LclServer {
            async fn run(&self, conn: ServerConn) -> ServerConn {
                let n = self.0.fetch_add(1, Ordering::SeqCst);
                if conn.method() == Method::Post {
                    conn.with_response_header(KnownHeaderName::Location, "/loc")
                        .with_response_header(KnownHeaderName::ContentLocation, "/cl")
                        .ok(format!("post-body-{n}"))
                } else {
                    conn.with_response_header(KnownHeaderName::CacheControl, "max-age=600")
                        .ok(format!("get-body-{n}"))
                }
            }
        }

        let server = LclServer::default();
        let counter = Arc::clone(&server.0);
        let client = Client::new(ServerConnector::new(server))
            .with_handler(Cache::new(InMemoryStorage::new()));

        // Read each body so the streaming tee actually commits to storage.
        let mut loc = client.get("http://example.com/loc").await?;
        let _ = loc.response_body().read_string().await?;
        let mut cl = client.get("http://example.com/cl").await?;
        let _ = cl.response_body().read_string().await?;
        assert_eq!(counter.load(Ordering::SeqCst), 2);

        let _ = client.post("http://example.com/anything").await?;

        let _ = client.get("http://example.com/loc").await?;
        let _ = client.get("http://example.com/cl").await?;
        assert_eq!(
            counter.load(Ordering::SeqCst),
            5,
            "POST + 2 re-fetches should hit the origin again"
        );
        Ok(())
    }

    #[test(harness)]
    async fn cross_host_location_does_not_invalidate() -> TestResult {
        #[derive(Debug, Clone, Default)]
        struct CrossHostServer(Arc<AtomicUsize>);
        impl ServerHandler for CrossHostServer {
            async fn run(&self, conn: ServerConn) -> ServerConn {
                let n = self.0.fetch_add(1, Ordering::SeqCst);
                if conn.method() == Method::Post {
                    conn.with_response_header(KnownHeaderName::Location, "http://other.example/loc")
                        .ok(format!("post-{n}"))
                } else {
                    conn.with_response_header(KnownHeaderName::CacheControl, "max-age=600")
                        .ok(format!("get-{n}"))
                }
            }
        }

        let server = CrossHostServer::default();
        let counter = Arc::clone(&server.0);
        let client = Client::new(ServerConnector::new(server))
            .with_handler(Cache::new(InMemoryStorage::new()));

        // Read the body to drive the tee into storage. Streaming-cache contract: nothing is
        // cached unless the body is read.
        let mut populating = client.get("http://other.example/loc").await?;
        let _ = populating.response_body().read_string().await?;
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        let _ = client.post("http://example.com/anything").await?;

        let mut r = client.get("http://other.example/loc").await?;
        assert_eq!(r.response_body().read_string().await?, "get-0");
        assert_eq!(
            counter.load(Ordering::SeqCst),
            2,
            "no extra GET to other.example"
        );
        Ok(())
    }

    #[test(harness)]
    async fn stale_with_etag_revalidates_to_304() -> TestResult {
        let (client, counter) = cache_client(CountingServer::new("max-age=0").with_etag(r#""v1""#));

        let mut r1 = client.get("http://example.com/x").await?;
        assert_eq!(r1.response_body().read_string().await?, "body-0");
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        let mut r2 = client.get("http://example.com/x").await?;
        assert_eq!(r2.status(), Some(Status::Ok));
        assert_eq!(r2.response_body().read_string().await?, "body-0");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[test(harness)]
    async fn stale_with_mismatching_etag_replaces_body() -> TestResult {
        #[derive(Debug, Clone)]
        struct AlwaysFresh {
            counter: Arc<AtomicUsize>,
        }
        impl ServerHandler for AlwaysFresh {
            async fn run(&self, conn: ServerConn) -> ServerConn {
                let n = self.counter.fetch_add(1, Ordering::SeqCst);
                conn.with_response_header(KnownHeaderName::CacheControl, "max-age=0")
                    .with_response_header(KnownHeaderName::Etag, r#""rolling""#)
                    .ok(format!("body-{n}"))
            }
        }
        let counter = Arc::new(AtomicUsize::new(0));
        let server = AlwaysFresh {
            counter: counter.clone(),
        };
        let client = Client::new(ServerConnector::new(server))
            .with_handler(Cache::new(InMemoryStorage::new()));

        let mut r1 = client.get("http://example.com/x").await?;
        assert_eq!(r1.response_body().read_string().await?, "body-0");

        let mut r2 = client.get("http://example.com/x").await?;
        assert_eq!(r2.response_body().read_string().await?, "body-1");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[test(harness)]
    async fn vary_isolates_entries_by_request_header() -> TestResult {
        #[derive(Debug, Clone)]
        struct VaryServer {
            counter: Arc<AtomicUsize>,
        }
        impl ServerHandler for VaryServer {
            async fn run(&self, conn: ServerConn) -> ServerConn {
                self.counter.fetch_add(1, Ordering::SeqCst);
                let ae = conn
                    .request_headers()
                    .get_str(KnownHeaderName::AcceptEncoding)
                    .unwrap_or("none")
                    .to_string();
                conn.with_response_header(KnownHeaderName::CacheControl, "max-age=600")
                    .with_response_header(KnownHeaderName::Vary, "Accept-Encoding")
                    .ok(format!("body-for-{ae}"))
            }
        }
        let counter = Arc::new(AtomicUsize::new(0));
        let server = VaryServer {
            counter: counter.clone(),
        };
        let client = Client::new(ServerConnector::new(server))
            .with_handler(Cache::new(InMemoryStorage::new()));

        let mut r1 = client
            .get("http://example.com/x")
            .with_request_header(KnownHeaderName::AcceptEncoding, "gzip")
            .await?;
        assert_eq!(r1.response_body().read_string().await?, "body-for-gzip");

        let mut r2 = client
            .get("http://example.com/x")
            .with_request_header(KnownHeaderName::AcceptEncoding, "br")
            .await?;
        assert_eq!(r2.response_body().read_string().await?, "body-for-br");

        let mut r3 = client
            .get("http://example.com/x")
            .with_request_header(KnownHeaderName::AcceptEncoding, "gzip")
            .await?;
        assert_eq!(r3.response_body().read_string().await?, "body-for-gzip");

        assert_eq!(counter.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[test(harness)]
    async fn oversized_body_is_served_but_not_cached() -> TestResult {
        let server = CountingServer::new("max-age=600");
        let counter = server.counter.clone();
        let client = Client::new(ServerConnector::new(server))
            .with_handler(Cache::new(InMemoryStorage::new()).with_max_cacheable_size(3));

        let mut r1 = client.get("http://example.com/x").await?;
        assert_eq!(r1.response_body().read_string().await?, "body-0");

        let mut r2 = client.get("http://example.com/x").await?;
        assert_eq!(r2.response_body().read_string().await?, "body-1");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        Ok(())
    }

    // A chunked (unknown-length) upstream body must be stored and replayed *decoded* — not as
    // raw chunk framing. Every other test here uses fixed-length bodies, which read raw and so
    // never exercised the framing path.
    #[test(harness)]
    async fn chunked_upstream_is_stored_and_replayed_decoded() -> TestResult {
        #[derive(Debug, Clone)]
        struct ChunkedServer {
            counter: Arc<AtomicUsize>,
        }
        impl ServerHandler for ChunkedServer {
            async fn run(&self, conn: ServerConn) -> ServerConn {
                self.counter.fetch_add(1, Ordering::SeqCst);
                // No known length -> the server frames this as Transfer-Encoding: chunked.
                let body = Body::new_streaming(
                    futures_lite::io::Cursor::new(b"chunked-body-content".to_vec()),
                    None,
                );
                conn.with_response_header(KnownHeaderName::CacheControl, "max-age=600")
                    .with_body(body)
                    .with_status(Status::Ok)
                    .halt()
            }
        }

        let counter = Arc::new(AtomicUsize::new(0));
        let server = ChunkedServer {
            counter: counter.clone(),
        };
        let client = Client::new(ServerConnector::new(server))
            .with_handler(Cache::new(InMemoryStorage::new()));

        // MISS: the pass-through must deliver the decoded body, not chunk framing.
        let mut r1 = client.get("http://example.com/x").await?;
        assert_eq!(
            r1.response_body().read_string().await?,
            "chunked-body-content"
        );

        // HIT: the stored copy must replay decoded, with a known content-length.
        let mut r2 = client.get("http://example.com/x").await?;
        assert_eq!(r2.status(), Some(Status::Ok));
        assert_eq!(
            r2.response_body().read_string().await?,
            "chunked-body-content"
        );
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "second request served from cache"
        );
        Ok(())
    }

    // ===== §4.2.4 / RFC 5861 stale-if-error =====

    use crate::test_helpers::exchange;
    use std::{io, net::SocketAddr};
    use trillium_client::{Connector, Url};

    /// Connector that always fails to connect. Used to drive the
    /// transport-error code path in `Conn::exec` for SIE tests.
    #[derive(Debug)]
    struct FailingConnector {
        inner: ServerConnector<Status>,
    }

    impl FailingConnector {
        fn new() -> Self {
            Self {
                inner: ServerConnector::new(Status::Ok),
            }
        }
    }

    impl Connector for FailingConnector {
        type Runtime = <ServerConnector<Status> as Connector>::Runtime;
        type Transport = <ServerConnector<Status> as Connector>::Transport;
        type Udp = <ServerConnector<Status> as Connector>::Udp;

        async fn connect(&self, _url: &Url) -> io::Result<Self::Transport> {
            Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "test failure",
            ))
        }

        fn runtime(&self) -> Self::Runtime {
            self.inner.runtime().clone()
        }

        async fn resolve(&self, host: &str, port: u16) -> io::Result<Vec<SocketAddr>> {
            self.inner.resolve(host, port).await
        }
    }

    /// Build a stale, SIE-eligible cache entry by hand and pre-populate
    /// `storage`. Returns the URL/key under which the entry was stored.
    async fn populate_stale_entry(
        storage: &InMemoryStorage,
        cache_control: &'static str,
        body: &'static [u8],
    ) -> CacheKey {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(KnownHeaderName::CacheControl, cache_control)],
        );
        let policy =
            crate::test_helpers::policy_from(&conn, SystemTime::now(), CacheOptions::default());
        let key = CacheKey::new(Method::Get, "http://example.com/x".parse().unwrap());
        let mut handle = storage.put(key.clone(), policy).await.unwrap();
        use futures_lite::AsyncWriteExt;
        handle.write_all(body).await.unwrap();
        handle.finalize(None).await.unwrap();
        key
    }

    #[test(harness)]
    async fn sie_serves_stale_on_transport_error() -> TestResult {
        let storage = InMemoryStorage::new();
        let _ =
            populate_stale_entry(&storage, "max-age=0, stale-if-error=3600", b"stale body").await;
        let client = Client::new(FailingConnector::new()).with_handler(Cache::new(storage));

        let mut conn = client.get("http://example.com/x").await?;
        assert_eq!(conn.status(), Some(Status::Ok));
        assert_eq!(conn.response_body().read_string().await?, "stale body");
        Ok(())
    }

    #[test(harness)]
    async fn no_sie_propagates_transport_error() -> TestResult {
        let storage = InMemoryStorage::new();
        let _ = populate_stale_entry(&storage, "max-age=0", b"stale body").await;
        let client = Client::new(FailingConnector::new()).with_handler(Cache::new(storage));

        let result = client.get("http://example.com/x").await;
        assert!(
            result.is_err(),
            "expected transport error to propagate, got {result:?}"
        );
        Ok(())
    }

    #[test(harness)]
    async fn sie_serves_stale_on_5xx() -> TestResult {
        let storage = InMemoryStorage::new();
        let _ =
            populate_stale_entry(&storage, "max-age=0, stale-if-error=3600", b"stale body").await;
        let server = ServerConnector::new(Status::ServiceUnavailable);
        let client = Client::new(server).with_handler(Cache::new(storage));

        let mut conn = client.get("http://example.com/x").await?;
        assert_eq!(conn.status(), Some(Status::Ok));
        assert_eq!(conn.response_body().read_string().await?, "stale body");
        Ok(())
    }

    #[test(harness)]
    async fn no_sie_serves_5xx_as_received() -> TestResult {
        let storage = InMemoryStorage::new();
        let _ = populate_stale_entry(&storage, "max-age=0", b"stale body").await;
        let server = ServerConnector::new(Status::ServiceUnavailable);
        let client = Client::new(server).with_handler(Cache::new(storage));

        let conn = client.get("http://example.com/x").await?;
        assert_eq!(conn.status(), Some(Status::ServiceUnavailable));
        Ok(())
    }

    // ===== §4.2.4 / RFC 5861 stale-while-revalidate =====

    use std::time::Duration;

    #[test(harness)]
    async fn swr_serves_stale_immediately_and_revalidates_in_background() -> TestResult {
        let storage = InMemoryStorage::new();
        let _ = populate_stale_entry(
            &storage,
            "max-age=0, stale-while-revalidate=3600",
            b"stale-body",
        )
        .await;

        let server = CountingServer::new("max-age=600");
        let counter = server.counter.clone();
        let client = Client::new(ServerConnector::new(server)).with_handler(Cache::new(storage));

        let mut conn = client.get("http://example.com/x").await?;
        assert_eq!(conn.status(), Some(Status::Ok));
        assert_eq!(conn.response_body().read_string().await?, "stale-body");

        let runtime = client.connector().runtime();
        for _ in 0..100 {
            if counter.load(Ordering::SeqCst) > 0 {
                break;
            }
            runtime.delay(Duration::from_millis(10)).await;
        }
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "background revalidation should hit the origin"
        );

        let cache = client
            .downcast_handler::<Cache<InMemoryStorage>>()
            .expect("cache handler installed");
        let key = CacheKey::new(Method::Get, "http://example.com/x".parse().unwrap());
        // Wait briefly for the background put to land in storage.
        for _ in 0..100 {
            if !cache.storage().get(&key).await.is_empty() {
                break;
            }
            runtime.delay(Duration::from_millis(10)).await;
        }
        let entries = cache.storage().get(&key).await;
        assert_eq!(entries.len(), 1);
        let body = entries[0].clone().open().await.unwrap();
        use futures_lite::AsyncReadExt;
        let mut buf = Vec::new();
        let mut body = body;
        body.read_to_end(&mut buf).await.unwrap();
        assert_eq!(&buf, b"body-0");
        Ok(())
    }

    #[test(harness)]
    async fn no_swr_falls_back_to_synchronous_revalidation() -> TestResult {
        let storage = InMemoryStorage::new();
        let _ = populate_stale_entry(&storage, "max-age=0", b"stale-body").await;

        let server = CountingServer::new("max-age=600");
        let counter = server.counter.clone();
        let client = Client::new(ServerConnector::new(server)).with_handler(Cache::new(storage));

        let mut conn = client.get("http://example.com/x").await?;
        assert_eq!(conn.response_body().read_string().await?, "body-0");
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[test(harness)]
    async fn must_revalidate_disables_swr() -> TestResult {
        let storage = InMemoryStorage::new();
        let _ = populate_stale_entry(
            &storage,
            "max-age=0, must-revalidate, stale-while-revalidate=3600",
            b"stale-body",
        )
        .await;

        let server = CountingServer::new("max-age=600");
        let client = Client::new(ServerConnector::new(server)).with_handler(Cache::new(storage));

        let mut conn = client.get("http://example.com/x").await?;
        assert_eq!(conn.response_body().read_string().await?, "body-0");
        Ok(())
    }
}
