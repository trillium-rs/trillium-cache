//! `Cache: ClientHandler` — wires [`CacheStorage`] + [`CachePolicy`]
//! onto a `trillium-client` request lifecycle.
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
//!   `after_response` so it can read the response body and replace it with a synthetic replay
//!   before any other handler reads the (one-shot) network body.

use crate::{
    AfterResponse, BeforeRequest, CacheEntry, CacheKey, CacheOptions, CachePolicy, CacheStorage,
};
use std::{sync::Arc, time::SystemTime};
use trillium_client::{Client, ClientHandler, Conn, Headers, KnownHeaderName, Method, Result, Url};

/// Default cap on body bytes the cache will store. Larger responses
/// pass through unmodified but are not cached.
pub const DEFAULT_MAX_CACHEABLE_SIZE: usize = 16 * 1024 * 1024;

/// Cache handler. Mount on a [`trillium_client::Client`] together with
/// a [`CacheStorage`] backend.
///
/// `Cache` is `Clone`: storage is held internally in an `Arc`, so cloning
/// is cheap and the spawned background revalidation future used by
/// `stale-while-revalidate` shares the same backend.
#[derive(Debug)]
pub struct Cache<S: CacheStorage> {
    storage: Arc<S>,
    options: CacheOptions,
    max_cacheable_size: usize,
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
    /// ([`CacheOptions::default`]) and the default body-size cap
    /// ([`DEFAULT_MAX_CACHEABLE_SIZE`]).
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

    /// Mark this cache as a *shared cache* (proxy/CDN per RFC 9111
    /// §1.2.1). Equivalent to `with_options` with `shared: true`.
    pub fn shared(mut self) -> Self {
        self.options.shared = true;
        self
    }

    /// Set the cap on response body bytes the cache will store.
    /// Responses larger than this pass through but are not stored.
    pub fn with_max_cacheable_size(mut self, max: usize) -> Self {
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
#[derive(Debug)]
enum CacheCtx {
    /// Cache hit — `run` already populated a synthetic response and
    /// halted. `after_response` is a no-op.
    Hit,
    /// Stored entry was stale and a conditional revalidation request
    /// has been spliced onto the conn. `after_response` reconciles the
    /// origin's reply (304 vs 200) with the stored entry.
    Revalidation { stored: CacheEntry, key: CacheKey },
    /// Cache miss — no stored entry matched. If the response is
    /// storable, `after_response` will buffer the body and store it.
    Miss { key: CacheKey },
    /// Unsafe method (POST/PUT/DELETE/...). On a non-error response,
    /// `after_response` invalidates the target URI per RFC 9111 §4.4.
    /// We carry a `Url` rather than a `CacheKey` because invalidation
    /// covers all cached methods (typically GET and HEAD), not just
    /// the unsafe method's own key.
    Unsafe { url: Url },
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
            conn.insert_state(CacheCtx::Unsafe {
                url: conn.url().clone(),
            });
            return Ok(());
        }

        let now = SystemTime::now();
        let entries = self.storage.get(&key).await;
        log::trace!("cache: {} stored candidate(s) for {key}", entries.len());

        for entry in entries {
            match entry.policy().before_request(conn, now) {
                BeforeRequest::Fresh(cached) => {
                    // Apply cached response head; serve cached body;
                    // halt to skip the network round-trip.
                    log::trace!("cache: hit (fresh) for {key}, serving cached response");
                    *conn.response_headers_mut() = cached.headers;
                    let (_, body) = entry.into_parts();
                    conn.set_status(cached.status)
                        .set_response_body(body)
                        .halt()
                        .insert_state(CacheCtx::Hit);
                    return Ok(());
                }

                BeforeRequest::NotModified(cached) => {
                    // RFC 9111 §4.3.2 + RFC 9110 §13.2.2: client's
                    // conditional already matches the cached entry. Send
                    // 304 with stripped headers and no body.
                    log::trace!(
                        "cache: hit (fresh, conditional matches) for {key}, serving 304"
                    );
                    *conn.response_headers_mut() = cached.headers;
                    conn.set_status(cached.status)
                        .set_response_body(b"" as &[u8])
                        .halt()
                        .insert_state(CacheCtx::Hit);
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
                        self.spawn_background_revalidation(
                            conn,
                            entry.clone(),
                            key.clone(),
                            request_headers,
                        );
                        self.serve_stale(conn, entry, now);
                        conn.halt();
                        conn.insert_state(CacheCtx::Hit);
                        return Ok(());
                    }
                    // Otherwise fall through to synchronous
                    // revalidation: splice conditional-revalidation
                    // headers onto the outbound request; resolve in
                    // `after_response`.
                    log::trace!(
                        "cache: stale for {key}, sending conditional revalidation request"
                    );
                    *conn.request_headers_mut() = request_headers;
                    conn.insert_state(CacheCtx::Revalidation { stored: entry, key });
                    return Ok(());
                }

                BeforeRequest::Stale { matches: false, .. } => {
                    // Wrong vary signature; try the next stored
                    // candidate.
                    log::trace!("cache: candidate vary-mismatch for {key}, trying next");
                    continue;
                }
            }
        }

        // No matching entry. Note the key so `after_response` can store
        // a fresh response if it's cacheable.
        log::trace!("cache: miss for {key}, forwarding to origin");
        conn.insert_state(CacheCtx::Miss { key });
        Ok(())
    }

    async fn after_response(&self, conn: &mut Conn) -> Result<()> {
        let Some(ctx) = conn.take_state::<CacheCtx>() else {
            log::trace!("cache: after_response with no CacheCtx, nothing to do");
            return Ok(());
        };

        // RFC 9111 §4.2.4 stale-if-error path: if revalidation hit a
        // transport-level failure or a 5xx, and the stored entry is
        // SIE-eligible, serve it instead. Clear the error so the
        // awaited conn returns Ok.
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
                self.serve_stale(conn, stored.clone(), now);
                conn.take_error();
                return Ok(());
            }
        }

        // Past the SIE recovery point. If transport failed and we
        // didn't recover, leave the error in place for the caller to
        // see.
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

                    // §4.4: also invalidate URIs from `Location` and
                    // `Content-Location` response headers, but only when
                    // their host matches the request URI's host (DoS
                    // prevention, per the same paragraph).
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
    // (immediate serve, then revalidate in background) and
    // stale-if-error (recovery on origin failure).
    fn serve_stale(&self, conn: &mut Conn, stored: CacheEntry, now: SystemTime) {
        let cached = stored.policy().cached_response(now);
        let (_, body) = stored.into_parts();
        conn.set_status(cached.status);
        *conn.response_headers_mut() = cached.headers;
        conn.set_response_body(body);
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
        stored: CacheEntry,
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
        stored: CacheEntry,
        key: CacheKey,
    ) {
        let mut new_conn = client.build_conn(method, url);
        *new_conn.request_headers_mut() = request_headers;

        if let Err(e) = (&mut new_conn).await {
            // Background revalidation failed at the transport level;
            // leave the stored entry in place. Future requests in the
            // SIE window will get the stale entry served, and outside
            // the SWR/SIE windows they'll attempt synchronous
            // revalidation again.
            log::trace!(
                "cache: background revalidation transport error for {key} ({e}), leaving \
                 stored entry"
            );
            return;
        }

        let now = SystemTime::now();
        match stored.policy().after_response(&new_conn, now) {
            AfterResponse::NotModified(new_policy, _) => {
                // 304 with matching validators: keep cached body,
                // refresh headers + response_time.
                log::trace!("cache: background revalidation 304 for {key}, refreshing entry");
                let (_, body) = stored.into_parts();
                self.storage
                    .put(key, CacheEntry::new(new_policy, body))
                    .await;
            }
            AfterResponse::Modified(new_policy, _) => {
                let Ok(body) = new_conn.response_body().read_bytes().await else {
                    log::trace!(
                        "cache: background revalidation read error for {key}, dropping"
                    );
                    return;
                };
                let body_len = body.len();
                if body_len > self.max_cacheable_size {
                    log::trace!(
                        "cache: background revalidation 200 for {key}, body {body_len} > \
                         max {}, dropping",
                        self.max_cacheable_size
                    );
                } else if !CachePolicy::is_storable(&new_conn, &self.options) {
                    log::trace!(
                        "cache: background revalidation 200 for {key}, response not storable, \
                         dropping"
                    );
                } else {
                    log::trace!(
                        "cache: background revalidation 200 for {key}, storing {body_len} bytes"
                    );
                    self.storage
                        .put(key, CacheEntry::new(new_policy, body))
                        .await;
                }
            }
        }
    }

    async fn handle_revalidation(
        &self,
        conn: &mut Conn,
        stored: CacheEntry,
        key: CacheKey,
    ) -> Result<()> {
        let now = SystemTime::now();
        match stored.policy().after_response(conn, now) {
            AfterResponse::NotModified(new_policy, cached_response) => {
                // 304 with matching validators: reuse stored body,
                // apply merged head, refresh storage entry.
                log::trace!(
                    "cache: revalidation 304 for {key}, reusing stored body and refreshing entry"
                );
                let (_, body) = stored.into_parts();
                conn.set_status(cached_response.status);
                *conn.response_headers_mut() = cached_response.headers;
                conn.set_response_body(body.clone());
                self.storage
                    .put(key, CacheEntry::new(new_policy, body))
                    .await;
                Ok(())
            }
            AfterResponse::Modified(new_policy, _) => {
                // Origin returned a fresh body (or the validators
                // didn't match). Capture it; restore as synthetic body
                // for downstream consumers; persist if cacheable.
                let body = take_response_body(conn).await?;
                let body_len = body.len();
                conn.set_response_body(body.clone());
                if body_len > self.max_cacheable_size {
                    log::trace!(
                        "cache: revalidation 200 for {key}, body {body_len} > max {}, served \
                         but not stored",
                        self.max_cacheable_size
                    );
                } else if !CachePolicy::is_storable(conn, &self.options) {
                    log::trace!(
                        "cache: revalidation 200 for {key}, response not storable, served but \
                         not stored"
                    );
                } else {
                    log::trace!(
                        "cache: revalidation 200 for {key}, replacing stored entry \
                         ({body_len} bytes)"
                    );
                    self.storage
                        .put(key, CacheEntry::new(new_policy, body))
                        .await;
                }
                Ok(())
            }
        }
    }

    async fn handle_miss(&self, conn: &mut Conn, key: CacheKey) -> Result<()> {
        if !CachePolicy::is_storable(conn, &self.options) {
            log::trace!("cache: miss for {key}, response not storable, passing through");
            return Ok(());
        }
        let body = take_response_body(conn).await?;
        let body_len = body.len();
        conn.set_response_body(body.clone());
        if body_len > self.max_cacheable_size {
            log::trace!(
                "cache: miss for {key}, body {body_len} > max {}, served but not stored",
                self.max_cacheable_size
            );
        } else {
            log::trace!("cache: miss for {key}, storing {body_len} bytes");
            let policy = CachePolicy::new(conn, SystemTime::now(), self.options);
            self.storage.put(key, CacheEntry::new(policy, body)).await;
        }
        Ok(())
    }
}

// Read the response body off a conn. The conn's body source is
// consumed; the caller should `set_response_body` with the returned
// bytes to give downstream consumers a synthetic replay.
async fn take_response_body(conn: &mut Conn) -> Result<Vec<u8>> {
    conn.response_body().read_bytes().await
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

            // Conditional GET: return 304 when If-None-Match matches.
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
        // Cache hit: still body-0, not body-1.
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
        // Not cached; server saw the request again.
        assert_eq!(r2.response_body().read_string().await?, "body-1");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[test(harness)]
    async fn post_invalidates_existing_entry() -> TestResult {
        let (client, counter) = cache_client(CountingServer::new("max-age=600"));

        // Populate the cache.
        let mut r1 = client.get("http://example.com/x").await?;
        assert_eq!(r1.response_body().read_string().await?, "body-0");

        // POST to the same URL → invalidate.
        let _ = client.post("http://example.com/x").await?;

        // Subsequent GET re-fetches.
        let mut r3 = client.get("http://example.com/x").await?;
        assert_eq!(r3.response_body().read_string().await?, "body-2");
        assert_eq!(counter.load(Ordering::SeqCst), 3);
        Ok(())
    }

    // §4.4: a successful unsafe response also invalidates entries for the
    // URIs in its Location and Content-Location headers (same-host only).
    #[test(harness)]
    async fn post_invalidates_location_and_content_location_targets() -> TestResult {
        // Server: GETs return a cacheable body; POSTs return Location and
        // Content-Location pointing at sibling cacheable URLs.
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

        // Populate cache for both Location and Content-Location targets.
        let _ = client.get("http://example.com/loc").await?;
        let _ = client.get("http://example.com/cl").await?;
        assert_eq!(counter.load(Ordering::SeqCst), 2);

        // Unsafe request whose response advertises both as side-effect
        // targets — invalidation must extend to them.
        let _ = client.post("http://example.com/anything").await?;

        // Subsequent GETs to the targets miss and re-fetch.
        let _ = client.get("http://example.com/loc").await?;
        let _ = client.get("http://example.com/cl").await?;
        assert_eq!(
            counter.load(Ordering::SeqCst),
            5,
            "POST + 2 re-fetches should hit the origin again"
        );
        Ok(())
    }

    // §4.4: cross-host Location/Content-Location values are NOT invalidated
    // (DoS prevention).
    #[test(harness)]
    async fn cross_host_location_does_not_invalidate() -> TestResult {
        #[derive(Debug, Clone, Default)]
        struct CrossHostServer(Arc<AtomicUsize>);
        impl ServerHandler for CrossHostServer {
            async fn run(&self, conn: ServerConn) -> ServerConn {
                let n = self.0.fetch_add(1, Ordering::SeqCst);
                if conn.method() == Method::Post {
                    // Absolute URL on a different host.
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
        // Single ServerConnector serves both hosts (the server doesn't care
        // which host the URL points to; it just runs the handler).
        let client = Client::new(ServerConnector::new(server))
            .with_handler(Cache::new(InMemoryStorage::new()));

        let _ = client.get("http://other.example/loc").await?;
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // POST to a *different* host whose response Locations the cached URI.
        // The host mismatch means we must not invalidate.
        let _ = client.post("http://example.com/anything").await?;

        // The cached entry for other.example/loc is still intact.
        let mut r = client.get("http://other.example/loc").await?;
        assert_eq!(r.response_body().read_string().await?, "get-0");
        assert_eq!(counter.load(Ordering::SeqCst), 2, "no extra GET to other.example");
        Ok(())
    }

    // §4.3 + §3.2: stored stale → revalidation → 304 → reuse cached body.
    #[test(harness)]
    async fn stale_with_etag_revalidates_to_304() -> TestResult {
        // max-age=0 → immediately stale. Etag present → server can 304.
        let (client, counter) = cache_client(CountingServer::new("max-age=0").with_etag(r#""v1""#));

        let mut r1 = client.get("http://example.com/x").await?;
        assert_eq!(r1.response_body().read_string().await?, "body-0");
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // Stored is stale; revalidation goes out with If-None-Match.
        // Server responds 304; cache reuses body-0.
        let mut r2 = client.get("http://example.com/x").await?;
        assert_eq!(r2.status(), Some(Status::Ok));
        assert_eq!(r2.response_body().read_string().await?, "body-0");
        // Server saw 2 requests total (the original + the conditional).
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        Ok(())
    }

    // §4.3.4: stored stale → revalidation → 200 → replace cached body.
    #[test(harness)]
    async fn stale_with_mismatching_etag_replaces_body() -> TestResult {
        // Server etag changes per request would need state machinery;
        // simplest version: server returns the current etag value but
        // increments body counter. The conditional request's
        // If-None-Match: "v1" matches the server's current etag → 304.
        // To test the 200 path, we use a server that lies (always
        // returns body, ignoring If-None-Match).
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

        // Origin returns 200 with same etag but different body — counts
        // as Modified (validators match in our policy code: same etag →
        // NotModified actually). Hmm — let me make etags differ.
        // (Actually: the stored etag is "rolling", request sends
        // If-None-Match: "rolling", server ignores it and returns 200
        // with etag "rolling". Our policy sees: status != 304 →
        // Modified.)
        let mut r2 = client.get("http://example.com/x").await?;
        assert_eq!(r2.response_body().read_string().await?, "body-1");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        Ok(())
    }

    // §4.1: Vary header isolates entries by selecting headers.
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

        // Different AE → cache miss, separate entry.
        let mut r2 = client
            .get("http://example.com/x")
            .with_request_header(KnownHeaderName::AcceptEncoding, "br")
            .await?;
        assert_eq!(r2.response_body().read_string().await?, "body-for-br");

        // Same AE as r1 → cache hit.
        let mut r3 = client
            .get("http://example.com/x")
            .with_request_header(KnownHeaderName::AcceptEncoding, "gzip")
            .await?;
        assert_eq!(r3.response_body().read_string().await?, "body-for-gzip");

        assert_eq!(counter.load(Ordering::SeqCst), 2);
        Ok(())
    }

    // Body over max_cacheable_size: served correctly, but not stored.
    #[test(harness)]
    async fn oversized_body_is_served_but_not_cached() -> TestResult {
        let server = CountingServer::new("max-age=600");
        let counter = server.counter.clone();
        let client = Client::new(ServerConnector::new(server))
            .with_handler(Cache::new(InMemoryStorage::new()).with_max_cacheable_size(3));

        // "body-0" is 6 bytes — over our 3-byte cap.
        let mut r1 = client.get("http://example.com/x").await?;
        assert_eq!(r1.response_body().read_string().await?, "body-0");

        // Not cached → server hit again on second request.
        let mut r2 = client.get("http://example.com/x").await?;
        assert_eq!(r2.response_body().read_string().await?, "body-1");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
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
        let policy = CachePolicy::new(&conn, SystemTime::now(), CacheOptions::default());
        let key = CacheKey::new(Method::Get, "http://example.com/x".parse().unwrap());
        storage
            .put(key.clone(), CacheEntry::new(policy, body.to_vec()))
            .await;
        key
    }

    #[test(harness)]
    async fn sie_serves_stale_on_transport_error() -> TestResult {
        let storage = InMemoryStorage::new();
        let _ =
            populate_stale_entry(&storage, "max-age=0, stale-if-error=3600", b"stale body").await;
        let client = Client::new(FailingConnector::new()).with_handler(Cache::new(storage));

        // Origin is unreachable, but stored entry is SIE-eligible.
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
        // Origin returns 503 on every request.
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
        // 5xx flows through to the user; cache does not recover.
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

        // User gets cached stale body immediately, NOT the server's
        // body-0 response.
        let mut conn = client.get("http://example.com/x").await?;
        assert_eq!(conn.status(), Some(Status::Ok));
        assert_eq!(conn.response_body().read_string().await?, "stale-body");

        // Wait for the spawned background revalidation to complete.
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

        // Storage should now reflect the revalidated entry.
        let cache = client
            .downcast_handler::<Cache<InMemoryStorage>>()
            .expect("cache handler installed");
        let key = CacheKey::new(Method::Get, "http://example.com/x".parse().unwrap());
        let entries = cache.storage().get(&key).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].body(), b"body-0");
        Ok(())
    }

    // No `stale-while-revalidate` directive → falls back to
    // synchronous revalidation.
    #[test(harness)]
    async fn no_swr_falls_back_to_synchronous_revalidation() -> TestResult {
        let storage = InMemoryStorage::new();
        let _ = populate_stale_entry(&storage, "max-age=0", b"stale-body").await;

        let server = CountingServer::new("max-age=600");
        let counter = server.counter.clone();
        let client = Client::new(ServerConnector::new(server)).with_handler(Cache::new(storage));

        // No SWR window → user waits for revalidation; gets fresh body.
        let mut conn = client.get("http://example.com/x").await?;
        assert_eq!(conn.response_body().read_string().await?, "body-0");
        // Server hit synchronously during the user's request.
        assert_eq!(counter.load(Ordering::SeqCst), 1);
        Ok(())
    }

    // must-revalidate disables SWR — falls back to synchronous
    // revalidation even with stale-while-revalidate set.
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

        // must-revalidate forbids stale serving; user gets fresh body.
        let mut conn = client.get("http://example.com/x").await?;
        assert_eq!(conn.response_body().read_string().await?, "body-0");
        Ok(())
    }
}
