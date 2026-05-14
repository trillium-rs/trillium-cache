//! Server-side cache handler.
//!
//! [`Cache`] wires [`CacheStorage`] + [`CachePolicy`] onto a `trillium` server's handler chain.
//!
//! ## Position in the handler chain
//!
//! Place `Cache` *before* the handler whose responses you want to cache:
//!
//! ```ignore
//! let app = (
//!     Logger::new(),
//!     trillium_cache::Cache::new(InMemoryStorage::new()),
//!     my_app_handler,
//! );
//! ```
//!
//! ## Stale-while-revalidate not currently implemented
//!
//! This handler does not yet implement `stale-while-revalidate`. A stale entry within its
//! `stale-while-revalidate` window will fall through to synchronous revalidation (the inner
//! handler runs while the request is in flight). `stale-if-error` recovery *is* supported: when
//! the downstream handler produces a 5xx and the stored entry is SIE-eligible, the cache serves
//! the stored entry instead.

use crate::{
    AfterResponse, BeforeRequest, CacheEntry, CacheKey, CacheOptions, CachePolicy, CacheStorage,
};
use std::{sync::Arc, time::SystemTime};
use trillium::{Body, Conn, Handler, KnownHeaderName, Method};
use url::Url;

/// Default cap on body bytes the cache will store. Larger responses
/// pass through unmodified but are not cached.
pub const DEFAULT_MAX_CACHEABLE_SIZE: usize = 16 * 1024 * 1024;

/// Server-side cache handler. Mount on a trillium handler chain together with a
/// [`CacheStorage`] backend.
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

// State stashed in the conn's typeset by `run` for `before_send` to pick up.
#[derive(Debug)]
enum CacheCtx {
    /// Cache hit — `run` already populated a synthetic response and halted.
    Hit,
    /// Stored entry was stale and a conditional revalidation request has been spliced onto the
    /// conn. `before_send` reconciles the downstream handler's reply (304 vs 200) with the stored
    /// entry.
    Revalidation { stored: CacheEntry, key: CacheKey },
    /// Cache miss — no stored entry matched. If the response is storable, `before_send` will
    /// buffer the body and store it.
    Miss { key: CacheKey },
    /// Unsafe method (POST/PUT/DELETE/...). On a non-error response, `before_send` invalidates the
    /// target URI per RFC 9111 §4.4.
    Unsafe { url: Url },
}

// Build a `Url` from the request's effective scheme, host, and path-and-query. `is_secure()`
// reflects `trillium-forwarding`'s view of TLS termination, which is the right scheme to key on
// for a shared cache fronting trusted reverse proxies.
fn url_from_conn(conn: &Conn) -> Option<Url> {
    let scheme = if conn.is_secure() { "https" } else { "http" };
    let host = conn.host()?;
    let path_and_query = conn.path_and_query();
    Url::parse(&format!("{scheme}://{host}{path_and_query}")).ok()
}

impl<S: CacheStorage> Handler for Cache<S> {
    async fn run(&self, mut conn: Conn) -> Conn {
        let method = conn.method();
        let Some(url) = url_from_conn(&conn) else {
            log::trace!("cache: no host on request, passing through without caching");
            return conn;
        };
        let key = CacheKey::new(method, url.clone());
        log::trace!("cache: run {method} {url}");

        // RFC 9111 §4.4: don't read from cache for unsafe methods;
        // possibly invalidate after the round-trip.
        if !method.is_safe() {
            log::trace!("cache: unsafe method {method}, bypassing cache read");
            return conn.with_state(CacheCtx::Unsafe { url });
        }

        let now = SystemTime::now();
        let entries = self.storage.get(&key).await;
        log::trace!("cache: {} stored candidate(s) for {key}", entries.len());

        for entry in entries {
            match entry.policy().before_request(conn.request_headers(), now) {
                BeforeRequest::Fresh(cached) => {
                    log::trace!("cache: hit (fresh) for {key}, serving cached response");
                    *conn.response_headers_mut() = cached.headers;
                    let (_, body) = entry.into_parts();
                    return conn
                        .with_state(CacheCtx::Hit)
                        .with_status(cached.status)
                        .with_body(body)
                        .halt();
                }

                BeforeRequest::NotModified(cached) => {
                    // RFC 9111 §4.3.2 + RFC 9110 §13.2.2: client's conditional already matches
                    // the cached entry. Send 304 with stripped headers and no body.
                    log::trace!("cache: hit (fresh, conditional matches) for {key}, serving 304");
                    *conn.response_headers_mut() = cached.headers;
                    return conn
                        .with_state(CacheCtx::Hit)
                        .with_status(cached.status)
                        .with_body(Body::default())
                        .halt();
                }

                BeforeRequest::Stale {
                    request_headers,
                    matches: true,
                } => {
                    // RFC 9111 §4.3: splice conditional-revalidation headers onto the request;
                    // let the downstream handler run; reconcile in `before_send`.
                    //
                    // 0.1 caveat: no `stale-while-revalidate` — we always do synchronous
                    // revalidation here, even for SWR-eligible entries.
                    log::trace!("cache: stale for {key}, sending conditional revalidation request");
                    *conn.request_headers_mut() = request_headers;
                    return conn.with_state(CacheCtx::Revalidation { stored: entry, key });
                }

                BeforeRequest::Stale { matches: false, .. } => {
                    log::trace!("cache: candidate vary-mismatch for {key}, trying next");
                    continue;
                }
            }
        }

        log::trace!("cache: miss for {key}, forwarding to downstream handler");
        conn.with_state(CacheCtx::Miss { key })
    }

    async fn before_send(&self, mut conn: Conn) -> Conn {
        let Some(ctx) = conn.take_state::<CacheCtx>() else {
            return conn;
        };

        // RFC 9111 §4.2.4 stale-if-error: if revalidation produced a 5xx and the stored entry is
        // SIE-eligible, serve the stored entry instead.
        if let CacheCtx::Revalidation { ref stored, .. } = ctx {
            let now = SystemTime::now();
            let origin_failed = conn.status().is_some_and(|s| s.is_server_error());
            if origin_failed && stored.policy().is_sie_eligible(now) {
                log::trace!(
                    "cache: stale-if-error recovery for {} (downstream {:?}), serving stale",
                    conn.method(),
                    conn.status()
                );
                return apply_stale(conn, stored.clone(), now);
            }
        }

        if conn.status().is_none() {
            log::trace!("cache: downstream produced no status, passing through");
            return conn;
        }

        match ctx {
            CacheCtx::Hit => conn,
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

                    // §4.4: also invalidate URIs in `Location` and `Content-Location` headers
                    // when their host matches (DoS prevention).
                    for header in [KnownHeaderName::Location, KnownHeaderName::ContentLocation] {
                        let Some(value) = conn.response_headers().get_str(header) else {
                            continue;
                        };
                        let Ok(target) = url.join(value) else {
                            continue;
                        };
                        if target.host_str() != url.host_str() {
                            continue;
                        }
                        log::trace!(
                            "cache: unsafe method secondary invalidation via {header}: {target}"
                        );
                        self.invalidate_url(&target).await;
                    }
                }
                conn
            }
        }
    }
}

impl<S: CacheStorage> Cache<S> {
    async fn invalidate_url(&self, url: &Url) {
        self.storage
            .invalidate(&CacheKey::new(Method::Get, url.clone()))
            .await;
        self.storage
            .invalidate(&CacheKey::new(Method::Head, url.clone()))
            .await;
    }

    async fn handle_revalidation(&self, mut conn: Conn, stored: CacheEntry, key: CacheKey) -> Conn {
        let now = SystemTime::now();
        let status = conn.status().expect("checked above");
        match stored.policy().after_response(
            conn.request_headers(),
            status,
            conn.response_headers(),
            now,
        ) {
            AfterResponse::NotModified(new_policy, cached_response) => {
                log::trace!(
                    "cache: revalidation 304 for {key}, reusing stored body and refreshing entry"
                );
                let (_, body) = stored.into_parts();
                *conn.response_headers_mut() = cached_response.headers;
                conn.set_status(cached_response.status);
                conn.set_body(body.clone());
                self.storage
                    .put(key, CacheEntry::new(new_policy, body))
                    .await;
                conn
            }
            AfterResponse::Modified(new_policy, _) => {
                let Some(body) = drain_response_body(&mut conn).await else {
                    log::trace!(
                        "cache: revalidation 200 for {key}, body unavailable, passing through"
                    );
                    return conn;
                };
                let body_len = body.len();
                if body_len > self.max_cacheable_size {
                    log::trace!(
                        "cache: revalidation 200 for {key}, body {body_len} > max {}, served but \
                         not stored",
                        self.max_cacheable_size
                    );
                } else if !CachePolicy::is_storable(
                    conn.method(),
                    conn.request_headers(),
                    status,
                    conn.response_headers(),
                    &self.options,
                ) {
                    log::trace!(
                        "cache: revalidation 200 for {key}, response not storable, served but not \
                         stored"
                    );
                } else {
                    log::trace!(
                        "cache: revalidation 200 for {key}, replacing stored entry ({body_len} \
                         bytes)"
                    );
                    self.storage
                        .put(key, CacheEntry::new(new_policy, body.clone()))
                        .await;
                }
                conn.with_body(body)
            }
        }
    }

    async fn handle_miss(&self, mut conn: Conn, key: CacheKey) -> Conn {
        let status = conn.status().expect("checked above");
        if !CachePolicy::is_storable(
            conn.method(),
            conn.request_headers(),
            status,
            conn.response_headers(),
            &self.options,
        ) {
            log::trace!("cache: miss for {key}, response not storable, passing through");
            return conn;
        }
        let Some(body) = drain_response_body(&mut conn).await else {
            log::trace!("cache: miss for {key}, body unavailable, passing through");
            return conn;
        };
        let body_len = body.len();
        if body_len > self.max_cacheable_size {
            log::trace!(
                "cache: miss for {key}, body {body_len} > max {}, served but not stored",
                self.max_cacheable_size
            );
        } else {
            log::trace!("cache: miss for {key}, storing {body_len} bytes");
            let policy = CachePolicy::new(
                conn.method(),
                conn.request_headers(),
                status,
                conn.response_headers().clone(),
                SystemTime::now(),
                self.options,
            );
            self.storage
                .put(key, CacheEntry::new(policy, body.clone()))
                .await;
        }
        conn.with_body(body)
    }
}

// RFC 5861 stale-if-error recovery: replace conn's response state with the stored entry's.
fn apply_stale(mut conn: Conn, stored: CacheEntry, now: SystemTime) -> Conn {
    let cached = stored.policy().cached_response(now);
    let (_, body) = stored.into_parts();
    *conn.response_headers_mut() = cached.headers;
    conn.set_status(cached.status);
    conn.set_body(body);
    conn
}

// Take the response body off a conn and drain to bytes. Returns `None` when the conn has no body
// or the drain fails — caller passes through unchanged in either case.
async fn drain_response_body(conn: &mut Conn) -> Option<Vec<u8>> {
    let body = conn.take_response_body()?;
    match body.into_bytes().await {
        Ok(bytes) => Some(bytes.into_owned()),
        Err(e) => {
            log::warn!("cache: error draining response body: {e}");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryStorage;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use trillium_testing::{TestResult, TestServer, harness, test};

    #[derive(Debug, Clone)]
    struct CountingHandler {
        counter: Arc<AtomicUsize>,
        cache_control: &'static str,
        etag: Option<&'static str>,
    }

    impl CountingHandler {
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

    impl Handler for CountingHandler {
        async fn run(&self, conn: Conn) -> Conn {
            let n = self.counter.fetch_add(1, Ordering::SeqCst);
            if let Some(etag) = self.etag
                && conn.request_headers().get_str(KnownHeaderName::IfNoneMatch) == Some(etag)
            {
                return conn
                    .with_response_header(KnownHeaderName::Etag, etag)
                    .with_status(304)
                    .halt();
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

    fn cache_app(inner: CountingHandler) -> impl Handler {
        (Cache::new(InMemoryStorage::new()), inner)
    }

    #[test(harness)]
    async fn first_request_misses_subsequent_request_hits() -> TestResult {
        let inner = CountingHandler::new("max-age=600");
        let counter = inner.counter.clone();
        let app = TestServer::new(cache_app(inner)).await;

        let r1 = app.get("/x").await;
        r1.assert_ok().assert_body("body-0");

        let r2 = app.get("/x").await;
        r2.assert_ok().assert_body("body-0");
        assert_eq!(
            counter.load(Ordering::SeqCst),
            1,
            "inner handler only hit once"
        );
        Ok(())
    }

    #[test(harness)]
    async fn different_urls_dont_collide() -> TestResult {
        let inner = CountingHandler::new("max-age=600");
        let counter = inner.counter.clone();
        let app = TestServer::new(cache_app(inner)).await;

        app.get("/a").await.assert_body("body-0");
        app.get("/b").await.assert_body("body-1");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[test(harness)]
    async fn no_store_response_is_not_cached() -> TestResult {
        let inner = CountingHandler::new("no-store");
        let counter = inner.counter.clone();
        let app = TestServer::new(cache_app(inner)).await;

        app.get("/x").await.assert_body("body-0");
        app.get("/x").await.assert_body("body-1");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[test(harness)]
    async fn post_invalidates_existing_entry() -> TestResult {
        let inner = CountingHandler::new("max-age=600");
        let counter = inner.counter.clone();
        let app = TestServer::new(cache_app(inner)).await;

        app.get("/x").await.assert_body("body-0");
        let _ = app.post("/x").await;
        app.get("/x").await.assert_body("body-2");
        assert_eq!(counter.load(Ordering::SeqCst), 3);
        Ok(())
    }

    // §4.3 + §3.2: stored stale → revalidation → 304 → reuse cached body.
    #[test(harness)]
    async fn stale_with_etag_revalidates_to_304() -> TestResult {
        let inner = CountingHandler::new("max-age=0").with_etag(r#""v1""#);
        let counter = inner.counter.clone();
        let app = TestServer::new(cache_app(inner)).await;

        app.get("/x").await.assert_body("body-0");
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // Stale: cache sends conditional revalidation, inner returns 304, cache serves
        // the cached body with original status.
        let r2 = app.get("/x").await;
        r2.assert_ok().assert_body("body-0");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[test(harness)]
    async fn vary_isolates_entries_by_request_header() -> TestResult {
        #[derive(Debug, Clone, Default)]
        struct VaryHandler(Arc<AtomicUsize>);
        impl Handler for VaryHandler {
            async fn run(&self, conn: Conn) -> Conn {
                self.0.fetch_add(1, Ordering::SeqCst);
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

        let inner = VaryHandler::default();
        let counter = inner.0.clone();
        let app = TestServer::new((Cache::new(InMemoryStorage::new()), inner)).await;

        app.get("/x")
            .with_request_header(KnownHeaderName::AcceptEncoding, "gzip")
            .await
            .assert_body("body-for-gzip");
        app.get("/x")
            .with_request_header(KnownHeaderName::AcceptEncoding, "br")
            .await
            .assert_body("body-for-br");
        app.get("/x")
            .with_request_header(KnownHeaderName::AcceptEncoding, "gzip")
            .await
            .assert_body("body-for-gzip");

        assert_eq!(counter.load(Ordering::SeqCst), 2);
        Ok(())
    }

    #[test(harness)]
    async fn oversized_body_is_served_but_not_cached() -> TestResult {
        let inner = CountingHandler::new("max-age=600");
        let counter = inner.counter.clone();
        // "body-N" is 6 bytes — cap at 3 so nothing is stored.
        let app = TestServer::new((
            Cache::new(InMemoryStorage::new()).with_max_cacheable_size(3),
            inner,
        ))
        .await;

        app.get("/x").await.assert_body("body-0");
        app.get("/x").await.assert_body("body-1");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        Ok(())
    }

    // RFC 5861 stale-if-error: downstream returns 5xx, cache serves stored stale entry.
    #[test(harness)]
    async fn sie_serves_stale_on_5xx() -> TestResult {
        // First request populates the cache with a stale-if-error window. Subsequent requests
        // get a 5xx from the inner handler.
        #[derive(Debug, Clone)]
        struct FlakyHandler(Arc<AtomicUsize>);
        impl Handler for FlakyHandler {
            async fn run(&self, conn: Conn) -> Conn {
                let n = self.0.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    // First call: succeed with a SIE-eligible cacheable response.
                    conn.with_response_header(
                        KnownHeaderName::CacheControl,
                        "max-age=0, stale-if-error=3600",
                    )
                    .ok("stable")
                } else {
                    // Subsequent calls: fail with 5xx.
                    conn.with_status(500).halt()
                }
            }
        }

        let inner = FlakyHandler(Arc::new(AtomicUsize::new(0)));
        let counter = inner.0.clone();
        let app = TestServer::new((Cache::new(InMemoryStorage::new()), inner)).await;

        app.get("/x").await.assert_ok().assert_body("stable");
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // Stored entry is stale; cache revalidates synchronously; inner returns 5xx; SIE kicks
        // in and the stored body is served as 200.
        let r2 = app.get("/x").await;
        r2.assert_ok().assert_body("stable");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
        Ok(())
    }
}
