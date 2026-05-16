//! Cache storage trait.
//!
//! [`CacheStorage`] is the persistence boundary for a cache: lookup,
//! streaming insert, and invalidate, all keyed by [`CacheKey`] (URL +
//! method). A key may hold multiple entries — one per `Vary` signature
//! — and [`get`] returns the full list under a key. [`CachePolicy`] is
//! opaque to storage backends; use
//! [`CachePolicy::same_variant_as`] to dedupe by `Vary` signature when
//! finalizing an insert.
//!
//! ## Streaming
//!
//! [`put`] returns a [`PutHandle`] — an [`AsyncWrite`] sink the handler
//! writes body bytes into as they arrive from the origin. On EOF the
//! handler calls [`PutHandle::finalize`] with any trailers from the
//! body source. Dropping a `PutHandle` without finalizing aborts the
//! store; the partial data is discarded.
//!
//! On hit, [`StoredEntry::open`] returns a [`Body`] for replay. The
//! entry's stored trailers (if any) are attached to the returned Body
//! via [`Body::new_with_trailers`], so consumers see them by calling
//! [`Body::trailers`] / [`BodySource::trailers`] on the response body
//! after reaching EOF.
//!
//! [`get`]: CacheStorage::get
//! [`put`]: CacheStorage::put
//! [`AsyncWrite`]: futures_lite::AsyncWrite
//! [`Body::new_with_trailers`]: trillium_http::Body::new_with_trailers
//! [`Body::trailers`]: trillium_http::Body::trailers
//! [`BodySource::trailers`]: trillium_http::BodySource::trailers

use crate::CachePolicy;
use futures_lite::AsyncWrite;
use std::{
    fmt::{self, Debug, Display, Formatter},
    io,
};
use trillium_http::{Body, Headers, Method};
use url::Url;

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

/// Storage backend for cached responses.
///
/// A key may carry multiple entries, one per `Vary` signature. [`get`]
/// returns the full list under a key; the cache handler picks among
/// them internally.
///
/// Writes are streaming — [`put`] returns a [`PutHandle`] that the
/// caller writes bytes into as they arrive. The handler signals end-of-
/// stream by calling [`PutHandle::finalize`]; dropping the handle
/// without finalizing aborts the write.
///
/// [`get`]: CacheStorage::get
/// [`put`]: CacheStorage::put
pub trait CacheStorage: Debug + Send + Sync + 'static {
    /// Concrete entry type returned by [`get`][Self::get].
    type StoredEntry: StoredEntry;

    /// Streaming writer returned by [`put`][Self::put].
    type PutHandle: PutHandle;

    /// Fetch all entries stored under `key`. Returns an empty vec when
    /// the key has no entries.
    fn get(&self, key: &CacheKey) -> impl Future<Output = Vec<Self::StoredEntry>> + Send;

    /// Open a streaming insert for `key` with the supplied policy.
    /// Returns a [`PutHandle`] that the caller writes body bytes into,
    /// then closes with [`PutHandle::finalize`]. If an existing entry
    /// has the same `Vary` signature, finalize replaces it; otherwise
    /// the new entry is appended.
    ///
    /// Returning `Err` aborts the cache write — the caller passes the
    /// origin response through to the user but does not cache it.
    fn put(
        &self,
        key: CacheKey,
        policy: CachePolicy,
    ) -> impl Future<Output = io::Result<Self::PutHandle>> + Send;

    /// Remove all entries stored under `key`.
    fn invalidate(&self, key: &CacheKey) -> impl Future<Output = ()> + Send;
}

/// One stored response.
///
/// Returned by [`CacheStorage::get`]. Cheap to hold and pass around —
/// in-memory backends share underlying buffers via [`Arc`][std::sync::Arc],
/// and other backends typically hold only metadata until
/// [`open`][Self::open] is called. The [`Clone`] bound supports the cache
/// handler's stale-while-revalidate flow, which needs one handle to serve
/// the stale entry to the user and another to drive background
/// revalidation; for typical backends `clone` is a cheap pointer copy.
pub trait StoredEntry: Clone + Debug + Send + Sync + 'static {
    /// Borrow the [`CachePolicy`] this entry was stored with.
    fn policy(&self) -> &CachePolicy;

    /// Replace the stored policy without rewriting the body.
    ///
    /// Used on a successful 304 revalidation (RFC 9111 §3.2) to refresh
    /// validators and freshness directives while keeping the previously
    /// stored body bytes. The supplied policy carries the merged
    /// stored+304 headers and a fresh `response_time`.
    fn refresh_policy(
        &mut self,
        new_policy: CachePolicy,
    ) -> impl Future<Output = io::Result<()>> + Send;

    /// Open the stored response body for replay.
    ///
    /// Consumes the entry and returns a [`Body`] that yields the stored
    /// bytes when read. If trailers were captured on store, they are
    /// attached via [`Body::new_with_trailers`] and surface to readers
    /// after EOF via [`BodySource::trailers`][trillium_http::BodySource::trailers].
    fn open(self) -> impl Future<Output = io::Result<Body>> + Send;
}

/// Streaming writer returned by [`CacheStorage::put`].
///
/// Write body bytes via the [`AsyncWrite`] impl, then call
/// [`finalize`][Self::finalize] once the body is fully consumed.
/// Dropping a `PutHandle` without finalizing aborts the write; partial
/// data MUST NOT be exposed by a subsequent [`CacheStorage::get`].
pub trait PutHandle: AsyncWrite + Send + Unpin + 'static {
    /// Commit the buffered bytes to storage with any trailers from the
    /// body source.
    ///
    /// `trailers` is `Some` when the body source produced a trailers
    /// section (a `BodySource` whose `trailers()` returned `Some`
    /// after EOF), `None` otherwise — distinguishing "no trailers
    /// section" from "empty trailers section."
    fn finalize(self, trailers: Option<Headers>) -> impl Future<Output = io::Result<()>> + Send;
}
