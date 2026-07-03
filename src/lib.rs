//! HTTP cache for trillium implementing [RFC 9111] semantics, in two handler forms that
//! share one caching engine.
//!
//! The primary form is a [`trillium-client`](https://docs.rs/trillium-client) handler,
//! provided by the `client` module behind the `client` feature. Mount it on the client a
//! [`trillium-proxy`](https://docs.rs/trillium-proxy) uses to reach its upstream, mark it
//! shared, and the proxy becomes a CDN-style shared cache in front of that origin. On any
//! other `trillium-client` it serves as a user-agent cache.
//!
//! The server form, [`Cache`], sits before a trillium handler and caches that handler's own
//! responses.
//!
//! See the `client` module to get started, or [`Cache::new`] for the server form.
//!
//! ## Features
//!
//! - `client` — the `trillium-client` handler form (the `client` module), for caching at the
//!   user-agent layer and as a proxy's shared upstream cache.
//!
//! ## 0.1 status
//!
//! The server cache implements the bulk of RFC 9111: storability, freshness, conditional
//! revalidation, `Vary`, unsafe-method invalidation, plus `stale-if-error` recovery from
//! [RFC 5861]. The `stale-while-revalidate` directive is parsed but treated as synchronous
//! revalidation in this release. The client handler supports the full set including
//! background `stale-while-revalidate`.
//!
//! [RFC 9111]: https://www.rfc-editor.org/rfc/rfc9111
//! [RFC 5861]: https://www.rfc-editor.org/rfc/rfc5861
#![forbid(unsafe_code)]
#![deny(
    clippy::dbg_macro,
    missing_copy_implementations,
    rustdoc::missing_crate_level_docs,
    missing_debug_implementations,
    missing_docs,
    nonstandard_style,
    unused_qualifications
)]

#[cfg(doctest)]
#[doc = include_str!("../README.md")]
mod readme {}

mod freshness;
mod memory;
mod policy;
mod server;
mod storability;
mod storage;
mod tee;
mod validation;

#[cfg(feature = "fs")]
mod fs;
#[cfg(feature = "fs")]
mod fs_shims;

#[cfg(feature = "client")]
pub mod client;

#[cfg(test)]
mod test_helpers;

#[cfg(feature = "fs")]
pub use fs::{FileSystemStorage, FsPutHandle, FsStoredEntry};
pub use memory::{InMemoryEntry, InMemoryPutHandle, InMemoryStorage};
pub use policy::{CacheOptions, CachePolicy};
pub use server::Cache;
pub use storage::{CacheKey, CacheStorage, PutHandle, StoredEntry};
