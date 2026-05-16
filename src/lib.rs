//! HTTP cache handler for Trillium, implementing [RFC 9111] semantics.
//!
//! The primary form is a server handler — [`Cache`] sits before the handler whose responses
//! should be cacheable, or in front of a [`trillium-proxy`](https://docs.rs/trillium-proxy)
//! upstream for shared (CDN-style) caching. With the `client` feature enabled, the
//! `client` module provides the same caching logic for a
//! [`trillium-client`](https://docs.rs/trillium-client) user agent.
//!
//! See [`Cache::new`] for getting started on the server side.
//!
//! ## Features
//!
//! - `client` — exposes a `trillium-client` `ClientHandler` form of the cache for caching at the
//!   user-agent layer.
//!
//! ## 0.1 status
//!
//! The server cache implements the bulk of RFC 9111: storability, freshness, conditional
//! revalidation, `Vary`, unsafe-method invalidation, plus `stale-if-error` recovery from
//! [RFC 5861]. The `stale-while-revalidate` directive is parsed but treated as synchronous
//! revalidation in this release. The client handler supports the full set including
//! background `stale-while-revalidate`.
//!
//! ## Streaming
//!
//! Cacheable responses stream through the cache: bytes flow to storage and to the user
//! concurrently as they arrive from the origin. Trailers propagate to both sides. The
//! body-size cap is enforced mid-stream; if exceeded, the cache write is aborted and the
//! remainder of the body passes through unchanged.
//!
//! The streaming contract is "we cache what you consume": if a caller drops a `Conn`
//! without reading the response body, nothing is stored for that response.
//!
//! [RFC 9111]: https://www.rfc-editor.org/rfc/rfc9111
//! [RFC 5861]: https://www.rfc-editor.org/rfc/rfc5861
#![forbid(unsafe_code)]
#![deny(
    missing_copy_implementations,
    rustdoc::missing_crate_level_docs,
    missing_debug_implementations,
    missing_docs,
    nonstandard_style,
    unused_qualifications
)]

#[cfg(test)]
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

#[cfg(feature = "client")]
pub mod client;

#[cfg(test)]
mod test_helpers;

pub use memory::{InMemoryEntry, InMemoryPutHandle, InMemoryStorage};
pub use policy::{CacheOptions, CachePolicy};
pub use server::Cache;
pub use storage::{CacheKey, CacheStorage, PutHandle, StoredEntry};
