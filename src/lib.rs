//! HTTP cache handler for trillium.rs.
//!
//! This crate implements a client-side HTTP cache for use with
//! [`trillium-client`][trillium_client]. The primary intended consumer is
//! [`trillium-proxy`](https://docs.rs/trillium-proxy) (a shared cache in
//! front of upstream origins), but it can also be used directly with
//! `trillium-client` for any caching HTTP user agent.
//!
//! Caching semantics follow [RFC 9111] including the
//! `stale-while-revalidate` and `stale-if-error` extensions originally
//! specified in [RFC 5861].
//!
//! Status: work in progress.
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
mod handler;
mod policy;
mod storability;
mod storage;
mod validation;

#[cfg(test)]
mod test_helpers;

pub use handler::{Cache, DEFAULT_MAX_CACHEABLE_SIZE};
pub use policy::{CacheOptions, CachePolicy};
pub use storage::{CacheEntry, CacheKey, CacheStorage, InMemoryStorage};
pub use validation::{AfterResponse, BeforeRequest, CachedResponse};
