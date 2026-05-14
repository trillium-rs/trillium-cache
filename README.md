# 🗃 trillium-cache — HTTP cache handler

[![ci][ci-badge]][ci]
[![crates.io version][version-badge]][crate]
[![docs.rs][docs-badge]][docs]

[ci]: https://github.com/trillium-rs/trillium/actions?query=workflow%3ACI
[ci-badge]: https://github.com/trillium-rs/trillium/workflows/CI/badge.svg
[version-badge]: https://img.shields.io/crates/v/trillium-cache.svg?style=flat-square
[crate]: https://crates.io/crates/trillium-cache
[docs-badge]: https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square
[docs]: https://docs.rs/trillium-cache

An [RFC 9111] HTTP cache for Trillium. The primary form is a server handler — place it before the handler whose responses you want to cache, or in front of a `trillium-proxy` upstream for shared (CDN-style) caching. With the `client` feature enabled, the same caching logic is also available as a [`trillium-client`](https://docs.rs/trillium-client) handler.

## Example

```rust,no_run
use trillium::Conn;
use trillium_cache::{Cache, InMemoryStorage};

let app = (
    Cache::new(InMemoryStorage::new()),
    |conn: Conn| async move { conn.ok("hello") },
);

// run with your chosen runtime adapter, e.g.:
// trillium_smol::run(app);
```

For a shared cache (proxy/CDN per [RFC 9111] §1.2.1), enable shared-cache semantics with `.shared()` and place it in front of your upstream handler:

```rust,no_run
use trillium_cache::{Cache, InMemoryStorage};

let cache = Cache::new(InMemoryStorage::new()).shared();
```

## Status

0.1. RFC 9111 coverage: storability, freshness, conditional revalidation, `Vary`, unsafe-method invalidation, plus `stale-if-error` recovery from [RFC 5861]. Background `stale-while-revalidate` is not yet implemented for the server handler — stale entries within their SWR window currently fall through to synchronous revalidation.

[RFC 9111]: https://www.rfc-editor.org/rfc/rfc9111
[RFC 5861]: https://www.rfc-editor.org/rfc/rfc5861

## Safety

This crate uses `#![forbid(unsafe_code)]`.

## License

<sup>
Licensed under either of <a href="LICENSE-APACHE">Apache License, Version
2.0</a> or <a href="LICENSE-MIT">MIT license</a> at your option.
</sup>

---

<sub>
Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in this crate by you, as defined in the Apache-2.0 license, shall
be dual licensed as above, without any additional terms or conditions.
</sub>
