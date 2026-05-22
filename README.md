# 🗃 trillium-cache — HTTP cache handler

[![ci][ci-badge]][ci]
[![crates.io version][version-badge]][crate]
[![docs.rs][docs-badge]][docs]
[![codecov][codecov-badge]][codecov]

[ci]: https://github.com/trillium-rs/trillium-cache/actions?query=workflow%3ACI
[ci-badge]: https://github.com/trillium-rs/trillium-cache/workflows/CI/badge.svg
[version-badge]: https://img.shields.io/crates/v/trillium-cache.svg?style=flat-square
[crate]: https://crates.io/crates/trillium-cache
[docs-badge]: https://img.shields.io/badge/docs-latest-blue.svg?style=flat-square
[docs]: https://docs.rs/trillium-cache
[codecov-badge]: https://codecov.io/gh/trillium-rs/trillium-cache/graph/badge.svg
[codecov]: https://codecov.io/gh/trillium-rs/trillium-cache

An [RFC 9111] HTTP cache for trillium, in two handler forms that share one caching engine.

The primary form is a [`trillium-client`](https://docs.rs/trillium-client) handler (the `client` feature). Add it to your client to cache at the user-agent layer; mark it `.shared()` for shared-cache (proxy/CDN) semantics. The server form caches a trillium handler's own responses.

## Example

The client handler, with shared-cache semantics:

```rust,no_run
use trillium_cache::{client::Cache, InMemoryStorage};
use trillium_client::Client;
use trillium_smol::ClientConfig;

let client = Client::from(ClientConfig::new())
    .with_handler(Cache::new(InMemoryStorage::new()).shared());
```

Hand that client to [`trillium-proxy`](https://docs.rs/trillium-proxy) as its upstream client and the proxy becomes a shared, CDN-style cache in front of the origin:

```rust,ignore
let proxy = Proxy::new(client, "http://origin.example")
    .with_via_pseudonym("trillium-proxy");
```

The server form caches a trillium handler's own responses — place `Cache` before the handler whose responses you want cached:

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

## Status

0.1. RFC 9111 coverage: storability, freshness, conditional revalidation, `Vary`, unsafe-method invalidation, plus `stale-if-error` recovery from [RFC 5861]. The client handler performs background `stale-while-revalidate`; the server handler does not — on the server, stale entries within their SWR window fall through to synchronous revalidation.

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
