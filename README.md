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

A client-side HTTP cache for [`trillium-client`](https://docs.rs/trillium-client),
implementing [RFC 9111](https://www.rfc-editor.org/rfc/rfc9111) caching semantics
including the `stale-while-revalidate` and `stale-if-error` extensions from
[RFC 5861](https://www.rfc-editor.org/rfc/rfc5861).

The primary intended consumer is
[`trillium-proxy`](https://docs.rs/trillium-proxy), but the handler can be
mounted on any `trillium-client::Client` for caching HTTP user-agent use.

**Status: work in progress.**

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
