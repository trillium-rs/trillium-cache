# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `FileSystemStorage`, a disk-backed `CacheStorage` that persists cached responses under a
  root directory so they survive process restarts. Bodies stream to and from disk rather than
  being buffered. Each response is stored as a compact rkyv-encoded metadata sidecar plus a
  raw body file; the metadata is optimized for fast loading rather than being human-readable.
  Enable it with the `fs` feature and select an async runtime with one of `smol`, `tokio`, or
  `async-std`. A byte cap (1 GiB by default) bounds total stored body size, evicting
  least-recently-used entries and deleting their files once it is reached; override it with
  `with_max_capacity_bytes` or remove it with `unbounded`. The cap is enforced across
  restarts and trims a directory that grew beyond it under an earlier configuration.

## [0.1.1] - 2026-05-26

### Fixed

- The client-side cache handler (the `client` feature) corrupted bodies from origins that
  responded with chunked transfer-encoding and no `Content-Length`. The chunk framing —
  size lines, CRLFs, and the terminating `0\r\n` — was treated as body content: it was
  written into storage and served back, both on a cache hit and on the initial
  pass-through on a miss. Responses with an unknown-length (chunked) body are now stored
  and replayed decoded, the same as fixed-length responses.

## [0.1.0] - 2026-05-21

Initial release. An RFC 9111 HTTP cache for trillium in two handler forms that share one
caching engine. With the optional `client` cargo feature, a `trillium-client` handler caches
at the user-agent layer; mounted on the client a `trillium-proxy` uses to reach its upstream,
it gives you a shared, CDN-style cache in front of the origin. A server handler caches a
trillium handler's own responses. `stale-if-error` is supported on both sides; background
`stale-while-revalidate` is supported on the client and falls back to synchronous
revalidation on the server in this release.

Bodies stream through the cache rather than being buffered: as bytes arrive from the
origin they flow to storage and to the user concurrently via a teeing reader. Trailers
propagate to both sides. Storage backends implement `CacheStorage` + `StoredEntry` +
`PutHandle`; the included `InMemoryStorage` offers a byte-aware size cap (256 MiB by
default), scan-resistant admission, and optional time-to-idle / time-to-live, with
concurrent reads and writes on distinct keys not contending. Configure via chained
`with_*` setters directly on `InMemoryStorage`. Stored variants are reference-counted
internally so cache lookups are cheap (Arc clones, no header / body copies). The
body-size cap is enforced mid-stream — when exceeded, the cache write is aborted and
the remainder of the body passes through unchanged.

The streaming contract is "cache populates when the body is consumed": if a caller
drops a `Conn` without reading the response body, nothing is stored for that response.
Read the body (e.g. via `ResponseBody::read_string`) on the request you want cached.
