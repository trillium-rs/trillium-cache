# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.1] - 2026-07-04

### Added

- `FileSystemStorage::with_time_to_idle` and `FileSystemStorage::with_time_to_live`, bringing the
  disk backend to parity with `InMemoryStorage`'s time-based eviction. Idle eviction drops
  variants not read within a duration; TTL drops them a duration after they are stored. Both
  delete the variant's `.meta`/`.body` files on eviction, like the existing size cap. Because
  `TieredStorage` composes configured backends, a `FileSystemStorage` cold tier carries its own
  expiry — no tiered API change. Expiry is best-effort space reclamation, not a hard read gate:
  `get` enumerates on-disk files, so a just-expired variant may be served in the brief window
  before its files are deleted. It is never stale — RFC 9111 freshness stays enforced by the
  `Cache` handler from the stored `CachePolicy`, independent of storage-level expiry.

## [0.2.0] - 2026-06-03

### Added

- `TieredStorage<Hot, Cold>`, a `CacheStorage` that layers a fast hot tier over a durable cold
  tier — for example an `InMemoryStorage` working set in front of a `FileSystemStorage` durable
  set, though any two backends compose. Lookups check the hot tier first; a cold hit is promoted
  into the hot tier as it is read, so a hot tier emptied by a restart repopulates from cold on
  demand. Writes populate the hot tier as the body streams and are flushed to the cold tier by a
  background task, so evicting an entry from the hot tier only drops a fast-path copy while the
  entry stays served from cold. Because the flush runs in the background, construct it with the
  two backends and the runtime the surrounding server or client already uses:
  `TieredStorage::new(hot, cold, runtime)`.

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
