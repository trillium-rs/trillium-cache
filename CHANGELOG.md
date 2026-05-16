# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Initial release. HTTP cache handler for Trillium implementing RFC 9111 semantics, with
both a server handler (place in front of a producer or a `trillium-proxy` upstream) and
— with the optional `client` cargo feature — a `ClientHandler` for caching at the
`trillium-client` user-agent layer. `stale-if-error` is supported on both sides;
background `stale-while-revalidate` is supported on the client and falls back to
synchronous revalidation on the server in this release.

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
