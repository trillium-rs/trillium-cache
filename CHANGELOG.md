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
