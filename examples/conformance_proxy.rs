//! Conformance proxy for the cache-tests.fyi corpus.
//!
//! Runs a `trillium-proxy` whose client carries a shared [`trillium_cache::Cache`], in front
//! of the cache-tests origin server. This is the proxy-under-test for the external HTTP-cache
//! conformance suite at <https://cache-tests.fyi>.
//!
//! ## Workflow (three terminals)
//!
//! 1. cache-tests' own origin server (listens on `:8000`):
//!    ```text
//!    cd ../cache-tests && npm run server
//!    ```
//! 2. this proxy, pointed at that origin (listens on `:8080` by default):
//!    ```text
//!    cargo run --example conformance_proxy --features client
//!    ```
//! 3. the test runner, pointed at this proxy:
//!    ```text
//!    cd ../cache-tests && ./test-host.sh 127.0.0.1:8080
//!    ```
//!
//! ## Configuration (environment)
//!
//! - `ORIGIN` — upstream base URL. Default `http://localhost:8000`.
//! - `PORT` — port this proxy listens on. Default `8080`.
//! - `RUST_LOG` — e.g. `trillium_cache=trace` to watch cache decisions.

use trillium_cache::{InMemoryStorage, client::Cache};
use trillium_caching_headers::CachingHeaders;
use trillium_client::Client;
use trillium_proxy::Proxy;
use trillium_smol::{ClientConfig, config};

fn main() {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("trillium_cache=trace"),
    )
    .init();

    let origin = std::env::var("ORIGIN").unwrap_or_else(|_| String::from("http://localhost:8000"));
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);

    // Shared (CDN-style) cache on the proxy's upstream client.
    let cache = Cache::new(InMemoryStorage::new()).shared();
    let proxy_client = Client::new(ClientConfig::new()).with_handler(cache);

    // `proxy_not_found` so 404s from the origin are forwarded verbatim rather than passed
    // through to a bare trillium 404 — cache-tests exercises 404 responses directly.
    let proxy = Proxy::new(proxy_client, origin.as_str()).proxy_not_found();

    log::info!("conformance proxy: 127.0.0.1:{port} -> {origin}");

    config()
        .with_port(port)
        .with_host("127.0.0.1")
        .without_signals()
        .run((CachingHeaders::new(), proxy));
}
