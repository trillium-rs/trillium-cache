//! Reproduction harness for the cache-hit-through-proxy body bug.
//!
//! Spawns two servers in one process:
//!   * an *origin* that serves a cacheable body and a non-cacheable body, and
//!   * a *proxy* (`trillium-proxy`) whose client carries a shared
//!     [`trillium_cache::Cache`].
//!
//! Then it drives requests through the proxy with a plain (uncached) client and
//! prints the status, byte count, and body for each, so a cache HIT can be
//! compared directly against a MISS / pass-through.
//!
//! Run with trace logging to watch the cache decisions:
//!
//! ```text
//! RUST_LOG=trillium_cache=trace cargo run --example proxy_cache --features client
//! ```

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use trillium::{Conn, KnownHeaderName};
use trillium_cache::{InMemoryStorage, client::Cache};
use trillium_client::Client;
use trillium_proxy::Proxy;
use trillium_smol::{ClientConfig, async_global_executor, config};

fn main() {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("trillium_cache=trace"),
    )
    .init();
    async_global_executor::block_on(run());
}

async fn run() {
    let origin_hits = Arc::new(AtomicUsize::new(0));

    // --- origin server ---
    let origin_hits_for_handler = origin_hits.clone();
    let origin = config()
        .with_port(0)
        .with_host("127.0.0.1")
        .without_signals()
        .spawn(move |conn: Conn| {
            let hits = origin_hits_for_handler.clone();
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                match conn.path() {
                    "/cacheable" => conn
                        .with_response_header(KnownHeaderName::CacheControl, "max-age=600")
                        .ok("hello from origin (cacheable)"),
                    "/chunked" => {
                        // Streaming body with unknown length -> origin frames it as
                        // Transfer-Encoding: chunked, no Content-Length.
                        let bytes = b"hello from origin (chunked, cacheable)".to_vec();
                        let body = trillium::Body::new_streaming(
                            futures_lite::io::Cursor::new(bytes),
                            None,
                        );
                        conn.with_response_header(KnownHeaderName::CacheControl, "max-age=600")
                            .with_body(body)
                            .with_status(200)
                            .halt()
                    }
                    "/nocache" => conn
                        .with_response_header(KnownHeaderName::CacheControl, "no-store")
                        .ok("hello from origin (no-store)"),
                    _ => conn.with_status(404).halt(),
                }
            }
        });
    let origin_addr = *origin.info().await.tcp_socket_addr().unwrap();
    let upstream = format!("http://{origin_addr}");
    println!("origin listening on {origin_addr}");

    // --- proxy server, with a shared cache on its client ---
    let cache = Cache::new(InMemoryStorage::new()).shared();
    let proxy_client = Client::new(ClientConfig::new()).with_handler(cache);
    let proxy = Proxy::new(proxy_client, upstream.as_str());
    let proxy_server = config()
        .with_port(0)
        .with_host("127.0.0.1")
        .without_signals()
        .spawn(proxy);
    let proxy_addr = *proxy_server.info().await.tcp_socket_addr().unwrap();
    println!("proxy  listening on {proxy_addr}\n");

    // --- drive requests through the proxy with a plain client ---
    let client = Client::new(ClientConfig::new());

    for (label, path) in [
        ("cacheable #1 (expect MISS)", "/cacheable"),
        ("cacheable #2 (expect HIT)", "/cacheable"),
        ("chunked   #1 (expect MISS)", "/chunked"),
        ("chunked   #2 (expect HIT)", "/chunked"),
        ("nocache   #1 (passthrough)", "/nocache"),
        ("nocache   #2 (passthrough)", "/nocache"),
    ] {
        let url = format!("http://{proxy_addr}{path}");
        let mut conn = client.get(url.as_str()).await.expect("request failed");
        let status = conn.status();
        let cl = conn
            .response_headers()
            .get_str(KnownHeaderName::ContentLength)
            .map(String::from);
        let body = conn.response_body().read_string().await.expect("read body");
        println!(
            "[{label}] status={status:?} content-length={cl:?} body={} bytes: {body:?}",
            body.len()
        );
    }

    println!(
        "\norigin was hit {} times",
        origin_hits.load(Ordering::SeqCst)
    );
    println!(
        "(expected 4: cacheable + chunked once each + nocache twice; the cacheable and chunked \
         HITs should not reach origin)"
    );

    origin.shut_down().await;
    proxy_server.shut_down().await;
}
