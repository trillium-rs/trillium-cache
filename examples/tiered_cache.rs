//! Wiring a [`TieredStorage`] — an in-memory hot tier over a filesystem cold tier — into a
//! server [`Cache`], with a focus on where its background-flush runtime comes from.
//!
//! The server serves one cacheable route through the cache. The first request is a miss and
//! runs the wrapped handler; the second is served from the hot (in-memory) tier without
//! touching the handler, while the cold (filesystem) tier receives a durable copy from the
//! background write-back.
//!
//! Run with trace logging to watch the cache decisions:
//!
//! ```text
//! RUST_LOG=trillium_cache=trace cargo run --example tiered_cache --features smol,client
//! ```
//!
//! [`TieredStorage`]: trillium_cache::TieredStorage
//! [`Cache`]: trillium_cache::Cache

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use trillium::{Conn, KnownHeaderName};
use trillium_cache::{
    Cache, CacheKey, CacheStorage, FileSystemStorage, InMemoryStorage, TieredStorage,
};
use trillium_client::Client;
use trillium_http::Method;
use trillium_smol::{ClientConfig, SmolRuntime, async_global_executor, config};

fn main() {
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("trillium_cache=trace"),
    )
    .init();
    async_global_executor::block_on(run());
}

async fn run() {
    // The cold tier lives on disk; use a fresh temp dir for the demo.
    let cache_dir = std::env::temp_dir().join("trillium-cache-tiered-example");
    let _ = std::fs::remove_dir_all(&cache_dir);

    // TieredStorage flushes to its cold tier on a background task, so it needs the runtime that
    // task runs on. On smol that is `SmolRuntime::default()` — a handle to the same global
    // executor `trillium_smol` drives. On tokio it would be `TokioRuntime::default()` from
    // inside your tokio context. On a *client* you would instead take it from the connector:
    // `client.connector().runtime()` (see the `proxy_cache` example for the client-cache shape).
    let storage = TieredStorage::new(
        InMemoryStorage::new(),
        FileSystemStorage::new(&cache_dir),
        SmolRuntime::default(),
    );

    // Count how often the wrapped handler actually runs; a cache hit skips it.
    let handler_runs = Arc::new(AtomicUsize::new(0));
    let runs = handler_runs.clone();

    let server = config()
        .with_port(0)
        .with_host("127.0.0.1")
        .without_signals()
        .spawn((Cache::new(storage), move |conn: Conn| {
            let runs = runs.clone();
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                conn.with_response_header(KnownHeaderName::CacheControl, "max-age=600")
                    .ok("hello from the cached handler")
            }
        }));
    let addr = *server.info().await.tcp_socket_addr().unwrap();
    println!("server listening on {addr}\n");

    let client = Client::new(ClientConfig::new());
    for label in ["request #1 (expect MISS)", "request #2 (expect HIT)"] {
        let url = format!("http://{addr}/");
        let mut conn = client.get(url.as_str()).await.expect("request failed");
        let body = conn.response_body().read_string().await.expect("read body");
        println!("[{label}] {body:?}");
    }

    println!(
        "\nhandler ran {} time(s) (expected 1 — the second request was served from the hot tier)",
        handler_runs.load(Ordering::SeqCst)
    );

    // The cold-tier flush runs on a detached task, so poll a freshly opened FileSystemStorage
    // over the same directory — the view a restarted process would get — until the write-back
    // commits (or give up). This is the durability the hot tier alone can't offer.
    let key = CacheKey::new(Method::Get, format!("http://{addr}/").parse().unwrap());
    let reopened_cold = FileSystemStorage::new(&cache_dir);
    let mut committed = false;
    for _ in 0..50 {
        if !reopened_cold.get(&key).await.is_empty() {
            committed = true;
            break;
        }
        SmolRuntime::default()
            .delay(Duration::from_millis(20))
            .await;
    }
    println!(
        "cold tier under {} {} the entry after write-back",
        cache_dir.display(),
        if committed {
            "durably holds"
        } else {
            "did not receive"
        }
    );

    server.shut_down().await;
}
