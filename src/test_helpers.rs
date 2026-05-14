//! Shared test fixtures for the policy submodules.

use crate::{
    CacheOptions, CachePolicy,
    validation::{AfterResponse, BeforeRequest},
};
use std::time::{Duration, SystemTime};
use trillium_client::{Client, Conn, ConnExt, KnownHeaderName, Method, Status};
use trillium_testing::ServerConnector;

/// Build a conn whose request side and response side are both fully
/// populated. The transport stub would respond 500 if invoked, but we
/// never await — we just inspect the synthetic state.
pub fn exchange(
    method: Method,
    request_headers: &[(KnownHeaderName, &str)],
    status: Status,
    response_headers: &[(KnownHeaderName, &str)],
) -> Conn {
    let client = Client::new(ServerConnector::new(Status::InternalServerError));
    let mut conn = match method {
        Method::Get => client.get("http://example.com/"),
        Method::Post => client.post("http://example.com/"),
        Method::Put => client.put("http://example.com/"),
        Method::Head => client.build_conn(Method::Head, "http://example.com/"),
        other => client.build_conn(other, "http://example.com/"),
    };
    for (n, v) in request_headers {
        conn.request_headers_mut().insert(*n, v.to_string());
    }
    conn.set_status(status);
    for (n, v) in response_headers {
        conn.response_headers_mut().insert(*n, v.to_string());
    }
    conn
}

/// Build a *new* request conn — request side populated, response side
/// empty. This is what the cache handler would produce just before
/// calling [`crate::CachePolicy::before_request`].
pub fn request(method: Method, headers: &[(KnownHeaderName, &str)]) -> Conn {
    let client = Client::new(ServerConnector::new(Status::InternalServerError));
    let mut conn = match method {
        Method::Get => client.get("http://example.com/"),
        Method::Head => client.build_conn(Method::Head, "http://example.com/"),
        m => client.build_conn(m, "http://example.com/"),
    };
    for (n, v) in headers {
        conn.request_headers_mut().insert(*n, v.to_string());
    }
    conn
}

pub fn private_cache() -> CacheOptions {
    CacheOptions::default()
}

pub fn shared_cache() -> CacheOptions {
    CacheOptions {
        shared: true,
        ..CacheOptions::default()
    }
}

/// A fixed reference time: 2023-11-14T22:13:20Z (1_700_000_000s past
/// the unix epoch). Tests use `at(t0(), N)` to express "N seconds
/// later" deterministically.
pub fn t0() -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(1_700_000_000)
}

pub fn at(t: SystemTime, secs: u64) -> SystemTime {
    t + Duration::from_secs(secs)
}

// Conn → raw-parts adapters: the policy module's primary API takes
// `(method, request_headers, status, response_headers, ...)` so that it can
// be driven from both client and server conns. The test harness still
// composes exchanges via `trillium_client::Conn` for convenience; these
// thin helpers feed those conns into the parts-shaped APIs.

pub fn policy_from(conn: &Conn, response_time: SystemTime, options: CacheOptions) -> CachePolicy {
    CachePolicy::new(
        conn.method(),
        conn.request_headers(),
        conn.status().expect("response not yet received"),
        conn.response_headers().clone(),
        response_time,
        options,
    )
}

pub fn is_storable(conn: &Conn, options: &CacheOptions) -> bool {
    CachePolicy::is_storable(
        conn.method(),
        conn.request_headers(),
        conn.status().expect("response not yet received"),
        conn.response_headers(),
        options,
    )
}

pub fn before_request(policy: &CachePolicy, conn: &Conn, now: SystemTime) -> BeforeRequest {
    policy.before_request(conn.request_headers(), now)
}

pub fn after_response(
    policy: &CachePolicy,
    conn: &Conn,
    response_time: SystemTime,
) -> AfterResponse {
    policy.after_response(
        conn.request_headers(),
        conn.status().expect("response not yet received"),
        conn.response_headers(),
        response_time,
    )
}
