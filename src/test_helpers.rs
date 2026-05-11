//! Shared test fixtures for the policy submodules.

use crate::CacheOptions;
use std::time::{Duration, SystemTime};
use trillium_client::{Client, Conn, KnownHeaderName, Method, Status};
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
