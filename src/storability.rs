//! RFC 9111 §3 — *Storing Responses in Caches*.
//!
//! Implements [`CachePolicy::is_storable`], the question "may we put
//! this exchange in a cache at all?". Pure read of the conn's request
//! and response sides; mutates nothing.

use crate::{
    CacheOptions,
    policy::{CachePolicy, effective_response_cache_control},
};
use trillium_caching_headers::{CacheControlHeader, CachingHeadersExt};
use trillium_http::{Headers, KnownHeaderName, Method, Status};

// RFC 9110 §15 / RFC 9111 §3: status codes that a cache may reuse without
// any explicit expiration ("cacheable by default"). Other statuses can
// still be stored when the response carries explicit expiration
// (max-age / s-maxage / Expires / public) — see §3 (5) below.
const STATUS_CODE_CACHEABLE_BY_DEFAULT: &[u16] =
    &[200, 203, 204, 206, 300, 301, 308, 404, 405, 410, 414, 501];

fn is_cacheable_by_default(status: Status) -> bool {
    STATUS_CODE_CACHEABLE_BY_DEFAULT.contains(&(status as u16))
}

// RFC 9111 §4.2.1: any of these gives the response an explicit freshness
// lifetime, distinct from heuristic freshness. When a RFC 9213 targeted
// field is in effect, `Expires` is ignored (§2.2).
pub(crate) fn response_has_explicit_expiration(
    response_cc: &Option<CacheControlHeader>,
    response_headers: &Headers,
    options: &CacheOptions,
    targeted_cc_in_effect: bool,
) -> bool {
    if options.shared
        && response_cc
            .as_ref()
            .is_some_and(|cc| cc.s_maxage().is_some())
    {
        return true;
    }
    if response_cc
        .as_ref()
        .is_some_and(|cc| cc.max_age().is_some())
    {
        return true;
    }
    !targeted_cc_in_effect && response_headers.has_header(KnownHeaderName::Expires)
}

impl CachePolicy {
    // Whether a cache may store the response described by the supplied
    // parts. When false, the caller MUST NOT store anything for this
    // exchange.
    pub(crate) fn is_storable(
        method: Method,
        request_headers: &Headers,
        status: Status,
        response_headers: &Headers,
        options: &CacheOptions,
    ) -> bool {
        let request_cc = request_headers.cache_control();
        // RFC 9213 §2.2: when a targeted field (CDN-Cache-Control on a shared
        // cache) is in effect, it fully replaces Cache-Control AND Expires
        // for caching policy decisions — including storability.
        let (response_cc, targeted_cc_in_effect) =
            effective_response_cache_control(response_headers, options);

        // §5.2.1.5
        if request_cc.as_ref().is_some_and(|cc| cc.is_no_store()) {
            return false;
        }

        // §3 (1) + §4.2.1
        let method_ok = matches!(method, Method::Get | Method::Head)
            || (method == Method::Post
                && response_has_explicit_expiration(
                    &response_cc,
                    response_headers,
                    options,
                    targeted_cc_in_effect,
                ));
        if !method_ok {
            return false;
        }

        // §3 (2): "the cache understands the response" — for HTTP status
        // codes, this is intentionally permissive in the spec; the more
        // operative gate is §3 (5) below, which requires explicit freshness
        // (max-age / s-maxage / Expires / public) for any status not on the
        // cacheable-by-default list. So a 500 with `Cache-Control: max-age=N`
        // is stored, but a 500 without explicit freshness is not.

        // §5.2.2.5
        if response_cc.as_ref().is_some_and(|cc| cc.is_no_store()) {
            return false;
        }

        // §5.2.2.7
        if options.shared && response_cc.as_ref().is_some_and(|cc| cc.is_private()) {
            return false;
        }

        // §3.5
        if options.shared
            && request_headers.has_header(KnownHeaderName::Authorization)
            && !response_cc
                .as_ref()
                .is_some_and(|cc| cc.must_revalidate() || cc.is_public() || cc.s_maxage().is_some())
        {
            return false;
        }

        // §3 (5) — Expires is ignored when a targeted field is in effect.
        (!targeted_cc_in_effect && response_headers.has_header(KnownHeaderName::Expires))
            || response_cc.as_ref().is_some_and(|cc| {
                cc.max_age().is_some()
                    || cc.is_public()
                    || (options.shared && cc.s_maxage().is_some())
            })
            || is_cacheable_by_default(status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::*;
    use trillium_http::KnownHeaderName::*;

    // §5.2.1.5
    #[test]
    fn no_store_request_blocks_storage() {
        let conn = exchange(Method::Get, &[(CacheControl, "no-store")], Status::Ok, &[]);
        assert!(!is_storable(&conn, &private_cache()));
    }

    // §5.2.2.5
    #[test]
    fn no_store_response_blocks_storage() {
        let conn = exchange(Method::Get, &[], Status::Ok, &[(CacheControl, "no-store")]);
        assert!(!is_storable(&conn, &private_cache()));
    }

    // §3 (5): 200 is cacheable-by-default with no caching headers.
    #[test]
    fn cacheable_by_default_status_is_storable() {
        let conn = exchange(Method::Get, &[], Status::Ok, &[]);
        assert!(is_storable(&conn, &private_cache()));
    }

    // §3 (5): 500 is not cacheable-by-default; without explicit freshness
    // it cannot be stored.
    #[test]
    fn non_default_status_not_storable_without_freshness() {
        let conn = exchange(Method::Get, &[], Status::InternalServerError, &[]);
        assert!(!is_storable(&conn, &private_cache()));
    }

    // §3 (5): non-default statuses (4xx/5xx) ARE storable when the response
    // carries explicit freshness directives.
    #[test]
    fn non_default_status_storable_with_explicit_freshness() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::InternalServerError,
            &[(CacheControl, "max-age=600")],
        );
        assert!(is_storable(&conn, &private_cache()));

        let conn = exchange(
            Method::Get,
            &[],
            Status::BadRequest,
            &[(CacheControl, "max-age=600")],
        );
        assert!(is_storable(&conn, &private_cache()));
    }

    // §3 (1): PUT is not a cacheable method.
    #[test]
    fn put_not_storable() {
        let conn = exchange(Method::Put, &[], Status::Ok, &[]);
        assert!(!is_storable(&conn, &private_cache()));
    }

    // §4.2.1: POST with explicit expiration is storable.
    #[test]
    fn post_with_max_age_is_storable() {
        let conn = exchange(
            Method::Post,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600")],
        );
        assert!(is_storable(&conn, &private_cache()));
    }

    // §4.2.1: POST without explicit expiration is not storable.
    #[test]
    fn post_without_expiration_not_storable() {
        let conn = exchange(Method::Post, &[], Status::Ok, &[]);
        assert!(!is_storable(&conn, &private_cache()));
    }

    // §5.2.2.7
    #[test]
    fn shared_cache_refuses_private_response() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "private, max-age=600")],
        );
        assert!(!is_storable(&conn, &shared_cache()));
    }

    #[test]
    fn private_cache_accepts_private_response() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "private, max-age=600")],
        );
        assert!(is_storable(&conn, &private_cache()));
    }

    // §3.5
    #[test]
    fn shared_cache_refuses_authorization_by_default() {
        let conn = exchange(
            Method::Get,
            &[(Authorization, "Bearer x")],
            Status::Ok,
            &[(CacheControl, "max-age=600")],
        );
        assert!(!is_storable(&conn, &shared_cache()));
    }

    #[test]
    fn shared_cache_allows_authorization_with_public() {
        let conn = exchange(
            Method::Get,
            &[(Authorization, "Bearer x")],
            Status::Ok,
            &[(CacheControl, "public, max-age=600")],
        );
        assert!(is_storable(&conn, &shared_cache()));
    }

    #[test]
    fn shared_cache_allows_authorization_with_s_maxage() {
        let conn = exchange(
            Method::Get,
            &[(Authorization, "Bearer x")],
            Status::Ok,
            &[(CacheControl, "s-maxage=600")],
        );
        assert!(is_storable(&conn, &shared_cache()));
    }

    #[test]
    fn shared_cache_allows_authorization_with_must_revalidate() {
        let conn = exchange(
            Method::Get,
            &[(Authorization, "Bearer x")],
            Status::Ok,
            &[(CacheControl, "must-revalidate, max-age=600")],
        );
        assert!(is_storable(&conn, &shared_cache()));
    }

    #[test]
    fn private_cache_allows_authorization() {
        let conn = exchange(
            Method::Get,
            &[(Authorization, "Bearer x")],
            Status::Ok,
            &[(CacheControl, "max-age=600")],
        );
        assert!(is_storable(&conn, &private_cache()));
    }

    // Pragma synthesis: a 200 with only Pragma: no-cache is still storable
    // (status is cacheable by default), but freshness will require
    // revalidation.
    #[test]
    fn pragma_no_cache_does_not_block_storage() {
        let conn = exchange(Method::Get, &[], Status::Ok, &[(Pragma, "no-cache")]);
        assert!(is_storable(&conn, &private_cache()));
    }

    // ===== RFC 9213: targeted CDN-Cache-Control =====

    // §3.1 example 2: CDN-CC=max-age, CC=no-store. CDN cache stores it
    // (CDN-CC fully replaces CC for the targeted cache).
    #[test]
    fn shared_cache_stores_when_cdn_cc_overrides_cc_no_store() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[
                (CacheControl, "no-store"),
                (CdnCacheControl, "max-age=10000"),
            ],
        );
        assert!(is_storable(&conn, &shared_cache()));
    }

    // §2.2 inverse: CDN-CC=no-store overrides CC=max-age — CDN cache must
    // not store the response.
    #[test]
    fn shared_cache_refuses_when_cdn_cc_no_store_overrides_cc_max_age() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[
                (CacheControl, "max-age=10000"),
                (CdnCacheControl, "no-store"),
            ],
        );
        assert!(!is_storable(&conn, &shared_cache()));
    }

    // §2.2: private (non-shared) caches MUST ignore the targeted field.
    // Same headers as the previous test, but a private cache stores
    // because it sees only Cache-Control: max-age=10000.
    #[test]
    fn private_cache_ignores_cdn_cache_control() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[
                (CacheControl, "max-age=10000"),
                (CdnCacheControl, "no-store"),
            ],
        );
        assert!(is_storable(&conn, &private_cache()));
    }
}
