//! RFC 9111 §4.2 — *Freshness*.
//!
//! Age math (§4.2.3) and freshness lifetime computation (§4.2.1, §4.2.2)
//! for a stored [`CachePolicy`]. All times are absolute `SystemTime`.

use crate::policy::CachePolicy;
use std::time::{Duration, SystemTime};
use trillium_http::KnownHeaderName;

impl CachePolicy {
    // How long the response has been in the cache system: any prior `Age`
    // header value plus the wall-clock time resident in this cache.
    pub(crate) fn age(&self, now: SystemTime) -> Duration {
        let mut age = self.age_header_value();
        if let Ok(resident_time) = now.duration_since(self.response_time) {
            age += resident_time;
        }
        age
    }

    // Approximate time until the response becomes stale (`Duration::ZERO`
    // once stale).
    pub(crate) fn time_to_live(&self, now: SystemTime) -> Duration {
        self.max_age()
            .checked_sub(self.age(now))
            .unwrap_or_default()
    }

    // Whether the stored response is past its freshness lifetime.
    pub(crate) fn is_stale(&self, now: SystemTime) -> bool {
        self.max_age() <= self.age(now)
    }

    // RFC 9111 §4.2.1 + §4.2.2 freshness lifetime, normalized to a
    // `Duration` since the origin's `Date`. `Duration::ZERO` means
    // "treat as immediately stale".
    pub(crate) fn max_age(&self) -> Duration {
        // §5.2.2.4: `no-cache` requires revalidation on every use.
        if self
            .response_cache_control
            .as_ref()
            .is_some_and(|cc| cc.is_no_cache())
        {
            return Duration::ZERO;
        }

        // Shared caches: a `Set-Cookie`-bearing response is technically
        // cacheable per RFC, but storing per-user cookies in a shared
        // cache is almost always a mistake. Require explicit opt-in via
        // `public` or `immutable`.
        if self.options.shared
            && self.response_headers.has_header(KnownHeaderName::SetCookie)
            && !self
                .response_cache_control
                .as_ref()
                .is_some_and(|cc| cc.is_public() || cc.is_immutable())
        {
            return Duration::ZERO;
        }

        // RFC 9111 §4.1: `Vary: *` means the response can never be reused
        // for a future request.
        if self
            .response_headers
            .get_str(KnownHeaderName::Vary)
            .map(str::trim)
            == Some("*")
        {
            return Duration::ZERO;
        }

        // §5.2.2.8: shared caches must revalidate on `proxy-revalidate`.
        if self.options.shared
            && self
                .response_cache_control
                .as_ref()
                .is_some_and(|cc| cc.is_proxy_revalidate())
        {
            return Duration::ZERO;
        }

        // §5.2.2.10: shared cache prefers `s-maxage` over `max-age` /
        // `Expires`. (Private caches ignore `s-maxage`.)
        if self.options.shared
            && let Some(s) = self
                .response_cache_control
                .as_ref()
                .and_then(|cc| cc.s_maxage())
        {
            return s;
        }

        // §5.2.2.1: `max-age` overrides `Expires`.
        if let Some(m) = self
            .response_cache_control
            .as_ref()
            .and_then(|cc| cc.max_age())
        {
            return m;
        }

        // RFC 9111 §5.2.2.2: `immutable` provides a default freshness
        // lifetime when nothing else does.
        let default_min_ttl = if self
            .response_cache_control
            .as_ref()
            .is_some_and(|cc| cc.is_immutable())
        {
            self.options.immutable_min_time_to_live
        } else {
            Duration::ZERO
        };

        let server_date = self.raw_server_date();

        // §4.2.1: explicit `Expires`. Invalid date format → already
        // expired. RFC 9213 §2.2: when a targeted field (CDN-Cache-Control)
        // is in effect, the cache MUST ignore Expires too — skip this
        // fallback in that case.
        if !self.targeted_cc_in_effect
            && let Some(expires_str) = self.response_headers.get_str(KnownHeaderName::Expires)
        {
            return match httpdate::parse_http_date(expires_str) {
                Err(_) => Duration::ZERO,
                Ok(expires) => {
                    default_min_ttl.max(expires.duration_since(server_date).unwrap_or_default())
                }
            };
        }

        // §4.2.2: heuristic freshness from `Last-Modified`.
        if let Some(lm_str) = self.response_headers.get_str(KnownHeaderName::LastModified)
            && let Ok(last_modified) = httpdate::parse_http_date(lm_str)
            && let Ok(diff) = server_date.duration_since(last_modified)
        {
            let secs = (diff.as_secs() as f64 * f64::from(self.options.cache_heuristic)) as u64;
            return default_min_ttl.max(Duration::from_secs(secs));
        }

        default_min_ttl
    }

    // The origin's `Date` header parsed to a `SystemTime`, falling back
    // to when this cache received the response if `Date` is missing or
    // malformed.
    fn raw_server_date(&self) -> SystemTime {
        self.response_headers
            .get_str(KnownHeaderName::Date)
            .and_then(|d| httpdate::parse_http_date(d).ok())
            .unwrap_or(self.response_time)
    }

    // The `Age` header value, in seconds. Defaults to zero if missing or
    // unparseable.
    //
    // Uses the first header line when several are present (RFC 9110 §5.3:
    // folding multiple lines is equivalent to a single comma-separated value,
    // and Age is a single delta-seconds, not a list, so the first value wins).
    fn age_header_value(&self) -> Duration {
        let secs = self
            .response_headers
            .get_values(KnownHeaderName::Age)
            .and_then(|values| values.first())
            .and_then(|value| value.as_str())
            .and_then(parse_age_value)
            .unwrap_or(0);
        Duration::from_secs(secs)
    }

    // Window past `max_age` during which a cache may serve this stale
    // response while a background revalidation runs. `Duration::ZERO` if the
    // response did not advertise `stale-while-revalidate`.
    //
    // Gated on `client`: only the client handler currently does background
    // revalidation. Ungate when the server gains background SWR.
    #[cfg(feature = "client")]
    pub(crate) fn stale_while_revalidate_window(&self) -> Duration {
        self.response_cache_control
            .as_ref()
            .and_then(|cc| cc.stale_while_revalidate())
            .unwrap_or(Duration::ZERO)
    }

    // Window past `max_age` during which a cache may serve this stale
    // response when origin revalidation fails. `Duration::ZERO` if the
    // response did not advertise `stale-if-error`.
    pub(crate) fn stale_if_error_window(&self) -> Duration {
        self.response_cache_control
            .as_ref()
            .and_then(|cc| cc.stale_if_error())
            .unwrap_or(Duration::ZERO)
    }

    // True when the response is stale, has not been declared
    // `must-revalidate` (or `proxy-revalidate` for shared caches), and the
    // staleness is within the `stale-while-revalidate` window. Indicates the
    // cache MAY serve this entry immediately while revalidating in the
    // background.
    //
    // Gated on `client`: only the client handler currently does background
    // revalidation. Ungate when the server gains background SWR.
    #[cfg(feature = "client")]
    pub(crate) fn is_swr_eligible(&self, now: SystemTime) -> bool {
        self.is_eligible_for_stale_serving(now, self.stale_while_revalidate_window())
    }

    // True when the response is stale, has not been declared
    // `must-revalidate` (or `proxy-revalidate` for shared caches), and the
    // staleness is within the `stale-if-error` window. Indicates the cache
    // MAY serve this entry as a fallback when origin revalidation fails or
    // returns a 5xx.
    pub(crate) fn is_sie_eligible(&self, now: SystemTime) -> bool {
        self.is_eligible_for_stale_serving(now, self.stale_if_error_window())
    }

    fn is_eligible_for_stale_serving(&self, now: SystemTime, window: Duration) -> bool {
        if !self.is_stale(now) || window.is_zero() {
            return false;
        }
        // §5.2.2.2: must-revalidate forbids serving stale.
        if self
            .response_cache_control
            .as_ref()
            .is_some_and(|cc| cc.must_revalidate())
        {
            return false;
        }
        // For shared caches, proxy-revalidate has the same effect.
        if self.options.shared
            && self
                .response_cache_control
                .as_ref()
                .is_some_and(|cc| cc.is_proxy_revalidate())
        {
            return false;
        }
        let staleness = self.age(now).saturating_sub(self.max_age());
        staleness < window
    }
}

// Per RFC 9111 §5.1, Age is a single non-negative integer (delta-seconds), and
// invalid values SHOULD be ignored — which collapses to "treat as 0" at our
// call site. The cache-tests corpus expects a more permissive parse: take the
// segment before any `;` (structured-field parameter syntax) or `,` (list
// separator from RFC 9110 §5.6.1.2 line-folding), trim, then parse as u64.
// Floats, negatives, and non-numeric prefixes return None and the caller
// defaults to zero.
fn parse_age_value(raw: &str) -> Option<u64> {
    raw.split([';', ',']).next()?.trim().parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::*;
    use trillium_http::{KnownHeaderName::*, Method, Status};

    // §5.2.2.1: max-age=N is fresh for N, stale after.
    #[test]
    fn max_age_freshness_window() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600")],
        );
        let policy = policy_from(&conn, t0(), private_cache());
        assert!(!policy.is_stale(t0()));
        assert!(!policy.is_stale(at(t0(), 599)));
        assert!(policy.is_stale(at(t0(), 600)));
        assert_eq!(policy.time_to_live(t0()), Duration::from_secs(600));
        assert_eq!(policy.time_to_live(at(t0(), 100)), Duration::from_secs(500));
        assert_eq!(policy.time_to_live(at(t0(), 700)), Duration::ZERO);
    }

    // §5.2.2.10: shared cache prefers s-maxage over max-age.
    #[test]
    fn shared_cache_prefers_s_maxage_over_max_age() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600, s-maxage=300")],
        );
        let shared = policy_from(&conn, t0(), shared_cache());
        let private = policy_from(&conn, t0(), private_cache());
        assert!(shared.is_stale(at(t0(), 301)));
        assert!(!shared.is_stale(at(t0(), 299)));
        assert!(!private.is_stale(at(t0(), 301)));
        assert!(private.is_stale(at(t0(), 601)));
    }

    // §5.2.2.4: no-cache is immediately stale (must always revalidate).
    #[test]
    fn no_cache_is_immediately_stale() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600, no-cache")],
        );
        let policy = policy_from(&conn, t0(), private_cache());
        assert!(policy.is_stale(t0()));
        assert_eq!(policy.time_to_live(t0()), Duration::ZERO);
    }

    // §4.1: Vary: * means we can never reuse for a future request.
    #[test]
    fn vary_star_is_immediately_stale() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600"), (Vary, "*")],
        );
        let policy = policy_from(&conn, t0(), private_cache());
        assert!(policy.is_stale(t0()));
    }

    // §5.2.2.8: shared cache treats proxy-revalidate as immediate stale.
    #[test]
    fn proxy_revalidate_immediately_stale_on_shared_only() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600, proxy-revalidate")],
        );
        assert!(policy_from(&conn, t0(), shared_cache()).is_stale(t0()));
        assert!(!policy_from(&conn, t0(), private_cache()).is_stale(t0()));
    }

    // Set-Cookie on shared cache without public/immutable: treat as
    // stale (a defensive policy choice — RFC permits storing but it's
    // almost never what the operator wants).
    #[test]
    fn set_cookie_on_shared_without_public_is_stale() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600"), (SetCookie, "k=v")],
        );
        assert!(policy_from(&conn, t0(), shared_cache()).is_stale(t0()));
        assert!(!policy_from(&conn, t0(), private_cache()).is_stale(t0()));
    }

    #[test]
    fn set_cookie_on_shared_with_public_respects_max_age() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "public, max-age=600"), (SetCookie, "k=v")],
        );
        assert!(!policy_from(&conn, t0(), shared_cache()).is_stale(t0()));
    }

    // §5.2.2.2: immutable provides a default freshness lifetime.
    #[test]
    fn immutable_uses_immutable_min_ttl() {
        let conn = exchange(Method::Get, &[], Status::Ok, &[(CacheControl, "immutable")]);
        let policy = policy_from(&conn, t0(), private_cache());
        // Default immutable_min_time_to_live is 24h.
        assert!(!policy.is_stale(at(t0(), 23 * 3600)));
        assert!(policy.is_stale(at(t0(), 24 * 3600 + 1)));
    }

    // §4.2.1: Expires header — but max-age (when present) wins.
    #[test]
    fn max_age_overrides_expires() {
        let date = httpdate::fmt_http_date(t0());
        let expires_far = httpdate::fmt_http_date(at(t0(), 86400));
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[
                (Date, &date),
                (Expires, &expires_far),
                (CacheControl, "max-age=60"),
            ],
        );
        let policy = policy_from(&conn, t0(), private_cache());
        assert!(policy.is_stale(at(t0(), 61)));
    }

    // §4.2.1: Expires alone (no max-age) — diff is from origin's Date.
    #[test]
    fn expires_alone_uses_date_diff() {
        let date = httpdate::fmt_http_date(t0());
        let expires = httpdate::fmt_http_date(at(t0(), 600));
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(Date, &date), (Expires, &expires)],
        );
        let policy = policy_from(&conn, t0(), private_cache());
        assert!(!policy.is_stale(at(t0(), 599)));
        assert!(policy.is_stale(at(t0(), 601)));
    }

    // §4.2.1: invalid Expires → immediately stale.
    #[test]
    fn invalid_expires_is_stale() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(Expires, "not a real date")],
        );
        let policy = policy_from(&conn, t0(), private_cache());
        assert!(policy.is_stale(t0()));
    }

    // §4.2.2: heuristic freshness from Last-Modified (10% of age).
    #[test]
    fn heuristic_freshness_from_last_modified() {
        // Last-Modified 100 days ago → heuristic 10 days fresh.
        let last_modified = at(t0(), 0) - Duration::from_secs(100 * 86400);
        let date = httpdate::fmt_http_date(t0());
        let lm = httpdate::fmt_http_date(last_modified);
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(Date, &date), (LastModified, &lm)],
        );
        let policy = policy_from(&conn, t0(), private_cache());
        assert!(!policy.is_stale(at(t0(), 9 * 86400)));
        assert!(policy.is_stale(at(t0(), 11 * 86400)));
    }

    // §4.2.3: Age header value contributes to current age.
    #[test]
    fn age_header_increments_current_age() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600"), (Age, "300")],
        );
        let policy = policy_from(&conn, t0(), private_cache());
        assert_eq!(policy.age(t0()), Duration::from_secs(300));
        assert_eq!(policy.time_to_live(t0()), Duration::from_secs(300));
        assert!(policy.is_stale(at(t0(), 301)));
    }

    // §4.2.3: resident time accumulates as wall-clock advances.
    #[test]
    fn resident_time_adds_to_age() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600")],
        );
        let policy = policy_from(&conn, t0(), private_cache());
        assert_eq!(policy.age(t0()), Duration::ZERO);
        assert_eq!(policy.age(at(t0(), 250)), Duration::from_secs(250));
    }

    // ===== §4.2.4 / RFC 5861 stale extensions =====

    #[test]
    fn swr_window_parsed_from_response() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=60, stale-while-revalidate=300")],
        );
        let policy = policy_from(&conn, t0(), private_cache());
        assert_eq!(
            policy.stale_while_revalidate_window(),
            Duration::from_secs(300)
        );
        assert_eq!(policy.stale_if_error_window(), Duration::ZERO);
    }

    #[test]
    fn sie_window_parsed_from_response() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=60, stale-if-error=300")],
        );
        let policy = policy_from(&conn, t0(), private_cache());
        assert_eq!(policy.stale_if_error_window(), Duration::from_secs(300));
        assert_eq!(policy.stale_while_revalidate_window(), Duration::ZERO);
    }

    // Fresh entries are not SWR/SIE eligible.
    #[test]
    fn fresh_response_not_eligible_for_stale_serving() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600, stale-while-revalidate=300")],
        );
        let policy = policy_from(&conn, t0(), private_cache());
        assert!(!policy.is_swr_eligible(t0()));
        assert!(!policy.is_swr_eligible(at(t0(), 500)));
    }

    // Stale within the SWR window → eligible.
    #[test]
    fn stale_within_swr_window_is_eligible() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=60, stale-while-revalidate=300")],
        );
        let policy = policy_from(&conn, t0(), private_cache());
        // age=100, max_age=60 → staleness=40 < 300 (window).
        assert!(policy.is_swr_eligible(at(t0(), 100)));
    }

    // Stale past the SWR window → not eligible.
    #[test]
    fn stale_past_swr_window_not_eligible() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=60, stale-while-revalidate=300")],
        );
        let policy = policy_from(&conn, t0(), private_cache());
        // age=400, max_age=60 → staleness=340 > 300.
        assert!(!policy.is_swr_eligible(at(t0(), 400)));
    }

    // must-revalidate disables both SWR and SIE.
    #[test]
    fn must_revalidate_disables_stale_serving() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(
                CacheControl,
                "max-age=60, must-revalidate, stale-while-revalidate=300, stale-if-error=300",
            )],
        );
        let policy = policy_from(&conn, t0(), private_cache());
        assert!(!policy.is_swr_eligible(at(t0(), 100)));
        assert!(!policy.is_sie_eligible(at(t0(), 100)));
    }

    // proxy-revalidate disables stale serving for shared caches only.
    #[test]
    fn proxy_revalidate_disables_stale_serving_on_shared() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(
                CacheControl,
                "max-age=60, proxy-revalidate, stale-while-revalidate=300",
            )],
        );
        let shared = policy_from(&conn, t0(), shared_cache());
        let private = policy_from(&conn, t0(), private_cache());
        // Shared cache: proxy-revalidate forbids stale serving.
        assert!(!shared.is_swr_eligible(at(t0(), 100)));
        // But max_age=0 for shared due to proxy-revalidate (freshness
        // commit) → stale immediately, but stale serving still
        // forbidden.
        // Private cache: proxy-revalidate is a no-op.
        assert!(private.is_swr_eligible(at(t0(), 100)));
    }

    // SIE window independent from SWR.
    #[test]
    fn sie_eligibility_uses_sie_window() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=60, stale-if-error=300")],
        );
        let policy = policy_from(&conn, t0(), private_cache());
        assert!(policy.is_sie_eligible(at(t0(), 100)));
        assert!(!policy.is_sie_eligible(at(t0(), 400)));
        // No SWR set → never SWR-eligible.
        assert!(!policy.is_swr_eligible(at(t0(), 100)));
    }

    // No SWR/SIE directive present → no eligibility, even when stale.
    #[test]
    fn no_directive_means_no_eligibility() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=60")],
        );
        let policy = policy_from(&conn, t0(), private_cache());
        assert!(policy.is_stale(at(t0(), 100)));
        assert!(!policy.is_swr_eligible(at(t0(), 100)));
        assert!(!policy.is_sie_eligible(at(t0(), 100)));
    }

    // ===== `parse_age_value` (RFC 9111 §5.1 + cache-tests corpus quirks) =====

    #[test]
    fn age_parses_plain_integer() {
        assert_eq!(parse_age_value("7200"), Some(7200));
        assert_eq!(parse_age_value("0"), Some(0));
        // u64 fits 2^31 and beyond — RFC 9111 §1.2.2 only requires 31 bits.
        assert_eq!(parse_age_value("2147483648"), Some(2147483648));
    }

    #[test]
    fn age_takes_first_segment_of_comma_list() {
        // RFC 9110 §5.3 line-folding: `Age: 7200, 0` collapses two header lines.
        // Age is single-valued, so the first segment wins.
        assert_eq!(parse_age_value("7200, 0"), Some(7200));
        assert_eq!(parse_age_value("0, 7200"), Some(0));
        assert_eq!(parse_age_value("0, 0"), Some(0));
    }

    #[test]
    fn age_takes_prefix_before_structured_field_parameter() {
        // RFC 8941 parameter syntax: `7200;foo=bar` — Age isn't a structured
        // field, but cache-tests treats the integer prefix as the value.
        assert_eq!(parse_age_value("7200;foo=bar"), Some(7200));
        assert_eq!(parse_age_value("7200;foo=111"), Some(7200));
    }

    // ===== RFC 9213: targeted CDN-Cache-Control freshness =====

    // §2.2: when CDN-CC takes effect on a shared cache, max-age comes from
    // CDN-CC and Expires is ignored. Here CC says max-age=1 + Expires=1
    // (both immediately stale) but CDN-CC says max-age=10000 → fresh.
    #[test]
    fn shared_cache_uses_cdn_cc_max_age_and_ignores_cc_and_expires() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[
                (Date, "Thu, 01 Jan 1970 00:00:00 GMT"),
                (Expires, "Thu, 01 Jan 1970 00:00:01 GMT"),
                (CacheControl, "max-age=1"),
                (CdnCacheControl, "max-age=10000"),
            ],
        );
        let policy = policy_from(&conn, t0(), shared_cache());
        assert_eq!(policy.max_age(), Duration::from_secs(10000));
        // 100s past response_time, with max_age=10000 → still fresh.
        assert!(!policy.is_stale(at(t0(), 100)));
    }

    // §2.2: private cache MUST ignore CDN-CC and use only CC + Expires.
    #[test]
    fn private_cache_ignores_cdn_cc_for_freshness() {
        let conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[
                (Date, "Thu, 01 Jan 1970 00:00:00 GMT"),
                (CacheControl, "max-age=1"),
                (CdnCacheControl, "max-age=10000"),
            ],
        );
        let policy = policy_from(&conn, t0(), private_cache());
        assert_eq!(policy.max_age(), Duration::from_secs(1));
        assert!(policy.is_stale(at(t0(), 100)));
    }

    #[test]
    fn age_rejects_non_integer_forms() {
        // Floats, negatives, and non-numeric content all return None so the
        // caller defaults Age to zero (RFC 9111 §5.1: ignore invalid).
        assert_eq!(parse_age_value("7200.0"), None);
        assert_eq!(parse_age_value("-7200"), None);
        assert_eq!(parse_age_value("abc"), None);
        assert_eq!(parse_age_value(""), None);
    }
}
