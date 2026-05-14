//! Stored cache policy — the value type for a captured exchange.
//!
//! Section-specific logic lives in sibling modules:
//! - [`crate::storability`] — RFC 9111 §3 (`is_storable`)
//! - [`crate::freshness`]   — RFC 9111 §4.2 (`age` / `time_to_live` / `is_stale`)
//! - [`crate::validation`]  — RFC 9111 §4.3 (`before_request`)
//!
//! Portions of this and the sibling modules are derived from
//! [`rusty-http-cache-semantics`](https://github.com/kornelski/rusty-http-cache-semantics)
//! by Kornel Lesiński, used under the BSD-2-Clause license. See
//! `LICENSE-BSD-2-CLAUSE-http-cache-semantics` at the crate root for the
//! original notice.

use std::time::{Duration, SystemTime};
use trillium_caching_headers::{CacheControlDirective, CacheControlHeader, CachingHeadersExt};
use trillium_http::{Headers, KnownHeaderName, Method, Status};

/// Resolve the effective response Cache-Control for a response, applying
/// the RFC 9213 §2.2 targeted-field override:
/// when the cache is shared and a non-empty, validly-structured
/// `CDN-Cache-Control` is present, it fully replaces `Cache-Control` (and
/// downstream code MUST also ignore `Expires`, signalled by the returned
/// `targeted_cc_in_effect`). Per §2.1, parse-error or empty targeted
/// fields MUST be ignored.
pub(crate) fn effective_response_cache_control(
    response_headers: &Headers,
    options: &CacheOptions,
) -> (Option<CacheControlHeader>, bool) {
    if options.shared
        && let Some(raw) = response_headers.get_str(KnownHeaderName::CdnCacheControl)
        && looks_like_valid_sf_dictionary(raw)
        && let Some(cdn_cc) = response_headers.cdn_cache_control()
        && !cdn_cc.is_empty()
    {
        return (Some(cdn_cc), true);
    }
    (response_headers.cache_control(), false)
}

/// RFC 9213 §2.1: targeted fields are Dictionary Structured Fields (RFC
/// 8941 §3.2). A full SF parser is out of scope, but this catches the
/// common "garbage trailing tokens" case (e.g. `max-age=10000, &&&&&`) by
/// requiring each comma-separated member to begin with a valid sf-key
/// (RFC 8941 §3.1.2). Unrecognized but well-formed members are kept; the
/// `CacheControlHeader` parser handles those as `UnknownDirective`.
fn looks_like_valid_sf_dictionary(s: &str) -> bool {
    let s = s.trim();
    if s.is_empty() {
        return false;
    }
    s.split(',').all(|member| {
        let member = member.trim();
        if member.is_empty() {
            return false;
        }
        let key = member.split_once('=').map_or(member, |(k, _)| k).trim_end();
        is_valid_sf_key(key)
    })
}

// RFC 8941 §3.1.2 grammar requires sf-key to be lowercase, but
// `CacheControlHeader::parse` lowercases the whole header before parsing
// (matching the case-insensitive convention of Cache-Control directives).
// We mirror that here so a permissive parser isn't gated by a strict
// validator — a server sending `CDN-Cache-Control: MaX-aGe=3600` is
// honored, while genuinely-invalid keys like `&&&&&` are still rejected.
fn is_valid_sf_key(s: &str) -> bool {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_alphabetic() && first != '*' {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '*'))
}

/// Configuration that controls cache behavior.
#[derive(Debug, Copy, Clone, fieldwork::Fieldwork)]
#[fieldwork(get, set, get_mut, with, rename_predicates)]
pub struct CacheOptions {
    /// whether the cache is treated as a *shared cache*
    ///
    /// Shared cache, suitable for a proxy or cdn: `s-maxage` is honored, `private` responses are
    /// refused, and `Authorization`-bearing requests require explicit opt-in (`public`,
    /// `s-maxage`, or `must-revalidate`)
    ///
    /// Non-shared-cache (the default) treats the cache as a single-user (browser-style) private
    /// cache.
    ///
    /// Default: false
    pub(crate) shared: bool,

    /// heuristic-freshness ratio
    ///
    /// When a response has no explicit expiration but does have `Last-Modified`, freshness
    /// lifetime is computed as `cache_heuristic * (Date - Last-Modified)`.
    ///
    /// Default: 0.1 (10%)
    pub(crate) cache_heuristic: f32,

    /// the default freshness lifetime for responses with `Cache-Control:
    /// immutable` and no other expiration
    ///
    /// Default: 24h
    #[field(copy)]
    pub(crate) immutable_min_time_to_live: Duration,
}

impl Default for CacheOptions {
    fn default() -> Self {
        Self {
            shared: false,
            cache_heuristic: 0.1,
            immutable_min_time_to_live: Duration::from_secs(24 * 3600),
        }
    }
}

/// Captured snapshot of a request/response exchange.
///
/// `CachePolicy` is the value type that [`Cache`][crate::Cache] hands to
/// a [`CacheStorage`][crate::CacheStorage] backend for storage and
/// retrieval. To a storage backend it's an opaque blob: store it,
/// return it on lookup, and use [`same_variant_as`][Self::same_variant_as]
/// to decide whether a new entry replaces an existing one or appends as
/// a new `Vary` variant.
#[derive(Debug, Clone)]
pub struct CachePolicy {
    pub(crate) request_method: Method,
    /// Captured request header values for the headers named in the
    /// response's `Vary`. Empty if no `Vary` header. Each entry is
    /// `(lowercase-name, Option<value>)`; `None` value means the header
    /// was absent on the original request.
    pub(crate) vary_snapshot: Vec<(String, Option<String>)>,
    pub(crate) response_status: Status,
    pub(crate) response_headers: Headers,
    pub(crate) response_cache_control: Option<CacheControlHeader>,
    /// True when `response_cache_control` came from a targeted field
    /// (RFC 9213 — currently `CDN-Cache-Control`) rather than `Cache-Control`.
    /// Per §2.2, the cache MUST then ignore both `Cache-Control` and
    /// `Expires` for caching policy decisions; freshness math uses this flag
    /// to suppress the `Expires` fallback.
    pub(crate) targeted_cc_in_effect: bool,
    pub(crate) response_time: SystemTime,
    pub(crate) options: CacheOptions,
}

impl CachePolicy {
    /// True when `other` would select the same stored variant as `self`
    /// for the same [`CacheKey`][crate::CacheKey] — i.e. both responses
    /// were captured with matching values for every header listed in
    /// `Vary`. [`CacheStorage`][crate::CacheStorage] implementations use
    /// this to decide whether a `put` should replace an existing variant
    /// or append a new one.
    pub fn same_variant_as(&self, other: &Self) -> bool {
        self.vary_snapshot == other.vary_snapshot
    }

    // Build a stored policy from a completed exchange. `response_time` is the
    // wall-clock time the response was received from the origin.
    pub(crate) fn new(
        request_method: Method,
        request_headers: &Headers,
        response_status: Status,
        response_headers: Headers,
        response_time: SystemTime,
        options: CacheOptions,
    ) -> Self {
        let (mut response_cache_control, targeted_cc_in_effect) =
            effective_response_cache_control(&response_headers, &options);

        // RFC 9111 §5.4: when no Cache-Control is present, treat
        // `Pragma: no-cache` as if `Cache-Control: no-cache` were set. This
        // is suppressed when a targeted field took effect (Pragma is part of
        // the Cache-Control / Expires family the targeted-field rule
        // displaces).
        if response_cache_control.is_none()
            && response_headers
                .get_str(KnownHeaderName::Pragma)
                .is_some_and(|p| p.contains("no-cache"))
        {
            response_cache_control = Some(CacheControlHeader::from(CacheControlDirective::NoCache));
        }

        let vary_snapshot = build_vary_snapshot(&response_headers, request_headers);

        Self {
            request_method,
            vary_snapshot,
            response_status,
            response_headers,
            response_cache_control,
            targeted_cc_in_effect,
            response_time,
            options,
        }
    }
}

fn build_vary_snapshot(
    response_headers: &Headers,
    request_headers: &Headers,
) -> Vec<(String, Option<String>)> {
    // RFC 9110 §5.3: multiple `Vary:` header lines are equivalent to one
    // line with comma-separated values. `get_str` returns None when more
    // than one line is present (HeaderValues::one), so iterate the values
    // and flatten — otherwise we'd silently miss a `Vary: *` on a second
    // line and incorrectly serve a non-matching cached entry.
    let Some(values) = response_headers.get_values(KnownHeaderName::Vary) else {
        return Vec::new();
    };
    values
        .iter()
        .filter_map(|v| v.as_str())
        .flat_map(|line| line.split(','))
        .map(str::trim)
        .filter(|n| !n.is_empty())
        .map(|name| {
            let lower = name.to_ascii_lowercase();
            let value = request_headers.get_str(lower.as_str()).map(str::to_string);
            (lower, value)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::*;
    use trillium_client::ConnExt;
    use trillium_http::KnownHeaderName::*;

    // RFC 9110 §5.3: multiple `Vary:` lines fold to one comma-list.
    // `Headers::get_str` returns None for multi-value headers, so a naive
    // implementation would silently miss the second line and over-cache.
    #[test]
    fn vary_snapshot_handles_multiple_header_lines() {
        let mut conn = exchange(
            Method::Get,
            &[(AcceptEncoding, "gzip"), (AcceptLanguage, "en-US")],
            Status::Ok,
            &[(Vary, "Accept-Encoding")],
        );
        // Append a second `Vary:` line — the test fixture's `insert`
        // would replace, so we have to call append directly.
        conn.response_headers_mut().append(Vary, "Accept-Language");

        let policy = policy_from(&conn, SystemTime::now(), private_cache());
        assert_eq!(
            policy.vary_snapshot,
            vec![
                ("accept-encoding".to_string(), Some("gzip".to_string())),
                ("accept-language".to_string(), Some("en-US".to_string())),
            ]
        );
    }

    // RFC 9111 §4.1: `Vary: *` means "never reuse" — a `*` on any line
    // should be honored even when paired with empty or other tokens.
    #[test]
    fn vary_snapshot_captures_star_from_second_line() {
        let mut conn = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(Vary, "")], // empty first line
        );
        conn.response_headers_mut().append(Vary, "*");

        let policy = policy_from(&conn, SystemTime::now(), private_cache());
        // The `*` survives flattening so vary_matches will return false.
        assert!(policy.vary_snapshot.iter().any(|(name, _)| name == "*"));
    }

    #[test]
    fn vary_snapshot_captures_named_request_headers() {
        let conn = exchange(
            Method::Get,
            &[(AcceptEncoding, "gzip"), (AcceptLanguage, "en-US")],
            Status::Ok,
            &[(Vary, "Accept-Encoding, Accept-Language")],
        );
        let policy = policy_from(&conn, SystemTime::now(), private_cache());
        assert_eq!(
            policy.vary_snapshot,
            vec![
                ("accept-encoding".to_string(), Some("gzip".to_string())),
                ("accept-language".to_string(), Some("en-US".to_string())),
            ]
        );
    }

    #[test]
    fn sf_dictionary_validator() {
        // Valid sf-key starts with [a-z*] and contains [a-z0-9_*\-.]
        assert!(looks_like_valid_sf_dictionary("max-age=600"));
        assert!(looks_like_valid_sf_dictionary("no-store"));
        assert!(looks_like_valid_sf_dictionary("max-age=600, no-store"));
        // Wrong-type values are caught downstream by CC parsing, not here —
        // we only validate keys at this layer.
        assert!(looks_like_valid_sf_dictionary(r#"max-age="600""#));

        // Mixed-case keys are accepted — `CacheControlHeader::parse`
        // lowercases before parsing, so this matches the actual parser's
        // case-insensitive behavior.
        assert!(looks_like_valid_sf_dictionary("MaX-aGe=3600"));

        // Invalid: garbage-character keys.
        assert!(!looks_like_valid_sf_dictionary("max-age=10000, &&&&&"));
        assert!(!looks_like_valid_sf_dictionary("&&&&&"));
        // Invalid: empty.
        assert!(!looks_like_valid_sf_dictionary(""));
        assert!(!looks_like_valid_sf_dictionary("   "));
        // Invalid: trailing/middle empty members from stray commas.
        assert!(!looks_like_valid_sf_dictionary("max-age=600,"));
    }

    #[test]
    fn vary_snapshot_records_absent_request_header_as_none() {
        let conn = exchange(Method::Get, &[], Status::Ok, &[(Vary, "Accept-Encoding")]);
        let policy = policy_from(&conn, SystemTime::now(), private_cache());
        assert_eq!(
            policy.vary_snapshot,
            vec![("accept-encoding".to_string(), None)]
        );
    }
}
