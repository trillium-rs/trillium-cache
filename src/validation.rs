//! RFC 9111 §4.3 — *Validation*.
//!
//! The "is the cached response usable for this new request?" decision,
//! plus the conditional-revalidation request that lets the origin
//! return 304 Not Modified instead of replaying the body.

use crate::policy::CachePolicy;
use std::time::SystemTime;
use trillium_caching_headers::CachingHeadersExt;
use trillium_http::{Headers, KnownHeaderName, Method, Status};

impl CachePolicy {
    /// RFC 9111 §4.2 + §4.3: decide what to do with a new request given
    /// this stored response.
    ///
    /// Returns:
    /// - [`BeforeRequest::Fresh`] when the cached response satisfies the request directly. The
    ///   caller pairs the returned [`CachedResponse`] head with the body bytes from storage.
    /// - [`BeforeRequest::Stale`] otherwise. The caller should send the request to the origin using
    ///   the supplied `request_headers` (which may carry conditional validators for a 304
    ///   round-trip). The `matches` flag is `true` when the stored entry is the right one for this
    ///   request (vary signature matches); `false` means the handler should consider another stored
    ///   candidate or send an unconditional request.
    pub fn before_request(&self, request_headers: &Headers, now: SystemTime) -> BeforeRequest {
        let matches = self.vary_matches(request_headers);

        if matches && self.satisfies_without_revalidation(request_headers, now) {
            // RFC 9111 §4.3.2 + RFC 9110 §13.2.2: a cache that can satisfy
            // the request from a stored response MUST evaluate the request's
            // conditional headers itself rather than serving the full body.
            if self.inbound_conditional_matches(request_headers) {
                return BeforeRequest::NotModified(self.cached_response_304(now));
            }
            BeforeRequest::Fresh(self.cached_response(now))
        } else {
            BeforeRequest::Stale {
                request_headers: self.revalidation_request_headers(request_headers),
                matches,
            }
        }
    }

    // RFC 9110 §13.1.2 + §13.1.4: evaluate the request's `If-None-Match`
    // (preferred) or `If-Modified-Since` against this stored response.
    // Returns true when the precondition fails — i.e., the client already
    // has the current representation and we should reply 304.
    //
    // Per §13.1.4, `If-Modified-Since` MUST be ignored when `If-None-Match`
    // is present; the latter is the more accurate replacement.
    fn inbound_conditional_matches(&self, request_headers: &Headers) -> bool {
        if let Some(inm) = request_headers.get_str(KnownHeaderName::IfNoneMatch) {
            return inm_matches(inm, self.response_headers.get_str(KnownHeaderName::Etag));
        }
        if let Some(ims) = request_headers.get_str(KnownHeaderName::IfModifiedSince) {
            return ims_matches(
                ims,
                self.response_headers.get_str(KnownHeaderName::LastModified),
            );
        }
        false
    }

    // RFC 9110 §15.4.5: 304 response carries only the metadata useful for
    // updating a previously cached response — validators (ETag,
    // Last-Modified), Date, Vary, Content-Location, plus Cache-Control and
    // Expires for client-side cache-freshness math. Strips body-related
    // headers (Content-Length, Content-Type, Content-Encoding, ...).
    fn cached_response_304(&self, now: SystemTime) -> CachedResponse {
        const HEADERS_TO_KEEP: &[KnownHeaderName] = &[
            KnownHeaderName::Date,
            KnownHeaderName::Etag,
            KnownHeaderName::LastModified,
            KnownHeaderName::Vary,
            KnownHeaderName::CacheControl,
            KnownHeaderName::ContentLocation,
            KnownHeaderName::Expires,
        ];
        let mut headers = Headers::new();
        for name in HEADERS_TO_KEEP {
            if let Some(value) = self.response_headers.get_values(*name) {
                headers.insert(*name, value.clone());
            }
        }
        let age = self.age(now);
        headers.insert(KnownHeaderName::Age, age.as_secs().to_string());
        CachedResponse {
            status: Status::NotModified,
            headers,
        }
    }

    // RFC 9111 §4.1: the stored response's selecting headers (those
    // listed in its `Vary`) must equal those on the new request.
    fn vary_matches(&self, request_headers: &Headers) -> bool {
        for (name, stored_value) in &self.vary_snapshot {
            if name == "*" {
                return false;
            }
            let new_value = request_headers.get_str(name.as_str());
            if new_value != stored_value.as_deref() {
                return false;
            }
        }
        true
    }

    // RFC 9111 §4 + §5.2.1 request directives that allow / disallow
    // serving the stored response without contacting the origin.
    fn satisfies_without_revalidation(&self, request_headers: &Headers, now: SystemTime) -> bool {
        let req_cc = request_headers.cache_control();

        // §5.2.1.4 + §5.4: request `no-cache` (or legacy `Pragma: no-cache`)
        // forces revalidation.
        if req_cc.as_ref().is_some_and(|cc| cc.is_no_cache())
            || request_headers
                .get_str(KnownHeaderName::Pragma)
                .is_some_and(|p| p.contains("no-cache"))
        {
            return false;
        }

        // §5.2.1.1: request `max-age=N` — stored must be at most N old.
        if let Some(req_max_age) = req_cc.as_ref().and_then(|cc| cc.max_age())
            && self.age(now) > req_max_age
        {
            return false;
        }

        // §5.2.1.3: request `min-fresh=N` — stored must remain fresh
        // for at least N more seconds.
        if let Some(min_fresh) = req_cc.as_ref().and_then(|cc| cc.min_fresh())
            && self.time_to_live(now) < min_fresh
        {
            return false;
        }

        // §4.2.4: serving stale.
        if self.is_stale(now) {
            // §5.2.1.2: request `max-stale[=N]` allows stale up to N
            // seconds. `max-stale` without a value means "any staleness
            // is fine".
            let max_stale = req_cc.as_ref().and_then(|cc| cc.max_stale());
            let must_revalidate = self
                .response_cache_control
                .as_ref()
                .is_some_and(|cc| cc.must_revalidate());
            let allows_stale = !must_revalidate
                && match max_stale {
                    None => false,
                    Some(None) => true,
                    Some(Some(allowed)) => allowed > self.age(now) - self.max_age(),
                };
            if !allows_stale {
                return false;
            }
        }

        true
    }

    // RFC 9111 §4.3.1: build a conditional revalidation request from
    // the new incoming request. Carries forward the new request's
    // headers (minus hop-by-hop), then layers on validators derived
    // from the stored response's `ETag` / `Last-Modified`.
    fn revalidation_request_headers(&self, request_headers: &Headers) -> Headers {
        let mut headers = copy_without_hop_by_hop_headers(request_headers);

        // We don't support partial responses; drop any range-validator
        // the caller might have set.
        headers.remove(KnownHeaderName::IfRange);

        // §4.3.1: forward stored ETag as `If-None-Match`.
        if let Some(etag) = self.response_headers.get_str(KnownHeaderName::Etag) {
            let combined = match headers.get_str(KnownHeaderName::IfNoneMatch) {
                Some(existing) => format!("{existing}, {etag}"),
                None => etag.to_string(),
            };
            headers.insert(KnownHeaderName::IfNoneMatch, combined);
        }

        // RFC 9110 §13.1.3: weak validators are forbidden in some
        // revalidation contexts.
        let forbids_weak_validators = self.request_method != Method::Get
            || headers.has_header(KnownHeaderName::AcceptRanges)
            || headers.has_header(KnownHeaderName::IfMatch)
            || headers.has_header(KnownHeaderName::IfUnmodifiedSince);

        if forbids_weak_validators {
            headers.remove(KnownHeaderName::IfModifiedSince);

            if let Some(inm) = headers.get_str(KnownHeaderName::IfNoneMatch) {
                let strong: String = inm
                    .split(',')
                    .map(str::trim)
                    .filter(|t| !t.starts_with("W/"))
                    .collect::<Vec<_>>()
                    .join(", ");
                if strong.is_empty() {
                    headers.remove(KnownHeaderName::IfNoneMatch);
                } else {
                    headers.insert(KnownHeaderName::IfNoneMatch, strong);
                }
            }
        } else if !headers.has_header(KnownHeaderName::IfModifiedSince) {
            // §4.3.1: forward stored `Last-Modified` as `If-Modified-Since`
            // when we can use weak validators.
            if let Some(lm) = self.response_headers.get_str(KnownHeaderName::LastModified) {
                headers.insert(KnownHeaderName::IfModifiedSince, lm.to_string());
            }
        }

        headers
    }

    /// RFC 9111 §3.2: the response head to return on a cache hit.
    /// Updates `Age`, strips hop-by-hop headers, preserves the origin's
    /// `Date`. Pair with the body bytes from storage to construct the
    /// served response.
    ///
    /// We deliberately preserve the stored `Date` rather than rewriting
    /// it to `now`: per RFC 9110 §6.6.1, `Date` is the time the response
    /// was generated by the origin, and rewriting it to the cache's
    /// current clock is misleading. Recipients combine `Date` and `Age`
    /// to reason about effective freshness.
    ///
    /// `before_request` already returns this for the fresh path; this
    /// accessor is exposed for callers that need to serve a stale entry
    /// directly — e.g. a `stale-if-error` fallback when origin
    /// revalidation fails.
    pub fn cached_response(&self, now: SystemTime) -> CachedResponse {
        let mut headers = copy_without_hop_by_hop_headers(&self.response_headers);
        let age = self.age(now);
        headers.insert(KnownHeaderName::Age, age.as_secs().to_string());
        CachedResponse {
            status: self.response_status,
            headers,
        }
    }

    /// RFC 9111 §3.2 + §4.3.4: integrate the origin's response to a
    /// revalidation request.
    ///
    /// The supplied `request_headers` / `new_status` / `new_response_headers` describe the
    /// just-received origin response; `response_time` is the wall-clock time of receipt.
    ///
    /// Returns:
    /// - [`AfterResponse::NotModified`] if the origin returned 304 with validators matching this
    ///   stored entry. The handler should reuse the cached body and serve the returned
    ///   `CachedResponse` head. The included `CachePolicy` carries the merged stored+304 headers
    ///   and a refreshed `response_time`; replace the stored entry with it.
    /// - [`AfterResponse::Modified`] otherwise. The handler should read the body off the origin
    ///   response and serve it. Call [`CachePolicy::is_storable`] before persisting the new entry.
    pub fn after_response(
        &self,
        request_headers: &Headers,
        new_status: Status,
        new_response_headers: &Headers,
        response_time: SystemTime,
    ) -> AfterResponse {
        // RFC 9111 §3.2: a 304 received in response to a conditional we
        // sent with our stored validators is, by construction, for that
        // stored entry — "the cache MAY treat any 304 (Not Modified)
        // response that is generated for that stored response as a
        // successful update of that stored response, regardless of which
        // validator the new response is for". We always send our stored
        // validators in `revalidation_request_headers`, so any 304 we
        // receive is one we asked for. Trust it and let the header merge
        // bring across any new validators the origin chose to send.
        let entity_matches = new_status == Status::NotModified;

        let (final_status, final_response_headers) = if entity_matches {
            // §3.2: merge stored + 304 headers, except body-description
            // headers (which would lie about the cached body).
            (
                self.response_status,
                merge_revalidation_headers(&self.response_headers, new_response_headers),
            )
        } else {
            (new_status, new_response_headers.clone())
        };

        if entity_matches {
            let new_policy = CachePolicy::new(
                self.request_method,
                request_headers,
                final_status,
                final_response_headers,
                response_time,
                self.options,
            );
            let new_response = new_policy.cached_response(response_time);
            AfterResponse::NotModified(new_policy, new_response)
        } else {
            AfterResponse::Modified
        }
    }
}

// RFC 9111 §3.2 — *Updating Stored Header Fields*. Build the union of
// stored headers and the 304 response's headers, with the 304's values
// winning except for body-description headers that the cached body's
// integrity depends on.
fn merge_revalidation_headers(stored: &Headers, new: &Headers) -> Headers {
    let mut out = stored.clone();
    for (name, values) in new.iter() {
        if is_excluded_from_revalidation_update(&name) {
            continue;
        }
        out.insert(name.into_owned(), values.clone());
    }
    out
}

fn is_excluded_from_revalidation_update(name: &trillium_http::HeaderName<'_>) -> bool {
    name == KnownHeaderName::ContentLength
        || name == KnownHeaderName::ContentEncoding
        || name == KnownHeaderName::TransferEncoding
        || name == KnownHeaderName::ContentRange
}

// RFC 9110 §13.1.2: `If-None-Match` MUST be evaluated using weak
// comparison (validators "match" if their opaque-tag bytes are equal,
// regardless of whether either or both are weak). The header value is a
// list of entity-tags (or `*`).
fn inm_matches(if_none_match: &str, cached_etag: Option<&str>) -> bool {
    let inm = if_none_match.trim();
    if inm == "*" {
        // §13.1.2: "*" matches if the origin has any current
        // representation. We're in the cache-hit path, so it does.
        return true;
    }
    let Some(cached_opaque) = cached_etag.and_then(etag_opaque) else {
        return false;
    };
    iter_etag_opaques(inm).any(|tag| tag == cached_opaque)
}

// RFC 9110 §13.1.4: cached `Last-Modified` no later than the IMS date
// means the client already has the current representation. Both dates
// must parse for the precondition to fail (304); a parse error makes the
// header non-evaluable, which RFC 9110 says to treat as if absent.
fn ims_matches(if_modified_since: &str, cached_last_modified: Option<&str>) -> bool {
    let Ok(ims) = httpdate::parse_http_date(if_modified_since) else {
        return false;
    };
    let Some(lm_str) = cached_last_modified else {
        return false;
    };
    let Ok(lm) = httpdate::parse_http_date(lm_str) else {
        return false;
    };
    lm <= ims
}

// Strip optional `W/` weak prefix and surrounding double-quotes; return
// the opaque-tag bytes for weak-equality comparison. Returns None if the
// input isn't a syntactically valid entity-tag.
fn etag_opaque(etag: &str) -> Option<&str> {
    let etag = etag.trim();
    let etag = etag.strip_prefix("W/").unwrap_or(etag);
    let inner = etag.strip_prefix('"')?;
    inner.strip_suffix('"')
}

// Iterate the opaque-tag bytes of each entity-tag in a comma-separated
// list (e.g. `If-None-Match`'s value). Tags inside double-quoted opaque
// content can't contain raw `,`, so a forward scan for matched quote
// pairs works without needing a full ABNF parser.
fn iter_etag_opaques(s: &str) -> impl Iterator<Item = &str> + '_ {
    let mut remaining = s;
    std::iter::from_fn(move || {
        // Skip leading whitespace, commas, and an optional weak prefix
        // before the next opaque-tag.
        loop {
            remaining = remaining.trim_start();
            if let Some(rest) = remaining.strip_prefix(',') {
                remaining = rest;
                continue;
            }
            if let Some(rest) = remaining.strip_prefix("W/") {
                remaining = rest;
                continue;
            }
            break;
        }
        let inner = remaining.strip_prefix('"')?;
        let close = inner.find('"')?;
        let tag = &inner[..close];
        remaining = &inner[close + 1..];
        Some(tag)
    })
}

// RFC 9110 §7.6.1: hop-by-hop headers are scoped to a single connection
// hop and must not be forwarded by intermediaries. `Date` is intentionally
// NOT in this list — for cache hits we preserve the origin's Date so the
// `Date + Age` pair correctly reflects when the response was generated.
fn copy_without_hop_by_hop_headers(headers: &Headers) -> Headers {
    let mut out = headers.clone();
    out.remove_all([
        KnownHeaderName::Connection,
        KnownHeaderName::KeepAlive,
        KnownHeaderName::ProxyAuthenticate,
        KnownHeaderName::ProxyAuthorization,
        KnownHeaderName::Te,
        KnownHeaderName::Trailer,
        KnownHeaderName::TransferEncoding,
        KnownHeaderName::Upgrade,
    ]);

    // The `Connection` header may also enumerate per-hop header names
    // that must be stripped (RFC 9110 §7.6.1).
    if let Some(connection) = headers.get_str(KnownHeaderName::Connection) {
        for name in connection
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            out.remove(name);
        }
    }

    out
}

// Outcome of `CachePolicy::before_request`.
#[derive(Debug, Clone)]
pub(crate) enum BeforeRequest {
    // The cached response is fresh and may be served directly. Pair the
    // returned head with the body bytes from storage.
    Fresh(CachedResponse),
    /// The cached response is fresh AND the request's conditional
    /// headers indicate the client already has the current
    /// representation. Serve the returned head with status 304 and an
    /// empty body — RFC 9111 §4.3.2 + RFC 9110 §13.2.2.
    NotModified(CachedResponse),
    // Send the request to the origin using these headers.
    Stale {
        // Full set of request headers to send (carries forward the caller's
        // headers minus hop-by-hop, plus any conditional validators the
        // policy could derive).
        request_headers: Headers,
        /// `true` when the stored entry actually matches this request's
        /// vary signature. `false` means the handler should consider
        /// another stored candidate (or send unconditionally).
        matches: bool,
    },
}

// The response head returned on a cache hit. Pair with body bytes from
// storage to construct the served response.
#[derive(Debug, Clone)]
pub(crate) struct CachedResponse {
    pub(crate) status: Status,
    pub(crate) headers: Headers,
}

// Outcome of `CachePolicy::after_response`. The size asymmetry between
// NotModified and Modified is intentional — both are matched and consumed
// at the same await point, so boxing would trade a per-revalidation
// allocation for no real benefit.
#[derive(Debug, Clone)]
pub enum AfterResponse {
    /// Origin returned 304 Not Modified with validators matching the
    /// stored entry. Reuse the cached body; serve the returned
    /// [`CachedResponse`]; replace the stored policy with the included
    /// new one (merged headers + refreshed `response_time`).
    NotModified(CachePolicy, CachedResponse),
    /// Origin returned a fresh response. Read the body from the conn,
    /// serve the returned [`CachedResponse`], and (if
    /// [`CachePolicy::is_storable`] is true) persist the new policy
    /// with that body.
    Modified(CachePolicy, CachedResponse),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_helpers::*;
    use std::time::Duration;
    use trillium_http::KnownHeaderName::*;

    // Fresh stored response, fresh request → Fresh, with Age inserted
    // and the origin's Date preserved.
    #[test]
    fn before_request_fresh_returns_cached_response() {
        let stored = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[
                (Date, "Thu, 01 Jan 1970 00:00:00 GMT"),
                (CacheControl, "max-age=600"),
                (Etag, r#""abc""#),
            ],
        );
        let policy = policy_from(&stored, t0(), private_cache());
        let new = request(Method::Get, &[]);

        match before_request(&policy, &new, at(t0(), 100)) {
            BeforeRequest::Fresh(cached) => {
                assert_eq!(cached.status, Status::Ok);
                assert_eq!(cached.headers.get_str(Age), Some("100"));
                assert_eq!(
                    cached.headers.get_str(Date),
                    Some("Thu, 01 Jan 1970 00:00:00 GMT"),
                    "origin Date is preserved on cache hit"
                );
                assert_eq!(cached.headers.get_str(Etag), Some(r#""abc""#));
            }
            other => panic!("expected Fresh, got {other:?}"),
        }
    }

    // Stale stored response → Stale, with If-None-Match populated from
    // ETag.
    #[test]
    fn before_request_stale_uses_etag_for_revalidation() {
        let stored = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=10"), (Etag, r#""abc""#)],
        );
        let policy = policy_from(&stored, t0(), private_cache());
        let new = request(Method::Get, &[]);

        match before_request(&policy, &new, at(t0(), 1000)) {
            BeforeRequest::Stale {
                request_headers,
                matches,
            } => {
                assert!(matches);
                assert_eq!(request_headers.get_str(IfNoneMatch), Some(r#""abc""#));
            }
            other => panic!("expected Stale, got {other:?}"),
        }
    }

    // Stale stored response, no ETag, has Last-Modified →
    // If-Modified-Since.
    #[test]
    fn before_request_stale_uses_last_modified_for_revalidation() {
        let lm = httpdate::fmt_http_date(t0() - Duration::from_secs(86400));
        let stored = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=10"), (LastModified, &lm)],
        );
        let policy = policy_from(&stored, t0(), private_cache());
        let new = request(Method::Get, &[]);

        match before_request(&policy, &new, at(t0(), 1000)) {
            BeforeRequest::Stale {
                request_headers, ..
            } => {
                assert_eq!(request_headers.get_str(IfModifiedSince), Some(lm.as_str()));
            }
            other => panic!("expected Stale, got {other:?}"),
        }
    }

    // §4.1: Vary mismatch → Stale with matches=false.
    #[test]
    fn vary_mismatch_returns_stale_not_matching() {
        let stored = exchange(
            Method::Get,
            &[(AcceptEncoding, "gzip")],
            Status::Ok,
            &[(CacheControl, "max-age=600"), (Vary, "Accept-Encoding")],
        );
        let policy = policy_from(&stored, t0(), private_cache());
        let new = request(Method::Get, &[(AcceptEncoding, "br")]);

        match before_request(&policy, &new, t0()) {
            BeforeRequest::Stale { matches, .. } => assert!(!matches),
            other => panic!("expected Stale, got {other:?}"),
        }
    }

    // §4.1: Vary match → Fresh.
    #[test]
    fn vary_match_returns_fresh() {
        let stored = exchange(
            Method::Get,
            &[(AcceptEncoding, "gzip")],
            Status::Ok,
            &[(CacheControl, "max-age=600"), (Vary, "Accept-Encoding")],
        );
        let policy = policy_from(&stored, t0(), private_cache());
        let new = request(Method::Get, &[(AcceptEncoding, "gzip")]);

        assert!(matches!(
            before_request(&policy, &new, t0()),
            BeforeRequest::Fresh(_)
        ));
    }

    // §5.2.1.4: request `no-cache` forces revalidation.
    #[test]
    fn request_no_cache_forces_stale() {
        let stored = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600")],
        );
        let policy = policy_from(&stored, t0(), private_cache());
        let new = request(Method::Get, &[(CacheControl, "no-cache")]);

        assert!(matches!(
            before_request(&policy, &new, t0()),
            BeforeRequest::Stale { .. }
        ));
    }

    // §5.4: legacy `Pragma: no-cache` on the request also forces
    // revalidation.
    #[test]
    fn request_pragma_no_cache_forces_stale() {
        let stored = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600")],
        );
        let policy = policy_from(&stored, t0(), private_cache());
        let new = request(Method::Get, &[(Pragma, "no-cache")]);

        assert!(matches!(
            before_request(&policy, &new, t0()),
            BeforeRequest::Stale { .. }
        ));
    }

    // §5.2.1.1: request max-age limits how old the stored response may be.
    #[test]
    fn request_max_age_limits_freshness() {
        let stored = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600")],
        );
        let policy = policy_from(&stored, t0(), private_cache());
        // Stored is 100s old; request demands at most 50s.
        let new = request(Method::Get, &[(CacheControl, "max-age=50")]);

        assert!(matches!(
            before_request(&policy, &new, at(t0(), 100)),
            BeforeRequest::Stale { .. }
        ));
    }

    // §5.2.1.3: request min-fresh demands remaining freshness lifetime.
    #[test]
    fn request_min_fresh_demands_headroom() {
        let stored = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600")],
        );
        let policy = policy_from(&stored, t0(), private_cache());
        // 500s left; request demands 600.
        let new = request(Method::Get, &[(CacheControl, "min-fresh=600")]);

        assert!(matches!(
            before_request(&policy, &new, at(t0(), 100)),
            BeforeRequest::Stale { .. }
        ));
    }

    // §5.2.1.2: request max-stale=N allows stale response up to N seconds.
    #[test]
    fn request_max_stale_allows_stale_response() {
        let stored = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=100")],
        );
        let policy = policy_from(&stored, t0(), private_cache());
        // Response is now 50s past expiry (age=150, max_age=100).
        let new = request(Method::Get, &[(CacheControl, "max-stale=200")]);

        assert!(matches!(
            before_request(&policy, &new, at(t0(), 150)),
            BeforeRequest::Fresh(_)
        ));
    }

    // §5.2.2.2: must-revalidate ignores max-stale.
    #[test]
    fn must_revalidate_ignores_max_stale() {
        let stored = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=100, must-revalidate")],
        );
        let policy = policy_from(&stored, t0(), private_cache());
        let new = request(Method::Get, &[(CacheControl, "max-stale=200")]);

        assert!(matches!(
            before_request(&policy, &new, at(t0(), 150)),
            BeforeRequest::Stale { .. }
        ));
    }

    // §7.6.1: hop-by-hop headers stripped from the cached response.
    #[test]
    fn cached_response_strips_hop_by_hop_headers() {
        let stored = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[
                (CacheControl, "max-age=600"),
                (Connection, "close"),
                (TransferEncoding, "chunked"),
            ],
        );
        let policy = policy_from(&stored, t0(), private_cache());
        let new = request(Method::Get, &[]);

        match before_request(&policy, &new, t0()) {
            BeforeRequest::Fresh(cached) => {
                assert!(!cached.headers.has_header(Connection));
                assert!(!cached.headers.has_header(TransferEncoding));
            }
            other => panic!("expected Fresh, got {other:?}"),
        }
    }

    // §7.6.1: Connection: foo also strips the named header.
    #[test]
    fn connection_header_value_strips_named_headers() {
        let stored = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600"), (Connection, "X-Custom-Hop")],
        );
        let stored = {
            let mut s = stored;
            s.response_headers_mut()
                .insert("x-custom-hop", "value".to_string());
            s
        };
        let policy = policy_from(&stored, t0(), private_cache());
        let new = request(Method::Get, &[]);

        match before_request(&policy, &new, t0()) {
            BeforeRequest::Fresh(cached) => {
                assert!(!cached.headers.has_header("x-custom-hop"));
            }
            other => panic!("expected Fresh, got {other:?}"),
        }
    }

    // ===== §4.3.4 after_response =====

    // 304 with matching strong ETag → NotModified, served as stored
    // status (e.g. 200).
    #[test]
    fn after_response_304_strong_etag_match() {
        let stored = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=10"), (Etag, r#""v1""#)],
        );
        let policy = policy_from(&stored, t0(), private_cache());

        let revalidation = exchange(
            Method::Get,
            &[],
            Status::NotModified,
            &[(Etag, r#""v1""#), (CacheControl, "max-age=600")],
        );

        match after_response(&policy, &revalidation, at(t0(), 100)) {
            AfterResponse::NotModified(new_policy, cached) => {
                assert_eq!(cached.status, Status::Ok);
                // 304's Cache-Control replaces stored.
                assert_eq!(
                    new_policy.response_headers.get_str(CacheControl),
                    Some("max-age=600")
                );
                assert_eq!(new_policy.response_headers.get_str(Etag), Some(r#""v1""#));
            }
            other => panic!("expected NotModified, got {other:?}"),
        }
    }

    // RFC 9111 §3.2: a 304 carrying a *different* ETag than the stored
    // one is still a successful update — the cache trusts the 304 (it
    // was generated for our conditional) and adopts the new validator.
    // The stored body is preserved; the stored ETag is replaced.
    #[test]
    fn after_response_304_with_different_etag_updates_stored_validator() {
        let stored = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=10"), (Etag, r#""v1""#)],
        );
        let policy = policy_from(&stored, t0(), private_cache());
        let revalidation = exchange(Method::Get, &[], Status::NotModified, &[(Etag, r#""v2""#)]);

        match after_response(&policy, &revalidation, at(t0(), 100)) {
            AfterResponse::NotModified(new_policy, _) => {
                assert_eq!(
                    new_policy.response_headers.get_str(Etag),
                    Some(r#""v2""#),
                    "304's new ETag replaces the stored one"
                );
            }
            other => panic!("expected NotModified, got {other:?}"),
        }
    }

    // Weak ETag on the 304 — match by weakness-stripped equality.
    #[test]
    fn after_response_304_weak_etag_match() {
        let stored = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=10"), (Etag, r#"W/"v1""#)],
        );
        let policy = policy_from(&stored, t0(), private_cache());
        let revalidation = exchange(
            Method::Get,
            &[],
            Status::NotModified,
            &[(Etag, r#"W/"v1""#)],
        );

        assert!(matches!(
            after_response(&policy, &revalidation, at(t0(), 100)),
            AfterResponse::NotModified(..)
        ));
    }

    // Last-Modified comparison when no ETag available.
    #[test]
    fn after_response_304_last_modified_match() {
        let lm = httpdate::fmt_http_date(t0() - Duration::from_secs(86400));
        let stored = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=10"), (LastModified, &lm)],
        );
        let policy = policy_from(&stored, t0(), private_cache());
        let revalidation = exchange(
            Method::Get,
            &[],
            Status::NotModified,
            &[(LastModified, &lm)],
        );

        assert!(matches!(
            after_response(&policy, &revalidation, at(t0(), 100)),
            AfterResponse::NotModified(..)
        ));
    }

    // RFC 9110 §15.4.5 allows a 304 to omit validators; we sent the
    // conditional with our stored validators, so a bare 304 implicitly
    // confirms the cached entry is still current.
    #[test]
    fn after_response_304_without_validators_trusts_stored_entry() {
        let stored = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=10"), (Etag, r#""abcdef""#)],
        );
        let policy = policy_from(&stored, t0(), private_cache());
        // 304 with neither ETag nor Last-Modified — what the cache-tests
        // server (and any RFC-9110-compliant origin) is allowed to send.
        let revalidation = exchange(Method::Get, &[], Status::NotModified, &[]);

        assert!(matches!(
            after_response(&policy, &revalidation, at(t0(), 100)),
            AfterResponse::NotModified(..)
        ));
    }

    // Non-304 status → Modified.
    #[test]
    fn after_response_200_is_modified() {
        let stored = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=10"), (Etag, r#""v1""#)],
        );
        let policy = policy_from(&stored, t0(), private_cache());
        let fresh = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=600"), (Etag, r#""v2""#)],
        );

        assert!(matches!(
            after_response(&policy, &fresh, at(t0(), 100)),
            AfterResponse::Modified
        ));
    }

    // §3.2: body-description headers from stored are preserved through
    // a 304 merge; non-body headers from the 304 win.
    #[test]
    fn after_response_merge_preserves_body_headers_from_stored() {
        let stored = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[
                (CacheControl, "max-age=10"),
                (Etag, r#""v1""#),
                (ContentLength, "1234"),
                (ContentEncoding, "gzip"),
            ],
        );
        let policy = policy_from(&stored, t0(), private_cache());

        // 304 with different (lying) body headers and a new
        // non-body-description header.
        let revalidation = exchange(
            Method::Get,
            &[],
            Status::NotModified,
            &[
                (Etag, r#""v1""#),
                (ContentLength, "0"),
                (ContentEncoding, "identity"),
                (CacheControl, "max-age=600"),
            ],
        );

        match after_response(&policy, &revalidation, at(t0(), 100)) {
            AfterResponse::NotModified(new_policy, _) => {
                // Body headers stay from stored.
                assert_eq!(
                    new_policy.response_headers.get_str(ContentLength),
                    Some("1234")
                );
                assert_eq!(
                    new_policy.response_headers.get_str(ContentEncoding),
                    Some("gzip")
                );
                // Non-body header from 304 wins.
                assert_eq!(
                    new_policy.response_headers.get_str(CacheControl),
                    Some("max-age=600")
                );
            }
            other => panic!("expected NotModified, got {other:?}"),
        }
    }

    // §3.2: 304 may add headers that weren't in the stored response.
    #[test]
    fn after_response_merge_includes_new_304_headers() {
        let stored = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=10"), (Etag, r#""v1""#)],
        );
        let policy = policy_from(&stored, t0(), private_cache());
        let revalidation = exchange(
            Method::Get,
            &[],
            Status::NotModified,
            &[(Etag, r#""v1""#), (Vary, "Accept-Encoding")],
        );

        match after_response(&policy, &revalidation, at(t0(), 100)) {
            AfterResponse::NotModified(new_policy, _) => {
                assert_eq!(
                    new_policy.response_headers.get_str(Vary),
                    Some("Accept-Encoding")
                );
            }
            other => panic!("expected NotModified, got {other:?}"),
        }
    }

    // No validators on either side → match (assumes a single stored
    // entry per §4.3.4).
    #[test]
    fn after_response_no_validators_treated_as_match() {
        let stored = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[(CacheControl, "max-age=10")],
        );
        let policy = policy_from(&stored, t0(), private_cache());
        let revalidation = exchange(
            Method::Get,
            &[],
            Status::NotModified,
            &[(CacheControl, "max-age=600")],
        );

        assert!(matches!(
            after_response(&policy, &revalidation, at(t0(), 100)),
            AfterResponse::NotModified(..)
        ));
    }

    // ===== inbound conditional → 304 synthesis =====

    #[test]
    fn etag_opaque_strips_quotes_and_weak_prefix() {
        assert_eq!(etag_opaque(r#""abc""#), Some("abc"));
        assert_eq!(etag_opaque(r#"W/"abc""#), Some("abc"));
        assert_eq!(etag_opaque(" \"abc\" "), Some("abc"));
        assert_eq!(etag_opaque("abc"), None); // no quotes
        assert_eq!(etag_opaque(""), None);
    }

    #[test]
    fn iter_etag_opaques_handles_multi_value_lists() {
        let v: Vec<&str> = iter_etag_opaques(r#""a", W/"b", "c""#).collect();
        assert_eq!(v, vec!["a", "b", "c"]);
        let v: Vec<&str> = iter_etag_opaques(r#""only""#).collect();
        assert_eq!(v, vec!["only"]);
        let v: Vec<&str> = iter_etag_opaques("").collect();
        assert!(v.is_empty());
    }

    #[test]
    fn inm_matches_strong_and_weak_forms_via_weak_comparison() {
        // §13.1.2: weak comparison ignores W/ prefix on either side.
        assert!(inm_matches(r#""abc""#, Some(r#""abc""#)));
        assert!(inm_matches(r#"W/"abc""#, Some(r#""abc""#)));
        assert!(inm_matches(r#""abc""#, Some(r#"W/"abc""#)));
        assert!(inm_matches(r#"W/"abc""#, Some(r#"W/"abc""#)));
        // List with one matching tag.
        assert!(inm_matches(r#""x", "abc", "y""#, Some(r#""abc""#)));
        // No match.
        assert!(!inm_matches(r#""def""#, Some(r#""abc""#)));
        // No cached etag → never match.
        assert!(!inm_matches(r#""abc""#, None));
        // Wildcard.
        assert!(inm_matches("*", Some(r#""abc""#)));
        assert!(inm_matches("*", None)); // wildcard always matches in cache-hit path
    }

    #[test]
    fn ims_matches_when_cached_lm_is_no_later_than_request_ims() {
        let lm_str = "Thu, 01 Jan 1970 00:00:00 GMT";
        let ims_str = "Thu, 01 Jan 1970 00:00:01 GMT";
        // Cached LM <= IMS → not modified (304).
        assert!(ims_matches(ims_str, Some(lm_str)));
        // Cached LM > IMS → modified.
        assert!(!ims_matches(lm_str, Some(ims_str)));
        // Equal → not modified.
        assert!(ims_matches(lm_str, Some(lm_str)));
        // Missing cached LM → can't evaluate.
        assert!(!ims_matches(ims_str, None));
        // Unparseable → can't evaluate.
        assert!(!ims_matches("not-a-date", Some(lm_str)));
        assert!(!ims_matches(ims_str, Some("not-a-date")));
    }

    // §4.3.2 + §13.2.2: fresh stored entry + matching If-None-Match →
    // BeforeRequest::NotModified with stripped 304 headers.
    #[test]
    fn before_request_returns_not_modified_when_inm_matches() {
        let stored = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[
                (Date, "Thu, 01 Jan 1970 00:00:00 GMT"),
                (CacheControl, "max-age=600"),
                (Etag, r#""abcdef""#),
                (ContentLength, "1234"),
                (ContentType, "application/json"),
            ],
        );
        let policy = policy_from(&stored, t0(), private_cache());
        let new = request(Method::Get, &[(IfNoneMatch, r#""abcdef""#)]);

        match before_request(&policy, &new, at(t0(), 100)) {
            BeforeRequest::NotModified(cached) => {
                assert_eq!(cached.status, Status::NotModified);
                assert_eq!(cached.headers.get_str(Etag), Some(r#""abcdef""#));
                assert_eq!(cached.headers.get_str(Age), Some("100"));
                assert_eq!(cached.headers.get_str(CacheControl), Some("max-age=600"));
                // Body-related headers MUST be stripped on a 304.
                assert!(!cached.headers.has_header(ContentLength));
                assert!(!cached.headers.has_header(ContentType));
            }
            other => panic!("expected NotModified, got {other:?}"),
        }
    }

    // §13.1.4: If-None-Match takes precedence — when present and not
    // matching, IMS MUST be ignored (no fall-through).
    #[test]
    fn before_request_inm_takes_precedence_over_ims() {
        let stored = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[
                (CacheControl, "max-age=600"),
                (Etag, r#""abcdef""#),
                (LastModified, "Thu, 01 Jan 1970 00:00:00 GMT"),
            ],
        );
        let policy = policy_from(&stored, t0(), private_cache());
        // INM doesn't match (different etag); IMS would match. Per §13.1.4,
        // IMS is ignored → serve full body, not 304.
        let new = request(
            Method::Get,
            &[
                (IfNoneMatch, r#""different""#),
                (IfModifiedSince, "Thu, 01 Jan 1970 00:00:01 GMT"),
            ],
        );
        assert!(matches!(
            before_request(&policy, &new, at(t0(), 100)),
            BeforeRequest::Fresh(_)
        ));
    }

    // §13.1.4: IMS is honored when INM is absent.
    #[test]
    fn before_request_falls_back_to_ims_when_inm_absent() {
        let stored = exchange(
            Method::Get,
            &[],
            Status::Ok,
            &[
                (CacheControl, "max-age=600"),
                (LastModified, "Thu, 01 Jan 1970 00:00:00 GMT"),
            ],
        );
        let policy = policy_from(&stored, t0(), private_cache());
        let new = request(
            Method::Get,
            &[(IfModifiedSince, "Thu, 01 Jan 1970 00:00:01 GMT")],
        );
        assert!(matches!(
            before_request(&policy, &new, at(t0(), 100)),
            BeforeRequest::NotModified(_)
        ));
    }
}
