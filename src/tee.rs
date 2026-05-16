//! [`TeeingReader`] — streams an upstream body through to a reader
//! while writing the same bytes into a [`PutHandle`].
//!
//! The cache handler installs a `TeeingReader` between the origin (or
//! upstream handler) and the user: each chunk read upstream is written
//! to the cache storage *and* returned to the user. Trailers from the
//! upstream body propagate to both sides — the user reads them via
//! [`Body::trailers`][trillium_http::Body::trailers] on the served body
//! after EOF, and the storage sees them via [`PutHandle::finalize`].
//!
//! ## Cap behavior
//!
//! When `bytes_written` would exceed `cap`, the writer is dropped
//! (aborts the cache write) and the user continues to receive the
//! remainder of the body unchanged. The cache simply doesn't get a
//! stored copy of that response.
//!
//! ## Slow-writer behavior
//!
//! Writes to the storage are awaited before the same bytes are returned
//! to the caller. A slow storage backend back-pressures the user. The
//! intended escape hatch is the backend's own buffering — e.g., an
//! in-memory tier in front of disk — not a buffer in the tee itself.

use crate::PutHandle;
use futures_lite::AsyncRead;
use std::{
    io,
    pin::Pin,
    task::{Context, Poll, ready},
};
use trillium_http::{Body, BodySource, Headers};

/// Streaming body source that tees its bytes into a [`PutHandle`].
///
/// Constructed with a [`Body`] upstream — server-side callers should pass
/// `body.without_chunked_framing()` so the tee sees raw bytes rather than
/// wire-format chunked encoding. Client-side callers convert a
/// `ResponseBody<'static>` into a `Body` first via `Into`, which routes
/// trailers through `Body::new_with_trailers`.
pub(crate) struct TeeingReader<W: PutHandle> {
    upstream: Body,
    state: TeeState<W>,
    cap: u64,
    bytes_written: u64,
    /// Bytes read from upstream but not yet accepted by the writer.
    /// Drained before more bytes are read.
    pending: Vec<u8>,
    /// Trailers captured from upstream at EOF. Cloned into [`PutHandle::finalize`]
    /// AND surfaced to readers of the wrapping `Body` via `BodySource::trailers`.
    /// `None` before EOF; taken on the first `trailers()` call after EOF.
    trailers: Option<Headers>,
}

enum TeeState<W: PutHandle> {
    /// Streaming bytes through to the writer.
    Active { writer: W },
    /// Cap exceeded or writer errored; remainder passes through unmodified.
    Aborted,
    /// Upstream EOF reached; finalize future in flight.
    Finalizing(Pin<Box<dyn Future<Output = io::Result<()>> + Send>>),
    /// Finalize completed (or never started, on abort). Subsequent polls return Ok(0).
    Done,
}

impl<W: PutHandle> TeeingReader<W> {
    pub(crate) fn new(upstream: Body, writer: W, cap: u64) -> Self {
        Self {
            upstream,
            state: TeeState::Active { writer },
            cap,
            bytes_written: 0,
            pending: Vec::new(),
            trailers: None,
        }
    }

    fn abort(&mut self) {
        // Dropping the writer aborts the cache write; remaining bytes pass through.
        self.state = TeeState::Aborted;
        self.pending.clear();
    }
}

impl<W: PutHandle> AsyncRead for TeeingReader<W> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();

        // Drive an in-flight finalize, if any, before reading more upstream.
        if let TeeState::Finalizing(fut) = &mut this.state {
            match fut.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(result) => {
                    if let Err(e) = result {
                        log::warn!("cache: finalize failed: {e}");
                    }
                    this.state = TeeState::Done;
                }
            }
        }
        if matches!(this.state, TeeState::Done) {
            return Poll::Ready(Ok(0));
        }

        // Drain any pending bytes that didn't make it into the writer on the previous poll.
        while !this.pending.is_empty() {
            let TeeState::Active { writer } = &mut this.state else {
                this.pending.clear();
                break;
            };
            match Pin::new(writer).poll_write(cx, &this.pending) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(0)) | Poll::Ready(Err(_)) => this.abort(),
                Poll::Ready(Ok(n)) => {
                    this.pending.drain(..n);
                }
            }
        }

        // Read the next chunk from upstream.
        let n = ready!(Pin::new(&mut this.upstream).poll_read(cx, buf))?;

        if n == 0 {
            // Upstream EOF: pull trailers once (Body::trailers handles the done-gate), keep
            // them for our own `BodySource::trailers` surface, and pass a clone to finalize.
            if let TeeState::Active { .. } = this.state {
                this.trailers = this.upstream.trailers();
                let TeeState::Active { writer } =
                    std::mem::replace(&mut this.state, TeeState::Done)
                else {
                    unreachable!("just matched Active above");
                };
                let fut = Box::pin(writer.finalize(this.trailers.clone()))
                    as Pin<Box<dyn Future<Output = io::Result<()>> + Send>>;
                this.state = TeeState::Finalizing(fut);
                // Drive once before returning so we don't always pay an extra poll round-trip.
                let TeeState::Finalizing(fut) = &mut this.state else {
                    unreachable!("just set Finalizing");
                };
                match fut.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(result) => {
                        if let Err(e) = result {
                            log::warn!("cache: finalize failed: {e}");
                        }
                        this.state = TeeState::Done;
                    }
                }
            }
            return Poll::Ready(Ok(0));
        }

        // We have bytes for the caller. Tee them (if still active).
        if let TeeState::Active { writer } = &mut this.state {
            if this.bytes_written.saturating_add(n as u64) > this.cap {
                this.abort();
            } else {
                match Pin::new(writer).poll_write(cx, &buf[..n]) {
                    Poll::Ready(Ok(0)) | Poll::Ready(Err(_)) => this.abort(),
                    Poll::Ready(Ok(written)) => {
                        this.bytes_written += n as u64;
                        if written < n {
                            this.pending.extend_from_slice(&buf[written..n]);
                        }
                    }
                    Poll::Pending => {
                        this.bytes_written += n as u64;
                        this.pending.extend_from_slice(&buf[..n]);
                    }
                }
            }
        }

        Poll::Ready(Ok(n))
    }
}

impl<W: PutHandle> BodySource for TeeingReader<W> {
    fn trailers(self: Pin<&mut Self>) -> Option<Headers> {
        self.get_mut().trailers.take()
    }
}
