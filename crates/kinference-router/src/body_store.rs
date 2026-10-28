//! `BodyStore`: server-side buffer for request bodies pushed by callers.
//!
//! A caller streams the body in via `PushBody`, names the resulting
//! `stream_id` on `Pick`, then streams the (possibly rewritten) body back out
//! via `PullBody`. kinference holds and bounds the bytes in between.
//!
//! Lifecycle of one body, and the id it lives under:
//!
//! ```text
//!   PushBody chunks ──► local Vec<u8> + Reservation   (no map entry yet)
//!        half-close ──► commit()   ──► State::Ready       key = stream_id
//!              Pick ──► begin_pick() + PickHandle::commit()
//!                         ├─ State::Consumed{..}          key = stream_id
//!                         └─ State::Outbound(..)          key = response_id
//!          PullBody ──► take_outbound() ──► BodyLease ──► (freed on drop)
//! ```
//!
//! - There is no `Receiving` state. A push in progress lives in a local buffer
//!   in the handler, with a [`Reservation`] holding its bytes against the
//!   global budget, so a push that breaks or goes idle is reclaimed by `Drop`
//!   without a TTL. The body behind a non-OK `Pick` is likewise released at
//!   once by [`PickHandle::discard`].
//! - `Consumed` is a tombstone, so a second `Pick` on the same id gets
//!   `FAILED_PRECONDITION` rather than `NOT_FOUND`.
//!
//! This module returns `tonic::Status` directly; unlike `Pick`, these
//! operations report failures as gRPC status codes (see `crate::grpc`).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tonic::Status;

use crate::metrics::RouterMetrics;
use crate::proto::BodyChunk;

/// Reason a push was refused, for the `kinference_body_rejected_total{reason}`
/// counter. Kept alongside the `Status` so the caller-facing message and the
/// metric label can't drift apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    /// The `kinference-declared-length` header alone exceeded the per-request cap,
    /// so we refused before reading any chunk.
    DeclaredTooLarge,
    /// The bytes actually received exceeded the per-request cap.
    RequestTooLarge,
    /// The server-wide budget is full.
    ServerFull,
}

impl RejectReason {
    pub fn as_label(self) -> &'static str {
        match self {
            RejectReason::DeclaredTooLarge => "declared_too_large",
            RejectReason::RequestTooLarge => "request_too_large",
            RejectReason::ServerFull => "server_full",
        }
    }
}

/// What a stored entry currently is. See the module diagram.
enum State {
    /// A complete body awaiting `Pick`, keyed by `stream_id`.
    Ready(Arc<Vec<u8>>),
    /// Tombstone left under `stream_id` once `Pick` consumed it, so a second
    /// `Pick` is distinguishable from an unknown id. Carries no bytes.
    Consumed {
        #[allow(dead_code)] // diagnostic: which response_id took over this body
        response_id: u64,
    },
    /// The body `PullBody` will stream, keyed by `response_id`.
    Outbound(Arc<Vec<u8>>),
}

struct Entry {
    state: State,
    /// Bytes charged to `bytes_in_use` for this entry (0 for a tombstone).
    bytes: u64,
    /// When the entry last changed state; drives TTL eviction.
    at: Instant,
    /// Caller's `kinference-client-request-id`, so an abandoned body is traceable
    /// in the reaper's logs before any `Pick` named it.
    client_request_id: Option<String>,
}

/// Buffered request bodies, bounded by a per-request cap, a server-wide cap,
/// and a TTL. Share via `Arc`; every method takes `&self`.
pub struct BodyStore {
    entries: DashMap<u64, Entry>,
    /// Reserved (mid-push) + stored bytes. One counter for both so the
    /// server-wide budget covers uploads in progress, not just finished ones.
    bytes_in_use: AtomicU64,
    max_total_bytes: u64,
    max_request_bytes: u64,
    ttl: Duration,
}

impl BodyStore {
    pub fn new(max_total_bytes: u64, max_request_bytes: u64, ttl: Duration) -> Self {
        Self {
            entries: DashMap::new(),
            bytes_in_use: AtomicU64::new(0),
            max_total_bytes,
            max_request_bytes,
            ttl,
        }
    }

    /// The per-request budget, for the gRPC decoding limit.
    pub fn max_request_bytes(&self) -> u64 {
        self.max_request_bytes
    }

    /// `(entries, bytes_in_use)`, for `/metrics` gauges and tests.
    pub fn stats(&self) -> (usize, u64) {
        (
            self.entries.len(),
            self.bytes_in_use.load(Ordering::Relaxed),
        )
    }

    /// Refuse early when the caller's declared length can't possibly fit.
    /// Advisory per the proto: absent or wrong, the per-chunk checks still
    /// bound the push.
    pub fn check_declared_length(&self, declared: u64) -> Result<(), (Status, RejectReason)> {
        if declared > self.max_request_bytes {
            return Err((
                Status::resource_exhausted(format!(
                    "declared body length {declared} exceeds the per-request limit of {} bytes",
                    self.max_request_bytes
                )),
                RejectReason::DeclaredTooLarge,
            ));
        }
        Ok(())
    }

    /// Open a byte reservation for one in-progress push. Bytes are charged as
    /// chunks arrive (see [`Reservation::charge`]) and released on drop unless
    /// [`BodyStore::commit`] takes ownership.
    pub fn reservation(self: &Arc<Self>) -> Reservation {
        Reservation {
            store: self.clone(),
            bytes: 0,
            client_request_id: None,
        }
    }

    /// Store a completed push under a fresh random id and hand the id back.
    /// Consumes the reservation, transferring its bytes to the new entry.
    pub fn commit(&self, mut reservation: Reservation, body: Vec<u8>) -> u64 {
        // The reservation already charged exactly these bytes; neutralize it so
        // its Drop doesn't release what the entry now owns.
        let bytes = reservation.take();
        debug_assert_eq!(bytes, body.len() as u64);
        let client_request_id = reservation.client_request_id.take();
        let id = self.mint_id();
        self.entries.insert(
            id,
            Entry {
                state: State::Ready(Arc::new(body)),
                bytes,
                at: Instant::now(),
                client_request_id,
            },
        );
        id
    }

    /// Claim the body behind `stream_id` for a `Pick`.
    ///
    /// The entry is left in place until the caller resolves the returned
    /// handle, so a concurrent second `Pick` on the same id sees `Consumed`
    /// and fails rather than racing onto the same body.
    pub fn begin_pick(self: &Arc<Self>, stream_id: u64) -> Result<PickHandle, Status> {
        if stream_id == 0 {
            return Err(Status::invalid_argument(
                "body_stream_id is required (0 is reserved to mean \"no id\")",
            ));
        }
        let mut entry = self
            .entries
            .get_mut(&stream_id)
            .ok_or_else(|| Status::not_found(format!("unknown or expired body {stream_id}")))?;
        let body = match &entry.state {
            State::Ready(body) => body.clone(),
            State::Consumed { .. } => {
                return Err(Status::failed_precondition(format!(
                    "body {stream_id} was already consumed by an earlier Pick"
                )));
            }
            // An id handed out by PushBody is never Outbound; defensive only.
            State::Outbound(_) => {
                return Err(Status::failed_precondition(format!(
                    "id {stream_id} names a picked body, not a pushed one"
                )));
            }
        };
        // Mark consumed up-front: this is the atomic step that makes a
        // double-Pick deterministic. `bytes` moves to the outbound entry on
        // commit, or is released on discard.
        let bytes = entry.bytes;
        entry.state = State::Consumed { response_id: 0 };
        entry.bytes = 0;
        entry.at = Instant::now();
        let client_request_id = entry.client_request_id.clone();
        drop(entry);
        Ok(PickHandle {
            store: self.clone(),
            stream_id,
            body,
            bytes,
            client_request_id,
            resolved: false,
        })
    }

    /// Take the body for a `PullBody`. Single-shot: the entry is removed, so a
    /// second call for the same `response_id` is `NOT_FOUND`. The returned
    /// lease keeps the bytes charged until the stream finishes.
    pub fn take_outbound(self: &Arc<Self>, response_id: u64) -> Result<BodyLease, Status> {
        if response_id == 0 {
            return Err(Status::invalid_argument(
                "response_id is required (0 is reserved to mean \"no id\")",
            ));
        }
        let Some((_, entry)) = self.entries.remove(&response_id) else {
            return Err(Status::not_found(format!(
                "unknown, expired, or already-pulled body {response_id}"
            )));
        };
        match entry.state {
            State::Outbound(body) => Ok(BodyLease {
                body,
                _guard: ByteGuard {
                    store: self.clone(),
                    bytes: entry.bytes,
                },
            }),
            // Re-insert what we removed: it wasn't ours to take.
            other => {
                let bytes = entry.bytes;
                self.entries.insert(
                    response_id,
                    Entry {
                        state: other,
                        bytes,
                        at: entry.at,
                        client_request_id: entry.client_request_id,
                    },
                );
                Err(Status::failed_precondition(format!(
                    "body {response_id} is not ready to pull"
                )))
            }
        }
    }

    /// Release whatever is held for `response_id`. Returns `(released, bytes)`.
    /// Idempotent: an unknown or already-released id is `(false, 0)`, never an
    /// error, because `Abort` must tolerate duplicates.
    pub fn abort(&self, response_id: u64) -> (bool, u64) {
        let Some((_, entry)) = self.entries.remove(&response_id) else {
            return (false, 0);
        };
        self.release(entry.bytes);
        // A tombstone carries no bytes but removing it is still a release.
        (true, entry.bytes)
    }

    /// Drop every entry past the TTL. Returns `(entries, bytes)` reclaimed.
    ///
    /// `Abort` only shortens the window; bodies whose caller vanished at any
    /// stage are reclaimed here.
    pub fn take_expired(&self) -> (usize, u64) {
        let cutoff = Instant::now() - self.ttl;
        let expired: Vec<u64> = self
            .entries
            .iter()
            .filter(|e| e.value().at < cutoff)
            .map(|e| *e.key())
            .collect();
        let mut count = 0usize;
        let mut bytes = 0u64;
        for id in expired {
            if let Some((_, entry)) = self.entries.remove(&id) {
                if entry.bytes > 0 {
                    tracing::debug!(
                        body = id,
                        bytes = entry.bytes,
                        req = entry.client_request_id.as_deref().unwrap_or("-"),
                        "body_store: reclaimed a buffered body past TTL"
                    );
                }
                self.release(entry.bytes);
                count += 1;
                bytes += entry.bytes;
            }
        }
        (count, bytes)
    }

    /// Charge `n` bytes against both budgets, or fail.
    fn try_charge(&self, n: u64, already: u64) -> Result<(), (Status, RejectReason)> {
        if already + n > self.max_request_bytes {
            return Err((
                Status::resource_exhausted(format!(
                    "body exceeds the per-request limit of {} bytes",
                    self.max_request_bytes
                )),
                RejectReason::RequestTooLarge,
            ));
        }
        // CAS loop so concurrent pushes can't jointly overshoot the budget.
        let mut cur = self.bytes_in_use.load(Ordering::Relaxed);
        loop {
            let next = cur + n;
            if next > self.max_total_bytes {
                return Err((
                    Status::resource_exhausted(format!(
                        "server body buffer is full ({} of {} bytes in use)",
                        cur, self.max_total_bytes
                    )),
                    RejectReason::ServerFull,
                ));
            }
            match self.bytes_in_use.compare_exchange_weak(
                cur,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => cur = observed,
            }
        }
    }

    fn release(&self, n: u64) {
        if n == 0 {
            return;
        }
        // Saturating: a double release would otherwise wrap and permanently
        // "free" the budget.
        self.bytes_in_use
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |cur| {
                Some(cur.saturating_sub(n))
            })
            .ok();
    }

    /// Random, non-zero, and unused. Random so a peer can't guess into another
    /// caller's body; non-zero because 0 means "no id" on the wire.
    fn mint_id(&self) -> u64 {
        loop {
            let mut buf = [0u8; 8];
            let candidate = match getrandom::getrandom(&mut buf) {
                Ok(()) => u64::from_le_bytes(buf),
                Err(e) => {
                    tracing::error!(error = %e, "getrandom failed while minting a body id");
                    // Wall-clock nanos: guessable, but this path is effectively
                    // unreachable and failing the push would be worse.
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_nanos() as u64)
                        .unwrap_or(1)
                }
            };
            if candidate != 0 && !self.entries.contains_key(&candidate) {
                return candidate;
            }
        }
    }
}

/// Bytes charged to the server-wide budget, released on drop. Used for
/// in-progress pushes and for the duration of a `PullBody` stream.
struct ByteGuard {
    store: Arc<BodyStore>,
    bytes: u64,
}

impl Drop for ByteGuard {
    fn drop(&mut self) {
        self.store.release(self.bytes);
    }
}

/// Byte budget for one in-progress `PushBody`. Charge per chunk; on drop
/// (broken stream, idle timeout, over-budget) the bytes are released with no
/// TTL involvement.
pub struct Reservation {
    store: Arc<BodyStore>,
    bytes: u64,
    /// Rides along so `commit` can stamp it onto the entry, and so an
    /// abandoned push is traceable in the Drop log.
    client_request_id: Option<String>,
}

impl Reservation {
    /// Charge `n` more bytes for this push. `Err` means the push must be
    /// refused; the reservation still releases what it already holds on drop.
    pub fn charge(&mut self, n: u64) -> Result<(), (Status, RejectReason)> {
        self.store.try_charge(n, self.bytes)?;
        self.bytes += n;
        Ok(())
    }

    pub fn with_client_request_id(mut self, id: Option<String>) -> Self {
        self.client_request_id = id;
        self
    }

    /// Neutralize and report the charge, so ownership can move to an entry.
    fn take(&mut self) -> u64 {
        std::mem::take(&mut self.bytes)
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let bytes = self.bytes;
        if bytes > 0 {
            tracing::debug!(
                bytes,
                req = self.client_request_id.as_deref().unwrap_or("-"),
                "body_store: released an abandoned in-progress push"
            );
        }
        self.store.release(bytes);
    }
}

/// A `Pick` in progress against a pushed body. Resolve it exactly once with
/// [`Self::commit`] (the pick succeeded) or [`Self::discard`] (it didn't).
/// Dropping it unresolved discards, so an early `?` can't leak the body.
pub struct PickHandle {
    store: Arc<BodyStore>,
    stream_id: u64,
    body: Arc<Vec<u8>>,
    bytes: u64,
    client_request_id: Option<String>,
    resolved: bool,
}

impl PickHandle {
    /// The pushed bytes, to route on.
    pub fn body(&self) -> &Arc<Vec<u8>> {
        &self.body
    }

    /// Re-key the body to `response_id` for `PullBody` to stream. `outbound`
    /// is the body the upstream should receive, rewritten or not.
    pub fn commit(mut self, response_id: u64, outbound: Arc<Vec<u8>>) {
        self.resolved = true;
        let new_bytes = outbound.len() as u64;
        // A rewrite may add a few bytes; charge the delta without failing the
        // pick.
        if new_bytes > self.bytes {
            self.store
                .bytes_in_use
                .fetch_add(new_bytes - self.bytes, Ordering::AcqRel);
        } else {
            self.store.release(self.bytes - new_bytes);
        }
        // Record which response_id took over, for diagnostics on a double-Pick.
        if let Some(mut e) = self.store.entries.get_mut(&self.stream_id) {
            e.state = State::Consumed { response_id };
            e.at = Instant::now();
        }
        self.store.entries.insert(
            response_id,
            Entry {
                state: State::Outbound(outbound),
                bytes: new_bytes,
                at: Instant::now(),
                client_request_id: self.client_request_id.take(),
            },
        );
    }

    /// Free the body: the pick produced no endpoint, and callers don't `Abort`
    /// a non-OK `Pick`, so release now rather than at the TTL.
    pub fn discard(mut self) {
        self.resolved = true;
        self.store.release(self.bytes);
        // Leave the Consumed tombstone: a retry on the same stream_id should
        // still say "consumed", not "unknown". The reaper clears it.
    }
}

// Manual impls: both types hold an `Arc<BodyStore>`, which isn't `Debug`.
// Needed so tests can `expect_err`/`unwrap_err` on the `Result`s.
impl std::fmt::Debug for PickHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PickHandle")
            .field("stream_id", &self.stream_id)
            .field("bytes", &self.bytes)
            .field("resolved", &self.resolved)
            .finish()
    }
}

impl std::fmt::Debug for BodyLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BodyLease")
            .field("len", &self.body.len())
            .finish()
    }
}

impl Drop for PickHandle {
    fn drop(&mut self) {
        if !self.resolved {
            // An unresolved handle means an early return between begin_pick and
            // commit/discard. Release rather than leak.
            self.store.release(self.bytes);
        }
    }
}

/// A body checked out for streaming. Holds its bytes against the budget until
/// the `PullBody` stream ends (successfully or not).
pub struct BodyLease {
    body: Arc<Vec<u8>>,
    _guard: ByteGuard,
}

impl BodyLease {
    pub fn body(&self) -> &Arc<Vec<u8>> {
        &self.body
    }

    pub fn len(&self) -> usize {
        self.body.len()
    }

    pub fn is_empty(&self) -> bool {
        self.body.is_empty()
    }
}

/// Why a `PushBody` drain stopped short.
#[derive(Debug)]
enum PushFailure {
    /// No chunk arrived within the inter-chunk budget.
    Idle(Duration),
    /// A byte budget was exceeded; carries the metric label.
    Budget(Status, RejectReason),
    /// The stream reset before half-close, so the body is incomplete.
    Transport(Status),
    /// The caller sent `kinference-declared-length` and delivered a different
    /// number of bytes, e.g. a body truncated by an early half-close.
    LengthMismatch { declared: u64, received: u64 },
}

impl PushFailure {
    fn reject_reason(&self) -> Option<RejectReason> {
        match self {
            PushFailure::Budget(_, reason) => Some(*reason),
            _ => None,
        }
    }

    fn into_status(self) -> Status {
        match self {
            PushFailure::Idle(idle) => Status::deadline_exceeded(format!(
                "no body chunk received within {}s",
                idle.as_secs()
            )),
            PushFailure::Budget(status, _) | PushFailure::Transport(status) => status,
            PushFailure::LengthMismatch { declared, received } => {
                Status::invalid_argument(format!(
                    "body length {received} does not match the declared \
                     kinference-declared-length of {declared}"
                ))
            }
        }
    }
}

/// Drain a client-streaming body into one buffer, enforcing the idle budget and
/// the byte budgets as chunks arrive.
///
/// Generic over the stream so it is testable without a gRPC transport.
///
/// The timeout applies per chunk, not to the call, since a push spans the
/// client's upload. Stream end is a clean half-close: the body is complete. An
/// `Err` item is a reset stream, and the bytes are discarded.
///
/// `declared`, the caller's `kinference-declared-length`, is enforced: a
/// mismatch either way is rejected.
async fn accumulate_body<S>(
    stream: &mut S,
    reservation: &mut Reservation,
    idle: Duration,
    declared: Option<u64>,
) -> Result<Vec<u8>, PushFailure>
where
    S: futures::Stream<Item = Result<BodyChunk, Status>> + Unpin,
{
    use futures::StreamExt;
    // `declared` is only an allocation hint here, clamped.
    let mut buf: Vec<u8> = Vec::with_capacity(declared.unwrap_or(0).min(1024 * 1024) as usize);
    loop {
        match tokio::time::timeout(idle, stream.next()).await {
            Err(_) => return Err(PushFailure::Idle(idle)),
            Ok(None) => {
                // Clean half-close. If a length was declared, this is where a
                // truncated body is caught.
                if let Some(declared) = declared
                    && buf.len() as u64 != declared
                {
                    return Err(PushFailure::LengthMismatch {
                        declared,
                        received: buf.len() as u64,
                    });
                }
                return Ok(buf);
            }
            Ok(Some(Ok(chunk))) => {
                if let Err((status, reason)) = reservation.charge(chunk.chunk.len() as u64) {
                    return Err(PushFailure::Budget(status, reason));
                }
                buf.extend_from_slice(&chunk.chunk);
                // Fail fast on an over-long body rather than buffering past
                // what the caller said it would send.
                if let Some(declared) = declared
                    && buf.len() as u64 > declared
                {
                    return Err(PushFailure::LengthMismatch {
                        declared,
                        received: buf.len() as u64,
                    });
                }
            }
            Ok(Some(Err(e))) => return Err(PushFailure::Transport(e)),
        }
    }
}

/// The budgets the server resolves from its config, in bytes and seconds.
#[derive(Debug, Clone, Copy)]
pub struct BodyLimits {
    pub max_total_bytes: u64,
    pub max_request_bytes: u64,
    pub ttl: Duration,
    /// Maximum silence between two `PushBody` chunks before the push is
    /// abandoned.
    pub idle_timeout: Duration,
}

/// The service's part for bodies: the store, the push drain with its idle
/// budget, and the buffer metrics.
pub(crate) struct Bodies {
    pub(crate) store: Arc<BodyStore>,
    /// Maximum silence between two `PushBody` chunks before the push is
    /// abandoned. The push itself has no deadline, since it spans the client's
    /// upload.
    pub(crate) idle_timeout: Duration,
    prom: Arc<RouterMetrics>,
}

impl Bodies {
    pub(crate) fn new(
        store: Arc<BodyStore>,
        idle_timeout: Duration,
        prom: Arc<RouterMetrics>,
    ) -> Self {
        Self {
            store,
            idle_timeout,
            prom,
        }
    }

    /// Reclaim buffered bodies past their TTL, count them, and refresh the
    /// buffer gauges. Returns `(entries, bytes)` freed.
    pub(crate) fn sweep(&self) -> (usize, u64) {
        let (reaped, bytes) = self.store.take_expired();
        if reaped > 0 {
            self.prom.record_bodies_reaped(reaped as u64);
        }
        let (entries, in_use) = self.store.stats();
        self.prom.set_body_buffer(entries as u64, in_use);
        (reaped, bytes)
    }

    /// `PushBody` after its metadata is read: refuse on the declared length
    /// alone, drain the stream under the idle and byte budgets, commit.
    /// Returns `(stream_id, total_bytes)`.
    pub(crate) async fn push<S>(
        &self,
        stream: &mut S,
        client_request_id: Option<String>,
        declared: Option<u64>,
    ) -> Result<(u64, u64), Status>
    where
        S: futures::Stream<Item = Result<BodyChunk, Status>> + Unpin,
    {
        let req_label = client_request_id.clone().unwrap_or_else(|| "-".to_string());

        // Refuse an over-budget body on its declared length alone, before
        // reading a single chunk.
        if let Some(declared) = declared
            && let Err((status, reason)) = self.store.check_declared_length(declared)
        {
            self.prom.record_body_rejected(reason.as_label());
            tracing::warn!(
                req = %req_label,
                declared,
                "push_body: refused on declared length"
            );
            return Err(status);
        }

        let mut reservation = self
            .store
            .reservation()
            .with_client_request_id(client_request_id);
        let started = Instant::now();

        let buf = match accumulate_body(stream, &mut reservation, self.idle_timeout, declared).await
        {
            Ok(buf) => buf,
            Err(failure) => {
                if let Some(reason) = failure.reject_reason() {
                    self.prom.record_body_rejected(reason.as_label());
                }
                let status = failure.into_status();
                tracing::warn!(
                    req = %req_label,
                    code = ?status.code(),
                    "push_body: aborted before half-close; discarding"
                );
                return Err(status);
            }
        };

        let total_bytes = buf.len() as u64;
        let stream_id = self.store.commit(reservation, buf);
        self.prom.record_body_pushed(total_bytes);
        tracing::debug!(
            req = %req_label,
            stream_id,
            total_bytes,
            elapsed_ms = started.elapsed().as_millis() as u64,
            "push_body: buffered"
        );
        Ok((stream_id, total_bytes))
    }
}

/// A store behind an `Arc`, for the service and the tests.
pub(crate) fn shared(
    max_total_bytes: u64,
    max_request_bytes: u64,
    ttl: Duration,
) -> Arc<BodyStore> {
    Arc::new(BodyStore::new(max_total_bytes, max_request_bytes, ttl))
}

#[cfg(test)]
mod tests;
