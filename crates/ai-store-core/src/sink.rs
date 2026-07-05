//! Projection output sinks — the read-side integration point.
//!
//! `ProjectionSink` receives materialized state after each successful append.
//! Its contract is idempotence: replaying the same `(stream, seq)` a second
//! time must produce the same result as the first, because `Store::catch_up`
//! and `Store::rebuild` may re-emit events after crash or configuration change.

use async_trait::async_trait;
use serde_json::Value;

use crate::error::StoreError;
use crate::event::Event;
use crate::id::{Label, Seq, StreamId};

/// A read-side consumer of committed events.
///
/// Sinks are dispatched after the event has been durably appended to the
/// backend. A sink whose `commit` returns `Err` leaves its checkpoint in place;
/// the sink will be re-driven on the next `catch_up`.
#[async_trait]
pub trait ProjectionSink: Send + Sync {
    /// Stable identifier used as the checkpoint key.
    fn id(&self) -> &str;

    /// Whether this sink is interested in `stream`.
    ///
    /// Default: every sink accepts every stream. Override this when a sink
    /// is scoped to a single stream (or a subset) and should be left alone
    /// by every other stream's traffic — the alternative, prevalent before
    /// this method existed, was for the sink's own `commit` /
    /// `on_label_set` / `on_label_deleted` to compare `stream` against a
    /// remembered value and no-op otherwise, duplicating this filter in
    /// every implementation and in every one of the facade's automatic
    /// dispatch sites.
    ///
    /// Honored by the facade's *automatic* dispatch: the post-`append`
    /// `commit` call, `Store::catch_up` / `Store::rebuild` (a stream this
    /// sink does not accept is skipped entirely — not counted in
    /// [`CatchUpReport::skipped`], since it was never this sink's concern to
    /// begin with), and the `on_label_set` / `on_label_deleted`
    /// notifications from `Store::label_set` / `Store::label_delete`.
    ///
    /// **Not** honored by [`crate::Store::materialize_to_sink`]: that call
    /// is an explicit, single-shot request naming both the stream and the
    /// sink id, so a caller invoking it directly is assumed to know what it
    /// is asking for.
    fn accepts(&self, _stream: &StreamId) -> bool {
        true
    }

    /// Apply a single committed event.
    ///
    /// `state` is the materialized state at `seq` (i.e. after `event.patch` has
    /// been applied). Implementations must be idempotent under retries of the
    /// same `(stream, seq)`.
    async fn commit(
        &self,
        stream: &StreamId,
        seq: Seq,
        state: &Value,
        event: &Event,
    ) -> Result<(), StoreError>;

    /// React to a label being pinned or moved.
    ///
    /// Called from the facade after `Store::label_set` succeeds. `state` is
    /// the materialized state at `at` (freshly reconstructed); `event` is
    /// the committed [`Event`] the label now points at — most usefully its
    /// `at` (wall-clock or imported timestamp) and `meta`, so a sink that
    /// names its output after the labeled moment (e.g. a snapshot file keyed
    /// by millis) does not have to smuggle that information through
    /// `Store::append`'s `meta` argument as a side channel. The default
    /// implementation is a no-op; sinks that render label snapshots (e.g. a
    /// `<label>.md` file per label) override this. Implementations must be
    /// idempotent — the same `(stream, label, at)` may arrive more than once
    /// after retries or a crash-and-catch-up.
    async fn on_label_set(
        &self,
        _stream: &StreamId,
        _label: &Label,
        _at: Seq,
        _state: &Value,
        _event: &Event,
    ) -> Result<(), StoreError> {
        Ok(())
    }

    /// React to a label being deleted.
    ///
    /// Called from the facade after `Store::label_delete` succeeds. Unlike
    /// `on_label_set` there is no `Seq` or materialized `state` argument —
    /// the label's target has already been forgotten by the backend by the
    /// time this fires. The default implementation is a no-op; sinks that
    /// render label snapshots (e.g. a `<label>.md` file per label) override
    /// this to archive or remove the rendered artifact. Implementations must
    /// be idempotent — the same `(stream, label)` may arrive more than once
    /// after retries or a crash-and-catch-up.
    async fn on_label_deleted(&self, _stream: &StreamId, _label: &Label) -> Result<(), StoreError> {
        Ok(())
    }
}

/// Detail for a single `(stream, sink)` pairing that failed during a
/// `Store::catch_up` / `Store::rebuild` call.
///
/// Recorded once per stream: catch-up isolates failures to the stream that
/// produced them (order within a stream must be preserved, so the first
/// failure on a stream halts *that stream only*), and continues driving
/// every other stream to completion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatchUpFailure {
    /// The stream whose catch-up was interrupted.
    pub stream: StreamId,
    /// The sink that failed (matches the `sink_id` argument to `catch_up`).
    pub sink_id: String,
    /// Human-readable failure reason (the failing `commit`'s `StoreError`,
    /// or a note about checkpoint persistence — see
    /// [`crate::CheckpointBackend`]).
    pub message: String,
}

/// Summary returned from `Store::catch_up` and `Store::rebuild`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatchUpReport {
    /// Number of events applied to the sink.
    pub applied: usize,
    /// Number of events skipped because a prior event on the same stream
    /// failed. Order must be preserved within a stream, so once an event
    /// fails, every event after it on that stream is skipped rather than
    /// applied out of order.
    pub skipped: usize,
    /// Number of events that failed and left the checkpoint unadvanced.
    pub failed: usize,
    /// One entry per stream whose catch-up failed this call. See
    /// [`CatchUpFailure`].
    pub failures: Vec<CatchUpFailure>,
}

impl CatchUpReport {
    /// An empty report (no events processed).
    pub const EMPTY: CatchUpReport = CatchUpReport {
        applied: 0,
        skipped: 0,
        failed: 0,
        failures: Vec::new(),
    };
}
