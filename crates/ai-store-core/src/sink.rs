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
    /// the materialized state at `at` (freshly reconstructed). The default
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
    ) -> Result<(), StoreError> {
        Ok(())
    }
}

/// Summary returned from `Store::catch_up` and `Store::rebuild`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatchUpReport {
    /// Number of events applied to the sink.
    pub applied: usize,
    /// Number of events skipped (already past the checkpoint).
    pub skipped: usize,
    /// Number of events that failed and left the checkpoint unadvanced.
    pub failed: usize,
}

impl CatchUpReport {
    /// An empty report (no events processed).
    pub const EMPTY: CatchUpReport = CatchUpReport {
        applied: 0,
        skipped: 0,
        failed: 0,
    };
}
