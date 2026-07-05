//! Error types surfaced by the store facade.
//!
//! `StoreError` is the public error enum returned from `Store` methods.
//! `SchemaViolation` is a distinct value the `SchemaGate` returns when a
//! candidate write is rejected before it reaches the backend.

use thiserror::Error;

use crate::id::{Seq, StreamId};

/// Error variants returned from the `Store` public facade.
#[derive(Debug, Error)]
pub enum StoreError {
    /// The candidate write was rejected by a `SchemaGate`.
    #[error("schema violation: {0}")]
    Schema(SchemaViolation),

    /// Attempt to apply a patch that produced an invalid state.
    #[error("patch application failed: {0}")]
    Patch(String),

    /// Backend I/O or persistence failure (SQLite error, disk error, etc.).
    #[error("backend error: {0}")]
    Backend(String),

    /// Requested stream does not exist.
    #[error("unknown stream: {0}")]
    UnknownStream(StreamId),

    /// Requested coordinate is beyond the current head of the stream.
    #[error("seq out of range: stream has head={head:?}, requested={requested:?}")]
    SeqOutOfRange {
        /// Current head of the stream (`None` if the stream is empty).
        head: Option<Seq>,
        /// The seq that was requested.
        requested: Seq,
    },

    /// Requested coordinate falls before the compaction boundary of this
    /// stream — the events that would be needed to reconstruct that state
    /// have been replaced by a snapshot at `boundary`, so the state is no
    /// longer materially reachable.
    ///
    /// Callers can still reach `boundary` itself (the snapshot event
    /// materializes exactly that state) and any seq strictly after it. See
    /// [`crate::SNAPSHOT_KIND`] and the "Compaction and history boundary"
    /// section in `Store`'s module-level rustdoc.
    #[error("seq below compaction boundary: stream compacted to {boundary:?}, requested={requested:?}")]
    SeqCompacted {
        /// Earliest seq still materially reachable (the snapshot event's seq).
        boundary: Seq,
        /// The seq that was requested.
        requested: Seq,
    },

    /// Requested label is not defined on the stream.
    #[error("unknown label: {0}")]
    UnknownLabel(String),

    /// Requested sink id is not registered on the store.
    #[error("unknown sink: {0}")]
    UnknownSink(String),

    /// Optimistic-concurrency append rejected: the stream's head moved
    /// between the caller's expectation and the backend's atomic check.
    ///
    /// Surfaced from [`crate::Store::append_if_head`] (and the underlying
    /// [`crate::EventBackend::append_if_head`]) when a compare-and-swap
    /// append cannot proceed because the stream already has more (or fewer)
    /// events than the caller assumed. `expected` is the head the caller
    /// passed; `actual` is the head the backend observed inside the same
    /// transaction it would have appended in (`None` if the stream is
    /// empty). The caller can retry after reconstructing state at `actual`
    /// or surface the conflict to the domain.
    #[error("head conflict: expected={expected:?}, actual={actual:?}")]
    HeadConflict {
        /// The head coordinate the caller expected before appending.
        expected: Seq,
        /// The head coordinate the backend actually observed.
        actual: Option<Seq>,
    },

    /// The backend does not support this operation.
    ///
    /// Returned by the default implementation of
    /// [`crate::EventBackend::import_event`] when a backend has not opted in
    /// to honoring a caller-supplied historical timestamp. The payload is a
    /// short operation name (e.g. `"import_event"`).
    #[error("backend does not support this operation: {0}")]
    BackendUnsupported(String),
}

/// Rejection payload emitted by a `SchemaGate` implementation.
///
/// Kept structured (kind + message) so consumers can pattern-match on well-known
/// rejection categories while still surfacing a human-readable reason.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{kind}: {reason}")]
pub struct SchemaViolation {
    /// Short machine-readable rejection category (e.g. `"missing_field"`).
    pub kind: String,
    /// Human-readable explanation.
    pub reason: String,
}

impl SchemaViolation {
    /// Construct a violation from kind and reason.
    pub fn new(kind: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            kind: kind.into(),
            reason: reason.into(),
        }
    }
}
