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

    /// Requested label is not defined on the stream.
    #[error("unknown label: {0}")]
    UnknownLabel(String),
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
