//! Event record and its pre-append draft form.
//!
//! An `Event` is a fully committed entry in the log: it carries an assigned `Seq`
//! and a `Timestamp` set at append time. `NewEvent` is what a backend receives —
//! the same shape minus the store-assigned coordinates, so the backend cannot
//! forge them.

use json_patch::Patch;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::id::{Seq, Timestamp};

/// A committed event in an append-only stream.
///
/// The `patch` is an RFC 6902 forward diff applied to the stream state at
/// `seq - 1` to produce the state at `seq`. The `meta` field carries
/// domain-specific attribution (author, cause, correlation id) that does not
/// affect the reconstructed state.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// Store-assigned stream coordinate.
    pub seq: Seq,
    /// Consumer-defined event kind (e.g. `"node_updated"`, `"reverted"`).
    pub kind: String,
    /// RFC 6902 forward patch from state at `seq - 1` to state at `seq`.
    pub patch: Patch,
    /// Domain-specific attribution not affecting state reconstruction.
    pub meta: Value,
    /// Wall-clock time of append.
    pub at: Timestamp,
}

/// A pre-append event draft handed to `EventBackend::append`.
///
/// The backend is responsible for assigning `Seq` and `Timestamp`. Consumers
/// never construct `NewEvent` directly — the `Store` facade builds it from the
/// public `append` arguments.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NewEvent {
    /// Consumer-defined event kind.
    pub kind: String,
    /// RFC 6902 forward patch to apply.
    pub patch: Patch,
    /// Domain-specific attribution.
    pub meta: Value,
}
