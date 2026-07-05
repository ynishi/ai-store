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

/// The coordinates a backend assigns to a write, handed back to the caller.
///
/// Every write path (`EventBackend::append`, `EventBackend::import_event`,
/// and the `Store` facade methods built on top of them) returns `Committed`
/// instead of a bare `Seq`. Without it, a caller that needs the backend's
/// stamped `at` (rather than the wall-clock instant the caller itself
/// observed) had no choice but to immediately `read(stream, seq, 1)` the
/// event straight back — a round-trip this type exists to remove. `at`
/// mirrors [`Event::at`]/[`Event::seq`] exactly (same value, same backend
/// assignment), it is simply returned inline instead of requiring a
/// follow-up read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Committed {
    /// Store-assigned stream coordinate.
    pub seq: Seq,
    /// Wall-clock time `append` stamped, or the caller-supplied historical
    /// time `import_event` stamped — whichever the backend actually wrote.
    pub at: Timestamp,
}
