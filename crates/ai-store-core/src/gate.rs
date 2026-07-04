//! Schema validation gate — the only pre-append check the facade exposes.
//!
//! `SchemaGate` implementations receive the full context around a candidate
//! write (`GateCtx`) — the stream, the event kind, the forward patch, the state
//! immediately before the patch, and the state that would result after the
//! patch. They may reject on any of these axes. The `Store` facade guarantees
//! that every gate is invoked before the event reaches the backend.

use json_patch::Patch;
use serde_json::Value;

use crate::error::SchemaViolation;
use crate::id::StreamId;

/// Context handed to a `SchemaGate` for a single candidate write.
///
/// All fields are borrowed so the gate cannot mutate the write. `current` and
/// `next` let a gate enforce postconditions on the resulting state (e.g. "the
/// document has 5 required sections after this write"), not just structural
/// checks on the patch.
pub struct GateCtx<'a> {
    /// Target stream.
    pub stream: &'a StreamId,
    /// Consumer-defined event kind.
    pub kind: &'a str,
    /// The forward patch to be applied.
    pub patch: &'a Patch,
    /// State immediately before applying the patch.
    pub current: &'a Value,
    /// State that would result from applying the patch.
    pub next: &'a Value,
}

/// Pre-append validation hook.
///
/// The `Store` facade invokes every registered gate before delegating to the
/// backend. Any gate returning `Err` aborts the write; no partial state is ever
/// visible.
pub trait SchemaGate: Send + Sync {
    /// Validate a candidate write. Return `Err` to abort the append.
    fn validate(&self, ctx: &GateCtx<'_>) -> Result<(), SchemaViolation>;
}
