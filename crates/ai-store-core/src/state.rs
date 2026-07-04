//! Pure state reconstruction from an event log.
//!
//! `replay_from` applies a slice of `Event`s (each carrying an RFC 6902
//! forward patch) onto a starting state. It is deliberately domain-free — the
//! store never needs a consumer-supplied reducer because the patch itself
//! carries all state-transition information.

use serde_json::Value;

use crate::error::StoreError;
use crate::event::Event;

/// Sentinel value representing an empty stream state (before any event has
/// been applied). Callers who need a domain-specific empty state should
/// establish it by appending a first "initial" event that populates the root.
pub fn empty_state() -> Value {
    Value::Null
}

/// Apply a chain of events onto `base`, in order, returning the final state.
///
/// Each event's `patch` is applied to the running state. If any patch fails
/// to apply, returns `StoreError::Patch` naming the offending seq.
pub fn replay_from(base: Value, events: &[Event]) -> Result<Value, StoreError> {
    let mut state = base;
    for ev in events {
        json_patch::patch(&mut state, &ev.patch)
            .map_err(|e| StoreError::Patch(format!("event seq={}: {e}", ev.seq)))?;
    }
    Ok(state)
}
