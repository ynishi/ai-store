//! Kind-dispatching `SchemaGate` helper.
//!
//! `SchemaGate::validate` receives every candidate write regardless of
//! `kind`, so a consumer whose validation rules differ per event kind (e.g.
//! `journal-mcp`: an `"append"` write must satisfy a section-transition
//! invariant, a `"close"` write must satisfy a "5 required sections present"
//! invariant) has to write that `match ctx.kind { ... }` dispatch by hand
//! inside its own `SchemaGate` impl every time. `KindGate` is that dispatch,
//! implemented once: register a validator per kind with `.on`, optionally a
//! `.fallback` for anything unregistered, and hand the result to
//! `Store::builder(..).gate(Arc::new(kind_gate))` like any other gate.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::SchemaViolation;
use crate::gate::{GateCtx, SchemaGate};

type Validator = dyn Fn(&GateCtx<'_>) -> Result<(), SchemaViolation> + Send + Sync;

/// A [`SchemaGate`] that dispatches to a per-kind validator.
///
/// Kinds with no registered validator (and no [`KindGate::fallback`]) pass
/// through unconditionally — `KindGate::new()` alone accepts every write,
/// same as having no gate registered at all. This is deliberate: adding a
/// new event kind to a stream should not silently start failing validation
/// just because `KindGate` exists, only because *someone* registered a
/// validator for that kind (or a fallback that rejects unknowns).
///
/// ## `REVERT_KIND` is not special-cased
///
/// [`crate::REVERT_KIND`] (the `"reverted"` kind `Store::revert` /
/// `Store::revert_with_meta` append) is dispatched through `.on` /
/// `.fallback` exactly like any consumer-defined kind — `KindGate` has no
/// built-in allowance for it. A validator or fallback that rejects it can
/// make a stream's history unrecoverable via `Store::revert` (the revert
/// event itself never reaches the backend). Consumers that gate on kind and
/// also want `revert` to remain usable should either register an explicit
/// `.on(ai_store_core::REVERT_KIND, ..)` that only checks what it must, or
/// make sure the fallback (if any) accepts it.
pub struct KindGate {
    validators: HashMap<String, Arc<Validator>>,
    fallback: Option<Arc<Validator>>,
}

impl Default for KindGate {
    fn default() -> Self {
        Self::new()
    }
}

impl KindGate {
    /// Construct an empty `KindGate`. Every kind passes through until `.on`
    /// or `.fallback` registers a validator for it.
    pub fn new() -> Self {
        Self {
            validators: HashMap::new(),
            fallback: None,
        }
    }

    /// Register a validator invoked for events whose `kind` exactly equals
    /// `kind`.
    ///
    /// Registering the same `kind` twice replaces the previous validator —
    /// the most recent `.on(kind, ..)` call for a given `kind` wins, mirroring
    /// how a plain `HashMap::insert` behaves.
    pub fn on<F>(mut self, kind: impl Into<String>, validator: F) -> Self
    where
        F: Fn(&GateCtx<'_>) -> Result<(), SchemaViolation> + Send + Sync + 'static,
    {
        self.validators.insert(kind.into(), Arc::new(validator));
        self
    }

    /// Register a fallback validator invoked for any `kind` with no `.on`
    /// registration. Without a fallback, unregistered kinds pass through
    /// unconditionally (`Ok(())`) — see the "REVERT_KIND is not
    /// special-cased" note above for what that means for reverts
    /// specifically.
    pub fn fallback<F>(mut self, validator: F) -> Self
    where
        F: Fn(&GateCtx<'_>) -> Result<(), SchemaViolation> + Send + Sync + 'static,
    {
        self.fallback = Some(Arc::new(validator));
        self
    }
}

impl SchemaGate for KindGate {
    fn validate(&self, ctx: &GateCtx<'_>) -> Result<(), SchemaViolation> {
        match self.validators.get(ctx.kind) {
            Some(validator) => validator(ctx),
            None => match &self.fallback {
                Some(fallback) => fallback(ctx),
                None => Ok(()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::id::StreamId;

    fn empty_patch() -> json_patch::Patch {
        serde_json::from_value(json!([])).unwrap()
    }

    fn ctx<'a>(stream: &'a StreamId, kind: &'a str, patch: &'a json_patch::Patch) -> GateCtx<'a> {
        GateCtx {
            stream,
            kind,
            patch,
            current: &serde_json::Value::Null,
            next: &serde_json::Value::Null,
        }
    }

    #[test]
    fn dispatches_to_the_matching_kind_validator_only() {
        let gate = KindGate::new()
            .on("append", |_ctx| Ok(()))
            .on("close", |_ctx| {
                Err(SchemaViolation::new("no_close", "close is forbidden"))
            });

        let stream = StreamId::new("doc");
        let patch = empty_patch();

        assert!(gate.validate(&ctx(&stream, "append", &patch)).is_ok());
        let err = gate.validate(&ctx(&stream, "close", &patch)).unwrap_err();
        assert_eq!(err.kind, "no_close");
    }

    #[test]
    fn unregistered_kind_without_fallback_passes_through() {
        let gate = KindGate::new().on("append", |_ctx| {
            Err(SchemaViolation::new("no_append", "denied"))
        });

        let stream = StreamId::new("doc");
        let patch = empty_patch();

        // "rename" has no registration and there is no fallback — it passes.
        assert!(gate.validate(&ctx(&stream, "rename", &patch)).is_ok());
    }

    #[test]
    fn fallback_handles_every_unregistered_kind() {
        let gate = KindGate::new().on("append", |_ctx| Ok(())).fallback(|ctx| {
            Err(SchemaViolation::new(
                "unknown_kind",
                format!("kind '{}' has no registered validator", ctx.kind),
            ))
        });

        let stream = StreamId::new("doc");
        let patch = empty_patch();

        // Registered kind still uses its own validator, not the fallback.
        assert!(gate.validate(&ctx(&stream, "append", &patch)).is_ok());

        // Unregistered kind is routed to the fallback.
        let err = gate.validate(&ctx(&stream, "rename", &patch)).unwrap_err();
        assert_eq!(err.kind, "unknown_kind");
    }

    #[test]
    fn duplicate_registration_for_the_same_kind_last_wins() {
        let gate = KindGate::new()
            .on("append", |_ctx| Err(SchemaViolation::new("first", "first")))
            .on("append", |_ctx| Ok(()));

        let stream = StreamId::new("doc");
        let patch = empty_patch();

        assert!(gate.validate(&ctx(&stream, "append", &patch)).is_ok());
    }
}
