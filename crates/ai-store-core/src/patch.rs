//! Convenience constructors for single-purpose [`json_patch::Patch`] values.
//!
//! ## Why
//!
//! `json_patch::Patch` is a general-purpose RFC 6902 document (any mix of
//! `add`/`remove`/`replace`/`move`/`copy`/`test` operations). Building one by
//! hand for the common "add a value at a pointer" shape means either
//! constructing `PatchOperation` variants directly (verbose, easy to typo a
//! field) or round-tripping through `serde_json::json!` + `from_value`
//! (works, but is exactly the string-assembly boilerplate this module
//! removes — every call site pays a JSON-serialize-then-deserialize tax to
//! build a value it could have constructed directly). Domain-event-style
//! writes (e.g. "append the new body text at `/body`") only ever need one or
//! a handful of single-pointer operations, so [`add`], [`replace`],
//! [`remove`], and [`Builder`] cover that shape without either cost.
//!
//! ## Path validity
//!
//! `path` validity (RFC 6901 JSON Pointer syntax: empty, or starting with
//! `/`, with `~0`/`~1` escaping) is delegated entirely to
//! [`json_patch::jsonptr::PointerBuf`] — this module does not re-validate or
//! restrict what a syntactically valid pointer may look like (e.g. whether
//! it points at an existing location in some document is *never* checked
//! here; that is `json_patch::patch`'s job at apply time). See the "Panics"
//! section on each function for what happens when `path` fails that
//! delegated check.

use json_patch::jsonptr::PointerBuf;
use json_patch::{AddOperation, Patch, PatchOperation, RemoveOperation, ReplaceOperation};
use serde_json::Value;

/// Parse `path` as a JSON Pointer, panicking on malformed input.
///
/// # Panics
///
/// Panics if `path` is not a syntactically valid RFC 6901 JSON Pointer (for
/// example, a non-empty path missing its leading `/`). `path` is expected to
/// be a caller-controlled literal or a value built from known-safe
/// components (e.g. `format!("/{field}")` for a field name that cannot
/// contain `/`) — not unsanitized external input — so a malformed pointer is
/// treated as a programmer error rather than a runtime condition to recover
/// from. Callers that must accept untrusted paths should validate with
/// [`json_patch::jsonptr::PointerBuf::try_from`] themselves before reaching
/// for `add`/`replace`/`remove`/[`Builder`].
fn parse_ptr(path: &str) -> PointerBuf {
    PointerBuf::try_from(path).unwrap_or_else(|e| {
        panic!("ai_store_core::patch: {path:?} is not a valid RFC 6901 JSON Pointer: {e}")
    })
}

/// Build a one-operation `Patch` that adds `value` at `path`.
///
/// # Panics
///
/// See the "Panics" section of `parse_ptr` — `path` must be a
/// syntactically valid JSON Pointer.
pub fn add(path: &str, value: Value) -> Patch {
    Patch(vec![PatchOperation::Add(AddOperation {
        path: parse_ptr(path),
        value,
    })])
}

/// Build a one-operation `Patch` that replaces the value at `path`.
///
/// # Panics
///
/// See the "Panics" section of `parse_ptr` — `path` must be a
/// syntactically valid JSON Pointer.
pub fn replace(path: &str, value: Value) -> Patch {
    Patch(vec![PatchOperation::Replace(ReplaceOperation {
        path: parse_ptr(path),
        value,
    })])
}

/// Build a one-operation `Patch` that removes the value at `path`.
///
/// # Panics
///
/// See the "Panics" section of `parse_ptr` — `path` must be a
/// syntactically valid JSON Pointer.
pub fn remove(path: &str) -> Patch {
    Patch(vec![PatchOperation::Remove(RemoveOperation {
        path: parse_ptr(path),
    })])
}

/// Accumulates `add`/`replace`/`remove` operations into a single `Patch`.
///
/// Operations are applied in the order they were pushed — [`Builder`] does
/// not reorder or deduplicate them, matching `json_patch::patch`'s own
/// sequential-application semantics.
///
/// ```
/// use ai_store_core::patch::Builder;
/// use serde_json::json;
///
/// let p = Builder::new()
///     .add("/title", json!("draft"))
///     .replace("/n", json!(1))
///     .remove("/scratch")
///     .build();
/// assert_eq!(p.0.len(), 3);
/// ```
#[derive(Debug, Default, Clone)]
pub struct Builder {
    ops: Vec<PatchOperation>,
}

impl Builder {
    /// Start an empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Queue an `add` operation at `path`.
    ///
    /// # Panics
    ///
    /// See the "Panics" section of `parse_ptr` — `path` must be a
    /// syntactically valid JSON Pointer.
    pub fn add(mut self, path: &str, value: Value) -> Self {
        self.ops.push(PatchOperation::Add(AddOperation {
            path: parse_ptr(path),
            value,
        }));
        self
    }

    /// Queue a `replace` operation at `path`.
    ///
    /// # Panics
    ///
    /// See the "Panics" section of `parse_ptr` — `path` must be a
    /// syntactically valid JSON Pointer.
    pub fn replace(mut self, path: &str, value: Value) -> Self {
        self.ops.push(PatchOperation::Replace(ReplaceOperation {
            path: parse_ptr(path),
            value,
        }));
        self
    }

    /// Queue a `remove` operation at `path`.
    ///
    /// # Panics
    ///
    /// See the "Panics" section of `parse_ptr` — `path` must be a
    /// syntactically valid JSON Pointer.
    pub fn remove(mut self, path: &str) -> Self {
        self.ops.push(PatchOperation::Remove(RemoveOperation {
            path: parse_ptr(path),
        }));
        self
    }

    /// Consume the builder, producing the accumulated `Patch`.
    pub fn build(self) -> Patch {
        Patch(self.ops)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Apply `p` to `doc`, panicking (test failure) if application fails —
    /// keeps call sites below to a single line instead of repeating the
    /// `unwrap()` boilerplate for every case.
    fn apply(doc: &mut Value, p: &Patch) {
        json_patch::patch(doc, p).unwrap();
    }

    #[test]
    fn add_applies_and_inserts_value() {
        let mut doc = json!({});
        apply(&mut doc, &add("/title", json!("draft")));
        assert_eq!(doc, json!({ "title": "draft" }));
    }

    #[test]
    fn add_at_root_replaces_whole_document() {
        let mut doc = json!({ "old": true });
        apply(&mut doc, &add("", json!({ "n": 1 })));
        assert_eq!(doc, json!({ "n": 1 }));
    }

    #[test]
    fn replace_applies_and_overwrites_existing_value() {
        let mut doc = json!({ "n": 0 });
        apply(&mut doc, &replace("/n", json!(9)));
        assert_eq!(doc, json!({ "n": 9 }));
    }

    #[test]
    fn remove_applies_and_deletes_the_key() {
        let mut doc = json!({ "n": 1, "scratch": "drop me" });
        apply(&mut doc, &remove("/scratch"));
        assert_eq!(doc, json!({ "n": 1 }));
    }

    #[test]
    fn builder_applies_ops_in_push_order() {
        let mut doc = json!({});
        let p = Builder::new()
            .add("/n", json!(0))
            .replace("/n", json!(1))
            .add("/scratch", json!("temp"))
            .remove("/scratch")
            .build();
        assert_eq!(p.0.len(), 4);
        apply(&mut doc, &p);
        assert_eq!(doc, json!({ "n": 1 }));
    }

    #[test]
    fn builder_with_no_ops_builds_an_empty_patch() {
        let p = Builder::new().build();
        assert_eq!(p.0.len(), 0);
    }

    #[test]
    #[should_panic(expected = "not a valid RFC 6901 JSON Pointer")]
    fn add_panics_on_malformed_path() {
        // Missing the required leading `/` — a programmer-error precondition
        // violation, not a runtime condition callers are expected to handle.
        let _ = add("no-leading-slash", json!(1));
    }
}
