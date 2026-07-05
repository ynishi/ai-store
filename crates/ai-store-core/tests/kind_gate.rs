//! End-to-end test: `KindGate` wired into a real `Store` via `StoreBuilder`.
//!
//! Unit-level dispatch behavior (kind matching / fallback / duplicate
//! registration) lives in `ai_store_core::kind_gate`'s own `#[cfg(test)]`
//! module; this test pins the integration seam — a `KindGate` registered
//! through `Store::builder(..).gate(..)` actually blocks the matching
//! `Store::append` call before it reaches the backend.

use std::sync::Arc;

use ai_store_core::{
    EventBackend, GateCtx, KindGate, SchemaViolation, Seq, Store, StoreError, StreamId,
};
use ai_store_mem::{MemCacheBackend, MemEventBackend};
use serde_json::json;

fn patch(v: serde_json::Value) -> json_patch::Patch {
    serde_json::from_value(v).unwrap()
}

#[tokio::test]
async fn kind_gate_blocks_the_matching_kind_and_lets_others_through() {
    let events = Arc::new(MemEventBackend::new());
    let gate = KindGate::new().on("close", |ctx: &GateCtx<'_>| {
        if ctx.next.get("sections").is_some() {
            Ok(())
        } else {
            Err(SchemaViolation::new(
                "missing_sections",
                "close requires a populated 'sections' field",
            ))
        }
    });

    let store = Store::builder(events.clone(), Arc::new(MemCacheBackend::new()))
        .gate(Arc::new(gate))
        .build();
    let s = StreamId::new("chapter");

    // "append" has no registration and no fallback — passes through.
    store
        .append(
            &s,
            "append",
            patch(json!([{ "op": "add", "path": "", "value": { "n": 1 } }])),
            json!({}),
        )
        .await
        .unwrap();

    // "close" is registered and the candidate write does not add
    // "sections" — rejected before it reaches the backend.
    let err = store
        .append(
            &s,
            "close",
            patch(json!([{ "op": "add", "path": "/x", "value": 1 }])),
            json!({}),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Schema(_)));
    assert_eq!(events.head(&s).await.unwrap(), Some(Seq(1)));

    // A "close" write that does populate "sections" passes.
    store
        .append(
            &s,
            "close",
            patch(json!([{ "op": "add", "path": "/sections", "value": [] }])),
            json!({}),
        )
        .await
        .unwrap();
    assert_eq!(
        store.state(&s).await.unwrap(),
        json!({ "n": 1, "sections": [] })
    );
}
