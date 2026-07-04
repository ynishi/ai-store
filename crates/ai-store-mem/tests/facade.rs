//! End-to-end tests for `Store` running against the in-memory backends.
//!
//! These tests pin the facade's public contract — every assertion here is a
//! guarantee `Store` must uphold regardless of the backend it wraps.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex as StdMutex,
};

use ai_store_core::{
    empty_state, CatchUpReport, Event, EventBackend, GateCtx, Label, Patch, ProjectionSink,
    SchemaGate, SchemaViolation, Seq, Store, StoreConfig, StoreError, StreamId, Timestamp,
};
use ai_store_mem::{MemCacheBackend, MemEventBackend};
use async_trait::async_trait;
use serde_json::{json, Value};

fn patch(v: Value) -> Patch {
    serde_json::from_value::<Patch>(v).unwrap()
}

fn store_no_gate_no_sink() -> Store {
    Store::new(
        Arc::new(MemEventBackend::new()),
        Arc::new(MemCacheBackend::new()),
        Vec::new(),
        Vec::new(),
        StoreConfig::default(),
    )
}

#[tokio::test]
async fn append_then_state_reconstructs_through_patches() {
    let store = store_no_gate_no_sink();
    let s = StreamId::new("doc");

    assert_eq!(store.state(&s).await.unwrap(), empty_state());

    store
        .append(
            &s,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": { "count": 0 } }])),
            json!({}),
        )
        .await
        .unwrap();

    store
        .append(
            &s,
            "bump",
            patch(json!([{ "op": "replace", "path": "/count", "value": 1 }])),
            json!({}),
        )
        .await
        .unwrap();

    assert_eq!(store.state(&s).await.unwrap(), json!({ "count": 1 }));
    assert_eq!(
        store.state_at(&s, Seq(1)).await.unwrap(),
        json!({ "count": 0 })
    );
}

#[tokio::test]
async fn state_at_rejects_seq_past_head() {
    let store = store_no_gate_no_sink();
    let s = StreamId::new("doc");
    store
        .append(
            &s,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": {} }])),
            json!({}),
        )
        .await
        .unwrap();

    let err = store.state_at(&s, Seq(99)).await.unwrap_err();
    assert!(matches!(
        err,
        StoreError::SeqOutOfRange {
            head: Some(Seq(1)),
            requested: Seq(99),
        }
    ));
}

#[tokio::test]
async fn revert_appends_as_new_event_and_restores_prior_state() {
    let store = store_no_gate_no_sink();
    let s = StreamId::new("doc");

    store
        .append(
            &s,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": { "n": 1 } }])),
            json!({}),
        )
        .await
        .unwrap();
    store
        .append(
            &s,
            "bump",
            patch(json!([{ "op": "replace", "path": "/n", "value": 2 }])),
            json!({}),
        )
        .await
        .unwrap();
    store
        .append(
            &s,
            "bump",
            patch(json!([{ "op": "replace", "path": "/n", "value": 3 }])),
            json!({}),
        )
        .await
        .unwrap();

    // revert to seq 1 should append a fourth event whose net effect is n=1.
    let revert_seq = store.revert(&s, Seq(1)).await.unwrap();
    assert_eq!(revert_seq, Seq(4));
    assert_eq!(store.state(&s).await.unwrap(), json!({ "n": 1 }));

    // History remains intact — the intermediate n=2 state is still readable.
    assert_eq!(store.state_at(&s, Seq(2)).await.unwrap(), json!({ "n": 2 }));
    assert_eq!(store.head(&s).await.unwrap(), Some(Seq(4)));

    // We can revert the revert — restoration is symmetric.
    store.revert(&s, Seq(3)).await.unwrap();
    assert_eq!(store.state(&s).await.unwrap(), json!({ "n": 3 }));
}

// ---- gate ---------------------------------------------------------------

struct RejectKind {
    forbidden: &'static str,
}

impl SchemaGate for RejectKind {
    fn validate(&self, ctx: &GateCtx<'_>) -> Result<(), SchemaViolation> {
        if ctx.kind == self.forbidden {
            Err(SchemaViolation::new(
                "forbidden_kind",
                format!("kind '{}' is not permitted", self.forbidden),
            ))
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn gate_rejection_aborts_before_backend_append() {
    let events = Arc::new(MemEventBackend::new());
    let cache = Arc::new(MemCacheBackend::new());
    let gate: Arc<dyn SchemaGate> = Arc::new(RejectKind {
        forbidden: "denied",
    });
    let store = Store::new(
        events.clone(),
        cache,
        vec![gate],
        Vec::new(),
        StoreConfig::default(),
    );

    let s = StreamId::new("doc");
    store
        .append(
            &s,
            "ok",
            patch(json!([{ "op": "add", "path": "", "value": {} }])),
            json!({}),
        )
        .await
        .unwrap();

    let err = store
        .append(
            &s,
            "denied",
            patch(json!([{ "op": "add", "path": "/x", "value": 1 }])),
            json!({}),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Schema(_)));

    // Backend head did not advance past the earlier successful append.
    assert_eq!(events.head(&s).await.unwrap(), Some(Seq(1)));
}

// ---- sink ---------------------------------------------------------------

#[derive(Default)]
struct RecordSink {
    id: String,
    seen: StdMutex<Vec<(String, u64)>>,
}

#[async_trait]
impl ProjectionSink for RecordSink {
    fn id(&self) -> &str {
        &self.id
    }
    async fn commit(
        &self,
        stream: &StreamId,
        seq: Seq,
        _state: &Value,
        _event: &Event,
    ) -> Result<(), StoreError> {
        self.seen
            .lock()
            .unwrap()
            .push((stream.as_str().to_string(), seq.0));
        Ok(())
    }
}

#[tokio::test]
async fn sink_receives_each_committed_event_once_via_append_dispatch() {
    let sink = Arc::new(RecordSink {
        id: "record".to_string(),
        seen: StdMutex::new(Vec::new()),
    });
    let store = Store::new(
        Arc::new(MemEventBackend::new()),
        Arc::new(MemCacheBackend::new()),
        Vec::new(),
        vec![sink.clone()],
        StoreConfig::default(),
    );

    let s = StreamId::new("doc");
    store
        .append(
            &s,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": {} }])),
            json!({}),
        )
        .await
        .unwrap();
    store
        .append(
            &s,
            "add",
            patch(json!([{ "op": "add", "path": "/x", "value": 1 }])),
            json!({}),
        )
        .await
        .unwrap();

    let seen = sink.seen.lock().unwrap().clone();
    assert_eq!(seen, vec![("doc".to_string(), 1), ("doc".to_string(), 2)]);

    // catch_up with nothing new to do reports zero applied.
    let report = store.catch_up("record").await.unwrap();
    assert_eq!(report, CatchUpReport::EMPTY);
}

#[tokio::test]
async fn rebuild_re_drives_sink_from_zero() {
    let sink = Arc::new(RecordSink {
        id: "record".to_string(),
        seen: StdMutex::new(Vec::new()),
    });
    let store = Store::new(
        Arc::new(MemEventBackend::new()),
        Arc::new(MemCacheBackend::new()),
        Vec::new(),
        vec![sink.clone()],
        StoreConfig::default(),
    );

    let s = StreamId::new("doc");
    store
        .append(
            &s,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": {} }])),
            json!({}),
        )
        .await
        .unwrap();
    store
        .append(
            &s,
            "add",
            patch(json!([{ "op": "add", "path": "/x", "value": 1 }])),
            json!({}),
        )
        .await
        .unwrap();

    // Clear the sink's own record so we can see the rebuild replay.
    sink.seen.lock().unwrap().clear();

    let report = store.rebuild("record").await.unwrap();
    assert_eq!(report.applied, 2);
    assert_eq!(report.failed, 0);

    let seen = sink.seen.lock().unwrap().clone();
    assert_eq!(seen, vec![("doc".to_string(), 1), ("doc".to_string(), 2)]);
}

struct FlakySink {
    id: String,
    fail_until: AtomicUsize,
    seen: StdMutex<Vec<u64>>,
}

#[async_trait]
impl ProjectionSink for FlakySink {
    fn id(&self) -> &str {
        &self.id
    }
    async fn commit(
        &self,
        _stream: &StreamId,
        seq: Seq,
        _state: &Value,
        _event: &Event,
    ) -> Result<(), StoreError> {
        if self.fail_until.load(Ordering::SeqCst) > 0 {
            self.fail_until.fetch_sub(1, Ordering::SeqCst);
            return Err(StoreError::Backend("flaky".to_string()));
        }
        self.seen.lock().unwrap().push(seq.0);
        Ok(())
    }
}

#[tokio::test]
async fn failed_sink_stays_at_checkpoint_and_catches_up_after_recovery() {
    let sink = Arc::new(FlakySink {
        id: "flaky".to_string(),
        fail_until: AtomicUsize::new(1),
        seen: StdMutex::new(Vec::new()),
    });
    let store = Store::new(
        Arc::new(MemEventBackend::new()),
        Arc::new(MemCacheBackend::new()),
        Vec::new(),
        vec![sink.clone()],
        StoreConfig::default(),
    );

    let s = StreamId::new("doc");
    // First append fails at the sink; append itself still succeeds because
    // the event is durable in the backend before dispatch.
    store
        .append(
            &s,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": {} }])),
            json!({}),
        )
        .await
        .unwrap();
    store
        .append(
            &s,
            "add",
            patch(json!([{ "op": "add", "path": "/x", "value": 1 }])),
            json!({}),
        )
        .await
        .unwrap();

    // After the first (failed) dispatch and one successful one, only the
    // second event should be observed by the sink so far.
    assert_eq!(sink.seen.lock().unwrap().clone(), vec![2]);

    // catch_up re-drives from the checkpoint (Seq(0)) up to head. Sinks are
    // contracted to be idempotent under retries, so seq 2 may be replayed;
    // what we assert here is that seq 1 (the gap) is now covered and that
    // the sink ends up seeing every seq up to head.
    let report = store.catch_up("flaky").await.unwrap();
    assert_eq!(report.applied, 2);
    assert_eq!(report.failed, 0);

    let seen_set: std::collections::BTreeSet<u64> =
        sink.seen.lock().unwrap().iter().copied().collect();
    assert_eq!(
        seen_set,
        [1u64, 2u64]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
    );
}

// ---- seq_at_time --------------------------------------------------------

#[tokio::test]
async fn seq_at_time_composes_with_state_at_for_wall_clock_restore() {
    let store = store_no_gate_no_sink();
    let s = StreamId::new("doc");

    store
        .append(
            &s,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": { "n": 1 } }])),
            json!({}),
        )
        .await
        .unwrap();

    // Capture the wall-clock at seq 1 by peeking at the raw event, then
    // separate the next append by more than one ms so timestamps are
    // strictly distinct on fast machines.
    let t1 = store.read(&s, Seq(1), 1).await.unwrap()[0].at;
    std::thread::sleep(std::time::Duration::from_millis(3));

    store
        .append(
            &s,
            "bump",
            patch(json!([{ "op": "replace", "path": "/n", "value": 2 }])),
            json!({}),
        )
        .await
        .unwrap();

    // Composing seq_at_time -> state_at recovers the state as of an instant.
    let seq = store.seq_at_time(&s, t1).await.unwrap().unwrap();
    let state = store.state_at(&s, seq).await.unwrap();
    assert_eq!(state, json!({ "n": 1 }));

    // A timestamp far in the past resolves to None.
    assert_eq!(store.seq_at_time(&s, Timestamp(0)).await.unwrap(), None);
}

// ---- labels + facade wrapping ------------------------------------------

#[tokio::test]
async fn label_resolve_via_facade_surfaces_unknown_label_error() {
    let store = store_no_gate_no_sink();
    let s = StreamId::new("doc");
    store
        .append(
            &s,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": {} }])),
            json!({}),
        )
        .await
        .unwrap();

    let err = store
        .label_resolve(&s, &Label::new("v1"))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::UnknownLabel(name) if name == "v1"));

    store
        .label_set(&s, &Label::new("v1"), Seq(1))
        .await
        .unwrap();
    assert_eq!(
        store.label_resolve(&s, &Label::new("v1")).await.unwrap(),
        Seq(1)
    );
}

#[tokio::test]
async fn read_by_meta_default_impl_filters_client_side() {
    let store = store_no_gate_no_sink();
    let s = StreamId::new("doc");

    // Initialize empty object so subsequent /step_N adds have a path parent.
    store
        .append(
            &s,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": {} }])),
            json!({ "entity_id": "init" }),
        )
        .await
        .unwrap();

    // Seed three events: two touch entity A, one touches entity B (seqs 2..=4).
    for (i, ent) in ["A", "B", "A"].iter().enumerate() {
        store
            .append(
                &s,
                "touch",
                patch(json!([{ "op": "add", "path": format!("/step_{}", i), "value": ent }])),
                json!({ "entity_id": ent }),
            )
            .await
            .unwrap();
    }

    // Filter by entity A → two matches (seq 2 and seq 4).
    let hits = store
        .read_by_meta(&s, "entity_id", &json!("A"), Seq(2), 10)
        .await
        .unwrap();
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].seq, Seq(2));
    assert_eq!(hits[1].seq, Seq(4));

    // Limit caps at match count.
    let capped = store
        .read_by_meta(&s, "entity_id", &json!("A"), Seq(2), 1)
        .await
        .unwrap();
    assert_eq!(capped.len(), 1);
    assert_eq!(capped[0].seq, Seq(2));

    // Non-matching value → empty.
    let none = store
        .read_by_meta(&s, "entity_id", &json!("Z"), Seq(1), 10)
        .await
        .unwrap();
    assert!(none.is_empty());

    // `from` skips earlier matches.
    let from_mid = store
        .read_by_meta(&s, "entity_id", &json!("A"), Seq(3), 10)
        .await
        .unwrap();
    assert_eq!(from_mid.len(), 1);
    assert_eq!(from_mid[0].seq, Seq(4));

    // limit=0 short-circuits to empty.
    let zero = store
        .read_by_meta(&s, "entity_id", &json!("A"), Seq(1), 0)
        .await
        .unwrap();
    assert!(zero.is_empty());
}

/// Verify the append fast path: gates absent, sinks absent, cache stride
/// misses — state is still reconstructible via replay from the log, since
/// the fast path skips both pre- and post-commit `next` materialization.
#[tokio::test]
async fn append_fast_path_preserves_state_semantics() {
    // Explicit stride large enough that seqs 1..=5 all miss the boundary.
    let store = Store::new(
        Arc::new(MemEventBackend::new()),
        Arc::new(MemCacheBackend::new()),
        Vec::new(),
        Vec::new(),
        StoreConfig { cache_stride: 100 },
    );
    let s = StreamId::new("doc");

    store
        .append(
            &s,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": { "n": 0 } }])),
            json!({}),
        )
        .await
        .unwrap();
    for i in 1..5 {
        store
            .append(
                &s,
                "bump",
                patch(json!([{ "op": "replace", "path": "/n", "value": i }])),
                json!({}),
            )
            .await
            .unwrap();
    }

    // No cache entries were written (all stride misses); state must still
    // reconstruct correctly from the log alone.
    assert_eq!(store.state(&s).await.unwrap(), json!({ "n": 4 }));
    assert_eq!(store.state_at(&s, Seq(3)).await.unwrap(), json!({ "n": 2 }));
    assert_eq!(store.head(&s).await.unwrap(), Some(Seq(5)));
}

#[tokio::test]
async fn read_by_meta_matches_json_null() {
    let store = store_no_gate_no_sink();
    let s = StreamId::new("doc");

    store
        .append(
            &s,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": {} }])),
            json!({ "owner": Value::Null }),
        )
        .await
        .unwrap();
    store
        .append(
            &s,
            "with_value",
            patch(json!([{ "op": "add", "path": "/x", "value": 1 }])),
            json!({ "owner": "alice" }),
        )
        .await
        .unwrap();

    let hits = store
        .read_by_meta(&s, "owner", &Value::Null, Seq(1), 10)
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].seq, Seq(1));
}
