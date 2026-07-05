//! End-to-end tests for `Store` running against the in-memory backends.
//!
//! These tests pin the facade's public contract — every assertion here is a
//! guarantee `Store` must uphold regardless of the backend it wraps.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex as StdMutex,
};

use ai_store_core::{
    empty_state, CatchUpReport, CheckpointBackend, Event, EventBackend, GateCtx, Label, Patch,
    ProjectionSink, SchemaGate, SchemaViolation, Seq, Store, StoreConfig, StoreError, StreamId,
    Timestamp,
};
use ai_store_mem::{MemCacheBackend, MemCheckpointBackend, MemEventBackend};
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
    let reverted = store.revert(&s, Seq(1)).await.unwrap();
    assert_eq!(reverted.seq, Seq(4));
    assert_eq!(store.state(&s).await.unwrap(), json!({ "n": 1 }));

    // History remains intact — the intermediate n=2 state is still readable.
    assert_eq!(store.state_at(&s, Seq(2)).await.unwrap(), json!({ "n": 2 }));
    assert_eq!(store.head(&s).await.unwrap(), Some(Seq(4)));

    // We can revert the revert — restoration is symmetric.
    store.revert(&s, Seq(3)).await.unwrap();
    assert_eq!(store.state(&s).await.unwrap(), json!({ "n": 3 }));
}

// ---- revert_with_meta -----------------------------------------------------

#[tokio::test]
async fn revert_with_meta_merges_extra_meta_into_the_appended_event() {
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

    let reverted = store
        .revert_with_meta(&s, Seq(1), json!({ "node_id": "abc123" }))
        .await
        .unwrap();

    let events = store.read(&s, reverted.seq, 1).await.unwrap();
    assert_eq!(
        events[0].meta,
        json!({ "node_id": "abc123", "revert_to": 1 })
    );
}

#[tokio::test]
async fn revert_with_meta_reserved_revert_to_key_always_wins() {
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

    // A caller-supplied `revert_to` in extra_meta must not be able to spoof
    // the target seq the store actually reverted to.
    let reverted = store
        .revert_with_meta(&s, Seq(1), json!({ "revert_to": 999 }))
        .await
        .unwrap();

    let events = store.read(&s, reverted.seq, 1).await.unwrap();
    assert_eq!(events[0].meta, json!({ "revert_to": 1 }));
}

#[tokio::test]
async fn revert_with_meta_ignores_non_object_extra_meta() {
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

    // Value::Null (== plain `revert`'s own extra_meta) and any other
    // non-object shape are silently dropped rather than rejected.
    let reverted_null = store
        .revert_with_meta(&s, Seq(1), Value::Null)
        .await
        .unwrap();
    let events = store.read(&s, reverted_null.seq, 1).await.unwrap();
    assert_eq!(events[0].meta, json!({ "revert_to": 1 }));

    let reverted_string = store
        .revert_with_meta(&s, Seq(1), json!("not an object"))
        .await
        .unwrap();
    let events = store.read(&s, reverted_string.seq, 1).await.unwrap();
    assert_eq!(events[0].meta, json!({ "revert_to": 1 }));
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

// ---- import_event ---------------------------------------------------------

#[tokio::test]
async fn import_event_preserves_caller_supplied_timestamp() {
    let store = store_no_gate_no_sink();
    let s = StreamId::new("doc");
    let at = Timestamp(1_700_000_000_000);

    let committed = store
        .import_event(
            &s,
            "legacy_create",
            patch(json!([{ "op": "add", "path": "", "value": { "n": 0 } }])),
            json!({}),
            at,
        )
        .await
        .unwrap();
    assert_eq!(committed.seq, Seq(1));
    assert_eq!(committed.at, at);

    let events = store.read(&s, Seq(1), 1).await.unwrap();
    assert_eq!(events[0].at, at);
}

#[tokio::test]
async fn import_event_into_empty_stream_supports_historical_seq_at_time() {
    let store = store_no_gate_no_sink();
    let s = StreamId::new("doc");

    store
        .import_event(
            &s,
            "create",
            patch(json!([{ "op": "add", "path": "", "value": { "n": 0 } }])),
            json!({}),
            Timestamp(1_700_000_000_000),
        )
        .await
        .unwrap();
    store
        .import_event(
            &s,
            "bump",
            patch(json!([{ "op": "replace", "path": "/n", "value": 1 }])),
            json!({}),
            Timestamp(1_700_000_060_000),
        )
        .await
        .unwrap();

    // Between the two historical timestamps resolves to the first import.
    let seq = store
        .seq_at_time(&s, Timestamp(1_700_000_030_000))
        .await
        .unwrap();
    assert_eq!(seq, Some(Seq(1)));

    // At or after the second timestamp resolves to the second import.
    let seq2 = store
        .seq_at_time(&s, Timestamp(1_700_000_060_000))
        .await
        .unwrap();
    assert_eq!(seq2, Some(Seq(2)));
}

#[tokio::test]
async fn import_event_is_rejected_by_gates_same_as_append() {
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
    let err = store
        .import_event(
            &s,
            "denied",
            patch(json!([{ "op": "add", "path": "", "value": {} }])),
            json!({}),
            Timestamp(1_700_000_000_000),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Schema(_)));

    // The rejected write never reached the backend.
    assert_eq!(events.head(&s).await.unwrap(), None);
}

#[tokio::test]
async fn import_event_dispatches_to_sinks_same_as_append() {
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
        .import_event(
            &s,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": {} }])),
            json!({}),
            Timestamp(1_700_000_000_000),
        )
        .await
        .unwrap();

    assert_eq!(
        sink.seen.lock().unwrap().clone(),
        vec![("doc".to_string(), 1)]
    );
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

// ---- label_delete --------------------------------------------------------

#[derive(Default)]
struct LabelDeleteRecordSink {
    id: String,
    deleted: StdMutex<Vec<String>>,
}

#[async_trait]
impl ProjectionSink for LabelDeleteRecordSink {
    fn id(&self) -> &str {
        &self.id
    }
    async fn commit(
        &self,
        _stream: &StreamId,
        _seq: Seq,
        _state: &Value,
        _event: &Event,
    ) -> Result<(), StoreError> {
        Ok(())
    }
    async fn on_label_deleted(&self, _stream: &StreamId, label: &Label) -> Result<(), StoreError> {
        self.deleted
            .lock()
            .unwrap()
            .push(label.as_str().to_string());
        Ok(())
    }
}

#[tokio::test]
async fn label_delete_removes_label_and_notifies_sinks() {
    let sink = Arc::new(LabelDeleteRecordSink {
        id: "ld".to_string(),
        deleted: StdMutex::new(Vec::new()),
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
        .label_set(&s, &Label::new("v1"), Seq(1))
        .await
        .unwrap();

    let existed = store.label_delete(&s, &Label::new("v1")).await.unwrap();
    assert!(existed);

    let err = store
        .label_resolve(&s, &Label::new("v1"))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::UnknownLabel(name) if name == "v1"));
    assert_eq!(sink.deleted.lock().unwrap().clone(), vec!["v1".to_string()]);
}

#[tokio::test]
async fn label_delete_of_unknown_label_is_idempotent_and_returns_false() {
    let sink = Arc::new(LabelDeleteRecordSink {
        id: "ld".to_string(),
        deleted: StdMutex::new(Vec::new()),
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

    let existed = store.label_delete(&s, &Label::new("nope")).await.unwrap();
    assert!(!existed);
    // No sink notification for a no-op delete — nothing changed.
    assert!(sink.deleted.lock().unwrap().is_empty());
}

#[tokio::test]
async fn label_delete_leaves_event_log_unchanged() {
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
    store
        .label_set(&s, &Label::new("v1"), Seq(1))
        .await
        .unwrap();

    let head_before = store.head(&s).await.unwrap();
    let events_before = store.read(&s, Seq::ZERO.next(), 10).await.unwrap();

    let existed = store.label_delete(&s, &Label::new("v1")).await.unwrap();
    assert!(existed);

    let head_after = store.head(&s).await.unwrap();
    let events_after = store.read(&s, Seq::ZERO.next(), 10).await.unwrap();

    assert_eq!(head_before, head_after);
    assert_eq!(events_before, events_after);
}

// ---- materialize_to_sink -------------------------------------------------

#[tokio::test]
async fn materialize_to_sink_dumps_head_without_advancing_checkpoint() {
    let events = Arc::new(MemEventBackend::new());
    let cache = Arc::new(MemCacheBackend::new());

    // Build history with no sink attached.
    let store_no_sink = Store::new(
        events.clone(),
        cache.clone(),
        Vec::new(),
        Vec::new(),
        StoreConfig::default(),
    );
    let s = StreamId::new("doc");
    store_no_sink
        .append(
            &s,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": { "n": 1 } }])),
            json!({}),
        )
        .await
        .unwrap();
    store_no_sink
        .append(
            &s,
            "bump",
            patch(json!([{ "op": "replace", "path": "/n", "value": 2 }])),
            json!({}),
        )
        .await
        .unwrap();

    // Attach a sink and dump the current head imperatively.
    let sink = Arc::new(RecordSink {
        id: "record".to_string(),
        seen: StdMutex::new(Vec::new()),
    });
    let store_with_sink = Store::new(
        events,
        cache,
        Vec::new(),
        vec![sink.clone()],
        StoreConfig::default(),
    );

    let dumped = store_with_sink
        .materialize_to_sink(&s, "record", None)
        .await
        .unwrap();
    assert_eq!(dumped, Seq(2));
    assert_eq!(
        sink.seen.lock().unwrap().clone(),
        vec![("doc".to_string(), 2)]
    );

    // The checkpoint was not advanced by materialize_to_sink — catch_up
    // still has to drive both events from scratch.
    let report = store_with_sink.catch_up("record").await.unwrap();
    assert_eq!(report.applied, 2);
    assert_eq!(report.failed, 0);
}

#[tokio::test]
async fn materialize_to_sink_with_at_dumps_a_past_state() {
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

    // append() already dispatched twice; isolate the effect of the explicit
    // historical dump.
    sink.seen.lock().unwrap().clear();

    let dumped = store
        .materialize_to_sink(&s, "record", Some(Seq(1)))
        .await
        .unwrap();
    assert_eq!(dumped, Seq(1));
    assert_eq!(
        sink.seen.lock().unwrap().clone(),
        vec![("doc".to_string(), 1)]
    );

    // Head is still 2 — dumping a past seq did not touch the log or move
    // the stream forward/backward.
    assert_eq!(store.head(&s).await.unwrap(), Some(Seq(2)));
}

#[tokio::test]
async fn materialize_to_sink_with_at_beyond_head_returns_seq_out_of_range() {
    let sink = Arc::new(RecordSink {
        id: "record".to_string(),
        seen: StdMutex::new(Vec::new()),
    });
    let store = Store::new(
        Arc::new(MemEventBackend::new()),
        Arc::new(MemCacheBackend::new()),
        Vec::new(),
        vec![sink],
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

    let err = store
        .materialize_to_sink(&s, "record", Some(Seq(5)))
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        StoreError::SeqOutOfRange {
            head: Some(Seq(1)),
            requested: Seq(5)
        }
    ));
}

#[tokio::test]
async fn materialize_to_sink_unknown_sink_returns_error() {
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
        .materialize_to_sink(&s, "nope", None)
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::UnknownSink(id) if id == "nope"));
}

#[tokio::test]
async fn materialize_to_sink_unknown_stream_returns_error() {
    let sink = Arc::new(RecordSink {
        id: "record".to_string(),
        seen: StdMutex::new(Vec::new()),
    });
    let store = Store::new(
        Arc::new(MemEventBackend::new()),
        Arc::new(MemCacheBackend::new()),
        Vec::new(),
        vec![sink],
        StoreConfig::default(),
    );

    let err = store
        .materialize_to_sink(&StreamId::new("nope"), "record", None)
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::UnknownStream(id) if id == StreamId::new("nope")));
}

// ---- with_checkpoint_backend --------------------------------------------

#[tokio::test]
async fn with_checkpoint_backend_survives_a_fresh_store_instance() {
    let events = Arc::new(MemEventBackend::new());
    let cache = Arc::new(MemCacheBackend::new());
    let checkpoint_backend: Arc<dyn CheckpointBackend> = Arc::new(MemCheckpointBackend::new());
    let sink = Arc::new(RecordSink {
        id: "record".to_string(),
        seen: StdMutex::new(Vec::new()),
    });
    let s = StreamId::new("doc");

    let store = Store::with_checkpoint_backend(
        events.clone(),
        cache.clone(),
        Vec::new(),
        vec![sink.clone()],
        StoreConfig::default(),
        checkpoint_backend.clone(),
    );
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
    assert_eq!(
        sink.seen.lock().unwrap().clone(),
        vec![("doc".to_string(), 1), ("doc".to_string(), 2)]
    );

    // The persisted checkpoint backend actually recorded the advance.
    assert_eq!(
        checkpoint_backend.get("record", &s).await.unwrap(),
        Some(Seq(2))
    );

    // A brand-new `Store` sharing only the backends (fresh in-memory
    // checkpoint map, as a process restart would produce) restores the
    // checkpoint from `checkpoint_backend` instead of re-driving from
    // `Seq(0)`.
    let sink2 = Arc::new(RecordSink {
        id: "record".to_string(),
        seen: StdMutex::new(Vec::new()),
    });
    let restarted = Store::with_checkpoint_backend(
        events,
        cache,
        Vec::new(),
        vec![sink2.clone()],
        StoreConfig::default(),
        checkpoint_backend,
    );
    let report = restarted.catch_up("record").await.unwrap();
    assert_eq!(report, CatchUpReport::EMPTY);
    assert!(sink2.seen.lock().unwrap().is_empty());
}

// ---- catch_up failure isolation -----------------------------------------

/// A `ProjectionSink` that always fails `commit` for one specific stream and
/// records every other stream's committed `(stream, seq)` pairs.
struct StreamFailSink {
    id: String,
    fail_stream: StreamId,
    seen: StdMutex<Vec<(String, u64)>>,
}

#[async_trait]
impl ProjectionSink for StreamFailSink {
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
        if stream == &self.fail_stream {
            return Err(StoreError::Backend("stream fail".to_string()));
        }
        self.seen
            .lock()
            .unwrap()
            .push((stream.as_str().to_string(), seq.0));
        Ok(())
    }
}

#[tokio::test]
async fn catch_up_isolates_failure_to_one_stream_and_continues_others() {
    let events = Arc::new(MemEventBackend::new());
    let cache = Arc::new(MemCacheBackend::new());
    let good = StreamId::new("good");
    let bad = StreamId::new("bad");

    // Build history on both streams with no sink attached, so neither has
    // any prior dispatch — a single `catch_up` call below has to drive both
    // from scratch.
    let store_no_sink = Store::new(
        events.clone(),
        cache.clone(),
        Vec::new(),
        Vec::new(),
        StoreConfig::default(),
    );
    for stream in [&good, &bad] {
        // Seq 1: establish an empty object root so the subsequent adds have
        // a path parent. Seq 2, 3: two more events, for 3 total per stream.
        store_no_sink
            .append(
                stream,
                "init",
                patch(json!([{ "op": "add", "path": "", "value": {} }])),
                json!({}),
            )
            .await
            .unwrap();
        for i in 0..2 {
            store_no_sink
                .append(
                    stream,
                    "k",
                    patch(json!([{ "op": "add", "path": format!("/n{i}"), "value": i }])),
                    json!({}),
                )
                .await
                .unwrap();
        }
    }

    let sink = Arc::new(StreamFailSink {
        id: "faulty".to_string(),
        fail_stream: bad.clone(),
        seen: StdMutex::new(Vec::new()),
    });
    let store = Store::new(
        events,
        cache,
        Vec::new(),
        vec![sink.clone()],
        StoreConfig::default(),
    );

    let report = store.catch_up("faulty").await.unwrap();

    // "good" drives to completion: all 3 events applied.
    assert_eq!(
        sink.seen.lock().unwrap().clone(),
        vec![
            ("good".to_string(), 1),
            ("good".to_string(), 2),
            ("good".to_string(), 3),
        ]
    );
    assert_eq!(report.applied, 3);

    // "bad" fails at its very first undrivable event (seq 1); the remaining
    // two (seq 2, seq 3) are counted as skipped rather than silently lost or
    // applied out of order.
    assert_eq!(report.failed, 1);
    assert_eq!(report.skipped, 2);
    assert_eq!(report.failures.len(), 1);
    assert_eq!(report.failures[0].stream, bad);
    assert_eq!(report.failures[0].sink_id, "faulty");
}

// ---- concurrent per-stream write serialization --------------------------

/// A `SchemaGate` that records the size of `ctx.current` (as a JSON object)
/// on every validation call. Used to detect a read-validate-write race: if
/// two concurrent `append`s to the same stream both observe the same
/// `current`, the same size is recorded twice.
struct SizeGate {
    seen: StdMutex<Vec<usize>>,
}

impl SchemaGate for SizeGate {
    fn validate(&self, ctx: &GateCtx<'_>) -> Result<(), SchemaViolation> {
        let size = ctx.current.as_object().map(|o| o.len()).unwrap_or(0);
        self.seen.lock().unwrap().push(size);
        Ok(())
    }
}

#[tokio::test]
async fn concurrent_append_to_same_stream_serializes_gate_observations() {
    let gate = Arc::new(SizeGate {
        seen: StdMutex::new(Vec::new()),
    });
    let gate_dyn: Arc<dyn SchemaGate> = gate.clone();
    let store = Arc::new(Store::new(
        Arc::new(MemEventBackend::new()),
        Arc::new(MemCacheBackend::new()),
        vec![gate_dyn],
        Vec::new(),
        StoreConfig::default(),
    ));
    let s = StreamId::new("doc");

    // Establish an empty object root so every subsequent add has a path
    // parent. Clear the gate's log afterward so only the concurrent appends
    // below are counted.
    store
        .append(
            &s,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": {} }])),
            json!({}),
        )
        .await
        .unwrap();
    gate.seen.lock().unwrap().clear();

    const N: usize = 50;
    let mut handles = Vec::with_capacity(N);
    for i in 0..N {
        let store = store.clone();
        let s = s.clone();
        handles.push(tokio::spawn(async move {
            store
                .append(
                    &s,
                    "bump",
                    patch(json!([{ "op": "add", "path": format!("/task_{i}"), "value": i }])),
                    json!({}),
                )
                .await
                .unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    // Each of the N concurrent appends adds exactly one new top-level key.
    // If `Store::append` correctly serializes the state-read -> gate ->
    // backend-append critical section per stream, the gate must observe
    // every size in 0..N exactly once. A duplicate would mean two appends
    // both read the same pre-write `current` — the TOCTOU this fix closes.
    let mut seen = gate.seen.lock().unwrap().clone();
    seen.sort_unstable();
    let expected: Vec<usize> = (0..N).collect();
    assert_eq!(seen, expected);

    let final_state = store.state(&s).await.unwrap();
    assert_eq!(final_state.as_object().unwrap().len(), N);
}

// ---- ProjectionSink::accepts --------------------------------------------

/// A sink that only accepts one specific stream, recording every dispatch it
/// actually received.
struct SelectiveSink {
    id: String,
    accepted_stream: StreamId,
    commits: StdMutex<Vec<(String, u64)>>,
    label_sets: StdMutex<Vec<(String, String)>>,
    label_deletes: StdMutex<Vec<(String, String)>>,
}

#[async_trait]
impl ProjectionSink for SelectiveSink {
    fn id(&self) -> &str {
        &self.id
    }
    fn accepts(&self, stream: &StreamId) -> bool {
        stream == &self.accepted_stream
    }
    async fn commit(
        &self,
        stream: &StreamId,
        seq: Seq,
        _state: &Value,
        _event: &Event,
    ) -> Result<(), StoreError> {
        self.commits
            .lock()
            .unwrap()
            .push((stream.as_str().to_string(), seq.0));
        Ok(())
    }
    async fn on_label_set(
        &self,
        stream: &StreamId,
        label: &Label,
        _at: Seq,
        _state: &Value,
        _event: &Event,
    ) -> Result<(), StoreError> {
        self.label_sets
            .lock()
            .unwrap()
            .push((stream.as_str().to_string(), label.as_str().to_string()));
        Ok(())
    }
    async fn on_label_deleted(&self, stream: &StreamId, label: &Label) -> Result<(), StoreError> {
        self.label_deletes
            .lock()
            .unwrap()
            .push((stream.as_str().to_string(), label.as_str().to_string()));
        Ok(())
    }
}

fn selective_sink(accepted: &StreamId) -> Arc<SelectiveSink> {
    Arc::new(SelectiveSink {
        id: "selective".to_string(),
        accepted_stream: accepted.clone(),
        commits: StdMutex::new(Vec::new()),
        label_sets: StdMutex::new(Vec::new()),
        label_deletes: StdMutex::new(Vec::new()),
    })
}

#[tokio::test]
async fn accepts_filters_append_dispatch_and_catch_up_to_one_stream() {
    let accepted = StreamId::new("accepted");
    let other = StreamId::new("other");
    let sink = selective_sink(&accepted);
    let store = Store::new(
        Arc::new(MemEventBackend::new()),
        Arc::new(MemCacheBackend::new()),
        Vec::new(),
        vec![sink.clone()],
        StoreConfig::default(),
    );

    store
        .append(
            &accepted,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": {} }])),
            json!({}),
        )
        .await
        .unwrap();
    store
        .append(
            &other,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": {} }])),
            json!({}),
        )
        .await
        .unwrap();

    // Only the accepted stream's append was dispatched to the sink.
    assert_eq!(
        sink.commits.lock().unwrap().clone(),
        vec![("accepted".to_string(), 1)]
    );

    // rebuild (reset + catch_up) never touches "other" at all: it is not
    // applied, and it does not inflate `skipped`/`failed` either — the
    // sink was never on the hook for that stream to begin with.
    let report = store.rebuild("selective").await.unwrap();
    assert_eq!(report.applied, 1);
    assert_eq!(report.skipped, 0);
    assert_eq!(report.failed, 0);
    assert_eq!(
        sink.commits.lock().unwrap().clone(),
        vec![("accepted".to_string(), 1), ("accepted".to_string(), 1)]
    );
}

#[tokio::test]
async fn accepts_filters_label_set_and_label_delete_notifications() {
    let accepted = StreamId::new("accepted");
    let other = StreamId::new("other");
    let sink = selective_sink(&accepted);
    let store = Store::new(
        Arc::new(MemEventBackend::new()),
        Arc::new(MemCacheBackend::new()),
        Vec::new(),
        vec![sink.clone()],
        StoreConfig::default(),
    );

    for s in [&accepted, &other] {
        store
            .append(
                s,
                "init",
                patch(json!([{ "op": "add", "path": "", "value": {} }])),
                json!({}),
            )
            .await
            .unwrap();
    }
    sink.commits.lock().unwrap().clear();

    store
        .label_set(&accepted, &Label::new("v1"), Seq(1))
        .await
        .unwrap();
    store
        .label_set(&other, &Label::new("v1"), Seq(1))
        .await
        .unwrap();
    assert_eq!(
        sink.label_sets.lock().unwrap().clone(),
        vec![("accepted".to_string(), "v1".to_string())]
    );

    store
        .label_delete(&accepted, &Label::new("v1"))
        .await
        .unwrap();
    store.label_delete(&other, &Label::new("v1")).await.unwrap();
    assert_eq!(
        sink.label_deletes.lock().unwrap().clone(),
        vec![("accepted".to_string(), "v1".to_string())]
    );
}

#[tokio::test]
async fn materialize_to_sink_bypasses_the_accepts_filter() {
    let accepted = StreamId::new("accepted");
    let other = StreamId::new("other");
    let sink = selective_sink(&accepted);
    let store = Store::new(
        Arc::new(MemEventBackend::new()),
        Arc::new(MemCacheBackend::new()),
        Vec::new(),
        vec![sink.clone()],
        StoreConfig::default(),
    );

    store
        .append(
            &other,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": { "n": 1 } }])),
            json!({}),
        )
        .await
        .unwrap();
    // The normal append dispatch above never reached the sink (it rejects
    // "other"); clear defensively so the assertion below is unambiguous.
    sink.commits.lock().unwrap().clear();

    // An explicit `materialize_to_sink` call still succeeds and dispatches,
    // even though `accepts("other")` is false — it is a caller-named,
    // one-shot request, not automatic dispatch.
    let dumped = store
        .materialize_to_sink(&other, "selective", None)
        .await
        .unwrap();
    assert_eq!(dumped, Seq(1));
    assert_eq!(
        sink.commits.lock().unwrap().clone(),
        vec![("other".to_string(), 1)]
    );
}

// ---- on_label_set carries the labeled event ------------------------------

/// Records the `(at, event.at, event.meta)` triple observed by every
/// `on_label_set` call.
#[derive(Default)]
struct LabelEventRecordSink {
    id: String,
    seen: StdMutex<Vec<(Seq, i64, Value)>>,
}

#[async_trait]
impl ProjectionSink for LabelEventRecordSink {
    fn id(&self) -> &str {
        &self.id
    }
    async fn commit(
        &self,
        _stream: &StreamId,
        _seq: Seq,
        _state: &Value,
        _event: &Event,
    ) -> Result<(), StoreError> {
        Ok(())
    }
    async fn on_label_set(
        &self,
        _stream: &StreamId,
        _label: &Label,
        at: Seq,
        _state: &Value,
        event: &Event,
    ) -> Result<(), StoreError> {
        self.seen
            .lock()
            .unwrap()
            .push((at, event.at.0, event.meta.clone()));
        Ok(())
    }
}

#[tokio::test]
async fn label_set_notification_carries_the_labeled_events_timestamp_and_meta() {
    let sink = Arc::new(LabelEventRecordSink {
        id: "le".to_string(),
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
    let at = Timestamp(1_700_000_000_000);

    store
        .import_event(
            &s,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": {} }])),
            json!({ "author": "alice" }),
            at,
        )
        .await
        .unwrap();

    store
        .label_set(&s, &Label::new("v1"), Seq(1))
        .await
        .unwrap();

    let seen = sink.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].0, Seq(1));
    assert_eq!(seen[0].1, at.0);
    assert_eq!(seen[0].2, json!({ "author": "alice" }));
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
