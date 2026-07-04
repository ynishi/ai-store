//! Facade-level tests running against `SqliteBackends`.
//!
//! These check that `Store` composed with `SqliteEventBackend` +
//! `SqliteCacheBackend` upholds the same public contract as the in-memory
//! backend, and that durability across reopen actually works (the property
//! the memory backend cannot exercise).

use std::sync::{Arc, Mutex as StdMutex};

use ai_store_core::{
    Event, Label, Patch, ProjectionSink, Seq, Store, StoreConfig, StoreError, StreamId, Timestamp,
};
use ai_store_sqlite::SqliteBackends;
use async_trait::async_trait;
use serde_json::{json, Value};
use tempfile::TempDir;

fn patch(v: Value) -> Patch {
    serde_json::from_value::<Patch>(v).unwrap()
}

async fn open_facade(be: &SqliteBackends) -> Store {
    Store::new(
        Arc::new(be.events.clone()),
        Arc::new(be.cache.clone()),
        Vec::new(),
        Vec::new(),
        StoreConfig::default(),
    )
}

async fn open_facade_with_sinks(be: &SqliteBackends, sinks: Vec<Arc<dyn ProjectionSink>>) -> Store {
    Store::new(
        Arc::new(be.events.clone()),
        Arc::new(be.cache.clone()),
        Vec::new(),
        sinks,
        StoreConfig::default(),
    )
}

/// Minimal `ProjectionSink` recording each `(stream, seq)` it was asked to
/// commit, for asserting dispatch counts and ordering.
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
async fn append_and_state_reconstruct_end_to_end() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = open_facade(&be).await;
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
    store
        .append(
            &s,
            "bump",
            patch(json!([{ "op": "replace", "path": "/n", "value": 7 }])),
            json!({}),
        )
        .await
        .unwrap();

    assert_eq!(store.state(&s).await.unwrap(), json!({ "n": 7 }));
    assert_eq!(store.state_at(&s, Seq(1)).await.unwrap(), json!({ "n": 0 }));

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn revert_participates_in_the_log() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = open_facade(&be).await;
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

    let revert_seq = store.revert(&s, Seq(1)).await.unwrap();
    assert_eq!(revert_seq, Seq(3));
    assert_eq!(store.state(&s).await.unwrap(), json!({ "n": 1 }));
    assert_eq!(store.state_at(&s, Seq(2)).await.unwrap(), json!({ "n": 2 }));

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn state_persists_across_reopen() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store.db");

    // First open: write two events + one label + shut down.
    {
        let be = SqliteBackends::open(&path).await.unwrap();
        let store = open_facade(&be).await;
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
        store
            .append(
                &s,
                "bump",
                patch(json!([{ "op": "replace", "path": "/n", "value": 42 }])),
                json!({}),
            )
            .await
            .unwrap();
        store
            .label_set(&s, &ai_store_core::Label::new("v1"), Seq(2))
            .await
            .unwrap();
        be.driver.shutdown().await.unwrap();
    }

    // Second open: state, head, labels all come back intact.
    {
        let be = SqliteBackends::open(&path).await.unwrap();
        let store = open_facade(&be).await;
        let s = StreamId::new("doc");
        assert_eq!(store.head(&s).await.unwrap(), Some(Seq(2)));
        assert_eq!(store.state(&s).await.unwrap(), json!({ "n": 42 }));
        assert_eq!(
            store
                .label_resolve(&s, &ai_store_core::Label::new("v1"))
                .await
                .unwrap(),
            Seq(2)
        );
        be.driver.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn read_by_meta_uses_indexed_lookup() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = open_facade(&be).await;
    let s = StreamId::new("doc");

    // Init empty object so subsequent path adds have a parent.
    store
        .append(
            &s,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": {} }])),
            json!({ "entity_id": "init" }),
        )
        .await
        .unwrap();

    for (i, (ent, count)) in [("A", 10), ("B", 20), ("A", 30)].iter().enumerate() {
        store
            .append(
                &s,
                "touch",
                patch(json!([{ "op": "add", "path": format!("/step_{}", i), "value": count }])),
                json!({ "entity_id": ent, "n": count }),
            )
            .await
            .unwrap();
    }

    // String-typed value → matches seq 2 and 4 (init is seq 1).
    let hits = store
        .read_by_meta(&s, "entity_id", &json!("A"), Seq(2), 10)
        .await
        .unwrap();
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].seq, Seq(2));
    assert_eq!(hits[1].seq, Seq(4));

    // Number-typed value (json_extract normalizes both sides via canonical JSON)
    let by_n = store
        .read_by_meta(&s, "n", &json!(20), Seq(1), 10)
        .await
        .unwrap();
    assert_eq!(by_n.len(), 1);
    assert_eq!(by_n[0].seq, Seq(3));

    // Non-match
    let none = store
        .read_by_meta(&s, "entity_id", &json!("Z"), Seq(1), 10)
        .await
        .unwrap();
    assert!(none.is_empty());

    // Missing field never matches (SQL NULL not equal to any bound value)
    let missing = store
        .read_by_meta(&s, "no_such_key", &json!("A"), Seq(1), 10)
        .await
        .unwrap();
    assert!(missing.is_empty());

    // `from` gating
    let from_mid = store
        .read_by_meta(&s, "entity_id", &json!("A"), Seq(3), 10)
        .await
        .unwrap();
    assert_eq!(from_mid.len(), 1);
    assert_eq!(from_mid[0].seq, Seq(4));

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn read_by_meta_matches_json_null_via_json_type() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = open_facade(&be).await;
    let s = StreamId::new("doc");

    store
        .append(
            &s,
            "init_null",
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
    store
        .append(
            &s,
            "missing_owner",
            patch(json!([{ "op": "add", "path": "/y", "value": 2 }])),
            json!({ "other": "irrelevant" }),
        )
        .await
        .unwrap();

    // Value::Null matches JSON null but NOT missing field.
    let hits = store
        .read_by_meta(&s, "owner", &Value::Null, Seq(1), 10)
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].seq, Seq(1));

    be.driver.shutdown().await.unwrap();
}

/// Smoke-test the migration recipe encoded in
/// `examples/migrate_from_json.rs`: chain-checked import via
/// `Store::import_event` from `(before, after)` snapshots reconstructs
/// `Store::state` exactly, and `seq_at_time` answers against the imported
/// (source-system) timestamps rather than wall-clock import time.
#[tokio::test]
async fn migrate_from_json_recipe_reconstructs_final_state() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = open_facade(&be).await;
    let s = StreamId::new("doc/legacy");

    let legacy: Vec<(&str, i64, Value, Value)> = vec![
        (
            "create",
            1_700_000_000_000,
            Value::Null,
            json!({ "title": "draft", "n": 0 }),
        ),
        (
            "rename",
            1_700_000_060_000,
            json!({ "title": "draft", "n": 0 }),
            json!({ "title": "final", "n": 0 }),
        ),
        (
            "bump",
            1_700_000_120_000,
            json!({ "title": "final", "n": 0 }),
            json!({ "title": "final", "n": 3 }),
        ),
    ];

    let mut prev = Value::Null;
    for (i, (kind, at_ms, before, after)) in legacy.iter().enumerate() {
        assert_eq!(before, &prev, "chain broken at index {i}");
        let p: Patch = json_patch::diff(before, after);
        store
            .import_event(&s, kind, p, json!({ "legacy_index": i }), Timestamp(*at_ms))
            .await
            .unwrap();
        prev = after.clone();
    }

    let expected = legacy.last().unwrap().3.clone();
    assert_eq!(store.state(&s).await.unwrap(), expected);
    assert_eq!(
        store.head(&s).await.unwrap(),
        Some(Seq(legacy.len() as u64))
    );

    // `seq_at_time` answers against the source system's timeline, since
    // every event's `at` was imported verbatim rather than stamped at
    // migration time (mirrors Assertion 4 in the example).
    let at_second_entry = Timestamp(legacy[1].1);
    assert_eq!(
        store.seq_at_time(&s, at_second_entry).await.unwrap(),
        Some(Seq(2))
    );

    be.driver.shutdown().await.unwrap();
}

// ---- import_event ---------------------------------------------------------

#[tokio::test]
async fn import_event_preserves_caller_supplied_timestamp() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = open_facade(&be).await;
    let s = StreamId::new("doc");
    let at = Timestamp(1_700_000_000_000);

    let seq = store
        .import_event(
            &s,
            "legacy_create",
            patch(json!([{ "op": "add", "path": "", "value": { "n": 0 } }])),
            json!({}),
            at,
        )
        .await
        .unwrap();
    assert_eq!(seq, Seq(1));

    let events = store.read(&s, Seq(1), 1).await.unwrap();
    assert_eq!(events[0].at, at);

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn import_event_into_empty_stream_supports_historical_seq_at_time() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = open_facade(&be).await;
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

    let seq = store
        .seq_at_time(&s, Timestamp(1_700_000_030_000))
        .await
        .unwrap();
    assert_eq!(seq, Some(Seq(1)));

    let seq2 = store
        .seq_at_time(&s, Timestamp(1_700_000_060_000))
        .await
        .unwrap();
    assert_eq!(seq2, Some(Seq(2)));

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn import_event_is_rejected_by_gates_same_as_append() {
    use ai_store_core::{GateCtx, SchemaGate, SchemaViolation};

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

    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = Store::new(
        Arc::new(be.events.clone()),
        Arc::new(be.cache.clone()),
        vec![Arc::new(RejectKind {
            forbidden: "denied",
        })],
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
    assert_eq!(store.head(&s).await.unwrap(), None);

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn import_event_dispatches_to_sinks_same_as_append() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let sink = Arc::new(RecordSink {
        id: "record".to_string(),
        seen: StdMutex::new(Vec::new()),
    });
    let store = open_facade_with_sinks(&be, vec![sink.clone()]).await;
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

    be.driver.shutdown().await.unwrap();
}

// ---- label_delete ---------------------------------------------------------

#[tokio::test]
async fn label_delete_removes_label_and_surfaces_unknown_label_after() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = open_facade(&be).await;
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

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn label_delete_of_unknown_label_is_idempotent_and_returns_false() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = open_facade(&be).await;
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

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn label_delete_leaves_event_log_unchanged() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = open_facade(&be).await;
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

    be.driver.shutdown().await.unwrap();
}

// ---- materialize_to_sink ---------------------------------------------------

#[tokio::test]
async fn materialize_to_sink_dumps_head_without_advancing_checkpoint() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store_no_sink = open_facade(&be).await;
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

    let sink = Arc::new(RecordSink {
        id: "record".to_string(),
        seen: StdMutex::new(Vec::new()),
    });
    let store_with_sink = open_facade_with_sinks(&be, vec![sink.clone()]).await;

    let dumped = store_with_sink
        .materialize_to_sink(&s, "record", None)
        .await
        .unwrap();
    assert_eq!(dumped, Seq(2));
    assert_eq!(
        sink.seen.lock().unwrap().clone(),
        vec![("doc".to_string(), 2)]
    );

    // Checkpoint untouched — catch_up still drives from scratch.
    let report = store_with_sink.catch_up("record").await.unwrap();
    assert_eq!(report.applied, 2);
    assert_eq!(report.failed, 0);

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn materialize_to_sink_with_at_dumps_a_past_state() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let s = StreamId::new("doc");

    let sink = Arc::new(RecordSink {
        id: "record".to_string(),
        seen: StdMutex::new(Vec::new()),
    });
    let store = open_facade_with_sinks(&be, vec![sink.clone()]).await;

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

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn materialize_to_sink_with_at_beyond_head_returns_seq_out_of_range() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let s = StreamId::new("doc");
    let sink = Arc::new(RecordSink {
        id: "record".to_string(),
        seen: StdMutex::new(Vec::new()),
    });
    let store = open_facade_with_sinks(&be, vec![sink]).await;

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

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn materialize_to_sink_unknown_sink_returns_error() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = open_facade(&be).await;
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

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn materialize_to_sink_unknown_stream_returns_error() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let sink = Arc::new(RecordSink {
        id: "record".to_string(),
        seen: StdMutex::new(Vec::new()),
    });
    let store = open_facade_with_sinks(&be, vec![sink]).await;

    let err = store
        .materialize_to_sink(&StreamId::new("nope"), "record", None)
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::UnknownStream(id) if id == StreamId::new("nope")));

    be.driver.shutdown().await.unwrap();
}
