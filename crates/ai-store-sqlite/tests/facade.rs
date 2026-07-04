//! Facade-level tests running against `SqliteBackends`.
//!
//! These check that `Store` composed with `SqliteEventBackend` +
//! `SqliteCacheBackend` upholds the same public contract as the in-memory
//! backend, and that durability across reopen actually works (the property
//! the memory backend cannot exercise).

use std::sync::{Arc, Mutex as StdMutex};

use ai_store_core::{
    Event, Label, Patch, ProjectionSink, Seq, Store, StoreConfig, StoreError, StreamId,
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
/// `examples/migrate_from_json.rs`: chain-checked import from `(before,
/// after)` snapshots reconstructs `Store::state` exactly.
#[tokio::test]
async fn migrate_from_json_recipe_reconstructs_final_state() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = open_facade(&be).await;
    let s = StreamId::new("doc/legacy");

    let legacy: Vec<(&str, Value, Value)> = vec![
        ("create", Value::Null, json!({ "title": "draft", "n": 0 })),
        (
            "rename",
            json!({ "title": "draft", "n": 0 }),
            json!({ "title": "final", "n": 0 }),
        ),
        (
            "bump",
            json!({ "title": "final", "n": 0 }),
            json!({ "title": "final", "n": 3 }),
        ),
    ];

    let mut prev = Value::Null;
    for (i, (kind, before, after)) in legacy.iter().enumerate() {
        assert_eq!(before, &prev, "chain broken at index {i}");
        let p: Patch = json_patch::diff(before, after);
        store
            .append(&s, kind, p, json!({ "legacy_index": i }))
            .await
            .unwrap();
        prev = after.clone();
    }

    let expected = legacy.last().unwrap().2.clone();
    assert_eq!(store.state(&s).await.unwrap(), expected);
    assert_eq!(
        store.head(&s).await.unwrap(),
        Some(Seq(legacy.len() as u64))
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

    store.label_delete(&s, &Label::new("v1")).await.unwrap();

    let err = store
        .label_resolve(&s, &Label::new("v1"))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::UnknownLabel(name) if name == "v1"));

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn label_delete_of_unknown_label_returns_unknown_label_error() {
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
        .label_delete(&s, &Label::new("nope"))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::UnknownLabel(name) if name == "nope"));

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
        .materialize_to_sink("record", &s)
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

    let err = store.materialize_to_sink("nope", &s).await.unwrap_err();
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
        .materialize_to_sink("record", &StreamId::new("nope"))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::UnknownStream(id) if id == StreamId::new("nope")));

    be.driver.shutdown().await.unwrap();
}
