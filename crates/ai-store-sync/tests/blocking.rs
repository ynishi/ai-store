//! Integration tests for `BlockingStore`. Cover the async ↔ sync boundary
//! from both directions: (a) a plain synchronous caller using
//! `BlockingStore::new`, and (b) an existing tokio runtime handing a `Handle`
//! to `BlockingStore::with_handle`.

use std::sync::{Arc, Mutex as StdMutex};

use ai_store_core::{
    Event, Label, Patch, ProjectionSink, Seq, Store, StoreConfig, StoreError, StreamId, Timestamp,
};
use ai_store_mem::{MemCacheBackend, MemEventBackend};
use ai_store_sync::BlockingStore;
use async_trait::async_trait;
use serde_json::{json, Value};

fn patch(v: Value) -> Patch {
    serde_json::from_value::<Patch>(v).unwrap()
}

fn build_store() -> Store {
    Store::new(
        Arc::new(MemEventBackend::new()),
        Arc::new(MemCacheBackend::new()),
        Vec::new(),
        Vec::new(),
        StoreConfig::default(),
    )
}

/// Minimal `ProjectionSink` recording each `(stream, seq)` it was asked to
/// commit.
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

/// Owned-runtime path: no tokio in the caller.
#[test]
fn owned_runtime_append_state_read_roundtrip() {
    let bs = BlockingStore::new(build_store()).expect("runtime");
    let s = StreamId::new("doc");

    let seq1 = bs
        .append(
            &s,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": { "n": 0 } }])),
            json!({}),
        )
        .unwrap();
    assert_eq!(seq1.seq, Seq(1));

    let seq2 = bs
        .append(
            &s,
            "bump",
            patch(json!([{ "op": "replace", "path": "/n", "value": 1 }])),
            json!({}),
        )
        .unwrap();
    assert_eq!(seq2.seq, Seq(2));

    assert_eq!(bs.state(&s).unwrap(), json!({ "n": 1 }));
    assert_eq!(bs.head(&s).unwrap(), Some(Seq(2)));

    let events = bs.read(&s, Seq(1), 10).unwrap();
    assert_eq!(events.len(), 2);
}

/// Owned-runtime path: revert-as-commit still lands as a new event.
#[test]
fn owned_runtime_revert_appends_event() {
    let bs = BlockingStore::new(build_store()).expect("runtime");
    let s = StreamId::new("doc");

    bs.append(
        &s,
        "init",
        patch(json!([{ "op": "add", "path": "", "value": { "n": 0 } }])),
        json!({}),
    )
    .unwrap();
    bs.append(
        &s,
        "bump",
        patch(json!([{ "op": "replace", "path": "/n", "value": 9 }])),
        json!({}),
    )
    .unwrap();

    let seq_reverted = bs.revert(&s, Seq(1)).unwrap();
    assert_eq!(seq_reverted.seq, Seq(3));
    assert_eq!(bs.state(&s).unwrap(), json!({ "n": 0 }));
    assert_eq!(bs.head(&s).unwrap(), Some(Seq(3)));
}

/// `BlockingStore::import_event` mirrors `Store::import_event`: the backend
/// records the caller-supplied historical `Timestamp` verbatim rather than
/// stamping wall-clock time.
#[test]
fn import_event_preserves_caller_supplied_timestamp_via_blocking_facade() {
    let bs = BlockingStore::new(build_store()).expect("runtime");
    let s = StreamId::new("doc");
    let at = Timestamp(1_700_000_000_000);

    let committed = bs
        .import_event(
            &s,
            "legacy_create",
            patch(json!([{ "op": "add", "path": "", "value": { "n": 0 } }])),
            json!({}),
            at,
        )
        .unwrap();
    assert_eq!(committed.seq, Seq(1));
    assert_eq!(committed.at, at);

    let events = bs.read(&s, Seq(1), 1).unwrap();
    assert_eq!(events[0].at, at);
}

/// `Clone` shares the same runtime + inner store: writes on one handle are
/// visible to another.
#[test]
fn clone_shares_state() {
    let bs = BlockingStore::new(build_store()).expect("runtime");
    let bs2 = bs.clone();
    let s = StreamId::new("doc");

    bs.append(
        &s,
        "init",
        patch(json!([{ "op": "add", "path": "", "value": { "hello": "world" } }])),
        json!({}),
    )
    .unwrap();

    assert_eq!(bs2.state(&s).unwrap(), json!({ "hello": "world" }));
}

/// Borrowed-handle path: caller drives its own runtime and hands a `Handle`
/// to the blocking facade. The blocking facade is invoked from
/// `tokio::task::spawn_blocking` so we are not in an async context when we
/// call `block_on` — this is the canonical bridge pattern.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn borrowed_handle_from_spawn_blocking() {
    let store = build_store();
    let handle = tokio::runtime::Handle::current();
    let bs = BlockingStore::with_handle(store, handle);

    let bs2 = bs.clone();
    tokio::task::spawn_blocking(move || {
        let s = StreamId::new("doc");
        bs2.append(
            &s,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": { "k": 1 } }])),
            json!({}),
        )
        .unwrap();
    })
    .await
    .unwrap();

    let s = StreamId::new("doc");
    assert_eq!(bs.as_async().state(&s).await.unwrap(), json!({ "k": 1 }));
}

/// `BlockingStore::label_delete` mirrors `Store::label_delete`: it removes
/// the label and a subsequent `label_resolve` surfaces `UnknownLabel`, while
/// the delete itself is idempotent (`Ok(bool)`, never an error).
#[test]
fn label_delete_removes_label_via_blocking_facade() {
    let bs = BlockingStore::new(build_store()).expect("runtime");
    let s = StreamId::new("doc");

    bs.append(
        &s,
        "init",
        patch(json!([{ "op": "add", "path": "", "value": {} }])),
        json!({}),
    )
    .unwrap();
    bs.label_set(&s, &Label::new("v1"), Seq(1)).unwrap();

    let existed = bs.label_delete(&s, &Label::new("v1")).unwrap();
    assert!(existed);

    let err = bs.label_resolve(&s, &Label::new("v1")).unwrap_err();
    assert!(matches!(err, StoreError::UnknownLabel(name) if name == "v1"));

    // Deleting an already-gone label is idempotent: no error, just `false`.
    let existed = bs.label_delete(&s, &Label::new("v1")).unwrap();
    assert!(!existed);
}

/// `BlockingStore::materialize_to_sink` mirrors `Store::materialize_to_sink`:
/// it dumps the current head to the named sink and returns that `Seq`.
#[test]
fn materialize_to_sink_dumps_head_via_blocking_facade() {
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
    let bs = BlockingStore::new(store).expect("runtime");
    let s = StreamId::new("doc");

    bs.append(
        &s,
        "init",
        patch(json!([{ "op": "add", "path": "", "value": { "n": 1 } }])),
        json!({}),
    )
    .unwrap();

    // append() already dispatched once; clear it so we can isolate the
    // effect of materialize_to_sink.
    sink.seen.lock().unwrap().clear();

    let dumped = bs.materialize_to_sink(&s, "record", None).unwrap();
    assert_eq!(dumped, Seq(1));
    assert_eq!(
        sink.seen.lock().unwrap().clone(),
        vec![("doc".to_string(), 1)]
    );

    let err = bs.materialize_to_sink(&s, "nope", None).unwrap_err();
    assert!(matches!(err, StoreError::UnknownSink(id) if id == "nope"));
}
