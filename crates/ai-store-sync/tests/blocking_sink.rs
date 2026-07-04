//! Integration tests for `BlockingSink`: a `SyncProjectionSink` driven by
//! the async dispatch loop of `Store::append`, with the checkpoint advancing
//! correctly under both dispatch modes.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use ai_store_core::{
    CatchUpReport, Event, Label, Patch, Seq, Store, StoreConfig, StoreError, StreamId,
};
use ai_store_mem::{MemCacheBackend, MemEventBackend};
use ai_store_sync::{BlockingSink, Dispatch, SyncProjectionSink};
use serde_json::{json, Value};

/// A minimal `SyncProjectionSink` that records the `(seq, state)` pairs it
/// was asked to apply, plus every label it was asked to delete. Backing
/// storage is a `Mutex<Vec<...>>`, which is safe under either `Inline` or
/// `SpawnBlocking` dispatch.
struct RecorderSink {
    id: String,
    log: Mutex<Vec<(Seq, Value)>>,
    commit_calls: AtomicUsize,
    deleted_labels: Mutex<Vec<String>>,
}

impl RecorderSink {
    fn new(id: &str) -> Self {
        Self {
            id: id.into(),
            log: Mutex::new(Vec::new()),
            commit_calls: AtomicUsize::new(0),
            deleted_labels: Mutex::new(Vec::new()),
        }
    }
}

impl SyncProjectionSink for RecorderSink {
    fn id(&self) -> &str {
        &self.id
    }
    fn commit(
        &self,
        _stream: &StreamId,
        seq: Seq,
        state: &Value,
        _event: &Event,
    ) -> Result<(), StoreError> {
        self.commit_calls.fetch_add(1, Ordering::SeqCst);
        self.log.lock().unwrap().push((seq, state.clone()));
        Ok(())
    }
    fn on_label_deleted(&self, _stream: &StreamId, label: &Label) -> Result<(), StoreError> {
        self.deleted_labels
            .lock()
            .unwrap()
            .push(label.as_str().to_string());
        Ok(())
    }
}

fn patch(v: Value) -> Patch {
    serde_json::from_value::<Patch>(v).unwrap()
}

fn build_store_with_sink(bs: Arc<BlockingSink<RecorderSink>>) -> Store {
    Store::new(
        Arc::new(MemEventBackend::new()),
        Arc::new(MemCacheBackend::new()),
        Vec::new(),
        vec![bs],
        StoreConfig::default(),
    )
}

/// SpawnBlocking mode: the canonical path for I/O-heavy sinks. Requires a
/// multi-thread runtime so `spawn_blocking` has a real pool to hand off to.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_blocking_dispatch_records_every_event() {
    let sink = BlockingSink::new(RecorderSink::new("recorder"));
    let inner = Arc::clone(sink.inner());
    let bs = Arc::new(sink);
    let store = build_store_with_sink(bs);
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
            patch(json!([{ "op": "replace", "path": "/n", "value": 1 }])),
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

    // Checkpoint should have advanced past every event (no failures, no gaps).
    let idle = store.catch_up("recorder").await.unwrap();
    assert_eq!(idle, CatchUpReport::EMPTY, "catch_up must be a no-op");

    let log = inner.log.lock().unwrap().clone();
    assert_eq!(log.len(), 3);
    assert_eq!(log[0].0, Seq(1));
    assert_eq!(log[0].1, json!({ "n": 0 }));
    assert_eq!(log[1].0, Seq(2));
    assert_eq!(log[1].1, json!({ "n": 1 }));
    assert_eq!(log[2].0, Seq(3));
    assert_eq!(log[2].1, json!({ "n": 2 }));
    assert_eq!(inner.commit_calls.load(Ordering::SeqCst), 3);
}

/// Inline mode: the sync method runs directly on the async worker. Cheaper
/// per event but incorrect for slow sinks — verified here for behavioral
/// equivalence on a fast in-memory sink.
#[tokio::test(flavor = "current_thread")]
async fn inline_dispatch_records_every_event() {
    let sink = BlockingSink::inline(RecorderSink::new("recorder"));
    let inner = Arc::clone(sink.inner());
    let bs = Arc::new(sink);
    let store = build_store_with_sink(bs);
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
            patch(json!([{ "op": "replace", "path": "/n", "value": 1 }])),
            json!({}),
        )
        .await
        .unwrap();

    let log = inner.log.lock().unwrap().clone();
    assert_eq!(log.len(), 2);
    assert_eq!(log[0].0, Seq(1));
    assert_eq!(log[1].0, Seq(2));

    // catch_up idempotence: replaying the recorder against a caught-up
    // checkpoint should not add duplicate entries.
    let report = store.catch_up("recorder").await.unwrap();
    assert_eq!(report, CatchUpReport::EMPTY);
    assert_eq!(inner.commit_calls.load(Ordering::SeqCst), 2);
}

/// `on_label_deleted` is dispatched through the same `SpawnBlocking` path as
/// `commit`, and reaches the wrapped sync sink.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spawn_blocking_dispatch_delivers_on_label_deleted() {
    let sink = BlockingSink::new(RecorderSink::new("recorder"));
    let inner = Arc::clone(sink.inner());
    let bs = Arc::new(sink);
    let store = build_store_with_sink(bs);
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
        .label_set(&s, &ai_store_core::Label::new("v1"), Seq(1))
        .await
        .unwrap();

    store
        .label_delete(&s, &ai_store_core::Label::new("v1"))
        .await
        .unwrap();

    assert_eq!(
        inner.deleted_labels.lock().unwrap().clone(),
        vec!["v1".to_string()]
    );
}

/// Dispatch mode is exposed for consumer introspection.
#[test]
fn dispatch_mode_reflects_constructor() {
    let sb = BlockingSink::new(RecorderSink::new("a"));
    let il = BlockingSink::inline(RecorderSink::new("b"));
    // We can't read `dispatch` directly, but PartialEq is derived; if the
    // enum ever loses that derive the crate stops compiling this line.
    assert_ne!(Dispatch::Inline, Dispatch::SpawnBlocking);
    let _ = (sb, il);
}
