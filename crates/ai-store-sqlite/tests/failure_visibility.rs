//! Integration tests for the sink-failure-observer, cache prune wiring, and
//! corruption detection paths added in issue #13.

use std::sync::{Arc, Mutex};

use ai_store_core::{
    Event, Label, Patch, ProjectionSink, Seq, SinkDispatchFailure, SinkFailureObserver, SinkOp,
    Store, StoreConfig, StoreError, StreamId,
};
use ai_store_sqlite::SqliteBackends;
use async_trait::async_trait;
use serde_json::{json, Value};

fn patch(v: Value) -> Patch {
    serde_json::from_value::<Patch>(v).unwrap()
}

fn set_root(v: Value) -> Patch {
    patch(json!([{ "op": "add", "path": "", "value": v }]))
}

/// Records every `on_failure` callback the store made against it.
#[derive(Default)]
struct RecordingObserver {
    seen: Mutex<Vec<SinkDispatchFailure>>,
}

impl RecordingObserver {
    fn snapshot(&self) -> Vec<SinkDispatchFailure> {
        self.seen.lock().unwrap().clone()
    }
}

impl SinkFailureObserver for RecordingObserver {
    fn on_failure(&self, failure: &SinkDispatchFailure) {
        self.seen.lock().unwrap().push(failure.clone());
    }
}

/// A sink whose behavior is controlled by a shared flag: normally succeeds,
/// but fails every dispatch call when `Fail::Yes` is toggled.
#[derive(Clone)]
struct FailingSink {
    id: String,
    mode: Arc<Mutex<Mode>>,
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Pass,
    Fail,
}

impl FailingSink {
    fn new(id: &str) -> Self {
        Self {
            id: id.to_string(),
            mode: Arc::new(Mutex::new(Mode::Pass)),
        }
    }
    fn set_mode(&self, m: Mode) {
        *self.mode.lock().unwrap() = m;
    }
}

#[async_trait]
impl ProjectionSink for FailingSink {
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
        match *self.mode.lock().unwrap() {
            Mode::Pass => Ok(()),
            Mode::Fail => Err(StoreError::Backend("sink refused commit".into())),
        }
    }
    async fn on_label_set(
        &self,
        _stream: &StreamId,
        _label: &Label,
        _at: Seq,
        _state: &Value,
        _event: &Event,
    ) -> Result<(), StoreError> {
        match *self.mode.lock().unwrap() {
            Mode::Pass => Ok(()),
            Mode::Fail => Err(StoreError::Backend("sink refused label_set".into())),
        }
    }
    async fn on_label_deleted(&self, _stream: &StreamId, _label: &Label) -> Result<(), StoreError> {
        match *self.mode.lock().unwrap() {
            Mode::Pass => Ok(()),
            Mode::Fail => Err(StoreError::Backend("sink refused label_deleted".into())),
        }
    }
}

// ---- 1. observer receives commit failures --------------------------------

#[tokio::test]
async fn observer_receives_sink_commit_failure_but_append_still_succeeds() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let sink = FailingSink::new("failing");
    let observer = Arc::new(RecordingObserver::default());

    let store = Store::builder(Arc::new(be.events.clone()), Arc::new(be.cache.clone()))
        .sink(Arc::new(sink.clone()))
        .sink_failure_observer(observer.clone())
        .build();

    let s = StreamId::new("doc");
    sink.set_mode(Mode::Fail);

    // The append itself succeeds — the event is durable regardless of the
    // sink outcome. The sink dispatch failure is surfaced through the
    // observer instead.
    let committed = store
        .append(&s, "init", set_root(json!({ "n": 1 })), json!({}))
        .await
        .unwrap();
    assert_eq!(committed.seq, Seq(1));

    let seen = observer.snapshot();
    assert_eq!(
        seen.len(),
        1,
        "expected one observer callback, got {}",
        seen.len()
    );
    let failure = &seen[0];
    assert_eq!(failure.sink_id, "failing");
    assert_eq!(failure.stream, s);
    assert_eq!(failure.seq, Some(Seq(1)));
    assert_eq!(failure.op, SinkOp::Commit);
    assert!(
        failure.error.contains("sink refused commit"),
        "expected error text to include sink message, got {:?}",
        failure.error
    );

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn observer_is_not_called_when_dispatch_succeeds() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let sink = FailingSink::new("s");
    let observer = Arc::new(RecordingObserver::default());

    let store = Store::builder(Arc::new(be.events.clone()), Arc::new(be.cache.clone()))
        .sink(Arc::new(sink.clone()))
        .sink_failure_observer(observer.clone())
        .build();

    let s = StreamId::new("doc");
    store
        .append(&s, "init", set_root(json!({ "n": 1 })), json!({}))
        .await
        .unwrap();

    assert!(observer.snapshot().is_empty());

    be.driver.shutdown().await.unwrap();
}

// ---- 2. observer receives label_set and label_deleted failures -----------

#[tokio::test]
async fn observer_receives_label_set_and_label_deleted_failures() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let sink = FailingSink::new("s");
    let observer = Arc::new(RecordingObserver::default());

    let store = Store::builder(Arc::new(be.events.clone()), Arc::new(be.cache.clone()))
        .sink(Arc::new(sink.clone()))
        .sink_failure_observer(observer.clone())
        .build();

    let s = StreamId::new("doc");
    store
        .append(&s, "init", set_root(json!({ "n": 1 })), json!({}))
        .await
        .unwrap();
    // Clear the append's own successful dispatch from the observer log
    // (there is none, since the sink is still in Pass mode). Now flip.
    sink.set_mode(Mode::Fail);
    let label = Label::new("v1");

    // label_set should succeed at the backend, but the sink notification
    // fails and the observer gets a LabelSet event.
    store.label_set(&s, &label, Seq(1)).await.unwrap();
    // label_delete should also succeed at the backend; the sink
    // notification fails and the observer gets a LabelDeleted event.
    assert!(store.label_delete(&s, &label).await.unwrap());

    let seen = observer.snapshot();
    let ops: Vec<SinkOp> = seen.iter().map(|f| f.op).collect();
    assert!(
        ops.contains(&SinkOp::LabelSet),
        "expected LabelSet in observer log, got {ops:?}"
    );
    assert!(
        ops.contains(&SinkOp::LabelDeleted),
        "expected LabelDeleted in observer log, got {ops:?}"
    );
    // The LabelDeleted callback has no seq — that field is None.
    let deleted = seen.iter().find(|f| f.op == SinkOp::LabelDeleted).unwrap();
    assert_eq!(deleted.seq, None);

    be.driver.shutdown().await.unwrap();
}

// ---- 3. cache_keep_latest trims automatically -----------------------------

#[tokio::test]
async fn cache_keep_latest_bounds_cache_rows_per_stream() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = Store::builder(Arc::new(be.events.clone()), Arc::new(be.cache.clone()))
        .cache_stride(1)
        .cache_keep_latest(2)
        .build();

    let s = StreamId::new("doc");
    store
        .append(&s, "init", set_root(json!({ "n": 1 })), json!({}))
        .await
        .unwrap();
    for i in 2..=5 {
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

    // With stride=1 and keep_latest=2, only the two most-recent cache rows
    // (at_seq = 4 and 5) survive.
    let isle = be.isle();
    let count: i64 = isle
        .call(|conn| {
            conn.query_row("SELECT COUNT(*) FROM cache WHERE stream = 'doc'", [], |r| {
                r.get(0)
            })
        })
        .await
        .unwrap();
    assert_eq!(count, 2, "expected exactly 2 cache rows, got {count}");

    // State reconstruction still works — cache misses just replay forward.
    assert_eq!(store.state(&s).await.unwrap(), json!({ "n": 5 }));

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn cache_keep_latest_default_none_keeps_pre_existing_behavior() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = Store::new(
        Arc::new(be.events.clone()),
        Arc::new(be.cache.clone()),
        Vec::new(),
        Vec::new(),
        StoreConfig {
            cache_stride: 1,
            ..StoreConfig::default()
        },
    );

    let s = StreamId::new("doc");
    store
        .append(&s, "init", set_root(json!({ "n": 1 })), json!({}))
        .await
        .unwrap();
    for i in 2..=5 {
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

    // Without cache_keep_latest set, every stride hit lands as a row.
    let isle = be.isle();
    let count: i64 = isle
        .call(|conn| {
            conn.query_row("SELECT COUNT(*) FROM cache WHERE stream = 'doc'", [], |r| {
                r.get(0)
            })
        })
        .await
        .unwrap();
    assert_eq!(count, 5);

    be.driver.shutdown().await.unwrap();
}

// ---- 4. Store::prune_cache explicit maintenance --------------------------

#[tokio::test]
async fn prune_cache_explicit_call_trims_to_keep_latest() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = Store::new(
        Arc::new(be.events.clone()),
        Arc::new(be.cache.clone()),
        Vec::new(),
        Vec::new(),
        StoreConfig {
            cache_stride: 1,
            ..StoreConfig::default()
        },
    );

    let s = StreamId::new("doc");
    store
        .append(&s, "init", set_root(json!({ "n": 1 })), json!({}))
        .await
        .unwrap();
    for i in 2..=6 {
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

    store.prune_cache(&s, 3).await.unwrap();

    let isle = be.isle();
    let count: i64 = isle
        .call(|conn| {
            conn.query_row("SELECT COUNT(*) FROM cache WHERE stream = 'doc'", [], |r| {
                r.get(0)
            })
        })
        .await
        .unwrap();
    assert_eq!(count, 3);
    // State reconstruction still works.
    assert_eq!(store.state(&s).await.unwrap(), json!({ "n": 6 }));

    be.driver.shutdown().await.unwrap();
}

// ---- 5. is_retryable helper -----------------------------------------------

#[test]
fn is_retryable_flags_busy_only() {
    assert!(StoreError::Busy("x".into()).is_retryable());

    assert!(!StoreError::Storage("x".into()).is_retryable());
    assert!(!StoreError::Corruption("x".into()).is_retryable());
    assert!(!StoreError::Backend("x".into()).is_retryable());
    assert!(!StoreError::UnknownStream(StreamId::new("s")).is_retryable());
    assert!(!StoreError::HeadConflict {
        expected: Seq::ZERO,
        actual: Some(Seq(1))
    }
    .is_retryable());
    assert!(!StoreError::BackendUnsupported("op".into()).is_retryable());
}

// ---- 6. corruption surfaces from a raw-inserted bogus row -----------------

#[tokio::test]
async fn decoding_a_corrupt_events_row_surfaces_as_corruption_variant() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = Store::new(
        Arc::new(be.events.clone()),
        Arc::new(be.cache.clone()),
        Vec::new(),
        Vec::new(),
        StoreConfig::default(),
    );
    let s = StreamId::new("doc");

    // Land one well-formed event so the stream exists and has a head.
    store
        .append(&s, "init", set_root(json!({ "n": 1 })), json!({}))
        .await
        .unwrap();

    // Insert a second row directly with malformed JSON in the `patch`
    // column. This bypasses the append-only triggers (INSERT is allowed);
    // the trigger only blocks UPDATE/DELETE.
    let isle = be.isle();
    isle.call(|conn| {
        conn.execute(
            "INSERT INTO events (stream, seq, kind, patch, meta, at_ms) \
             VALUES ('doc', 2, 'bump', '{{ not valid json', '{}', 0)",
            [],
        )
    })
    .await
    .unwrap();

    // Reading forward hits the malformed row and surfaces as Corruption
    // rather than a generic Backend error.
    let err = store.read(&s, Seq(2), 1).await.unwrap_err();
    match err {
        StoreError::Corruption(msg) => {
            assert!(msg.contains("events.patch"), "unexpected message: {msg}");
        }
        other => panic!("expected Corruption, got {other:?}"),
    }

    be.driver.shutdown().await.unwrap();
}
