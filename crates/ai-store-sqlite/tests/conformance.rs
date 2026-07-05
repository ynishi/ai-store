//! End-to-end conformance tests for the SQLite backends.
//!
//! Mirrors the assertions in `ai-store-mem::tests::conformance` so that the
//! SQLite implementation is proven to satisfy the same SPI contract.
//! Each test opens a fresh in-memory database via `SqliteBackends`, exercises
//! the backend, and shuts the driver down cleanly at the end.

use ai_store_core::{
    CacheBackend, CheckpointBackend, EventBackend, Label, NewEvent, Seq, StoreError, StreamId,
    Timestamp,
};
use ai_store_sqlite::SqliteBackends;
use json_patch::Patch;
use serde_json::json;

fn empty_patch() -> Patch {
    serde_json::from_value::<Patch>(json!([])).unwrap()
}

fn set_root_patch() -> Patch {
    serde_json::from_value::<Patch>(json!([
        { "op": "add", "path": "", "value": { "x": 1 } }
    ]))
    .unwrap()
}

fn new_event(kind: &str, patch: Patch) -> NewEvent {
    NewEvent {
        kind: kind.to_string(),
        patch,
        meta: json!({}),
    }
}

async fn fresh() -> SqliteBackends {
    SqliteBackends::open_in_memory().await.unwrap()
}

#[tokio::test]
async fn append_assigns_gap_free_monotonic_seq_from_one() {
    let be = fresh().await;
    let s = StreamId::new("stream-a");

    let a = be
        .events
        .append(&s, new_event("k1", empty_patch()))
        .await
        .unwrap();
    let b = be
        .events
        .append(&s, new_event("k2", empty_patch()))
        .await
        .unwrap();
    let c = be
        .events
        .append(&s, new_event("k3", empty_patch()))
        .await
        .unwrap();
    assert_eq!(a.seq, Seq(1));
    assert_eq!(b.seq, Seq(2));
    assert_eq!(c.seq, Seq(3));
    assert_eq!(be.events.head(&s).await.unwrap(), Some(Seq(3)));

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn head_is_none_for_unknown_stream() {
    let be = fresh().await;
    let head = be.events.head(&StreamId::new("nope")).await.unwrap();
    assert_eq!(head, None);
    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn read_slices_by_from_and_limit() {
    let be = fresh().await;
    let s = StreamId::new("s");
    for i in 0..5 {
        be.events
            .append(&s, new_event(&format!("k{i}"), empty_patch()))
            .await
            .unwrap();
    }

    let mid = be.events.read(&s, Seq(3), 10).await.unwrap();
    assert_eq!(mid.len(), 3);
    assert_eq!(mid[0].seq, Seq(3));
    assert_eq!(mid[2].seq, Seq(5));

    let capped = be.events.read(&s, Seq(1), 2).await.unwrap();
    assert_eq!(capped.len(), 2);
    assert_eq!(capped[0].seq, Seq(1));
    assert_eq!(capped[1].seq, Seq(2));

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn read_past_head_returns_empty() {
    let be = fresh().await;
    let s = StreamId::new("s");
    be.events
        .append(&s, new_event("only", empty_patch()))
        .await
        .unwrap();

    let out = be.events.read(&s, Seq(99), 10).await.unwrap();
    assert!(out.is_empty());

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn streams_enumerates_distinct_ids() {
    let be = fresh().await;
    be.events
        .append(&StreamId::new("alpha"), new_event("k", empty_patch()))
        .await
        .unwrap();
    be.events
        .append(&StreamId::new("beta"), new_event("k", empty_patch()))
        .await
        .unwrap();
    be.events
        .append(&StreamId::new("alpha"), new_event("k", empty_patch()))
        .await
        .unwrap();

    let mut got: Vec<String> = be
        .events
        .streams()
        .await
        .unwrap()
        .into_iter()
        .map(|s| s.0)
        .collect();
    got.sort();
    assert_eq!(got, vec!["alpha".to_string(), "beta".to_string()]);

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn label_set_requires_existing_seq() {
    let be = fresh().await;
    let s = StreamId::new("s");
    let v = Label::new("v1");

    let err = be.events.label_set(&s, &v, Seq(1)).await.unwrap_err();
    assert!(matches!(err, StoreError::SeqOutOfRange { .. }));

    be.events
        .append(&s, new_event("k", empty_patch()))
        .await
        .unwrap();
    be.events.label_set(&s, &v, Seq(1)).await.unwrap();
    assert_eq!(be.events.label_resolve(&s, &v).await.unwrap(), Some(Seq(1)));

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn label_set_is_a_rewritable_pointer() {
    let be = fresh().await;
    let s = StreamId::new("s");
    let v = Label::new("v1");
    for _ in 0..3 {
        be.events
            .append(&s, new_event("k", empty_patch()))
            .await
            .unwrap();
    }
    be.events.label_set(&s, &v, Seq(1)).await.unwrap();
    assert_eq!(be.events.label_resolve(&s, &v).await.unwrap(), Some(Seq(1)));
    be.events.label_set(&s, &v, Seq(3)).await.unwrap();
    assert_eq!(be.events.label_resolve(&s, &v).await.unwrap(), Some(Seq(3)));
    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn labels_returns_all_pinned_in_stable_order() {
    let be = fresh().await;
    let s = StreamId::new("s");
    for _ in 0..3 {
        be.events
            .append(&s, new_event("k", empty_patch()))
            .await
            .unwrap();
    }
    be.events
        .label_set(&s, &Label::new("v2"), Seq(2))
        .await
        .unwrap();
    be.events
        .label_set(&s, &Label::new("v1"), Seq(1))
        .await
        .unwrap();
    be.events
        .label_set(&s, &Label::new("v3"), Seq(3))
        .await
        .unwrap();

    let got: Vec<(String, u64)> = be
        .events
        .labels(&s)
        .await
        .unwrap()
        .into_iter()
        .map(|(l, s)| (l.0, s.0))
        .collect();
    assert_eq!(
        got,
        vec![
            ("v1".to_string(), 1),
            ("v2".to_string(), 2),
            ("v3".to_string(), 3),
        ]
    );

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn seq_at_time_returns_greatest_seq_at_or_before_timestamp() {
    let be = fresh().await;
    let s = StreamId::new("s");

    assert_eq!(
        be.events.seq_at_time(&s, Timestamp(1_000)).await.unwrap(),
        None
    );

    let seq1 = be
        .events
        .append(&s, new_event("a", empty_patch()))
        .await
        .unwrap()
        .seq;
    let t1 = be.events.read(&s, seq1, 1).await.unwrap()[0].at;
    std::thread::sleep(std::time::Duration::from_millis(3));
    let seq2 = be
        .events
        .append(&s, new_event("b", empty_patch()))
        .await
        .unwrap()
        .seq;
    let t2 = be.events.read(&s, seq2, 1).await.unwrap()[0].at;
    assert!(t2.0 > t1.0);

    assert_eq!(
        be.events
            .seq_at_time(&s, Timestamp(t1.0 - 1))
            .await
            .unwrap(),
        None
    );
    assert_eq!(be.events.seq_at_time(&s, t1).await.unwrap(), Some(seq1));
    assert_eq!(
        be.events
            .seq_at_time(&s, Timestamp(t2.0 - 1))
            .await
            .unwrap(),
        Some(seq1)
    );
    assert_eq!(be.events.seq_at_time(&s, t2).await.unwrap(), Some(seq2));

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn seq_at_time_on_unknown_stream_returns_none() {
    let be = fresh().await;
    assert_eq!(
        be.events
            .seq_at_time(&StreamId::new("nope"), Timestamp(0))
            .await
            .unwrap(),
        None
    );
    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn cache_nearest_returns_closest_not_exceeding_at() {
    let be = fresh().await;
    let s = StreamId::new("s");
    be.cache.put(&s, Seq(2), &json!({ "at": 2 })).await.unwrap();
    be.cache.put(&s, Seq(6), &json!({ "at": 6 })).await.unwrap();
    be.cache.put(&s, Seq(9), &json!({ "at": 9 })).await.unwrap();

    assert_eq!(be.cache.nearest(&s, Seq(1)).await.unwrap(), None);
    assert_eq!(
        be.cache.nearest(&s, Seq(5)).await.unwrap(),
        Some((Seq(2), json!({ "at": 2 })))
    );
    assert_eq!(
        be.cache.nearest(&s, Seq(6)).await.unwrap(),
        Some((Seq(6), json!({ "at": 6 })))
    );
    assert_eq!(
        be.cache.nearest(&s, Seq(100)).await.unwrap(),
        Some((Seq(9), json!({ "at": 9 })))
    );

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn cache_nearest_on_unknown_stream_returns_none() {
    let be = fresh().await;
    assert_eq!(
        be.cache
            .nearest(&StreamId::new("nope"), Seq(1))
            .await
            .unwrap(),
        None
    );
    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn cache_prune_keeps_latest_only() {
    let be = fresh().await;
    let s = StreamId::new("s");
    for i in 1..=10u64 {
        be.cache.put(&s, Seq(i), &json!({ "at": i })).await.unwrap();
    }
    be.cache.prune(&s, 3).await.unwrap();

    assert_eq!(be.cache.nearest(&s, Seq(7)).await.unwrap(), None);
    assert_eq!(
        be.cache.nearest(&s, Seq(8)).await.unwrap(),
        Some((Seq(8), json!({ "at": 8 })))
    );
    assert_eq!(
        be.cache.nearest(&s, Seq(15)).await.unwrap(),
        Some((Seq(10), json!({ "at": 10 })))
    );

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn label_delete_removes_an_existing_label() {
    let be = fresh().await;
    let s = StreamId::new("s");
    let v = Label::new("v1");

    be.events
        .append(&s, new_event("k", empty_patch()))
        .await
        .unwrap();
    be.events.label_set(&s, &v, Seq(1)).await.unwrap();
    assert_eq!(be.events.label_resolve(&s, &v).await.unwrap(), Some(Seq(1)));

    let existed = be.events.label_delete(&s, &v).await.unwrap();
    assert!(existed);
    assert_eq!(be.events.label_resolve(&s, &v).await.unwrap(), None);

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn label_delete_of_unknown_label_reports_not_found() {
    let be = fresh().await;
    let s = StreamId::new("s");

    be.events
        .append(&s, new_event("k", empty_patch()))
        .await
        .unwrap();
    let existed = be
        .events
        .label_delete(&s, &Label::new("nope"))
        .await
        .unwrap();
    assert!(!existed);

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn import_event_assigns_next_seq_and_preserves_supplied_timestamp() {
    let be = fresh().await;
    let s = StreamId::new("s");

    be.events
        .append(&s, new_event("k1", empty_patch()))
        .await
        .unwrap();

    let at = Timestamp(1_700_000_000_000);
    let committed = be
        .events
        .import_event(&s, new_event("k2", empty_patch()), at)
        .await
        .unwrap();
    assert_eq!(committed.seq, Seq(2));
    assert_eq!(committed.at, at);
    assert_eq!(be.events.head(&s).await.unwrap(), Some(Seq(2)));

    let events = be.events.read(&s, Seq(2), 1).await.unwrap();
    assert_eq!(events[0].at, at);

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn import_event_on_empty_stream_starts_at_seq_one() {
    let be = fresh().await;
    let s = StreamId::new("s");
    let at = Timestamp(1_700_000_000_000);

    let committed = be
        .events
        .import_event(&s, new_event("k1", empty_patch()), at)
        .await
        .unwrap();
    assert_eq!(committed.seq, Seq(1));
    assert_eq!(be.events.head(&s).await.unwrap(), Some(Seq(1)));

    be.driver.shutdown().await.unwrap();
}

// ---- checkpoint backend ---------------------------------------------------

#[tokio::test]
async fn checkpoint_put_then_get_round_trips() {
    let be = fresh().await;
    let s = StreamId::new("s");

    assert_eq!(be.checkpoints.get("sink-a", &s).await.unwrap(), None);

    be.checkpoints.put("sink-a", &s, Seq(5)).await.unwrap();
    assert_eq!(
        be.checkpoints.get("sink-a", &s).await.unwrap(),
        Some(Seq(5))
    );

    // A second put overwrites (upsert on the (sink_id, stream) primary key)
    // rather than accumulating a new row.
    be.checkpoints.put("sink-a", &s, Seq(9)).await.unwrap();
    assert_eq!(
        be.checkpoints.get("sink-a", &s).await.unwrap(),
        Some(Seq(9))
    );

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn checkpoint_is_scoped_per_sink_and_per_stream() {
    let be = fresh().await;
    let a = StreamId::new("a");
    let b = StreamId::new("b");

    be.checkpoints.put("sink-1", &a, Seq(1)).await.unwrap();
    be.checkpoints.put("sink-2", &a, Seq(2)).await.unwrap();
    be.checkpoints.put("sink-1", &b, Seq(3)).await.unwrap();

    assert_eq!(
        be.checkpoints.get("sink-1", &a).await.unwrap(),
        Some(Seq(1))
    );
    assert_eq!(
        be.checkpoints.get("sink-2", &a).await.unwrap(),
        Some(Seq(2))
    );
    assert_eq!(
        be.checkpoints.get("sink-1", &b).await.unwrap(),
        Some(Seq(3))
    );
    assert_eq!(be.checkpoints.get("sink-2", &b).await.unwrap(), None);

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn event_patch_round_trips_through_append_and_read() {
    let be = fresh().await;
    let s = StreamId::new("s");
    be.events
        .append(&s, new_event("init", set_root_patch()))
        .await
        .unwrap();

    let out = be.events.read(&s, Seq(1), 1).await.unwrap();
    assert_eq!(out.len(), 1);
    let ev = &out[0];
    assert_eq!(ev.seq, Seq(1));
    assert_eq!(ev.kind, "init");
    let expected = serde_json::to_value(set_root_patch()).unwrap();
    let observed = serde_json::to_value(&ev.patch).unwrap();
    assert_eq!(observed, expected);

    be.driver.shutdown().await.unwrap();
}

/// `Committed.at` (returned inline from `append`) must be exactly the `at`
/// a follow-up `read` reports for that same event — the round-trip this
/// type exists to make unnecessary.
#[tokio::test]
async fn append_returned_at_matches_the_read_back_event() {
    let be = fresh().await;
    let s = StreamId::new("s");

    let committed = be
        .events
        .append(&s, new_event("init", empty_patch()))
        .await
        .unwrap();

    let events = be.events.read(&s, committed.seq, 1).await.unwrap();
    assert_eq!(events[0].at, committed.at);

    be.driver.shutdown().await.unwrap();
}
