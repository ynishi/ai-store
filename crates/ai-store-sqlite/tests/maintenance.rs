//! Integration tests for `ai_store_sqlite::maintenance::SqliteMaintenance`.
//!
//! Exercises the compaction contract end-to-end against a live `Store` +
//! `SqliteBackends` triple: prefix removal, snapshot insertion, cache
//! pruning, boundary error surfaces on `state_at`/`revert`, and continued
//! trigger enforcement after the maintenance operation returns.

use std::sync::Arc;

use ai_store_core::{
    Patch, Seq, Store, StoreConfig, StoreError, StreamId, SNAPSHOT_KIND,
};
use ai_store_sqlite::{
    snapshot_meta_compacted_at_seq, SqliteBackends, SqliteMaintenance,
    SNAPSHOT_META_KEY_COMPACTED_AT_SEQ,
};
use serde_json::{json, Value};

fn patch(v: Value) -> Patch {
    serde_json::from_value::<Patch>(v).unwrap()
}

fn set_root(value: Value) -> Patch {
    patch(json!([{ "op": "add", "path": "", "value": value }]))
}

async fn store_with_backends(be: &SqliteBackends) -> Store {
    Store::new(
        Arc::new(be.events.clone()),
        Arc::new(be.cache.clone()),
        Vec::new(),
        Vec::new(),
        StoreConfig::default(),
    )
}

/// Append `n` events building an integer counter — seq 1 `add root = {n: 1}`,
/// then seq 2..n replacing `/n` with the seq index. Returns the final head.
async fn append_counter(store: &Store, s: &StreamId, n: u64) -> Seq {
    store
        .append(s, "init", set_root(json!({ "n": 1 })), json!({}))
        .await
        .unwrap();
    for i in 2..=n {
        store
            .append(
                s,
                "bump",
                patch(json!([{ "op": "replace", "path": "/n", "value": i }])),
                json!({}),
            )
            .await
            .unwrap();
    }
    Seq(n)
}

// ---- 1. happy path ---------------------------------------------------------

#[tokio::test]
async fn compact_stream_replaces_prefix_with_a_snapshot_event() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = store_with_backends(&be).await;
    let maint = SqliteMaintenance::new(be.isle());
    let s = StreamId::new("doc");

    let head = append_counter(&store, &s, 5).await;
    assert_eq!(head, Seq(5));
    assert_eq!(store.state(&s).await.unwrap(), json!({ "n": 5 }));

    let report = maint.compact_stream(&store, &s, Seq(3)).await.unwrap();
    assert_eq!(report.stream, s);
    assert_eq!(report.boundary, Seq(3));
    assert_eq!(report.head_at_compaction, Seq(5));

    // Head unchanged: only the prefix moved.
    assert_eq!(store.head(&s).await.unwrap(), Some(Seq(5)));

    // The snapshot event now occupies seq=3 with kind=SNAPSHOT_KIND and a
    // patch that materializes state@3 directly.
    let evs = store.read(&s, Seq(3), 1).await.unwrap();
    let snap = &evs[0];
    assert_eq!(snap.kind, SNAPSHOT_KIND);
    assert_eq!(
        snap.meta.get(SNAPSHOT_META_KEY_COMPACTED_AT_SEQ),
        Some(&json!(3))
    );
    assert_eq!(snapshot_meta_compacted_at_seq(snap), Some(Seq(3)));

    // Prefix events (seq 1, 2) are gone: the earliest event on the stream
    // is now the snapshot at seq=3.
    let earliest = store.read(&s, Seq(1), 1).await.unwrap();
    assert_eq!(earliest[0].seq, Seq(3));
    assert_eq!(earliest[0].kind, SNAPSHOT_KIND);
    // The full remaining log is [snapshot@3, ev@4, ev@5].
    let all = store.read(&s, Seq(1), 100).await.unwrap();
    assert_eq!(all.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![Seq(3), Seq(4), Seq(5)]);

    // state_at at and after boundary reconstruct correctly.
    assert_eq!(store.state_at(&s, Seq(3)).await.unwrap(), json!({ "n": 3 }));
    assert_eq!(store.state_at(&s, Seq(4)).await.unwrap(), json!({ "n": 4 }));
    assert_eq!(store.state(&s).await.unwrap(), json!({ "n": 5 }));

    be.driver.shutdown().await.unwrap();
}

// ---- 2. boundary error on state_at and revert ------------------------------

#[tokio::test]
async fn state_at_below_boundary_returns_seq_compacted() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = store_with_backends(&be).await;
    let maint = SqliteMaintenance::new(be.isle());
    let s = StreamId::new("doc");

    append_counter(&store, &s, 5).await;
    maint.compact_stream(&store, &s, Seq(3)).await.unwrap();

    // seq 1 and 2 are below the boundary.
    for seq in [Seq(1), Seq(2)] {
        let err = store.state_at(&s, seq).await.unwrap_err();
        match err {
            StoreError::SeqCompacted { boundary, requested } => {
                assert_eq!(boundary, Seq(3));
                assert_eq!(requested, seq);
            }
            other => panic!("expected SeqCompacted, got {other:?}"),
        }
    }
    // seq == boundary is still fine.
    assert!(store.state_at(&s, Seq(3)).await.is_ok());

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn revert_below_boundary_returns_seq_compacted() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = store_with_backends(&be).await;
    let maint = SqliteMaintenance::new(be.isle());
    let s = StreamId::new("doc");

    append_counter(&store, &s, 5).await;
    maint.compact_stream(&store, &s, Seq(3)).await.unwrap();

    // revert to seq 1 fails because state_at(1) fails.
    let err = store.revert(&s, Seq(1)).await.unwrap_err();
    assert!(
        matches!(err, StoreError::SeqCompacted { boundary, .. } if boundary == Seq(3)),
        "expected SeqCompacted at boundary Seq(3), got {err:?}"
    );

    // revert to boundary itself works and appends a new event at seq=6.
    let committed = store.revert(&s, Seq(3)).await.unwrap();
    assert_eq!(committed.seq, Seq(6));
    assert_eq!(store.state(&s).await.unwrap(), json!({ "n": 3 }));

    be.driver.shutdown().await.unwrap();
}

// ---- 3. triggers stay in force after compaction ----------------------------

#[tokio::test]
async fn append_only_triggers_are_restored_after_compaction() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = store_with_backends(&be).await;
    let maint = SqliteMaintenance::new(be.isle());
    let s = StreamId::new("doc");

    append_counter(&store, &s, 5).await;
    maint.compact_stream(&store, &s, Seq(3)).await.unwrap();

    // Ordinary append still works (writer path is unaffected).
    store
        .append(
            &s,
            "bump",
            patch(json!([{ "op": "replace", "path": "/n", "value": 6 }])),
            json!({}),
        )
        .await
        .unwrap();
    assert_eq!(store.state(&s).await.unwrap(), json!({ "n": 6 }));

    // Raw UPDATE / DELETE on the events table are rejected by the reinstalled
    // triggers, just like before compaction ran.
    let isle = be.isle();
    let update_err = isle
        .call(|conn| {
            conn.execute(
                "UPDATE events SET kind = 'tampered' WHERE seq = 4",
                [],
            )
        })
        .await
        .unwrap_err();
    assert!(
        update_err.to_string().contains("append-only"),
        "expected append-only trigger to reject UPDATE, got: {update_err}"
    );

    let delete_err = isle
        .call(|conn| conn.execute("DELETE FROM events WHERE seq = 4", []))
        .await
        .unwrap_err();
    assert!(
        delete_err.to_string().contains("append-only"),
        "expected append-only trigger to reject DELETE, got: {delete_err}"
    );

    be.driver.shutdown().await.unwrap();
}

// ---- 4. successive compactions --------------------------------------------

#[tokio::test]
async fn compacting_twice_advances_the_boundary() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = store_with_backends(&be).await;
    let maint = SqliteMaintenance::new(be.isle());
    let s = StreamId::new("doc");

    append_counter(&store, &s, 10).await;

    maint.compact_stream(&store, &s, Seq(3)).await.unwrap();
    assert_eq!(store.state_at(&s, Seq(3)).await.unwrap(), json!({ "n": 3 }));

    // Second compaction: absorbs the previous snapshot and events 4..7 into a
    // new snapshot at seq=7.
    let report = maint.compact_stream(&store, &s, Seq(7)).await.unwrap();
    assert_eq!(report.boundary, Seq(7));

    let evs = store.read(&s, Seq(7), 1).await.unwrap();
    assert_eq!(evs[0].kind, SNAPSHOT_KIND);

    // seq 3 is now below the new boundary.
    let err = store.state_at(&s, Seq(3)).await.unwrap_err();
    assert!(matches!(err, StoreError::SeqCompacted { boundary, .. } if boundary == Seq(7)));

    // State at and after the new boundary is intact.
    assert_eq!(store.state_at(&s, Seq(7)).await.unwrap(), json!({ "n": 7 }));
    assert_eq!(store.state(&s).await.unwrap(), json!({ "n": 10 }));

    be.driver.shutdown().await.unwrap();
}

// ---- 5. edge cases: bounds, up_to_seq == head -----------------------------

#[tokio::test]
async fn compact_stream_at_head_leaves_only_the_snapshot() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = store_with_backends(&be).await;
    let maint = SqliteMaintenance::new(be.isle());
    let s = StreamId::new("doc");

    append_counter(&store, &s, 5).await;
    maint.compact_stream(&store, &s, Seq(5)).await.unwrap();

    // Only the snapshot event remains.
    assert_eq!(store.head(&s).await.unwrap(), Some(Seq(5)));
    let evs = store.read(&s, Seq(1), 100).await.unwrap();
    assert_eq!(evs.len(), 1);
    assert_eq!(evs[0].seq, Seq(5));
    assert_eq!(evs[0].kind, SNAPSHOT_KIND);
    assert_eq!(store.state(&s).await.unwrap(), json!({ "n": 5 }));

    // Subsequent appends land at seq=6+ and replay from the snapshot.
    store
        .append(
            &s,
            "bump",
            patch(json!([{ "op": "replace", "path": "/n", "value": 6 }])),
            json!({}),
        )
        .await
        .unwrap();
    assert_eq!(store.state(&s).await.unwrap(), json!({ "n": 6 }));

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn compact_stream_rejects_seq_zero() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = store_with_backends(&be).await;
    let maint = SqliteMaintenance::new(be.isle());
    let s = StreamId::new("doc");

    append_counter(&store, &s, 3).await;

    let err = maint
        .compact_stream(&store, &s, Seq::ZERO)
        .await
        .unwrap_err();
    match err {
        StoreError::Backend(msg) => assert!(msg.contains("up_to_seq must be > 0"), "{msg}"),
        other => panic!("expected Backend error, got {other:?}"),
    }

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn compact_stream_rejects_seq_beyond_head() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = store_with_backends(&be).await;
    let maint = SqliteMaintenance::new(be.isle());
    let s = StreamId::new("doc");

    append_counter(&store, &s, 3).await;

    let err = maint
        .compact_stream(&store, &s, Seq(10))
        .await
        .unwrap_err();
    match err {
        StoreError::SeqOutOfRange { head, requested } => {
            assert_eq!(head, Some(Seq(3)));
            assert_eq!(requested, Seq(10));
        }
        other => panic!("expected SeqOutOfRange, got {other:?}"),
    }

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn compact_stream_rejects_unknown_stream() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = store_with_backends(&be).await;
    let maint = SqliteMaintenance::new(be.isle());
    let s = StreamId::new("does-not-exist");

    let err = maint
        .compact_stream(&store, &s, Seq(1))
        .await
        .unwrap_err();
    assert!(
        matches!(err, StoreError::UnknownStream(ref sid) if sid == &s),
        "expected UnknownStream, got {err:?}"
    );

    be.driver.shutdown().await.unwrap();
}

// ---- 6. cache pruning ------------------------------------------------------

#[tokio::test]
async fn compact_stream_prunes_cache_entries_below_the_boundary() {
    // Use a small cache_stride so the writer path lays down cache rows we
    // can observe pre-/post-compaction.
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
    let maint = SqliteMaintenance::new(be.isle());
    let s = StreamId::new("doc");

    append_counter(&store, &s, 6).await;

    // Sanity: cache has rows at every seq.
    let isle_before = be.isle();
    let before_count: i64 = isle_before
        .call(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM cache WHERE stream = 'doc'",
                [],
                |r| r.get(0),
            )
        })
        .await
        .unwrap();
    assert_eq!(before_count, 6);

    maint.compact_stream(&store, &s, Seq(4)).await.unwrap();

    // Cache rows at at_seq < 4 are pruned; rows at at_seq >= 4 survive.
    let isle_after = be.isle();
    let (below, at_or_above): (i64, i64) = isle_after
        .call(|conn| {
            let below: i64 = conn.query_row(
                "SELECT COUNT(*) FROM cache WHERE stream = 'doc' AND at_seq < 4",
                [],
                |r| r.get(0),
            )?;
            let at_or_above: i64 = conn.query_row(
                "SELECT COUNT(*) FROM cache WHERE stream = 'doc' AND at_seq >= 4",
                [],
                |r| r.get(0),
            )?;
            Ok((below, at_or_above))
        })
        .await
        .unwrap();
    assert_eq!(below, 0, "expected pre-boundary cache rows to be pruned");
    assert!(
        at_or_above >= 1,
        "expected at least one cache row at at_seq >= boundary (got {at_or_above})"
    );

    be.driver.shutdown().await.unwrap();
}
