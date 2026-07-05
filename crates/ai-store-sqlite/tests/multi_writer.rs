//! Integration tests for cross-connection (multi-writer / multi-process)
//! behavior against a single SQLite file.
//!
//! Each test opens two independent `SqliteBackends` bundles against the
//! same file path. Since every bundle spawns its own `AsyncIsle` (its own
//! writer thread + its own rusqlite `Connection`), two bundles on the same
//! file simulate two OS processes contending on that file at the SQLite
//! level: locking, `busy_timeout`, and the head-CAS transaction all
//! behave the same way they would across a real process boundary.

use std::sync::Arc;

use ai_store_core::{Patch, Seq, Store, StoreConfig, StoreError, StreamId};
use ai_store_sqlite::SqliteBackends;
use serde_json::{json, Value};
use tempfile::TempDir;

fn patch(v: Value) -> Patch {
    serde_json::from_value::<Patch>(v).unwrap()
}

fn set_root(value: Value) -> Patch {
    patch(json!([{ "op": "add", "path": "", "value": value }]))
}

fn open_store(be: &SqliteBackends) -> Store {
    Store::new(
        Arc::new(be.events.clone()),
        Arc::new(be.cache.clone()),
        Vec::new(),
        Vec::new(),
        StoreConfig::default(),
    )
}

// ---- 1. plain appends across two connections serialize correctly ---------

#[tokio::test]
async fn plain_appends_from_two_connections_serialize_via_busy_timeout() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store.db");
    let be_a = SqliteBackends::open(&path).await.unwrap();
    let be_b = SqliteBackends::open(&path).await.unwrap();
    let store_a = open_store(&be_a);
    let store_b = open_store(&be_b);
    let s = StreamId::new("doc");

    // First writer establishes seq=1.
    store_a
        .append(&s, "init", set_root(json!({ "n": 1 })), json!({}))
        .await
        .unwrap();

    // Second connection sees the write and assigns seq=2 via the backend's
    // gap-free MAX(seq)+1 allocation. busy_timeout=5000 keeps the contention
    // benign — no SQLITE_BUSY error even under back-to-back writes.
    let committed = store_b
        .append(
            &s,
            "bump",
            patch(json!([{ "op": "replace", "path": "/n", "value": 2 }])),
            json!({}),
        )
        .await
        .unwrap();
    assert_eq!(committed.seq, Seq(2));

    // Both connections see the final state (WAL means the reader picks up
    // the writer's commit without a fresh open).
    assert_eq!(store_a.state(&s).await.unwrap(), json!({ "n": 2 }));
    assert_eq!(store_b.state(&s).await.unwrap(), json!({ "n": 2 }));

    be_a.driver.shutdown().await.unwrap();
    be_b.driver.shutdown().await.unwrap();
}

// ---- 2. append_if_head detects head drift across connections -------------

#[tokio::test]
async fn append_if_head_conflicts_when_a_second_writer_moved_the_head() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store.db");
    let be_a = SqliteBackends::open(&path).await.unwrap();
    let be_b = SqliteBackends::open(&path).await.unwrap();
    let store_a = open_store(&be_a);
    let store_b = open_store(&be_b);
    let s = StreamId::new("doc");

    // Both writers agree the stream is empty (Seq::ZERO). A commits first.
    let committed_a = store_a
        .append_if_head(
            &s,
            "init",
            set_root(json!({ "n": 1 })),
            json!({}),
            Seq::ZERO,
        )
        .await
        .unwrap();
    assert_eq!(committed_a.seq, Seq(1));

    // B still believes the stream is empty; its CAS must reject.
    let err = store_b
        .append_if_head(
            &s,
            "init-b",
            set_root(json!({ "n": 100 })),
            json!({}),
            Seq::ZERO,
        )
        .await
        .unwrap_err();
    match err {
        StoreError::HeadConflict { expected, actual } => {
            assert_eq!(expected, Seq::ZERO);
            assert_eq!(actual, Some(Seq(1)));
        }
        other => panic!("expected HeadConflict, got {other:?}"),
    }

    // A's write is the only one on the log.
    let evs = store_a.read(&s, Seq(1), 10).await.unwrap();
    assert_eq!(evs.len(), 1);
    assert_eq!(evs[0].kind, "init");
    assert_eq!(store_b.state(&s).await.unwrap(), json!({ "n": 1 }));

    be_a.driver.shutdown().await.unwrap();
    be_b.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn append_if_head_succeeds_when_expected_head_matches_the_current_head() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store.db");
    let be_a = SqliteBackends::open(&path).await.unwrap();
    let be_b = SqliteBackends::open(&path).await.unwrap();
    let store_a = open_store(&be_a);
    let store_b = open_store(&be_b);
    let s = StreamId::new("doc");

    // A initializes.
    store_a
        .append(&s, "init", set_root(json!({ "n": 1 })), json!({}))
        .await
        .unwrap();

    // B reads via the shared file, computes expected_head=1, and CASes.
    let head = store_b.head(&s).await.unwrap().unwrap();
    assert_eq!(head, Seq(1));
    let committed = store_b
        .append_if_head(
            &s,
            "bump",
            patch(json!([{ "op": "replace", "path": "/n", "value": 2 }])),
            json!({}),
            head,
        )
        .await
        .unwrap();
    assert_eq!(committed.seq, Seq(2));
    assert_eq!(store_a.state(&s).await.unwrap(), json!({ "n": 2 }));

    be_a.driver.shutdown().await.unwrap();
    be_b.driver.shutdown().await.unwrap();
}

// ---- 3. HeadConflict edge cases: pre-flight state remap ------------------

#[tokio::test]
async fn append_if_head_reports_unknown_stream_as_head_conflict() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store.db");
    let be = SqliteBackends::open(&path).await.unwrap();
    let store = open_store(&be);
    let s = StreamId::new("empty");

    // Caller expects head=Seq(5), but the stream has never been written to.
    // The pre-flight state read surfaces UnknownStream; the facade remaps to
    // HeadConflict so the caller sees one uniform failure mode.
    let err = store
        .append_if_head(&s, "init", set_root(json!({ "n": 1 })), json!({}), Seq(5))
        .await
        .unwrap_err();
    match err {
        StoreError::HeadConflict { expected, actual } => {
            assert_eq!(expected, Seq(5));
            assert_eq!(actual, None);
        }
        other => panic!("expected HeadConflict, got {other:?}"),
    }

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn append_if_head_reports_seq_out_of_range_as_head_conflict() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store.db");
    let be = SqliteBackends::open(&path).await.unwrap();
    let store = open_store(&be);
    let s = StreamId::new("doc");

    store
        .append(&s, "init", set_root(json!({ "n": 1 })), json!({}))
        .await
        .unwrap();

    // Caller believes head=Seq(10), but head is really Seq(1). state_at(10)
    // returns SeqOutOfRange, which the facade remaps to HeadConflict.
    let err = store
        .append_if_head(
            &s,
            "bump",
            patch(json!([{ "op": "replace", "path": "/n", "value": 2 }])),
            json!({}),
            Seq(10),
        )
        .await
        .unwrap_err();
    match err {
        StoreError::HeadConflict { expected, actual } => {
            assert_eq!(expected, Seq(10));
            assert_eq!(actual, Some(Seq(1)));
        }
        other => panic!("expected HeadConflict, got {other:?}"),
    }

    be.driver.shutdown().await.unwrap();
}

// ---- 4. mixing append_if_head and plain append on one connection ---------

#[tokio::test]
async fn append_if_head_and_append_can_be_interleaved_on_one_store() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let store = open_store(&be);
    let s = StreamId::new("doc");

    // Start with a CAS init on an empty stream.
    store
        .append_if_head(
            &s,
            "init",
            set_root(json!({ "n": 1 })),
            json!({}),
            Seq::ZERO,
        )
        .await
        .unwrap();

    // A plain append lands at seq=2.
    store
        .append(
            &s,
            "bump",
            patch(json!([{ "op": "replace", "path": "/n", "value": 2 }])),
            json!({}),
        )
        .await
        .unwrap();

    // Another CAS with the up-to-date head lands at seq=3.
    let committed = store
        .append_if_head(
            &s,
            "bump",
            patch(json!([{ "op": "replace", "path": "/n", "value": 3 }])),
            json!({}),
            Seq(2),
        )
        .await
        .unwrap();
    assert_eq!(committed.seq, Seq(3));
    assert_eq!(store.state(&s).await.unwrap(), json!({ "n": 3 }));

    be.driver.shutdown().await.unwrap();
}
