//! Integration tests for `ai_store_sqlite::read_model::SqliteReadModel`.
//!
//! Mirrors the construction style of `tests/facade.rs`: each test opens a
//! `SqliteBackends` bundle, wires a `SqliteReadModel` in as a
//! `ProjectionSink`, drives writes through the `Store` facade, and asserts on
//! the read-model's query surface.

use std::sync::Arc;

use ai_store_core::{
    CatchUpReport, CheckpointBackend, Event, Patch, ProjectionSink, Seq, Store, StoreConfig,
    StoreError, StreamId, Timestamp, TOMBSTONE_KIND,
};
use ai_store_sqlite::{Filter, Order, Query, SqliteBackends, SqliteReadModel};
use rusqlite::Connection;
use serde_json::{json, Value};
use tempfile::TempDir;

fn patch(v: Value) -> Patch {
    serde_json::from_value::<Patch>(v).unwrap()
}

fn set_root(value: Value) -> Patch {
    patch(json!([{ "op": "add", "path": "", "value": value }]))
}

/// A `Store` wired with one plain (no tombstone kind) `SqliteReadModel` sink,
/// sharing the same SQLite thread as `be`.
async fn store_with_read_model(be: &SqliteBackends) -> (Store, SqliteReadModel) {
    let rm = SqliteReadModel::new(be.isle());
    let sinks: Vec<Arc<dyn ProjectionSink>> = vec![Arc::new(rm.clone())];
    let store = Store::new(
        Arc::new(be.events.clone()),
        Arc::new(be.cache.clone()),
        Vec::new(),
        sinks,
        StoreConfig::default(),
    );
    (store, rm)
}

// ---- 1. filters after catch_up -------------------------------------------

#[tokio::test]
async fn query_filters_eq_in_and_or_like_after_catch_up() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let (store, rm) = store_with_read_model(&be).await;

    for (id, title, status) in [
        ("doc-1", "Alpha report", "open"),
        ("doc-2", "Beta memo", "closed"),
        ("doc-3", "Alpha memo", "open"),
    ] {
        let s = StreamId::new(id);
        store
            .append(
                &s,
                "created",
                set_root(json!({ "title": title, "status": status })),
                json!({}),
            )
            .await
            .unwrap();
    }

    // Sinks are dispatched inline on append (best-effort); drive catch_up
    // too, so the checkpoint + replay path is exercised explicitly as the
    // task spec calls for.
    store.catch_up(rm.id()).await.unwrap();

    let ids_of = |rows: &[ai_store_sqlite::ReadModelRow]| -> Vec<String> {
        let mut v: Vec<String> = rows.iter().map(|r| r.stream.as_str().to_string()).collect();
        v.sort();
        v
    };

    // Eq
    let open_rows = rm
        .query(&Query {
            filter: Some(Filter::Eq("status".to_string(), json!("open"))),
            ..Query::default()
        })
        .await
        .unwrap();
    assert_eq!(ids_of(&open_rows), vec!["doc-1", "doc-3"]);

    // In
    let in_rows = rm
        .query(&Query {
            filter: Some(Filter::In(
                "status".to_string(),
                vec![json!("closed"), json!("nonexistent")],
            )),
            ..Query::default()
        })
        .await
        .unwrap();
    assert_eq!(ids_of(&in_rows), vec!["doc-2"]);

    // And
    let and_rows = rm
        .query(&Query {
            filter: Some(Filter::And(vec![
                Filter::Eq("status".to_string(), json!("open")),
                Filter::Like("title".to_string(), "Alpha%".to_string()),
            ])),
            ..Query::default()
        })
        .await
        .unwrap();
    assert_eq!(ids_of(&and_rows), vec!["doc-1", "doc-3"]);

    // Or
    let or_rows = rm
        .query(&Query {
            filter: Some(Filter::Or(vec![
                Filter::Eq("status".to_string(), json!("closed")),
                Filter::Eq("title".to_string(), json!("Alpha report")),
            ])),
            ..Query::default()
        })
        .await
        .unwrap();
    assert_eq!(ids_of(&or_rows), vec!["doc-1", "doc-2"]);

    // Like
    let like_rows = rm
        .query(&Query {
            filter: Some(Filter::Like("title".to_string(), "%memo".to_string())),
            ..Query::default()
        })
        .await
        .unwrap();
    assert_eq!(ids_of(&like_rows), vec!["doc-2", "doc-3"]);

    be.driver.shutdown().await.unwrap();
}

// ---- 2. order_by + pagination ---------------------------------------------

#[tokio::test]
async fn query_order_by_and_pagination() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let (store, rm) = store_with_read_model(&be).await;

    for (id, n) in [("s1", 3), ("s2", 1), ("s3", 2)] {
        let s = StreamId::new(id);
        store
            .append(&s, "created", set_root(json!({ "n": n })), json!({}))
            .await
            .unwrap();
    }

    let ns_of = |rows: &[ai_store_sqlite::ReadModelRow]| -> Vec<i64> {
        rows.iter()
            .map(|r| r.state["n"].as_i64().unwrap())
            .collect()
    };

    let page1 = rm
        .query(&Query {
            order_by: Some(("n".to_string(), Order::Asc)),
            limit: 2,
            ..Query::default()
        })
        .await
        .unwrap();
    assert_eq!(ns_of(&page1), vec![1, 2]);

    let page2 = rm
        .query(&Query {
            order_by: Some(("n".to_string(), Order::Asc)),
            limit: 2,
            offset: 2,
            ..Query::default()
        })
        .await
        .unwrap();
    assert_eq!(ns_of(&page2), vec![3]);

    let desc = rm
        .query(&Query {
            order_by: Some(("n".to_string(), Order::Desc)),
            ..Query::default()
        })
        .await
        .unwrap();
    assert_eq!(ns_of(&desc), vec![3, 2, 1]);

    be.driver.shutdown().await.unwrap();
}

// ---- 3. count / get / tail -------------------------------------------------

#[tokio::test]
async fn count_get_and_tail() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let (store, rm) = store_with_read_model(&be).await;

    // Explicit, strictly increasing `at` via `import_event` so `tail`'s
    // `updated_at DESC` ordering is deterministic regardless of how fast
    // wall-clock `append` calls land in the same millisecond.
    for (id, n, at_ms) in [("s1", 1, 1_000_i64), ("s2", 2, 2_000), ("s3", 3, 3_000)] {
        let s = StreamId::new(id);
        store
            .import_event(
                &s,
                "created",
                set_root(json!({ "n": n })),
                json!({}),
                Timestamp(at_ms),
            )
            .await
            .unwrap();
    }

    assert_eq!(rm.count(None, false).await.unwrap(), 3);
    assert_eq!(
        rm.count(Some(&Filter::Eq("n".to_string(), json!(2))), false)
            .await
            .unwrap(),
        1
    );

    let row = rm.get(&StreamId::new("s2")).await.unwrap().unwrap();
    assert_eq!(row.state, json!({ "n": 2 }));
    assert_eq!(row.last_seq, Seq(1));
    assert_eq!(row.updated_at, Timestamp(2_000));

    assert!(rm.get(&StreamId::new("missing")).await.unwrap().is_none());

    let last = rm.tail(1).await.unwrap();
    assert_eq!(last.len(), 1);
    assert_eq!(last[0].stream, StreamId::new("s3"));

    let all = rm.tail(10).await.unwrap();
    let all_ids: Vec<String> = all.iter().map(|r| r.stream.as_str().to_string()).collect();
    assert_eq!(
        all_ids,
        vec!["s3".to_string(), "s2".to_string(), "s1".to_string()]
    );

    be.driver.shutdown().await.unwrap();
}

// ---- 4. tombstone kind ------------------------------------------------------

#[tokio::test]
async fn default_tombstone_kind_matches_store_delete() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    // Note: SqliteReadModel::new() no longer requires with_tombstone_kind —
    // it defaults to the core-level TOMBSTONE_KIND, so Store::delete()
    // integrates without any extra wiring.
    let rm = SqliteReadModel::new(be.isle());
    let sinks: Vec<Arc<dyn ProjectionSink>> = vec![Arc::new(rm.clone())];
    let store = Store::new(
        Arc::new(be.events.clone()),
        Arc::new(be.cache.clone()),
        Vec::new(),
        sinks,
        StoreConfig::default(),
    );
    let s = StreamId::new("doc-1");

    store
        .append(&s, "created", set_root(json!({ "title": "x" })), json!({}))
        .await
        .unwrap();
    assert!(rm.get(&s).await.unwrap().unwrap().live);

    store.delete(&s, json!({})).await.unwrap();
    // The tombstone event used TOMBSTONE_KIND; the read-model's default
    // recognizes it and flips `live` to false.
    assert_eq!(rm.query(&Query::default()).await.unwrap().len(), 0);
    assert!(!rm.get(&s).await.unwrap().unwrap().live);
    let _ = TOMBSTONE_KIND; // touch the re-export so this stays a compile-time link

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn without_tombstone_kind_disables_live_toggling() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let rm = SqliteReadModel::new(be.isle()).without_tombstone_kind();
    let sinks: Vec<Arc<dyn ProjectionSink>> = vec![Arc::new(rm.clone())];
    let store = Store::new(
        Arc::new(be.events.clone()),
        Arc::new(be.cache.clone()),
        Vec::new(),
        sinks,
        StoreConfig::default(),
    );
    let s = StreamId::new("doc-1");

    store
        .append(&s, "created", set_root(json!({ "title": "x" })), json!({}))
        .await
        .unwrap();
    // Even Store::delete() (whose kind is TOMBSTONE_KIND) leaves live=true
    // when tombstoning is disabled on this sink.
    store.delete(&s, json!({})).await.unwrap();
    assert!(rm.get(&s).await.unwrap().unwrap().live);

    be.driver.shutdown().await.unwrap();
}

#[tokio::test]
async fn tombstone_kind_toggles_live_and_revives_on_further_append() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let rm = SqliteReadModel::new(be.isle()).with_tombstone_kind("deleted");
    let sinks: Vec<Arc<dyn ProjectionSink>> = vec![Arc::new(rm.clone())];
    let store = Store::new(
        Arc::new(be.events.clone()),
        Arc::new(be.cache.clone()),
        Vec::new(),
        sinks,
        StoreConfig::default(),
    );
    let s = StreamId::new("doc-1");

    store
        .append(&s, "created", set_root(json!({ "title": "x" })), json!({}))
        .await
        .unwrap();

    let live_query = Query::default(); // include_dead = false
    assert_eq!(rm.query(&live_query).await.unwrap().len(), 1);

    store
        .append(&s, "deleted", patch(json!([])), json!({}))
        .await
        .unwrap();

    assert_eq!(rm.query(&live_query).await.unwrap().len(), 0);
    let with_dead = Query {
        include_dead: true,
        ..Query::default()
    };
    assert_eq!(rm.query(&with_dead).await.unwrap().len(), 1);
    assert!(!rm.get(&s).await.unwrap().unwrap().live);

    // A further, non-tombstone-kind append revives the row.
    store
        .append(
            &s,
            "updated",
            patch(json!([{ "op": "replace", "path": "/title", "value": "y" }])),
            json!({}),
        )
        .await
        .unwrap();

    assert_eq!(rm.query(&live_query).await.unwrap().len(), 1);
    assert!(rm.get(&s).await.unwrap().unwrap().live);

    be.driver.shutdown().await.unwrap();
}

// ---- 5. reopen survival + catch_up no-op -----------------------------------

#[tokio::test]
async fn read_model_survives_reopen_and_catch_up_is_a_no_op() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store.db");
    let s = StreamId::new("doc-1");

    {
        let be = SqliteBackends::open(&path).await.unwrap();
        let rm = SqliteReadModel::new(be.isle());
        let sinks: Vec<Arc<dyn ProjectionSink>> = vec![Arc::new(rm.clone())];
        let checkpoint_backend: Arc<dyn CheckpointBackend> = Arc::new(be.checkpoints.clone());
        let store = Store::with_checkpoint_backend(
            Arc::new(be.events.clone()),
            Arc::new(be.cache.clone()),
            Vec::new(),
            sinks,
            StoreConfig::default(),
            checkpoint_backend,
        );

        store
            .append(&s, "created", set_root(json!({ "n": 1 })), json!({}))
            .await
            .unwrap();

        assert_eq!(rm.count(None, false).await.unwrap(), 1);
        be.driver.shutdown().await.unwrap();
    }

    {
        let be = SqliteBackends::open(&path).await.unwrap();
        let rm = SqliteReadModel::new(be.isle());
        // The row is already there — the table itself is durable.
        let row = rm.get(&s).await.unwrap().unwrap();
        assert_eq!(row.state, json!({ "n": 1 }));

        // catch_up against a freshly re-attached sink, with the checkpoint
        // backend restoring the persisted watermark, redrives nothing.
        let sinks: Vec<Arc<dyn ProjectionSink>> = vec![Arc::new(rm.clone())];
        let checkpoint_backend: Arc<dyn CheckpointBackend> = Arc::new(be.checkpoints.clone());
        let store = Store::with_checkpoint_backend(
            Arc::new(be.events.clone()),
            Arc::new(be.cache.clone()),
            Vec::new(),
            sinks,
            StoreConfig::default(),
            checkpoint_backend,
        );
        let report = store.catch_up(rm.id()).await.unwrap();
        assert_eq!(report, CatchUpReport::EMPTY);

        be.driver.shutdown().await.unwrap();
    }
}

// ---- 6. field path validation ----------------------------------------------

#[tokio::test]
async fn invalid_field_path_is_rejected() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let rm = SqliteReadModel::new(be.isle());

    let err = rm
        .query(&Query {
            filter: Some(Filter::Eq(
                "a'; DROP TABLE read_model; --".to_string(),
                json!(1),
            )),
            ..Query::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Backend(_)));

    let err = rm.create_field_index("bad path!").await.unwrap_err();
    assert!(matches!(err, StoreError::Backend(_)));

    let err = rm
        .query(&Query {
            order_by: Some(("..bad..".to_string(), Order::Asc)),
            ..Query::default()
        })
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::Backend(_)));

    // The table is still intact — the rejected query above never reached the
    // backend.
    assert_eq!(rm.count(None, false).await.unwrap(), 0);

    be.driver.shutdown().await.unwrap();
}

// ---- 7. out-of-order redelivery does not roll back -------------------------

#[tokio::test]
async fn out_of_order_redelivery_does_not_roll_back_the_row() {
    let be = SqliteBackends::open_in_memory().await.unwrap();
    let rm = SqliteReadModel::new(be.isle());
    let s = StreamId::new("doc-1");

    let newer_event = Event {
        seq: Seq(5),
        kind: "created".to_string(),
        patch: patch(json!([])),
        meta: json!({}),
        at: Timestamp(5_000),
    };
    rm.commit(&s, Seq(5), &json!({ "n": 5 }), &newer_event)
        .await
        .unwrap();

    let older_event = Event {
        seq: Seq(2),
        kind: "created".to_string(),
        patch: patch(json!([])),
        meta: json!({}),
        at: Timestamp(2_000),
    };
    // Simulates a stale redelivery (e.g. a rebuild racing a live append).
    rm.commit(&s, Seq(2), &json!({ "n": 2 }), &older_event)
        .await
        .unwrap();

    let row = rm.get(&s).await.unwrap().unwrap();
    assert_eq!(row.last_seq, Seq(5));
    assert_eq!(row.state, json!({ "n": 5 }));
    assert_eq!(row.updated_at, Timestamp(5_000));

    // The same seq redelivered a second time is likewise a no-op.
    rm.commit(&s, Seq(5), &json!({ "n": 999 }), &newer_event)
        .await
        .unwrap();
    let row = rm.get(&s).await.unwrap().unwrap();
    assert_eq!(row.state, json!({ "n": 5 }));

    be.driver.shutdown().await.unwrap();
}

// ---- 8. migration lands read_model on an existing v2 database -------------

const V2_DDL: &str = r#"
    CREATE TABLE IF NOT EXISTS events (
        stream TEXT NOT NULL,
        seq    INTEGER NOT NULL,
        kind   TEXT NOT NULL,
        patch  TEXT NOT NULL,
        meta   TEXT NOT NULL,
        at_ms  INTEGER NOT NULL,
        PRIMARY KEY (stream, seq)
    );
    CREATE INDEX IF NOT EXISTS ix_events_stream_at ON events(stream, at_ms);

    CREATE TABLE IF NOT EXISTS labels (
        stream TEXT NOT NULL,
        name   TEXT NOT NULL,
        at_seq INTEGER NOT NULL,
        PRIMARY KEY (stream, name)
    );

    CREATE TABLE IF NOT EXISTS cache (
        stream TEXT NOT NULL,
        at_seq INTEGER NOT NULL,
        state  TEXT NOT NULL,
        PRIMARY KEY (stream, at_seq)
    );

    CREATE TABLE IF NOT EXISTS sink_checkpoints (
        sink_id TEXT NOT NULL,
        stream  TEXT NOT NULL,
        at_seq  INTEGER NOT NULL,
        PRIMARY KEY (sink_id, stream)
    );
"#;

#[tokio::test]
async fn opening_an_existing_v2_database_lands_the_read_model_table() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("v2.db");

    {
        // Simulate a database written by a pre-read-model `ai-store-sqlite`
        // (user_version = 2, migrations 1+2 already applied).
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch(V2_DDL).unwrap();
        conn.pragma_update(None, "user_version", 2i64).unwrap();
    }

    let be = SqliteBackends::open(&path).await.unwrap();
    let rm = SqliteReadModel::new(be.isle());
    // Would error (`no such table: read_model`) if migration 3 had not run.
    let rows = rm.query(&Query::default()).await.unwrap();
    assert!(rows.is_empty());

    be.driver.shutdown().await.unwrap();
}
