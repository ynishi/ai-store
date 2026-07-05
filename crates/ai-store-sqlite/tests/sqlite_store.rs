//! Tests for the `SqliteStore` one-shot assembly (open -> Deref<Target =
//! Store> -> shutdown), including `open_with`'s builder callback and
//! `read_model`'s direct-query behavior.

use std::sync::Arc;

use ai_store_core::{
    GateCtx, ProjectionSink, SchemaGate, SchemaViolation, Seq, StoreError, StreamId,
};
use ai_store_sqlite::{Query, SqliteStore};
use serde_json::json;
use tempfile::TempDir;

fn patch(v: serde_json::Value) -> json_patch::Patch {
    serde_json::from_value(v).unwrap()
}

#[tokio::test]
async fn open_in_memory_builds_a_usable_store_via_deref() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    let s = StreamId::new("doc");

    // `SqliteStore` derefs to `Store` — no `.store()` call needed for
    // ordinary facade methods.
    store
        .append(
            &s,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": { "n": 1 } }])),
            json!({}),
        )
        .await
        .unwrap();

    assert_eq!(store.state(&s).await.unwrap(), json!({ "n": 1 }));
    assert_eq!(store.head(&s).await.unwrap(), Some(Seq(1)));

    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn open_persists_across_reopen_with_durable_checkpoints() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("store.db");
    let s = StreamId::new("doc");

    {
        let store = SqliteStore::open(&path).await.unwrap();
        store
            .append(
                &s,
                "init",
                patch(json!([{ "op": "add", "path": "", "value": { "n": 0 } }])),
                json!({}),
            )
            .await
            .unwrap();
        store.shutdown().await.unwrap();
    }

    {
        let store = SqliteStore::open(&path).await.unwrap();
        assert_eq!(store.state(&s).await.unwrap(), json!({ "n": 0 }));
        assert_eq!(store.head(&s).await.unwrap(), Some(Seq(1)));
        store.shutdown().await.unwrap();
    }
}

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
async fn open_with_extends_the_builder_with_a_gate() {
    let store = SqliteStore::open_in_memory_with(|builder| {
        builder.gate(Arc::new(RejectKind {
            forbidden: "denied",
        }))
    })
    .await
    .unwrap();
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
    assert_eq!(store.head(&s).await.unwrap(), Some(Seq(1)));

    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn read_model_shares_the_sqlite_thread_and_answers_direct_queries_after_commit() {
    let store = SqliteStore::open_in_memory().await.unwrap();
    let s = StreamId::new("doc");

    let committed = store
        .append(
            &s,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": { "owner": "alice" } }])),
            json!({}),
        )
        .await
        .unwrap();

    let read_model = store.read_model();

    // Not an automatic sink: no row exists until explicitly driven.
    assert!(read_model.get(&s).await.unwrap().is_none());

    // Drive it explicitly via `ProjectionSink::commit` — the documented
    // escape hatch for a read model built after the store already exists.
    // Succeeding here also proves `read_model` shares *this* store's own
    // SQLite thread (there is only ever one `events`/`cache` table per
    // in-memory database; a mismatched thread would see an empty schema).
    let state = store.state(&s).await.unwrap();
    let event = store.read(&s, committed.seq, 1).await.unwrap().remove(0);
    read_model
        .commit(&s, committed.seq, &state, &event)
        .await
        .unwrap();

    let row = read_model.get(&s).await.unwrap().unwrap();
    assert_eq!(row.state, json!({ "owner": "alice" }));

    let hits = read_model
        .query(&Query {
            filter: Some(ai_store_sqlite::Filter::Eq(
                "owner".to_string(),
                json!("alice"),
            )),
            ..Query::default()
        })
        .await
        .unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].stream, s);

    store.shutdown().await.unwrap();
}
