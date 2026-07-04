//! Facade-level tests running against `SqliteBackends`.
//!
//! These check that `Store` composed with `SqliteEventBackend` +
//! `SqliteCacheBackend` upholds the same public contract as the in-memory
//! backend, and that durability across reopen actually works (the property
//! the memory backend cannot exercise).

use std::sync::Arc;

use ai_store_core::{Patch, Seq, Store, StoreConfig, StreamId};
use ai_store_sqlite::SqliteBackends;
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
