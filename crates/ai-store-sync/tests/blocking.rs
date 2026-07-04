//! Integration tests for `BlockingStore`. Cover the async ↔ sync boundary
//! from both directions: (a) a plain synchronous caller using
//! `BlockingStore::new`, and (b) an existing tokio runtime handing a `Handle`
//! to `BlockingStore::with_handle`.

use std::sync::Arc;

use ai_store_core::{Patch, Seq, Store, StoreConfig, StreamId};
use ai_store_mem::{MemCacheBackend, MemEventBackend};
use ai_store_sync::BlockingStore;
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
    assert_eq!(seq1, Seq(1));

    let seq2 = bs
        .append(
            &s,
            "bump",
            patch(json!([{ "op": "replace", "path": "/n", "value": 1 }])),
            json!({}),
        )
        .unwrap();
    assert_eq!(seq2, Seq(2));

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
    assert_eq!(seq_reverted, Seq(3));
    assert_eq!(bs.state(&s).unwrap(), json!({ "n": 0 }));
    assert_eq!(bs.head(&s).unwrap(), Some(Seq(3)));
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
