//! Tests for `FileProjection`.

use std::sync::Arc;

use ai_store_core::{Label, Patch, ProjectionSink, Seq, Store, StoreConfig, StreamId};
use ai_store_fileproj::FileProjection;
use ai_store_mem::{MemCacheBackend, MemEventBackend};
use serde_json::{json, Value};
use tempfile::TempDir;

fn patch(v: Value) -> Patch {
    serde_json::from_value::<Patch>(v).unwrap()
}

fn store_with_fileproj(root: &std::path::Path) -> (Store, Arc<FileProjection>) {
    let sink = Arc::new(FileProjection::with_json_pretty("fs", root));
    let store = Store::new(
        Arc::new(MemEventBackend::new()),
        Arc::new(MemCacheBackend::new()),
        Vec::new(),
        vec![sink.clone()],
        StoreConfig::default(),
    );
    (store, sink)
}

#[tokio::test]
async fn append_writes_draft_md_with_head_state() {
    let dir = TempDir::new().unwrap();
    let (store, _sink) = store_with_fileproj(dir.path());
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

    let draft_path = dir.path().join("doc").join("draft.md");
    let body = std::fs::read_to_string(&draft_path).unwrap();
    let parsed: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(parsed, json!({ "n": 2 }));
}

#[tokio::test]
async fn label_set_writes_label_md_with_pinned_state() {
    let dir = TempDir::new().unwrap();
    let (store, _sink) = store_with_fileproj(dir.path());
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

    store
        .label_set(&s, &Label::new("v1"), Seq(1))
        .await
        .unwrap();

    let v1_path = dir.path().join("doc").join("v1.md");
    let body = std::fs::read_to_string(&v1_path).unwrap();
    let parsed: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(parsed, json!({ "n": 1 }));

    // draft.md still reflects the head, not the label target.
    let draft_body = std::fs::read_to_string(dir.path().join("doc").join("draft.md")).unwrap();
    let draft_parsed: Value = serde_json::from_str(&draft_body).unwrap();
    assert_eq!(draft_parsed, json!({ "n": 2 }));
}

#[tokio::test]
async fn label_rewrite_archives_previous_file() {
    let dir = TempDir::new().unwrap();
    let (store, _sink) = store_with_fileproj(dir.path());
    let s = StreamId::new("doc");

    // Two events → pin v1 at seq 1 → later re-pin v1 at seq 2.
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

    store
        .label_set(&s, &Label::new("v1"), Seq(1))
        .await
        .unwrap();
    // Ensure the archive timestamp is distinguishable across the rewrite.
    std::thread::sleep(std::time::Duration::from_millis(3));
    store
        .label_set(&s, &Label::new("v1"), Seq(2))
        .await
        .unwrap();

    // Current v1.md holds the state at seq 2.
    let current: Value = serde_json::from_str(
        &std::fs::read_to_string(dir.path().join("doc").join("v1.md")).unwrap(),
    )
    .unwrap();
    assert_eq!(current, json!({ "n": 2 }));

    // Archive dir should contain exactly one archived v1 file with the old
    // state.
    let archive_dir = dir.path().join("doc").join("_archive");
    let mut archived_files: Vec<_> = std::fs::read_dir(&archive_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name().to_string_lossy().starts_with("v1.")
                && e.file_name().to_string_lossy().ends_with(".md")
        })
        .collect();
    assert_eq!(archived_files.len(), 1);
    let archived_path = archived_files.remove(0).path();
    let archived: Value =
        serde_json::from_str(&std::fs::read_to_string(&archived_path).unwrap()).unwrap();
    assert_eq!(archived, json!({ "n": 1 }));
}

#[tokio::test]
async fn stream_names_with_path_separators_are_rejected() {
    let dir = TempDir::new().unwrap();
    let sink = FileProjection::with_json_pretty("fs", dir.path());
    let bad = StreamId::new("evil/../escape");
    let state = json!({});
    let event = ai_store_core::Event {
        seq: Seq(1),
        kind: "x".into(),
        patch: serde_json::from_value(json!([])).unwrap(),
        meta: json!({}),
        at: ai_store_core::Timestamp(0),
    };
    let err = sink.commit(&bad, Seq(1), &state, &event).await.unwrap_err();
    assert!(matches!(err, ai_store_core::StoreError::Backend(_)));
    // No file should have been written under root.
    let entries: Vec<_> = std::fs::read_dir(dir.path()).unwrap().collect();
    assert!(entries.is_empty(), "root leaked entries: {entries:?}");
}

#[tokio::test]
async fn label_names_with_null_byte_are_rejected() {
    let dir = TempDir::new().unwrap();
    let sink = FileProjection::with_json_pretty("fs", dir.path());
    let s = StreamId::new("ok");
    let bad = Label::new("v\0evil");
    let event = ai_store_core::Event {
        seq: Seq(1),
        kind: "x".into(),
        patch: serde_json::from_value(json!([])).unwrap(),
        meta: json!({}),
        at: ai_store_core::Timestamp(0),
    };
    let err = sink
        .on_label_set(&s, &bad, Seq(1), &json!({}), &event)
        .await
        .unwrap_err();
    assert!(matches!(err, ai_store_core::StoreError::Backend(_)));
}

#[tokio::test]
async fn draft_write_is_atomic_no_partial_file_left() {
    // Even after many appends there should be no leftover `.partial` file.
    let dir = TempDir::new().unwrap();
    let (store, _sink) = store_with_fileproj(dir.path());
    let s = StreamId::new("doc");

    store
        .append(
            &s,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": { "count": 0 } }])),
            json!({}),
        )
        .await
        .unwrap();
    for i in 1..=5 {
        store
            .append(
                &s,
                "inc",
                patch(json!([{ "op": "replace", "path": "/count", "value": i }])),
                json!({}),
            )
            .await
            .unwrap();
    }

    let doc_dir = dir.path().join("doc");
    let entries: Vec<_> = std::fs::read_dir(&doc_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(entries.contains(&"draft.md".to_string()));
    for e in &entries {
        assert!(
            !e.ends_with(".partial"),
            "partial file was left behind: {e}"
        );
    }
}

#[tokio::test]
async fn label_delete_archives_existing_label_file_and_removes_it() {
    let dir = TempDir::new().unwrap();
    let (store, _sink) = store_with_fileproj(dir.path());
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
        .label_set(&s, &Label::new("v1"), Seq(1))
        .await
        .unwrap();

    let v1_path = dir.path().join("doc").join("v1.md");
    assert!(v1_path.exists());

    store.label_delete(&s, &Label::new("v1")).await.unwrap();

    // The current file is gone; its last rendered content survives in the
    // archive dir instead.
    assert!(!v1_path.exists());
    let archive_dir = dir.path().join("doc").join("_archive");
    let mut archived_files: Vec<_> = std::fs::read_dir(&archive_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name().to_string_lossy().starts_with("v1.")
                && e.file_name().to_string_lossy().ends_with(".md")
        })
        .collect();
    assert_eq!(archived_files.len(), 1);
    let archived_path = archived_files.remove(0).path();
    let archived: Value =
        serde_json::from_str(&std::fs::read_to_string(&archived_path).unwrap()).unwrap();
    assert_eq!(archived, json!({ "n": 1 }));
}

#[tokio::test]
async fn on_label_deleted_is_a_no_op_when_no_file_was_ever_written() {
    let dir = TempDir::new().unwrap();
    let sink = FileProjection::with_json_pretty("fs", dir.path());
    let s = StreamId::new("doc");

    // Calling the sink hook directly (bypassing the facade, which would
    // otherwise require the label to exist in the backend first) — proves
    // the sink itself tolerates a missing file.
    sink.on_label_deleted(&s, &Label::new("never-set"))
        .await
        .unwrap();

    // No stream dir, no archive dir — nothing was created.
    assert!(!dir.path().join("doc").exists());
}

#[tokio::test]
async fn catch_up_replays_events_and_reconciles_draft_md() {
    // Simulate a sink that only starts observing partway through: build a
    // store, append twice, THEN attach a fresh projection via rebuild.
    let dir = TempDir::new().unwrap();
    let events = Arc::new(MemEventBackend::new());
    let cache = Arc::new(MemCacheBackend::new());

    // Store 1: no sink, two appends.
    let store_no_sink = Store::new(
        events.clone(),
        cache.clone(),
        Vec::new(),
        Vec::new(),
        StoreConfig::default(),
    );
    let s = StreamId::new("doc");
    store_no_sink
        .append(
            &s,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": { "n": 1 } }])),
            json!({}),
        )
        .await
        .unwrap();
    store_no_sink
        .append(
            &s,
            "bump",
            patch(json!([{ "op": "replace", "path": "/n", "value": 5 }])),
            json!({}),
        )
        .await
        .unwrap();

    // Store 2: same backends, sink attached. rebuild drives it from Seq(0).
    let sink = Arc::new(FileProjection::with_json_pretty("fs", dir.path()));
    let store_with_sink = Store::new(
        events,
        cache,
        Vec::new(),
        vec![sink.clone()],
        StoreConfig::default(),
    );
    let report = store_with_sink.rebuild("fs").await.unwrap();
    assert_eq!(report.applied, 2);
    assert_eq!(report.failed, 0);

    let draft_body = std::fs::read_to_string(dir.path().join("doc").join("draft.md")).unwrap();
    let draft: Value = serde_json::from_str(&draft_body).unwrap();
    assert_eq!(draft, json!({ "n": 5 }));
}

// ---- label archive redelivery idempotence -------------------------------

/// Count archive files matching `<label>.<...>.md` in the stream's
/// `_archive/` directory. `0` when the directory does not exist yet.
fn count_archived(dir: &std::path::Path, stream: &str, label: &str) -> usize {
    let archive = dir.join(stream).join("_archive");
    match std::fs::read_dir(&archive) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .filter(|e| {
                let name = e.file_name().to_string_lossy().into_owned();
                name.starts_with(&format!("{label}.")) && name.ends_with(".md")
            })
            .count(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
        Err(e) => panic!("read {archive:?} failed: {e}"),
    }
}

#[tokio::test]
async fn redelivered_label_set_does_not_duplicate_archive_entries() {
    let dir = TempDir::new().unwrap();
    let (store, sink) = store_with_fileproj(dir.path());
    let s = StreamId::new("doc");

    // A single event, pinned as v1.
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
        .label_set(&s, &Label::new("v1"), Seq(1))
        .await
        .unwrap();

    // First delivery: no archive yet (nothing was there to archive).
    assert_eq!(count_archived(dir.path(), "doc", "v1"), 0);

    // Simulate a redelivery of the same label_set. `catch_up` /
    // `rebuild` re-drive sink notifications after a checkpoint reset
    // or a crash; nothing about the store state has changed, so the
    // sink is asked to render + archive the same content again.
    //
    // Fetch the event so we can call the sink method directly with the
    // same inputs it received the first time.
    let event = store.read(&s, Seq(1), 1).await.unwrap().remove(0);
    let state = store.state_at(&s, Seq(1)).await.unwrap();
    sink.on_label_set(&s, &Label::new("v1"), Seq(1), &state, &event)
        .await
        .unwrap();
    // Redelivery: no archive entry created, no duplicated content.
    assert_eq!(
        count_archived(dir.path(), "doc", "v1"),
        0,
        "redelivered label_set must not archive identical content"
    );

    // A THIRD delivery with the same content — again idempotent.
    sink.on_label_set(&s, &Label::new("v1"), Seq(1), &state, &event)
        .await
        .unwrap();
    assert_eq!(count_archived(dir.path(), "doc", "v1"), 0);

    // Now the state genuinely changes: a fresh append + re-pin, and
    // now the archive gets exactly one entry (the previous content).
    store
        .append(
            &s,
            "bump",
            patch(json!([{ "op": "replace", "path": "/n", "value": 2 }])),
            json!({}),
        )
        .await
        .unwrap();
    store
        .label_set(&s, &Label::new("v1"), Seq(2))
        .await
        .unwrap();
    assert_eq!(
        count_archived(dir.path(), "doc", "v1"),
        1,
        "changed content should archive exactly once"
    );

    // Redeliver *the new* label_set: still one archive entry.
    let event2 = store.read(&s, Seq(2), 1).await.unwrap().remove(0);
    let state2 = store.state_at(&s, Seq(2)).await.unwrap();
    sink.on_label_set(&s, &Label::new("v1"), Seq(2), &state2, &event2)
        .await
        .unwrap();
    assert_eq!(
        count_archived(dir.path(), "doc", "v1"),
        1,
        "redelivered changed label_set must not double-archive"
    );

    // Current v1.md reflects the latest state.
    let current: Value = serde_json::from_str(
        &std::fs::read_to_string(dir.path().join("doc").join("v1.md")).unwrap(),
    )
    .unwrap();
    assert_eq!(current, json!({ "n": 2 }));
}
