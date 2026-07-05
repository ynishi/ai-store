//! Tests for `CombinedFileSink`.

use std::collections::BTreeMap;
use std::sync::Arc;

use ai_store_core::{Event, Patch, ProjectionSink, Seq, Store, StoreConfig, StreamId, Timestamp};
use ai_store_fileproj::{CombinedFileSink, Renderer};
use ai_store_mem::{MemCacheBackend, MemEventBackend};
use serde_json::{json, Value};
use tempfile::TempDir;

fn patch(v: Value) -> Patch {
    serde_json::from_value::<Patch>(v).unwrap()
}

/// Renders every stream as a `## <stream>\n<compact-json>\n\n` section, in
/// whatever order the caller iterates the map (a `BTreeMap` iterates in key
/// order, i.e. dictionary order over `StreamId`'s inner string).
fn renderer() -> Renderer {
    Arc::new(|streams: &BTreeMap<StreamId, Value>| {
        let mut out = String::new();
        for (stream, state) in streams {
            out.push_str(&format!(
                "## {}\n{}\n\n",
                stream.as_str(),
                serde_json::to_string(state).unwrap()
            ));
        }
        out
    })
}

fn fixed_event(seq: Seq) -> Event {
    Event {
        seq,
        kind: "k".into(),
        patch: serde_json::from_value(json!([])).unwrap(),
        meta: json!({}),
        at: Timestamp(0),
    }
}

#[tokio::test]
async fn catch_up_renders_all_streams_into_one_combined_file() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("combined.md");
    let events = Arc::new(MemEventBackend::new());
    let cache = Arc::new(MemCacheBackend::new());

    // Build history on two streams with no sink attached, THEN attach a
    // fresh combined sink via rebuild — mirrors the FileProjection
    // catch_up test, but here a single sink must fold both streams into
    // one file.
    let store_no_sink = Store::new(
        events.clone(),
        cache.clone(),
        Vec::new(),
        Vec::new(),
        StoreConfig::default(),
    );
    let alpha = StreamId::new("alpha");
    let beta = StreamId::new("beta");
    store_no_sink
        .append(
            &alpha,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": { "n": 1 } }])),
            json!({}),
        )
        .await
        .unwrap();
    store_no_sink
        .append(
            &beta,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": { "n": 2 } }])),
            json!({}),
        )
        .await
        .unwrap();

    let sink = Arc::new(CombinedFileSink::new("combined", path.clone(), renderer()));
    let store_with_sink = Store::new(
        events,
        cache,
        Vec::new(),
        vec![sink.clone()],
        StoreConfig::default(),
    );
    let report = store_with_sink.rebuild("combined").await.unwrap();
    assert_eq!(report.applied, 2);
    assert_eq!(report.failed, 0);

    let body = std::fs::read_to_string(&path).unwrap();
    assert!(body.contains("## alpha"));
    assert!(body.contains("## beta"));
    assert!(body.contains(r#"{"n":1}"#));
    assert!(body.contains(r#"{"n":2}"#));
    // BTreeMap<StreamId, _> iterates in dictionary order: "alpha" < "beta".
    assert!(body.find("## alpha").unwrap() < body.find("## beta").unwrap());
}

#[tokio::test]
async fn identical_re_commit_does_not_rewrite_the_file() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("combined.md");
    let sink = CombinedFileSink::new("combined", path.clone(), renderer());
    let s = StreamId::new("doc");
    let event = fixed_event(Seq(1));

    sink.commit(&s, Seq(1), &json!({ "n": 1 }), &event)
        .await
        .unwrap();
    let mtime1 = std::fs::metadata(&path).unwrap().modified().unwrap();

    // Distinguishable across a real rewrite on any filesystem's mtime
    // resolution.
    std::thread::sleep(std::time::Duration::from_millis(20));

    // Same stream, same state -> the rendered body is byte-identical ->
    // the write is skipped rather than repeated.
    sink.commit(&s, Seq(1), &json!({ "n": 1 }), &event)
        .await
        .unwrap();
    let mtime2 = std::fs::metadata(&path).unwrap().modified().unwrap();

    assert_eq!(
        mtime1, mtime2,
        "identical rendered content must not trigger a rewrite"
    );
}

#[tokio::test]
async fn changed_content_does_rewrite_the_file() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("combined.md");
    let sink = CombinedFileSink::new("combined", path.clone(), renderer());
    let s = StreamId::new("doc");

    sink.commit(&s, Seq(1), &json!({ "n": 1 }), &fixed_event(Seq(1)))
        .await
        .unwrap();
    let mtime1 = std::fs::metadata(&path).unwrap().modified().unwrap();

    std::thread::sleep(std::time::Duration::from_millis(20));

    // Different state -> different rendered body -> the write proceeds
    // (this is the control case proving the previous test's skip is real,
    // not just "commit is a no-op after the first call").
    sink.commit(&s, Seq(2), &json!({ "n": 2 }), &fixed_event(Seq(2)))
        .await
        .unwrap();
    let mtime2 = std::fs::metadata(&path).unwrap().modified().unwrap();

    assert_ne!(mtime1, mtime2, "changed content must trigger a rewrite");
    let body = std::fs::read_to_string(&path).unwrap();
    assert!(body.contains(r#"{"n":2}"#));
}

#[tokio::test]
async fn combined_write_is_atomic_and_content_reflects_every_stream() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("combined.md");
    let sink = CombinedFileSink::new("combined", path.clone(), renderer());
    let alpha = StreamId::new("alpha");
    let beta = StreamId::new("beta");

    for i in 0..5u64 {
        sink.commit(
            &alpha,
            Seq(i + 1),
            &json!({ "n": i }),
            &fixed_event(Seq(i + 1)),
        )
        .await
        .unwrap();
        sink.commit(
            &beta,
            Seq(i + 1),
            &json!({ "n": i * 10 }),
            &fixed_event(Seq(i + 1)),
        )
        .await
        .unwrap();
    }

    // No `.partial` sibling left behind by the atomic rename.
    let entries: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    assert!(entries.contains(&"combined.md".to_string()));
    assert!(
        entries.iter().all(|e| !e.ends_with(".partial")),
        "partial file was left behind: {entries:?}"
    );

    // The last-written body reflects the final state of *both* streams —
    // proof the composed render is not truncated mid-write.
    let body = std::fs::read_to_string(&path).unwrap();
    assert!(body.contains(r#"{"n":4}"#));
    assert!(body.contains(r#"{"n":40}"#));
}
