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

// ---- restart-cold auto-escalation ---------------------------------------

/// A `CheckpointBackend` that remembers checkpoints across `Store`
/// instances, simulating a restart where the checkpoint table survived
/// (e.g. `SqliteCheckpointBackend`) but every in-memory sink is fresh.
#[derive(Default, Clone)]
struct SharedCheckpoints(
    Arc<std::sync::Mutex<std::collections::HashMap<(String, StreamId), Seq>>>,
);

#[async_trait::async_trait]
impl ai_store_core::CheckpointBackend for SharedCheckpoints {
    async fn get(
        &self,
        sink_id: &str,
        stream: &StreamId,
    ) -> Result<Option<Seq>, ai_store_core::StoreError> {
        let map = self.0.lock().unwrap();
        Ok(map.get(&(sink_id.to_string(), stream.clone())).copied())
    }
    async fn put(
        &self,
        sink_id: &str,
        stream: &StreamId,
        at: Seq,
    ) -> Result<(), ai_store_core::StoreError> {
        self.0
            .lock()
            .unwrap()
            .insert((sink_id.to_string(), stream.clone()), at);
        Ok(())
    }
}

#[tokio::test]
async fn restart_cold_catch_up_auto_escalates_to_rebuild_for_combined_sink() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("combined.md");
    let events: Arc<dyn ai_store_core::EventBackend> = Arc::new(MemEventBackend::new());
    let cache: Arc<dyn ai_store_core::CacheBackend> = Arc::new(MemCacheBackend::new());
    let checkpoints: Arc<dyn ai_store_core::CheckpointBackend> =
        Arc::new(SharedCheckpoints::default());

    let alpha = StreamId::new("alpha");
    let beta = StreamId::new("beta");

    // ---- Pre-restart process: sink attached, writes land in the file ---
    {
        let sink = Arc::new(CombinedFileSink::new("combined", path.clone(), renderer()));
        let store = Store::with_checkpoint_backend(
            events.clone(),
            cache.clone(),
            Vec::new(),
            vec![sink.clone()],
            StoreConfig::default(),
            checkpoints.clone(),
        );
        store
            .append(
                &alpha,
                "init",
                patch(json!([{ "op": "add", "path": "", "value": { "n": 1 } }])),
                json!({}),
            )
            .await
            .unwrap();
        store
            .append(
                &beta,
                "init",
                patch(json!([{ "op": "add", "path": "", "value": { "n": 2 } }])),
                json!({}),
            )
            .await
            .unwrap();

        let body = std::fs::read_to_string(&path).unwrap();
        assert!(body.contains("## alpha"));
        assert!(body.contains("## beta"));
    }

    // ---- Post-restart process: same checkpoints, but a fresh sink -----
    // Simulates a restart: the same MemEventBackend + MemCacheBackend
    // survive (the log is durable at this layer), the SharedCheckpoints
    // survive (persisted watermark), but the sink is a brand-new
    // CombinedFileSink whose in-memory BTreeMap starts empty.
    //
    // If catch_up were to resume from the persisted checkpoints without
    // escalation, it would find nothing new to dispatch (checkpoints are
    // already at head) and never re-populate the map. The very next
    // append on a single stream would then re-render the file with only
    // that stream visible — the "silent truncation on restart" failure
    // mode this issue closes.
    {
        let sink = Arc::new(CombinedFileSink::new("combined", path.clone(), renderer()));
        let store = Store::with_checkpoint_backend(
            events.clone(),
            cache.clone(),
            Vec::new(),
            vec![sink.clone()],
            StoreConfig::default(),
            checkpoints.clone(),
        );

        // A plain `catch_up` here MUST auto-escalate to rebuild, because
        // the sink declares `requires_rebuild_on_attach() -> true`.
        let report = store.catch_up("combined").await.unwrap();
        assert_eq!(report.applied, 2, "auto-rebuild must re-drive every event");
        assert_eq!(report.failed, 0);

        // The file still contains both streams — the pre-restart snapshot
        // is intact, and the escalated rebuild's re-render matches it.
        let body = std::fs::read_to_string(&path).unwrap();
        assert!(body.contains("## alpha"));
        assert!(body.contains("## beta"));

        // A follow-up `catch_up` in the same process is NOT re-escalated:
        // it finds nothing new (both streams are at head) and applies 0.
        let follow_up = store.catch_up("combined").await.unwrap();
        assert_eq!(follow_up.applied, 0);

        // And an ordinary append to just one stream still preserves the
        // other stream's contribution — the map has been repopulated by
        // the first catch_up, so this render includes both.
        store
            .append(
                &alpha,
                "bump",
                patch(json!([{ "op": "replace", "path": "/n", "value": 10 }])),
                json!({}),
            )
            .await
            .unwrap();
        let body = std::fs::read_to_string(&path).unwrap();
        assert!(body.contains("## alpha"));
        assert!(body.contains("## beta"), "beta must remain in the file");
        assert!(body.contains(r#"{"n":10}"#));
    }
}
