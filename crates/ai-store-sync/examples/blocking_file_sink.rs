//! # Sync file-writer as a `ProjectionSink`
//!
//! Canonical example for the `BlockingSink` adapter: an existing synchronous
//! routine that writes a materialized snapshot to disk. Consumers usually
//! already have this routine (config dumps, Markdown snapshots, log
//! rotation); reusing it via the adapter avoids reimplementing on
//! `tokio::fs` or hand-rolling `spawn_blocking`.
//!
//! Run with:
//!
//! ```sh
//! cargo run --example blocking_file_sink -p ai-store-sync
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use ai_store_core::{Event, Patch, Seq, Store, StoreConfig, StoreError, StreamId};
use ai_store_mem::{MemCacheBackend, MemEventBackend};
use ai_store_sync::{BlockingSink, SyncProjectionSink};
use serde_json::{json, Value};

/// Writes every committed state as pretty JSON into `<dir>/<stream>.json`.
/// Uses only the stdlib — no async I/O anywhere.
struct FileSnapshotSink {
    id: String,
    dir: PathBuf,
}

impl FileSnapshotSink {
    fn new(id: impl Into<String>, dir: PathBuf) -> Self {
        std::fs::create_dir_all(&dir).expect("mkdir");
        Self { id: id.into(), dir }
    }
    fn path_for(&self, stream: &StreamId) -> PathBuf {
        // Flatten `/` in stream ids to avoid deep dirs in the example.
        let safe = stream.as_str().replace('/', "__");
        self.dir.join(format!("{safe}.json"))
    }
}

impl SyncProjectionSink for FileSnapshotSink {
    fn id(&self) -> &str {
        &self.id
    }
    fn commit(
        &self,
        stream: &StreamId,
        _seq: Seq,
        state: &Value,
        _event: &Event,
    ) -> Result<(), StoreError> {
        let path = self.path_for(stream);
        let body = serde_json::to_string_pretty(state)
            .map_err(|e| StoreError::Backend(format!("serialize: {e}")))?;
        // `std::fs::write` is blocking; `spawn_blocking` (the default
        // dispatch mode of `BlockingSink::new`) keeps this off the async
        // worker.
        std::fs::write(&path, body)
            .map_err(|e| StoreError::Backend(format!("write {}: {e}", path.display())))?;
        Ok(())
    }
}

fn patch(v: Value) -> Patch {
    serde_json::from_value::<Patch>(v).unwrap()
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let tmp = tempfile::tempdir()?;
    let sink = BlockingSink::new(FileSnapshotSink::new(
        "file-snapshot",
        tmp.path().to_path_buf(),
    ));

    let store = Store::new(
        Arc::new(MemEventBackend::new()),
        Arc::new(MemCacheBackend::new()),
        Vec::new(),
        vec![Arc::new(sink)],
        StoreConfig::default(),
    );
    let s = StreamId::new("doc/hello");

    store
        .append(
            &s,
            "init",
            patch(json!([{ "op": "add", "path": "", "value": { "title": "hello", "n": 0 } }])),
            json!({}),
        )
        .await?;
    store
        .append(
            &s,
            "bump",
            patch(json!([{ "op": "replace", "path": "/n", "value": 3 }])),
            json!({}),
        )
        .await?;

    // Read the file back to prove the sync sink actually wrote it via the
    // async dispatch loop.
    let final_snapshot = std::fs::read_to_string(tmp.path().join("doc__hello.json"))?;
    println!("File contents:\n{final_snapshot}");

    let parsed: Value = serde_json::from_str(&final_snapshot)?;
    assert_eq!(parsed, json!({ "title": "hello", "n": 3 }));
    println!("Snapshot matches Store::state exactly.");
    Ok(())
}
