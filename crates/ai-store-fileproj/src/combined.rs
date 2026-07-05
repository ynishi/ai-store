//! `CombinedFileSink` — a `ProjectionSink` that composes every stream it
//! observes into a single rendered file.
//!
//! `FileProjection` (the sibling in this crate) is one-stream-per-directory:
//! every stream gets its own `draft.md` / `<label>.md` set. That shape does
//! not fit a consumer whose read side is "one file, every stream's current
//! state folded into it" — e.g. journal-mcp's canonical history file, where
//! every chapter (stream) contributes a section to one `journal.md` in a
//! fixed (dictionary) order. `CombinedFileSink` fills that gap: it keeps an
//! in-memory `BTreeMap<StreamId, Value>` of the latest state per stream,
//! hands the whole map to a caller-supplied [`Renderer`] on every commit, and
//! writes the result to one fixed `path`.

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;

use ai_store_core::{Event, ProjectionSink, Seq, StoreError, StreamId};
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::Mutex;

use crate::projection::write_atomic;

/// Render function type for [`CombinedFileSink`]: turn the full multi-stream
/// snapshot into the file body that will be written to disk.
///
/// The map is keyed by [`StreamId`], which derives `Ord` lexicographically
/// over its inner string — iterating a `BTreeMap<StreamId, _>` therefore
/// yields streams in a stable dictionary order without the renderer having
/// to sort anything itself.
pub type Renderer = Arc<dyn Fn(&BTreeMap<StreamId, Value>) -> String + Send + Sync>;

/// In-memory snapshot plus the hash of the last body actually written to
/// disk, guarded together so a commit's "update snapshot, render, maybe
/// write" sequence is atomic with respect to concurrent commits.
struct Snapshot {
    streams: BTreeMap<StreamId, Value>,
    /// Hash of the last body written to `path`. `None` until the first
    /// write. Not a content-integrity check (a `DefaultHasher` collision
    /// would just cause one redundant write, never data loss) — purely a
    /// cheap way to skip a write when the rendered output has not changed
    /// since last time.
    last_written_hash: Option<u64>,
}

/// A `ProjectionSink` that folds every stream it observes into one file.
///
/// ## Contract
///
/// - Idempotent: replaying the same `(stream, seq)` re-renders the same
///   snapshot into the same body, and a same-content re-write is skipped
///   (see "Write skipping" below) — safe for `Store::catch_up` /
///   `Store::rebuild` to re-drive after a crash.
/// - Accepts every stream by default (`ProjectionSink::accepts` is not
///   overridden) — this sink's entire purpose is to fold every stream's
///   state into one file, so scoping it to a subset defeats the point.
///   Callers that need a filtered combined view should compose their own
///   `ProjectionSink::accepts` override on top, or wrap a caller-provided
///   filter into the `Renderer` itself.
/// - Restart-cold: the `BTreeMap` snapshot lives only in process memory (like
///   `FileProjection`'s draft, but unlike that per-stream layout there is no
///   way to reconstruct just the *changed* stream from disk — the file on
///   disk has already folded every stream together and lost the per-stream
///   boundary). This sink overrides
///   [`ProjectionSink::requires_rebuild_on_attach`] to `true`, so the
///   `Store` automatically escalates the first `catch_up(sink_id)` in a
///   fresh process to a `rebuild`: every stream is re-driven from
///   `Seq::ZERO` before the map is trusted. Callers do not need to
///   remember to invoke `rebuild` themselves.
///
/// ## Write skipping
///
/// Every `commit` re-renders the *entire* map (not just the changed
/// stream), even though only one stream's entry moved — the renderer is a
/// caller-supplied closure over the whole map, so there is no cheaper
/// partial-render path available generically. What is avoided is the file
/// *write*: the rendered body is hashed (a plain `DefaultHasher`, not a
/// cryptographic digest — this is purely a duplicate-write filter, not an
/// integrity check) and compared against the hash of the last body actually
/// written; an unchanged render is skipped rather than rewritten verbatim.
/// This matters when several streams' events land close together and each
/// commit would otherwise re-write an unchanged file.
///
/// Writes go through the same temp-sibling-then-rename as `FileProjection`,
/// so a concurrent reader of `path` never observes a partial write.
#[derive(Clone)]
pub struct CombinedFileSink {
    id: String,
    path: PathBuf,
    renderer: Renderer,
    snapshot: Arc<Mutex<Snapshot>>,
}

impl CombinedFileSink {
    /// Create a new combined sink writing to `path`, rendered by `renderer`.
    ///
    /// The `id` is used as the checkpoint key on the facade and should be
    /// stable across restarts. The in-memory snapshot starts empty — see
    /// the "Restart-cold" note on [`CombinedFileSink`] for why a fresh
    /// process should `Store::rebuild` this sink's id rather than trust a
    /// pre-restart `catch_up` checkpoint alone.
    pub fn new(id: impl Into<String>, path: impl Into<PathBuf>, renderer: Renderer) -> Self {
        Self {
            id: id.into(),
            path: path.into(),
            renderer,
            snapshot: Arc::new(Mutex::new(Snapshot {
                streams: BTreeMap::new(),
                last_written_hash: None,
            })),
        }
    }
}

#[async_trait]
impl ProjectionSink for CombinedFileSink {
    fn id(&self) -> &str {
        &self.id
    }

    /// The in-memory `BTreeMap` snapshot is process-local: after a restart
    /// there is no way to reconstruct the missing streams' entries just
    /// from the file on disk, because the file has already been folded
    /// through the renderer and lost the per-stream boundary. Signal this
    /// to the store so a fresh-process `catch_up` gets escalated to a
    /// `rebuild` (re-driving every stream from `Seq::ZERO`), closing the
    /// "silent truncation on restart-then-catch-up" failure mode at the
    /// facade layer.
    fn requires_rebuild_on_attach(&self) -> bool {
        true
    }

    async fn commit(
        &self,
        stream: &StreamId,
        _seq: Seq,
        state: &Value,
        _event: &Event,
    ) -> Result<(), StoreError> {
        let mut snapshot = self.snapshot.lock().await;
        snapshot.streams.insert(stream.clone(), state.clone());

        let body = (self.renderer)(&snapshot.streams);
        let mut hasher = DefaultHasher::new();
        body.hash(&mut hasher);
        let hash = hasher.finish();

        if snapshot.last_written_hash == Some(hash) {
            // Rendered output is unchanged since the last write — skip the
            // write entirely, but the in-memory snapshot above is still
            // updated so the next differing commit renders from a
            // complete, current map.
            return Ok(());
        }

        write_atomic(&self.path, &body).await?;
        snapshot.last_written_hash = Some(hash);
        Ok(())
    }
}
