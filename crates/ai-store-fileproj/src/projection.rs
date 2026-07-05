//! `FileProjection` implementation of `ProjectionSink`.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ai_store_core::{Event, Label, ProjectionSink, Seq, StoreError, StreamId, Timestamp};
use async_trait::async_trait;
use serde_json::Value;
use tokio::fs;

/// Render function type: turn the materialized `Value` state into the file
/// body that will be written to disk. Consumers plug in whatever formatter
/// makes sense for their domain (Markdown, JSON pretty-print, plain text).
pub type RenderFn = Arc<dyn Fn(&Value) -> String + Send + Sync>;

/// A `ProjectionSink` that writes stream state to `.md` files.
///
/// Cloneable — the sink is holds an `Arc` to the render function so multiple
/// consumers can share one projection.
#[derive(Clone)]
pub struct FileProjection {
    id: String,
    root: PathBuf,
    render: RenderFn,
}

impl FileProjection {
    /// Create a new projection with the given id, root directory, and
    /// render function.
    ///
    /// The `id` is used as the checkpoint key on the facade and should be
    /// stable across restarts.
    pub fn new(
        id: impl Into<String>,
        root: impl Into<PathBuf>,
        render: impl Fn(&Value) -> String + Send + Sync + 'static,
    ) -> Self {
        Self {
            id: id.into(),
            root: root.into(),
            render: Arc::new(render),
        }
    }

    /// Convenience constructor that pretty-prints the state as JSON.
    ///
    /// Useful when the consumer's state is already document-shaped and you
    /// just want a readable dump on disk.
    pub fn with_json_pretty(id: impl Into<String>, root: impl Into<PathBuf>) -> Self {
        Self::new(id, root, |v| {
            serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
        })
    }

    /// The directory (under `root`) where files for `stream` live.
    fn stream_dir(&self, stream: &StreamId) -> Result<PathBuf, StoreError> {
        let slug = sanitize_component(stream.as_str(), "stream")?;
        Ok(self.root.join(slug))
    }

    /// Move `target` (the current rendered file for `label_slug`, if it
    /// exists) into `<dir>/_archive/<label_slug>.<epoch-ms>.md`. A no-op
    /// when `target` does not exist.
    ///
    /// Shared by `on_label_set` (archive-then-rewrite) and `on_label_deleted`
    /// (archive-only — the store's label index is the source of truth for
    /// whether a label exists, so the sink preserves the last rendered
    /// snapshot rather than deleting it outright).
    async fn archive_if_exists(
        &self,
        dir: &Path,
        label_slug: &str,
        target: &Path,
    ) -> Result<(), StoreError> {
        if fs::try_exists(target)
            .await
            .map_err(|e| StoreError::Backend(format!("exists {target:?}: {e}")))?
        {
            let archive_dir = dir.join("_archive");
            fs::create_dir_all(&archive_dir)
                .await
                .map_err(|e| StoreError::Backend(format!("mkdir {archive_dir:?}: {e}")))?;
            let now = Timestamp::now().0;
            let archived = archive_dir.join(format!("{label_slug}.{now}.md"));
            fs::rename(target, &archived).await.map_err(|e| {
                StoreError::Backend(format!("archive {target:?} -> {archived:?}: {e}"))
            })?;
        }
        Ok(())
    }
}

/// Reject path components that could escape the projection root or produce
/// nonsense filenames on any of the platforms we target.
fn sanitize_component(raw: &str, kind: &str) -> Result<String, StoreError> {
    if raw.is_empty() {
        return Err(StoreError::Backend(format!("empty {kind} name")));
    }
    if raw == "." || raw == ".." {
        return Err(StoreError::Backend(format!(
            "reserved {kind} name: {raw:?}"
        )));
    }
    for ch in raw.chars() {
        match ch {
            '/' | '\\' | '\0' => {
                return Err(StoreError::Backend(format!(
                    "invalid character in {kind} name: {raw:?}"
                )))
            }
            _ => {}
        }
    }
    Ok(raw.to_string())
}

/// Read `path`'s contents as a `String` if the file exists, or return
/// `Ok(None)` if it does not. Used by the label-set idempotence path to
/// decide whether a re-render would change on-disk content before archiving
/// and rewriting.
async fn read_if_exists(path: &Path) -> Result<Option<String>, StoreError> {
    match fs::read_to_string(path).await {
        Ok(body) => Ok(Some(body)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(StoreError::Backend(format!("read {path:?}: {e}"))),
    }
}

/// Write `body` to `path` via a temp-sibling-then-rename, so a partial write
/// is never observed by a concurrent reader. Shared with
/// [`crate::CombinedFileSink`], which writes one composed file instead of
/// one file per stream but needs the same crash-safety property.
pub(crate) async fn write_atomic(path: &Path, body: &str) -> Result<(), StoreError> {
    // Write to a temp sibling then rename, so a partial write is never
    // observed by concurrent readers of the projection tree.
    let mut tmp = path.to_path_buf();
    let mut tmp_name = tmp
        .file_name()
        .map(|s| s.to_os_string())
        .ok_or_else(|| StoreError::Backend("target path has no filename".to_string()))?;
    tmp_name.push(".partial");
    tmp.set_file_name(tmp_name);

    fs::write(&tmp, body)
        .await
        .map_err(|e| StoreError::Backend(format!("write {tmp:?}: {e}")))?;
    fs::rename(&tmp, path)
        .await
        .map_err(|e| StoreError::Backend(format!("rename {tmp:?} -> {path:?}: {e}")))?;
    Ok(())
}

#[async_trait]
impl ProjectionSink for FileProjection {
    fn id(&self) -> &str {
        &self.id
    }

    async fn commit(
        &self,
        stream: &StreamId,
        _seq: Seq,
        state: &Value,
        _event: &Event,
    ) -> Result<(), StoreError> {
        let dir = self.stream_dir(stream)?;
        fs::create_dir_all(&dir)
            .await
            .map_err(|e| StoreError::Backend(format!("mkdir {dir:?}: {e}")))?;
        let target = dir.join("draft.md");
        let body = (self.render)(state);
        write_atomic(&target, &body).await
    }

    async fn on_label_set(
        &self,
        stream: &StreamId,
        label: &Label,
        _at: Seq,
        state: &Value,
        _event: &Event,
    ) -> Result<(), StoreError> {
        let label_slug = sanitize_component(label.as_str(), "label")?;
        let dir = self.stream_dir(stream)?;
        fs::create_dir_all(&dir)
            .await
            .map_err(|e| StoreError::Backend(format!("mkdir {dir:?}: {e}")))?;

        let target = dir.join(format!("{label_slug}.md"));
        let body = (self.render)(state);

        // Redelivery-idempotence: sink notifications can be re-emitted
        // (checkpoint advance is best-effort, `catch_up` re-drives failed
        // dispatches, an explicit `rebuild` re-plays the whole log). If
        // the existing target already matches the new render, both the
        // archive-and-rewrite steps are no-ops — running them anyway
        // would create an `_archive/<label>.<epoch-ms>.md` copy of
        // identical content on every redelivery, filling the archive dir
        // with byte-for-byte duplicates.
        //
        // Read + compare on the "same content" path costs one extra file
        // read per label_set; on the "changed content" path it costs one
        // extra file read before the write it was already going to do,
        // amortized against a rare event (labels move slowly relative to
        // ordinary appends).
        if let Some(existing) = read_if_exists(&target).await? {
            if existing == body {
                return Ok(());
            }
        }

        // Content differs from the on-disk snapshot: archive the previous
        // rendering before overwriting.
        self.archive_if_exists(&dir, &label_slug, &target).await?;
        write_atomic(&target, &body).await
    }

    async fn on_label_deleted(&self, stream: &StreamId, label: &Label) -> Result<(), StoreError> {
        let label_slug = sanitize_component(label.as_str(), "label")?;
        let dir = self.stream_dir(stream)?;
        let target = dir.join(format!("{label_slug}.md"));
        self.archive_if_exists(&dir, &label_slug, &target).await
    }
}
