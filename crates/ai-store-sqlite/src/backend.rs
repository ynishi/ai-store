//! SQLite `EventBackend` and `CacheBackend` implementations.
//!
//! Both handles share a single `AsyncIsle` — the same SQLite thread — so
//! appends and cache writes are serialized by construction. All mutations
//! run in one closure and use a `BEGIN IMMEDIATE ... COMMIT` transaction for
//! atomicity; row-format conversions live in `row_to_event`.

use ai_store_core::Patch;
use ai_store_core::{
    CacheBackend, CheckpointBackend, Committed, Event, EventBackend, Label, NewEvent, Seq,
    SqliteBackend, StoreError, StreamId, Timestamp,
};
use async_trait::async_trait;
use rusqlite::{params, OptionalExtension};
use rusqlite_isle::AsyncIsle;
use serde_json::Value;

/// SQLite-backed `EventBackend`. Cloneable; every clone shares the same
/// SQLite thread.
#[derive(Clone)]
pub struct SqliteEventBackend {
    isle: AsyncIsle,
}

impl SqliteEventBackend {
    /// Build from an existing rusqlite-isle handle.
    pub fn new(isle: AsyncIsle) -> Self {
        Self { isle }
    }
}

impl SqliteBackend for SqliteEventBackend {
    type Handle = AsyncIsle;

    fn new(handle: AsyncIsle) -> Self {
        SqliteEventBackend::new(handle)
    }
}

/// SQLite-backed `CacheBackend`. Cloneable; every clone shares the same
/// SQLite thread.
#[derive(Clone)]
pub struct SqliteCacheBackend {
    isle: AsyncIsle,
}

impl SqliteCacheBackend {
    /// Build from an existing rusqlite-isle handle.
    pub fn new(isle: AsyncIsle) -> Self {
        Self { isle }
    }
}

impl SqliteBackend for SqliteCacheBackend {
    type Handle = AsyncIsle;

    fn new(handle: AsyncIsle) -> Self {
        SqliteCacheBackend::new(handle)
    }
}

/// SQLite-backed `CheckpointBackend`. Cloneable; every clone shares the
/// same SQLite thread.
#[derive(Clone)]
pub struct SqliteCheckpointBackend {
    isle: AsyncIsle,
}

impl SqliteCheckpointBackend {
    /// Build from an existing rusqlite-isle handle.
    pub fn new(isle: AsyncIsle) -> Self {
        Self { isle }
    }
}

impl SqliteBackend for SqliteCheckpointBackend {
    type Handle = AsyncIsle;

    fn new(handle: AsyncIsle) -> Self {
        SqliteCheckpointBackend::new(handle)
    }
}

fn to_store_err<E: std::fmt::Display>(e: E) -> StoreError {
    StoreError::Backend(e.to_string())
}

fn row_to_event(
    seq: u64,
    kind: String,
    patch_json: String,
    meta_json: String,
    at_ms: i64,
) -> Result<Event, StoreError> {
    let patch: Patch = serde_json::from_str(&patch_json)
        .map_err(|e| StoreError::Backend(format!("patch decode: {e}")))?;
    let meta: Value = serde_json::from_str(&meta_json)
        .map_err(|e| StoreError::Backend(format!("meta decode: {e}")))?;
    Ok(Event {
        seq: Seq(seq),
        kind,
        patch,
        meta,
        at: Timestamp(at_ms),
    })
}

impl SqliteEventBackend {
    /// Shared append path: assigns the next gap-free monotonic `Seq` in one
    /// `BEGIN IMMEDIATE ... COMMIT` transaction and stamps the row with
    /// `at_ms`. `append` passes the current wall-clock time; `import_event`
    /// passes the caller-supplied historical timestamp.
    async fn insert_event(
        &self,
        stream: &StreamId,
        rec: NewEvent,
        at_ms: i64,
    ) -> Result<Committed, StoreError> {
        let stream_name = stream.as_str().to_string();
        let patch_json = serde_json::to_string(&rec.patch).map_err(to_store_err)?;
        let meta_json = serde_json::to_string(&rec.meta).map_err(to_store_err)?;
        let kind = rec.kind;
        let seq = self
            .isle
            .call(move |conn| {
                let tx =
                    conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                let head: Option<i64> = tx
                    .query_row(
                        "SELECT MAX(seq) FROM events WHERE stream = ?1",
                        params![stream_name],
                        |r| r.get(0),
                    )
                    .optional()?
                    .flatten();
                let next_seq = head.unwrap_or(0) + 1;
                tx.execute(
                    "INSERT INTO events (stream, seq, kind, patch, meta, at_ms) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![stream_name, next_seq, kind, patch_json, meta_json, at_ms],
                )?;
                tx.commit()?;
                Ok(next_seq as u64)
            })
            .await
            .map_err(to_store_err)
            .map(Seq)?;
        Ok(Committed {
            seq,
            at: Timestamp(at_ms),
        })
    }
}

#[async_trait]
impl EventBackend for SqliteEventBackend {
    async fn append(&self, stream: &StreamId, rec: NewEvent) -> Result<Committed, StoreError> {
        self.insert_event(stream, rec, Timestamp::now().0).await
    }

    async fn import_event(
        &self,
        stream: &StreamId,
        rec: NewEvent,
        at: Timestamp,
    ) -> Result<Committed, StoreError> {
        self.insert_event(stream, rec, at.0).await
    }

    async fn read(
        &self,
        stream: &StreamId,
        from: Seq,
        limit: usize,
    ) -> Result<Vec<Event>, StoreError> {
        let stream_name = stream.as_str().to_string();
        let from = from.0 as i64;
        let limit = limit as i64;
        let rows = self
            .isle
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT seq, kind, patch, meta, at_ms FROM events \
                     WHERE stream = ?1 AND seq >= ?2 \
                     ORDER BY seq ASC LIMIT ?3",
                )?;
                let rows: Result<Vec<_>, rusqlite::Error> = stmt
                    .query_map(params![stream_name, from, limit], |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, String>(2)?,
                            r.get::<_, String>(3)?,
                            r.get::<_, i64>(4)?,
                        ))
                    })?
                    .collect();
                rows
            })
            .await
            .map_err(to_store_err)?;

        rows.into_iter()
            .map(|(s, k, p, m, t)| row_to_event(s as u64, k, p, m, t))
            .collect()
    }

    async fn read_by_meta(
        &self,
        stream: &StreamId,
        field: &str,
        value: &Value,
        from: Seq,
        limit: usize,
    ) -> Result<Vec<Event>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let stream_name = stream.as_str().to_string();
        let from_i = from.0 as i64;
        let limit_i = limit as i64;
        // Quote the field name so keys containing dots or reserved chars route
        // to a genuine top-level lookup rather than a nested traversal.
        let escaped = field.replace('\\', "\\\\").replace('"', "\\\"");
        let path = format!("$.\"{}\"", escaped);
        let is_null = value.is_null();
        let value_json = serde_json::to_string(value).map_err(to_store_err)?;

        let rows = self
            .isle
            .call(move |conn| {
                // JSON null vs missing field both surface as SQL NULL through
                // `json_extract`, so we branch on the Rust-side type: match
                // JSON null via `json_type(...) = 'null'` when the caller
                // asked for null, and use canonical-form equality otherwise.
                let sql = if is_null {
                    "SELECT seq, kind, patch, meta, at_ms FROM events \
                     WHERE stream = ?1 AND seq >= ?2 \
                       AND json_type(meta, ?3) = 'null' \
                     ORDER BY seq ASC LIMIT ?4"
                } else {
                    "SELECT seq, kind, patch, meta, at_ms FROM events \
                     WHERE stream = ?1 AND seq >= ?2 \
                       AND json_extract(meta, ?3) = json_extract(?4, '$') \
                     ORDER BY seq ASC LIMIT ?5"
                };
                let mut stmt = conn.prepare(sql)?;
                let rows: Result<Vec<_>, rusqlite::Error> = if is_null {
                    stmt.query_map(params![stream_name, from_i, path, limit_i], |r| {
                        Ok((
                            r.get::<_, i64>(0)?,
                            r.get::<_, String>(1)?,
                            r.get::<_, String>(2)?,
                            r.get::<_, String>(3)?,
                            r.get::<_, i64>(4)?,
                        ))
                    })?
                    .collect()
                } else {
                    stmt.query_map(
                        params![stream_name, from_i, path, value_json, limit_i],
                        |r| {
                            Ok((
                                r.get::<_, i64>(0)?,
                                r.get::<_, String>(1)?,
                                r.get::<_, String>(2)?,
                                r.get::<_, String>(3)?,
                                r.get::<_, i64>(4)?,
                            ))
                        },
                    )?
                    .collect()
                };
                rows
            })
            .await
            .map_err(to_store_err)?;

        rows.into_iter()
            .map(|(s, k, p, m, t)| row_to_event(s as u64, k, p, m, t))
            .collect()
    }

    async fn head(&self, stream: &StreamId) -> Result<Option<Seq>, StoreError> {
        let stream_name = stream.as_str().to_string();
        let head: Option<i64> = self
            .isle
            .call(move |conn| {
                conn.query_row(
                    "SELECT MAX(seq) FROM events WHERE stream = ?1",
                    params![stream_name],
                    |r| r.get(0),
                )
                .optional()
                .map(|opt| opt.flatten())
            })
            .await
            .map_err(to_store_err)?;
        Ok(head.map(|h| Seq(h as u64)))
    }

    async fn seq_at_time(
        &self,
        stream: &StreamId,
        at: Timestamp,
    ) -> Result<Option<Seq>, StoreError> {
        let stream_name = stream.as_str().to_string();
        let at_ms = at.0;
        let seq: Option<i64> = self
            .isle
            .call(move |conn| {
                // The index (stream, at_ms) lets SQLite pick the greatest
                // matching row directly. `at_ms` is monotonic within a
                // stream that only ever used `append` (writer thread stamps
                // append-time), so ORDER BY at_ms DESC + LIMIT 1 is
                // well-defined. Streams that mix in out-of-order
                // `import_event` calls are not covered by that guarantee —
                // see `Store::import_event`'s rustdoc.
                conn.query_row(
                    "SELECT seq FROM events \
                     WHERE stream = ?1 AND at_ms <= ?2 \
                     ORDER BY at_ms DESC, seq DESC LIMIT 1",
                    params![stream_name, at_ms],
                    |r| r.get::<_, i64>(0),
                )
                .optional()
            })
            .await
            .map_err(to_store_err)?;
        Ok(seq.map(|s| Seq(s as u64)))
    }

    async fn streams(&self) -> Result<Vec<StreamId>, StoreError> {
        let rows: Vec<String> = self
            .isle
            .call(|conn| {
                let mut stmt = conn.prepare("SELECT DISTINCT stream FROM events")?;
                let rows: Result<Vec<String>, rusqlite::Error> =
                    stmt.query_map([], |r| r.get::<_, String>(0))?.collect();
                rows
            })
            .await
            .map_err(to_store_err)?;
        Ok(rows.into_iter().map(StreamId).collect())
    }

    async fn label_set(&self, stream: &StreamId, label: &Label, at: Seq) -> Result<(), StoreError> {
        let stream_name = stream.as_str().to_string();
        let label_name = label.as_str().to_string();
        let at_i = at.0 as i64;
        let outcome: (bool, Option<i64>) = self
            .isle
            .call(move |conn| {
                let tx =
                    conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                // Reject dangling labels: seq must correspond to a real event.
                let exists: bool = tx
                    .query_row(
                        "SELECT 1 FROM events WHERE stream = ?1 AND seq = ?2",
                        params![stream_name, at_i],
                        |_| Ok(true),
                    )
                    .optional()?
                    .unwrap_or(false);
                if !exists {
                    let head: Option<i64> = tx
                        .query_row(
                            "SELECT MAX(seq) FROM events WHERE stream = ?1",
                            params![stream_name],
                            |r| r.get(0),
                        )
                        .optional()?
                        .flatten();
                    return Ok((false, head));
                }
                tx.execute(
                    "INSERT INTO labels (stream, name, at_seq) VALUES (?1, ?2, ?3) \
                     ON CONFLICT(stream, name) DO UPDATE SET at_seq = excluded.at_seq",
                    params![stream_name, label_name, at_i],
                )?;
                tx.commit()?;
                Ok((true, None))
            })
            .await
            .map_err(to_store_err)?;

        if outcome.0 {
            Ok(())
        } else {
            Err(StoreError::SeqOutOfRange {
                head: outcome.1.map(|h| Seq(h as u64)),
                requested: at,
            })
        }
    }

    async fn label_resolve(
        &self,
        stream: &StreamId,
        label: &Label,
    ) -> Result<Option<Seq>, StoreError> {
        let stream_name = stream.as_str().to_string();
        let label_name = label.as_str().to_string();
        let seq: Option<i64> = self
            .isle
            .call(move |conn| {
                conn.query_row(
                    "SELECT at_seq FROM labels WHERE stream = ?1 AND name = ?2",
                    params![stream_name, label_name],
                    |r| r.get::<_, i64>(0),
                )
                .optional()
            })
            .await
            .map_err(to_store_err)?;
        Ok(seq.map(|s| Seq(s as u64)))
    }

    async fn labels(&self, stream: &StreamId) -> Result<Vec<(Label, Seq)>, StoreError> {
        let stream_name = stream.as_str().to_string();
        let rows: Vec<(String, i64)> = self
            .isle
            .call(move |conn| {
                let mut stmt = conn.prepare(
                    "SELECT name, at_seq FROM labels WHERE stream = ?1 ORDER BY name ASC",
                )?;
                let rows: Result<Vec<(String, i64)>, rusqlite::Error> = stmt
                    .query_map(params![stream_name], |r| {
                        Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
                    })?
                    .collect();
                rows
            })
            .await
            .map_err(to_store_err)?;
        Ok(rows
            .into_iter()
            .map(|(n, s)| (Label(n), Seq(s as u64)))
            .collect())
    }

    async fn label_delete(&self, stream: &StreamId, label: &Label) -> Result<bool, StoreError> {
        let stream_name = stream.as_str().to_string();
        let label_name = label.as_str().to_string();
        let changed = self
            .isle
            .call(move |conn| {
                conn.execute(
                    "DELETE FROM labels WHERE stream = ?1 AND name = ?2",
                    params![stream_name, label_name],
                )
            })
            .await
            .map_err(to_store_err)?;
        Ok(changed > 0)
    }
}

#[async_trait]
impl CacheBackend for SqliteCacheBackend {
    async fn put(&self, stream: &StreamId, at: Seq, state: &Value) -> Result<(), StoreError> {
        let stream_name = stream.as_str().to_string();
        let at_i = at.0 as i64;
        let state_json = serde_json::to_string(state).map_err(to_store_err)?;
        self.isle
            .call(move |conn| {
                conn.execute(
                    "INSERT INTO cache (stream, at_seq, state) VALUES (?1, ?2, ?3) \
                     ON CONFLICT(stream, at_seq) DO UPDATE SET state = excluded.state",
                    params![stream_name, at_i, state_json],
                )?;
                Ok(())
            })
            .await
            .map_err(to_store_err)
    }

    async fn nearest(
        &self,
        stream: &StreamId,
        at: Seq,
    ) -> Result<Option<(Seq, Value)>, StoreError> {
        let stream_name = stream.as_str().to_string();
        let at_i = at.0 as i64;
        let row: Option<(i64, String)> = self
            .isle
            .call(move |conn| {
                conn.query_row(
                    "SELECT at_seq, state FROM cache \
                     WHERE stream = ?1 AND at_seq <= ?2 \
                     ORDER BY at_seq DESC LIMIT 1",
                    params![stream_name, at_i],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
                )
                .optional()
            })
            .await
            .map_err(to_store_err)?;
        match row {
            None => Ok(None),
            Some((s, json)) => {
                let value: Value = serde_json::from_str(&json).map_err(to_store_err)?;
                Ok(Some((Seq(s as u64), value)))
            }
        }
    }

    async fn prune(&self, stream: &StreamId, keep_latest: usize) -> Result<(), StoreError> {
        let stream_name = stream.as_str().to_string();
        let keep = keep_latest as i64;
        self.isle
            .call(move |conn| {
                // Delete every entry not among the `keep` largest at_seqs.
                conn.execute(
                    "DELETE FROM cache WHERE stream = ?1 AND at_seq NOT IN \
                        (SELECT at_seq FROM cache WHERE stream = ?1 \
                         ORDER BY at_seq DESC LIMIT ?2)",
                    params![stream_name, keep],
                )?;
                Ok(())
            })
            .await
            .map_err(to_store_err)
    }
}

#[async_trait]
impl CheckpointBackend for SqliteCheckpointBackend {
    async fn get(&self, sink_id: &str, stream: &StreamId) -> Result<Option<Seq>, StoreError> {
        let sink_id = sink_id.to_string();
        let stream_name = stream.as_str().to_string();
        let seq: Option<i64> = self
            .isle
            .call(move |conn| {
                conn.query_row(
                    "SELECT at_seq FROM sink_checkpoints WHERE sink_id = ?1 AND stream = ?2",
                    params![sink_id, stream_name],
                    |r| r.get::<_, i64>(0),
                )
                .optional()
            })
            .await
            .map_err(to_store_err)?;
        Ok(seq.map(|s| Seq(s as u64)))
    }

    async fn put(&self, sink_id: &str, stream: &StreamId, at: Seq) -> Result<(), StoreError> {
        let sink_id = sink_id.to_string();
        let stream_name = stream.as_str().to_string();
        let at_i = at.0 as i64;
        self.isle
            .call(move |conn| {
                conn.execute(
                    "INSERT INTO sink_checkpoints (sink_id, stream, at_seq) VALUES (?1, ?2, ?3) \
                     ON CONFLICT(sink_id, stream) DO UPDATE SET at_seq = excluded.at_seq",
                    params![sink_id, stream_name, at_i],
                )?;
                Ok(())
            })
            .await
            .map_err(to_store_err)
    }
}
