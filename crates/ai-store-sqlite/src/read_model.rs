//! Queryable read-model projection (`ProjectionSink` + query API).
//!
//! [`SqliteReadModel`] is an opt-in `ProjectionSink` that materializes the
//! *latest* state of every stream into a single `read_model` row, keyed by
//! `stream`. It rides the existing checkpoint + `catch_up` machinery — there
//! is no dedicated wiring — so a consumer registers it with `Store` exactly
//! like any other sink and gets a queryable cache of "current state per
//! stream" for free.
//!
//! ## Why this exists
//!
//! `Store::state` reconstructs one stream's state via cache-nearest + replay,
//! which is the right tool for "what does *this* stream look like now or at
//! some point in the past". It has no answer for "which streams currently
//! have `meta.owner == "alice"`", or "list the 20 most recently updated
//! streams" — those require a query *across* streams, and the event log has
//! no cross-stream index. `SqliteReadModel` fills that gap: every consumer of
//! this crate that needs a filterable list view (`mini-app` list, `outline-mcp`
//! snapshot listing, `journal-mcp` tail) currently reimplements some version
//! of "keep a side table of current state" by hand. This module gives that
//! pattern a single, tested home.
//!
//! ## Idempotence under redelivery
//!
//! [`ProjectionSink::commit`] is contracted to be idempotent — `catch_up` and
//! `rebuild` may redeliver the same `(stream, seq)`, or (after a crash) an
//! out-of-order redelivery of an *older* seq than what is already stored. The
//! UPSERT below guards against both: it only overwrites a row when the
//! incoming `last_seq` is strictly greater than the row's current `last_seq`
//! (`ON CONFLICT ... DO UPDATE ... WHERE excluded.last_seq > read_model.last_seq`).
//! A same-seq redelivery is a no-op (the row already reflects that seq); an
//! older-seq redelivery is also a no-op rather than rewinding the row to a
//! stale state.
//!
//! ## Tombstones follow the core delete convention
//!
//! [`SqliteReadModel::new`] defaults to recognizing
//! [`ai_store_core::TOMBSTONE_KIND`] — the same canonical kind
//! [`ai_store_core::Store::delete`] appends — so a consumer that uses the
//! core-level delete API gets `live = 0` for tombstoned streams without
//! duplicating a kind string. Override with [`SqliteReadModel::with_tombstone_kind`]
//! only when the consumer has its own pre-existing kind convention that
//! predates the core constant, and opt out entirely with
//! [`SqliteReadModel::without_tombstone_kind`] when tombstoning should be
//! disabled for this sink.
//!
//! `live` toggles both ways: a committed event whose `kind` matches the
//! configured tombstone kind flips `live` to `0`, and any other kind flips it
//! back to `1` — so a further non-tombstone append "revives" the stream.
//! [`SqliteReadModel::query`] filters `live = 1` by default (toggle with
//! `Query::include_dead`), while [`SqliteReadModel::get`] returns tombstoned
//! rows so callers can inspect `ReadModelRow::live`. This module intentionally
//! stays inside its projection table — cross-stream listings that respect the
//! delete convention without a read model live on the facade
//! ([`ai_store_core::Store::streams_live`]), and physical log compaction is a
//! separate concern tracked in the retention issue.

use ai_store_core::{
    Event, ProjectionSink, Seq, SqliteBackend, StoreError, StreamId, Timestamp, TOMBSTONE_KIND,
};
use async_trait::async_trait;
use rusqlite::types::Value as SqlValue;
use rusqlite::{params, OptionalExtension};
use rusqlite_isle::AsyncIsle;
use serde_json::Value;

use crate::backend::{from_isle_err, to_store_err};

/// A single row materialized from the event log: the latest known state of
/// one stream, as of `last_seq`.
#[derive(Debug, Clone, PartialEq)]
pub struct ReadModelRow {
    /// The stream this row represents.
    pub stream: StreamId,
    /// The materialized state as of `last_seq`.
    pub state: Value,
    /// The seq this row was last updated from.
    pub last_seq: Seq,
    /// The `at` of the event that produced `last_seq` (i.e. `Event::at`, not
    /// the wall-clock time the row was written — those coincide for `append`
    /// but not for `import_event`).
    pub updated_at: Timestamp,
    /// `false` once a tombstone-kind event has been committed for this
    /// stream (see [`SqliteReadModel::with_tombstone_kind`]); `true`
    /// otherwise, including for streams that never opted into tombstoning.
    pub live: bool,
}

// Query vocabulary (`Filter` / `RawWhere` / `After` / `Query`) and the SQL
// builder free functions live in `crate::query` — re-exported here so the
// public path (`ai_store_sqlite::read_model::{After, Filter, Order, Query,
// RawWhere}` / `ai_store_sqlite::{After, Filter, Order, Query, RawWhere}`)
// is unchanged.
use crate::query::{apply_keyset, build_where, order_clause, validate_field_path};
pub use crate::query::{After, Filter, Order, Query, RawWhere};

type RawRow = (String, String, i64, i64, i64);

fn row_to_read_model_row(row: RawRow) -> Result<ReadModelRow, StoreError> {
    let (stream, state_json, last_seq, updated_at, live) = row;
    let state: Value = serde_json::from_str(&state_json)
        .map_err(|e| StoreError::Backend(format!("read-model state decode: {e}")))?;
    Ok(ReadModelRow {
        stream: StreamId(stream),
        state,
        last_seq: Seq(last_seq as u64),
        updated_at: Timestamp(updated_at),
        live: live != 0,
    })
}

const ROW_COLUMNS: &str = "stream, state, last_seq, updated_at, live";

fn read_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<RawRow> {
    Ok((
        r.get::<_, String>(0)?,
        r.get::<_, String>(1)?,
        r.get::<_, i64>(2)?,
        r.get::<_, i64>(3)?,
        r.get::<_, i64>(4)?,
    ))
}

/// SQLite-backed queryable read-model projection.
///
/// Implements [`ProjectionSink`], so it wires into `Store` exactly like any
/// other sink — no dedicated dispatch path exists or is needed. Cloneable;
/// every clone shares the same SQLite thread (see [`crate::SqliteBackends::isle`]).
#[derive(Clone)]
pub struct SqliteReadModel {
    isle: AsyncIsle,
    id: String,
    tombstone_kind: Option<String>,
}

impl SqliteReadModel {
    /// Build from an existing rusqlite-isle handle, with the default sink id
    /// `"read-model"` and the core-level tombstone kind
    /// ([`ai_store_core::TOMBSTONE_KIND`]) preconfigured — so
    /// [`ai_store_core::Store::delete`] flips `live` to `0` on this sink
    /// without any extra wiring.
    ///
    /// Override the kind with [`SqliteReadModel::with_tombstone_kind`] when
    /// the consumer has an incompatible pre-existing convention, or disable
    /// tombstoning entirely with [`SqliteReadModel::without_tombstone_kind`].
    pub fn new(isle: AsyncIsle) -> Self {
        Self {
            isle,
            id: "read-model".to_string(),
            tombstone_kind: Some(TOMBSTONE_KIND.to_string()),
        }
    }

    /// Override the checkpoint id used by [`ProjectionSink::id`]. Useful when
    /// a consumer registers more than one `SqliteReadModel` (e.g. one per
    /// domain) against the same `Store` and needs distinct checkpoints.
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    /// Override the tombstone event kind. A committed event whose `kind`
    /// matches sets the row's `live` to `0`; any other kind sets it back to
    /// `1`.
    ///
    /// [`SqliteReadModel::new`] already configures
    /// [`ai_store_core::TOMBSTONE_KIND`] — reach for this method only when
    /// the consumer has its own pre-existing kind convention that predates the
    /// core constant. New code should prefer [`ai_store_core::Store::delete`]
    /// and rely on the default.
    pub fn with_tombstone_kind(mut self, kind: impl Into<String>) -> Self {
        self.tombstone_kind = Some(kind.into());
        self
    }

    /// Disable tombstone-driven `live` toggling: every committed event leaves
    /// the row `live = 1` regardless of `kind`.
    ///
    /// Use this when a consumer wants the read-model as a pure
    /// latest-state-per-stream projection with no delete semantics — for
    /// example when tombstoning is enforced elsewhere and duplicating it
    /// here would be misleading.
    pub fn without_tombstone_kind(mut self) -> Self {
        self.tombstone_kind = None;
        self
    }

    /// Run `q` against the read-model table.
    pub async fn query(&self, q: &Query) -> Result<Vec<ReadModelRow>, StoreError> {
        let (sql, params) = self.build_query_sql(q)?;
        let rows: Vec<RawRow> = self
            .isle
            .call(move |conn| {
                let mut stmt = conn.prepare(&sql)?;
                let rows: Result<Vec<RawRow>, rusqlite::Error> = stmt
                    .query_map(rusqlite::params_from_iter(params.iter()), read_row)?
                    .collect();
                rows
            })
            .await
            .map_err(from_isle_err)?;

        rows.into_iter().map(row_to_read_model_row).collect()
    }

    /// Run `q` against the read-model table AND return the total row count
    /// that would have matched without `limit` / `offset` / `after`, in a
    /// single SQLite transaction.
    ///
    /// Useful for paginated UIs that need `(page_rows, total_matching)` as
    /// one consistent snapshot — issuing a separate `query` and `count` can
    /// let a concurrent writer's commit land between the two, silently
    /// shifting the total under the page.
    pub async fn query_with_count(
        &self,
        q: &Query,
    ) -> Result<(Vec<ReadModelRow>, u64), StoreError> {
        let (query_sql, query_params) = self.build_query_sql(q)?;
        let (count_sql, count_params) =
            self.build_count_sql(q.filter.as_ref(), q.raw_where.as_ref(), q.include_dead)?;

        let (rows, count): (Vec<RawRow>, i64) = self
            .isle
            .call(move |conn| {
                // Wrapping both statements in a DEFERRED transaction pins
                // them to one SQLite snapshot — no other writer can commit
                // between them.
                let tx = conn.transaction()?;
                let rows: Vec<RawRow> = {
                    let mut stmt = tx.prepare(&query_sql)?;
                    let rows: Result<Vec<RawRow>, rusqlite::Error> = stmt
                        .query_map(
                            rusqlite::params_from_iter(query_params.iter()),
                            read_row,
                        )?
                        .collect();
                    rows?
                };
                let count: i64 = tx.query_row(
                    &count_sql,
                    rusqlite::params_from_iter(count_params.iter()),
                    |r| r.get(0),
                )?;
                tx.commit()?;
                Ok((rows, count))
            })
            .await
            .map_err(from_isle_err)?;

        let out_rows: Result<Vec<_>, _> = rows.into_iter().map(row_to_read_model_row).collect();
        Ok((out_rows?, count as u64))
    }

    /// Build the SQL string + bind parameters for a `SELECT` query — shared
    /// by [`SqliteReadModel::query`] and [`SqliteReadModel::query_with_count`].
    fn build_query_sql(&self, q: &Query) -> Result<(String, Vec<SqlValue>), StoreError> {
        let mut params: Vec<SqlValue> = Vec::new();
        let mut where_sql = build_where(
            q.filter.as_ref(),
            q.raw_where.as_ref(),
            q.include_dead,
            &mut params,
        )?;
        apply_keyset(q.after.as_ref(), q.order_by.as_ref(), &mut where_sql, &mut params)?;
        let order_sql = order_clause(q.order_by.as_ref())?;
        let sql = format!(
            "SELECT {ROW_COLUMNS} FROM read_model WHERE {where_sql} \
             ORDER BY {order_sql} LIMIT ? OFFSET ?"
        );
        params.push(SqlValue::Integer(q.limit as i64));
        params.push(SqlValue::Integer(q.offset as i64));
        Ok((sql, params))
    }

    /// Build the SQL string + bind parameters for a `COUNT(*)` — shared by
    /// [`SqliteReadModel::count`] and [`SqliteReadModel::query_with_count`].
    fn build_count_sql(
        &self,
        filter: Option<&Filter>,
        raw: Option<&RawWhere>,
        include_dead: bool,
    ) -> Result<(String, Vec<SqlValue>), StoreError> {
        let mut params: Vec<SqlValue> = Vec::new();
        let where_sql = build_where(filter, raw, include_dead, &mut params)?;
        let sql = format!("SELECT COUNT(*) FROM read_model WHERE {where_sql}");
        Ok((sql, params))
    }

    /// Count rows matching `filter` (or every row when `filter` is `None`).
    pub async fn count(
        &self,
        filter: Option<&Filter>,
        include_dead: bool,
    ) -> Result<u64, StoreError> {
        let (sql, params) = self.build_count_sql(filter, None, include_dead)?;
        let count: i64 = self
            .isle
            .call(move |conn| {
                conn.query_row(&sql, rusqlite::params_from_iter(params.iter()), |r| {
                    r.get(0)
                })
            })
            .await
            .map_err(from_isle_err)?;
        Ok(count as u64)
    }

    /// Fetch the row for `stream`, if one has ever been committed. Returned
    /// regardless of `live` — callers that need to distinguish tombstoned
    /// streams should check `ReadModelRow::live`.
    pub async fn get(&self, stream: &StreamId) -> Result<Option<ReadModelRow>, StoreError> {
        let stream_name = stream.as_str().to_string();
        let row: Option<RawRow> = self
            .isle
            .call(move |conn| {
                conn.query_row(
                    &format!("SELECT {ROW_COLUMNS} FROM read_model WHERE stream = ?1"),
                    params![stream_name],
                    read_row,
                )
                .optional()
            })
            .await
            .map_err(from_isle_err)?;
        row.map(row_to_read_model_row).transpose()
    }

    /// The `n` most recently updated rows, regardless of `live` (newest
    /// first). Intended for journal-style "what changed recently" views.
    pub async fn tail(&self, n: usize) -> Result<Vec<ReadModelRow>, StoreError> {
        let limit = n as i64;
        let rows: Vec<RawRow> = self
            .isle
            .call(move |conn| {
                let mut stmt = conn.prepare(&format!(
                    "SELECT {ROW_COLUMNS} FROM read_model ORDER BY updated_at DESC LIMIT ?1"
                ))?;
                let rows: Result<Vec<RawRow>, rusqlite::Error> =
                    stmt.query_map(params![limit], read_row)?.collect();
                rows
            })
            .await
            .map_err(from_isle_err)?;
        rows.into_iter().map(row_to_read_model_row).collect()
    }

    /// Create a SQLite expression index on `json_extract(state, '$.<field>')`
    /// to speed up repeated [`Query`]/[`Filter`] lookups on `field`.
    ///
    /// `field` is validated exactly like a [`Filter`] field path. The index
    /// name is derived from `field` (dots replaced with underscores) and
    /// quoted, so it is safe even though it is built by string formatting —
    /// `field` has already been restricted to `[A-Za-z0-9_.]+`.
    pub async fn create_field_index(&self, field: &str) -> Result<(), StoreError> {
        validate_field_path(field)?;
        let index_name = format!("ix_rm_{}", field.replace('.', "_"));
        let sql = format!(
            "CREATE INDEX IF NOT EXISTS \"{index_name}\" \
             ON read_model(json_extract(state, '$.{field}'))"
        );
        self.isle
            .call(move |conn| conn.execute_batch(&sql))
            .await
            .map_err(from_isle_err)
    }
}

impl SqliteBackend for SqliteReadModel {
    type Handle = AsyncIsle;

    fn new(handle: AsyncIsle) -> Self {
        SqliteReadModel::new(handle)
    }
}

#[async_trait]
impl ProjectionSink for SqliteReadModel {
    fn id(&self) -> &str {
        &self.id
    }

    /// Upsert `stream`'s row from this committed event.
    ///
    /// The `WHERE excluded.last_seq > read_model.last_seq` guard on the
    /// conflict branch is the idempotence contract this module promises: a
    /// same-seq redelivery, or an out-of-order redelivery of an *older* seq
    /// than the row already reflects, leaves the row untouched rather than
    /// rewinding it. See the module-level rustdoc for the full rationale.
    async fn commit(
        &self,
        stream: &StreamId,
        seq: Seq,
        state: &Value,
        event: &Event,
    ) -> Result<(), StoreError> {
        let stream_name = stream.as_str().to_string();
        let state_json = serde_json::to_string(state).map_err(to_store_err)?;
        let seq_i = seq.0 as i64;
        let updated_at = event.at.0;
        let live: i64 = match &self.tombstone_kind {
            Some(kind) if kind == &event.kind => 0,
            _ => 1,
        };

        self.isle
            .call(move |conn| {
                conn.execute(
                    "INSERT INTO read_model (stream, state, last_seq, updated_at, live) \
                     VALUES (?1, ?2, ?3, ?4, ?5) \
                     ON CONFLICT(stream) DO UPDATE SET \
                        state = excluded.state, \
                        last_seq = excluded.last_seq, \
                        updated_at = excluded.updated_at, \
                        live = excluded.live \
                     WHERE excluded.last_seq > read_model.last_seq",
                    params![stream_name, state_json, seq_i, updated_at, live],
                )?;
                Ok(())
            })
            .await
            .map_err(from_isle_err)
    }
}
