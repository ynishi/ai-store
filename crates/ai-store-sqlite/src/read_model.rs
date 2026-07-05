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
//! ## Tombstones are a minimal hook, not a delete model
//!
//! [`SqliteReadModel::with_tombstone_kind`] flips `live` to `0` when a
//! committed event's `kind` matches the configured tombstone kind, and back
//! to `1` on every other kind (so a further append "revives" the stream).
//! This is deliberately shallow: there is no cascading removal from
//! [`SqliteReadModel::query`]'s default result set beyond the `live = 1`
//! filter (toggle with `Query::include_dead`), and no interaction with
//! `Store::streams` or the event log itself. Real delete semantics (e.g.
//! excluding tombstoned streams from `Store::streams`) are out of scope here.

use ai_store_core::{Event, ProjectionSink, Seq, SqliteBackend, StoreError, StreamId, Timestamp};
use async_trait::async_trait;
use rusqlite::types::Value as SqlValue;
use rusqlite::{params, OptionalExtension};
use rusqlite_isle::AsyncIsle;
use serde_json::Value;

use crate::backend::to_store_err;

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

/// Sort direction for [`Query::order_by`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Order {
    /// Ascending.
    Asc,
    /// Descending.
    Desc,
}

/// A predicate over `read_model.state`, expressed with dotted field paths
/// (e.g. `"title"`, `"meta.owner"`) rather than raw SQL.
///
/// Field paths are restricted to `[A-Za-z0-9_.]+` (no leading/trailing/
/// doubled dot) and rejected with `StoreError::Backend` otherwise — this is
/// the SQL-injection guard: every value (and every validated path) is bound
/// as a query parameter, never interpolated into the SQL string.
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    /// `state.<field> == value`.
    Eq(String, Value),
    /// `state.<field>` equals any of `values`.
    In(String, Vec<Value>),
    /// `state.<field> LIKE pattern` (SQL `LIKE` semantics; intended for
    /// string-typed fields).
    Like(String, String),
    /// Conjunction of sub-filters (`true` for an empty list).
    And(Vec<Filter>),
    /// Disjunction of sub-filters (`false` for an empty list).
    Or(Vec<Filter>),
}

/// A query against the read-model table.
#[derive(Debug, Clone)]
pub struct Query {
    /// Predicate to apply. `None` matches every row.
    pub filter: Option<Filter>,
    /// Sort key. `None` defaults to `updated_at DESC`.
    ///
    /// The field name may be one of the read-model's own columns
    /// (`"updated_at"`, `"last_seq"`, `"stream"`, `"live"`) or a dotted path
    /// into `state`, following the same validation as [`Filter`].
    pub order_by: Option<(String, Order)>,
    /// Maximum rows to return.
    pub limit: usize,
    /// Rows to skip before `limit` is applied.
    pub offset: usize,
    /// Include tombstoned (`live = 0`) rows. Defaults to `false` in
    /// [`Query::default`].
    pub include_dead: bool,
}

impl Default for Query {
    fn default() -> Self {
        Self {
            filter: None,
            order_by: None,
            limit: 100,
            offset: 0,
            include_dead: false,
        }
    }
}

/// A field path consists only of `[A-Za-z0-9_.]`, with no leading, trailing,
/// or doubled dot. Rejecting anything else keeps every `json_extract` path
/// this module builds free of characters that could otherwise break out of
/// the intended `$.<field>` shape.
fn validate_field_path(field: &str) -> Result<(), StoreError> {
    let charset_ok = !field.is_empty()
        && field
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.');
    let dots_ok = !field.starts_with('.') && !field.ends_with('.') && !field.contains("..");
    if charset_ok && dots_ok {
        Ok(())
    } else {
        Err(StoreError::Backend(format!(
            "invalid read-model field path {field:?}: only [A-Za-z0-9_.]+ is allowed, \
             with no leading/trailing/doubled dot"
        )))
    }
}

fn json_param(value: &Value) -> Result<SqlValue, StoreError> {
    serde_json::to_string(value)
        .map(SqlValue::Text)
        .map_err(|e| StoreError::Backend(format!("encode read-model filter value: {e}")))
}

/// Build the SQL fragment for one `Filter`, appending its bind parameters (in
/// left-to-right order matching the fragment's `?` placeholders) to `params`.
fn build_filter(filter: &Filter, params: &mut Vec<SqlValue>) -> Result<String, StoreError> {
    match filter {
        Filter::Eq(field, value) => {
            validate_field_path(field)?;
            let path = format!("$.{field}");
            if value.is_null() {
                params.push(SqlValue::Text(path));
                Ok("json_type(state, ?) = 'null'".to_string())
            } else {
                params.push(SqlValue::Text(path));
                params.push(json_param(value)?);
                Ok("json_extract(state, ?) = json_extract(?, '$')".to_string())
            }
        }
        Filter::In(field, values) => {
            if values.is_empty() {
                // No candidate can match an empty set.
                return Ok("0".to_string());
            }
            let mut parts = Vec::with_capacity(values.len());
            for v in values {
                parts.push(build_filter(&Filter::Eq(field.clone(), v.clone()), params)?);
            }
            Ok(format!("({})", parts.join(" OR ")))
        }
        Filter::Like(field, pattern) => {
            validate_field_path(field)?;
            let path = format!("$.{field}");
            params.push(SqlValue::Text(path));
            params.push(SqlValue::Text(pattern.clone()));
            Ok("json_extract(state, ?) LIKE ?".to_string())
        }
        Filter::And(filters) => {
            if filters.is_empty() {
                return Ok("1".to_string());
            }
            let mut parts = Vec::with_capacity(filters.len());
            for f in filters {
                parts.push(format!("({})", build_filter(f, params)?));
            }
            Ok(parts.join(" AND "))
        }
        Filter::Or(filters) => {
            if filters.is_empty() {
                return Ok("0".to_string());
            }
            let mut parts = Vec::with_capacity(filters.len());
            for f in filters {
                parts.push(format!("({})", build_filter(f, params)?));
            }
            Ok(parts.join(" OR "))
        }
    }
}

/// Combine an optional user [`Filter`] with the `live` constraint implied by
/// `include_dead` into one `WHERE`-clause body.
fn build_where(
    filter: Option<&Filter>,
    include_dead: bool,
    params: &mut Vec<SqlValue>,
) -> Result<String, StoreError> {
    let filter_sql = match filter {
        Some(f) => build_filter(f, params)?,
        None => "1".to_string(),
    };
    if include_dead {
        Ok(filter_sql)
    } else {
        Ok(format!("({filter_sql}) AND live = 1"))
    }
}

/// Build an `ORDER BY <expr> <dir>` sort key. Known read-model columns sort
/// directly; anything else is treated as a dotted path into `state`.
fn order_clause(order_by: Option<&(String, Order)>) -> Result<String, StoreError> {
    match order_by {
        None => Ok("updated_at DESC".to_string()),
        Some((field, order)) => {
            validate_field_path(field)?;
            let expr = match field.as_str() {
                "updated_at" | "last_seq" | "stream" | "live" => field.clone(),
                _ => format!("json_extract(state, '$.{field}')"),
            };
            let dir = match order {
                Order::Asc => "ASC",
                Order::Desc => "DESC",
            };
            Ok(format!("{expr} {dir}"))
        }
    }
}

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
    /// `"read-model"` and no tombstone kind configured (every committed
    /// stream stays `live = 1`).
    pub fn new(isle: AsyncIsle) -> Self {
        Self {
            isle,
            id: "read-model".to_string(),
            tombstone_kind: None,
        }
    }

    /// Override the checkpoint id used by [`ProjectionSink::id`]. Useful when
    /// a consumer registers more than one `SqliteReadModel` (e.g. one per
    /// domain) against the same `Store` and needs distinct checkpoints.
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    /// Configure a tombstone event kind: a committed event whose `kind`
    /// matches sets the row's `live` to `0`; any other kind sets it back to
    /// `1`. Without this, `live` is always `1`.
    ///
    /// This is a minimal hook, not a delete model — see the module-level
    /// rustdoc "Tombstones" section for what it deliberately does not do.
    pub fn with_tombstone_kind(mut self, kind: impl Into<String>) -> Self {
        self.tombstone_kind = Some(kind.into());
        self
    }

    /// Run `q` against the read-model table.
    pub async fn query(&self, q: &Query) -> Result<Vec<ReadModelRow>, StoreError> {
        let mut params: Vec<SqlValue> = Vec::new();
        let where_sql = build_where(q.filter.as_ref(), q.include_dead, &mut params)?;
        let order_sql = order_clause(q.order_by.as_ref())?;
        let sql = format!(
            "SELECT {ROW_COLUMNS} FROM read_model WHERE {where_sql} \
             ORDER BY {order_sql} LIMIT ? OFFSET ?"
        );
        params.push(SqlValue::Integer(q.limit as i64));
        params.push(SqlValue::Integer(q.offset as i64));

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
            .map_err(to_store_err)?;

        rows.into_iter().map(row_to_read_model_row).collect()
    }

    /// Count rows matching `filter` (or every row when `filter` is `None`).
    pub async fn count(
        &self,
        filter: Option<&Filter>,
        include_dead: bool,
    ) -> Result<u64, StoreError> {
        let mut params: Vec<SqlValue> = Vec::new();
        let where_sql = build_where(filter, include_dead, &mut params)?;
        let sql = format!("SELECT COUNT(*) FROM read_model WHERE {where_sql}");

        let count: i64 = self
            .isle
            .call(move |conn| {
                conn.query_row(&sql, rusqlite::params_from_iter(params.iter()), |r| {
                    r.get(0)
                })
            })
            .await
            .map_err(to_store_err)?;
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
            .map_err(to_store_err)?;
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
            .map_err(to_store_err)?;
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
            .map_err(to_store_err)
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
            .map_err(to_store_err)
    }
}
