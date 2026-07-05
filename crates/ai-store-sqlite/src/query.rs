//! Read-model query types and SQL assembly.
//!
//! This module owns the query vocabulary ([`Filter`] / [`RawWhere`] /
//! [`After`] / [`Query`]) and the pure functions that compile a [`Query`]
//! into a parameterized SQL `WHERE` clause. It does not execute anything —
//! [`crate::read_model::SqliteReadModel`] is the only consumer, and the only
//! place SQL built here actually runs against SQLite.

use ai_store_core::{StoreError, StreamId};
use rusqlite::types::Value as SqlValue;
use serde_json::Value;

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
///
/// `Filter` covers the common shortcuts (equality, membership, string
/// match, boolean composition, negation, range comparison). Queries that
/// need something outside this vocabulary — full-text search operators,
/// JSON path functions, custom SQL — should reach for
/// [`Query::raw_where`], which lets a caller drop in a parameterized `WHERE`
/// fragment without having to grow this enum forever.
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    /// `state.<field> == value`.
    Eq(String, Value),
    /// `state.<field>` equals any of `values`.
    In(String, Vec<Value>),
    /// `state.<field> LIKE pattern` (SQL `LIKE` semantics; intended for
    /// string-typed fields).
    Like(String, String),
    /// `state.<field> > value`. Both operands are compared under SQLite's
    /// [`json_extract`] semantics: numbers compare numerically, strings
    /// lexicographically, mixed types return no match (the row is silently
    /// filtered out rather than erroring — matching how JSON's
    /// heterogeneous comparison degrades).
    ///
    /// [`json_extract`]: https://www.sqlite.org/json1.html#the_json_extract_function
    Gt(String, Value),
    /// `state.<field> >= value`. See [`Filter::Gt`] for comparison
    /// semantics.
    Gte(String, Value),
    /// `state.<field> < value`. See [`Filter::Gt`] for comparison
    /// semantics.
    Lt(String, Value),
    /// `state.<field> <= value`. See [`Filter::Gt`] for comparison
    /// semantics.
    Lte(String, Value),
    /// Negation of a sub-filter.
    Not(Box<Filter>),
    /// Conjunction of sub-filters (`true` for an empty list).
    And(Vec<Filter>),
    /// Disjunction of sub-filters (`false` for an empty list).
    Or(Vec<Filter>),
}

/// Parameterized `WHERE`-clause fragment (see [`Query::raw_where`]).
#[derive(Debug, Clone)]
pub struct RawWhere {
    /// SQL fragment interpolated verbatim into the compiled `WHERE`
    /// clause. Must NOT contain user-supplied literals: substitute each
    /// value with a `?` placeholder and put the value in [`RawWhere::params`].
    /// Use `state` as the table alias for `json_extract`; the read-model's
    /// own columns (`stream`, `last_seq`, `updated_at`, `live`) are
    /// available unqualified.
    ///
    /// ## Comparing bound params to primitive columns
    ///
    /// [`RawWhere::params`] is serialized through the same
    /// `serde_json::to_string` path every typed [`Filter`] comparison
    /// uses — each value is bound as its JSON literal (a numeric `10`
    /// binds as `"10"`, the string `"foo"` binds as `"\"foo\""`). When
    /// comparing against `json_extract(state, '$.field)`'s JSON-typed
    /// output that just works; when comparing against a plain string
    /// column, wrap the placeholder with `json_extract(?, '$')` to
    /// unwrap the JSON quoting on the SQL side. The typed comparison
    /// builders already do this — see the string range example in
    /// [`Filter::Gt`]'s rustdoc for the same pattern.
    pub sql: String,
    /// Bind parameters, in `?`-placeholder order. `serde_json::Value` covers
    /// numbers, strings, booleans, arrays, and null — enough for every
    /// case a caller not writing raw SQL would express through [`Filter`].
    pub params: Vec<Value>,
}

/// Value + tie-breaker used for keyset pagination (see [`Query::after`]).
///
/// A caller reads page N by re-issuing the same [`Query`] with
/// `after = Some((<last row's order_by value>, <last row's stream id>))`.
/// The stream id is the tie-breaker for orderings that are not unique
/// (`updated_at`, `live`, or any `state.<field>` that can repeat) — without
/// it, two rows with the same order-by value can flip past each other from
/// page to page, silently dropping or duplicating rows.
#[derive(Debug, Clone)]
pub struct After {
    /// The last row's value under the effective `order_by` expression (a
    /// [`serde_json::Value`] so it works uniformly across numeric-typed and
    /// string-typed keys). For the default `updated_at DESC` sort this is
    /// a `Number(i64)`.
    pub order_value: Value,
    /// The last row's [`StreamId`] (used only when two rows share
    /// `order_value`; unique per row, so a tie-break is decisive).
    pub stream: StreamId,
}

/// A query against the read-model table.
#[derive(Debug, Clone)]
pub struct Query {
    /// Predicate to apply. `None` matches every row.
    pub filter: Option<Filter>,
    /// Additional parameterized `WHERE` fragment, `AND`-ed with `filter`
    /// (and with the `live = 1` constraint, when `include_dead = false`).
    ///
    /// Reach for this when a query cannot be expressed through the
    /// [`Filter`] variants — e.g. full-text search operators, JSON path
    /// functions the crate does not expose, backend-specific operators.
    /// The fragment is spliced into the compiled SQL literally, so it
    /// must never contain user-supplied literals; put those in
    /// [`RawWhere::params`].
    pub raw_where: Option<RawWhere>,
    /// Sort key. `None` defaults to `updated_at DESC`.
    ///
    /// The field name may be one of the read-model's own columns
    /// (`"updated_at"`, `"last_seq"`, `"stream"`, `"live"`) or a dotted path
    /// into `state`, following the same validation as [`Filter`].
    pub order_by: Option<(String, Order)>,
    /// Keyset pagination cursor — see [`After`].
    ///
    /// Preferred over `offset`-based pagination for stability on updated
    /// tables: rows re-sorted between page reads never cause a
    /// keyset-paginated query to skip or repeat rows the way an
    /// `OFFSET`-paginated one can. Combine with any `filter` /
    /// `raw_where` — `after` is applied as an extra `AND` predicate.
    ///
    /// When both `after` and `offset` are set, both apply — but that
    /// combination is unusual and typically indicates a bug in the
    /// caller. Prefer one or the other.
    pub after: Option<After>,
    /// Maximum rows to return.
    pub limit: usize,
    /// Rows to skip before `limit` is applied. Kept for callers that
    /// have not yet migrated to [`Query::after`]; prefer keyset
    /// pagination for stable page boundaries.
    pub offset: usize,
    /// Include tombstoned (`live = 0`) rows. Defaults to `false` in
    /// [`Query::default`].
    pub include_dead: bool,
}

impl Default for Query {
    fn default() -> Self {
        Self {
            filter: None,
            raw_where: None,
            order_by: None,
            after: None,
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
pub(crate) fn validate_field_path(field: &str) -> Result<(), StoreError> {
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

/// Shared SQL builder for the range-comparison filter variants (`Gt` /
/// `Gte` / `Lt` / `Lte`). `op` is the SQLite binary operator that goes
/// between the two `json_extract` calls.
fn build_compare(
    field: &str,
    value: &Value,
    op: &str,
    params: &mut Vec<SqlValue>,
) -> Result<String, StoreError> {
    validate_field_path(field)?;
    let path = format!("$.{field}");
    params.push(SqlValue::Text(path));
    params.push(json_param(value)?);
    Ok(format!("json_extract(state, ?) {op} json_extract(?, '$')"))
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
        Filter::Gt(field, value) => build_compare(field, value, ">", params),
        Filter::Gte(field, value) => build_compare(field, value, ">=", params),
        Filter::Lt(field, value) => build_compare(field, value, "<", params),
        Filter::Lte(field, value) => build_compare(field, value, "<=", params),
        Filter::Not(inner) => {
            let inner_sql = build_filter(inner, params)?;
            Ok(format!("NOT ({inner_sql})"))
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

/// Combine an optional user [`Filter`], an optional raw `WHERE` fragment,
/// and the `live` constraint implied by `include_dead` into one
/// `WHERE`-clause body. Parameters accumulate in `params` in the order they
/// appear in the returned SQL.
pub(crate) fn build_where(
    filter: Option<&Filter>,
    raw: Option<&RawWhere>,
    include_dead: bool,
    params: &mut Vec<SqlValue>,
) -> Result<String, StoreError> {
    let filter_sql = match filter {
        Some(f) => build_filter(f, params)?,
        None => "1".to_string(),
    };
    let with_raw = match raw {
        Some(r) => {
            // Splice raw params AFTER the filter params in the same order
            // the ?-placeholders appear in the final compiled string, so
            // rusqlite binds them correctly.
            for v in &r.params {
                params.push(json_param(v)?);
            }
            format!("({filter_sql}) AND ({})", r.sql)
        }
        None => filter_sql,
    };
    if include_dead {
        Ok(with_raw)
    } else {
        Ok(format!("({with_raw}) AND live = 1"))
    }
}

/// Append the keyset-pagination `AND` predicate to `where_body` when the
/// query carries an [`After`] cursor. Uses the effective `order_by` expr
/// (already computed by [`order_clause`]) as the left-hand side of the
/// comparison so tie-breaking on `stream` uses the same key.
pub(crate) fn apply_keyset(
    after: Option<&After>,
    order_by: Option<&(String, Order)>,
    where_body: &mut String,
    params: &mut Vec<SqlValue>,
) -> Result<(), StoreError> {
    let Some(after) = after else {
        return Ok(());
    };
    let (expr, direction) = order_expr_and_direction(order_by)?;
    let cmp = match direction {
        Order::Asc => ">",
        Order::Desc => "<",
    };
    // Serialize the cursor's order value once, bind it twice (the SQL
    // fragment references it in both the strict-greater-than case and
    // the equality-then-stream-tiebreaker case).
    let order_value_json = json_param(&after.order_value)?;
    params.push(order_value_json.clone());
    params.push(order_value_json);
    params.push(SqlValue::Text(after.stream.as_str().to_string()));
    let fragment = format!(
        "(({expr}) {cmp} json_extract(?, '$') OR \
          (({expr}) = json_extract(?, '$') AND stream {cmp} ?))"
    );
    where_body.push_str(" AND ");
    where_body.push_str(&fragment);
    Ok(())
}

/// Return the SQL expression + effective sort direction for `order_by`
/// (defaulting to `updated_at DESC`), factored out so keyset pagination
/// can reference the same expression [`order_clause`] emits.
fn order_expr_and_direction(
    order_by: Option<&(String, Order)>,
) -> Result<(String, Order), StoreError> {
    match order_by {
        None => Ok(("updated_at".to_string(), Order::Desc)),
        Some((field, order)) => {
            validate_field_path(field)?;
            let expr = match field.as_str() {
                "updated_at" | "last_seq" | "stream" | "live" => field.clone(),
                _ => format!("json_extract(state, '$.{field}')"),
            };
            Ok((expr, *order))
        }
    }
}

/// Build an `ORDER BY <expr> <dir>` sort key. Known read-model columns sort
/// directly; anything else is treated as a dotted path into `state`.
pub(crate) fn order_clause(order_by: Option<&(String, Order)>) -> Result<String, StoreError> {
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
