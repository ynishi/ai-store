//! `PRAGMA user_version`-based stepwise schema migration runner.
//!
//! `MIGRATIONS[i]` is the SQL statement batch that steps the database from
//! `user_version = i` to `user_version = i + 1`. [`apply`] runs every
//! outstanding step in its own transaction (schema change + `user_version`
//! bump commit together, so a crash mid-migration can never leave
//! `user_version` ahead of the schema it actually applied) and rejects
//! databases whose `user_version` is newer than this build understands
//! (opening a database written by a newer `ai-store-sqlite` with an older
//! one).
//!
//! PRAGMAs that cannot run inside a transaction (`journal_mode`,
//! `synchronous`, `foreign_keys`) are deliberately **not** part of any
//! migration step — the caller applies them separately, in autocommit mode,
//! before calling [`apply`]. See `driver::init_conn`.

use ai_store_core::StoreError;
use rusqlite::Connection;

/// Ordered migration steps. `MIGRATIONS[i]` moves the schema from
/// `user_version = i` to `user_version = i + 1`.
pub(crate) const MIGRATIONS: &[&str] = &[
    // Migration 1 (index 0): baseline schema — events, labels, cache.
    //
    // Uses `CREATE TABLE IF NOT EXISTS` so it stays idempotent against
    // databases created before schema versioning existed: a pre-existing
    // database file has `user_version = 0` by SQLite default, and may
    // already contain these exact tables from the original one-shot
    // `SCHEMA` constant this migration runner replaced.
    r#"
        CREATE TABLE IF NOT EXISTS events (
            stream TEXT NOT NULL,
            seq    INTEGER NOT NULL,
            kind   TEXT NOT NULL,
            patch  TEXT NOT NULL,
            meta   TEXT NOT NULL,
            at_ms  INTEGER NOT NULL,
            PRIMARY KEY (stream, seq)
        );
        CREATE INDEX IF NOT EXISTS ix_events_stream_at ON events(stream, at_ms);

        CREATE TABLE IF NOT EXISTS labels (
            stream TEXT NOT NULL,
            name   TEXT NOT NULL,
            at_seq INTEGER NOT NULL,
            PRIMARY KEY (stream, name)
        );

        CREATE TABLE IF NOT EXISTS cache (
            stream TEXT NOT NULL,
            at_seq INTEGER NOT NULL,
            state  TEXT NOT NULL,
            PRIMARY KEY (stream, at_seq)
        );
    "#,
    // Migration 2 (index 1): sink checkpoint persistence.
    //
    // Backs `SqliteCheckpointBackend` (see `crate::backend`), which lets
    // `ai_store_core::Store::with_checkpoint_backend` survive sink
    // checkpoints across process restarts.
    r#"
        CREATE TABLE IF NOT EXISTS sink_checkpoints (
            sink_id TEXT NOT NULL,
            stream  TEXT NOT NULL,
            at_seq  INTEGER NOT NULL,
            PRIMARY KEY (sink_id, stream)
        );
    "#,
    // Migration 3 (index 2): queryable read-model projection table.
    //
    // Backs `SqliteReadModel` (see `crate::read_model`), an opt-in
    // `ProjectionSink` that materializes the latest state of every stream
    // into one row each, queryable with `json_extract`-based filters instead
    // of full event-log replay. `live` supports an optional tombstone
    // convention (see `SqliteReadModel::with_tombstone_kind`); it defaults to
    // 1 (live) for streams that never opt into tombstoning.
    r#"
        CREATE TABLE IF NOT EXISTS read_model (
            stream     TEXT NOT NULL PRIMARY KEY,
            state      TEXT NOT NULL,
            last_seq   INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            live       INTEGER NOT NULL DEFAULT 1
        );
        CREATE INDEX IF NOT EXISTS ix_read_model_updated ON read_model(updated_at);
    "#,
    // Migration 4 (index 3): database-level append-only enforcement on
    // `events`.
    //
    // `ai_store_core`'s crate docs already state the invariant this backs:
    // `EventBackend` exposes no `delete`/`overwrite` method, so no code path
    // in this crate ever issues an `UPDATE`/`DELETE` against `events` (see
    // `insert_event` in `crate::backend`, the sole writer). That is a
    // guarantee about *this crate's* API surface, not about the database
    // file itself — a raw SQL client, a second process opening the same
    // file, or a manual `sqlite3` session could still mutate history. These
    // two triggers close that gap at the storage layer: any `UPDATE` or
    // `DELETE` on `events`, from any connection, aborts the statement before
    // it runs.
    //
    // `ai_store_core::Store::revert` (and `revert_with_meta`) are unaffected
    // — a revert is implemented as a new `INSERT` (the row for the
    // reverse-diff event), never an `UPDATE`/`DELETE` of an existing row, so
    // it commutes with both triggers unchanged.
    //
    // Scoped to `events` only. `labels` (upserted/deleted by `label_set` /
    // `label_delete`), `cache` (pruned by `prune`), `sink_checkpoints`
    // (advanced in place by checkpoint `put`), and `read_model` (upserted by
    // `SqliteReadModel::commit`) are all mutable-by-design derived state —
    // none of them carry the append-only invariant `events` does, so no
    // trigger is added to them. Always on (not feature-gated): no call path
    // in this crate ever needs to mutate `events`, so the trigger can never
    // reject a legitimate operation. A future migration that needs to relax
    // this (e.g. log compaction) would `DROP TRIGGER` in its own step.
    r#"
        CREATE TRIGGER IF NOT EXISTS trg_events_no_update
        BEFORE UPDATE ON events
        BEGIN
            SELECT RAISE(ABORT, 'ai-store events are append-only (UPDATE denied)');
        END;

        CREATE TRIGGER IF NOT EXISTS trg_events_no_delete
        BEFORE DELETE ON events
        BEGIN
            SELECT RAISE(ABORT, 'ai-store events are append-only (DELETE denied)');
        END;
    "#,
];

/// Apply every outstanding migration to `conn`, tracked via
/// `PRAGMA user_version`.
///
/// Idempotent: calling this on an already-migrated connection is a no-op
/// (the loop below starts at the connection's current `user_version` and
/// `MIGRATIONS.iter().skip(current)` is empty once `current == MIGRATIONS.len()`).
///
/// # Errors
///
/// Returns [`StoreError::Backend`] if:
/// - reading or bumping `PRAGMA user_version` fails,
/// - a migration step's SQL fails to apply, or
/// - `conn`'s `user_version` is already greater than `MIGRATIONS.len()` —
///   this build of `ai-store-sqlite` does not know how to open a database
///   written by a newer version.
pub(crate) fn apply(conn: &mut Connection) -> Result<(), StoreError> {
    let current: i64 = conn
        .query_row("PRAGMA user_version", [], |r| r.get(0))
        .map_err(|e| StoreError::Backend(format!("read user_version: {e}")))?;
    let current = current as usize;
    let total = MIGRATIONS.len();

    if current > total {
        return Err(StoreError::Backend(format!(
            "database schema version {current} is newer than this build of \
             ai-store-sqlite supports ({total} known migrations); refusing to open"
        )));
    }

    for (i, step) in MIGRATIONS.iter().enumerate().skip(current) {
        let step_num = i + 1;
        let tx = conn
            .transaction()
            .map_err(|e| StoreError::Backend(format!("begin migration {step_num}: {e}")))?;
        tx.execute_batch(step)
            .map_err(|e| StoreError::Backend(format!("apply migration {step_num}: {e}")))?;
        // `user_version` is a validated array index (`step_num <=
        // MIGRATIONS.len()`), never caller-controlled input, so a bound
        // parameter isn't needed here — `PRAGMA user_version = N` doesn't
        // accept one anyway.
        tx.pragma_update(None, "user_version", step_num as i64)
            .map_err(|e| StoreError::Backend(format!("bump user_version to {step_num}: {e}")))?;
        tx.commit()
            .map_err(|e| StoreError::Backend(format!("commit migration {step_num}: {e}")))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user_version(conn: &Connection) -> i64 {
        conn.query_row("PRAGMA user_version", [], |r| r.get(0))
            .unwrap()
    }

    #[test]
    fn fresh_database_reaches_the_latest_migration() {
        let mut conn = Connection::open_in_memory().unwrap();
        apply(&mut conn).unwrap();
        assert_eq!(user_version(&conn), MIGRATIONS.len() as i64);
    }

    #[test]
    fn applying_twice_is_idempotent() {
        let mut conn = Connection::open_in_memory().unwrap();
        apply(&mut conn).unwrap();
        apply(&mut conn).unwrap();
        assert_eq!(user_version(&conn), MIGRATIONS.len() as i64);

        // Re-applying did not error on (or duplicate) the already-created
        // baseline table.
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'events'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn preexisting_unversioned_tables_are_adopted_idempotently() {
        // Simulates a database created before schema versioning existed:
        // `user_version` defaults to 0, but the baseline tables already
        // exist (created by the original one-shot `SCHEMA` constant).
        let mut conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(MIGRATIONS[0]).unwrap();
        assert_eq!(user_version(&conn), 0);

        apply(&mut conn).unwrap();
        assert_eq!(user_version(&conn), MIGRATIONS.len() as i64);

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'sink_checkpoints'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn fresh_database_lands_the_read_model_table_and_index() {
        let mut conn = Connection::open_in_memory().unwrap();
        apply(&mut conn).unwrap();

        let table_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'read_model'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(table_count, 1);

        let index_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'ix_read_model_updated'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(index_count, 1);
    }

    #[test]
    fn fresh_database_lands_the_events_immutability_triggers() {
        let mut conn = Connection::open_in_memory().unwrap();
        apply(&mut conn).unwrap();

        let trigger_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'trigger' \
                 AND name IN ('trg_events_no_update', 'trg_events_no_delete')",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(trigger_count, 2);
    }

    #[test]
    fn update_on_events_is_rejected_by_the_trigger() {
        let mut conn = Connection::open_in_memory().unwrap();
        apply(&mut conn).unwrap();
        conn.execute_batch(
            "INSERT INTO events (stream, seq, kind, patch, meta, at_ms) \
             VALUES ('doc', 1, 'init', '[]', '{}', 0)",
        )
        .unwrap();

        let err = conn
            .execute("UPDATE events SET kind = 'tampered' WHERE seq = 1", [])
            .unwrap_err();
        assert!(
            err.to_string().contains("append-only"),
            "expected an append-only rejection, got: {err}"
        );

        // The row is untouched — the trigger aborted before the write.
        let kind: String = conn
            .query_row("SELECT kind FROM events WHERE seq = 1", [], |r| r.get(0))
            .unwrap();
        assert_eq!(kind, "init");
    }

    #[test]
    fn delete_on_events_is_rejected_by_the_trigger() {
        let mut conn = Connection::open_in_memory().unwrap();
        apply(&mut conn).unwrap();
        conn.execute_batch(
            "INSERT INTO events (stream, seq, kind, patch, meta, at_ms) \
             VALUES ('doc', 1, 'init', '[]', '{}', 0)",
        )
        .unwrap();

        let err = conn
            .execute("DELETE FROM events WHERE seq = 1", [])
            .unwrap_err();
        assert!(
            err.to_string().contains("append-only"),
            "expected an append-only rejection, got: {err}"
        );

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM events", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn migrating_an_existing_v3_database_lands_the_triggers() {
        // Simulates a database written before migration 4 existed:
        // migrations 1-3 already applied, `user_version = 3`.
        let mut conn = Connection::open_in_memory().unwrap();
        for step in &MIGRATIONS[0..3] {
            conn.execute_batch(step).unwrap();
        }
        conn.pragma_update(None, "user_version", 3i64).unwrap();
        assert_eq!(user_version(&conn), 3);

        apply(&mut conn).unwrap();
        assert_eq!(user_version(&conn), MIGRATIONS.len() as i64);

        conn.execute_batch(
            "INSERT INTO events (stream, seq, kind, patch, meta, at_ms) \
             VALUES ('doc', 1, 'init', '[]', '{}', 0)",
        )
        .unwrap();
        let err = conn
            .execute("DELETE FROM events WHERE seq = 1", [])
            .unwrap_err();
        assert!(err.to_string().contains("append-only"));
    }

    #[test]
    fn future_schema_version_is_rejected() {
        let mut conn = Connection::open_in_memory().unwrap();
        apply(&mut conn).unwrap();
        conn.pragma_update(None, "user_version", (MIGRATIONS.len() as i64) + 1)
            .unwrap();

        let err = apply(&mut conn).unwrap_err();
        match err {
            StoreError::Backend(msg) => assert!(
                msg.contains("newer"),
                "expected a 'newer than supported' message, got: {msg}"
            ),
            other => panic!("expected StoreError::Backend, got {other:?}"),
        }
    }
}
