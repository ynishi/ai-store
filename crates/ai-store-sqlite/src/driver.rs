//! Backend construction and lifecycle owner.
//!
//! `SqliteBackends::open` / `open_in_memory` spawn one rusqlite-isle actor,
//! apply the mandatory startup PRAGMAs plus every outstanding schema
//! migration (see `crate::migration`), and hand back a `SqliteBackends`
//! quadruple: the event backend, the cache backend, the checkpoint backend,
//! and the driver whose `shutdown` joins the SQLite thread cleanly.

use std::path::Path;

use ai_store_core::StoreError;
use rusqlite::Connection;
use rusqlite_isle::{AsyncIsle, AsyncIsleDriver, IsleError};

use crate::backend::{SqliteCacheBackend, SqliteCheckpointBackend, SqliteEventBackend};
use crate::migration;

/// Startup PRAGMAs. Deliberately kept out of `migration::MIGRATIONS`:
/// `journal_mode` in particular cannot be changed inside a transaction, and
/// all four are connection-scoped settings rather than schema state, so they
/// are re-applied on every open (autocommit, ahead of migrations) instead of
/// being tracked by `PRAGMA user_version`.
///
/// `busy_timeout = 5000` (5 s) matters most in multi-writer / multi-process
/// deployments — a second connection that hits an in-progress writer's
/// lock waits up to five seconds for the lock to clear rather than
/// surfacing `SQLITE_BUSY` immediately. SQLite's default is 0 (fail on
/// first contention); 5 s absorbs realistic local-IO stalls (fsync, a
/// concurrent maintenance transaction) while letting a genuine deadlock
/// surface promptly. Consumers that need a different value can override
/// this by opening the database themselves and re-issuing the PRAGMA before
/// handing the connection off; a first-class knob on `SqliteBackends` is a
/// carry (see the concurrency section in the crate-level rustdoc).
const STARTUP_PRAGMAS: &str = r#"
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous  = NORMAL;
    PRAGMA foreign_keys = ON;
    PRAGMA busy_timeout = 5000;
"#;

/// `init` closure passed to `AsyncIsle::spawn` / `open_in_memory`: applies
/// startup PRAGMAs, then runs the versioned migration chain. Runs once per
/// connection, before any application job is enqueued.
fn init_conn(conn: &mut Connection) -> rusqlite::Result<()> {
    conn.execute_batch(STARTUP_PRAGMAS)?;
    migration::apply(conn).map_err(|e| {
        // `migration::apply` reports failures as `StoreError`, but the
        // `init` closure signature (fixed by `rusqlite_isle::AsyncIsle`) is
        // `rusqlite::Result<()>`. `ToSqlConversionFailure` is reused here
        // purely as a generic "boxed error" carrier — the actual failure is
        // a migration error, not a `ToSql` conversion — so the message is
        // preserved verbatim rather than the variant name being meaningful.
        rusqlite::Error::ToSqlConversionFailure(e.to_string().into())
    })
}

/// Bundle of the three SPI backends plus their shared lifecycle owner.
///
/// The event, cache, and checkpoint backends share a single SQLite thread
/// (one connection, one writer) so append + cache put + checkpoint put stay
/// serialized on the same actor and never contend on separate locks.
pub struct SqliteBackends {
    /// The append-only event log backend.
    pub events: SqliteEventBackend,
    /// The materialization cache backend.
    pub cache: SqliteCacheBackend,
    /// The sink checkpoint backend (see [`ai_store_core::CheckpointBackend`]).
    pub checkpoints: SqliteCheckpointBackend,
    /// Lifecycle owner. Call `shutdown` when done to drain and join.
    pub driver: SqliteBackendDriver,
}

/// Lifecycle owner over the SQLite thread. Not `Clone`.
///
/// Drop the driver by calling [`shutdown`](Self::shutdown) to drain queued
/// work and join the thread cleanly. Dropping without `shutdown` is safe but
/// leaves the thread running as long as any backend handle is alive
/// (rusqlite-isle exits the thread when the last handle disconnects).
#[must_use = "call .shutdown().await for a clean thread join"]
pub struct SqliteBackendDriver {
    inner: AsyncIsleDriver,
}

impl SqliteBackendDriver {
    /// Drain queued jobs and join the SQLite thread.
    pub async fn shutdown(self) -> Result<(), StoreError> {
        self.inner
            .shutdown()
            .await
            .map_err(|e: IsleError| StoreError::Backend(e.to_string()))
    }
}

impl SqliteBackends {
    /// Open a database file, apply startup PRAGMAs plus every outstanding
    /// schema migration (see `crate::migration`), and return the backend
    /// quadruple.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let (isle, driver) = AsyncIsle::spawn(path.as_ref().to_path_buf(), init_conn)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(Self::from_isle(isle, driver))
    }

    /// Open an in-memory database (useful for tests). The database is
    /// discarded when the driver is shut down.
    pub async fn open_in_memory() -> Result<Self, StoreError> {
        let (isle, driver) = AsyncIsle::open_in_memory(init_conn)
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(Self::from_isle(isle, driver))
    }

    fn from_isle(isle: AsyncIsle, driver: AsyncIsleDriver) -> Self {
        Self {
            events: SqliteEventBackend::new(isle.clone()),
            cache: SqliteCacheBackend::new(isle.clone()),
            checkpoints: SqliteCheckpointBackend::new(isle),
            driver: SqliteBackendDriver { inner: driver },
        }
    }

    /// Borrow (clone) the `AsyncIsle` handle shared by every backend in this
    /// bundle.
    ///
    /// Use this to build an *additional*, opt-in backend on the same SQLite
    /// writer thread without spawning a second connection — e.g.
    /// `SqliteReadModel::new(backends.isle())`. `read_model` is deliberately
    /// not a field on `SqliteBackends` itself: unlike `events` / `cache` /
    /// `checkpoints`, it is an optional `ProjectionSink` a consumer opts into
    /// registering with `Store`, not a mandatory SPI backend every `Store`
    /// needs.
    pub fn isle(&self) -> AsyncIsle {
        self.events.isle()
    }
}
