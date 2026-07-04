//! Backend construction and lifecycle owner.
//!
//! `SqliteBackends::open` / `open_in_memory` spawn one rusqlite-isle actor,
//! apply the schema, and hand back a `SqliteBackends` triple: the event
//! backend, the cache backend, and the driver whose `shutdown` joins the
//! SQLite thread cleanly.

use std::path::Path;

use ai_store_core::StoreError;
use rusqlite_isle::{AsyncIsle, AsyncIsleDriver, IsleError};

use crate::backend::{SqliteCacheBackend, SqliteEventBackend};

const SCHEMA: &str = r#"
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous  = NORMAL;
    PRAGMA foreign_keys = ON;

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
"#;

/// Bundle of the two SPI backends plus their shared lifecycle owner.
///
/// The event and cache backend share a single SQLite thread (one connection,
/// one writer) so append + cache put stay serialized on the same actor and
/// never contend on separate locks.
pub struct SqliteBackends {
    /// The append-only event log backend.
    pub events: SqliteEventBackend,
    /// The materialization cache backend.
    pub cache: SqliteCacheBackend,
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
    /// Open a database file, apply the ai-store schema, and return the
    /// backend triple.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let (isle, driver) = AsyncIsle::spawn(path.as_ref().to_path_buf(), |conn| {
            conn.execute_batch(SCHEMA)
        })
        .await
        .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(Self::from_isle(isle, driver))
    }

    /// Open an in-memory database (useful for tests). The database is
    /// discarded when the driver is shut down.
    pub async fn open_in_memory() -> Result<Self, StoreError> {
        let (isle, driver) = AsyncIsle::open_in_memory(|conn| conn.execute_batch(SCHEMA))
            .await
            .map_err(|e| StoreError::Backend(e.to_string()))?;
        Ok(Self::from_isle(isle, driver))
    }

    fn from_isle(isle: AsyncIsle, driver: AsyncIsleDriver) -> Self {
        Self {
            events: SqliteEventBackend::new(isle.clone()),
            cache: SqliteCacheBackend::new(isle),
            driver: SqliteBackendDriver { inner: driver },
        }
    }
}
