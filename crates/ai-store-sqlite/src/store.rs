//! One-shot SQLite-backed `Store` (backends + driver + read-model handle in
//! a single type).
//!
//! ## Why
//!
//! Wiring a SQLite-backed `Store` by hand is a four-step assembly: open
//! `SqliteBackends`, `Arc`-wrap `events`/`cache`, pick
//! `Store::with_checkpoint_backend` over `Store::new` to get durable
//! checkpoints, and separately keep the `SqliteBackendDriver` alive for a
//! clean shutdown later — every consumer that wants a SQLite `Store` ends up
//! reimplementing that assembly, typically as its own ad hoc "store entry"
//! struct just to keep the driver alive alongside the `Store` handle (the
//! exact shape this type replaces). `SqliteStore` performs that assembly
//! once: `SqliteStore::open` is the shortest path from "a path on disk" to a
//! ready-to-use, checkpoint-durable `Store`.

use std::ops::Deref;
use std::path::Path;
use std::sync::Arc;

use ai_store_core::{CheckpointBackend, Store, StoreBuilder, StoreError};
use rusqlite_isle::AsyncIsle;

use crate::driver::{SqliteBackendDriver, SqliteBackends};
use crate::read_model::SqliteReadModel;

/// A SQLite-backed [`Store`] bundled with its driver and the shared
/// `AsyncIsle` handle needed to build a [`SqliteReadModel`].
///
/// Derefs to [`Store`], so most callers can use a `SqliteStore` exactly like
/// a `Store` without ever naming this type again. [`SqliteStore::store`] is
/// available for call sites that need an owned `&Store` reference
/// explicitly (e.g. to store alongside other fields without also carrying
/// the driver).
pub struct SqliteStore {
    store: Store,
    driver: SqliteBackendDriver,
    isle: AsyncIsle,
}

impl SqliteStore {
    /// Open a database file and build a `Store` with durable checkpoints
    /// (`events`/`cache`/`checkpoints` wired exactly as
    /// [`SqliteBackends::open`] would produce them), no gates, no
    /// application sinks.
    ///
    /// Use [`SqliteStore::open_with`] to register application-defined gates
    /// or sinks. [`SqliteStore::read_model`] is available afterward for
    /// direct queries against this store's SQLite thread — see that
    /// method's docs for what it does *not* do (register itself as an
    /// automatic `Store` sink).
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::open_with(path, |b| b).await
    }

    /// Open a database file, then let `f` extend the [`StoreBuilder`]
    /// (register gates, sinks) before it is built. See
    /// [`SqliteStore::open_in_memory_with`] for a runnable example of the
    /// same callback shape.
    pub async fn open_with(
        path: impl AsRef<Path>,
        f: impl FnOnce(StoreBuilder) -> StoreBuilder,
    ) -> Result<Self, StoreError> {
        let backends = SqliteBackends::open(path).await?;
        Ok(Self::from_backends(backends, f))
    }

    /// Open an in-memory database (test / throwaway use). Equivalent to
    /// [`SqliteStore::open`] with no extra gates/sinks.
    pub async fn open_in_memory() -> Result<Self, StoreError> {
        Self::open_in_memory_with(|b| b).await
    }

    /// Open an in-memory database, then let `f` extend the [`StoreBuilder`]
    /// before it is built.
    ///
    /// ```
    /// # async fn demo() -> Result<(), ai_store_core::StoreError> {
    /// use ai_store_sqlite::SqliteStore;
    ///
    /// // The common case — no extra gates/sinks — returns `builder`
    /// // unchanged; `open`/`open_in_memory` are exactly this.
    /// let store = SqliteStore::open_in_memory_with(|builder| builder).await?;
    /// let _ = store.read_model();
    /// store.shutdown().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn open_in_memory_with(
        f: impl FnOnce(StoreBuilder) -> StoreBuilder,
    ) -> Result<Self, StoreError> {
        let backends = SqliteBackends::open_in_memory().await?;
        Ok(Self::from_backends(backends, f))
    }

    fn from_backends(
        backends: SqliteBackends,
        f: impl FnOnce(StoreBuilder) -> StoreBuilder,
    ) -> Self {
        // `isle` must be cloned before `backends` is destructured below —
        // `SqliteBackends::isle` borrows `self` via the `events` field it
        // still owns at this point.
        let isle = backends.isle();
        let SqliteBackends {
            events,
            cache,
            checkpoints,
            driver,
        } = backends;
        let checkpoints: Arc<dyn CheckpointBackend> = Arc::new(checkpoints);
        let builder = Store::builder(Arc::new(events), Arc::new(cache)).checkpoints(checkpoints);
        let store = f(builder).build();
        Self {
            store,
            driver,
            isle,
        }
    }

    /// Borrow the underlying [`Store`] explicitly. Prefer using a
    /// `SqliteStore` directly via `Deref` unless an owned `&Store` reference
    /// (independent of the driver) is specifically what's needed.
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Build a [`SqliteReadModel`] sharing this `SqliteStore`'s SQLite
    /// thread, for direct queries (`query`/`get`/`tail`/`count`).
    ///
    /// This does **not** register the read model as a `Store` sink — `Store`
    /// fixes its sink list at construction time (there is no
    /// "add a sink to an already-built `Store`" method), and by the time
    /// this method can be called the `SqliteStore` — and the `Store` inside
    /// it — already exists. A read model built this way stays queryable but
    /// silent: it only reflects history you drive into it explicitly via
    /// [`ai_store_core::Store::materialize_to_sink`] or
    /// [`ai_store_core::Store::rebuild`] (using [`ProjectionSink::id`] as
    /// the `sink_id`), never the automatic post-`append` dispatch a sink
    /// registered *before* `build()` gets.
    ///
    /// To wire a read model as an automatic sink from the very first write,
    /// assemble manually via [`SqliteBackends`] + [`ai_store_core::Store::builder`]
    /// instead of `SqliteStore` — see the crate-level "Read-model projection"
    /// docs — since [`SqliteStore::open_with`]'s callback runs before this
    /// store's own `AsyncIsle` handle exists on `self`, so it has no way to
    /// hand a `SqliteReadModel` sharing *this* store's thread to itself.
    ///
    /// [`ProjectionSink::id`]: ai_store_core::ProjectionSink::id
    pub fn read_model(&self) -> SqliteReadModel {
        SqliteReadModel::new(self.isle.clone())
    }

    /// Drain queued jobs and join the SQLite thread — the graceful shutdown
    /// [`SqliteBackendDriver::shutdown`] provides, without the caller having
    /// to keep a separate driver handle alive alongside the `Store` just for
    /// this one call.
    pub async fn shutdown(self) -> Result<(), StoreError> {
        self.driver.shutdown().await
    }
}

impl Deref for SqliteStore {
    type Target = Store;

    fn deref(&self) -> &Store {
        &self.store
    }
}
