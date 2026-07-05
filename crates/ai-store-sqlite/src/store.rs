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
    /// The `SqliteReadModel` that was registered as a sink at build time.
    /// [`SqliteStore::read_model`] returns a clone of this so consumers
    /// see the queryable view of the same events the `Store` is
    /// dispatching to.
    read_model: SqliteReadModel,
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
        // Register a default read model as a sink at build time — the
        // one-shot entry point (`SqliteStore::open`) has always been the
        // path most callers reach for, and returning an unregistered read
        // model from `.read_model()` invited a silent "query is always
        // empty" failure mode (see the module doc). Registering here
        // means the read model reflects every subsequent append via the
        // ordinary sink dispatch, no extra wiring needed.
        //
        // Consumers that want an *un*registered read model (a separate
        // checkpoint scope, or a read model driven manually by
        // `materialize_to_sink`) can call `SqliteStore::read_model_detached`
        // — that path builds a fresh instance with a distinct sink id, so
        // it does not conflict with the one registered here.
        let read_model = SqliteReadModel::new(isle.clone());
        let builder = Store::builder(Arc::new(events), Arc::new(cache))
            .checkpoints(checkpoints)
            .sink(Arc::new(read_model.clone()));
        let store = f(builder).build();
        Self {
            store,
            driver,
            isle,
            read_model,
        }
    }

    /// Borrow the underlying [`Store`] explicitly. Prefer using a
    /// `SqliteStore` directly via `Deref` unless an owned `&Store` reference
    /// (independent of the driver) is specifically what's needed.
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Return a handle to the [`SqliteReadModel`] that this `SqliteStore`
    /// registered as a `Store` sink at build time.
    ///
    /// Because the read model is registered as an ordinary sink, every
    /// subsequent [`ai_store_core::Store::append`] on this `SqliteStore`
    /// is dispatched to it via the same post-commit path any other sink
    /// gets — the returned handle observes appends automatically. No
    /// explicit [`ai_store_core::Store::rebuild`] or
    /// [`ai_store_core::Store::materialize_to_sink`] is needed for the
    /// common one-shot case.
    ///
    /// Every call returns a clone of the same underlying handle (they all
    /// share this store's `AsyncIsle`), so a caller that wants to keep a
    /// queryable read model around alongside other work can hold onto the
    /// return value freely.
    ///
    /// Callers that need an *un*registered read model — a separate
    /// checkpoint scope, or an instance driven manually via
    /// `materialize_to_sink` / `rebuild` without being fed by the write
    /// path — should use [`SqliteStore::read_model_detached`] instead.
    pub fn read_model(&self) -> SqliteReadModel {
        self.read_model.clone()
    }

    /// Return a fresh, **un**registered [`SqliteReadModel`] sharing this
    /// store's SQLite thread but not wired into the sink dispatch path.
    ///
    /// The returned handle carries a distinct sink id
    /// (`"read-model:detached"`) so it can coexist with the one
    /// [`SqliteStore::read_model`] returns without checkpoint collisions.
    /// A detached read model only reflects history that a caller drives
    /// into it explicitly through
    /// [`ai_store_core::Store::materialize_to_sink`] or
    /// [`ai_store_core::Store::rebuild`] (using
    /// [`ai_store_core::ProjectionSink::id`] as the `sink_id`) — automatic
    /// post-`append` dispatch never touches it.
    ///
    /// Reach for this in three cases:
    ///
    /// - A view of only a subset of the log's history (drive
    ///   `materialize_to_sink` for the streams the consumer cares about).
    /// - A checkpoint scope independent of the auto-registered read
    ///   model's — e.g. a separate consumer that wants to drive queries
    ///   from a different lag position.
    /// - Building a `SqliteReadModel` for use outside a `SqliteStore`
    ///   context (though [`crate::SqliteBackends::isle`] + a manual
    ///   [`ai_store_core::Store::builder`] assembly is usually cleaner for
    ///   that use case).
    pub fn read_model_detached(&self) -> SqliteReadModel {
        SqliteReadModel::new(self.isle.clone()).with_id("read-model:detached")
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
