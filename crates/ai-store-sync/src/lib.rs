#![warn(missing_docs)]

//! # ai-store-sync
//!
//! Blocking (synchronous) facade over [`ai_store_core::Store`].
//!
//! ## Why
//!
//! `Store` is async by design; the SPI traits (`EventBackend`, `CacheBackend`,
//! `ProjectionSink`) are `async_trait`. Consumers embedded in an otherwise
//! synchronous codebase would otherwise have to spin up a `tokio::Runtime` and
//! wrap every call in `runtime.block_on(...)`, and get the details right
//! (current-thread vs multi-thread, runtime lifetime, `Send` bounds).
//!
//! [`BlockingStore`] does that once, in-tree.
//!
//! ## Choosing a constructor
//!
//! - [`BlockingStore::new`] — owns a dedicated `current_thread` runtime. Use
//!   this from a plain synchronous `main` / thread when the caller has no
//!   tokio runtime of its own. Analogous to `reqwest::blocking::Client::new`.
//! - [`BlockingStore::with_handle`] — borrows an existing [`tokio::runtime::Handle`].
//!   Use this when the surrounding process already runs a runtime (e.g. a
//!   library that hosts tokio internally and hands a `Handle` down to sync
//!   plugin code).
//!
//! ## Nested-runtime pitfall
//!
//! Do **not** call a `BlockingStore` method from inside an async task on the
//! same tokio runtime — that would attempt to `block_on` from within a runtime
//! worker and will panic. If you must bridge from async code:
//!
//! - prefer calling `Store` directly with `.await`, or
//! - wrap the blocking call in
//!   [`tokio::task::block_in_place`](https://docs.rs/tokio/latest/tokio/task/fn.block_in_place.html)
//!   on a multi-thread runtime.
//!
//! ## Errors
//!
//! All methods return [`StoreError`] verbatim from the async facade. No new
//! error variants are introduced.

mod sink;

pub use sink::{BlockingSink, Dispatch, SyncProjectionSink};

use std::sync::Arc;

use ai_store_core::{Event, Label, Patch, Seq, Store, StoreError, StreamId, Timestamp};
use serde_json::Value;
use tokio::runtime::{Handle, Runtime};

/// How the blocking facade drives async calls.
enum Driver {
    /// Owned `current_thread` runtime, created by [`BlockingStore::new`].
    Owned(Runtime),
    /// Borrowed handle from a runtime the caller already runs.
    Borrowed(Handle),
}

impl Driver {
    fn block_on<F: std::future::Future>(&self, fut: F) -> F::Output {
        match self {
            Driver::Owned(rt) => rt.block_on(fut),
            Driver::Borrowed(h) => h.block_on(fut),
        }
    }
}

/// Synchronous mirror of [`Store`].
///
/// Cheap to clone: only the inner `Store` handle is `Arc`-shared. The runtime
/// driver is shared through `Arc` as well, so cloned handles all drive the
/// same runtime.
pub struct BlockingStore {
    inner: Store,
    driver: Arc<Driver>,
}

impl Clone for BlockingStore {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            driver: Arc::clone(&self.driver),
        }
    }
}

impl BlockingStore {
    /// Build a `BlockingStore` that owns a dedicated `current_thread` tokio
    /// runtime.
    ///
    /// Fails only if the runtime cannot be constructed (rare — typically an
    /// exhausted file descriptor budget). See the crate-level docs for the
    /// nested-runtime pitfall.
    pub fn new(store: Store) -> std::io::Result<Self> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        Ok(Self {
            inner: store,
            driver: Arc::new(Driver::Owned(rt)),
        })
    }

    /// Build a `BlockingStore` that drives calls on the caller's runtime,
    /// identified by a [`Handle`].
    ///
    /// Prefer this constructor when the surrounding process already owns a
    /// tokio runtime — reusing the handle avoids spawning a second one.
    pub fn with_handle(store: Store, handle: Handle) -> Self {
        Self {
            inner: store,
            driver: Arc::new(Driver::Borrowed(handle)),
        }
    }

    /// Access the underlying async [`Store`]. Useful when a caller needs to
    /// mix async and blocking paths, or hand the async handle to a
    /// concurrently-running task.
    pub fn as_async(&self) -> &Store {
        &self.inner
    }

    /// See [`Store::append`].
    pub fn append(
        &self,
        stream: &StreamId,
        kind: &str,
        patch: Patch,
        meta: Value,
    ) -> Result<Seq, StoreError> {
        self.driver
            .block_on(self.inner.append(stream, kind, patch, meta))
    }

    /// See [`Store::state`].
    pub fn state(&self, stream: &StreamId) -> Result<Value, StoreError> {
        self.driver.block_on(self.inner.state(stream))
    }

    /// See [`Store::state_at`].
    pub fn state_at(&self, stream: &StreamId, at: Seq) -> Result<Value, StoreError> {
        self.driver.block_on(self.inner.state_at(stream, at))
    }

    /// See [`Store::revert`].
    pub fn revert(&self, stream: &StreamId, to: Seq) -> Result<Seq, StoreError> {
        self.driver.block_on(self.inner.revert(stream, to))
    }

    /// See [`Store::read`].
    pub fn read(
        &self,
        stream: &StreamId,
        from: Seq,
        limit: usize,
    ) -> Result<Vec<Event>, StoreError> {
        self.driver.block_on(self.inner.read(stream, from, limit))
    }

    /// See [`Store::read_by_meta`].
    pub fn read_by_meta(
        &self,
        stream: &StreamId,
        field: &str,
        value: &Value,
        from: Seq,
        limit: usize,
    ) -> Result<Vec<Event>, StoreError> {
        self.driver
            .block_on(self.inner.read_by_meta(stream, field, value, from, limit))
    }

    /// See [`Store::head`].
    pub fn head(&self, stream: &StreamId) -> Result<Option<Seq>, StoreError> {
        self.driver.block_on(self.inner.head(stream))
    }

    /// See [`Store::seq_at_time`].
    pub fn seq_at_time(&self, stream: &StreamId, at: Timestamp) -> Result<Option<Seq>, StoreError> {
        self.driver.block_on(self.inner.seq_at_time(stream, at))
    }

    /// See [`Store::streams`].
    pub fn streams(&self) -> Result<Vec<StreamId>, StoreError> {
        self.driver.block_on(self.inner.streams())
    }

    /// See [`Store::label_set`].
    pub fn label_set(&self, stream: &StreamId, label: &Label, at: Seq) -> Result<(), StoreError> {
        self.driver
            .block_on(self.inner.label_set(stream, label, at))
    }

    /// See [`Store::label_resolve`].
    pub fn label_resolve(&self, stream: &StreamId, label: &Label) -> Result<Seq, StoreError> {
        self.driver
            .block_on(self.inner.label_resolve(stream, label))
    }

    /// See [`Store::labels`].
    pub fn labels(&self, stream: &StreamId) -> Result<Vec<(Label, Seq)>, StoreError> {
        self.driver.block_on(self.inner.labels(stream))
    }

    /// See [`Store::catch_up`].
    pub fn catch_up(&self, sink_id: &str) -> Result<ai_store_core::CatchUpReport, StoreError> {
        self.driver.block_on(self.inner.catch_up(sink_id))
    }

    /// See [`Store::rebuild`].
    pub fn rebuild(&self, sink_id: &str) -> Result<ai_store_core::CatchUpReport, StoreError> {
        self.driver.block_on(self.inner.rebuild(sink_id))
    }
}
