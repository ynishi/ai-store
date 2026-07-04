//! Synchronous [`ProjectionSink`] bridge.
//!
//! [`SyncProjectionSink`] is the sibling of [`ProjectionSink`] with sync
//! method signatures. [`BlockingSink`] wraps an implementation of
//! [`SyncProjectionSink`] and exposes it to the async facade.
//!
//! ## Two dispatch modes
//!
//! - [`Dispatch::SpawnBlocking`] (default via [`BlockingSink::new`]) hands
//!   each `commit` / `on_label_set` off to `tokio::task::spawn_blocking`. Use
//!   this whenever the sync method may block for more than a few hundred
//!   microseconds — file I/O, `fsync`, database drivers with only sync APIs.
//!   Requires a runtime with a blocking pool (any `tokio::runtime` built via
//!   `Builder::new_multi_thread()` or `Builder::new_current_thread()` with
//!   `enable_all()` has one).
//!
//! - [`Dispatch::Inline`] (via [`BlockingSink::inline`]) runs the sync method
//!   directly on the async worker. Correct only for fast in-memory
//!   bookkeeping — anything else stalls the runtime.
//!
//! ## Idempotence
//!
//! [`SyncProjectionSink`] inherits the same idempotence contract as
//! [`ProjectionSink`]: replaying the same `(stream, seq)` must produce the
//! same effect as the first application, because `Store::catch_up` and
//! `Store::rebuild` may re-drive events after a crash or configuration
//! change.

use std::sync::Arc;

use ai_store_core::{Event, Label, ProjectionSink, Seq, StoreError, StreamId};
use async_trait::async_trait;
use serde_json::Value;

/// Synchronous variant of [`ProjectionSink`]. Implement this when the sink's
/// body is inherently blocking (file I/O, synchronous database drivers,
/// stdlib `println!`), then wrap in [`BlockingSink`] to plug into the async
/// facade.
pub trait SyncProjectionSink: Send + Sync + 'static {
    /// Stable identifier used as the checkpoint key. See
    /// [`ProjectionSink::id`].
    fn id(&self) -> &str;

    /// Apply one committed event. Must be idempotent under retries.
    fn commit(
        &self,
        stream: &StreamId,
        seq: Seq,
        state: &Value,
        event: &Event,
    ) -> Result<(), StoreError>;

    /// React to a label being pinned or moved. Default is a no-op, matching
    /// the async trait's default.
    fn on_label_set(
        &self,
        _stream: &StreamId,
        _label: &Label,
        _at: Seq,
        _state: &Value,
    ) -> Result<(), StoreError> {
        Ok(())
    }

    /// React to a label being deleted. Default is a no-op, matching the
    /// async trait's default.
    fn on_label_deleted(&self, _stream: &StreamId, _label: &Label) -> Result<(), StoreError> {
        Ok(())
    }
}

/// How [`BlockingSink`] hands off calls to the wrapped
/// [`SyncProjectionSink`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dispatch {
    /// Run the sync method inline on the async worker.
    ///
    /// Safe only when the method is guaranteed to return in a few hundred
    /// microseconds or less. Anything else blocks the runtime.
    Inline,
    /// Offload to `tokio::task::spawn_blocking`.
    ///
    /// The default. Correct for any sink that touches the file system,
    /// synchronous DB drivers, or otherwise blocking code.
    SpawnBlocking,
}

/// Adapter turning a [`SyncProjectionSink`] into a [`ProjectionSink`].
pub struct BlockingSink<T: SyncProjectionSink> {
    inner: Arc<T>,
    dispatch: Dispatch,
}

impl<T: SyncProjectionSink> BlockingSink<T> {
    /// Wrap `inner` in `spawn_blocking` dispatch — the safe default for any
    /// sink that may block.
    pub fn new(inner: T) -> Self {
        Self {
            inner: Arc::new(inner),
            dispatch: Dispatch::SpawnBlocking,
        }
    }

    /// Wrap `inner` in inline dispatch. Use only for fast in-memory sinks;
    /// see [`Dispatch::Inline`].
    pub fn inline(inner: T) -> Self {
        Self {
            inner: Arc::new(inner),
            dispatch: Dispatch::Inline,
        }
    }

    /// Access the wrapped sink. Useful for tests that need to peek at
    /// accumulated state after the async dispatch loop advanced the
    /// checkpoint.
    pub fn inner(&self) -> &Arc<T> {
        &self.inner
    }
}

#[async_trait]
impl<T: SyncProjectionSink> ProjectionSink for BlockingSink<T> {
    fn id(&self) -> &str {
        // `id()` on the trait returns a borrowed `&str` from `T`. Since
        // `self.inner: Arc<T>` is stable for the lifetime of `&self`, this
        // borrow is sound.
        self.inner.id()
    }

    async fn commit(
        &self,
        stream: &StreamId,
        seq: Seq,
        state: &Value,
        event: &Event,
    ) -> Result<(), StoreError> {
        match self.dispatch {
            Dispatch::Inline => self.inner.commit(stream, seq, state, event),
            Dispatch::SpawnBlocking => {
                let inner = Arc::clone(&self.inner);
                let stream = stream.clone();
                let state = state.clone();
                let event = event.clone();
                tokio::task::spawn_blocking(move || inner.commit(&stream, seq, &state, &event))
                    .await
                    .map_err(|e| StoreError::Backend(format!("blocking sink join: {e}")))?
            }
        }
    }

    async fn on_label_set(
        &self,
        stream: &StreamId,
        label: &Label,
        at: Seq,
        state: &Value,
    ) -> Result<(), StoreError> {
        match self.dispatch {
            Dispatch::Inline => self.inner.on_label_set(stream, label, at, state),
            Dispatch::SpawnBlocking => {
                let inner = Arc::clone(&self.inner);
                let stream = stream.clone();
                let label = label.clone();
                let state = state.clone();
                tokio::task::spawn_blocking(move || inner.on_label_set(&stream, &label, at, &state))
                    .await
                    .map_err(|e| StoreError::Backend(format!("blocking sink join: {e}")))?
            }
        }
    }

    async fn on_label_deleted(&self, stream: &StreamId, label: &Label) -> Result<(), StoreError> {
        match self.dispatch {
            Dispatch::Inline => self.inner.on_label_deleted(stream, label),
            Dispatch::SpawnBlocking => {
                let inner = Arc::clone(&self.inner);
                let stream = stream.clone();
                let label = label.clone();
                tokio::task::spawn_blocking(move || inner.on_label_deleted(&stream, &label))
                    .await
                    .map_err(|e| StoreError::Backend(format!("blocking sink join: {e}")))?
            }
        }
    }
}
