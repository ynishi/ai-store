//! In-memory `CheckpointBackend` implementation.
//!
//! Checkpoints are held in a single `HashMap<(String, StreamId), Seq>`
//! guarded by a `tokio::sync::Mutex`, mirroring `MemEventBackend` /
//! `MemCacheBackend`. Intended as a test double / lightweight in-process
//! sink-checkpoint store; state does not survive past the backend's own
//! lifetime, so it is only useful for exercising `Store::with_checkpoint_backend`
//! in tests, not for real restart durability (use `ai-store-sqlite`'s
//! `SqliteCheckpointBackend` for that).

use std::collections::HashMap;
use std::sync::Arc;

use ai_store_core::{CheckpointBackend, Seq, StoreError, StreamId};
use async_trait::async_trait;
use tokio::sync::Mutex;

/// In-memory `CheckpointBackend`.
///
/// Cloning yields a handle sharing the same underlying map via `Arc`.
#[derive(Clone, Default)]
pub struct MemCheckpointBackend {
    inner: Arc<Mutex<HashMap<(String, StreamId), Seq>>>,
}

impl MemCheckpointBackend {
    /// Construct an empty checkpoint backend.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl CheckpointBackend for MemCheckpointBackend {
    async fn get(&self, sink_id: &str, stream: &StreamId) -> Result<Option<Seq>, StoreError> {
        let inner = self.inner.lock().await;
        Ok(inner.get(&(sink_id.to_string(), stream.clone())).copied())
    }

    async fn put(&self, sink_id: &str, stream: &StreamId, at: Seq) -> Result<(), StoreError> {
        let mut inner = self.inner.lock().await;
        inner.insert((sink_id.to_string(), stream.clone()), at);
        Ok(())
    }
}
