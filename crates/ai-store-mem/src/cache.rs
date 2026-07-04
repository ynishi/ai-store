//! In-memory `CacheBackend` implementation.
//!
//! Cache entries are held in a `BTreeMap<Seq, Value>` per stream so that
//! `nearest(at)` is a single logarithmic lookup. As with the event backend,
//! all operations serialize through a single `tokio::sync::Mutex`.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use ai_store_core::{CacheBackend, Seq, StoreError, StreamId};
use async_trait::async_trait;
use serde_json::Value;
use tokio::sync::Mutex;

/// In-memory `CacheBackend`.
///
/// Cloning yields a handle sharing the same underlying cache map via `Arc`.
#[derive(Clone, Default)]
pub struct MemCacheBackend {
    inner: Arc<Mutex<HashMap<StreamId, BTreeMap<Seq, Value>>>>,
}

impl MemCacheBackend {
    /// Construct an empty cache backend.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl CacheBackend for MemCacheBackend {
    async fn put(&self, stream: &StreamId, at: Seq, state: &Value) -> Result<(), StoreError> {
        let mut inner = self.inner.lock().await;
        inner
            .entry(stream.clone())
            .or_default()
            .insert(at, state.clone());
        Ok(())
    }

    async fn nearest(
        &self,
        stream: &StreamId,
        at: Seq,
    ) -> Result<Option<(Seq, Value)>, StoreError> {
        let inner = self.inner.lock().await;
        let Some(entries) = inner.get(stream) else {
            return Ok(None);
        };
        Ok(entries
            .range(..=at)
            .next_back()
            .map(|(seq, state)| (*seq, state.clone())))
    }

    async fn prune(&self, stream: &StreamId, keep_latest: usize) -> Result<(), StoreError> {
        let mut inner = self.inner.lock().await;
        let Some(entries) = inner.get_mut(stream) else {
            return Ok(());
        };
        if entries.len() <= keep_latest {
            return Ok(());
        }
        let drop_count = entries.len() - keep_latest;
        let drop_keys: Vec<Seq> = entries.keys().copied().take(drop_count).collect();
        for k in drop_keys {
            entries.remove(&k);
        }
        Ok(())
    }
}
