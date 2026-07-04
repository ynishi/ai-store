//! In-memory `EventBackend` implementation.
//!
//! One `tokio::sync::Mutex` guards both the per-stream event vectors and the
//! per-stream label map. Holding the lock across `append` gives us gap-free
//! monotonic `Seq` assignment for free — no separate writer thread is needed
//! because there is no blocking I/O to offload.

use std::collections::HashMap;
use std::sync::Arc;

use ai_store_core::{Event, EventBackend, Label, NewEvent, Seq, StoreError, StreamId, Timestamp};
use async_trait::async_trait;
use tokio::sync::Mutex;

/// Per-stream state held under the backend's mutex.
#[derive(Default)]
struct StreamState {
    events: Vec<Event>,
    labels: HashMap<Label, Seq>,
}

impl StreamState {
    fn head_seq(&self) -> Option<Seq> {
        self.events.last().map(|e| e.seq)
    }
}

/// In-memory `EventBackend` suitable for tests and lightweight in-process use.
///
/// Cloning yields a handle sharing the same underlying state via `Arc`.
#[derive(Clone, Default)]
pub struct MemEventBackend {
    inner: Arc<Mutex<HashMap<StreamId, StreamState>>>,
}

impl MemEventBackend {
    /// Construct an empty backend.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl EventBackend for MemEventBackend {
    async fn append(&self, stream: &StreamId, rec: NewEvent) -> Result<Seq, StoreError> {
        let mut inner = self.inner.lock().await;
        let state = inner.entry(stream.clone()).or_default();
        let seq = match state.head_seq() {
            Some(head) => head.next(),
            None => Seq::ZERO.next(),
        };
        state.events.push(Event {
            seq,
            kind: rec.kind,
            patch: rec.patch,
            meta: rec.meta,
            at: Timestamp::now(),
        });
        Ok(seq)
    }

    async fn read(
        &self,
        stream: &StreamId,
        from: Seq,
        limit: usize,
    ) -> Result<Vec<Event>, StoreError> {
        let inner = self.inner.lock().await;
        let Some(state) = inner.get(stream) else {
            return Ok(Vec::new());
        };
        let out = state
            .events
            .iter()
            .filter(|e| e.seq >= from)
            .take(limit)
            .cloned()
            .collect();
        Ok(out)
    }

    async fn head(&self, stream: &StreamId) -> Result<Option<Seq>, StoreError> {
        let inner = self.inner.lock().await;
        Ok(inner.get(stream).and_then(|s| s.head_seq()))
    }

    async fn seq_at_time(
        &self,
        stream: &StreamId,
        at: ai_store_core::Timestamp,
    ) -> Result<Option<Seq>, StoreError> {
        let inner = self.inner.lock().await;
        let Some(state) = inner.get(stream) else {
            return Ok(None);
        };
        // Events are appended in monotonic Seq order and each event's Timestamp
        // is taken at append time; we assume timestamps are non-decreasing.
        // Find the greatest seq whose at <= given.
        Ok(state
            .events
            .iter()
            .rev()
            .find(|e| e.at <= at)
            .map(|e| e.seq))
    }

    async fn streams(&self) -> Result<Vec<StreamId>, StoreError> {
        let inner = self.inner.lock().await;
        Ok(inner.keys().cloned().collect())
    }

    async fn label_set(&self, stream: &StreamId, label: &Label, at: Seq) -> Result<(), StoreError> {
        let mut inner = self.inner.lock().await;
        let state = inner.entry(stream.clone()).or_default();
        // Reject dangling labels: `at` must correspond to an existing event.
        let exists = state.events.iter().any(|e| e.seq == at);
        if !exists {
            return Err(StoreError::SeqOutOfRange {
                head: state.head_seq(),
                requested: at,
            });
        }
        state.labels.insert(label.clone(), at);
        Ok(())
    }

    async fn label_resolve(
        &self,
        stream: &StreamId,
        label: &Label,
    ) -> Result<Option<Seq>, StoreError> {
        let inner = self.inner.lock().await;
        Ok(inner.get(stream).and_then(|s| s.labels.get(label).copied()))
    }

    async fn labels(&self, stream: &StreamId) -> Result<Vec<(Label, Seq)>, StoreError> {
        let inner = self.inner.lock().await;
        let Some(state) = inner.get(stream) else {
            return Ok(Vec::new());
        };
        let mut out: Vec<(Label, Seq)> =
            state.labels.iter().map(|(l, s)| (l.clone(), *s)).collect();
        out.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
        Ok(out)
    }
}
