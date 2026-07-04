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

    /// Shared append path: assigns the next gap-free monotonic `Seq` and
    /// stamps the event with `at`. `append` passes `Timestamp::now()`;
    /// `import_event` passes the caller-supplied historical timestamp.
    async fn push_event(
        &self,
        stream: &StreamId,
        rec: NewEvent,
        at: Timestamp,
    ) -> Result<Seq, StoreError> {
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
            at,
        });
        Ok(seq)
    }
}

#[async_trait]
impl EventBackend for MemEventBackend {
    async fn append(&self, stream: &StreamId, rec: NewEvent) -> Result<Seq, StoreError> {
        self.push_event(stream, rec, Timestamp::now()).await
    }

    async fn import_event(
        &self,
        stream: &StreamId,
        rec: NewEvent,
        at: Timestamp,
    ) -> Result<Seq, StoreError> {
        self.push_event(stream, rec, at).await
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
        // Assumes timestamps are non-decreasing in Seq order — true for every
        // event written via `append` (stamped at write time). `import_event`
        // callers are responsible for preserving that order themselves (see
        // `Store::import_event`'s rustdoc); this scan does not verify it.
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

    async fn label_delete(&self, stream: &StreamId, label: &Label) -> Result<bool, StoreError> {
        let mut inner = self.inner.lock().await;
        let Some(state) = inner.get_mut(stream) else {
            return Ok(false);
        };
        Ok(state.labels.remove(label).is_some())
    }
}
