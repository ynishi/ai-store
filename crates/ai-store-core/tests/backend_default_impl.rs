//! Verifies the default `EventBackend::import_event` implementation.
//!
//! Backends that have not opted in to historical-timestamp imports (i.e.
//! have not overridden `import_event`) must decline with
//! `StoreError::BackendUnsupported` rather than silently misbehaving.

use std::sync::Mutex;

use ai_store_core::{Event, EventBackend, Label, NewEvent, Seq, StoreError, StreamId, Timestamp};
use async_trait::async_trait;
use serde_json::json;

/// Minimal `EventBackend` that implements only the required methods —
/// `import_event` and `read_by_meta` are left at their trait defaults.
#[derive(Default)]
struct DummyBackend {
    events: Mutex<Vec<Event>>,
}

#[async_trait]
impl EventBackend for DummyBackend {
    async fn append(&self, _stream: &StreamId, rec: NewEvent) -> Result<Seq, StoreError> {
        let mut events = self.events.lock().unwrap();
        let seq = Seq((events.len() + 1) as u64);
        events.push(Event {
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
        _stream: &StreamId,
        from: Seq,
        limit: usize,
    ) -> Result<Vec<Event>, StoreError> {
        let events = self.events.lock().unwrap();
        Ok(events
            .iter()
            .filter(|e| e.seq >= from)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn head(&self, _stream: &StreamId) -> Result<Option<Seq>, StoreError> {
        Ok(self.events.lock().unwrap().last().map(|e| e.seq))
    }

    async fn seq_at_time(
        &self,
        _stream: &StreamId,
        _at: Timestamp,
    ) -> Result<Option<Seq>, StoreError> {
        Ok(None)
    }

    async fn streams(&self) -> Result<Vec<StreamId>, StoreError> {
        Ok(Vec::new())
    }

    async fn label_set(
        &self,
        _stream: &StreamId,
        _label: &Label,
        _at: Seq,
    ) -> Result<(), StoreError> {
        Ok(())
    }

    async fn label_resolve(
        &self,
        _stream: &StreamId,
        _label: &Label,
    ) -> Result<Option<Seq>, StoreError> {
        Ok(None)
    }

    async fn labels(&self, _stream: &StreamId) -> Result<Vec<(Label, Seq)>, StoreError> {
        Ok(Vec::new())
    }

    async fn label_delete(&self, _stream: &StreamId, _label: &Label) -> Result<bool, StoreError> {
        Ok(false)
    }
}

fn empty_patch() -> json_patch::Patch {
    serde_json::from_value(json!([])).unwrap()
}

#[tokio::test]
async fn default_import_event_returns_backend_unsupported() {
    let be = DummyBackend::default();
    let s = StreamId::new("s");
    let rec = NewEvent {
        kind: "k".to_string(),
        patch: empty_patch(),
        meta: json!({}),
    };

    let err = be
        .import_event(&s, rec, Timestamp(1_700_000_000_000))
        .await
        .unwrap_err();
    assert!(matches!(err, StoreError::BackendUnsupported(op) if op == "import_event"));

    // The declined call never reached the backend's own state.
    assert_eq!(be.head(&s).await.unwrap(), None);
}
