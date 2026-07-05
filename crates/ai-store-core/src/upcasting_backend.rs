//! [`UpcastingBackend`] — read-time upcaster application at the
//! `EventBackend` boundary.
//!
//! Read-time upcasting ([`crate::Upcaster`]) used to be threaded manually
//! through roughly a dozen call sites in [`crate::facade`]: every internal
//! read (`state_at`, `read`, `read_by_meta`, `streams_live`, `label_set`,
//! `materialize_to_sink`, `catch_up_inner`, plus the post-commit paths in
//! `write_event_locked`) had to remember to call `apply_upcasters` /
//! `apply_upcasters_all` on whatever it fetched from the backend — and a
//! new read path added later could silently forget to. This module
//! collapses that into one decorator: when a `Store` is built with a
//! non-empty upcaster chain, its `events: Arc<dyn EventBackend>` is
//! wrapped in a `UpcastingBackend` instead of pointing at the real backend
//! directly (see `Store::new_inner_full`). Every facade call site that
//! reads through `self.events` then gets already-upcasted events for
//! free, with no per-call-site upcast call required.
//!
//! Write methods (`append`, `import_event`, `append_if_head`) and the
//! label index methods pass straight through to the inner backend
//! unchanged — upcasting is a read-time projection over stored bytes, not
//! a write-time transform (see the crate-level "Schema evolution"
//! section in [`crate::facade`]). `read_by_meta` also passes its
//! predicate straight to the inner backend *before* upcasting the matches
//! it gets back, preserving the documented caveat that the predicate is
//! evaluated against the stored event, not the upcasted one.
//!
//! When no upcasters are registered, `Store` never constructs this
//! wrapper at all — `events` points directly at the real backend, so the
//! no-upcasters cost profile is unchanged from before this decorator
//! existed.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use crate::backend::EventBackend;
use crate::error::StoreError;
use crate::event::{Committed, Event, NewEvent};
use crate::id::{Label, Seq, StreamId, Timestamp};
use crate::upcaster::Upcaster;

/// Decorates an [`EventBackend`] so every event a read method returns has
/// already passed through the registered [`Upcaster`] chain.
///
/// See the module-level rustdoc for why this exists and what it leaves
/// untouched.
pub(crate) struct UpcastingBackend {
    inner: Arc<dyn EventBackend>,
    upcasters: Vec<Arc<dyn Upcaster>>,
}

impl UpcastingBackend {
    /// Wrap `inner`, applying `upcasters` (in registration order) to every
    /// event any read method returns.
    pub(crate) fn new(inner: Arc<dyn EventBackend>, upcasters: Vec<Arc<dyn Upcaster>>) -> Self {
        Self { inner, upcasters }
    }

    /// Walk the chain over one event, in registration order.
    fn upcast_one(&self, mut event: Event) -> Event {
        for uc in &self.upcasters {
            event = uc.upcast(event);
        }
        event
    }

    /// Walk the chain over every event in `events`.
    fn upcast_all(&self, events: Vec<Event>) -> Vec<Event> {
        events.into_iter().map(|e| self.upcast_one(e)).collect()
    }
}

#[async_trait]
impl EventBackend for UpcastingBackend {
    async fn append(&self, stream: &StreamId, rec: NewEvent) -> Result<Committed, StoreError> {
        self.inner.append(stream, rec).await
    }

    async fn import_event(
        &self,
        stream: &StreamId,
        rec: NewEvent,
        at: Timestamp,
    ) -> Result<Committed, StoreError> {
        self.inner.import_event(stream, rec, at).await
    }

    async fn append_if_head(
        &self,
        stream: &StreamId,
        rec: NewEvent,
        expected_head: Seq,
    ) -> Result<Committed, StoreError> {
        self.inner.append_if_head(stream, rec, expected_head).await
    }

    async fn read(
        &self,
        stream: &StreamId,
        from: Seq,
        limit: usize,
    ) -> Result<Vec<Event>, StoreError> {
        let events = self.inner.read(stream, from, limit).await?;
        Ok(self.upcast_all(events))
    }

    async fn read_by_meta(
        &self,
        stream: &StreamId,
        field: &str,
        value: &Value,
        from: Seq,
        limit: usize,
    ) -> Result<Vec<Event>, StoreError> {
        // Evaluate the predicate against the *stored* event by delegating
        // straight to the inner backend's own `read_by_meta` (a native
        // `json_extract`-based filter, or the trait's default client-side
        // filter over its own raw `read` — either way, entirely inside
        // `inner` and untouched by our `read` override above). Only the
        // matches that come back are upcasted. See the `read_by_meta`
        // caveat in `crate::upcaster`'s module rustdoc.
        let events = self
            .inner
            .read_by_meta(stream, field, value, from, limit)
            .await?;
        Ok(self.upcast_all(events))
    }

    async fn head(&self, stream: &StreamId) -> Result<Option<Seq>, StoreError> {
        self.inner.head(stream).await
    }

    async fn seq_at_time(
        &self,
        stream: &StreamId,
        at: Timestamp,
    ) -> Result<Option<Seq>, StoreError> {
        self.inner.seq_at_time(stream, at).await
    }

    async fn streams(&self) -> Result<Vec<StreamId>, StoreError> {
        self.inner.streams().await
    }

    async fn compaction_boundary(&self, stream: &StreamId) -> Result<Option<Seq>, StoreError> {
        self.inner.compaction_boundary(stream).await
    }

    async fn label_set(&self, stream: &StreamId, label: &Label, at: Seq) -> Result<(), StoreError> {
        self.inner.label_set(stream, label, at).await
    }

    async fn label_resolve(
        &self,
        stream: &StreamId,
        label: &Label,
    ) -> Result<Option<Seq>, StoreError> {
        self.inner.label_resolve(stream, label).await
    }

    async fn labels(&self, stream: &StreamId) -> Result<Vec<(Label, Seq)>, StoreError> {
        self.inner.labels(stream).await
    }

    async fn label_delete(&self, stream: &StreamId, label: &Label) -> Result<bool, StoreError> {
        self.inner.label_delete(stream, label).await
    }
}
