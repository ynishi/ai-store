//! Label path: `label_set` / `label_resolve` / `labels` / `label_delete`.
//!
//! A label is a named pointer to a `Seq` on a stream — a lightweight
//! alternative to a full snapshot for "give this coordinate a name"
//! use cases (e.g. `"published"`, `"draft"`). Both mutating methods here
//! dispatch to every accepting [`crate::ProjectionSink`] on success, mirroring
//! the best-effort sink-failure policy [`super::write`] uses for `append`.

use crate::error::StoreError;
use crate::id::{Label, Seq, StreamId};

use super::Store;

impl Store {
    /// Pin `label` on `stream` to `at`.
    ///
    /// After the backend records the pin, every registered `ProjectionSink`
    /// that [`crate::ProjectionSink::accepts`] `stream` receives an `on_label_set`
    /// notification carrying the freshly materialized state at `at` and the
    /// [`crate::Event`] the label now points at. Sink failures are
    /// best-effort — they do not roll back the label change, matching the
    /// append dispatch policy.
    ///
    /// Fetching that event is itself best-effort: the label change has
    /// already succeeded in the backend by this point, so a failure to read
    /// the event back (or the read returning nothing, which should not
    /// happen if `state_at` above just succeeded for the same `at`) is
    /// swallowed the same way a failing `commit` is — it simply means no
    /// sink is notified for this call, not that `label_set` itself fails.
    pub async fn label_set(
        &self,
        stream: &StreamId,
        label: &Label,
        at: Seq,
    ) -> Result<(), StoreError> {
        self.events.label_set(stream, label, at).await?;
        if self.dispatcher.has_sinks().await {
            let state = self.state_at(stream, at).await?;
            // `self.events` already applies the registered upcaster chain
            // (see `crate::upcasting_backend::UpcastingBackend`) when one is
            // registered, so no separate upcast step is needed here.
            let event = self
                .events
                .read(stream, at, 1)
                .await
                .ok()
                .and_then(|mut evs| (!evs.is_empty()).then(|| evs.remove(0)));
            if let Some(event) = event {
                self.dispatcher
                    .dispatch_label_set(stream, label, at, &state, &event)
                    .await;
            }
        }
        Ok(())
    }

    /// Resolve `label` on `stream`.
    pub async fn label_resolve(&self, stream: &StreamId, label: &Label) -> Result<Seq, StoreError> {
        self.events
            .label_resolve(stream, label)
            .await?
            .ok_or_else(|| StoreError::UnknownLabel(label.as_str().to_string()))
    }

    /// Enumerate labels on `stream`.
    pub async fn labels(&self, stream: &StreamId) -> Result<Vec<(Label, Seq)>, StoreError> {
        self.events.labels(stream).await
    }

    /// Delete `label` from `stream`.
    ///
    /// Idempotent: deleting a label that is not defined is **not** an error.
    /// Returns `Ok(true)` when the label existed and was removed, `Ok(false)`
    /// when it was already absent — mirroring the backend's
    /// [`crate::EventBackend::label_delete`] contract. Callers that need the strict
    /// "must have existed" behavior can match on the returned `bool`
    /// themselves.
    ///
    /// After the backend removes the label, every registered
    /// [`crate::ProjectionSink`] that [`crate::ProjectionSink::accepts`] `stream` receives
    /// an `on_label_deleted` notification — but only when the label actually
    /// existed (a no-op delete dispatches nothing, since nothing changed).
    /// Sink failures are best-effort — matching the dispatch policy of
    /// [`Store::label_set`] and [`Store::append`], a sink error does not
    /// roll back the deletion.
    pub async fn label_delete(&self, stream: &StreamId, label: &Label) -> Result<bool, StoreError> {
        let existed = self.events.label_delete(stream, label).await?;
        if existed && self.dispatcher.has_sinks().await {
            self.dispatcher.dispatch_label_deleted(stream, label).await;
        }
        Ok(existed)
    }
}
