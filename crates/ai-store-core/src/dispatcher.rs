//! Sink registry, checkpoint bookkeeping, and failure-observer dispatch.
//!
//! [`SinkDispatcher`] owns the operational state around driving registered
//! [`ProjectionSink`]s: the sink list itself, the per-`(sink, stream)`
//! checkpoint cache (with optional [`CheckpointBackend`] durability), the
//! optional [`SinkFailureObserver`], and the per-process "has this sink
//! completed a full rebuild yet" tracking used by [`crate::Store::catch_up`]'s
//! auto-rebuild escalation.
//!
//! [`crate::Store`] holds one `Arc<SinkDispatcher>` and delegates every
//! sink-related operation to it. The event-read side of sink dispatch
//! (fetching the committed [`Event`], materializing `state` via
//! `Store::state_at`, applying the upcaster chain) stays on `Store` — those
//! steps depend on the event backend, the cache backend, and the upcaster
//! chain, none of which this collaborator owns. Methods here receive
//! already-upcasted `event` / `state` values from the caller rather than
//! reading or upcasting anything themselves.
//!
//! This is also the attachment point [`crate::Store::attach_sink`] uses for
//! dynamic sink registration after construction: [`SinkDispatcher::attach`]
//! pushes onto `self.sinks` here rather than requiring a new `Store`. The
//! registry is an `RwLock` (rather than a plain `Vec` set once at
//! construction) specifically so `attach` can run concurrently with the
//! read-heavy `dispatch_commit` / `dispatch_label_set` /
//! `dispatch_label_deleted` / `find_sink` traffic every other `Store` method
//! generates.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex};

use serde_json::Value;
use tokio::sync::{Mutex, RwLock};

use crate::backend::CheckpointBackend;
use crate::error::StoreError;
use crate::event::Event;
use crate::id::{Label, Seq, StreamId};
use crate::sink::{ProjectionSink, SinkDispatchFailure, SinkFailureObserver, SinkOp};

/// Sink registry + checkpoint bookkeeping collaborator for [`crate::Store`].
///
/// See the module-level rustdoc for the responsibility split between this
/// type and `Store` itself.
pub(crate) struct SinkDispatcher {
    /// The sink registry. `RwLock` rather than a plain `Vec` so
    /// [`SinkDispatcher::attach`] can add a sink after construction without
    /// blocking (or being blocked by) concurrent dispatch — see
    /// [`SinkDispatcher::attach`] for the duplicate-id contract and the
    /// module-level rustdoc for why this exists at all.
    sinks: RwLock<Vec<Arc<dyn ProjectionSink>>>,
    /// In-memory L1 cache of `(sink_id, stream) -> Seq`. See
    /// [`SinkDispatcher::checkpoint_get`] for the fallback to
    /// `checkpoint_backend` on a miss.
    checkpoints: Mutex<HashMap<(String, StreamId), Seq>>,
    checkpoint_backend: Option<Arc<dyn CheckpointBackend>>,
    /// Optional visibility hook — see [`SinkFailureObserver`]. Invoked from
    /// [`SinkDispatcher::dispatch_commit`] / `dispatch_label_set` /
    /// `dispatch_label_deleted` whenever a sink returns `Err`. Dispatch
    /// semantics themselves (checkpoint parked / label backend already
    /// advanced) are unchanged.
    sink_failure_observer: Option<Arc<dyn SinkFailureObserver>>,
    /// Sink ids that have completed `catch_up` (or `rebuild`) in this
    /// process. See [`crate::Store::catch_up`]'s auto-rebuild escalation.
    /// Cleared on process restart by construction (in-memory only, never
    /// persisted).
    rebuilt_this_process: StdMutex<HashSet<String>>,
}

impl SinkDispatcher {
    /// Construct a dispatcher over `sinks`, with optional checkpoint
    /// durability and failure observation. Checkpoints and rebuild
    /// tracking always start empty — the caller (`Store::new_inner_full`)
    /// restores nothing else at construction time.
    pub(crate) fn new(
        sinks: Vec<Arc<dyn ProjectionSink>>,
        checkpoint_backend: Option<Arc<dyn CheckpointBackend>>,
        sink_failure_observer: Option<Arc<dyn SinkFailureObserver>>,
    ) -> Self {
        Self {
            sinks: RwLock::new(sinks),
            checkpoints: Mutex::new(HashMap::new()),
            checkpoint_backend,
            sink_failure_observer,
            rebuilt_this_process: StdMutex::new(HashSet::new()),
        }
    }

    /// Register `sink` for dispatch going forward.
    ///
    /// Returns [`StoreError::SinkAlreadyAttached`] if a sink with the same
    /// [`ProjectionSink::id`] is already registered — whether that sink was
    /// registered at construction time (via [`crate::StoreBuilder::sink`])
    /// or through an earlier `attach` call. Ids double as the checkpoint
    /// key, so two live sinks sharing one id would silently share (and
    /// corrupt) each other's checkpoint bookkeeping.
    ///
    /// The newly attached sink starts participating in
    /// [`SinkDispatcher::dispatch_commit`] / `dispatch_label_set` /
    /// `dispatch_label_deleted` the moment this call returns — see
    /// [`crate::Store::attach_sink`] for how the caller backfills the
    /// sink's pre-attach history without racing that live dispatch.
    pub(crate) async fn attach(&self, sink: Arc<dyn ProjectionSink>) -> Result<(), StoreError> {
        let mut sinks = self.sinks.write().await;
        if sinks.iter().any(|s| s.id() == sink.id()) {
            return Err(StoreError::SinkAlreadyAttached(sink.id().to_string()));
        }
        sinks.push(sink);
        Ok(())
    }

    /// Whether any sink is registered. Callers use this to skip fetching
    /// the committed event / materialized state entirely when there is
    /// nothing to dispatch to.
    pub(crate) async fn has_sinks(&self) -> bool {
        !self.sinks.read().await.is_empty()
    }

    /// Look up a registered sink by [`ProjectionSink::id`].
    pub(crate) async fn find_sink(&self, sink_id: &str) -> Option<Arc<dyn ProjectionSink>> {
        self.sinks
            .read()
            .await
            .iter()
            .find(|s| s.id() == sink_id)
            .cloned()
    }

    /// Look up `sink_id`'s checkpoint for `stream`.
    ///
    /// Consults the in-memory cache first; on a miss, restores from the
    /// persisted [`CheckpointBackend`] (if one is configured) instead of
    /// assuming `Seq::ZERO` — this is what makes checkpoints survive a
    /// restart. A backend read failure is treated the same as "no
    /// checkpoint found" (fail open): sinks are contracted to be idempotent
    /// under redelivery, so the worst case is a redundant re-dispatch on the
    /// next `catch_up`, never a missed one.
    pub(crate) async fn checkpoint_get(&self, sink_id: &str, stream: &StreamId) -> Seq {
        let key = (sink_id.to_string(), stream.clone());
        {
            let cps = self.checkpoints.lock().await;
            if let Some(seq) = cps.get(&key) {
                return *seq;
            }
        }
        let restored = match &self.checkpoint_backend {
            Some(backend) => backend
                .get(sink_id, stream)
                .await
                .ok()
                .flatten()
                .unwrap_or(Seq::ZERO),
            None => Seq::ZERO,
        };
        let mut cps = self.checkpoints.lock().await;
        cps.entry(key).or_insert(restored);
        restored
    }

    /// Advance `sink_id`'s checkpoint for `stream` to `seq`.
    ///
    /// When a [`CheckpointBackend`] is configured, persists first and only
    /// updates the in-memory cache on success — returning `false` and
    /// leaving the in-memory value untouched if persistence fails. This
    /// keeps memory and backend from drifting apart: a failed persist here
    /// means the next `catch_up` re-drives from the last *durably* recorded
    /// position, rather than from a position only this process remembers.
    pub(crate) async fn checkpoint_advance(
        &self,
        sink_id: &str,
        stream: &StreamId,
        seq: Seq,
    ) -> bool {
        if let Some(backend) = &self.checkpoint_backend {
            if backend.put(sink_id, stream, seq).await.is_err() {
                return false;
            }
        }
        let mut cps = self.checkpoints.lock().await;
        cps.insert((sink_id.to_string(), stream.clone()), seq);
        true
    }

    /// Reset `sink_id`'s checkpoint for `stream` to zero, in memory and
    /// (best-effort) in the [`CheckpointBackend`].
    ///
    /// A failed persist here is not surfaced: the very next successful
    /// [`SinkDispatcher::checkpoint_advance`] call re-persists the correct
    /// seq, self-healing the drift. Reset is only ever a prelude to
    /// immediately driving the sink forward again (see
    /// [`crate::Store::rebuild`]), so there is no window where a stale
    /// persisted value would be read back.
    pub(crate) async fn checkpoint_reset(&self, sink_id: &str, stream: &StreamId) {
        {
            let mut cps = self.checkpoints.lock().await;
            cps.remove(&(sink_id.to_string(), stream.clone()));
        }
        if let Some(backend) = &self.checkpoint_backend {
            let _ = backend.put(sink_id, stream, Seq::ZERO).await;
        }
    }

    /// Whether `sink_id` has already completed a `catch_up` or `rebuild`
    /// in this process. Consulted only by [`crate::Store::catch_up`]'s
    /// auto-rebuild escalation path.
    pub(crate) fn has_been_rebuilt_this_process(&self, sink_id: &str) -> bool {
        let guard = self
            .rebuilt_this_process
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        guard.contains(sink_id)
    }

    /// Mark `sink_id` as having completed at least one full `catch_up` or
    /// `rebuild` cycle in this process, so future `catch_up` calls skip the
    /// auto-rebuild escalation.
    pub(crate) fn mark_rebuilt_this_process(&self, sink_id: &str) {
        let mut guard = self
            .rebuilt_this_process
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        guard.insert(sink_id.to_string());
    }

    /// Notify the observer if one is attached; no-op otherwise. Kept small
    /// enough to inline at each dispatch site without noise.
    fn notify_sink_failure(
        &self,
        sink_id: &str,
        stream: &StreamId,
        seq: Option<Seq>,
        op: SinkOp,
        error: &StoreError,
    ) {
        if let Some(observer) = &self.sink_failure_observer {
            observer.on_failure(&SinkDispatchFailure {
                sink_id: sink_id.to_string(),
                stream: stream.clone(),
                seq,
                op,
                error: error.to_string(),
            });
        }
    }

    /// Inline post-`Store::append` (or `import_event`) dispatch: hand
    /// `event` / `next_state` (already upcasted by the caller) to every
    /// registered sink that [`ProjectionSink::accepts`] `stream`.
    ///
    /// Best-effort, matching the append dispatch contract: a sink whose
    /// `commit` returns `Err` leaves its checkpoint parked so the next
    /// `catch_up` re-drives it, and the failure is only surfaced through
    /// the attached [`SinkFailureObserver`] (if any) — never propagated to
    /// the `append` caller.
    pub(crate) async fn dispatch_commit(
        &self,
        stream: &StreamId,
        seq: Seq,
        next_state: &Value,
        event: &Event,
    ) {
        // Snapshot the registry under a short-lived read lock rather than
        // holding the guard across every sink's `commit` await below — a
        // concurrent `attach` only has to wait for the (cheap) clone, not
        // for however long dispatch to every sink takes.
        let sinks = self.sinks.read().await.clone();
        for sink in &sinks {
            if !sink.accepts(stream) {
                continue;
            }
            let checkpoint = self.checkpoint_get(sink.id(), stream).await;
            // Skip if already past this seq (catch_up ran concurrently).
            if seq <= checkpoint {
                continue;
            }
            match sink.commit(stream, seq, next_state, event).await {
                Ok(()) => {
                    // Only advance the checkpoint contiguously. If there is
                    // a gap (an earlier seq failed dispatch), leave the
                    // checkpoint parked so catch_up will re-drive the gap.
                    // A failed persist (see `checkpoint_advance`) is
                    // likewise left parked for catch_up to redrive.
                    if seq == checkpoint.next() {
                        self.checkpoint_advance(sink.id(), stream, seq).await;
                    }
                }
                Err(e) => {
                    // Best-effort dispatch: checkpoint stays put so
                    // catch_up re-drives this seq. Surface the failure
                    // through the observer (if attached) so the caller can
                    // see the sink falling behind without waiting for a
                    // manual catch_up call.
                    self.notify_sink_failure(sink.id(), stream, Some(seq), SinkOp::Commit, &e);
                }
            }
        }
    }

    /// Inline post-`Store::label_set` dispatch: notify every registered
    /// sink that [`ProjectionSink::accepts`] `stream` via
    /// [`ProjectionSink::on_label_set`]. Sink failures are best-effort —
    /// matching [`SinkDispatcher::dispatch_commit`], they do not roll back
    /// the label change.
    pub(crate) async fn dispatch_label_set(
        &self,
        stream: &StreamId,
        label: &Label,
        at: Seq,
        state: &Value,
        event: &Event,
    ) {
        let sinks = self.sinks.read().await.clone();
        for sink in &sinks {
            if !sink.accepts(stream) {
                continue;
            }
            if let Err(e) = sink.on_label_set(stream, label, at, state, event).await {
                self.notify_sink_failure(sink.id(), stream, Some(at), SinkOp::LabelSet, &e);
            }
        }
    }

    /// Inline post-`Store::label_delete` dispatch: notify every registered
    /// sink that [`ProjectionSink::accepts`] `stream` via
    /// [`ProjectionSink::on_label_deleted`]. Sink failures are best-effort,
    /// same as [`SinkDispatcher::dispatch_commit`] / `dispatch_label_set`.
    pub(crate) async fn dispatch_label_deleted(&self, stream: &StreamId, label: &Label) {
        let sinks = self.sinks.read().await.clone();
        for sink in &sinks {
            if !sink.accepts(stream) {
                continue;
            }
            if let Err(e) = sink.on_label_deleted(stream, label).await {
                self.notify_sink_failure(sink.id(), stream, None, SinkOp::LabelDeleted, &e);
            }
        }
    }
}
