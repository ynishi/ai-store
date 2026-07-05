//! Sink lifecycle: `attach_sink` / `materialize_to_sink` / `catch_up` /
//! `rebuild` / `catch_up_inner`.
//!
//! `catch_up_inner` is the shared driver `catch_up` and `rebuild` both call
//! (with `reset` selecting whether checkpoints are zeroed first) — see
//! [`super::Store`]'s module-level rustdoc for how sink dispatch fits into
//! the overall write pipeline.

use std::sync::Arc;

use serde_json::Value;

use crate::error::StoreError;
use crate::id::{Seq, StreamId};
use crate::sink::{CatchUpFailure, CatchUpReport, ProjectionSink};

use super::Store;

impl Store {
    /// Register `sink` on this store and bring it up to date with every
    /// stream's current history in one call.
    ///
    /// This is the dynamic counterpart to [`crate::StoreBuilder::sink`]: the
    /// builder method fixes the sink list at construction time, while
    /// `attach_sink` lets a caller add a [`ProjectionSink`] to an
    /// already-running `Store` — the shape most consumers actually need
    /// (open a store, hand out a handle, then let a caller-supplied
    /// projection opt in later without reconstructing the store around it).
    ///
    /// Returns [`StoreError::SinkAlreadyAttached`] if a sink with the same
    /// [`ProjectionSink::id`] is already registered (whether from
    /// [`crate::StoreBuilder::sink`] or an earlier `attach_sink` call) — ids
    /// are the checkpoint key, so a collision would otherwise silently
    /// corrupt both sinks' bookkeeping. The rejected sink is never
    /// registered and never dispatched to.
    ///
    /// ## Ordering: register before backfill
    ///
    /// `attach_sink` registers `sink` with the dispatcher *first*, then
    /// drives it forward with the same routine [`Store::catch_up`] uses
    /// (including the one-shot [`ProjectionSink::requires_rebuild_on_attach`]
    /// escalation to a full [`Store::rebuild`] — see that trait method's
    /// rustdoc for which sinks need it). Registering before backfilling
    /// means the live post-`append` / `label_set` / `label_delete` dispatch
    /// path starts handing `sink` every subsequent commit *before* the
    /// backfill below has even started reading history — so no commit that
    /// lands after this call returns can be missed, even one racing the
    /// backfill itself.
    ///
    /// A commit dispatched live while the backfill is still in flight will
    /// not necessarily land contiguously with the sink's checkpoint (the
    /// checkpoint only advances when the dispatched `seq` immediately
    /// follows it — see `SinkDispatcher::dispatch_commit`
    /// — so a live dispatch that arrives ahead of the backfill's cursor
    /// leaves the checkpoint parked and the intervening gap for the
    /// backfill to fill in). The backfill below always runs to the head it
    /// observes once it starts, so it closes that gap in the same call;
    /// [`ProjectionSink::commit`]'s idempotence-under-redelivery contract
    /// covers any event `sink` happens to see twice as a result (once via
    /// the live path, once via backfill). The net effect: every event
    /// committed to this store — before, during, or after this call — is
    /// eventually delivered to `sink` at least once, with no window in
    /// which one can be silently dropped.
    ///
    /// The backfill (a [`Store::catch_up`], or [`Store::rebuild`] when
    /// [`ProjectionSink::requires_rebuild_on_attach`] returns `true`) is
    /// isolated per stream the same way `catch_up` is — a failure on one
    /// stream does not stop other streams from being driven to completion.
    /// Those per-stream failures are reported through a
    /// [`crate::CatchUpReport`], not through this method's `Result`, which
    /// only reports registration/backend failures; a caller that needs the
    /// full report (applied/skipped/failed counts, per-stream failure
    /// detail) should call [`Store::catch_up`] again for `sink.id()`
    /// immediately after a successful `attach_sink` — that follow-up call
    /// is a cheap no-op when the backfill above already caught the sink up.
    pub async fn attach_sink(&self, sink: Arc<dyn ProjectionSink>) -> Result<(), StoreError> {
        let sink_id = sink.id().to_string();
        self.dispatcher.attach(sink).await?;
        self.catch_up(&sink_id).await?;
        Ok(())
    }

    /// Materialize `stream`'s state at `at` (or the current head when `at`
    /// is `None`) and hand it to `sink_id` immediately, without waiting for
    /// [`Store::catch_up`] to drive it.
    ///
    /// This is the imperative "dump now" escape hatch: previously the only
    /// way to force a snapshot out of a sink was to pin a synthetic label
    /// just to trigger `on_label_set`. Unlike the best-effort dispatch in
    /// [`Store::append`] and [`Store::label_set`], a failing `commit` here
    /// is propagated to the caller — an explicit dump is expected to
    /// succeed or report why it didn't.
    ///
    /// Unlike every automatic dispatch path (`append`, `catch_up` /
    /// `rebuild`, `label_set`, `label_delete`), this call **bypasses**
    /// [`ProjectionSink::accepts`] — the caller names `sink_id` and `stream`
    /// explicitly, so the request is assumed to know what it is asking for
    /// even if the sink would otherwise filter that stream out of its
    /// automatic traffic.
    ///
    /// `at` lets a caller replay an arbitrary point in history to a sink,
    /// not just the head — useful for backfilling a newly attached sink
    /// with a historical snapshot without first winding the whole stream
    /// forward through `catch_up`.
    ///
    /// The sink's checkpoint is **not** advanced. `commit` is contracted to
    /// be idempotent (see [`ProjectionSink`]), so a subsequent `catch_up`
    /// may re-deliver this same `(stream, seq)` pair; that redelivery is
    /// harmless under the same contract that makes crash recovery safe.
    /// Advancing the checkpoint here would risk skipping a gap that
    /// `catch_up` still needs to fill.
    ///
    /// Returns `StoreError::UnknownSink` if `sink_id` is not registered.
    /// Returns `StoreError::UnknownStream` if `stream` has no events —
    /// whether `at` was given or resolved from an empty head. When `at` is
    /// `Some(seq)` beyond the stream's current head, returns
    /// `StoreError::SeqOutOfRange` (surfaced by [`Store::state_at`]).
    pub async fn materialize_to_sink(
        &self,
        stream: &StreamId,
        sink_id: &str,
        at: Option<Seq>,
    ) -> Result<Seq, StoreError> {
        let sink = self
            .dispatcher
            .find_sink(sink_id)
            .await
            .ok_or_else(|| StoreError::UnknownSink(sink_id.to_string()))?;

        let target = match at {
            Some(seq) => seq,
            None => self
                .events
                .head(stream)
                .await?
                .ok_or_else(|| StoreError::UnknownStream(stream.clone()))?,
        };

        let state = self.state_at(stream, target).await?;
        // `self.events` already applies the registered upcaster chain (see
        // `crate::upcasting_backend::UpcastingBackend`) when one is
        // registered, so no separate upcast step is needed here.
        let events = self.events.read(stream, target, 1).await?;
        let event = events
            .into_iter()
            .next()
            .ok_or_else(|| StoreError::UnknownStream(stream.clone()))?;

        sink.commit(stream, target, &state, &event).await?;
        Ok(target)
    }

    /// Drive `sink_id` forward from its checkpoint to head on every known
    /// stream. On success the checkpoint advances; on failure it stays put.
    ///
    /// The first `catch_up` in this `Store` instance for a sink whose
    /// [`ProjectionSink::requires_rebuild_on_attach`] returns `true` is
    /// silently escalated to [`Store::rebuild`] — the sink's in-memory
    /// state is empty in a fresh process, and resuming from the persisted
    /// checkpoint alone would silently drop every stream whose head is
    /// already past that checkpoint. See the trait method's rustdoc for
    /// the shape of that failure mode. Subsequent `catch_up` calls to the
    /// same sink id in the same instance behave normally.
    pub async fn catch_up(&self, sink_id: &str) -> Result<CatchUpReport, StoreError> {
        let needs_rebuild_escalation = self
            .dispatcher
            .find_sink(sink_id)
            .await
            .map(|s| s.requires_rebuild_on_attach())
            .unwrap_or(false)
            && !self.dispatcher.has_been_rebuilt_this_process(sink_id);
        let reset = needs_rebuild_escalation;
        let report = self.catch_up_inner(sink_id, reset).await?;
        // Record success — whether we ran a plain catch_up or an escalated
        // rebuild — so we do not re-escalate on the next call.
        self.dispatcher.mark_rebuilt_this_process(sink_id);
        Ok(report)
    }

    /// Reset `sink_id`'s checkpoint to zero on every stream, then drive it
    /// forward. Equivalent to `catch_up` after checkpoint reset — no special
    /// rebuild API is needed at the backend level.
    pub async fn rebuild(&self, sink_id: &str) -> Result<CatchUpReport, StoreError> {
        let report = self.catch_up_inner(sink_id, true).await?;
        // An explicit rebuild also satisfies the "must rebuild once per
        // process" contract, so a subsequent `catch_up` for the same sink
        // id will not re-escalate.
        self.dispatcher.mark_rebuilt_this_process(sink_id);
        Ok(report)
    }

    /// Drive `sink_id` forward on every stream, isolating failures per
    /// stream rather than aborting the whole call on the first one.
    ///
    /// A failing `commit` (or a failed checkpoint persist — see the
    /// dispatcher's `checkpoint_advance` helper) halts catch-up for *that
    /// stream only*:
    /// order within a stream must be preserved, so every remaining event on
    /// the failed stream is counted in [`CatchUpReport::skipped`] rather
    /// than applied out of order, and one [`CatchUpFailure`] is recorded.
    /// Every other stream is still driven to completion in the same call.
    async fn catch_up_inner(
        &self,
        sink_id: &str,
        reset: bool,
    ) -> Result<CatchUpReport, StoreError> {
        let Some(sink) = self.dispatcher.find_sink(sink_id).await else {
            return Ok(CatchUpReport::EMPTY);
        };
        let streams = self.events.streams().await?;
        let mut report = CatchUpReport::EMPTY;

        for stream in streams {
            // A stream this sink does not accept is not this sink's concern
            // at all: no checkpoint reset, no advance, and no contribution
            // to `applied` / `skipped` / `failed` — counting it as
            // "skipped" would imply it was owed a dispatch it never was.
            if !sink.accepts(&stream) {
                continue;
            }
            if reset {
                self.dispatcher.checkpoint_reset(sink_id, &stream).await;
            }

            let head = match self.events.head(&stream).await? {
                Some(h) => h,
                None => continue,
            };
            let mut cursor = self.dispatcher.checkpoint_get(sink_id, &stream).await;
            let mut stream_failed = false;

            // Bootstrap the running state *once* at `cursor` and fold each
            // event's patch onto it forward, instead of re-materializing
            // state via `state_at` for every event. `state_at` costs a
            // cache-nearest lookup + a replay of events since that cache
            // stride; running it once per event turned catch_up over N
            // events into ~O(N * stride) patch applications. Threading one
            // running state through the batch loop below drops that to
            // exactly N patch applications total (plus one bootstrap).
            let mut running_state: Option<Value> = None;

            while cursor < head && !stream_failed {
                let from = cursor.next();
                // `self.events` already applies the registered upcaster
                // chain when one is registered — see
                // `crate::upcasting_backend::UpcastingBackend`.
                let events = self.events.read(&stream, from, 32).await?;
                if events.is_empty() {
                    break;
                }
                for ev in events {
                    // Lazy bootstrap: only pay for `state_at` when we're
                    // about to hand the sink its first event of this call.
                    // Streams with an already-at-head checkpoint never
                    // reach here, so an empty catch_up is O(1) per stream.
                    let state = match running_state.take() {
                        Some(mut s) => {
                            if let Err(e) = json_patch::patch(&mut s, &ev.patch) {
                                // A patch that fails to apply here — after
                                // the event was already durably committed
                                // — is a corruption signal on the log, not
                                // a plausible operational failure. Report
                                // it against this stream and halt catch_up
                                // for it: continuing would apply
                                // subsequent patches on top of a stale /
                                // partially-mutated state and dispatch
                                // wrong `state` values to the sink.
                                let msg = format!("patch apply seq={}: {}", ev.seq.0, e);
                                report.failed += 1;
                                report.failures.push(CatchUpFailure {
                                    stream: stream.clone(),
                                    sink_id: sink_id.to_string(),
                                    message: msg,
                                });
                                report.skipped += (head.0 - ev.seq.0) as usize;
                                stream_failed = true;
                                break;
                            }
                            s
                        }
                        None => {
                            // No running state yet: rebuild it as of the
                            // event we're about to dispatch. Cache-nearest
                            // + replay does this cheaply when the cache
                            // stride has ever seen a snapshot near
                            // `ev.seq`; on a rebuild with reset=true and
                            // an empty cache it falls back to a full
                            // replay from Seq(1), which is fine on the
                            // *first* event of the stream but exactly the
                            // cost the running-state carry avoids on
                            // every subsequent event.
                            self.state_at(&stream, ev.seq).await?
                        }
                    };
                    match sink.commit(&stream, ev.seq, &state, &ev).await {
                        Ok(()) => {
                            if self
                                .dispatcher
                                .checkpoint_advance(sink_id, &stream, ev.seq)
                                .await
                            {
                                report.applied += 1;
                                cursor = ev.seq;
                                // Carry state to the next iteration
                                // instead of re-materializing.
                                running_state = Some(state);
                            } else {
                                report.failed += 1;
                                report.failures.push(CatchUpFailure {
                                    stream: stream.clone(),
                                    sink_id: sink_id.to_string(),
                                    message: "checkpoint persistence failed".to_string(),
                                });
                                report.skipped += (head.0 - ev.seq.0) as usize;
                                stream_failed = true;
                                break;
                            }
                        }
                        Err(e) => {
                            report.failed += 1;
                            report.failures.push(CatchUpFailure {
                                stream: stream.clone(),
                                sink_id: sink_id.to_string(),
                                message: e.to_string(),
                            });
                            // Every event after this one on `stream` is
                            // un-processed, not just un-counted — order must
                            // be preserved so we cannot skip ahead to a
                            // later seq while this one is unacknowledged.
                            report.skipped += (head.0 - ev.seq.0) as usize;
                            stream_failed = true;
                            break;
                        }
                    }
                }
            }
        }

        Ok(report)
    }
}
