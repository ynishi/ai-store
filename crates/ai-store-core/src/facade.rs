//! `Store` — the single public write channel and read facade.
//!
//! Every append flows through `Store::append`:
//!
//! 1. Reconstruct `current` state via cache-nearest + event replay.
//! 2. Apply the candidate `patch` to obtain `next`.
//! 3. Invoke every registered `SchemaGate` with a `GateCtx` covering both
//!    states. Any rejection aborts before the backend is touched.
//! 4. Delegate to `EventBackend::append` (one backend-native transaction).
//! 5. Materialize the new state into `CacheBackend` on the configured stride.
//! 6. Dispatch the committed event to every registered `ProjectionSink`
//!    (best-effort — failure leaves the sink's checkpoint unadvanced, so a
//!    later `catch_up` will re-drive it).
//!
//! `state` / `state_at` reconstruct via cache-nearest + replay. `revert` is
//! syntactic sugar: it computes the reverse patch (current → target state)
//! and appends it as a single event, so restoration participates in the same
//! append-only history as any other write.
//!
//! ## Checkpoint storage note
//!
//! Sink checkpoints are held in memory on the facade. A restarted process
//! will re-drive every sink from `Seq(0)`; this is safe because sinks are
//! contracted to be idempotent under retries. Persistent checkpoints are a
//! deliberate follow-up (typically co-located in the `EventBackend`'s DB).

use std::collections::HashMap;
use std::sync::Arc;

use json_patch::diff;
use serde_json::Value;
use tokio::sync::Mutex;

use crate::backend::{CacheBackend, EventBackend};
use crate::error::StoreError;
use crate::event::{Event, NewEvent};
use crate::gate::{GateCtx, SchemaGate};
use crate::id::{Label, Seq, StreamId, Timestamp};
use crate::sink::{CatchUpReport, ProjectionSink};
use crate::state::{empty_state, replay_from};

/// Kind used for the internal event a `revert` writes to the log.
pub const REVERT_KIND: &str = "reverted";

/// Configuration knobs for a `Store` instance.
#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// Materialize state into the cache every N events (0 = never cache).
    pub cache_stride: u64,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self { cache_stride: 64 }
    }
}

/// Public read/write facade. All consumer traffic goes through this type.
#[derive(Clone)]
pub struct Store {
    events: Arc<dyn EventBackend>,
    cache: Arc<dyn CacheBackend>,
    gates: Vec<Arc<dyn SchemaGate>>,
    sinks: Vec<Arc<dyn ProjectionSink>>,
    checkpoints: Arc<Mutex<HashMap<(String, StreamId), Seq>>>,
    config: StoreConfig,
}

impl Store {
    /// Construct a store from a backend pair plus optional gates and sinks.
    pub fn new(
        events: Arc<dyn EventBackend>,
        cache: Arc<dyn CacheBackend>,
        gates: Vec<Arc<dyn SchemaGate>>,
        sinks: Vec<Arc<dyn ProjectionSink>>,
        config: StoreConfig,
    ) -> Self {
        Self {
            events,
            cache,
            gates,
            sinks,
            checkpoints: Arc::new(Mutex::new(HashMap::new())),
            config,
        }
    }

    /// Append one event to `stream`. Returns the assigned `Seq`.
    pub async fn append(
        &self,
        stream: &StreamId,
        kind: &str,
        patch: json_patch::Patch,
        meta: Value,
    ) -> Result<Seq, StoreError> {
        let current = self.state(stream).await?;
        let mut next = current.clone();
        json_patch::patch(&mut next, &patch)
            .map_err(|e| StoreError::Patch(format!("gate preview: {e}")))?;

        for g in &self.gates {
            g.validate(&GateCtx {
                stream,
                kind,
                patch: &patch,
                current: &current,
                next: &next,
            })
            .map_err(StoreError::Schema)?;
        }

        let rec = NewEvent {
            kind: kind.to_string(),
            patch,
            meta,
        };
        let seq = self.events.append(stream, rec).await?;

        if self.config.cache_stride > 0 && seq.0 % self.config.cache_stride == 0 {
            self.cache.put(stream, seq, &next).await?;
        }

        // Post-commit sink dispatch (best-effort; failure leaves checkpoint alone).
        if !self.sinks.is_empty() {
            let events = self.events.read(stream, seq, 1).await?;
            if let Some(ev) = events.into_iter().next() {
                for sink in &self.sinks {
                    let key = (sink.id().to_string(), stream.clone());
                    let checkpoint = {
                        let cps = self.checkpoints.lock().await;
                        cps.get(&key).copied().unwrap_or(Seq::ZERO)
                    };
                    // Skip if already past this seq (catch_up ran concurrently).
                    if seq <= checkpoint {
                        continue;
                    }
                    if sink.commit(stream, seq, &next, &ev).await.is_ok() {
                        // Only advance the checkpoint contiguously. If there is
                        // a gap (an earlier seq failed dispatch), leave the
                        // checkpoint parked so catch_up will re-drive the gap.
                        if seq == checkpoint.next() {
                            let mut cps = self.checkpoints.lock().await;
                            cps.insert(key, seq);
                        }
                    }
                }
            }
        }

        Ok(seq)
    }

    /// Current state of `stream`. Empty streams yield `Value::Null`.
    pub async fn state(&self, stream: &StreamId) -> Result<Value, StoreError> {
        let head = self.events.head(stream).await?;
        let Some(head) = head else {
            return Ok(empty_state());
        };
        self.state_at(stream, head).await
    }

    /// State of `stream` at coordinate `at`. Uses cache-nearest + replay.
    pub async fn state_at(&self, stream: &StreamId, at: Seq) -> Result<Value, StoreError> {
        let head = self.events.head(stream).await?;
        match head {
            None => return Err(StoreError::UnknownStream(stream.clone())),
            Some(h) if at > h => {
                return Err(StoreError::SeqOutOfRange {
                    head: Some(h),
                    requested: at,
                })
            }
            Some(_) => {}
        }

        let (base_state, from) = match self.cache.nearest(stream, at).await? {
            Some((seq, state)) => (state, seq.next()),
            None => (empty_state(), Seq::ZERO.next()),
        };

        if from > at {
            return Ok(base_state);
        }
        let limit = (at.0 - from.0 + 1) as usize;
        let events = self.events.read(stream, from, limit).await?;
        replay_from(base_state, &events)
    }

    /// Revert `stream` to the state at `to` by appending the reverse diff as a
    /// new event. The prior state stays in the log; recovery from mistakes is
    /// yet another revert.
    pub async fn revert(&self, stream: &StreamId, to: Seq) -> Result<Seq, StoreError> {
        let current = self.state(stream).await?;
        let target = self.state_at(stream, to).await?;
        let patch = diff(&current, &target);
        let meta = serde_json::json!({ "revert_to": to.0 });
        self.append(stream, REVERT_KIND, patch, meta).await
    }

    /// Enumerate events. See `EventBackend::read`.
    pub async fn read(
        &self,
        stream: &StreamId,
        from: Seq,
        limit: usize,
    ) -> Result<Vec<Event>, StoreError> {
        self.events.read(stream, from, limit).await
    }

    /// Current head coordinate of `stream`.
    pub async fn head(&self, stream: &StreamId) -> Result<Option<Seq>, StoreError> {
        self.events.head(stream).await
    }

    /// Greatest `Seq` whose event timestamp is `<= at`.
    ///
    /// Useful for wall-clock-anchored operations (e.g. "restore to how the
    /// document looked at 09:00"). Compose with `state_at` to materialize.
    pub async fn seq_at_time(
        &self,
        stream: &StreamId,
        at: Timestamp,
    ) -> Result<Option<Seq>, StoreError> {
        self.events.seq_at_time(stream, at).await
    }

    /// Enumerate all streams.
    pub async fn streams(&self) -> Result<Vec<StreamId>, StoreError> {
        self.events.streams().await
    }

    /// Pin `label` on `stream` to `at`.
    ///
    /// After the backend records the pin, every registered `ProjectionSink`
    /// receives an `on_label_set` notification carrying the freshly
    /// materialized state at `at`. Sink failures are best-effort — they do
    /// not roll back the label change, matching the append dispatch policy.
    pub async fn label_set(
        &self,
        stream: &StreamId,
        label: &Label,
        at: Seq,
    ) -> Result<(), StoreError> {
        self.events.label_set(stream, label, at).await?;
        if !self.sinks.is_empty() {
            let state = self.state_at(stream, at).await?;
            for sink in &self.sinks {
                let _ = sink.on_label_set(stream, label, at, &state).await;
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

    /// Drive `sink_id` forward from its checkpoint to head on every known
    /// stream. On success the checkpoint advances; on failure it stays put.
    pub async fn catch_up(&self, sink_id: &str) -> Result<CatchUpReport, StoreError> {
        self.catch_up_inner(sink_id, false).await
    }

    /// Reset `sink_id`'s checkpoint to zero on every stream, then drive it
    /// forward. Equivalent to `catch_up` after checkpoint reset — no special
    /// rebuild API is needed at the backend level.
    pub async fn rebuild(&self, sink_id: &str) -> Result<CatchUpReport, StoreError> {
        self.catch_up_inner(sink_id, true).await
    }

    async fn catch_up_inner(
        &self,
        sink_id: &str,
        reset: bool,
    ) -> Result<CatchUpReport, StoreError> {
        let Some(sink) = self.sinks.iter().find(|s| s.id() == sink_id).cloned() else {
            return Ok(CatchUpReport::EMPTY);
        };
        let streams = self.events.streams().await?;
        let mut report = CatchUpReport::EMPTY;

        for stream in streams {
            if reset {
                let mut cps = self.checkpoints.lock().await;
                cps.remove(&(sink_id.to_string(), stream.clone()));
            }

            let head = match self.events.head(&stream).await? {
                Some(h) => h,
                None => continue,
            };
            let mut cursor = {
                let cps = self.checkpoints.lock().await;
                cps.get(&(sink_id.to_string(), stream.clone()))
                    .copied()
                    .unwrap_or(Seq::ZERO)
            };

            while cursor < head {
                let from = cursor.next();
                let events = self.events.read(&stream, from, 32).await?;
                if events.is_empty() {
                    break;
                }
                for ev in events {
                    let state = self.state_at(&stream, ev.seq).await?;
                    match sink.commit(&stream, ev.seq, &state, &ev).await {
                        Ok(()) => {
                            report.applied += 1;
                            cursor = ev.seq;
                            let mut cps = self.checkpoints.lock().await;
                            cps.insert((sink_id.to_string(), stream.clone()), ev.seq);
                        }
                        Err(_) => {
                            report.failed += 1;
                            // Leave checkpoint at cursor (not advanced past this seq).
                            return Ok(report);
                        }
                    }
                }
            }
        }

        Ok(report)
    }
}
