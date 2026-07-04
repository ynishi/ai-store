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
//! Every write above (`append` / `import_event` / `revert`) executes inside
//! a per-stream write lock: the state read, gate validation, backend append,
//! cache write, and sink dispatch for a single stream never interleave with
//! another concurrent write to that *same* stream. Writes to different
//! streams remain fully concurrent — the lock is keyed on `StreamId`, not
//! global. This closes a read-validate-write race: without it, two
//! concurrent writers could both read the same `current` state, both pass
//! gate validation against it, and both append — each backend-assigns its
//! own `Seq` correctly, but a gate enforcing a postcondition on `next` (e.g.
//! "at most N children") could be fooled because it never saw the other
//! writer's effect.
//!
//! ## Checkpoint storage note
//!
//! Sink checkpoints are held in memory by default ([`Store::new`]). A
//! restarted process then re-drives every sink from `Seq(0)`; this is safe
//! because sinks are contracted to be idempotent under retries, but it does
//! mean every sink replays its entire history after every restart.
//!
//! [`Store::with_checkpoint_backend`] attaches a [`crate::CheckpointBackend`]
//! so checkpoints survive process restarts instead: the in-memory map still
//! serves as an L1 cache, but a miss falls back to
//! [`crate::CheckpointBackend::get`] instead of assuming `Seq::ZERO`, and
//! every checkpoint advance persists via [`crate::CheckpointBackend::put`]
//! before it is considered durable. Backend read/write failures fail *open*
//! (treated as "no checkpoint" / "advance not yet durable") rather than
//! aborting the write or the catch-up loop — consistent with the
//! idempotence contract sinks already have to uphold, and strictly safer
//! than the alternative of blocking a successful backend append on
//! checkpoint bookkeeping.
//!
//! ## Cost model for large stream states
//!
//! `Store::append` computes `next = current + patch` in memory. When the
//! per-stream state is a large document tree (tens of MB, tens of thousands
//! of nodes) the shape of the append hot path matters.
//!
//! ### Per-append memory
//!
//! - `current` reconstruction is O(state size) once per append (cache-nearest
//!   + replay from the last cache entry).
//! - When any [`crate::SchemaGate`] is registered, `next` is materialized
//!   pre-commit for the gate loop: peak ≈ 2× state size.
//! - When no gates are registered and the assigned `seq` misses the
//!   [`StoreConfig::cache_stride`] boundary and no [`crate::ProjectionSink`]
//!   is registered, `next` is not materialized at all — the fast path skips
//!   the clone + patch entirely.
//! - When no gates are registered but either a cache write or sink dispatch
//!   is needed, `next` is materialized once post-commit (same total cost as
//!   the gate path, but ordering shifts).
//!
//! ### Cache stride trade-off
//!
//! - `cache_stride = N` materializes `next` and writes it into
//!   [`crate::CacheBackend`] every N events. Larger N → fewer JSON
//!   serializations and backend writes, at the cost of longer replay chains
//!   on `state_at`.
//! - `cache_stride = 0` disables the cache entirely; every state read replays
//!   from `Seq(0)` (or the last replay origin the backend chooses to
//!   pin). Only sensible when the stream is short-lived or state reads are
//!   rare.
//! - For large states, a stride in the 256–1024 range typically balances the
//!   two costs; measure and tune per workload.
//!
//! ### Stream granularity
//!
//! - Per-entity streams — many small states, low per-append cost, but every
//!   stream costs some backend index / metadata overhead.
//! - Document-level streams — one large state, high per-append cost, but
//!   invariants that span the whole document can be enforced by a single
//!   gate.
//!
//! A useful rule of thumb: split the document into per-entity streams once
//! per-append memory (≈ 2× state size when gates run) is measured in the
//! high-single-digit MB and no gate genuinely needs the whole document as
//! one unit. `read_by_meta` (indexed on the SQLite backend) then answers
//! per-entity histories without linear scans.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use json_patch::diff;
use serde_json::Value;
use tokio::sync::Mutex;

use crate::backend::{CacheBackend, CheckpointBackend, EventBackend};
use crate::error::StoreError;
use crate::event::{Event, NewEvent};
use crate::gate::{GateCtx, SchemaGate};
use crate::id::{Label, Seq, StreamId, Timestamp};
use crate::sink::{CatchUpFailure, CatchUpReport, ProjectionSink};
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
    checkpoint_backend: Option<Arc<dyn CheckpointBackend>>,
    /// Per-stream write lock, keyed on `StreamId`. Serializes the
    /// state-read -> gate -> backend-append -> sink-dispatch critical
    /// section of `append` / `import_event` / `revert` for a single stream,
    /// while leaving different streams fully concurrent. Guarded by a plain
    /// `std::sync::Mutex` (the critical section here is just a hashmap
    /// lookup/insert, never held across an `.await`); the inner lock is a
    /// `tokio::sync::Mutex` because *that* guard is held across awaits.
    ///
    /// Entries are never evicted — long-running processes that write to an
    /// unbounded number of distinct streams will accumulate one entry per
    /// stream ever seen. This mirrors the existing `checkpoints` map (same
    /// trade-off, same justification: the alternative is a correctness bug).
    stream_locks: Arc<StdMutex<HashMap<StreamId, Arc<Mutex<()>>>>>,
    config: StoreConfig,
}

impl Store {
    /// Construct a store from a backend pair plus optional gates and sinks.
    ///
    /// Sink checkpoints live only in process memory — see the crate-level
    /// "Checkpoint storage note". Use [`Store::with_checkpoint_backend`] for
    /// checkpoints that survive a restart.
    pub fn new(
        events: Arc<dyn EventBackend>,
        cache: Arc<dyn CacheBackend>,
        gates: Vec<Arc<dyn SchemaGate>>,
        sinks: Vec<Arc<dyn ProjectionSink>>,
        config: StoreConfig,
    ) -> Self {
        Self::new_inner(events, cache, gates, sinks, config, None)
    }

    /// Construct a store whose sink checkpoints are restored from (and
    /// persisted to) `checkpoint_backend`, surviving process restarts.
    ///
    /// Everything else is identical to [`Store::new`] — the same gates
    /// validate writes, the same sinks are dispatched, the same cache-stride
    /// rule governs materialization. See the crate-level "Checkpoint
    /// storage note" for the durability contract this adds.
    pub fn with_checkpoint_backend(
        events: Arc<dyn EventBackend>,
        cache: Arc<dyn CacheBackend>,
        gates: Vec<Arc<dyn SchemaGate>>,
        sinks: Vec<Arc<dyn ProjectionSink>>,
        config: StoreConfig,
        checkpoint_backend: Arc<dyn CheckpointBackend>,
    ) -> Self {
        Self::new_inner(
            events,
            cache,
            gates,
            sinks,
            config,
            Some(checkpoint_backend),
        )
    }

    fn new_inner(
        events: Arc<dyn EventBackend>,
        cache: Arc<dyn CacheBackend>,
        gates: Vec<Arc<dyn SchemaGate>>,
        sinks: Vec<Arc<dyn ProjectionSink>>,
        config: StoreConfig,
        checkpoint_backend: Option<Arc<dyn CheckpointBackend>>,
    ) -> Self {
        Self {
            events,
            cache,
            gates,
            sinks,
            checkpoints: Arc::new(Mutex::new(HashMap::new())),
            checkpoint_backend,
            stream_locks: Arc::new(StdMutex::new(HashMap::new())),
            config,
        }
    }

    /// Return the (cloned) per-stream write lock for `stream`, creating an
    /// entry if this is the first time `stream` has been written to.
    fn stream_lock(&self, stream: &StreamId) -> Arc<Mutex<()>> {
        let mut locks = self.stream_locks.lock().unwrap_or_else(|e| e.into_inner());
        locks
            .entry(stream.clone())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
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
    async fn checkpoint_get(&self, sink_id: &str, stream: &StreamId) -> Seq {
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
    async fn checkpoint_advance(&self, sink_id: &str, stream: &StreamId, seq: Seq) -> bool {
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
    /// [`Store::checkpoint_advance`] call re-persists the correct seq,
    /// self-healing the drift. Reset is only ever a prelude to immediately
    /// driving the sink forward again (see [`Store::rebuild`]), so there is
    /// no window where a stale persisted value would be read back.
    async fn checkpoint_reset(&self, sink_id: &str, stream: &StreamId) {
        {
            let mut cps = self.checkpoints.lock().await;
            cps.remove(&(sink_id.to_string(), stream.clone()));
        }
        if let Some(backend) = &self.checkpoint_backend {
            let _ = backend.put(sink_id, stream, Seq::ZERO).await;
        }
    }

    /// Append one event to `stream`. Returns the assigned `Seq`.
    ///
    /// The backend stamps `at` with the wall-clock time of this call — use
    /// this for ordinary domain writes. See [`Store::import_event`] for the
    /// historical-timestamp counterpart used by import/migration paths.
    ///
    /// Fast path: when no [`SchemaGate`] is registered, `next` is not
    /// materialized pre-commit. If the assigned `seq` misses the cache stride
    /// and no [`ProjectionSink`] is registered, `next` is not materialized at
    /// all. See the crate-level cost-model section for the full breakdown.
    pub async fn append(
        &self,
        stream: &StreamId,
        kind: &str,
        patch: json_patch::Patch,
        meta: Value,
    ) -> Result<Seq, StoreError> {
        let lock = self.stream_lock(stream);
        let _guard = lock.lock().await;
        self.write_event_locked(stream, kind, patch, meta, None)
            .await
    }

    /// Import one event into `stream`, recording `at` as its time coordinate
    /// instead of the wall-clock time of this call.
    ///
    /// Identical to [`Store::append`] in every other respect — the same
    /// gates validate the write, the same sinks are dispatched, the same
    /// cache-stride rule decides whether `next` is materialized. The only
    /// difference is which [`EventBackend`] method is invoked: `append`
    /// delegates to [`EventBackend::append`] (backend stamps "now"),
    /// `import_event` delegates to [`EventBackend::import_event`] (backend
    /// stamps the supplied `at`). This is the escape hatch for backfilling
    /// history that already has its own notion of "when" — an import from a
    /// legacy system, for example — without discarding that timeline.
    ///
    /// Use `append` for ordinary domain writes; reach for `import_event` only
    /// on import/migration paths.
    ///
    /// ## `at` is a time coordinate, not an ordering key
    ///
    /// Every event carries a time coordinate (`at`) and a log position
    /// (`seq`). `append` always sets `at` to the wall-clock moment of the
    /// write; `import_event` lets the caller substitute a different
    /// coordinate — typically the moment the change happened in the source
    /// system being migrated in. Either way, **`seq` orders the log; `at`
    /// never does.**
    ///
    /// ## Non-monotonic `at` and [`Store::seq_at_time`]
    ///
    /// [`Store::seq_at_time`] answers "the greatest `seq` whose `at` is `<=`
    /// the given timestamp", which only matches intuition when a stream's
    /// `at` values are non-decreasing in `seq` order — true automatically for
    /// a stream written entirely via `append` (wall-clock time only moves
    /// forward). `import_event` does not enforce that invariant: the
    /// supplied `at` may be less than, equal to, or greater than the
    /// timestamp already recorded at the stream's head.
    ///
    /// Backfilling into an **empty** stream in chronological source order —
    /// the typical migration shape — keeps the non-decreasing assumption
    /// intact by construction, so `seq_at_time` behaves exactly as it would
    /// for an `append`-only stream. Importing out of order, or mixing
    /// `import_event` into a stream that already has `append`ed events on a
    /// different clock, can leave `seq_at_time` answering a query in a way
    /// that does not match intuition for that stream (backends are not
    /// required to detect or reject this). Callers who need that guarantee
    /// should read the current head event (`Store::head` + `Store::read`)
    /// and compare its `at` before importing.
    ///
    /// Returns [`StoreError::BackendUnsupported`] if the underlying
    /// [`EventBackend`] has not overridden [`EventBackend::import_event`]
    /// (the default implementation declines).
    pub async fn import_event(
        &self,
        stream: &StreamId,
        kind: &str,
        patch: json_patch::Patch,
        meta: Value,
        at: Timestamp,
    ) -> Result<Seq, StoreError> {
        let lock = self.stream_lock(stream);
        let _guard = lock.lock().await;
        self.write_event_locked(stream, kind, patch, meta, Some(at))
            .await
    }

    /// Shared write path for [`Store::append`] and [`Store::import_event`].
    ///
    /// `at = None` delegates to [`EventBackend::append`] (backend stamps
    /// "now"); `at = Some(_)` delegates to [`EventBackend::import_event`]
    /// (backend stamps the supplied timestamp). Every other step — gate
    /// validation, `next` materialization, cache write, sink dispatch — is
    /// identical between the two callers.
    ///
    /// Callers must hold `self.stream_lock(stream)` before calling this —
    /// it assumes exclusive access to `stream`'s write path and does not
    /// acquire the lock itself, so that [`Store::revert`] can take the lock
    /// once and cover both its own state reads and this method's body in a
    /// single critical section.
    async fn write_event_locked(
        &self,
        stream: &StreamId,
        kind: &str,
        patch: json_patch::Patch,
        meta: Value,
        at: Option<Timestamp>,
    ) -> Result<Seq, StoreError> {
        let current = self.state(stream).await?;
        let has_gates = !self.gates.is_empty();

        // Pre-commit next materialization is only needed when a gate will
        // read it. Otherwise defer — post-commit paths (cache put / sink
        // dispatch) may not need it either.
        let precomputed_next = if has_gates {
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
            Some(next)
        } else {
            None
        };

        // Retain a patch clone only when we might have to reapply post-commit
        // (no gates but cache/sink paths may still need `next`). `Patch` is
        // `Vec<PatchOperation>` — cheap to clone relative to the state itself.
        let patch_for_reapply = if precomputed_next.is_none() {
            Some(patch.clone())
        } else {
            None
        };

        let rec = NewEvent {
            kind: kind.to_string(),
            patch,
            meta,
        };
        let seq = match at {
            Some(at) => self.events.import_event(stream, rec, at).await?,
            None => self.events.append(stream, rec).await?,
        };

        let cache_hit = self.config.cache_stride > 0 && seq.0 % self.config.cache_stride == 0;
        let needs_next = cache_hit || !self.sinks.is_empty();

        // Materialize `next` only if a downstream path actually reads it.
        let next: Option<Value> = if let Some(n) = precomputed_next {
            Some(n)
        } else if needs_next {
            let mut n = current.clone();
            json_patch::patch(&mut n, patch_for_reapply.as_ref().unwrap())
                .map_err(|e| StoreError::Patch(format!("post-commit reapply: {e}")))?;
            Some(n)
        } else {
            None
        };

        if let Some(ref next_state) = next {
            if cache_hit {
                self.cache.put(stream, seq, next_state).await?;
            }

            // Post-commit sink dispatch (best-effort; failure leaves checkpoint alone).
            if !self.sinks.is_empty() {
                let events = self.events.read(stream, seq, 1).await?;
                if let Some(ev) = events.into_iter().next() {
                    for sink in &self.sinks {
                        let checkpoint = self.checkpoint_get(sink.id(), stream).await;
                        // Skip if already past this seq (catch_up ran concurrently).
                        if seq <= checkpoint {
                            continue;
                        }
                        if sink.commit(stream, seq, next_state, &ev).await.is_ok() {
                            // Only advance the checkpoint contiguously. If there is
                            // a gap (an earlier seq failed dispatch), leave the
                            // checkpoint parked so catch_up will re-drive the gap.
                            // A failed persist (see `checkpoint_advance`) is
                            // likewise left parked for catch_up to redrive.
                            if seq == checkpoint.next() {
                                self.checkpoint_advance(sink.id(), stream, seq).await;
                            }
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
    ///
    /// Holds the per-stream write lock across both the `current`/`target`
    /// reads and the resulting append, so no concurrent write to `stream`
    /// can land between "diff computed against `current`" and "diff
    /// appended" — that gap would otherwise let the reverse patch apply on
    /// top of a `current` that is no longer the stream's real state.
    pub async fn revert(&self, stream: &StreamId, to: Seq) -> Result<Seq, StoreError> {
        let lock = self.stream_lock(stream);
        let _guard = lock.lock().await;
        let current = self.state(stream).await?;
        let target = self.state_at(stream, to).await?;
        let patch = diff(&current, &target);
        let meta = serde_json::json!({ "revert_to": to.0 });
        self.write_event_locked(stream, REVERT_KIND, patch, meta, None)
            .await
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

    /// Enumerate events whose top-level `meta[field]` equals `value`. See
    /// [`EventBackend::read_by_meta`].
    pub async fn read_by_meta(
        &self,
        stream: &StreamId,
        field: &str,
        value: &Value,
        from: Seq,
        limit: usize,
    ) -> Result<Vec<Event>, StoreError> {
        self.events
            .read_by_meta(stream, field, value, from, limit)
            .await
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

    /// Delete `label` from `stream`.
    ///
    /// Idempotent: deleting a label that is not defined is **not** an error.
    /// Returns `Ok(true)` when the label existed and was removed, `Ok(false)`
    /// when it was already absent — mirroring the backend's
    /// [`EventBackend::label_delete`] contract. Callers that need the strict
    /// "must have existed" behavior can match on the returned `bool`
    /// themselves.
    ///
    /// After the backend removes the label, every registered
    /// [`ProjectionSink`] receives an `on_label_deleted` notification — but
    /// only when the label actually existed (a no-op delete dispatches
    /// nothing, since nothing changed). Sink failures are best-effort —
    /// matching the dispatch policy of [`Store::label_set`] and
    /// [`Store::append`], a sink error does not roll back the deletion.
    pub async fn label_delete(&self, stream: &StreamId, label: &Label) -> Result<bool, StoreError> {
        let existed = self.events.label_delete(stream, label).await?;
        if existed && !self.sinks.is_empty() {
            for sink in &self.sinks {
                let _ = sink.on_label_deleted(stream, label).await;
            }
        }
        Ok(existed)
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
            .sinks
            .iter()
            .find(|s| s.id() == sink_id)
            .cloned()
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
    pub async fn catch_up(&self, sink_id: &str) -> Result<CatchUpReport, StoreError> {
        self.catch_up_inner(sink_id, false).await
    }

    /// Reset `sink_id`'s checkpoint to zero on every stream, then drive it
    /// forward. Equivalent to `catch_up` after checkpoint reset — no special
    /// rebuild API is needed at the backend level.
    pub async fn rebuild(&self, sink_id: &str) -> Result<CatchUpReport, StoreError> {
        self.catch_up_inner(sink_id, true).await
    }

    /// Drive `sink_id` forward on every stream, isolating failures per
    /// stream rather than aborting the whole call on the first one.
    ///
    /// A failing `commit` (or a failed checkpoint persist — see
    /// [`Store::checkpoint_advance`]) halts catch-up for *that stream only*:
    /// order within a stream must be preserved, so every remaining event on
    /// the failed stream is counted in [`CatchUpReport::skipped`] rather
    /// than applied out of order, and one [`CatchUpFailure`] is recorded.
    /// Every other stream is still driven to completion in the same call.
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
                self.checkpoint_reset(sink_id, &stream).await;
            }

            let head = match self.events.head(&stream).await? {
                Some(h) => h,
                None => continue,
            };
            let mut cursor = self.checkpoint_get(sink_id, &stream).await;
            let mut stream_failed = false;

            while cursor < head && !stream_failed {
                let from = cursor.next();
                let events = self.events.read(&stream, from, 32).await?;
                if events.is_empty() {
                    break;
                }
                for ev in events {
                    let state = self.state_at(&stream, ev.seq).await?;
                    match sink.commit(&stream, ev.seq, &state, &ev).await {
                        Ok(()) => {
                            if self.checkpoint_advance(sink_id, &stream, ev.seq).await {
                                report.applied += 1;
                                cursor = ev.seq;
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
