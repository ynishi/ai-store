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
//!
//! ## How deletion works
//!
//! Deletion is an event, not a physical removal from the log. [`Store::delete`]
//! appends a single event of kind [`TOMBSTONE_KIND`] through the same
//! `SchemaGate` + `ProjectionSink` pipeline as any other write, carrying an
//! empty patch (the materialized state does not change). Downstream consumers
//! recognize the tombstone by matching the kind against `TOMBSTONE_KIND`
//! rather than a per-consumer string:
//!
//! - [`Store::streams_live`] enumerates streams whose most recent event is
//!   *not* a tombstone, without requiring a read-model round-trip.
//! - The SQLite read-model sink (`SqliteReadModel`) flips its per-row `live`
//!   flag to `0` on the tombstone and back to `1` on any subsequent
//!   non-tombstone event, so the same append that "deletes" a stream can be
//!   followed by another that "revives" it.
//! - A [`crate::KindGate`] can register a validator on `TOMBSTONE_KIND` (or
//!   use it inside a fallback) to enforce a delete policy — e.g. reject
//!   further appends after a tombstone. `KindGate` itself does not special-case
//!   the constant, mirroring how it treats [`REVERT_KIND`].
//!
//! Because the tombstone is a normal event on the log, `state_at(stream, seq)`
//! for a `seq` *before* the tombstone still reconstructs the pre-delete state
//! bit-for-bit, and [`Store::revert`] to that `seq` produces a new event whose
//! reverse patch is empty (the tombstone did not change state) — the "undo"
//! for a delete is really the *next* non-tombstone append. Physical removal
//! of events is a separate concern; see "Compaction and history boundary"
//! below.
//!
//! ## Compaction and history boundary
//!
//! Deletion above is a logical marker — nothing shrinks on disk. Backend-side
//! *compaction* is the escape hatch for streams whose history is not needed
//! to a certain seq any more: a maintenance API on the backend
//! (`ai_store_sqlite::SqliteMaintenance::compact_stream` for the bundled
//! SQLite backend) atomically replaces a stream's event prefix
//! `[Seq(1) .. boundary]` with a single snapshot event of kind
//! [`SNAPSHOT_KIND`] at `boundary`, whose patch is an `add "/"` that
//! materializes the pre-compaction state directly. `Store::append` itself
//! never produces a snapshot event — the maintenance path is the only writer
//! — and the append-only DDL triggers stay in force during normal operation:
//! the maintenance operation drops them inside its own transaction and
//! re-creates them before committing, so no other connection ever observes a
//! state where a raw `DELETE` against `events` would be permitted.
//!
//! After compaction, `Store::state_at(stream, seq)`:
//!
//! - for `seq < boundary` returns [`StoreError::SeqCompacted`] — the events
//!   that would be needed to reconstruct the pre-boundary state have been
//!   discarded, so the state is not materially reachable any more,
//! - for `seq == boundary` returns the snapshot state directly (the snapshot
//!   event's patch is the only replay step needed),
//! - for `seq > boundary` replays forward from the snapshot as normal.
//!
//! `Store::revert(stream, to)` and `Store::revert_with_meta` inherit the
//! same error: they call `state_at(stream, to)` internally and surface
//! `SeqCompacted` verbatim when `to` falls below the boundary. `Store::head`
//! and `Store::seq_at_time` are not clamped: they can still return seqs at or
//! below the boundary purely because those events physically existed at that
//! coordinate in the log — a subsequent `state_at` on a `seq_at_time` result
//! that falls below the boundary is the one that surfaces the error.
//!
//! Archived exports of the compacted-away events and policy-driven retention
//! that drives when compaction runs are out of scope here and tracked as
//! Phase 2 / Phase 3 follow-ups.
//!
//! ## Concurrency model
//!
//! `Store` guarantees different amounts of isolation depending on whether
//! writers share only a process, or share a database file across processes.
//!
//! ### Within one process (single `Store` instance)
//!
//! [`Store::append`] / [`Store::import_event`] / [`Store::revert`] /
//! [`Store::delete`] each acquire the [`Store::stream_lock`] entry keyed on
//! the target `StreamId` (a per-stream `tokio::Mutex`) before running the
//! state read + gate validation + backend append + cache write + sink
//! dispatch. Writes to the *same* stream from concurrent tasks on the same
//! `Store` are serialized end-to-end by that lock, so a
//! [`SchemaGate`] enforcing a postcondition on `next` never sees an
//! intermediate state produced by another writer. Writes to *different*
//! streams remain fully concurrent (the lock is per-`StreamId`, not global).
//!
//! ### Across processes (or across `Store` instances on one file)
//!
//! The per-stream lock is a process-local `tokio::Mutex` — it does not
//! reach a second process, nor a second `Store` instance in this process
//! pointing at the same database file. In that shape, [`Store::append`]
//! offers only best-effort isolation:
//!
//! - The backend still assigns a gap-free monotonic `Seq` (the SQLite
//!   backend does its `MAX(seq) + 1` allocation inside a `BEGIN IMMEDIATE`
//!   transaction), so two concurrent `append` calls never produce
//!   duplicate seqs.
//! - But a [`SchemaGate`] validating against `current` in process A can
//!   have its precondition invalidated by a write from process B that
//!   lands between A's state read and A's own backend append. In this
//!   deployment shape, `SchemaGate` is a validation aid, not a hard
//!   invariant.
//!
//! [`Store::append_if_head`] is the escape hatch for that case: it hands
//! the caller's expected head down to the backend, which runs the head
//! check and the insert as a single transaction. A racing writer either
//! commits first (and this call returns
//! [`crate::StoreError::HeadConflict`]) or waits behind the file lock
//! until this call finishes — either way the invariant holds. Callers who
//! know their deployment has one process writing to the file can keep
//! using `append` unchanged; callers who cannot make that assumption
//! should reach for `append_if_head`.
//!
//! Backends that do not implement head-conditional append (the trait's
//! default returns [`crate::StoreError::BackendUnsupported`]) simply do
//! not support this pattern. The bundled `ai-store-sqlite` backend
//! overrides it; `ai-store-mem` intentionally does not, since a single
//! in-memory `Store` has no cross-process concurrency to guard against.
//!
//! ## Failure handling and best-effort paths
//!
//! `Store` classifies failures into typed variants so callers can decide
//! how to react without pattern-matching on error message text. The
//! actionable distinctions live on [`crate::StoreError`]:
//!
//! - [`crate::StoreError::Busy`] — resource contention that a bounded retry
//!   can resolve. [`crate::StoreError::is_retryable`] returns `true` for
//!   this variant only; every other failure is a durable rejection
//!   (schema, patch, head conflict, corruption, storage, unsupported).
//! - [`crate::StoreError::Storage`] — durable I/O failure (disk full,
//!   permission denied, read-only medium). Retrying will not help until
//!   the underlying condition changes.
//! - [`crate::StoreError::Corruption`] — persisted data no longer decodes
//!   to the expected shape. Indicates out-of-band tampering, an aborted
//!   maintenance job, or a version skew; not retryable.
//! - [`crate::StoreError::Backend`] — last-resort fallback when the
//!   backend could not classify the failure into any of the above.
//!
//! Two paths inside the write critical section are *best-effort by design*
//! rather than surfaced as errors:
//!
//! 1. **Sink dispatch**: after `append` durably commits the event, each
//!    registered [`crate::ProjectionSink`] receives the new state inline.
//!    A sink whose `commit` returns `Err` leaves its checkpoint parked so
//!    the next `catch_up` re-drives it — but the error itself is *not*
//!    propagated to the `append` caller (the event is durable regardless).
//!    Attach a [`crate::SinkFailureObserver`] via
//!    [`crate::StoreBuilder::sink_failure_observer`] to observe these
//!    silent falls-behind: the observer is invoked from within the write
//!    path (after the log write, before `append` returns), and receives a
//!    [`crate::SinkDispatchFailure`] naming the sink, stream, seq, and
//!    operation. The same observer covers `on_label_set` /
//!    `on_label_deleted` dispatches after `label_set` / `label_delete`.
//! 2. **Automatic cache pruning**: when
//!    [`StoreConfig::cache_keep_latest`] is set, a failed prune after a
//!    cache-stride write is silently ignored — the next cache-stride
//!    write retries, and [`Store::prune_cache`] remains available as an
//!    explicit maintenance entry point. Failures here never fail the
//!    `append`.
//!
//! ### Cache growth model
//!
//! [`crate::CacheBackend`] is derived state, not source of truth. Every
//! cache-stride write inserts one row per stream, so a stream with head
//! `H` and stride `S` carries `ceil(H / S)` cache rows unless something
//! prunes them. The default configuration
//! ([`StoreConfig::cache_keep_latest`] `= None`) preserves the pre-existing
//! behavior: cache rows accumulate and the caller is responsible for
//! pruning. Two ways to bound the footprint:
//!
//! - Set `cache_keep_latest = Some(k)` on [`StoreConfig`] (or use
//!   [`crate::StoreBuilder::cache_keep_latest`]) to automatically keep
//!   only the `k` most-recent cache rows per stream after every
//!   cache-stride write. This is the low-effort path — call sites keep
//!   using `append` unchanged.
//! - Call [`Store::prune_cache`] explicitly during maintenance windows.
//!   This is the low-latency path — a bulk prune in a quiet period
//!   trades one operation for many trimmed rows.
//!
//! Neither option risks history: [`Store::state_at`] reconstructs any lost
//! snapshot by replaying forward from a still-cached nearest neighbor.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use json_patch::diff;
use serde_json::{Map, Value};
use tokio::sync::Mutex;

use crate::backend::{CacheBackend, CheckpointBackend, EventBackend};
use crate::builder::StoreBuilder;
use crate::error::StoreError;
use crate::event::{Committed, Event, NewEvent};
use crate::gate::{GateCtx, SchemaGate};
use crate::id::{Label, Seq, StreamId, Timestamp};
use crate::sink::{
    CatchUpFailure, CatchUpReport, ProjectionSink, SinkDispatchFailure, SinkFailureObserver, SinkOp,
};
use crate::state::{empty_state, replay_from};

/// Kind used for the internal event a `revert` writes to the log.
pub const REVERT_KIND: &str = "reverted";

/// Canonical kind for the snapshot event a compaction maintenance operation
/// leaves at the compaction boundary.
///
/// Compaction is not a facade concern — the facade never *appends* an event
/// of this kind. It is produced by backend-specific maintenance APIs
/// (`ai_store_sqlite::SqliteMaintenance::compact_stream` for the bundled
/// SQLite backend) that atomically replace a stream's event prefix with one
/// event of this kind carrying the materialized state at the compaction
/// boundary in its patch. Consumers that gate on kind should either accept
/// `SNAPSHOT_KIND` (a normal replay path — the snapshot patch is a plain
/// `add "/"` that populates the base state) or reject appends of it via
/// [`crate::KindGate`], since a foreign `Store::append` with this kind would place a
/// bogus "snapshot" mid-stream that later replays would fold on top of real
/// state.
///
/// See the "Compaction and history boundary" section in the module-level
/// rustdoc, and [`StoreError::SeqCompacted`] for the boundary error returned
/// from [`Store::state_at`] / [`Store::revert`] on pre-boundary seqs.
pub const SNAPSHOT_KIND: &str = "compacted";

/// Canonical kind for the tombstone event [`Store::delete`] appends.
///
/// Deletion in ai-store is an event, not an out-of-band flag: `Store::delete`
/// appends an event of this kind through the same gate + sink pipeline as any
/// other write, and downstream consumers (`SqliteReadModel`, `KindGate`
/// validators, `Store::streams_live`) recognize it by matching this constant
/// rather than a per-consumer string. See the "How deletion works" section in
/// the module-level rustdoc.
pub const TOMBSTONE_KIND: &str = "tombstoned";

/// Which backend write method the shared `write_event_locked` path dispatches
/// to. Kept private — `Store` maps its public methods
/// ([`Store::append`] / [`Store::import_event`] / [`Store::append_if_head`] /
/// [`Store::revert_with_meta`]) to the appropriate variant.
enum WriteMode {
    /// Ordinary append. Backend stamps wall-clock `at`.
    Append,
    /// Historical import. Backend stamps the supplied timestamp instead of
    /// "now".
    Import(Timestamp),
    /// Optimistic-concurrency append. Backend runs the head check + insert
    /// as one transaction and returns [`StoreError::HeadConflict`] on
    /// mismatch. `expected_head == Seq::ZERO` means "expect the stream to
    /// currently be empty".
    AppendIfHead(Seq),
}

/// Configuration knobs for a `Store` instance.
#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// Materialize state into the cache every N events (0 = never cache).
    pub cache_stride: u64,
    /// If set, after every cache-stride write the facade opportunistically
    /// asks the [`crate::CacheBackend`] to keep only the `keep_latest` most
    /// recent snapshots for that stream, deleting older ones. Trades
    /// slightly-longer replays on `state_at` for a bounded cache footprint.
    ///
    /// `None` (the default) preserves the pre-existing behavior: cache
    /// entries accumulate at one per `cache_stride` events per stream, and
    /// pruning is the caller's responsibility (either via
    /// [`Store::prune_cache`] or a backend-specific maintenance API).
    /// `Some(k)` opts into automatic bounded caching: a cache-stride write
    /// that lands in an already-K-deep cache silently trims the oldest
    /// entries.
    pub cache_keep_latest: Option<usize>,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            cache_stride: 64,
            cache_keep_latest: None,
        }
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
    /// Optional visibility hook — see [`crate::SinkFailureObserver`].
    /// Invoked from inline dispatch (`commit` after `append`,
    /// `on_label_set` after `label_set`, `on_label_deleted` after
    /// `label_delete`) whenever a sink returns `Err`. Dispatch semantics
    /// themselves (checkpoint parked / label backend already advanced) are
    /// unchanged.
    sink_failure_observer: Option<Arc<dyn SinkFailureObserver>>,
    config: StoreConfig,
}

impl Store {
    /// Start an incremental [`StoreBuilder`] over the two mandatory
    /// backends.
    ///
    /// Reduces the construction boilerplate of [`Store::new`] /
    /// [`Store::with_checkpoint_backend`] at DI boundaries — a caller with no
    /// gates and no sinks no longer writes out `Vec::new(), Vec::new()`, and
    /// one that wants persisted checkpoints does not have to pick a
    /// different constructor up front:
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use ai_store_core::Store;
    /// # fn demo(events: Arc<dyn ai_store_core::EventBackend>, cache: Arc<dyn ai_store_core::CacheBackend>) {
    /// let store = Store::builder(events, cache)
    ///     .cache_stride(256)
    ///     .build();
    /// # let _ = store;
    /// # }
    /// ```
    pub fn builder(events: Arc<dyn EventBackend>, cache: Arc<dyn CacheBackend>) -> StoreBuilder {
        StoreBuilder::new(events, cache)
    }

    /// Construct a store from a backend pair plus optional gates and sinks.
    ///
    /// Sink checkpoints live only in process memory — see the crate-level
    /// "Checkpoint storage note". Use [`Store::with_checkpoint_backend`] for
    /// checkpoints that survive a restart. [`Store::builder`] is the more
    /// ergonomic entry point when only a subset of gates/sinks/checkpoints
    /// is needed.
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
        Self::new_inner_with_observer(events, cache, gates, sinks, config, checkpoint_backend, None)
    }

    /// Inner constructor that also accepts an optional sink failure
    /// observer. Kept crate-private because the exposed way to attach an
    /// observer is [`StoreBuilder::sink_failure_observer`] — the builder
    /// stays the one construction entry point that knows every optional
    /// hook.
    pub(crate) fn new_inner_with_observer(
        events: Arc<dyn EventBackend>,
        cache: Arc<dyn CacheBackend>,
        gates: Vec<Arc<dyn SchemaGate>>,
        sinks: Vec<Arc<dyn ProjectionSink>>,
        config: StoreConfig,
        checkpoint_backend: Option<Arc<dyn CheckpointBackend>>,
        sink_failure_observer: Option<Arc<dyn SinkFailureObserver>>,
    ) -> Self {
        Self {
            events,
            cache,
            gates,
            sinks,
            checkpoints: Arc::new(Mutex::new(HashMap::new())),
            checkpoint_backend,
            stream_locks: Arc::new(StdMutex::new(HashMap::new())),
            sink_failure_observer,
            config,
        }
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

    /// Append one event to `stream`. Returns the [`Committed`] coordinates
    /// (`seq` and the `at` the backend stamped) the backend assigned.
    ///
    /// Returning `Committed` instead of a bare `Seq` means a caller that
    /// needs the write's own timestamp (e.g. to echo it back to a client, or
    /// to key a downstream cache entry) does not have to immediately
    /// `read(stream, seq, 1)` the event straight back just to learn `at`.
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
    ) -> Result<Committed, StoreError> {
        let lock = self.stream_lock(stream);
        let _guard = lock.lock().await;
        self.write_event_locked(stream, kind, patch, meta, WriteMode::Append)
            .await
    }

    /// Append `patch` iff the stream's current head matches `expected_head`.
    ///
    /// This is the optimistic-concurrency counterpart to [`Store::append`]
    /// for deployments where multiple writers can touch the same event log —
    /// e.g. a long-running server sharing an SQLite file with a CLI, or two
    /// processes coordinating through a common database. The backend runs
    /// the head check and the insert inside one transaction, so a second
    /// writer that races us will serialize behind the backend's file lock
    /// rather than sneak a stale write past the CAS.
    ///
    /// `expected_head` semantics:
    ///
    /// - [`Seq::ZERO`] means "expect the stream to currently be empty". The
    ///   internal state materialization skips the log entirely and hands
    ///   [`crate::state::empty_state`] to the gate.
    /// - A positive `expected_head` means "expect the head to be exactly
    ///   this seq". The internal state materialization runs
    ///   [`Store::state_at`] at `expected_head`; if that read fails because
    ///   `expected_head` is past the real head (or the stream is empty),
    ///   the error is remapped to [`StoreError::HeadConflict`] so callers
    ///   see one uniform failure mode.
    ///
    /// Returns [`StoreError::HeadConflict`] when the observed head does not
    /// match `expected_head` (either at the pre-flight state read above or
    /// at the atomic backend check). Returns
    /// [`StoreError::BackendUnsupported`] when the underlying
    /// [`EventBackend`] has not overridden
    /// [`EventBackend::append_if_head`] (the default implementation
    /// declines).
    ///
    /// Everything else — gate validation, cache write, sink dispatch — is
    /// identical to [`Store::append`].
    pub async fn append_if_head(
        &self,
        stream: &StreamId,
        kind: &str,
        patch: json_patch::Patch,
        meta: Value,
        expected_head: Seq,
    ) -> Result<Committed, StoreError> {
        let lock = self.stream_lock(stream);
        let _guard = lock.lock().await;
        self.write_event_locked(stream, kind, patch, meta, WriteMode::AppendIfHead(expected_head))
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
    ) -> Result<Committed, StoreError> {
        let lock = self.stream_lock(stream);
        let _guard = lock.lock().await;
        self.write_event_locked(stream, kind, patch, meta, WriteMode::Import(at))
            .await
    }

    /// Shared write path for [`Store::append`], [`Store::import_event`], and
    /// [`Store::append_if_head`].
    ///
    /// `mode` selects which backend method to invoke:
    ///
    /// - [`WriteMode::Append`] → [`EventBackend::append`] (backend stamps
    ///   "now"),
    /// - [`WriteMode::Import(at)`][WriteMode::Import] →
    ///   [`EventBackend::import_event`] (backend stamps the supplied
    ///   timestamp),
    /// - [`WriteMode::AppendIfHead(expected_head)`][WriteMode::AppendIfHead]
    ///   → [`EventBackend::append_if_head`] (backend runs the head check +
    ///   insert as one transaction; state materialization uses
    ///   `expected_head` as the caller's assumed current head, mapping
    ///   [`StoreError::SeqOutOfRange`] / [`StoreError::UnknownStream`] on
    ///   the state read to [`StoreError::HeadConflict`]).
    ///
    /// Every other step — gate validation, `next` materialization, cache
    /// write, sink dispatch — is identical across the three modes.
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
        mode: WriteMode,
    ) -> Result<Committed, StoreError> {
        let current = match mode {
            WriteMode::AppendIfHead(expected_head) if expected_head != Seq::ZERO => {
                // The caller's assumed current state is state_at(expected_head).
                // Any mismatch surfaces here as SeqOutOfRange (expected_head is
                // past the real head) or UnknownStream (expected_head > 0 on
                // an empty stream); both are the "head moved" case in disguise
                // — remap to HeadConflict so the caller sees one uniform error.
                match self.state_at(stream, expected_head).await {
                    Ok(v) => v,
                    Err(StoreError::SeqOutOfRange { head, .. }) => {
                        return Err(StoreError::HeadConflict {
                            expected: expected_head,
                            actual: head,
                        });
                    }
                    Err(StoreError::UnknownStream(_)) => {
                        return Err(StoreError::HeadConflict {
                            expected: expected_head,
                            actual: None,
                        });
                    }
                    Err(e) => return Err(e),
                }
            }
            WriteMode::AppendIfHead(_) => empty_state(),
            _ => self.state(stream).await?,
        };
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
        let committed = match mode {
            WriteMode::Append => self.events.append(stream, rec).await?,
            WriteMode::Import(at) => self.events.import_event(stream, rec, at).await?,
            WriteMode::AppendIfHead(expected_head) => {
                self.events.append_if_head(stream, rec, expected_head).await?
            }
        };
        let seq = committed.seq;

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
                // Opportunistic bounded pruning when the caller opted in via
                // `StoreConfig::cache_keep_latest`. The cache is derived
                // state (see `CacheBackend`); a failed prune here is
                // silently swallowed — the next cache-stride write retries
                // and `Store::prune_cache` remains available as a manual
                // entry point. This never fails the append itself.
                if let Some(keep) = self.config.cache_keep_latest {
                    let _ = self.cache.prune(stream, keep).await;
                }
            }

            // Post-commit sink dispatch (best-effort; failure leaves checkpoint alone).
            if !self.sinks.is_empty() {
                let events = self.events.read(stream, seq, 1).await?;
                if let Some(ev) = events.into_iter().next() {
                    for sink in &self.sinks {
                        if !sink.accepts(stream) {
                            continue;
                        }
                        let checkpoint = self.checkpoint_get(sink.id(), stream).await;
                        // Skip if already past this seq (catch_up ran concurrently).
                        if seq <= checkpoint {
                            continue;
                        }
                        match sink.commit(stream, seq, next_state, &ev).await {
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
                                // through the observer (if attached) so the caller
                                // can see the sink falling behind without waiting
                                // for a manual catch_up call.
                                self.notify_sink_failure(
                                    sink.id(),
                                    stream,
                                    Some(seq),
                                    SinkOp::Commit,
                                    &e,
                                );
                            }
                        }
                    }
                }
            }
        }

        Ok(committed)
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
    ///
    /// Returns [`StoreError::SeqCompacted`] when `at` falls strictly before
    /// this stream's compaction boundary (see [`SNAPSHOT_KIND`] and the
    /// "Compaction and history boundary" section in the module-level rustdoc):
    /// the events that would be needed to reconstruct state at `at` have been
    /// replaced by a snapshot, so the pre-boundary state is no longer
    /// materially reachable. `state_at(stream, boundary)` itself still works
    /// — the snapshot event materializes exactly that state — and any seq
    /// after the boundary replays forward from it as usual.
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

        if let Some(boundary) = self.events.compaction_boundary(stream).await? {
            if at < boundary {
                return Err(StoreError::SeqCompacted {
                    boundary,
                    requested: at,
                });
            }
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
        // Compaction leaves gaps in the seq sequence — the reader may
        // legitimately return fewer than `limit` events, or events with
        // seqs beyond `at` when the earliest event on the stream is a
        // snapshot at `from > 1`. Trim the replay set to `seq <= at` so
        // the reconstructed state matches the caller's requested coordinate
        // regardless of where the compaction boundary sits.
        let events: Vec<_> = events.into_iter().take_while(|e| e.seq <= at).collect();
        replay_from(base_state, &events)
    }

    /// Revert `stream` to the state at `to` by appending the reverse diff as a
    /// new event. The prior state stays in the log; recovery from mistakes is
    /// yet another revert.
    ///
    /// Equivalent to [`Store::revert_with_meta`] with `extra_meta =
    /// Value::Null` — the appended event's `meta` is exactly `{"revert_to":
    /// to}`. See [`Store::revert_with_meta`] if the caller needs to attach
    /// its own attribution (e.g. a consumer-defined id) to the revert event.
    ///
    /// Holds the per-stream write lock across both the `current`/`target`
    /// reads and the resulting append, so no concurrent write to `stream`
    /// can land between "diff computed against `current`" and "diff
    /// appended" — that gap would otherwise let the reverse patch apply on
    /// top of a `current` that is no longer the stream's real state.
    pub async fn revert(&self, stream: &StreamId, to: Seq) -> Result<Committed, StoreError> {
        self.revert_with_meta(stream, to, Value::Null).await
    }

    /// Revert `stream` to the state at `to`, like [`Store::revert`], but let
    /// the caller merge its own fields into the appended event's `meta`.
    ///
    /// Without this, a consumer whose `meta` schema carries its own
    /// attribution (e.g. a `node_id`, an actor, a correlation id) has no way
    /// to express that on a revert — `revert`'s generated `meta` is always
    /// the fixed shape `{"revert_to": to}`, with nowhere for consumer fields
    /// to go.
    ///
    /// ## Merge semantics
    ///
    /// If `extra_meta` is a JSON object, its keys are merged into the
    /// generated `{"revert_to": to}` meta. `"revert_to"` is a reserved key:
    /// the generated value always wins, so a same-named key in `extra_meta`
    /// is silently overwritten rather than causing an error — this mirrors
    /// how [`Store::append`]'s `meta` argument is opaque to the store (no
    /// key is otherwise reserved), keeping the one exception explicit here
    /// rather than failing the call.
    ///
    /// If `extra_meta` is anything other than a JSON object — including
    /// `Value::Null` — it is ignored entirely and the appended event's
    /// `meta` is exactly `{"revert_to": to}`, same as [`Store::revert`]. This
    /// method does not validate `extra_meta`'s shape beyond that one
    /// object/non-object branch; a non-object value is silently dropped, not
    /// rejected, on the reasoning that a revert should not fail solely
    /// because the caller's optional attribution was malformed.
    ///
    /// See [`Store::revert`]'s rustdoc for the write-lock and history
    /// guarantees this method also provides — the merge described above is
    /// the only difference between the two.
    pub async fn revert_with_meta(
        &self,
        stream: &StreamId,
        to: Seq,
        extra_meta: Value,
    ) -> Result<Committed, StoreError> {
        let lock = self.stream_lock(stream);
        let _guard = lock.lock().await;
        let current = self.state(stream).await?;
        let target = self.state_at(stream, to).await?;
        let patch = diff(&current, &target);

        let mut meta = match extra_meta {
            Value::Object(map) => map,
            _ => Map::new(),
        };
        meta.insert("revert_to".to_string(), Value::from(to.0));

        self.write_event_locked(stream, REVERT_KIND, patch, Value::Object(meta), WriteMode::Append)
            .await
    }

    /// Mark `stream` as deleted by appending a tombstone event of kind
    /// [`TOMBSTONE_KIND`].
    ///
    /// The tombstone carries an empty patch — the materialized state does not
    /// change — but the event itself flows through the same write path as
    /// [`Store::append`]: every registered [`SchemaGate`] validates it, the
    /// per-stream write lock serializes it, and every accepting
    /// [`ProjectionSink`] receives it. `meta` is opaque to the store (same
    /// contract as `append`); pass `Value::Null` (or `Value::Object(Map::new())`)
    /// when there is nothing to attribute.
    ///
    /// This is a *convention*, not a physical removal: history stays
    /// append-only, `state_at(stream, seq)` for any `seq` before the tombstone
    /// still reconstructs the pre-delete state, and a further non-tombstone
    /// [`Store::append`] "revives" the stream (see the "How deletion works"
    /// section in the module-level rustdoc). Consumers that want to reject
    /// writes after a tombstone should register a [`crate::KindGate`]
    /// validator; consumers that want to hide tombstoned streams from a
    /// listing should use [`Store::streams_live`] or the read-model's
    /// `live = 1` filter.
    pub async fn delete(&self, stream: &StreamId, meta: Value) -> Result<Committed, StoreError> {
        let empty_patch: json_patch::Patch = serde_json::from_value(Value::Array(Vec::new()))
            .expect("empty JSON array parses as an empty JSON Patch");
        let lock = self.stream_lock(stream);
        let _guard = lock.lock().await;
        self.write_event_locked(stream, TOMBSTONE_KIND, empty_patch, meta, WriteMode::Append)
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

    /// Prune the cache for `stream`, keeping only the `keep_latest` most
    /// recent snapshot rows.
    ///
    /// The cache is derived state — `Store::state_at` reconstructs any lost
    /// snapshot from the log by replaying forward from a still-cached
    /// nearest neighbor — so this operation is always safe: it trades a
    /// slightly-longer replay on `state_at` for a bounded cache footprint.
    /// The append-only history on [`crate::EventBackend`] is untouched.
    ///
    /// Callers who want pruning to happen automatically after every
    /// cache-stride write should set [`StoreConfig::cache_keep_latest`] at
    /// construction; those errors are silently ignored (the next write
    /// retries). This method is the explicit alternative: run it during
    /// maintenance windows, or when a long-running stream's cache has
    /// grown past what a consumer wants to keep on disk.
    pub async fn prune_cache(
        &self,
        stream: &StreamId,
        keep_latest: usize,
    ) -> Result<(), StoreError> {
        self.cache.prune(stream, keep_latest).await
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

    /// Enumerate streams whose most recent event is *not* a tombstone (see
    /// [`Store::delete`] / [`TOMBSTONE_KIND`]).
    ///
    /// Streams whose entire history was appended via non-tombstone kinds are
    /// included; streams that never received a [`Store::delete`] but did
    /// receive a subsequent non-tombstone [`Store::append`] after one are
    /// included too (the tombstone is not the *most recent* event any more).
    /// Streams with no events at all are excluded — [`Store::streams`] does not
    /// return them either.
    ///
    /// This is the "listing without a read model" path: it walks every stream
    /// and reads its head event, so cost is O(N) in stream count with one
    /// additional backend `read` per stream. When a [`crate::ProjectionSink`]
    /// like `SqliteReadModel` is already materializing a `live` flag, its
    /// indexed query is cheaper and preferable — this method is the fallback
    /// for callers that have no read model wired up (or want a live listing
    /// without depending on one).
    pub async fn streams_live(&self) -> Result<Vec<StreamId>, StoreError> {
        let all = self.events.streams().await?;
        let mut out = Vec::with_capacity(all.len());
        for stream in all {
            let Some(head) = self.events.head(&stream).await? else {
                continue;
            };
            let events = self.events.read(&stream, head, 1).await?;
            let is_tombstoned = events.first().is_some_and(|e| e.kind == TOMBSTONE_KIND);
            if !is_tombstoned {
                out.push(stream);
            }
        }
        Ok(out)
    }

    /// Pin `label` on `stream` to `at`.
    ///
    /// After the backend records the pin, every registered `ProjectionSink`
    /// that [`ProjectionSink::accepts`] `stream` receives an `on_label_set`
    /// notification carrying the freshly materialized state at `at` and the
    /// [`Event`] the label now points at. Sink failures are best-effort —
    /// they do not roll back the label change, matching the append dispatch
    /// policy.
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
        if !self.sinks.is_empty() {
            let state = self.state_at(stream, at).await?;
            let event = self
                .events
                .read(stream, at, 1)
                .await
                .ok()
                .and_then(|mut evs| (!evs.is_empty()).then(|| evs.remove(0)));
            if let Some(event) = event {
                for sink in &self.sinks {
                    if !sink.accepts(stream) {
                        continue;
                    }
                    if let Err(e) = sink.on_label_set(stream, label, at, &state, &event).await {
                        self.notify_sink_failure(sink.id(), stream, Some(at), SinkOp::LabelSet, &e);
                    }
                }
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
    /// [`ProjectionSink`] that [`ProjectionSink::accepts`] `stream` receives
    /// an `on_label_deleted` notification — but only when the label actually
    /// existed (a no-op delete dispatches nothing, since nothing changed).
    /// Sink failures are best-effort — matching the dispatch policy of
    /// [`Store::label_set`] and [`Store::append`], a sink error does not
    /// roll back the deletion.
    pub async fn label_delete(&self, stream: &StreamId, label: &Label) -> Result<bool, StoreError> {
        let existed = self.events.label_delete(stream, label).await?;
        if existed && !self.sinks.is_empty() {
            for sink in &self.sinks {
                if !sink.accepts(stream) {
                    continue;
                }
                if let Err(e) = sink.on_label_deleted(stream, label).await {
                    self.notify_sink_failure(sink.id(), stream, None, SinkOp::LabelDeleted, &e);
                }
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
            // A stream this sink does not accept is not this sink's concern
            // at all: no checkpoint reset, no advance, and no contribution
            // to `applied` / `skipped` / `failed` — counting it as
            // "skipped" would imply it was owed a dispatch it never was.
            if !sink.accepts(&stream) {
                continue;
            }
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
