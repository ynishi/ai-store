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
//!
//! ## Schema evolution
//!
//! Long-lived streams outlast the JSON shape their patches were originally
//! written against. Without a translation hook, [`Store::state_at`] would
//! reconstruct a mixed old/new document (the old patches target old paths,
//! newer patches target new paths, and the two layers coexist on the same
//! `Value` tree) — and gates, sinks, and read-model field extraction would
//! see that mixed shape. The append-only enforcement on the backend (see
//! `ai-store-sqlite`'s migration 4) makes in-place event rewriting
//! impossible; the only sanctioned answer is a read-time projection.
//!
//! [`crate::Upcaster`] is that projection. It is a consumer-supplied
//! function `Event -> Event` registered on the [`StoreBuilder`], applied
//! by every read path in this facade — public [`Store::read`] /
//! [`Store::read_by_meta`], the internal reads inside [`Store::state_at`]
//! / [`Store::catch_up`] / [`Store::materialize_to_sink`], and the
//! post-`append` / post-`label_set` sink dispatch. The stored bytes on
//! the backend are untouched; the log stays byte-identical and the
//! append-only invariant is preserved.
//!
//! ### Chain semantics
//!
//! Multiple upcasters compose in registration order — one upcaster per
//! schema-version transition (`v1 → v2`, `v2 → v3`, …), each dispatching
//! internally on `event.meta[SCHEMA_VERSION_META_KEY]`. A `v1` event
//! walks the entire chain and arrives at `v3`; a `v3` event flows through
//! each step unchanged when the dispatch decides not to touch it. The
//! chain is walked once per event per read; there is no per-store cache.
//!
//! ### `read_by_meta` caveat
//!
//! [`Store::read_by_meta`] evaluates its `meta[field] == value` predicate
//! against the *stored* event, not the upcasted one — backends with a
//! native `json_extract`-based implementation filter inside the backend
//! before any upcaster sees the row. Consumers that mix schema evolution
//! with `read_by_meta` should keep the meta fields they filter on stable
//! across shape changes (add a compatible synonym first, then rename
//! after all readers understand the new key). Every other read path is
//! covered by the chain.
//!
//! ### Recommended workflow
//!
//! 1. Prefer additive changes — a new field with a default value in the
//!    reducer, or a new event kind that older readers ignore — over
//!    shape rewrites. Additive changes never need an upcaster.
//! 2. When a shape rewrite is unavoidable, ship the upcaster for the
//!    old-to-new transition *before* the first write of the new shape,
//!    and stamp new writes with the target
//!    [`crate::SCHEMA_VERSION_META_KEY`] value. Old readers still see
//!    what they expect (they don't have the upcaster); new readers
//!    upgrade old-shape events to the current shape on the fly.
//! 3. When an old shape is truly obsolete and the consumer no longer
//!    ships the upcaster for it, retire it permanently by compacting
//!    (see the "Compaction and history boundary" section) — the
//!    snapshot event captures the current-shape state and drops the
//!    old-shape prefix from the log entirely.
//!
//! ## Module layout
//!
//! This module is split by responsibility, all as inherent `impl Store`
//! blocks over the one [`Store`] type defined here:
//!
//! - [`write`] — `append` / `append_if_head` / `import_event` /
//!   `write_event_locked` / `revert` / `revert_with_meta` / `delete` /
//!   `prune_cache`.
//! - [`read`] — `state` / `state_at` / `read` / `read_by_meta` / `head` /
//!   `seq_at_time` / `streams` / `streams_live`.
//! - [`labels`] — `label_set` / `label_resolve` / `labels` / `label_delete`.
//! - [`lifecycle`] — `attach_sink` / `materialize_to_sink` / `catch_up` /
//!   `rebuild` / `catch_up_inner`.
//!
//! This file keeps the type definitions (`Store`, `StoreConfig`,
//! `WriteMode`), the public constructors, and the private helper shared
//! across every submodule (`stream_lock`). Sink registry, checkpoint
//! bookkeeping, and failure-observer dispatch live on the separate
//! [`crate::dispatcher::SinkDispatcher`] collaborator held in the
//! `dispatcher` field — see its module-level rustdoc for the
//! responsibility split. Upcaster chain application likewise lives off
//! this file, on the separate [`crate::upcasting_backend::UpcastingBackend`]
//! decorator that `events` is wrapped in whenever a chain is registered
//! (see [`Store::new_inner_full`]) — every submodule that reads through
//! `self.events` gets already-upcasted events without calling anything
//! upcaster-specific itself.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use tokio::sync::Mutex;

use crate::backend::{CacheBackend, CheckpointBackend, EventBackend};
use crate::builder::StoreBuilder;
use crate::dispatcher::SinkDispatcher;
use crate::gate::SchemaGate;
use crate::id::{Seq, StreamId, Timestamp};
use crate::sink::{ProjectionSink, SinkFailureObserver};
use crate::upcaster::Upcaster;
use crate::upcasting_backend::UpcastingBackend;

mod labels;
mod lifecycle;
mod read;
mod write;

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
/// rustdoc, and [`crate::StoreError::SeqCompacted`] for the boundary error
/// returned from [`Store::state_at`] / [`Store::revert`] on pre-boundary
/// seqs.
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
    /// Sink registry, checkpoint bookkeeping, and failure-observer
    /// dispatch. See [`crate::dispatcher::SinkDispatcher`] for the full
    /// responsibility split between it and this facade.
    dispatcher: Arc<SinkDispatcher>,
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
    /// stream ever seen. This mirrors the dispatcher's checkpoint map (same
    /// trade-off, same justification: the alternative is a correctness bug).
    stream_locks: Arc<StdMutex<HashMap<StreamId, Arc<Mutex<()>>>>>,
    /// Read-time event upcasters — see [`crate::Upcaster`]. The chain
    /// itself is applied by [`crate::upcasting_backend::UpcastingBackend`],
    /// which `events` is wrapped in (see [`Store::new_inner_full`])
    /// whenever this vec is non-empty; `events` points straight at the
    /// real backend otherwise. This copy is retained so the write path
    /// (`write_event_locked`) can tell whether `events` is wrapped
    /// without downcasting, and so it can reconstruct the post-commit
    /// `next` state from the (already-upcasted) committed event's patch.
    upcasters: Vec<Arc<dyn Upcaster>>,
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
        Self::new_inner_with_observer(
            events,
            cache,
            gates,
            sinks,
            config,
            checkpoint_backend,
            None,
        )
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
        Self::new_inner_full(
            events,
            cache,
            gates,
            sinks,
            config,
            checkpoint_backend,
            sink_failure_observer,
            Vec::new(),
        )
    }

    /// Inner constructor that accepts every optional slot, including the
    /// upcaster chain. Kept crate-private; the builder is the one exposed
    /// path.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new_inner_full(
        events: Arc<dyn EventBackend>,
        cache: Arc<dyn CacheBackend>,
        gates: Vec<Arc<dyn SchemaGate>>,
        sinks: Vec<Arc<dyn ProjectionSink>>,
        config: StoreConfig,
        checkpoint_backend: Option<Arc<dyn CheckpointBackend>>,
        sink_failure_observer: Option<Arc<dyn SinkFailureObserver>>,
        upcasters: Vec<Arc<dyn Upcaster>>,
    ) -> Self {
        // Wrap `events` in `UpcastingBackend` iff there is a chain to
        // apply — this makes every facade read that goes through
        // `self.events` (state_at / read / read_by_meta / streams_live /
        // label_set / materialize_to_sink / catch_up_inner) upcast for
        // free, with no per-call-site upcast call needed. `upcasters`
        // itself is retained on `Store` below (unwrapped) so
        // `write_event_locked` can still branch on "is there a chain at
        // all" and reconstruct its post-commit `next` state — see that
        // field's doc comment.
        let events: Arc<dyn EventBackend> = if upcasters.is_empty() {
            events
        } else {
            Arc::new(UpcastingBackend::new(events, upcasters.clone()))
        };
        Self {
            events,
            cache,
            gates,
            dispatcher: Arc::new(SinkDispatcher::new(
                sinks,
                checkpoint_backend,
                sink_failure_observer,
            )),
            stream_locks: Arc::new(StdMutex::new(HashMap::new())),
            upcasters,
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
}
