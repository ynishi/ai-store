//! Backend SPI traits — implemented by storage adapters, consumed by the facade.
//!
//! `EventBackend` owns the append-only log and the mutable label index. It
//! deliberately exposes no delete/overwrite methods on events; immutability of
//! the log is guaranteed by the absence of those methods rather than by runtime
//! checks.
//!
//! `CacheBackend` is a materialization cache for state snapshots. It is a
//! derived artifact — pruning entries never violates the log's SoT property.
//!
//! `CheckpointBackend` persists `ProjectionSink` checkpoints so they survive
//! process restarts. It is optional — [`crate::Store::new`] runs without one
//! (checkpoints live only in process memory), and [`crate::Store::with_checkpoint_backend`]
//! attaches one.
//!
//! `SqliteBackend` is a separate, narrower SPI: a generic constructor
//! pattern for backends built from an existing native handle, so downstream
//! crates can write handle-agnostic backend factories.

use async_trait::async_trait;
use serde_json::Value;

use crate::error::StoreError;
use crate::event::{Committed, Event, NewEvent};
use crate::id::{Label, Seq, StreamId, Timestamp};

/// Append-only event log backend.
///
/// Implementations guarantee gap-free monotonic `Seq` assignment within each
/// stream and atomicity of `append` (backend-native transaction). No method on
/// this trait deletes or rewrites existing events.
#[async_trait]
pub trait EventBackend: Send + Sync {
    /// Append one event. The backend assigns `seq` and `timestamp`.
    ///
    /// Returns the assigned [`Committed`] (the backend-assigned `seq` and the
    /// `at` it stamped) on success, so callers do not need a follow-up
    /// [`EventBackend::read`] just to learn the write's own timestamp.
    async fn append(&self, stream: &StreamId, rec: NewEvent) -> Result<Committed, StoreError>;

    /// Import one event, stamping it with a caller-supplied historical
    /// `Timestamp` instead of the wall-clock time of the call.
    ///
    /// This is the import/migration counterpart to [`EventBackend::append`]:
    /// the backend still assigns the next gap-free monotonic `Seq` exactly as
    /// `append` would, but records `at` as the event's time coordinate rather
    /// than stamping "now". See [`crate::Store::import_event`] for the full
    /// contract, including the caveat about non-monotonic `at` and
    /// [`crate::Store::seq_at_time`].
    ///
    /// The default implementation returns
    /// [`StoreError::BackendUnsupported`] so that existing external
    /// `EventBackend` implementations keep compiling — and keep behaving
    /// exactly as before this method was added — without being forced to
    /// fake support for historical timestamps they cannot actually honor.
    /// Backends that can persist an arbitrary `at` (both backends shipped in
    /// this workspace do) should override it.
    async fn import_event(
        &self,
        stream: &StreamId,
        rec: NewEvent,
        at: Timestamp,
    ) -> Result<Committed, StoreError> {
        let _ = (stream, rec, at);
        Err(StoreError::BackendUnsupported("import_event".to_string()))
    }

    /// Append `rec` iff the stream's current head matches `expected_head`,
    /// as a single atomic backend transaction.
    ///
    /// This is the optimistic-concurrency counterpart to
    /// [`EventBackend::append`]: instead of assigning the next gap-free
    /// monotonic `Seq` unconditionally, the backend reads the head *inside*
    /// its append transaction and rejects the write with
    /// [`StoreError::HeadConflict`] if the observed head is not
    /// `expected_head`. Because the head read and the insert share one
    /// transaction, this is safe across processes — a second writer that
    /// interleaves between the read and the insert serializes behind SQLite's
    /// (or the equivalent backend's) file lock rather than sneaking a stale
    /// write past the CAS.
    ///
    /// `expected_head` uses [`Seq::ZERO`] to mean "expect the stream to
    /// currently be empty". Non-zero `expected_head` means "expect the head
    /// to be exactly this seq". The `Some(actual)` in the returned
    /// `HeadConflict` reports what the backend observed instead (or `None`
    /// when the observed stream was empty).
    ///
    /// The default implementation returns
    /// [`StoreError::BackendUnsupported`] so backends that do not (and
    /// cannot cheaply) support atomic head reads keep compiling. The
    /// SQLite backend shipped in this workspace overrides it.
    async fn append_if_head(
        &self,
        stream: &StreamId,
        rec: NewEvent,
        expected_head: Seq,
    ) -> Result<Committed, StoreError> {
        let _ = (stream, rec, expected_head);
        Err(StoreError::BackendUnsupported(
            "append_if_head".to_string(),
        ))
    }

    /// Read events in `[from, from + limit)` order.
    ///
    /// If `from` is greater than the head, returns an empty vector.
    async fn read(
        &self,
        stream: &StreamId,
        from: Seq,
        limit: usize,
    ) -> Result<Vec<Event>, StoreError>;

    /// Read events whose top-level `meta[field]` equals `value`, scanning
    /// forward from `from` and returning at most `limit` matches.
    ///
    /// `field` is a single top-level key of the event's `meta` object; nested
    /// paths are out of scope for this method. Callers that need deeper
    /// matching should post-filter after [`EventBackend::read`].
    ///
    /// The default implementation pages through [`EventBackend::read`] and
    /// filters client-side — it is O(N) in the number of events scanned.
    /// Backends that can index `meta` (e.g. SQLite via `json_extract`) should
    /// override this method for sub-linear lookups.
    async fn read_by_meta(
        &self,
        stream: &StreamId,
        field: &str,
        value: &Value,
        from: Seq,
        limit: usize,
    ) -> Result<Vec<Event>, StoreError> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        const BATCH: usize = 128;
        let mut out = Vec::new();
        let mut cursor = from;
        loop {
            let events = self.read(stream, cursor, BATCH).await?;
            if events.is_empty() {
                break;
            }
            let last_seq = events.last().unwrap().seq;
            for ev in events {
                if ev.meta.get(field) == Some(value) {
                    out.push(ev);
                    if out.len() >= limit {
                        return Ok(out);
                    }
                }
            }
            cursor = last_seq.next();
        }
        Ok(out)
    }

    /// Current head coordinate. `None` if the stream has no events.
    async fn head(&self, stream: &StreamId) -> Result<Option<Seq>, StoreError>;

    /// Greatest seq whose event timestamp is `<= at`.
    ///
    /// Returns `None` if the stream is empty or every event was appended after
    /// `at`. Backends should implement this with a timestamp index rather than
    /// a linear scan when possible; the in-memory backend scans because a scan
    /// is already O(events) for other reads.
    async fn seq_at_time(
        &self,
        stream: &StreamId,
        at: Timestamp,
    ) -> Result<Option<Seq>, StoreError>;

    /// Enumerate all known streams.
    async fn streams(&self) -> Result<Vec<StreamId>, StoreError>;

    /// Earliest seq still materially reachable in `stream` after a compaction
    /// maintenance operation has replaced its prefix with a snapshot event
    /// (kind [`crate::SNAPSHOT_KIND`]).
    ///
    /// Returns `None` for streams that have never been compacted — every
    /// event since `Seq(1)` is still on the log, and state at any prior seq
    /// remains materially reachable via cache-nearest + replay. Backends that
    /// support compaction return `Some(boundary)` where `boundary` is the seq
    /// of the snapshot event: [`crate::Store::state_at`] rejects requests for
    /// any seq strictly below it with [`crate::StoreError::SeqCompacted`].
    ///
    /// The default implementation returns `Ok(None)` so backends that do not
    /// (and will not) support compaction — such as the in-memory backend —
    /// continue to compile and behave exactly as before this method was
    /// added. Backends that ship a maintenance API (e.g.
    /// `ai_store_sqlite::SqliteMaintenance`) override it with a real lookup.
    async fn compaction_boundary(
        &self,
        stream: &StreamId,
    ) -> Result<Option<Seq>, StoreError> {
        let _ = stream;
        Ok(None)
    }

    /// Set (or overwrite) a label to point at a specific seq on this stream.
    async fn label_set(&self, stream: &StreamId, label: &Label, at: Seq) -> Result<(), StoreError>;

    /// Resolve a label to its current target seq, if defined.
    async fn label_resolve(
        &self,
        stream: &StreamId,
        label: &Label,
    ) -> Result<Option<Seq>, StoreError>;

    /// Enumerate all labels on a stream.
    async fn labels(&self, stream: &StreamId) -> Result<Vec<(Label, Seq)>, StoreError>;

    /// Remove a label from a stream, if it exists.
    ///
    /// Returns `true` when the label was present and has been removed, `false`
    /// when it was not defined. This mirrors `label_resolve`'s `Option` shape
    /// for "not found" rather than a bespoke error variant — the facade
    /// (`Store::label_delete`) surfaces this `bool` verbatim, since deleting
    /// an absent label is a no-op rather than a failure.
    ///
    /// Only the mutable label index is affected; the append-only event log
    /// this label pointed into is untouched.
    async fn label_delete(&self, stream: &StreamId, label: &Label) -> Result<bool, StoreError>;
}

/// Materialization cache for reconstructed stream states.
///
/// Values stored here are derived from the event log and can be regenerated at
/// any time. `prune` is safe to call at any point — evicting cache entries
/// never invalidates the log's ability to reconstruct state.
#[async_trait]
pub trait CacheBackend: Send + Sync {
    /// Store a materialized state for a stream at a given seq.
    async fn put(&self, stream: &StreamId, at: Seq, state: &Value) -> Result<(), StoreError>;

    /// Find the cached state closest to `at` without exceeding it.
    ///
    /// Returns `None` if no cache entry exists at or before `at`.
    async fn nearest(&self, stream: &StreamId, at: Seq)
        -> Result<Option<(Seq, Value)>, StoreError>;

    /// Prune cached entries for a stream, keeping the `keep_latest` most recent.
    async fn prune(&self, stream: &StreamId, keep_latest: usize) -> Result<(), StoreError>;
}

/// Persistence for `ProjectionSink` checkpoints (the `(sink_id, stream) ->
/// Seq` watermark the facade uses to know how far a sink has been driven).
///
/// This is an optional SPI: [`crate::Store::new`] runs with checkpoints held
/// only in process memory, which is safe (sinks are contracted to be
/// idempotent under redelivery) but means every sink replays its entire
/// history after a restart. [`crate::Store::with_checkpoint_backend`]
/// attaches an implementation of this trait so checkpoints survive restarts
/// instead.
///
/// Implementations do not need to be transactional with the event log —
/// the facade only ever calls [`CheckpointBackend::put`] *after* the
/// corresponding event is already durable, and a checkpoint that lags
/// slightly behind (or is momentarily unreadable) only costs a redundant
/// redelivery on the next `catch_up`, never a lost event.
#[async_trait]
pub trait CheckpointBackend: Send + Sync {
    /// Fetch the persisted checkpoint for `(sink_id, stream)`.
    ///
    /// `None` means no checkpoint has ever been recorded for this pair,
    /// equivalent to `Seq::ZERO` (drive the sink from the very start of the
    /// stream).
    async fn get(&self, sink_id: &str, stream: &StreamId) -> Result<Option<Seq>, StoreError>;

    /// Persist the checkpoint for `(sink_id, stream)` as `at`.
    ///
    /// Overwrites any previously persisted value. Implementations should
    /// treat this as an upsert keyed on `(sink_id, stream)`.
    async fn put(&self, sink_id: &str, stream: &StreamId, at: Seq) -> Result<(), StoreError>;
}

/// SPI trait for backends built from an existing native storage handle
/// (e.g. a connection, pool, or DB-specific async handle) rather than one
/// that owns connection setup itself.
///
/// `ai-store-sqlite`'s `SqliteEventBackend` and `SqliteCacheBackend` both
/// follow this shape today: each wraps a `rusqlite-isle` `AsyncIsle` handed
/// in by the caller via an inherent `new(isle: AsyncIsle) -> Self`. This
/// trait generalizes that constructor pattern so downstream crates can
/// write code generic over "any backend built this way" without depending
/// on `ai-store-sqlite` (or any other concrete backend crate) directly —
/// the associated `Handle` type keeps `ai-store-core` free of any
/// infrastructure dependency.
///
/// Implementing this trait is additive: existing inherent `new` methods on
/// concrete backend types are unaffected and remain the primary
/// construction path for direct callers.
pub trait SqliteBackend: Sized {
    /// The native storage handle type this backend wraps.
    type Handle;

    /// Build a backend instance from an existing handle.
    fn new(handle: Self::Handle) -> Self;
}
