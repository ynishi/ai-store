//! Write path: `append` / `append_if_head` / `import_event` /
//! `write_event_locked` / `revert` / `revert_with_meta` / `delete` /
//! `prune_cache`.
//!
//! `write_event_locked` is the shared critical section every public write
//! method funnels through (see [`super::Store`]'s module-level rustdoc for
//! the full write pipeline). Everything here assumes the caller already
//! holds the per-stream write lock ([`super::Store::stream_lock`], defined
//! on the parent module) for the duration of the call.

use json_patch::diff;
use serde_json::{Map, Value};

use crate::error::StoreError;
use crate::event::{Committed, Event, NewEvent};
use crate::gate::GateCtx;
use crate::id::{Seq, StreamId, Timestamp};
use crate::state::empty_state;

use super::{Store, WriteMode, REVERT_KIND, TOMBSTONE_KIND};

impl Store {
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
    /// Fast path: when no [`crate::SchemaGate`] is registered, `next` is not
    /// materialized pre-commit. If the assigned `seq` misses the cache stride
    /// and no [`crate::ProjectionSink`] is registered, `next` is not materialized at
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
    /// [`crate::EventBackend`] has not overridden
    /// [`crate::EventBackend::append_if_head`] (the default implementation
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
        self.write_event_locked(
            stream,
            kind,
            patch,
            meta,
            WriteMode::AppendIfHead(expected_head),
        )
        .await
    }

    /// Import one event into `stream`, recording `at` as its time coordinate
    /// instead of the wall-clock time of this call.
    ///
    /// Identical to [`Store::append`] in every other respect — the same
    /// gates validate the write, the same sinks are dispatched, the same
    /// cache-stride rule decides whether `next` is materialized. The only
    /// difference is which [`crate::EventBackend`] method is invoked: `append`
    /// delegates to [`crate::EventBackend::append`] (backend stamps "now"),
    /// `import_event` delegates to [`crate::EventBackend::import_event`] (backend
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
    /// [`crate::EventBackend`] has not overridden [`crate::EventBackend::import_event`]
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
    /// - [`WriteMode::Append`] → [`crate::EventBackend::append`] (backend stamps
    ///   "now"),
    /// - [`WriteMode::Import(at)`][WriteMode::Import] →
    ///   [`crate::EventBackend::import_event`] (backend stamps the supplied
    ///   timestamp),
    /// - [`WriteMode::AppendIfHead(expected_head)`][WriteMode::AppendIfHead]
    ///   → [`crate::EventBackend::append_if_head`] (backend runs the head check +
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
                self.events
                    .append_if_head(stream, rec, expected_head)
                    .await?
            }
        };
        let seq = committed.seq;

        let cache_hit = self.config.cache_stride > 0 && seq.0 % self.config.cache_stride == 0;
        let has_upcasters = !self.upcasters.is_empty();
        let has_sinks = self.dispatcher.has_sinks().await;

        // What "next state" to hand downstream (cache put + sink dispatch)
        // has to match what `state_at` would reconstruct at `seq` — so
        // when upcasters are registered, materialize `next` from the
        // *upcasted* patch, not the raw one the backend stored. The raw
        // fast path is preserved when no upcasters are in play, to keep
        // the pre-upcaster performance profile intact.
        let downstream: Option<(Value, Option<Event>)> = if cache_hit || has_sinks {
            if has_upcasters {
                // `self.events` is wrapped in `UpcastingBackend` whenever
                // upcasters are registered (see `Store::new_inner_full`),
                // so this read already comes back upcasted — no second
                // pass through the chain is needed here. What *does* stay
                // in the facade is reconstructing `next` from the
                // (already-upcasted) event's patch: the decorator only
                // hands back events, it does not know how to fold a patch
                // onto the `current` state this call already
                // materialized, and `current` never went through a raw
                // backend read that the decorator could have intercepted.
                // Fetch the event once and reuse it for the sink dispatch
                // below.
                let raw = self.events.read(stream, seq, 1).await?;
                let ev = raw.into_iter().next().ok_or_else(|| {
                    StoreError::Backend(format!(
                        "post-commit read at seq={} returned nothing",
                        seq.0
                    ))
                })?;
                let mut next = current.clone();
                json_patch::patch(&mut next, &ev.patch)
                    .map_err(|e| StoreError::Patch(format!("post-commit upcasted reapply: {e}")))?;
                Some((next, Some(ev)))
            } else if let Some(precomputed) = precomputed_next {
                Some((precomputed, None))
            } else {
                let mut n = current.clone();
                json_patch::patch(&mut n, patch_for_reapply.as_ref().unwrap())
                    .map_err(|e| StoreError::Patch(format!("post-commit reapply: {e}")))?;
                Some((n, None))
            }
        } else {
            None
        };

        if let Some((ref next_state, ref preloaded_event)) = downstream {
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
            if has_sinks {
                // Reuse the event we already fetched (and, if `has_upcasters`,
                // already upcasted) above if that path took it. Otherwise
                // fetch here — `self.events` applies the upcaster chain
                // itself when one is registered, so no separate upcast
                // step is needed on this fallback fetch either.
                let events: Vec<Event> = if let Some(ev) = preloaded_event.clone() {
                    vec![ev]
                } else {
                    self.events.read(stream, seq, 1).await?
                };
                if let Some(ev) = events.into_iter().next() {
                    self.dispatcher
                        .dispatch_commit(stream, seq, next_state, &ev)
                        .await;
                }
            }
        }

        Ok(committed)
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

        self.write_event_locked(
            stream,
            REVERT_KIND,
            patch,
            Value::Object(meta),
            WriteMode::Append,
        )
        .await
    }

    /// Mark `stream` as deleted by appending a tombstone event of kind
    /// [`TOMBSTONE_KIND`].
    ///
    /// The tombstone carries an empty patch — the materialized state does not
    /// change — but the event itself flows through the same write path as
    /// [`Store::append`]: every registered [`crate::SchemaGate`] validates it, the
    /// per-stream write lock serializes it, and every accepting
    /// [`crate::ProjectionSink`] receives it. `meta` is opaque to the store (same
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
    /// cache-stride write should set [`crate::StoreConfig::cache_keep_latest`] at
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
}
