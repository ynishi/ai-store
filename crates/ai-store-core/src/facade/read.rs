//! Read path: `state` / `state_at` / `read` / `read_by_meta` / `head` /
//! `seq_at_time` / `streams` / `streams_live`.
//!
//! `state_at` is the shared cache-nearest + replay reconstruction every
//! other read (and every write's pre-commit gate preview) is built on. See
//! [`super::Store`]'s module-level rustdoc for the compaction-boundary and
//! schema-evolution semantics these methods honor.

use serde_json::Value;

use crate::error::StoreError;
use crate::event::Event;
use crate::id::{Seq, StreamId, Timestamp};
use crate::state::{empty_state, replay_from};

use super::{Store, TOMBSTONE_KIND};

impl Store {
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
    /// this stream's compaction boundary (see [`super::SNAPSHOT_KIND`] and the
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
        // `self.events` already applies the registered upcaster chain (see
        // `crate::upcasting_backend::UpcastingBackend`) when one is
        // registered, so this is already upcasted-shape events.
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

    /// Enumerate events. See `EventBackend::read`.
    ///
    /// Every returned [`Event`] is passed through the registered
    /// [`crate::Upcaster`] chain before it is handed back to the caller (see
    /// the "Schema evolution" section in the module-level rustdoc). When no
    /// upcasters are registered, this is a straight pass-through of the
    /// backend's response. The chain application itself happens inside
    /// `self.events` — see `UpcastingBackend`.
    pub async fn read(
        &self,
        stream: &StreamId,
        from: Seq,
        limit: usize,
    ) -> Result<Vec<Event>, StoreError> {
        self.events.read(stream, from, limit).await
    }

    /// Enumerate events whose top-level `meta[field]` equals `value`. See
    /// [`crate::EventBackend::read_by_meta`].
    ///
    /// The `meta[field] == value` predicate is evaluated against the
    /// *stored* event (backends with a native `json_extract`-based
    /// implementation — the SQLite backend here, for one — filter inside
    /// the backend before any upcaster gets a chance). Once matching
    /// events have been selected, they are then run through the
    /// [`crate::Upcaster`] chain the same as [`Store::read`]. Consumers that
    /// mix schema evolution with `read_by_meta` should keep the meta fields
    /// they filter on stable across shape changes — see the
    /// `read_by_meta` caveat in [`crate::Upcaster`]'s module rustdoc.
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
}
