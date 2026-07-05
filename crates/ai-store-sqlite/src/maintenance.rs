//! Off-hot-path maintenance operations (compaction, retention). All entry
//! points here temporarily reach *around* the storage-layer append-only
//! guarantee that migration 4 installs — the append-only triggers live on
//! the `events` table specifically so unmanaged `UPDATE` / `DELETE` traffic
//! against event history is impossible from any connection, and the only
//! sanctioned way to relax that (log compaction) is what this module
//! implements.
//!
//! ## `compact_stream` in one paragraph
//!
//! [`SqliteMaintenance::compact_stream`] atomically replaces a stream's
//! prefix `[Seq(1) .. up_to_seq]` with one snapshot event of kind
//! [`ai_store_core::SNAPSHOT_KIND`] at `seq = up_to_seq`, whose patch is an
//! `add "/"` that materializes the pre-compaction state. The whole
//! operation runs inside a single SQLite transaction: the append-only
//! triggers are dropped, the prefix is deleted, the snapshot is inserted,
//! stale cache rows below the new boundary are pruned, and the triggers are
//! re-created — in that order. If anything in the transaction fails the
//! whole thing rolls back, including the trigger drop, so no other
//! connection ever observes an intermediate state where a raw `DELETE` on
//! `events` would be accepted. See the "Compaction and history boundary"
//! section in `ai_store_core`'s facade rustdoc for the shape of the
//! contract this operation upholds.

use ai_store_core::{Event, Seq, Store, StoreError, StreamId, SNAPSHOT_KIND};
use rusqlite::params;
use rusqlite_isle::AsyncIsle;
use serde_json::{json, Value};

use crate::backend::{from_isle_err, to_store_err};

/// Outcome of a successful [`SqliteMaintenance::compact_stream`] call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionReport {
    /// The stream that was compacted.
    pub stream: StreamId,
    /// The seq of the snapshot event written by this call — the new earliest
    /// materially reachable coordinate on the stream.
    pub boundary: Seq,
    /// The stream's head coordinate at the time of the call. Compaction does
    /// not touch events after `boundary`, so this is unchanged in the log
    /// after the operation returns; it is reported here so callers can
    /// bookkeep "range compacted" without a follow-up
    /// [`Store::head`][ai_store_core::Store::head] round-trip.
    pub head_at_compaction: Seq,
}

/// Maintenance handle for a SQLite-backed store.
///
/// Cloneable; every clone shares the same SQLite thread as the
/// [`crate::SqliteBackends`] bundle it was built from — [`SqliteMaintenance`]
/// runs its maintenance transaction on the same writer thread as ordinary
/// [`ai_store_core::Store::append`] traffic, so an append issued concurrently
/// with a maintenance call is serialized by construction (no separate lock
/// layer needed).
#[derive(Clone)]
pub struct SqliteMaintenance {
    isle: AsyncIsle,
}

impl SqliteMaintenance {
    /// Build from an existing rusqlite-isle handle. The typical entry point
    /// is `SqliteBackends::isle()`.
    pub fn new(isle: AsyncIsle) -> Self {
        Self { isle }
    }

    /// Compact `stream` up to (and including) `up_to_seq`: atomically
    /// replace every event at seq `<= up_to_seq` with a single snapshot
    /// event of kind [`SNAPSHOT_KIND`] at `up_to_seq`, whose patch
    /// materializes the pre-compaction state at that coordinate.
    ///
    /// After the call, [`Store::state_at`] returns [`StoreError::SeqCompacted`]
    /// for any `seq < up_to_seq`; `state_at(up_to_seq)` returns the snapshot
    /// state directly; every later seq replays forward from the snapshot.
    /// [`Store::head`] and events strictly after `up_to_seq` are untouched.
    ///
    /// ### Requirements on inputs
    ///
    /// - `up_to_seq` must satisfy `Seq(1) <= up_to_seq <= head(stream)`.
    /// - The stream must not have already been compacted past `up_to_seq`
    ///   — the internal `state_at` call would surface
    ///   [`StoreError::SeqCompacted`] and this method returns that error
    ///   verbatim.
    /// - `up_to_seq == head(stream)` is permitted: the resulting log is a
    ///   single snapshot event and any subsequent [`Store::append`] takes
    ///   it from there.
    ///
    /// ### Crash-safety and trigger recreation
    ///
    /// The trigger drop, prefix delete, snapshot insert, cache prune, and
    /// trigger recreate are all executed inside one SQLite transaction. If
    /// any step raises a rusqlite error the transaction is rolled back —
    /// including the trigger drop — before this method returns, so
    /// concurrent connections continue to see the append-only invariant
    /// enforced. A process crash mid-transaction leaves the database in
    /// its pre-transaction state (SQLite's atomic commit).
    ///
    /// ### Ordering with concurrent writers
    ///
    /// `store` is used to reconstruct the snapshot state via
    /// [`Store::state_at`] and to look up the anchor event's original
    /// `at_ms` (preserved verbatim in the snapshot). Both reads run before
    /// the maintenance transaction opens. A concurrent
    /// [`Store::append`] that lands *after* those reads but *before* the
    /// transaction is fine — the new event lives at `seq > head_at_compaction`
    /// and is left untouched by the prefix delete. A concurrent append that
    /// interleaves *during* the transaction serializes behind it on the
    /// shared writer thread.
    pub async fn compact_stream(
        &self,
        store: &Store,
        stream: &StreamId,
        up_to_seq: Seq,
    ) -> Result<CompactionReport, StoreError> {
        // ---- 1. Validate up_to_seq bounds ---------------------------------
        if up_to_seq == Seq::ZERO {
            return Err(StoreError::Backend(
                "compact_stream: up_to_seq must be > 0".to_string(),
            ));
        }
        let head = store
            .head(stream)
            .await?
            .ok_or_else(|| StoreError::UnknownStream(stream.clone()))?;
        if up_to_seq > head {
            return Err(StoreError::SeqOutOfRange {
                head: Some(head),
                requested: up_to_seq,
            });
        }

        // ---- 2. Materialize the snapshot state and anchor timestamp -------
        //
        // `state_at` here also enforces the compaction boundary: if the
        // stream is already compacted past `up_to_seq`, it returns
        // `SeqCompacted` and we propagate that as-is.
        let snapshot_state: Value = store.state_at(stream, up_to_seq).await?;
        let anchor: Vec<Event> = store.read(stream, up_to_seq, 1).await?;
        let anchor_at_ms = anchor
            .first()
            .ok_or_else(|| {
                StoreError::Backend(format!(
                    "compact_stream: expected event at seq={} on stream {:?}",
                    up_to_seq.0,
                    stream.as_str(),
                ))
            })?
            .at
            .0;

        // ---- 3. Serialize snapshot patch + meta once, outside the tx -----
        let snapshot_patch_json = serde_json::to_string(&json!([
            { "op": "add", "path": "", "value": snapshot_state }
        ]))
        .map_err(to_store_err)?;
        let snapshot_meta_json =
            serde_json::to_string(&json!({ "compacted_at_seq": up_to_seq.0 }))
                .map_err(to_store_err)?;

        let stream_name = stream.as_str().to_string();
        let up_seq_i = up_to_seq.0 as i64;

        // ---- 4. Execute the maintenance transaction ----------------------
        self.isle
            .call(move |conn| {
                let tx = conn.transaction()?;

                // Drop the append-only triggers within the tx. SQLite DDL
                // participates in transactions, so a rollback below restores
                // them before any other connection can observe their absence.
                tx.execute_batch(
                    "DROP TRIGGER IF EXISTS trg_events_no_update; \
                     DROP TRIGGER IF EXISTS trg_events_no_delete;",
                )?;

                // Delete the compacted prefix (events 1..=up_to_seq, including
                // the anchor row we're about to replace with the snapshot).
                tx.execute(
                    "DELETE FROM events WHERE stream = ?1 AND seq <= ?2",
                    params![stream_name, up_seq_i],
                )?;

                // Insert the snapshot event AT seq = up_to_seq. Reusing the
                // exact coordinate the anchor row occupied means downstream
                // seq_at_time / label targets that pointed at up_to_seq still
                // resolve to a real row on the log, just one that now
                // materializes the pre-compaction state directly.
                tx.execute(
                    "INSERT INTO events (stream, seq, kind, patch, meta, at_ms) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                    params![
                        stream_name,
                        up_seq_i,
                        SNAPSHOT_KIND,
                        snapshot_patch_json,
                        snapshot_meta_json,
                        anchor_at_ms,
                    ],
                )?;

                // Prune cache rows that referred to pre-boundary states. Those
                // states are no longer materially reachable via `state_at`, so
                // keeping the rows would only waste space. Cache entries at
                // `at_seq >= up_to_seq` are still valid (their state is either
                // the snapshot state or something replayable forward from it)
                // and are left in place.
                tx.execute(
                    "DELETE FROM cache WHERE stream = ?1 AND at_seq < ?2",
                    params![stream_name, up_seq_i],
                )?;

                // Recreate the append-only triggers. Must exactly match the
                // migration-installed shape (see `crate::migration::MIGRATIONS`
                // entry 4) so a follow-up `migration::apply` finds them and
                // stays a no-op.
                tx.execute_batch(
                    "CREATE TRIGGER trg_events_no_update \
                     BEFORE UPDATE ON events \
                     BEGIN \
                         SELECT RAISE(ABORT, 'ai-store events are append-only (UPDATE denied)'); \
                     END; \
                     CREATE TRIGGER trg_events_no_delete \
                     BEFORE DELETE ON events \
                     BEGIN \
                         SELECT RAISE(ABORT, 'ai-store events are append-only (DELETE denied)'); \
                     END;",
                )?;

                tx.commit()?;
                Ok(())
            })
            .await
            .map_err(from_isle_err)?;

        Ok(CompactionReport {
            stream: stream.clone(),
            boundary: up_to_seq,
            head_at_compaction: head,
        })
    }
}

/// Metadata attached to the snapshot event a successful
/// [`SqliteMaintenance::compact_stream`] leaves on the log — surfaced here
/// so downstream consumers (e.g. a projection sink that wants to know it
/// received a compaction-produced event) can pattern-match on the shape
/// without duplicating the key string.
pub const SNAPSHOT_META_KEY_COMPACTED_AT_SEQ: &str = "compacted_at_seq";

/// Extract the [`Seq`] recorded in a snapshot event's meta (see
/// [`SNAPSHOT_META_KEY_COMPACTED_AT_SEQ`]).
///
/// Returns `None` for events whose kind is not [`SNAPSHOT_KIND`] or whose
/// meta lacks the expected key — this is a convenience over
/// `event.meta.get("compacted_at_seq")` for callers that would otherwise
/// duplicate the key string.
pub fn snapshot_meta_compacted_at_seq(event: &Event) -> Option<Seq> {
    if event.kind != SNAPSHOT_KIND {
        return None;
    }
    event
        .meta
        .get(SNAPSHOT_META_KEY_COMPACTED_AT_SEQ)
        .and_then(|v| v.as_u64())
        .map(Seq)
}
