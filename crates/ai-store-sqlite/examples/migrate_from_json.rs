//! # Migrating a legacy JSON log into `ai-store`
//!
//! Runnable end-to-end example. Invoke with:
//!
//! ```sh
//! cargo run --example migrate_from_json -p ai-store-sqlite
//! ```
//!
//! Consumers adopting `ai-store` often already have an append-only history
//! persisted as a flat JSON array (`Vec<LegacyEntry>`), typically with
//! `before` / `after` snapshots and a wall-clock timestamp. Rebuilding that
//! into a `Store`-backed stream is mechanical, but four questions come up
//! every time. This example answers them in one place.
//!
//! ## 1. Timestamps: preserve the source system's clock via `import_event`
//!
//! `Store::import_event` is the import/migration counterpart to
//! `Store::append`: it takes an explicit `Timestamp` and the backend records
//! it as the event's time coordinate instead of stamping the wall-clock time
//! of the call. Using it here means `Store::seq_at_time` answers *"when did
//! the change happen in the source system"*, not *"when did we run this
//! migration"*.
//!
//! `at` is a time coordinate, not the log's ordering key — `seq` orders the
//! log, `at` never does. Backfilling into an *empty* stream in chronological
//! order (the shape this example follows) keeps `seq_at_time`'s
//! non-decreasing-`at` assumption intact automatically. See
//! `Store::import_event`'s rustdoc for the caveat that applies to
//! non-chronological imports.
//!
//! ## 2. Event `kind`: uniform or per-action?
//!
//! Preserve the legacy `kind` verbatim whenever the source log carries one.
//! Uniform `"legacy_imported"` is easier to filter (one predicate) but
//! discards the semantic information that likely justified adopting
//! `ai-store` in the first place. This example preserves `kind`.
//!
//! ## 3. Reconstructing per-event patches from `(before, after)` pairs
//!
//! Use `json_patch::diff(&before, &after)` (RFC 6902). The important part
//! is verifying the *chain*: entry N's `before` must equal entry N-1's
//! `after`. Legacy writers that respect the log invariant produce chained
//! entries automatically; broken chains signal a source-data bug and are
//! worth aborting on rather than silently patching over.
//!
//! ## 4. Cache stride during backfill
//!
//! `StoreConfig::cache_stride` (default 64) materializes a full state
//! snapshot every N events. For very large legacy backfills a larger stride
//! reduces the JSON-serialize + backend-write cost during import at the
//! expense of slightly longer replay chains on `state_at` reads. If
//! post-migration reads are rare, consider `cache_stride = 512` or higher
//! for the import phase.
//!
//! ## Scope
//!
//! This example targets **backfilling into an empty stream**. Importing on
//! top of an existing (non-empty) stream requires the first legacy entry's
//! `before` to equal the current `Store::state(stream)` — the same chain
//! invariant, extended one step. That case is out of scope here; the
//! recipe is otherwise identical.

use std::sync::Arc;

use ai_store_core::{Patch, Seq, Store, StoreConfig, StreamId, Timestamp};
use ai_store_sqlite::SqliteBackends;
use json_patch::diff;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

/// Shape of a legacy log entry: before/after snapshots + wall-clock time.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LegacyEntry {
    kind: String,
    at_ms: i64,
    before: Value,
    after: Value,
}

/// Backfill `entries` into an empty `stream` on `store`. Returns the number
/// of events written.
async fn migrate(
    store: &Store,
    stream: &StreamId,
    entries: &[LegacyEntry],
) -> Result<usize, Box<dyn std::error::Error>> {
    let mut prev_after = Value::Null;
    for (i, entry) in entries.iter().enumerate() {
        // Chain invariant: entry N's `before` must equal entry N-1's `after`.
        // The first entry chains against `Value::Null` (empty stream base).
        if entry.before != prev_after {
            return Err(format!(
                "legacy chain broken at index {i}: `before` != previous `after` (or null for the head)"
            )
            .into());
        }

        let patch: Patch = diff(&entry.before, &entry.after);
        let meta = json!({ "legacy_index": i });
        store
            .import_event(stream, &entry.kind, patch, meta, Timestamp(entry.at_ms))
            .await?;

        prev_after = entry.after.clone();
    }
    Ok(entries.len())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Simulated legacy log. In practice this comes from
    // `serde_json::from_reader(File::open("legacy.json")?)`.
    let legacy = vec![
        LegacyEntry {
            kind: "create".into(),
            at_ms: 1_700_000_000_000,
            before: Value::Null,
            after: json!({ "title": "draft", "n": 0 }),
        },
        LegacyEntry {
            kind: "update_title".into(),
            at_ms: 1_700_000_060_000,
            before: json!({ "title": "draft", "n": 0 }),
            after: json!({ "title": "final", "n": 0 }),
        },
        LegacyEntry {
            kind: "bump".into(),
            at_ms: 1_700_000_120_000,
            before: json!({ "title": "final", "n": 0 }),
            after: json!({ "title": "final", "n": 1 }),
        },
    ];

    // In-memory SQLite for a self-contained example; swap for
    // `SqliteBackends::open(&path)` to persist.
    let be = SqliteBackends::open_in_memory().await?;
    let store = Store::new(
        Arc::new(be.events.clone()),
        Arc::new(be.cache.clone()),
        Vec::new(),
        Vec::new(),
        // Larger stride than default suits bulk backfills. Tune per workload.
        StoreConfig {
            cache_stride: 256,
            ..StoreConfig::default()
        },
    );
    let stream = StreamId::new("doc/legacy-001");

    let n = migrate(&store, &stream, &legacy).await?;
    println!("Imported {n} legacy entries into `{}`.", stream.as_str());

    // Assertion 1: reconstructed state matches the last legacy `after`.
    let final_state = store.state(&stream).await?;
    let expected = legacy.last().unwrap().after.clone();
    assert_eq!(final_state, expected);
    println!("Final state OK: {final_state}");

    // Assertion 2: log length matches source length.
    let head = store.head(&stream).await?;
    assert_eq!(head, Some(Seq(legacy.len() as u64)));

    // Assertion 3: intermediate state at seq 2 matches the second entry's `after`.
    let mid = store.state_at(&stream, Seq(2)).await?;
    assert_eq!(mid, legacy[1].after);

    // Assertion 4: `seq_at_time` answers against the *source system's*
    // timeline, since every event's `at` was imported verbatim rather than
    // stamped at migration time (Issue #8).
    let at_second_entry = Timestamp(legacy[1].at_ms);
    let seq_at_second = store.seq_at_time(&stream, at_second_entry).await?;
    assert_eq!(seq_at_second, Some(Seq(2)));
    println!(
        "seq_at_time({}) → {:?} (source-system timeline, not import time)",
        legacy[1].at_ms, seq_at_second
    );

    // Demo: retrieve the imported event by its position in the legacy log —
    // exercises `read_by_meta` (Issue #2) on the `legacy_index` metadata
    // carried during migration.
    let hits = store
        .read_by_meta(&stream, "legacy_index", &json!(1), Seq(1), 10)
        .await?;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].kind, "update_title");
    println!(
        "read_by_meta(legacy_index=1) → seq {:?}, kind={}",
        hits[0].seq, hits[0].kind
    );

    be.driver.shutdown().await?;
    Ok(())
}
