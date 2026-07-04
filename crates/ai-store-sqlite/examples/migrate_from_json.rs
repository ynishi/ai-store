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
//! ## 1. Timestamps: use "now" or the legacy `at_ms`?
//!
//! `EventBackend::append` stamps its own timestamp (the moment of import).
//! There is deliberately no SPI method that lets a caller override the
//! backend-stamped `at_ms` — allowing arbitrary timestamps would break the
//! monotonicity assumption `seq_at_time` relies on.
//!
//! Consequence: `Store::seq_at_time` answers *"when was this event
//! imported"*, not *"when did the change happen in the source system"*.
//!
//! Recipe: carry the legacy timestamp inside `meta` (see `legacy_at_ms`
//! below). Wall-clock queries against the source system's timeline then
//! become `read_by_meta` (equality) or `read` + client-side filter (range).
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

use ai_store_core::{Patch, Seq, Store, StoreConfig, StreamId};
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
        let meta = json!({
            "legacy_at_ms": entry.at_ms,
            "legacy_index": i,
        });
        store.append(stream, &entry.kind, patch, meta).await?;

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
        StoreConfig { cache_stride: 256 },
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

    // Demo: retrieve the imported event that carried a specific
    // `legacy_at_ms` — exercises `read_by_meta` (Issue #2) on the same
    // metadata carried during migration.
    let hits = store
        .read_by_meta(
            &stream,
            "legacy_at_ms",
            &json!(1_700_000_060_000_i64),
            Seq(1),
            10,
        )
        .await?;
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].kind, "update_title");
    println!(
        "read_by_meta(legacy_at_ms=1700000060000) → seq {:?}, kind={}",
        hits[0].seq, hits[0].kind
    );

    be.driver.shutdown().await?;
    Ok(())
}
