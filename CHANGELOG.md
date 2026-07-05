# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `ai_store_sqlite`: `events` now carries a database-level append-only guard
  (migration 4 — `trg_events_no_update` / `trg_events_no_delete`). Previously
  the append-only invariant relied solely on `EventBackend` exposing no
  `delete`/`overwrite` method; a raw SQL client, a second process opening the
  same file, or a manual `sqlite3` session could still mutate `events`
  directly. Any `UPDATE`/`DELETE` on `events` now aborts at the storage
  layer regardless of the connection issuing it. `Store::revert` is
  unaffected (it appends the reverse-diff event rather than mutating an
  existing row). `labels` / `cache` / `sink_checkpoints` / `read_model` are
  mutable by design and are not triggered.
- `ai-store-core::Store::builder(events, cache) -> StoreBuilder`: incremental
  construction alternative to `Store::new` / `Store::with_checkpoint_backend`.
  `StoreBuilder` accumulates `.gate(..)` / `.sink(..)` one call at a time,
  `.checkpoints(..)` selects durable checkpoints, and `.config(..)` /
  `.cache_stride(..)` set `StoreConfig` — `.build()` dispatches to whichever
  underlying constructor `.checkpoints(..)` implies. Purely additive; `Store::new`
  and `Store::with_checkpoint_backend` are unchanged and still the direct path
  when every collaborator is known up front.
- `ai_store_sqlite::SqliteStore`: one-shot SQLite-backed `Store` bundling the
  backends, driver, and shared `AsyncIsle` handle that a hand-assembled
  `SqliteBackends` + `Store::builder` + "keep the driver alive somewhere"
  previously required a bespoke wrapper type for. `SqliteStore::open` /
  `open_in_memory` build a `Store` with durable checkpoints and no gates/sinks
  in one call; `open_with` / `open_in_memory_with` take an
  `impl FnOnce(StoreBuilder) -> StoreBuilder` to register application-defined
  gates/sinks before `build()`. Derefs to `Store`, so most call sites never
  need to name `SqliteBackends` directly. `read_model()` builds a
  `SqliteReadModel` sharing the same SQLite thread for direct queries (it does
  not register itself as an automatic sink — see its rustdoc for why and for
  the manual-assembly alternative when automatic dispatch from the first
  write is needed). `shutdown()` joins the SQLite thread without a separate
  driver handle.
- `ai_store_core::KindGate`: a `SchemaGate` that dispatches validation by
  event `kind` — `KindGate::new().on(kind, |ctx| ..).fallback(|ctx| ..)`
  replaces the hand-written `match ctx.kind { .. }` a consumer with
  per-kind validation rules (e.g. an `"append"` transition invariant vs. a
  `"close"` required-fields invariant) previously had to write inside its own
  `SchemaGate` impl. Unregistered kinds pass through unconditionally unless a
  `.fallback` is set. `REVERT_KIND` is dispatched like any other kind — it is
  not special-cased, so a validator or fallback that rejects it can make a
  stream's history unrecoverable via `Store::revert`; see the rustdoc note on
  `KindGate` for how to keep reverts usable.
- `ai-store-core::ProjectionSink::accepts(&self, stream) -> bool`: default
  method (default `true`) letting a sink declare it is only interested in a
  subset of streams. The facade's automatic dispatch (post-`append` commit,
  `catch_up` / `rebuild`, `label_set` / `label_delete` notifications) skips a
  stream a sink does not accept entirely — it is not counted in
  `CatchUpReport::skipped`, since it was never that sink's concern.
  `Store::materialize_to_sink` deliberately bypasses this filter (an explicit
  caller-named dump). Replaces the previous pattern of a sink comparing
  `stream` against a remembered value inside its own `commit` and no-op'ing
  otherwise.
- `ai_store_fileproj::CombinedFileSink` + `Renderer`: a `ProjectionSink` that
  folds every stream it observes into one rendered file, keyed by an
  in-memory `BTreeMap<StreamId, Value>` snapshot (dictionary order, since
  `StreamId` now derives `Ord`). A repeat commit whose rendered output is
  byte-identical to the last write is skipped rather than rewritten
  (`DefaultHasher`-based duplicate-write filter, not an integrity check);
  writes go through the same temp-sibling-then-rename as `FileProjection`.
  The snapshot is process-memory only — after a restart, drive it via
  `Store::rebuild` rather than trusting `catch_up` alone.
- `ai-store-core::Store::revert_with_meta`: like `revert`, but merges
  caller-supplied fields into the appended revert event's `meta` instead of
  always writing the fixed `{"revert_to": to}` shape. `"revert_to"` is a
  reserved key — a same-named field in the caller's `extra_meta` is
  overwritten by the generated value rather than erroring. `revert` is now
  defined in terms of `revert_with_meta(stream, to, Value::Null)`.
- `ai-store-core::patch` module: `add`/`replace`/`remove` helpers and a
  chainable `Builder` for constructing single- or multi-operation
  `json_patch::Patch` values without hand-assembling JSON Patch documents.
- `ai-store-sqlite::read_model::SqliteReadModel`: an opt-in `ProjectionSink`
  that materializes the latest state of every stream into one queryable
  `read_model` row (new migration 3 table), answering cross-stream questions
  the event log itself has no index for (e.g. "which streams have
  `meta.owner == \"alice\"`", "the 20 most recently updated streams") without
  a dedicated dispatch path — it rides the existing `ProjectionSink` +
  `catch_up` machinery. Query surface: `query` (dotted-field `Filter::{Eq,
  In, Like, And, Or}` + `order_by` + `limit`/`offset`), `count`, `get`,
  `tail`, and `create_field_index` for indexing a hot filter field. Field
  paths are restricted to `[A-Za-z0-9_.]+` and always bound as query
  parameters, never interpolated into SQL. The upsert is idempotent under
  redelivery: a same- or older-`seq` `commit` for a stream never rewinds its
  row (`WHERE excluded.last_seq > read_model.last_seq` on the conflict
  branch). `with_tombstone_kind` opts a sink into a minimal live/dead toggle
  (`Query::include_dead`) without any cascading delete semantics.
  `SqliteBackends::isle` exposes the shared SQLite-thread handle needed to
  construct one alongside the mandatory `events`/`cache`/`checkpoints` trio.

### Changed

- **BREAKING**: `ProjectionSink::on_label_set` gains a fifth parameter,
  `event: &Event` — the committed event the label now points at, so a sink
  can read its `at` (wall-clock or imported timestamp) and `meta` without a
  separate `read` round-trip or smuggling that information through
  `Store::append`'s `meta` as a side channel. `Store::label_set` now fetches
  that event (best-effort — a fetch failure or empty read is swallowed the
  same way a failing `commit` is, since the label change itself already
  succeeded). Every implementation in this workspace (`FileProjection`,
  `ai-store-sync::SyncProjectionSink` + its `BlockingSink` bridge) is updated;
  external implementations must add the parameter to keep compiling.
- **BREAKING**: `EventBackend::append` / `EventBackend::import_event` now
  return `Result<Committed, StoreError>` instead of `Result<Seq, StoreError>`
  (new struct `Committed { seq, at }`). `Store::append`, `Store::import_event`,
  and `Store::revert` / `revert_with_meta` follow suit, as do the
  `ai-store-sync::BlockingStore` mirrors of those three methods. Consumers
  that previously round-tripped through `read(stream, seq, 1)` just to learn
  the backend-stamped `at` can now read it directly off the returned
  `Committed` value.
- `ai-store-core::StreamId` now derives `PartialOrd, Ord` (lexicographic over
  the inner `String`), needed for `CombinedFileSink`'s
  `BTreeMap<StreamId, _>` snapshot. Purely additive for existing consumers.

### Deprecated

### Removed

### Fixed

### Security

## [0.8.0] - 2026-07-05

### Added

- `ai-store-core::CheckpointBackend`: new SPI trait persisting
  `ProjectionSink` checkpoints across process restarts, with
  `Store::with_checkpoint_backend` as the opt-in constructor
  (`Store::new` keeps the in-memory-only behavior). Implementations:
  `ai-store-sqlite::SqliteCheckpointBackend` (new `sink_checkpoints`
  table) and `ai-store-mem::MemCheckpointBackend`. Checkpoint advances
  persist before the in-memory cache updates; backend read failures fail
  open (worst case is a redundant redelivery, never a missed event).
- `ai-store-sqlite`: `PRAGMA user_version`-tracked stepwise schema
  migration runner replacing the one-shot `SCHEMA` constant. Each step
  applies its DDL and bumps `user_version` in one transaction; databases
  written by a newer `ai-store-sqlite` are refused at open. Pre-existing
  unversioned databases are adopted idempotently.
- `CatchUpReport.failures`: per-stream failure details
  (`CatchUpFailure { stream, sink_id, message }`) recorded by `catch_up`
  / `rebuild`.

### Changed

- `Store::catch_up` / `Store::rebuild` no longer abort the whole call on
  the first sink failure. A failing commit halts catch-up for that stream
  only (order within a stream is preserved); every other stream is still
  driven to completion. Remaining events on a failed stream are counted
  in `CatchUpReport.skipped`, which was previously never incremented.

### Fixed

- Closed the gate-validate/append TOCTOU race: `append`, `import_event`
  and `revert` now hold a per-stream write lock across the whole
  state-read → gate-validate → backend-append → sink-dispatch critical
  section, so concurrent writes to the same stream can no longer validate
  against the same stale `current`. Writes to different streams remain
  fully concurrent.

## [0.7.0] - 2026-07-05

### Added

- `ai-store-core::SqliteBackend`: a new SPI trait generalizing the
  `new(handle) -> Self` constructor pattern that `ai-store-sqlite`'s
  `SqliteEventBackend` / `SqliteCacheBackend` already used. It is
  implemented for both types via an associated `Handle` type, so
  `ai-store-core` gains no infrastructure dependency of its own. Downstream
  crates can write backend-construction code generic over "any backend
  built from an existing native handle" without depending on
  `ai-store-sqlite` directly. Existing inherent `new` constructors are
  unchanged; the trait impl is purely additive.

### Changed

- `ai-store-sqlite`: bumped `rusqlite` 0.32 → 0.37 and `rusqlite-isle` 0.3 →
  0.4 (bringing `libsqlite3-sys` up to 0.30 → 0.35). This returns to the
  `rusqlite-isle` 0.4 band, the latest published `rusqlite-isle` release at
  time of writing. `libsqlite3-sys` is now also pinned as an explicit
  direct dependency (`0.35`) instead of being left to transitive
  resolution, to make feature-unification conflicts with other
  `libsqlite3-sys` consumers in a dependent's tree visible at `cargo
  update` time rather than silently resolved. No backend-facing API
  changes; `SqliteEventBackend` / `SqliteCacheBackend` behavior is
  unchanged.
- Downgrade ladder retrospective: v0.5.0 stepped down to the
  `agent-block-core` band (`rusqlite` 0.31) so that crate could adopt
  `ai-store` without a dependency-tree version conflict; v0.6.0 moved to
  the `journal-mcp-core` band (`rusqlite` 0.32) for the same reason on that
  crate's behalf; v0.7.0 (this release) returns to the latest
  `rusqlite-isle` band (`rusqlite` 0.37), completing the 3-hop ladder. Each
  hop's target dependent (`agent-block-core`, `journal-mcp-core`) adopted
  `ai-store` at its corresponding band; this release does not require
  either of those crates to move again.

## [0.6.0] - 2026-07-05

### Changed

- `ai-store-sqlite`: bumped `rusqlite` 0.31 → 0.32 and `rusqlite-isle` 0.2 →
  0.3 (bringing `libsqlite3-sys` up to 0.28 → 0.30). This lands the
  `journal-mcp-core` dependency band, so that project can adopt `ai-store`
  without a version conflict. No backend-facing API changes;
  `SqliteEventBackend` / `SqliteCacheBackend` behavior is unchanged.
- Roadmap: v0.5.0 stepped down to the `agent-block-core` band (`rusqlite`
  0.31), v0.6.0 (this release) moves to the `journal-mcp-core` band
  (`rusqlite` 0.32), and v0.7.0 is planned to return to the `rusqlite-isle`
  0.4 (`rusqlite` 0.37) band.

## [0.5.0] - 2026-07-05

### Changed

- `ai-store-sqlite`: downgraded `rusqlite` 0.37 → 0.31 and `rusqlite-isle`
  0.1 → 0.2 (bringing `libsqlite3-sys` down to 0.35 → 0.28). This is a
  deliberate downgrade release: it puts `ai-store` on the same
  `rusqlite`/`libsqlite3-sys` band as `agent-block-core`, so that project can
  adopt `ai-store` without a dependency-tree version conflict. No
  backend-facing API changes; `SqliteEventBackend` / `SqliteCacheBackend`
  behavior is unchanged.
- Planned follow-up: v0.6.0 moves to `rusqlite-isle` 0.3 (`rusqlite` 0.32,
  `journal-mcp-core` band), then v0.7.0 returns to the `rusqlite-isle` 0.4
  (`rusqlite` 0.37) band this release stepped down from.

## [0.4.0] - 2026-07-04

### Added

- `Store::import_event(stream, kind, patch, meta, at: Timestamp)`: import one
  event with a caller-supplied historical timestamp instead of the
  wall-clock time of the call, for backfilling history from a system that
  already has its own notion of "when" (closes #8). `EventBackend` gains a
  matching `import_event` method with a default implementation returning
  `StoreError::BackendUnsupported` — existing external backend
  implementations keep compiling and behave exactly as before; `ai-store-mem`
  and `ai-store-sqlite` both override it. `BlockingStore::import_event`
  mirrors the async facade. `examples/migrate_from_json.rs` (ai-store-sqlite)
  now preserves the legacy log's original timestamps via `import_event`
  instead of stashing them in `meta`.

## [0.3.0] - 2026-07-04

### Added

- `Store::materialize_to_sink(stream, sink_id, at: Option<Seq>)`: dump a
  stream's state — at `at`, or the current head when `at` is `None` — to a
  named sink immediately, without a synthetic label or waiting for
  `catch_up`. Sink errors propagate to the caller; the sink's checkpoint is
  left untouched (closes #6).
- `Store::label_delete` + `ProjectionSink::on_label_deleted`: remove a label
  and notify sinks so they can react (e.g. `FileProjection` archives the
  rendered `<label>.md` instead of leaving a stale file behind). Idempotent:
  deleting a label that is not defined returns `Ok(false)` rather than an
  error, and dispatches no sink notification (closes #7).

### Changed

- `EventBackend`: added required method `label_delete` (breaking — every
  backend implementation must now provide label removal; `ai-store-mem` and
  `ai-store-sqlite` both do).

## [0.2.0] - 2026-07-04

### Added

- `EventBackend::read_by_meta`: page-forward filter on top-level `meta[field]`
  with a default O(N) implementation; SQLite backend overrides via
  `json_extract` for sub-linear lookups (closes #2).
- `ai-store-sync` crate: blocking (sync) facade `BlockingStore` over
  `ai-store-core::Store` for synchronous consumers (closes #1).
- `ai-store-sync` sink bridge: `SyncProjectionSink` / `BlockingSink` adapter
  for blocking sinks under the async `ProjectionSink` contract (closes #5).
- SQLite example `migrate_from_json`: end-to-end migration of legacy JSON
  event logs into a Store-backed SQLite backend (closes #4).

### Changed

- `Store::append` fast path: skip pre-commit `next` materialization when no
  `SchemaGate` is registered; further skip post-commit materialization when
  the assigned seq misses `cache_stride` and no `ProjectionSink` is
  registered.

### Documentation

- Crate-level cost model section for large document-level state:
  per-append memory, cache stride trade-off, stream granularity guidance
  (closes #3).

## [0.1.0] - 2026-07-04

Initial public release. Shared storage backbone for MCP servers whose state
is a JSON document — append-only event log, RFC 6902 JSON Patch as the diff
format, revert-as-commit semantics, materialization cache and file
projection as first-class sinks.

Crates:

- `ai-store-core` — facade + SPI traits + core types
- `ai-store-mem` — in-memory backend
- `ai-store-sqlite` — SQLite backend via rusqlite-isle 0.1.0
- `ai-store-fileproj` — draft.md / label.md ProjectionSink
