# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security

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
