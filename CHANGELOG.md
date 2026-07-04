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
