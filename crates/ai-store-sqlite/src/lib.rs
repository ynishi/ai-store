#![warn(missing_docs)]

//! # ai-store-sqlite
//!
//! SQLite implementations of `EventBackend`, `CacheBackend`, and
//! `CheckpointBackend` for the ai-store family, using [`rusqlite_isle`] to
//! confine the blocking rusqlite `Connection` to a dedicated OS thread and
//! expose an async facade.
//!
//! ## Architecture
//!
//! All three backends hold a cloneable `AsyncIsle` handle. Every mutation
//! (`append`, `label_set`, `put`) executes in a single closure on the SQLite
//! thread, so:
//!
//! - Gap-free monotonic `Seq` is guaranteed by the writer thread serializing
//!   inserts through one closure per append — no CTE gymnastics needed.
//! - Atomicity is native SQLite transaction (`BEGIN IMMEDIATE ... COMMIT`) —
//!   we don't add a second lock layer.
//! - The tokio task never blocks; every call routes through the isle's mpsc
//!   channel and awaits a oneshot response.
//!
//! ## Schema versioning
//!
//! The schema is applied by a stepwise migration runner tracked via
//! `PRAGMA user_version` (see `migration` module, private to this crate)
//! rather than a single `CREATE TABLE IF NOT EXISTS` batch. Opening an
//! existing database re-applies only the migrations it hasn't seen yet;
//! opening a database from a *newer* build of this crate is rejected rather
//! than silently misinterpreted. The current schema, as of the latest
//! migration:
//!
//! ```sql
//! CREATE TABLE events (
//!     stream TEXT NOT NULL,
//!     seq    INTEGER NOT NULL,
//!     kind   TEXT NOT NULL,
//!     patch  TEXT NOT NULL,   -- json_patch::Patch serialized
//!     meta   TEXT NOT NULL,   -- serde_json::Value serialized
//!     at_ms  INTEGER NOT NULL,
//!     PRIMARY KEY (stream, seq)
//! );
//! CREATE INDEX ix_events_stream_at ON events(stream, at_ms);
//!
//! CREATE TABLE labels (
//!     stream TEXT NOT NULL,
//!     name   TEXT NOT NULL,
//!     at_seq INTEGER NOT NULL,
//!     PRIMARY KEY (stream, name)
//! );
//!
//! CREATE TABLE cache (
//!     stream TEXT NOT NULL,
//!     at_seq INTEGER NOT NULL,
//!     state  TEXT NOT NULL,   -- serde_json::Value serialized
//!     PRIMARY KEY (stream, at_seq)
//! );
//!
//! CREATE TABLE sink_checkpoints (
//!     sink_id TEXT NOT NULL,
//!     stream  TEXT NOT NULL,
//!     at_seq  INTEGER NOT NULL,
//!     PRIMARY KEY (sink_id, stream)
//! );
//!
//! CREATE TABLE read_model (
//!     stream     TEXT NOT NULL PRIMARY KEY,
//!     state      TEXT NOT NULL,
//!     last_seq   INTEGER NOT NULL,
//!     updated_at INTEGER NOT NULL,
//!     live       INTEGER NOT NULL DEFAULT 1
//! );
//! CREATE INDEX ix_read_model_updated ON read_model(updated_at);
//! ```
//!
//! WAL journal mode is enabled at open so multi-reader consumers can proceed
//! concurrently with the writer.
//!
//! ## Read-model projection (opt-in)
//!
//! `events` / `cache` / `checkpoints` are the mandatory SPI triad every
//! `Store` needs. `read_model` (see the [`read_model`] module) is a fourth,
//! *optional* table backing [`read_model::SqliteReadModel`] — a
//! `ProjectionSink` a consumer registers with `Store` to get a queryable
//! "current state per stream" cache, answering questions the event log
//! itself has no cross-stream index for (e.g. "which streams have
//! `meta.owner == \"alice\"`", "the 20 most recently updated streams").
//! Because it rides the existing `ProjectionSink` + `catch_up` machinery,
//! opting in costs one `Store::new(..., vec![Arc::new(read_model.clone())])`
//! call — no separate wiring. Build it from the same SQLite thread as the
//! other backends via [`SqliteBackends::isle`].

mod backend;
mod driver;
mod migration;
pub mod read_model;

pub use backend::{SqliteCacheBackend, SqliteCheckpointBackend, SqliteEventBackend};
pub use driver::{SqliteBackendDriver, SqliteBackends};
pub use read_model::{Filter, Order, Query, ReadModelRow, SqliteReadModel};
