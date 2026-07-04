#![warn(missing_docs)]

//! # ai-store-sqlite
//!
//! SQLite implementations of `EventBackend` and `CacheBackend` for the
//! ai-store family, using [`rusqlite_isle`] to confine the blocking rusqlite
//! `Connection` to a dedicated OS thread and expose an async facade.
//!
//! ## Architecture
//!
//! Both backends hold a cloneable `AsyncIsle` handle. Every mutation
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
//! ## Schema
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
//! ```
//!
//! WAL journal mode is enabled at open so multi-reader consumers can proceed
//! concurrently with the writer.

mod backend;
mod driver;

pub use backend::{SqliteCacheBackend, SqliteEventBackend};
pub use driver::{SqliteBackendDriver, SqliteBackends};
