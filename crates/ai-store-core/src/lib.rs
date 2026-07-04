#![warn(missing_docs)]

//! # ai-store-core
//!
//! Facade, SPI traits, and core types for the ai-store family.
//!
//! ## Architecture
//!
//! ai-store separates the **write facade** (public API for domain consumers) from the
//! **SPI trait** layer (backend implementers). Consumers hold a `Store` handle and never
//! touch backends directly; backends implement `EventBackend` / `CacheBackend` /
//! `ProjectionSink` and are wired into the facade at construction time.
//!
//! Four invariants are structural (encoded in the type system, not enforced by
//! runtime lock):
//!
//! 1. **Append-only history.** `EventBackend` exposes no `delete` or `overwrite`
//!    method. Immutability is guaranteed by API absence, not by runtime checks.
//! 2. **Diff-based SoT.** The event log stores JSON Patch (RFC 6902) forward diffs.
//!    Full snapshots are a derived cache (`CacheBackend`) and can be pruned freely.
//! 3. **Revert-as-commit.** Restoring a past state produces a new event whose patch
//!    is the reverse diff. The prior state is never overwritten; the log grows.
//! 4. **Single write channel.** Every write flows through `Store::append`, which
//!    invokes `SchemaGate::validate` before delegating to the backend in a single
//!    transaction. There is no raw-append escape hatch on the public API.

mod backend;
mod error;
mod event;
mod facade;
mod gate;
mod id;
mod sink;
mod state;

pub use backend::{CacheBackend, CheckpointBackend, EventBackend, SqliteBackend};
pub use error::{SchemaViolation, StoreError};
pub use event::{Event, NewEvent};
pub use facade::{Store, StoreConfig, REVERT_KIND};
pub use gate::{GateCtx, SchemaGate};
pub use id::{Label, Seq, StreamId, Timestamp};
pub use sink::{CatchUpFailure, CatchUpReport, ProjectionSink};
pub use state::{empty_state, replay_from};

// Re-export the patch type so consumers don't need a direct json-patch dep to
// call `Store::append`.
pub use json_patch::Patch;
