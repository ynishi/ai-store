#![warn(missing_docs)]

//! # ai-store-mem
//!
//! In-memory implementations of `EventBackend` and `CacheBackend` for the
//! ai-store family.
//!
//! ## Architecture
//!
//! `MemEventBackend` and `MemCacheBackend` each own a single `tokio::sync::Mutex`
//! guarding their entire state (per-stream event vectors, label map, cache
//! entries). This deliberately mirrors the actor discipline used by
//! `ai-store-sqlite`: a single writer serializes all mutations so that `Seq`
//! assignment is gap-free and monotonic without any additional coordination on
//! the caller side.
//!
//! Intended use cases:
//!
//! 1. **Test double** — swap into consumers that would otherwise take
//!    `Arc<dyn EventBackend>` to avoid touching disk in unit tests.
//! 2. **Conformance rig** — validate that the SPI shape defined in
//!    `ai-store-core` is actually implementable end-to-end.
//! 3. **Lightweight in-process store** — for tools that never need durability
//!    across restarts.
//!
//! State is not persisted anywhere; it lives only for the lifetime of the
//! backend instance.

mod cache;
mod event;

pub use cache::MemCacheBackend;
pub use event::MemEventBackend;
