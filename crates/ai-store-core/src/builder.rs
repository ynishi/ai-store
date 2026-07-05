//! Incremental `Store` construction.
//!
//! `Store::new` / `Store::with_checkpoint_backend` take every collaborator
//! (gates, sinks, checkpoint backend, config) as positional arguments, which
//! is exact but noisy at call sites that need only a subset — a consumer with
//! no gates and no sinks still writes out `Vec::new(), Vec::new()`, and one
//! that wants durable checkpoints has to pick the second constructor instead
//! of the first. `StoreBuilder` removes that boilerplate: gates and sinks
//! accumulate one call at a time, `checkpoints` is what selects between the
//! two underlying constructors, and `build` performs that selection for the
//! caller.

use std::sync::Arc;

use crate::backend::{CacheBackend, CheckpointBackend, EventBackend};
use crate::facade::{Store, StoreConfig};
use crate::gate::SchemaGate;
use crate::sink::ProjectionSink;

/// Builder for [`Store`]. Obtain one via [`Store::builder`].
pub struct StoreBuilder {
    events: Arc<dyn EventBackend>,
    cache: Arc<dyn CacheBackend>,
    gates: Vec<Arc<dyn SchemaGate>>,
    sinks: Vec<Arc<dyn ProjectionSink>>,
    checkpoints: Option<Arc<dyn CheckpointBackend>>,
    config: StoreConfig,
}

impl StoreBuilder {
    /// Start a builder over the two mandatory backends. Not exposed directly
    /// — construct via [`Store::builder`] so there is exactly one entry
    /// point into the builder, matching the single-write-channel design of
    /// the facade itself.
    pub(crate) fn new(events: Arc<dyn EventBackend>, cache: Arc<dyn CacheBackend>) -> Self {
        Self {
            events,
            cache,
            gates: Vec::new(),
            sinks: Vec::new(),
            checkpoints: None,
            config: StoreConfig::default(),
        }
    }

    /// Register a `SchemaGate`. Gates accumulate in registration order and
    /// are invoked in that same order by `Store::append` — the same
    /// semantics as passing a `Vec` to `Store::new` directly.
    pub fn gate(mut self, gate: Arc<dyn SchemaGate>) -> Self {
        self.gates.push(gate);
        self
    }

    /// Register a `ProjectionSink`. Sinks accumulate in registration order.
    pub fn sink(mut self, sink: Arc<dyn ProjectionSink>) -> Self {
        self.sinks.push(sink);
        self
    }

    /// Attach a `CheckpointBackend`, selecting
    /// `Store::with_checkpoint_backend` (persisted checkpoints) over
    /// `Store::new` (in-memory-only checkpoints) at `build` time. See the
    /// crate-level "Checkpoint storage note" for the durability contract
    /// this adds.
    pub fn checkpoints(mut self, checkpoints: Arc<dyn CheckpointBackend>) -> Self {
        self.checkpoints = Some(checkpoints);
        self
    }

    /// Replace the whole `StoreConfig` in one call. Overrides any prior
    /// `checkpoints`/`config`/`cache_stride` call on `config` specifically
    /// (later calls to `config` or `cache_stride` win — last write wins,
    /// same as any other builder field).
    pub fn config(mut self, config: StoreConfig) -> Self {
        self.config = config;
        self
    }

    /// Shortcut for `config.cache_stride` without constructing a whole
    /// `StoreConfig` — the common case, since `StoreConfig` currently has
    /// exactly one field.
    pub fn cache_stride(mut self, stride: u64) -> Self {
        self.config.cache_stride = stride;
        self
    }

    /// Build the `Store`. Dispatches to `Store::with_checkpoint_backend` if
    /// `.checkpoints(..)` was called, otherwise `Store::new`.
    pub fn build(self) -> Store {
        match self.checkpoints {
            Some(checkpoints) => Store::with_checkpoint_backend(
                self.events,
                self.cache,
                self.gates,
                self.sinks,
                self.config,
                checkpoints,
            ),
            None => Store::new(self.events, self.cache, self.gates, self.sinks, self.config),
        }
    }
}
