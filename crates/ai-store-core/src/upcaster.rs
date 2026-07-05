//! Read-time event upcasting — the schema-evolution hook.
//!
//! Long-lived streams outlast the shape their patches were originally
//! written against. Without a way to translate old-shape events at read
//! time, [`Store::state_at`] would reconstruct a mixed old/new document
//! and every downstream (gates, sinks, read-model field extraction) would
//! see that mixed shape. This module provides the hook.
//!
//! ## Contract
//!
//! An [`Upcaster`] is a pure function `Event -> Event`. It is applied by
//! [`crate::Store`] at read time — every time an [`crate::Event`] leaves
//! the backend (public [`Store::read`], the internal reads inside
//! [`Store::state_at`] / [`Store::catch_up`] / the post-`append` sink
//! dispatch), the registered chain is walked in registration order and
//! each element sees the output of the previous one. The stored bytes on
//! the backend are never mutated; upcasting is a projection, not a
//! rewrite.
//!
//! ## Chain semantics
//!
//! Multiple upcasters compose in registration order — the intended shape
//! is one upcaster per schema-version transition (`v1 → v2`, `v2 → v3`,
//! …) with a dispatch on `event.meta[SCHEMA_VERSION_META_KEY]` inside
//! each. A `v1` event walks the entire chain and arrives at `v3`; a `v3`
//! event flows through each step unchanged when the dispatch decides not
//! to touch it. Idempotence under a given schema version is the
//! consumer's responsibility (see the recommended workflow in the facade
//! module's "Schema evolution" section).
//!
//! ## Not fallible
//!
//! `Upcaster::upcast` is signature-infallible on purpose. An upcaster
//! that cannot honor the transition it is registered for is a bug in the
//! consumer's schema-evolution plan, not a runtime condition the store
//! layer can meaningfully recover from — bubbling a `Result` up through
//! every read call site would let that bug turn into pervasive
//! error-handling ceremony. Consumers that want to signal an unhandled
//! case can leave the event untouched and log inside the impl.
//!
//! ## What is *not* upcasted
//!
//! [`crate::EventBackend::read_by_meta`] on backends that implement it
//! natively (SQLite via `json_extract` on the persisted `meta` column,
//! for one) evaluates the meta predicate against the *stored* event, not
//! the upcasted one — the filter runs inside the backend before any
//! upcaster gets a chance. Consumers who mix schema evolution with
//! `read_by_meta` should keep the meta fields they filter on stable
//! across shape changes (or add a compatible synonym before renaming an
//! old key). Every other read path — including the client-side default
//! `read_by_meta` implementation on backends that do not override it —
//! runs through the upcaster chain.
//!
//! [`Store::read`]: crate::Store::read
//! [`Store::state_at`]: crate::Store::state_at
//! [`Store::catch_up`]: crate::Store::catch_up

use crate::event::Event;

/// Reserved key on [`Event::meta`] for the consumer's schema version.
///
/// Store code does not read or write this key. It is documented here so
/// multiple [`Upcaster`] implementations across a codebase can agree on
/// where to look, rather than each inventing a private key that collides
/// with the next one.
///
/// Typical shape: `"_schema_version": 3`. Upcasters that need to dispatch
/// on it read `event.meta.get(SCHEMA_VERSION_META_KEY)` and match against
/// the version integers they know how to upgrade from.
pub const SCHEMA_VERSION_META_KEY: &str = "_schema_version";

/// Read-time transformation from one shape of [`Event`] to another.
///
/// See the module rustdoc for the contract, chain semantics, and the
/// documented `read_by_meta` caveat.
///
/// Implementations should be pure and stateless (`&self` methods only);
/// they are called from every read path and may be invoked concurrently.
pub trait Upcaster: Send + Sync {
    /// Transform `event` to the current-shape form, or return it
    /// unchanged when this upcaster does not apply. Should not panic;
    /// unrecognized versions are safe to pass through.
    fn upcast(&self, event: Event) -> Event;
}
