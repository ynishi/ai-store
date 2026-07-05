//! Identifier and coordinate newtypes.
//!
//! `StreamId` is the consumer-defined name of an event stream (book slug / table
//! name / journal id). `Seq` is the monotonic per-stream event coordinate — a `Seq`
//! value pins a unique point in the stream's history. `Label` is a mutable ref
//! (git-tag equivalent) pointing at a `Seq`; the label itself is rewritable but
//! its target `Seq` is immutable.

use std::fmt;

use serde::{Deserialize, Serialize};

/// Consumer-defined stream identifier.
///
/// Meaning is opaque to the store — typical values are a book slug, table name,
/// or journal id. Uniqueness is scoped to a single `Store` instance.
///
/// `Ord`/`PartialOrd` derive lexicographically over the inner `String` —
/// this is what lets a multi-stream sink (e.g.
/// `ai_store_fileproj::CombinedFileSink`) key a `BTreeMap<StreamId, _>` and
/// get a stable, deterministic render order for free, without inventing its
/// own comparator.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StreamId(pub String);

impl StreamId {
    /// Construct from an owned or borrowed string.
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Borrow the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Per-stream monotonic event coordinate.
///
/// `Seq` values are assigned by the store's writer, are gap-free within a stream,
/// and increase monotonically. A `Seq` pins a unique point in history and never
/// refers to a different event after assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Seq(pub u64);

impl Seq {
    /// The sentinel coordinate used for an empty stream (no events yet).
    pub const ZERO: Seq = Seq(0);

    /// Next sequential coordinate.
    pub fn next(self) -> Seq {
        Seq(self.0 + 1)
    }
}

impl fmt::Display for Seq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Mutable pointer from a name to a `Seq` (git-ref equivalent).
///
/// Labels are scoped per stream. The label name is a mutable pointer, but its
/// current target `Seq` refers to an immutable event, so pinning a label to a
/// `Seq` produces a stable reference until the label is explicitly moved.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Label(pub String);

impl Label {
    /// Construct from an owned or borrowed string.
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    /// Borrow the underlying string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Label {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Unix epoch time in milliseconds.
///
/// A newtype rather than a bare `i64` to keep it distinct from event
/// coordinates (`Seq`) at the type level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Timestamp(pub i64);

impl Timestamp {
    /// Current wall-clock time.
    pub fn now() -> Self {
        let ms = time::OffsetDateTime::now_utc().unix_timestamp_nanos() / 1_000_000;
        Self(ms as i64)
    }
}
