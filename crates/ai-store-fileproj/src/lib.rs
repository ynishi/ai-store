#![warn(missing_docs)]

//! # ai-store-fileproj
//!
//! `FileProjection` — a `ProjectionSink` that materializes stream state to
//! plain `.md` files. Modelled on the FileProjection layer of journal-mcp,
//! generalized so it works for any consumer that provides a render function
//! from `serde_json::Value` to a `String`.
//!
//! ## Layout
//!
//! For a projection rooted at `<root>` with stream slug `<slug>`:
//!
//! ```text
//! <root>/<slug>/
//!   ├── draft.md            # current state (rewritten on every event)
//!   ├── <label>.md          # state at label pin (rewritten on label_set)
//!   └── _archive/
//!       └── <label>.<epoch-ms>.md   # previous versions of a rewritten label
//! ```
//!
//! `draft.md` reflects the head state after the most recent event. Each
//! label produces one file whose contents are the state at the seq the
//! label pins. When a label is moved, the previous file is archived under
//! `_archive/` before the new one is written, so history is preserved.
//!
//! ## Contract
//!
//! - Idempotent: replaying the same `(stream, seq)` or `(stream, label,
//!   at)` a second time is safe. `catch_up` / `rebuild` on the facade will
//!   re-drive the sink after a crash.
//! - Best-effort: a failing write does not roll back the underlying event
//!   or label. The facade will retry via its checkpoint machinery.
//! - Path-safe: stream and label names containing path separators, null
//!   bytes, or `..` are rejected with `StoreError::Backend`; they never
//!   escape the projection root.

mod projection;

pub use projection::{FileProjection, RenderFn};
