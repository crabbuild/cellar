//! Committed business-feature adapters. Immutable values carry their actual
//! bounded bytes in the command transaction and in the archive dependency set.
//! Alarm and outbox scheduling are separate integration work.

#[path = "effect_blob.rs"]
mod blob;
pub(crate) use blob::{capture_dependencies, get_blob, put_blob};
pub use blob::{RaftBlobLimits, RaftBlobReference};
