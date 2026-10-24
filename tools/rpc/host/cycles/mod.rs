//! T15: bounded self/A -> B -> A dependency waits and original-scope cleanup.
mod matrix;
mod tracker;

pub use matrix::run;
pub use tracker::Tracker;
