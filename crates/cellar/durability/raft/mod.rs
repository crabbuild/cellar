//! Node-owned multi-group Raft effects. Business authority remains in the core.

pub(crate) mod cache;
mod filesystem;
pub(crate) mod options;
mod records;
pub(crate) mod storage;
