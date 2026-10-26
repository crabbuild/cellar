// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Node-owned multi-group Raft effects. Business authority remains in the core.

pub(crate) mod apply;
pub(crate) mod archive;
pub(crate) mod budget;
pub(crate) mod cache;
pub(crate) mod catalog;
mod catalog_wire;
pub(crate) mod completion;
pub(crate) mod effects;
pub(crate) mod execution;
mod filesystem;
pub(crate) mod host;
mod host_archive;
mod host_sql;
pub(crate) use host_sql::ResourceObserver as SqlResourceObserver;
pub(crate) mod inspection;
mod inspection_identity;
mod inspection_wire;
pub(crate) mod mailbox;
mod manifest;
pub(crate) mod member_state;
pub(crate) mod object_metrics;
pub(crate) mod operations;
pub(crate) mod options;
pub(crate) mod performance;
mod query;
mod query_identity;
mod query_route;
mod query_wire;
pub(crate) mod receipt_maintenance;
mod receipts;
mod records;
mod recovery;
mod replica;
pub(crate) mod rooted;
pub(crate) mod runtime;
pub(crate) mod snapshot;
pub(crate) mod storage;
pub(crate) mod transport;
mod transport_frame;
pub(crate) mod transport_options;
pub(crate) mod version;
mod working;
