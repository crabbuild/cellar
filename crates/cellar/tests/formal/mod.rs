// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! CG-13 formal recovery cases share the archive recovery fixtures through a
//! small path-anchored module. The target root supplies the engine imports.

#![allow(dead_code, unused_imports)]
use super::*;
use crate::storage_fault as fault;
use cellar_ltx::{
    generation::ReadServices,
    internal::{CacheLimits, EXTENT_BYTES},
    stream_snapshot::{ScratchLimits, ScratchPool},
};
use futures_util::TryStreamExt;
use object_store::{path::Path, ObjectStore};
use runtime_fixture::persist_archive_intent;
use std::sync::atomic::Ordering;
#[path = "../support/cell_group_archive_store.rs"]
mod archive_store;
#[path = "../support/cell_group_checkpoint.rs"]
mod checkpoint_fixture;
#[path = "../support/raft_ltx_fixture.rs"]
mod images;
#[path = "../support/cell_group_runtime.rs"]
mod runtime_fixture;
#[path = "../support/cell_group_archive_tail.rs"]
mod tail_fixture;
include!("../support/cell_group_engine_fixture.rs");
#[path = "../support/cell_group_archive_recovery_fixture.rs"]
mod archive_recovery;
pub(crate) use archive_recovery::*;
#[path = "../support/cell_group_disaster_fence.rs"]
mod disaster_fixture;
pub(super) use disaster_fixture::{
    all_disks_deleted, all_disks_deleted_rooted, all_disks_deleted_with, cancel_import, import,
    resume, topology_wins,
};
pub(super) fn backend(case: &str, rooted: bool) -> Result<Arc<dyn object_store::ObjectStore>> {
    if std::env::var_os("CELLAR_CELL_GROUPS_RUSTFS_ENDPOINT").is_some() {
        rustfs::store(case, rooted)
    } else {
        Ok(Arc::new(object_store::memory::InMemory::new()))
    }
}
