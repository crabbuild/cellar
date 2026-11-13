// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared managed fixtures for the lifecycle and public request targets.

use anyhow::{ensure, Context as _, Result};
use cellar::application::consensus::verification::cell_groups::RangeDirectory;
use cellar::bucket::{Bucket, StorageBackend};
use cellar::sdk::{consensus::*, *};
use std::{
    path::Path,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

#[path = "cell_group_archive_verifier.rs"]
mod archive_verifier;
#[path = "cell_group_managed_fixture_archive.rs"]
pub mod archives;
#[path = "cell_group_managed_fixture_cluster.rs"]
mod cluster;
#[path = "cell_group_managed_diagnostics.rs"]
pub mod diagnostics;
pub use crate::catalog_fault as fault;
#[path = "html.rs"]
pub mod graph;
#[path = "cell_group_managed_http.rs"]
pub mod http;
#[path = "cell_group_managed_fixture_maintenance.rs"]
pub mod maintenance;
#[path = "cell_group_managed_fixture_movement.rs"]
pub mod movement;
#[path = "cell_group_managed_fixture_node.rs"]
mod node;
#[path = "cell_group_managed_fixture_operator.rs"]
pub mod operator;
#[path = "cell_group_managed_requests.rs"]
pub mod requests;
#[path = "cell_group_managed_fixture_seed_failure.rs"]
pub mod seed_failures;
#[path = "cell_group_managed_fixture_verifier.rs"]
pub mod verifier;

pub use cluster::*;
pub use node::{limits, request, start_node, start_node_with_filesystem, Fixture, NoListing};

pub const TIMEOUT: Duration = Duration::from_secs(5);
