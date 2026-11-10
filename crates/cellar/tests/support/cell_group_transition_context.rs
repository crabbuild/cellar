// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Managed topology fixtures share the SDK and owned cluster used by public requests.
use super::*;
use crate::managed_fixture::{
    admit_capacity, checkpoint_limits, close_group_nodes, counter_class, inspect_current, leader,
    peer_fixture, seed_cells, ArchiveSetup, FirstGroupSetup, ManagedCluster, SeededCells,
};

#[path = "cell_group_managed_transition.rs"]
pub(crate) mod transition;
