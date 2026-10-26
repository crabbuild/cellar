// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fleet decisions over explicit, bounded inputs. These values grant no catalog,
//! membership, resource-release or platform authority. Adapters must authenticate
//! observations and atomically persist reservations before admitting work.

mod ledger;
mod topology;

pub use ledger::{
    CapacityScope, LedgerError, LedgerPolicy, LedgerSnapshot, Materialization, PhysicalSample,
    Reservation, ResourceCharge, ResourceKind, ResourceLedger, Resources, SampleAttribution,
};
pub use topology::{
    preview_topology, NodeRecovery, ReplicaMove, TopologyError, TopologyGroup, TopologyNode,
    TopologyPeak, TopologyPolicy, TopologyPreview, TopologyTarget, TopologyView,
};
