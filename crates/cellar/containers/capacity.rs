// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Atomic local admission, independent of application liveness observations.
use super::{instance_memory_bytes, network, CellContainer, CellState, ContainerEngine};
use anyhow::{ensure, Result};

/// Hard limits for this host's application containers and their network helpers.
/// Size these for the actual Engine/VM allocation, leaving room for the daemon,
/// host, image loading and the shared firewall helper. Fleet samples are advisory.
#[derive(Clone, Copy, Debug)]
pub struct CapacityLimits {
    max_instances: u64,
    max_memory_bytes: u64,
}

impl CapacityLimits {
    pub fn new(max_instances: u64, max_memory_bytes: u64) -> Result<Self> {
        ensure!(
            max_instances > 0 && max_memory_bytes > 0,
            "container capacity must be nonzero"
        );
        Ok(Self {
            max_instances,
            max_memory_bytes,
        })
    }

    pub fn max_instances(self) -> u64 {
        self.max_instances
    }

    pub fn max_memory_bytes(self) -> u64 {
        self.max_memory_bytes
    }
}

/// Proven refusal before this start submitted any Engine effects. Retrying later
/// is safe; this says nothing about mutations from an earlier invocation.
#[derive(Debug)]
pub struct CapacityExceeded;

impl std::fmt::Display for CapacityExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("container capacity exhausted")
    }
}

impl std::error::Error for CapacityExceeded {}

impl CellState {
    pub(super) fn reserves_instance(&self) -> bool {
        self.capacity_reserved || self.network.is_some()
    }

    /// A refusal/cancellation before create may release its admission. Once an
    /// effect was entered, retain the reservation until positive cleanup proof.
    pub(super) fn release_unused_capacity(&mut self) {
        if !self.creation_submitted && self.resource.is_none() && self.network.is_none() {
            self.capacity_reserved = false;
        }
    }
}

impl ContainerEngine {
    pub(super) fn reserved_bytes(&self, cell: &CellContainer, state: &CellState) -> u64 {
        u64::from(state.capacity_reserved)
            * instance_memory_bytes(cell.spec.instance_type.as_deref())
            + u64::from(
                state.network.is_some()
                    || (state.capacity_reserved && self.network_helper.is_some()),
            ) * network::MEMORY_BYTES
    }

    /// The registry lock serializes admission across different execution cells.
    /// No I/O or waits occur while it is held. SDK control serializes each cell.
    pub(super) fn admit_start(&self, cell: &CellContainer, run: u64) -> Result<()> {
        let cells = self.cells.lock().unwrap();
        let mut instances = 1u64;
        let mut class_instances = 1u64;
        let mut bytes = instance_memory_bytes(cell.spec.instance_type.as_deref())
            + u64::from(self.network_helper.is_some()) * network::MEMORY_BYTES;
        for other in cells
            .values()
            .filter(|other| !std::ptr::eq(other.as_ref(), cell))
        {
            let state = other.state.lock().unwrap();
            instances = instances.saturating_add(u64::from(state.reserves_instance()));
            if other.spec.class_name == cell.spec.class_name {
                class_instances =
                    class_instances.saturating_add(u64::from(state.reserves_instance()));
            }
            bytes = bytes.saturating_add(self.reserved_bytes(other, &state));
        }
        ensure!(
            cell.spec
                .max_instances
                .is_none_or(|max| class_instances <= max),
            CapacityExceeded
        );
        ensure!(
            self.capacity.is_none_or(
                |limit| instances <= limit.max_instances && bytes <= limit.max_memory_bytes
            ),
            CapacityExceeded
        );
        let mut state = cell.state.lock().unwrap();
        ensure!(
            state.run == run,
            "container admission belongs to a superseded run"
        );
        state.capacity_reserved = true;
        Ok(())
    }
}

#[cfg(test)]
#[path = "capacity_tests.rs"]
mod tests;
