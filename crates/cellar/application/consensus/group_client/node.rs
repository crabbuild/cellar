// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical metadata uses the shared engine/roster, independently of cell count.
//! Installed transport trust, not a separate root refresh, acknowledges routing.
use super::*;
use crate::raft::{
    host::{
        RaftHostEnrollmentObservation, RaftHostObservation, RaftHostStartupBounds,
        RaftPeerNodeObservation,
    },
    inspection::{InspectionError, NodeInspection, RESPONSE_RESERVATION},
    physical_inventory::{PhysicalNodeObservation, RESPONSE_BYTES},
};
use cellar_logic::consensus::{CommandFailure, NodeId};

impl RaftCells {
    /// A point-in-time local physical cut. No quorum, capacity activation,
    /// physical absence or release permission follows from retaining it.
    pub fn observe_node(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<RaftHostObservation, CommandFailure>> + Send + 'static {
        let admitted = self.metadata_admission(timeout, RESPONSE_BYTES);
        let owner = self.internal_observer();
        async move {
            let (deadline, _bytes) = admitted?;
            owner.metadata_live(deadline)?;
            let (view, _) = owner.capture_physical_node()?;
            owner.metadata_live(deadline)?;
            Ok(view)
        }
    }

    pub(crate) fn observe_physical_node(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<PhysicalNodeObservation, CommandFailure>> + Send + 'static
    {
        let admitted = self.metadata_admission(timeout, RESPONSE_BYTES);
        let owner = self.internal_observer();
        async move {
            let (deadline, _bytes) = admitted?;
            owner.metadata_live(deadline)?;
            let observed = owner.capture_physical_node()?;
            owner.metadata_live(deadline)?;
            Ok(observed)
        }
    }

    /// Inspect the exact enrolled process over the authenticated control lane.
    /// A local target uses the same installed-trust checks without loopback I/O.
    pub fn inspect_peer_node(
        &self,
        node: NodeId,
        minimum_revision: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<RaftPeerNodeObservation, InspectionError>> + Send + 'static
    {
        self.inspect_peer_node_mode(node, minimum_revision, timeout, false)
    }

    /// Include every durable engine slot, even if unattached or quarantined.
    /// An omitted inventory is never interpreted as an empty physical node.
    pub fn inspect_peer_physical_inventory(
        &self,
        node: NodeId,
        minimum_revision: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<RaftPeerNodeObservation, InspectionError>> + Send + 'static
    {
        self.inspect_peer_node_mode(node, minimum_revision, timeout, true)
    }

    fn inspect_peer_node_mode(
        &self,
        node: NodeId,
        minimum_revision: u64,
        timeout: Duration,
        physical: bool,
    ) -> impl Future<Output = Result<RaftPeerNodeObservation, InspectionError>> + Send + 'static
    {
        // Starting the remote transport at invocation preserves its original
        // deadline even if the response future is retained without polling.
        let admitted = (|| {
            if minimum_revision == 0
                || minimum_revision >= u64::MAX - 2
                || timeout > Duration::from_secs(60)
                || self.handle.core.managed.is_none()
            {
                return Err(InspectionError::Refused);
            }
            let (deadline, memory) = self
                .metadata_admission(
                    timeout,
                    if physical || node == self.handle.core.node.node {
                        RESPONSE_BYTES
                    } else {
                        RESPONSE_RESERVATION
                    },
                )
                .map_err(inspection_error)?;
            let started = deadline
                .checked_sub(timeout.as_millis() as u64)
                .ok_or(InspectionError::Unavailable)?;
            let remote = if node != self.handle.core.node.node {
                Some(self.handle.core.transport.inspect_fleet_node(
                    node,
                    minimum_revision,
                    deadline,
                    physical,
                )?)
            } else {
                None
            };
            Ok((deadline, started, memory, remote))
        })();
        let owner = self.internal_observer();
        async move {
            let (deadline, started, _memory, remote) = admitted?;
            owner.metadata_live(deadline).map_err(inspection_error)?;
            let (view, inventory) = if let Some(delivery) = remote {
                let received = delivery.receive().await?;
                if received.inventory.is_some() != physical {
                    return Err(InspectionError::Refused);
                }
                (*received.view, received.inventory)
            } else {
                let (view, inventory) = owner.capture_physical_node().map_err(inspection_error)?;
                (view, physical.then(|| Box::new(inventory)))
            };
            owner.metadata_live(deadline).map_err(inspection_error)?;
            owner
                .handle
                .core
                .transport
                .validate_fleet_node_observation(&view, minimum_revision)?;
            let sample_age_ms = owner
                .handle
                .domain
                .mono_ms()
                .ok()
                .and_then(|now| now.checked_sub(started))
                .ok_or(InspectionError::Unavailable)?;
            Ok(RaftPeerNodeObservation {
                view,
                inventory,
                sample_age_ms,
            })
        }
    }

    pub(super) fn metadata_admission(
        &self,
        timeout: Duration,
        bytes: usize,
    ) -> Result<(u64, crate::raft::budget::Permit), CommandFailure> {
        if crate::application::consensus::class::callback_active() {
            return Err(CommandFailure::UnsupportedCapability);
        }
        let _gate = self
            .handle
            .admission
            .registration(super::super::admission::Access::Cleanup)?;
        if self.is_closed() {
            return Err(CommandFailure::Unavailable);
        }
        if timeout.as_millis() == 0 || timeout > Duration::from_secs(60) {
            return Err(CommandFailure::VersionUnavailable);
        }
        let deadline = self
            .handle
            .domain
            .mono_ms()
            .ok()
            .and_then(|now| now.checked_add(timeout.as_millis() as u64))
            .ok_or(CommandFailure::Unavailable)?;
        let memory = self
            .control_budget()
            .acquire(bytes)
            .ok_or(CommandFailure::Overloaded)?;
        Ok((deadline, memory))
    }

    pub(super) fn metadata_live(&self, deadline: u64) -> Result<(), CommandFailure> {
        if self.is_closed() {
            return Err(CommandFailure::Unavailable);
        }
        if !self.handle.domain.mono_ms().is_ok_and(|now| now < deadline) {
            return Err(CommandFailure::VersionUnavailable);
        }
        Ok(())
    }

    pub(super) fn capture_physical_node(&self) -> Result<PhysicalNodeObservation, CommandFailure> {
        let core = &self.handle.core;
        let roster = core
            .controller
            .registration_inventory()
            .map_err(|_| CommandFailure::Unavailable)?;
        let inventory = core
            .log
            .cell_group_inventory(None)
            .map_err(|_| CommandFailure::Unavailable)?;
        if roster
            .0
            .iter()
            .any(|history| inventory.group_history(history).is_none())
        {
            return Err(CommandFailure::Unavailable);
        }
        let usage = core.controller.usage();
        let mut profile = core
            .node_profile
            .clone()
            .ok_or(CommandFailure::UnsupportedCapability)?;
        profile.draining |= self.handle.admission.application_paused();
        let count = |n| u32::try_from(n).map_err(|_| CommandFailure::Overloaded);
        let groups = count(inventory.histories().len())?;
        let reserved_bytes = profile
            .bytes_per_group
            .checked_mul(u64::from(groups))
            .ok_or(CommandFailure::Overloaded)?;
        let current = core
            .managed
            .as_ref()
            .map(|startup| {
                let current = core
                    .transport
                    .installed_fleet_node(core.node.node)
                    .map_err(|_| CommandFailure::Unavailable)?;
                let boot = current
                    .boot_binding()
                    .map_err(|_| CommandFailure::CommandConflict)?;
                let expected = startup
                    .boot_binding()
                    .map_err(|_| CommandFailure::CommandConflict)?;
                if boot != expected
                    || boot.node != core.node.node.bytes()
                    || boot.disk != core.node.disk.bytes()
                    || boot.process != core.node.process.bytes()
                    || boot.engine != core.engine.bytes()
                    || current.disk() != core.node.disk
                    || current.storage_instance() != Some(core.engine)
                {
                    return Err(CommandFailure::CommandConflict);
                }
                Ok(RaftHostEnrollmentObservation::capture(&current))
            })
            .transpose()?;
        let transport = core
            .transport
            .resource_observer()
            .capture()
            .ok_or(CommandFailure::Unavailable)?;
        let seed_workers = transport
            .snapshot_source
            .items
            .checked_add(usage.active_checkpoints)
            .and_then(|n| n.checked_add(usage.retained_preparations))
            .and_then(|n| n.checked_add(core.runtime.usage().read_pins.items))
            .ok_or(CommandFailure::Overloaded)?;
        let view = RaftHostObservation {
            node: NodeInspection {
                identity: core.node,
                storage_instance: core.engine,
                profile,
                groups,
                reserved_bytes,
            },
            fleet_boot: core
                .log
                .lifecycle_guard()
                .map_err(|_| CommandFailure::Unavailable)?
                .map(|guard| guard.boot),
            startup_bounds: core.startup_bounds.clone(),
            startup_enrollment: core
                .managed
                .as_ref()
                .map(RaftHostEnrollmentObservation::capture),
            fleet_trust_revision: current.as_ref().map(|n| n.revision),
            current_enrollment: current,
            declared_namespaces: count(core.host.usage().namespaces)?,
            registered_histories: count(roster.0.len())?,
            dormant_histories: count(roster.2)?,
            pending_history_registrations: count(
                roster
                    .1
                    .checked_add(usage.provision_waiters)
                    .ok_or(CommandFailure::Overloaded)?,
            )?,
            unattached_engine_histories: count(
                inventory
                    .histories()
                    .len()
                    .checked_sub(roster.0.len())
                    .ok_or(CommandFailure::CorruptState)?,
            )?,
            seed_workers: count(seed_workers)?,
        };
        // The catalog is append-only for this engine. Refuse a changed slot
        // count or roster rather than combine unrelated physical cuts.
        if core
            .log
            .group_count()
            .map_err(|_| CommandFailure::Unavailable)?
            != inventory.histories().len()
            || core
                .controller
                .registration_inventory()
                .map_err(|_| CommandFailure::Unavailable)?
                != roster
        {
            return Err(CommandFailure::Unavailable);
        }
        if core.managed.is_some() {
            inventory
                .validate(&view)
                .map_err(|_| CommandFailure::CorruptState)?;
        }
        Ok((view, inventory))
    }
}

pub(super) fn startup_bounds(
    controller: &CellGroupController,
    runtime: &CellGroupRuntime,
    log: &ReplicaLog,
    options: &crate::raft::host::RaftHostOptions,
) -> Result<RaftHostStartupBounds, CellGroupControllerError> {
    let c = controller.limits();
    let r = runtime.limits();
    let l = log.limits();
    let bytes = [
        c.max_command_bytes,
        c.max_command_bytes,
        options.max_control_bytes,
        r.max_request_bytes,
        r.max_event_bytes,
        r.max_ready_bytes,
        r.max_apply_bytes,
        r.max_read_pin_bytes,
        l.max_pending_bytes,
        l.max_cache_bytes,
    ];
    let bounded_buffer_bytes = bytes
        .into_iter()
        .try_fold(0u64, |sum, n| sum.checked_add(u64::try_from(n).ok()?))
        .ok_or(CellGroupControllerError::Invalid("node buffer capacities"))?;
    let count =
        |n| u32::try_from(n).map_err(|_| CellGroupControllerError::Invalid("node group capacity"));
    Ok(RaftHostStartupBounds {
        host_groups: count(c.max_groups)?,
        consensus_groups: count(r.max_groups)?,
        engine_groups: count(l.max_groups)?,
        bounded_buffer_bytes,
    })
}

fn inspection_error(error: CommandFailure) -> InspectionError {
    match error {
        CommandFailure::Overloaded => InspectionError::Overloaded,
        CommandFailure::UnsupportedCapability | CommandFailure::CommandConflict => {
            InspectionError::Refused
        }
        _ => InspectionError::Unavailable,
    }
}
