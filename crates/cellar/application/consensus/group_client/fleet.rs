// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fleet reads are metadata-only and require the exact rooted group admission.
//! The receiver independently reauthorizes it; node transport trust is not enough.
use super::*;
use crate::raft::{
    fleet_catalog::FleetGroupAuthorization,
    fleet_inspection_wire::{
        GroupSnapshot, History, LocalObservation, RaftPeerFleetMembershipObservation,
        RESPONSE_BYTES,
    },
    inspection::InspectionError,
};
use cellar_logic::consensus::{CommandFailure, NodeId};

impl RaftCells {
    /// Fresh complete-group membership/publication from an exact enrolled boot.
    /// Followers use safe ReadIndex forwarding and their own durable apply;
    /// this grants no leader, seed, mutation, archive or physical-release right.
    pub fn observe_peer_fleet_membership(
        &self,
        node: NodeId,
        authorization: FleetGroupAuthorization,
        minimum_revision: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<RaftPeerFleetMembershipObservation, InspectionError>> + Send + 'static
    {
        self.observe_peer_fleet_mode(node, authorization, minimum_revision, timeout, false)
    }

    /// Read the actual leader's bounded partition counters and geometry through
    /// the existing rooted group inspection channel. This is advisory telemetry.
    pub fn observe_peer_fleet_partition(
        &self,
        node: NodeId,
        authorization: FleetGroupAuthorization,
        minimum_revision: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<RaftPeerFleetMembershipObservation, InspectionError>> + Send + 'static
    {
        self.observe_peer_fleet_mode(node, authorization, minimum_revision, timeout, true)
    }

    fn observe_peer_fleet_mode(
        &self,
        node: NodeId,
        authorization: FleetGroupAuthorization,
        minimum_revision: u64,
        timeout: Duration,
        partition: bool,
    ) -> impl Future<Output = Result<RaftPeerFleetMembershipObservation, InspectionError>> + Send + 'static
    {
        let admitted = (|| {
            if minimum_revision == 0
                || minimum_revision >= u64::MAX - 2
                || self.handle.core.managed.is_none()
            {
                return Err(InspectionError::Refused);
            }
            let local = node == self.handle.core.node.node;
            let (deadline, memory) = self
                .metadata_admission(
                    timeout,
                    if local {
                        crate::raft::physical_inventory::RESPONSE_BYTES
                    } else {
                        RESPONSE_BYTES
                    },
                )
                .map_err(inspection_error)?;
            let started = deadline
                .checked_sub(timeout.as_millis() as u64)
                .ok_or(InspectionError::Unavailable)?;
            let remote = if local {
                None
            } else {
                Some(if partition {
                    self.handle.core.transport.inspect_fleet_partition(
                        node,
                        &authorization,
                        minimum_revision,
                        deadline,
                    )?
                } else {
                    self.handle.core.transport.inspect_fleet_membership(
                        node,
                        &authorization,
                        minimum_revision,
                        deadline,
                    )?
                })
            };
            Ok((deadline, started, memory, remote))
        })();
        let owner = self.internal_observer();
        async move {
            let (deadline, started, _memory, remote) = admitted?;
            owner.metadata_live(deadline).map_err(inspection_error)?;
            let local = if let Some(delivery) = remote {
                delivery.receive().await?.observation
            } else {
                owner
                    .local_fleet_at(
                        if partition {
                            History::for_partition(&authorization)
                        } else {
                            History::new(&authorization)
                        },
                        minimum_revision,
                        deadline,
                    )
                    .await
                    .map_err(inspection_error)?
            };
            owner.metadata_live(deadline).map_err(inspection_error)?;
            if partition && local.snapshot.partition.is_none() {
                return Err(InspectionError::Refused);
            }
            owner
                .handle
                .core
                .transport
                .validate_fleet_node_observation(&local.node, minimum_revision)?;
            let age = owner
                .handle
                .domain
                .mono_ms()
                .ok()
                .and_then(|now| now.checked_sub(started))
                .ok_or(InspectionError::Unavailable)?;
            local
                .into_peer(&authorization, age)
                .map_err(|_| InspectionError::Refused)
        }
    }

    pub(crate) fn observe_local_fleet_membership(
        &self,
        history: History,
        minimum_revision: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<LocalObservation, CommandFailure>> + Send + 'static {
        let admitted =
            self.metadata_admission(timeout, crate::raft::physical_inventory::RESPONSE_BYTES);
        let owner = self.internal_observer();
        async move {
            let (deadline, _memory) = admitted?;
            owner
                .local_fleet_at(history, minimum_revision, deadline)
                .await
        }
    }

    async fn local_fleet_at(
        &self,
        history: History,
        minimum_revision: u64,
        deadline: u64,
    ) -> Result<LocalObservation, CommandFailure> {
        self.metadata_live(deadline)?;
        if minimum_revision == 0 || minimum_revision >= u64::MAX - 2 {
            return Err(CommandFailure::CommandConflict);
        }
        let scope = history
            .scope()
            .map_err(|_| CommandFailure::CommandConflict)?;
        let reader = self
            .handle
            .core
            .fleet_inspection
            .as_ref()
            .ok_or(CommandFailure::UnsupportedCapability)?;
        let remaining = deadline
            .checked_sub(
                self.handle
                    .domain
                    .mono_ms()
                    .map_err(|_| CommandFailure::Unavailable)?,
            )
            .filter(|n| *n > 0)
            .map(Duration::from_millis)
            .ok_or(CommandFailure::VersionUnavailable)?;
        let authorization = if history.requests_partition() {
            reader
                .authorize_group_at_least(&scope, minimum_revision, remaining)
                .await
        } else {
            let (_, _routing_memory) = self.metadata_admission(
                remaining,
                crate::raft::fleet_catalog::FleetTrustReader::RESERVATION_BYTES,
            )?;
            let directory = self
                .handle
                .core
                .host
                .namespace_directory(scope.cluster, scope.namespace)
                .map_err(|_| CommandFailure::Unavailable)?;
            reader
                .authorize_router_observation_at_least(
                    &directory,
                    &scope,
                    minimum_revision,
                    remaining,
                )
                .await
        }
        .map_err(|error| match error {
            crate::raft::fleet_catalog::FleetCatalogError::Overloaded
            | crate::raft::fleet_catalog::FleetCatalogError::Limit => CommandFailure::Overloaded,
            _ => CommandFailure::Unavailable,
        })?;
        if !history.matches(&authorization) {
            return Err(CommandFailure::CommandConflict);
        }
        let selected = self
            .handle
            .core
            .runtime
            .quorum_membership(scope, deadline)
            .await
            .map_err(|error| match error {
                crate::raft::completion::RuntimeError::Overloaded => CommandFailure::Overloaded,
                _ => CommandFailure::Unavailable,
            })?;
        let mut snapshot = GroupSnapshot::capture(&selected);
        if history.requests_partition() {
            let remaining = deadline
                .checked_sub(
                    self.handle
                        .domain
                        .mono_ms()
                        .map_err(|_| CommandFailure::Unavailable)?,
                )
                .filter(|n| *n > 0)
                .map(Duration::from_millis)
                .ok_or(CommandFailure::Unavailable)?;
            let partition = self
                .group(scope)
                .map_err(|_| CommandFailure::Unavailable)?
                .observe_partition(remaining)
                .await
                .map_err(|_| CommandFailure::Unavailable)?;
            if !partition.stable_leader {
                return Err(CommandFailure::Unavailable);
            }
            snapshot.partition = Some(Box::new(partition));
        }
        // This descriptor contains no image/page pins. Release the temporary
        // metadata read reservation before sampling unrelated physical work.
        drop(selected);
        let load = match self.handle.core.runtime.leadership_sample(deadline).await {
            Ok(sample) => Some(sample),
            Err(
                crate::raft::completion::RuntimeError::NotReady
                | crate::raft::completion::RuntimeError::GroupQuarantined
                | crate::raft::completion::RuntimeError::Overloaded
                | crate::raft::completion::RuntimeError::DeadlineExceeded,
            ) => None,
            Err(_) => return Err(CommandFailure::Unavailable),
        };
        let (node, _) = self.capture_physical_node()?;
        // An unattached or initializing physical history cannot supply known
        // headroom. Count complete groups, independently of manifest cell count.
        if let Some(sample) = load.filter(|sample| {
            sample.groups == node.node.groups as usize
                && node.pending_history_registrations == 0
                && node.unattached_engine_histories == 0
        }) {
            snapshot.load = Some(crate::raft::inspection::LeaderLoad {
                leaders: sample.leaders,
                capacity: node.node.profile.leader_capacity,
                sample_age_ms: self
                    .handle
                    .domain
                    .mono_ms()
                    .map_err(|_| CommandFailure::Unavailable)?
                    .checked_sub(sample.started_at_ms)
                    .ok_or(CommandFailure::Unavailable)?,
            });
        }
        self.handle
            .core
            .transport
            .validate_fleet_node_observation(&node, minimum_revision)
            .map_err(|_| CommandFailure::Unavailable)?;
        self.metadata_live(deadline)?;
        let local = LocalObservation {
            node: Box::new(node),
            snapshot,
        };
        local.validate().map_err(|_| CommandFailure::CorruptState)?;
        // The same constructor checks binding, immutable birth/schema and the
        // actual local replica before anything reaches the transport encoder.
        crate::raft::host::FleetMembershipObservation::check_quorum_group(
            &local.snapshot,
            &authorization,
        )?;
        Ok(local)
    }
}

fn inspection_error(error: CommandFailure) -> InspectionError {
    match error {
        CommandFailure::Overloaded => InspectionError::Overloaded,
        CommandFailure::CommandConflict | CommandFailure::UnsupportedCapability => {
            InspectionError::Refused
        }
        _ => InspectionError::Unavailable,
    }
}
