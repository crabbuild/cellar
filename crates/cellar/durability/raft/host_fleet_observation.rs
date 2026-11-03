// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Fresh physical-group metadata. No application cell is selected or aliased.
use crate::raft::{
    cell_groups::GroupManifest,
    fleet_catalog::FleetGroupAuthorization,
    member_state::{FleetMembershipStatus, GroupMembership, JoiningMember, RepairStatus},
};
use cellar_logic::{
    cell_group::GroupHistory,
    consensus::{CommandFailure, LogIndex, LogPosition, MemberId, ReplicaIdentity},
};

/// The exact configuration at a quorum-backed membership observation. Lists
/// retain joint-consensus roles; outgoing voters are never silently omitted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetMembershipConfiguration {
    voters: Vec<MemberId>,
    outgoing_voters: Vec<MemberId>,
    learners: Vec<MemberId>,
    next_learners: Vec<MemberId>,
    auto_leave: bool,
}

impl FleetMembershipConfiguration {
    pub fn voters(&self) -> &[MemberId] {
        &self.voters
    }

    pub fn outgoing_voters(&self) -> &[MemberId] {
        &self.outgoing_voters
    }

    pub fn learners(&self) -> &[MemberId] {
        &self.learners
    }

    pub fn next_learners(&self) -> &[MemberId] {
        &self.next_learners
    }

    pub fn auto_leave(&self) -> bool {
        self.auto_leave
    }

    pub(crate) fn capture(conf: &::raft::prelude::ConfState) -> Result<Self, CommandFailure> {
        let ids = |values: &[u64]| {
            let mut ids = values
                .iter()
                .copied()
                .map(|id| MemberId::new(id).map_err(|_| CommandFailure::CorruptState))
                .collect::<Result<Vec<_>, _>>()?;
            ids.sort_unstable();
            Ok::<_, CommandFailure>(ids)
        };
        Ok(Self {
            voters: ids(&conf.voters)?,
            outgoing_voters: ids(&conf.voters_outgoing)?,
            learners: ids(&conf.learners)?,
            next_learners: ids(&conf.learners_next)?,
            auto_leave: conf.auto_leave,
        })
    }
}

/// A fresh safe-ReadIndex observation of the complete durable group root and
/// configuration. Its process/history bindings grant no current health, lease,
/// placement, archive coverage, or permission to release physical debt.
#[derive(Clone, Debug)]
pub struct FleetMembershipObservation {
    replica: ReplicaIdentity<GroupHistory>,
    root: GroupManifest,
    barrier: LogPosition,
    membership: GroupMembership,
    configuration: FleetMembershipConfiguration,
    status: FleetMembershipStatus,
    configuration_digest: [u8; 32],
}

impl FleetMembershipObservation {
    // Only the authenticated quorum RPC (or the same local owner) invokes this
    // constructor. Decoded status inspection cannot mint a quorum observation.
    pub(crate) fn check_quorum_group(
        snapshot: &crate::raft::fleet_inspection_wire::GroupSnapshot,
        authorization: &FleetGroupAuthorization,
    ) -> Result<(), CommandFailure> {
        snapshot
            .validate()
            .map_err(|_| CommandFailure::CorruptState)?;
        let route = snapshot.root.route();
        let status = snapshot
            .membership
            .fleet_status()
            .ok_or(CommandFailure::UnsupportedCapability)?;
        if &status.binding != authorization.binding()
            || !authorization.matches_scope(&route.history)
            || authorization.history_digests() != (route.birth_digest, route.schema_digest)
            || !authorization.matches_replica(&snapshot.replica, &snapshot.membership)
        {
            return Err(CommandFailure::CommandConflict);
        }
        Ok(())
    }

    pub(crate) fn from_quorum_group(
        snapshot: crate::raft::fleet_inspection_wire::GroupSnapshot,
        authorization: &FleetGroupAuthorization,
    ) -> Result<Self, CommandFailure> {
        Self::check_quorum_group(&snapshot, authorization)?;
        let status = snapshot
            .membership
            .fleet_status()
            .ok_or(CommandFailure::UnsupportedCapability)?;
        let configuration = FleetMembershipConfiguration::capture(&snapshot.configuration)?;
        let configuration_digest =
            group_membership_digest(&snapshot.membership, &snapshot.configuration)?;
        Ok(Self {
            replica: snapshot.replica,
            root: snapshot.root,
            barrier: snapshot.barrier,
            membership: snapshot.membership,
            configuration,
            status,
            configuration_digest,
        })
    }

    pub fn replica(&self) -> &ReplicaIdentity<GroupHistory> {
        &self.replica
    }

    pub fn root(&self) -> &GroupManifest {
        &self.root
    }

    pub fn birth_digest(&self) -> [u8; 32] {
        self.root.route().birth_digest
    }

    pub fn schema_digest(&self) -> [u8; 32] {
        self.root.route().schema_digest
    }

    pub fn barrier(&self) -> LogIndex {
        self.barrier.index
    }

    pub fn applied(&self) -> LogPosition {
        self.root.position()
    }

    pub(crate) fn member_high_water(&self) -> u64 {
        self.membership.high_water()
    }

    pub fn membership_revision(&self) -> LogIndex {
        self.membership.revision()
    }

    pub fn configuration(&self) -> &FleetMembershipConfiguration {
        &self.configuration
    }

    pub fn status(&self) -> &FleetMembershipStatus {
        &self.status
    }

    pub fn configuration_digest(&self) -> [u8; 32] {
        self.configuration_digest
    }

    pub fn joined_member(
        &self,
        member: MemberId,
    ) -> Result<Option<(JoiningMember, LogIndex)>, CommandFailure> {
        self.membership
            .joined(member)
            .map_err(|_| CommandFailure::CorruptState)
    }

    pub fn repair_status(&self) -> Result<Option<RepairStatus>, CommandFailure> {
        self.membership
            .repair_status()
            .map_err(|_| CommandFailure::CorruptState)
    }
}

pub(crate) fn group_membership_digest(
    membership: &GroupMembership,
    configuration: &::raft::prelude::ConfState,
) -> Result<[u8; 32], CommandFailure> {
    use sha2::{Digest, Sha256};
    let ledger = serde_json::to_vec(membership).map_err(|_| CommandFailure::CorruptState)?;
    let conf = crate::raft::manifest::encode_conf(configuration)
        .map_err(|_| CommandFailure::CorruptState)?;
    let mut digest = Sha256::new();
    digest.update(b"cellar-fleet-group-membership-1\0");
    digest.update((ledger.len() as u64).to_be_bytes());
    digest.update(ledger);
    digest.update((conf.len() as u64).to_be_bytes());
    digest.update(conf);
    Ok(digest.finalize().into())
}
