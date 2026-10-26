//! Fresh safe-ReadIndex membership observations, independent of SQL images.
//! Catalog and group lifecycles must still prove complete inventory separately.
use super::*;
use crate::raft::fleet_catalog::FleetGroupAuthorization;
use crate::raft::member_state::{
    FleetMembershipStatus, GroupMembership, JoiningMember, RepairStatus,
};

// Includes the bounded membership ledger, its fleet status, all four voter/
// learner sets and reply retention. Acquired before reading/cloning the ledger.
const OBSERVATION_BYTES: usize = 64 * 1024;

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

    fn capture(conf: &::raft::prelude::ConfState) -> Result<Self, CommandFailure> {
        let ids = |values: &[u64]| {
            values
                .iter()
                .copied()
                .map(|id| MemberId::new(id).map_err(|_| CommandFailure::CorruptState))
                .collect::<Result<Vec<_>, _>>()
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

/// An opaque observation minted after this invocation's safe ReadIndex and
/// local durable apply. It binds the physical process, exact history and full
/// configuration. Retaining it supplies no lease, complete inventory, archive
/// coverage or permission to remove a node or release a reservation.
#[derive(Clone, Debug)]
pub struct FleetMembershipObservation {
    replica: ReplicaIdentity,
    birth_digest: [u8; 32],
    schema_digest: [u8; 32],
    barrier: LogIndex,
    applied: LogPosition,
    membership: GroupMembership,
    configuration: FleetMembershipConfiguration,
    status: FleetMembershipStatus,
}

impl FleetMembershipObservation {
    pub(crate) fn matches_snapshot(
        &self,
        snapshot: &crate::raft::inspection::InspectionSnapshot,
    ) -> bool {
        snapshot.membership.as_ref() == Some(&self.membership)
            && snapshot.configuration.as_ref().is_some_and(|conf| {
                FleetMembershipConfiguration::capture(conf).as_ref() == Ok(&self.configuration)
            })
            && snapshot
                .view
                .member
                .as_ref()
                .is_some_and(|m| m.applied.index >= self.barrier)
    }

    // Only the purpose-specific authenticated quorum RPC calls this decoder.
    // An ordinary inspection snapshot alone cannot establish a read barrier.
    pub(crate) fn from_quorum_snapshot(
        snapshot: &crate::raft::inspection::InspectionSnapshot,
        authorization: &FleetGroupAuthorization,
        barrier: LogIndex,
    ) -> Result<Self, CommandFailure> {
        snapshot.validate()?;
        let view = &snapshot.view;
        let member = view.member.as_ref().ok_or(CommandFailure::Unavailable)?;
        let membership = snapshot
            .membership
            .as_ref()
            .ok_or(CommandFailure::Unavailable)?;
        let configuration = snapshot
            .configuration
            .as_ref()
            .ok_or(CommandFailure::Unavailable)?;
        let status = membership
            .fleet_status()
            .ok_or(CommandFailure::UnsupportedCapability)?;
        if member.applied.index < barrier
            || barrier.get() == 0
            || &status.binding != authorization.binding()
            || !authorization.matches_scope(&view.scope)
            || authorization.history_digests() != (view.birth_digest, view.schema_digest)
        {
            return Err(CommandFailure::CommandConflict);
        }
        let replica = ReplicaIdentity {
            scope: view.scope.clone(),
            member: member.member,
            node: view.node.identity.node,
            disk: view.node.identity.disk,
            process: view.node.identity.process,
        };
        if !authorization.matches_replica(&replica, membership) {
            return Err(CommandFailure::CommandConflict);
        }
        Ok(Self {
            replica,
            birth_digest: view.birth_digest,
            schema_digest: view.schema_digest,
            barrier,
            applied: member.applied,
            membership: membership.clone(),
            configuration: FleetMembershipConfiguration::capture(configuration)?,
            status,
        })
    }

    pub fn replica(&self) -> &ReplicaIdentity {
        &self.replica
    }

    pub fn birth_digest(&self) -> [u8; 32] {
        self.birth_digest
    }

    pub fn schema_digest(&self) -> [u8; 32] {
        self.schema_digest
    }

    pub fn barrier(&self) -> LogIndex {
        self.barrier
    }

    pub fn applied(&self) -> LogPosition {
        self.applied
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

    /// Only dynamically joined bindings have a committed disk here. Original
    /// members retain their immutable birth identity and separate engine pins.
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

    fn capture(
        cell: &Cell,
        authorization: &FleetGroupAuthorization,
        barrier: LogIndex,
    ) -> Result<Option<Self>, CommandFailure> {
        let cache = cell
            .group
            .read()
            .map_err(|_| CommandFailure::CorruptState)?;
        if cache.applied.position.index < barrier
            || cell.route_revision < cache.membership.revision()
        {
            return Ok(None);
        }
        cache
            .membership
            .validate(&cache.conf_state, cache.applied.position.index.get())
            .and_then(|()| cache.membership.validate_scope(cell.birth.scope()))
            .map_err(|_| CommandFailure::CorruptState)?;
        let status = cache
            .membership
            .fleet_status()
            .ok_or(CommandFailure::UnsupportedCapability)?;
        if &status.binding != authorization.binding()
            || !authorization.matches_birth(&cell.birth, cell.schema_digest)
        {
            return Err(CommandFailure::CommandConflict);
        }
        Ok(Some(Self {
            replica: cell.group.identity().clone(),
            birth_digest: cell.birth.digest(),
            schema_digest: cell.schema_digest,
            barrier,
            applied: cache.applied.position,
            membership: cache.membership.clone(),
            configuration: FleetMembershipConfiguration::capture(&cache.conf_state)?,
            status,
        }))
    }
}

impl Driver {
    pub(super) fn start_fleet_observation(
        &mut self,
        mut request: Request,
        authorization: FleetGroupAuthorization,
    ) {
        let valid = self.cells.get(&request.scope).is_some_and(|cell| {
            !cell.failed
                && !cell.retired
                && authorization.matches_scope(&request.scope)
                && authorization.matches_birth(&cell.birth, cell.schema_digest)
        });
        if !valid {
            reply(&mut request, Err(CommandFailure::CommandConflict));
            return;
        }
        let Some(charge) = self.control_budget.acquire(OBSERVATION_BYTES) else {
            reply(&mut request, Err(CommandFailure::Overloaded));
            return;
        };
        let Some(id) = self.next_read_id() else {
            reply(&mut request, Err(CommandFailure::Overloaded));
            return;
        };
        self.reads
            .insert(id, PendingRead::fleet(request, authorization, charge));
    }

    pub(super) fn finish_fleet_observation(&mut self, id: ReadId) {
        use crate::raft::query::ReadOperation;
        let outcome = (|| {
            let read = self.reads.get(&id).ok_or(CommandFailure::CorruptState)?;
            let ReadOperation::Fleet { authorization, .. } = &read.operation else {
                return Err(CommandFailure::CorruptState);
            };
            let Some(Minimum::ReadIndex(barrier)) = read.minimum.as_ref() else {
                return Err(CommandFailure::CorruptState);
            };
            let cell = self
                .cells
                .get(&read.request.scope)
                .ok_or(CommandFailure::Unavailable)?;
            FleetMembershipObservation::capture(cell, authorization, *barrier)
        })();
        if matches!(outcome, Ok(None)) {
            return;
        }
        let Some(mut read) = self.reads.remove(&id) else {
            return;
        };
        let ReadOperation::Fleet { charge, .. } = read.operation else {
            reply(&mut read.request, Err(CommandFailure::CorruptState));
            return;
        };
        let result = if read.request.control.cancelled() {
            Err(CommandFailure::VersionUnavailable)
        } else {
            match outcome {
                Ok(Some(observation)) => Ok(Response::FleetObservation(Box::new(observation))),
                Err(error) => Err(error),
                Ok(None) => Err(CommandFailure::CorruptState),
            }
        };
        reply_charged(&mut read.request, result, Some(charge));
    }
}
