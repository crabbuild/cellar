// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A complete authenticated descriptor must carry the exact consumed fleet
//! repair permit. These metadata checks mint no transport or catalog authority.
use super::*;
use crate::raft::member_state::{FleetMembershipPermit, FleetPermitPhase, RepairPhase};

#[cfg(test)]
#[path = "seed_tests.rs"]
mod tests;

pub(super) fn check_membership(
    membership: &GroupMembership,
    permit: Option<&FleetMembershipPermit>,
) -> Result<()> {
    match (membership.fleet_status(), permit) {
        (None, None) => Ok(()),
        (Some(status), Some(permit)) => {
            let repair = membership
                .repair_status()?
                .ok_or(CellGroupCheckpointError::Invalid(
                    "seed has no managed repair",
                ))?;
            if &status.binding != permit.binding()
                || status.consumed_sequence != permit.sequence()
                || status.permit.as_ref() != Some(permit)
                || status.phase != Some(FleetPermitPhase::Seeding)
                || repair.phase != RepairPhase::Seeding
            {
                return Err(CellGroupCheckpointError::Invalid("seed fleet permit"));
            }
            permit.check_plan(&repair.plan)?;
            Ok(())
        }
        _ => Err(CellGroupCheckpointError::Invalid("seed managed profile")),
    }
}

pub(crate) fn seed_profile(
    snapshot: &raft::prelude::Snapshot,
    permit: Option<&FleetMembershipPermit>,
) -> Result<()> {
    let header = Header::decode(&snapshot.data)?;
    if header.snapshot()? != *snapshot {
        return Err(CellGroupCheckpointError::Invalid("seed snapshot encoding"));
    }
    check_membership(&header.membership, permit)
}

pub(crate) fn seed_initial_membership(
    snapshot: &raft::prelude::Snapshot,
    conf: &raft::prelude::ConfState,
) -> Result<GroupMembership> {
    let header = Header::decode(&snapshot.data)?;
    Ok(header.membership.seed_initial(conf)?)
}

/// The responding process must also be a current, physically bound voter in
/// the authenticated checkpoint, independently of the joining target.
pub(crate) fn seed_source(
    snapshot: &raft::prelude::Snapshot,
    source: &cellar_logic::consensus::ReplicaIdentity<cellar_logic::cell_group::GroupHistory>,
) -> Result<()> {
    let header = Header::decode(&snapshot.data)?;
    let conf = header.configuration()?;
    if header.root.route().history != source.scope
        || !header.membership.permits_group(source)
        || !conf
            .voters
            .iter()
            .chain(&conf.voters_outgoing)
            .any(|id| *id == source.member.get())
    {
        return Err(CellGroupCheckpointError::Invalid("seed source member"));
    }
    Ok(())
}
