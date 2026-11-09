// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Completed local removal stops consensus independently of physical collection.
use super::*;
use crate::raft::{
    member_state::{FleetPermitPhase, RepairPhase},
    storage::LogError,
};
use cellar_logic::cell_group::GroupHistory;

pub(super) fn source(
    runtime: &CellGroupRuntime,
    history: GroupHistory,
) -> Result<Option<RetiredSource>, CellGroupControllerError> {
    let group = runtime
        .log()
        .cell_group(&history)
        .map_err(RuntimeError::from)?;
    let permit = {
        let cache = group.read().map_err(RuntimeError::from)?;
        let Some(status) = cache.membership.fleet_status() else {
            return Ok(None);
        };
        let Some(repair) = cache
            .membership
            .repair_status()
            .map_err(RuntimeError::from)?
        else {
            return Ok(None);
        };
        if status.phase != Some(FleetPermitPhase::Complete)
            || repair.phase != RepairPhase::Complete
            || repair.plan.obsolete != group.identity().member
            || cache
                .group_manifest
                .as_ref()
                .is_none_or(|marker| marker.source_seal().is_some())
        {
            return Ok(None);
        }
        status
            .permit
            .clone()
            .ok_or(CellGroupControllerError::Invalid(
                "completed member removal has no permit",
            ))?
    };
    // This verifies the exact synced image/position, completed Fleet permit,
    // obsolete local identity and full final RF3 configuration. Neither an
    // advisory route nor a merely admitted/joint move can stop this owner.
    let member =
        super::super::member_reclaim::Member::from_group(&group, &permit).map_err(|error| {
            match error {
                LogError::WrongIdentity => RuntimeError::NotReady,
                error => RuntimeError::from(error),
            }
        })?;
    RetiredSource::removed_member(runtime.log(), member)
        .map(Some)
        .map_err(Into::into)
}
