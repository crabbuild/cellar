// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Targeted partition telemetry for an adjacent range with no local replica.
use super::*;
use crate::application::consensus::CellGroupPartitionObservation;
use crate::raft::inspection::InspectionError;
use cellar_logic::cell_group::GroupHistory;

pub(super) async fn remote(
    control: &RaftFleetControl,
    history: GroupHistory,
    cached: Option<NodeId>,
    turn: &Turn<'_>,
) -> Result<(CellGroupPartitionObservation, NodeId)> {
    let observer = control
        .node_observer
        .as_ref()
        .ok_or_else(RaftFleetFailure::unsupported)?;
    let snapshot = control.catalog.read(turn.remaining()?).await?;
    let record = control
        .catalog
        .history(&snapshot, &history, turn.remaining()?)
        .await?;
    let voters = control
        .catalog
        .partition_voters(&snapshot, &record, turn.remaining()?)
        .await?;
    let authorization = control
        .catalog
        .authorize_group(&snapshot, &history, turn.remaining()?)
        .await?;
    // A cache only selects the first target. The current root and receiving
    // leader independently revalidate the history and actual membership.
    if let Some(node) = cached.filter(|node| voters.contains_key(node)) {
        match observer
            .observe_peer_fleet_partition(
                node,
                authorization.clone(),
                snapshot.revision(),
                turn.remaining()?.min(Duration::from_millis(500)),
            )
            .await
        {
            Ok(sample) => {
                if let Some(current) = sample.partition().filter(|p| p.stable_leader) {
                    return Ok((current.clone(), node));
                }
            }
            Err(InspectionError::Overloaded) => return Err(FleetCatalogError::Overloaded.into()),
            Err(_) => (),
        }
    }
    // At most three settled voters are queried. The follower's authenticated
    // ReadIndex result locates the actual leader, including joined members.
    for &node in voters.keys() {
        let sample = match observer
            .observe_peer_fleet_membership(
                node,
                authorization.clone(),
                snapshot.revision(),
                turn.remaining()?.min(Duration::from_millis(500)),
            )
            .await
        {
            Ok(sample) => sample,
            Err(InspectionError::Overloaded) => return Err(FleetCatalogError::Overloaded.into()),
            Err(_) => continue,
        };
        let Some(leader) = sample.progress().member.as_ref().and_then(|m| m.leader) else {
            continue;
        };
        let node = authorization
            .router_voters(sample.membership())?
            .into_iter()
            .find(|voter| voter.member == leader)
            .ok_or(FleetCatalogError::Unconfirmed)?
            .node;
        let sample = observer
            .observe_peer_fleet_partition(
                node,
                authorization,
                snapshot.revision(),
                turn.remaining()?,
            )
            .await
            .map_err(|_| FleetCatalogError::Unconfirmed)?;
        let current = sample
            .partition()
            .filter(|current| current.stable_leader)
            .ok_or(FleetCatalogError::Unconfirmed)?;
        return Ok((current.clone(), node));
    }
    Err(FleetCatalogError::Unconfirmed.into())
}
