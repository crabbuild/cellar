// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Maintenance uses completed placement, including dynamic and retired copies.
use super::*;
use cellar::application::consensus::verification::{
    FleetRestartGateRequest, FleetRestartHistoryProgress, FleetRestartRequest,
};

async fn verified(dynamic: bool) -> Result<()> {
    let s = Box::pin(Setup::with_histories(true, 2, 1)).await?;
    let r = replace(&s, true).await?;
    let (cut, completion) = completion(&s, &r.request, 230).await?;
    s.f.catalog
        .complete_movement(&cut, &completion, &s.nodes[r.source].host, TIMEOUT)
        .await?;
    let retired = r.request.plan.obsolete.get() as usize - 1;
    let host = if dynamic {
        &s.target
    } else {
        &s.nodes[retired].host
    };
    let boot = host
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("target boot")?;
    let node = NodeId::new(boot.node)?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let preparing = FleetRestartRequest {
        identity: Fixture::request(235, cut.revision()),
        node,
        expected_process: ProcessGeneration::new(boot.process)?,
        grace_seconds: 120,
    };
    let admitted =
        s.f.catalog
            .begin_restart_maintenance(
                &cut,
                &preparing,
                std::slice::from_ref(&s.namespace),
                TIMEOUT,
            )
            .await?;
    let mut identity = Fixture::request(237, admitted.snapshot.revision());
    identity.operation = preparing.identity.operation;
    let gate = FleetRestartGateRequest {
        identity,
        node,
        expected_process: preparing.expected_process,
        gate: ProposalId::new([239; 16])?,
    };
    s.f.catalog
        .admit_restart_gate(&admitted.snapshot, &gate, TIMEOUT)
        .await?;
    discover(
        &s.f,
        &[
            &s.nodes[0].host,
            &s.nodes[1].host,
            &s.nodes[2].host,
            &s.target,
        ],
    )
    .await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let physical = s.nodes[r.source]
        .host
        .inspect_peer_physical_inventory(node, cut.revision(), TIMEOUT)
        .await?;
    let slot = physical
        .inventory
        .as_ref()
        .context("physical inventory")?
        .history(&r.request.resources.group)?
        .context("physical history")?;
    ensure!(slot.in_configuration() == dynamic);
    let mut walk =
        s.f.catalog
            .prepare_restart_verification(
                &cut,
                ProposalId::new(preparing.identity.operation)?,
                std::slice::from_ref(&s.namespace),
                &s.nodes[r.source].host,
                TIMEOUT,
            )
            .await?;
    let progress = walk.verify_next(TIMEOUT).await?.context("history")?;
    if dynamic {
        let FleetRestartHistoryProgress::Voting {
            member, survivors, ..
        } = progress
        else {
            anyhow::bail!("dynamic voter skipped");
        };
        ensure!(member == r.request.plan.replacement.member && !survivors.contains(&node));
    } else {
        let FleetRestartHistoryProgress::Retained { member, .. } = progress else {
            anyhow::bail!("retired copy counted as voter or absent");
        };
        ensure!(member == r.request.plan.obsolete);
    }
    let candidate = walk.finish(TIMEOUT).await?;
    ensure!(candidate.histories() == 1 && candidate.physical_histories() == 1);
    ensure!(
        candidate.voting_histories() == u64::from(dynamic)
            && candidate.retained_histories() == u64::from(!dynamic)
    );
    write_read(
        &s.f,
        &[
            r.cells[r.source].clone(),
            r.cells[(r.source + 2) % 3].clone(),
            r.target,
        ],
        3,
        9,
        21,
    )
    .await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_runtime_retired_copy_stays_accounted_without_votes() -> Result<()> {
    verified(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_restart_runtime_dynamic_voter_requires_both_current_survivors() -> Result<()> {
    verified(true).await
}
