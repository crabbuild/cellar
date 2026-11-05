// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A retained sealed parent reopens for retirement after the route switch.
use super::*;
use std::time::Instant;

pub(super) async fn reopen_and_retire(
    cluster: &mut ManagedCluster,
    directory: &RangeDirectory,
    transition: &ReopenedManagedCellGroupTransition,
) -> Result<()> {
    let source = cluster.history;
    let fixture = &cluster.fixtures[0];
    let publication = fixture
        .domain
        .run(directory.publication(&transition.plan))
        .await??
        .context("selected transfer publication")?;
    let selected = fixture
        .catalog
        .reopen_managed_transition(directory, transition.plan.operation(), TIMEOUT)
        .await?
        .context("selected managed transfer")?;
    ensure!(
        selected
            .journal
            .as_ref()
            .context("transfer journal")?
            .phase()
            == TransitionPhase::Active
    );

    let old = cluster.nodes.remove(0);
    let previous = cluster.hosts.remove(0);
    let identity = previous.identity();
    ensure!(matches!(
        old.shutdown(ShutdownMode::Preserve).await?,
        NodeOutcome::Stopped(_)
    ));
    let report = previous.close().await?;
    ensure!(
        report.groups_total == 1
            && report.groups.len() == 1
            && report.omitted_groups == 0
            && report
                .groups
                .iter()
                .all(|group| !group.active && !group.failed)
            && report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers,
        "sealed source shutdown inventory: {report:?}"
    );
    let node = start_node(
        cluster.roots[0].path(),
        fixture.application.clone(),
        &fixture.spec.label,
    )
    .await?;
    let startup = async {
        let (mut config, services) = fixture.config(&node, LogOpenMode::Resume, 112).await?;
        crate::managed_fixture::archives::configure(&mut config);
        node.start_raft_cells(config, services).await
    }
    .await;
    let host = match startup {
        Ok(host) => host,
        Err(error) => {
            let stopped = node.shutdown(ShutdownMode::Preserve).await;
            return Err(error.context(format!(
                "retained sealed source startup, shutdown={stopped:?}"
            )));
        }
    };
    cluster.nodes.insert(0, node);
    cluster.hosts.insert(0, host);
    let reopened = &cluster.hosts[0];
    ensure!(reopened.identity().process != identity.process);
    ensure!(reopened.observe_node(TIMEOUT).await?.node.groups == 1);
    ensure!(!reopened.group(source)?.status().await?.ready);
    ensure!(reopened.group(source)?.archive_status()?.is_none());
    ensure!(fixture
        .domain
        .run(directory.materialize_selected_history(source))
        .await?
        .is_err());

    let voters = &cluster.hosts[..3];
    let tombstone = commit(voters, source, &selected).await?;
    ensure!(tombstone.tombstone().publication() == publication.journal_digest());
    let local = commit(&voters[..1], source, &selected).await?;
    ensure!(local.tombstone() == tombstone.tombstone());
    ensure!(!reopened.group(source)?.status().await?.ready);
    let retained = fixture
        .catalog
        .reopen_managed_transition(directory, transition.plan.operation(), TIMEOUT)
        .await?
        .context("managed retirement remains owned")?;
    let journal = retained.journal.context("active transition journal")?;
    ensure!(journal.phase() == TransitionPhase::Active && !journal.parent_holds_released());
    Ok(())
}

async fn commit(
    voters: &[RaftCells],
    source: GroupHistory,
    transition: &ReopenedManagedCellGroupTransition,
) -> Result<CellGroupSourceRetirementAck> {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        for voter in voters {
            let remaining = deadline.saturating_duration_since(Instant::now());
            ensure!(!remaining.is_zero(), "sealed source did not retire");
            match voter
                .group(source)?
                .retire_managed_source(transition, remaining)
                .await
            {
                Ok(CellGroupSourceRetirement::Applied(ack)) => return Ok(*ack),
                Ok(CellGroupSourceRetirement::Accepted(_))
                | Err(CellGroupControllerError::Runtime(
                    RuntimeError::NotLeader | RuntimeError::NotReady,
                )) => (),
                Err(error) => return Err(error.into()),
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
