// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Restarted joint voters resume an exact move on their retained engines.
use super::*;

const TURNS: usize = 8;

#[derive(Clone, Copy, Debug)]
enum RestartedVoter {
    Replacement,
    SeedOwner,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_joint_replacement_restart_settles_original_debit() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(with_store(
            rooted,
            Arc::new(fault::Store::default()),
            RestartedVoter::Replacement,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_managed_joint_replacement_restart_settles_original_debit() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(fault::Store::around(rustfs::store(
            "managed-joint-restart",
            rooted,
        )?));
        Box::pin(with_store(rooted, store, RestartedVoter::Replacement)).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_joint_seed_owner_restart_settles_original_debit() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(with_store(
            rooted,
            Arc::new(fault::Store::default()),
            RestartedVoter::SeedOwner,
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_managed_joint_seed_owner_restart_settles_original_debit() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(fault::Store::around(rustfs::store(
            "managed-joint-seed-owner-restart",
            rooted,
        )?));
        Box::pin(with_store(rooted, store, RestartedVoter::SeedOwner)).await?;
    }
    Ok(())
}

async fn with_store(
    rooted: bool,
    store: Arc<fault::Store>,
    restarted: RestartedVoter,
) -> Result<()> {
    let mut cluster = Box::pin(ManagedCluster::start_with_store(
        rooted,
        &[7],
        ArchiveSetup::Initialized,
        store,
    ))
    .await?;
    let result = Box::pin(exercise(&mut cluster, restarted)).await;
    let stopped = close(cluster.nodes, &cluster.hosts).await;
    cluster.fixtures[0].domain.shutdown().await;
    result
        .with_context(|| format!("joint restart of {restarted:?} with extent images={rooted}"))?;
    stopped
}

async fn enter_joint(
    cluster: &ManagedCluster,
    primary: usize,
    request: &FleetMovementExecutionRequest,
) -> Result<LogIndex> {
    let catalog = &cluster.fixtures[0].catalog;
    let observer = &cluster.hosts[primary];
    let group = observer.group(cluster.history)?;
    let mut attempts = Vec::new();
    for _ in 0..TURNS {
        discover(cluster).await?;
        let proof = group.quorum_membership(TIMEOUT).await?;
        if let Some(status) = proof.membership().fleet_status().filter(|status| {
            status.phase == Some(cellar::application::consensus::FleetPermitPhase::Joint)
        }) {
            ensure!(
                status
                    .permit
                    .as_ref()
                    .is_some_and(|p| p.id() == request.reservation)
                    && proof.configuration().voters.contains(&4)
                    && proof.configuration().voters_outgoing.len() == 3
            );
            return Ok(proof.membership().revision());
        }
        drop(proof);
        let cut = catalog.read(TIMEOUT).await?;
        let result = catalog
            .reconcile_movement(&cut, request, observer, TIMEOUT)
            .await;
        attempts.push(format!("{result:?}"));
        match result {
            Ok(progress) => ensure!(
                !progress.settled
                    && matches!(
                        progress.phase,
                        FleetMovementPhase::Reserved | FleetMovementPhase::Seeding
                    )
            ),
            Err(FleetCatalogError::Unconfirmed) => (),
            Err(error) => return Err(error).context("enter managed joint configuration"),
        }
    }
    anyhow::bail!("replacement did not enter joint consensus: {attempts:?}")
}

async fn restart_voter(cluster: &mut ManagedCluster, index: usize) -> Result<FleetBootBinding> {
    let report = cluster.hosts[index]
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("old boot")?;
    let node = cluster.nodes.remove(index);
    let old = cluster.hosts.remove(index);
    close(vec![node], std::slice::from_ref(&old)).await?;
    let fixture = &cluster.fixtures[index];
    let node = start_node(
        cluster.roots[index].path(),
        fixture.application.clone(),
        &fixture.spec.label,
    )
    .await?;
    let opened = async {
        let (mut config, services) = fixture.config(&node, LogOpenMode::Resume, 81).await?;
        archives::configure(&mut config);
        Ok::<_, anyhow::Error>(node.start_raft_cells(config, services).await?)
    }
    .await;
    let host = match opened {
        Ok(host) => host,
        Err(error) => {
            node.shutdown(ShutdownMode::Preserve).await?;
            return Err(error).context("reopen retained joint voter");
        }
    };
    cluster.nodes.insert(index, node);
    cluster.hosts.insert(index, host.clone());
    let boot = host
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("retained boot")?;
    ensure!(
        boot.node == report.node
            && boot.disk == report.disk
            && boot.engine == report.engine
            && boot.process != report.process
            && boot.epoch > report.epoch
    );
    Ok(boot)
}

async fn exercise(cluster: &mut ManagedCluster, restarted: RestartedVoter) -> Result<()> {
    let (seed, primary, obsolete, _, original) =
        Box::pin(reserve_replacement(cluster, false)).await?;
    let joint = enter_joint(cluster, primary, &original).await?;
    cluster.hosts[3]
        .group(cluster.history)?
        .quorum_membership_after(joint, TIMEOUT)
        .await?;
    let index = match restarted {
        RestartedVoter::Replacement => 3,
        RestartedVoter::SeedOwner => primary,
    };
    let boot = Box::pin(restart_voter(cluster, index)).await?;
    let catalog = &cluster.fixtures[0].catalog;
    let observer = &cluster.hosts[primary];
    let mut successor = original.clone();
    if matches!(restarted, RestartedVoter::Replacement) {
        successor.attestation.boot = boot;
        successor.attestation.used.engine_slots = 1;
        successor.attestation.evidence_digest = [81; 32];
        successor.attestation.evidence_reference = "fixture-only/retained-joint-occupancy".into();
        let unqualified = catalog.read(TIMEOUT).await?;
        ensure!(
            matches!(
                catalog
                    .reconcile_movement(&unqualified, &successor, observer, TIMEOUT)
                    .await,
                Err(FleetCatalogError::Conflict)
            ),
            "successor boot advanced the move before fresh capacity qualification"
        );
        ensure!(catalog.read(TIMEOUT).await?.revision() == unqualified.revision());
    }
    let fixture = &cluster.fixtures[index];
    managed_fixture::admit_capacity(fixture, &cluster.hosts[index], &cluster.directory, 220)
        .await?;
    discover(cluster).await?;
    let spare = &cluster.hosts[3];
    let reopened = cluster.hosts[index].group(cluster.history)?;
    let retained = tokio::time::timeout(TIMEOUT, async {
        loop {
            match reopened
                .quorum_membership_after(joint, Duration::from_millis(250))
                .await
            {
                Ok(proof) => return Ok(proof),
                Err(
                    RuntimeError::NotLeader
                    | RuntimeError::NotReady
                    | RuntimeError::DeadlineExceeded,
                ) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .context("reopened voter did not observe the joint quorum")??;
    ensure!(
        retained.membership().revision() == joint
            && retained.replica().member == MemberId::new(index as u64 + 1)?
            && retained.configuration().voters_outgoing.len() == 3
    );
    drop(retained);

    let cut = catalog.read(TIMEOUT).await?;
    let pending = catalog.pending_reservations(&cut, TIMEOUT).await?;
    ensure!(
        pending.len() == 1
            && pending[0].reservation == original.reservation
            && pending[0].boot == original.attestation.boot
    );
    if matches!(restarted, RestartedVoter::Replacement) {
        let mut foreign = successor.clone();
        foreign.attestation.boot.engine = [99; 16];
        ensure!(
            matches!(
                catalog
                    .reconcile_movement(&cut, &foreign, observer, TIMEOUT)
                    .await,
                Err(FleetCatalogError::Conflict)
            ),
            "foreign engine was accepted for the retained move"
        );
        ensure!(catalog.read(TIMEOUT).await?.revision() == cut.revision());
        let spare_boot = catalog
            .authorize_group(&cut, &cluster.history, TIMEOUT)
            .await?;
        let old_grant = catalog
            .authorize_membership(&cut, &cluster.history, TIMEOUT)
            .await?;
        let activation = spare
            .activate_managed_learner_group(
                spare_boot,
                old_grant,
                LearnerActivation {
                    member: MemberId::new(4)?,
                    source: MemberId::new(primary as u64 + 1)?,
                    source_node: observer.identity().node,
                    from_s3: false,
                    timeout: TIMEOUT,
                },
            )
            .await;
        ensure!(
            matches!(
                activation,
                Err(CellGroupStartupError::Refused(
                    CellGroupControllerError::Invalid("managed learner admission")
                ))
            ),
            "old-boot seed grant was accepted by the successor: {activation:?}"
        );
    }
    let mut settled = false;
    let mut attempts = Vec::new();
    for turn in 0..TURNS {
        let cut = catalog.read(TIMEOUT).await?;
        let result = catalog
            .reconcile_movement(&cut, &successor, observer, TIMEOUT)
            .await;
        attempts.push(format!("{result:?}"));
        match result {
            Ok(progress) if progress.settled => {
                ensure!(progress.phase == FleetMovementPhase::Complete);
                settled = true;
                break;
            }
            Ok(progress) => ensure!(progress.phase == FleetMovementPhase::Joint),
            Err(FleetCatalogError::Unconfirmed) => (),
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("reconcile retained joint voter turn {turn}"))
            }
        }
        let cut = catalog.read(TIMEOUT).await?;
        let pending = catalog.pending_reservations(&cut, TIMEOUT).await?;
        ensure!(
            pending.len() == 1
                && pending[0].reservation == original.reservation
                && pending[0].boot == original.attestation.boot
        );
    }
    if !settled {
        report_settlement_state(cluster, observer).await;
        anyhow::bail!("retained joint voter did not settle the same movement: {attempts:?}");
    }
    let cut = catalog.read(TIMEOUT).await?;
    ensure!(catalog
        .pending_reservations(&cut, TIMEOUT)
        .await?
        .is_empty());
    let replay = catalog
        .reconcile_movement(&cut, &successor, observer, TIMEOUT)
        .await?;
    ensure!(replay.settled && replay.phase == FleetMovementPhase::Complete);
    let proof = spare
        .group(cluster.history)?
        .quorum_membership(TIMEOUT)
        .await?;
    ensure!(
        proof.configuration().voters.contains(&4)
            && !proof
                .configuration()
                .voters
                .contains(&(obsolete as u64 + 1))
            && proof.configuration().voters_outgoing.is_empty()
    );
    drop(proof);
    let primary = current_leader(&cluster.hosts, &[1, 2, 3, 4], cluster.history).await?;
    verify_cells(&cluster.hosts[primary], &seed, 2).await?;
    Ok(())
}

async fn report_settlement_state(cluster: &ManagedCluster, observer: &RaftCells) {
    const PROBE: Duration = Duration::from_millis(250);
    let cut = cluster.fixtures[0].catalog.read(PROBE).await;
    for (fixture, host) in cluster.fixtures.iter().zip(&cluster.hosts) {
        let status = match host.group(cluster.history) {
            Ok(group) => group
                .observe(PROBE)
                .await
                .map(|sample| (sample.role, sample.status, sample.membership_revision)),
            Err(error) => Err(error),
        };
        let inventory = match &cut {
            Ok(cut) => observer
                .inspect_peer_physical_inventory(fixture.spec.node, cut.revision(), PROBE)
                .await
                .map(|sample| {
                    let history = sample
                        .inventory
                        .as_ref()
                        .and_then(|i| i.group_history(&cluster.history));
                    (
                        sample.view.seed_workers,
                        sample.view.pending_history_registrations,
                        history.map(|h| (h.member(), h.completed_sequence(), h.in_configuration())),
                    )
                })
                .map_err(anyhow::Error::from),
            Err(error) => Err(anyhow::anyhow!("catalog cut unavailable: {error}")),
        };
        eprintln!(
            "joint settlement voter {}: status={status:?} inventory={inventory:?}",
            fixture.spec.label
        );
    }
}
