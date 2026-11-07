// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A restarted replacement resumes an exact active joint move on its retained engine.
use super::*;

const TURNS: usize = 8;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_joint_replacement_restart_settles_original_debit() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(with_store(rooted, Arc::new(fault::Store::default()))).await?;
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
        Box::pin(with_store(rooted, store)).await?;
    }
    Ok(())
}

async fn with_store(rooted: bool, store: Arc<fault::Store>) -> Result<()> {
    let mut cluster = Box::pin(ManagedCluster::start_with_store(
        rooted,
        &[7],
        ArchiveSetup::Initialized,
        store,
    ))
    .await?;
    let result = Box::pin(exercise(&mut cluster)).await;
    let stopped = close(cluster.nodes, &cluster.hosts).await;
    cluster.fixtures[0].domain.shutdown().await;
    result.with_context(|| format!("joint restart with extent images={rooted}"))?;
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

async fn restart_spare(cluster: &mut ManagedCluster) -> Result<FleetBootBinding> {
    let report = cluster.hosts[3]
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("old boot")?;
    let node = cluster.nodes.pop().context("replacement node")?;
    let old = cluster.hosts.pop().context("replacement service")?;
    close(vec![node], std::slice::from_ref(&old)).await?;
    let fixture = &cluster.fixtures[3];
    let node = start_node(
        cluster.roots[3].path(),
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
    cluster.nodes.push(node);
    cluster.hosts.push(host.clone());
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

async fn exercise(cluster: &mut ManagedCluster) -> Result<()> {
    let (seed, primary, obsolete, _, original) =
        Box::pin(reserve_replacement(cluster, false)).await?;
    let joint = enter_joint(cluster, primary, &original).await?;
    cluster.hosts[3]
        .group(cluster.history)?
        .quorum_membership_after(joint, TIMEOUT)
        .await?;
    let boot = Box::pin(restart_spare(cluster)).await?;
    let catalog = &cluster.fixtures[0].catalog;
    let observer = &cluster.hosts[primary];
    let mut successor = original.clone();
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
    let fixture = &cluster.fixtures[3];
    managed_fixture::admit_capacity(fixture, &cluster.hosts[3], &cluster.directory, 220).await?;
    discover(cluster).await?;
    let spare = &cluster.hosts[3];
    let retained = spare
        .group(cluster.history)?
        .quorum_membership_after(joint, TIMEOUT)
        .await?;
    ensure!(
        retained.membership().revision() == joint
            && retained.replica().member == MemberId::new(4)?
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
    let mut settled = false;
    for turn in 0..TURNS {
        let cut = catalog.read(TIMEOUT).await?;
        match catalog
            .reconcile_movement(&cut, &successor, observer, TIMEOUT)
            .await
        {
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
    ensure!(
        settled,
        "retained joint voter did not settle the same movement"
    );
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
    verify_cells(observer, &seed, 2).await?;
    Ok(())
}
