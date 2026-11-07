// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Resource-backed whole-group replacement through the public managed APIs.
use super::*;
use crate::managed_fixture::movement::*;
use std::sync::atomic::AtomicUsize;

#[path = "cell_group_managed_discovery.rs"]
mod discovery;

#[path = "cell_group_managed_router.rs"]
mod router;

#[path = "cell_group_managed_cold_router.rs"]
mod cold_router;

#[path = "cell_group_managed_controls.rs"]
mod controls;

#[path = "cell_group_managed_s3.rs"]
mod s3;

#[path = "cell_group_managed_joint_restart.rs"]
mod joint_restart;

#[path = "cell_group_managed_isolation.rs"]
mod isolation;

#[path = "cell_group_managed_recovery.rs"]
mod recovery;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_group_replacement_and_joined_restart() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(replacement(rooted)).await?;
    }
    Ok(())
}

async fn replacement(rooted: bool) -> Result<()> {
    Box::pin(replacement_with_source(rooted, false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_s3_group_replacement_and_joined_restart() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(replacement_with_source(rooted, true)).await?;
    }
    Ok(())
}

async fn replacement_with_source(rooted: bool, from_s3: bool) -> Result<()> {
    Box::pin(replacement_with_store(
        rooted,
        from_s3,
        Arc::new(fault::Store::default()),
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_managed_s3_group_replacement_and_joined_restart() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(fault::Store::around(rustfs::store(
            "managed-s3-replacement",
            rooted,
        )?));
        Box::pin(replacement_with_store(rooted, true, store)).await?;
    }
    Ok(())
}

async fn replacement_with_store(
    rooted: bool,
    from_s3: bool,
    store: Arc<fault::Store>,
) -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let class = managed_fixture::counter_class_with_callback(move || {
        counter.fetch_add(1, Ordering::AcqRel);
    })?;
    let first = Box::pin(Fixture::with_store(rooted, class, store)).await?;
    let mut cluster = Box::pin(ManagedCluster::start_with_applications(
        first,
        &[7],
        ArchiveSetup::Initialized,
        false,
        None,
        FirstGroupSetup::Normal,
    ))
    .await?;
    if from_s3 {
        // Seed from retained startup, with the transport pool enabled and no
        // periodic checkpoint policy. Fresh attachment uses a different path.
        close(std::mem::take(&mut cluster.nodes), &cluster.hosts).await?;
        cluster.hosts.clear();
        for (index, fixture) in cluster.fixtures.iter().enumerate() {
            let node = start_node(
                cluster.roots[index].path(),
                fixture.application.clone(),
                &fixture.spec.label,
            )
            .await?;
            let (mut config, services) = fixture
                .config(&node, LogOpenMode::Resume, 34 + index as u8)
                .await?;
            archives::configure(&mut config);
            ensure!(
                config.shared.checkpoints.is_some()
                    && config.shared.controller.checkpoints.is_none()
            );
            cluster.peers[index] = config.peers[0].clone();
            cluster
                .hosts
                .push(node.start_raft_cells(config, services).await?);
            cluster.nodes.push(node);
        }
        for (index, fixture) in cluster.fixtures.iter().enumerate() {
            admit_capacity(
                fixture,
                &cluster.hosts[index],
                &cluster.directory,
                180 + 2 * index as u8,
            )
            .await
            .context("requalify the retained boot for repair")?;
        }
    }
    let work = async {
        let (seed, obsolete) = replace_with_source(&cluster, from_s3).await?;
        let joined_boot = cluster.hosts[3]
            .observe_node(TIMEOUT)
            .await?
            .fleet_boot
            .context("installed learner's boot")?;
        drain_leader(&mut cluster, &seed, obsolete).await?;
        ensure!(
            calls.load(Ordering::Acquire) == 6,
            "seed replay ran SQL again"
        );
        Ok((seed, obsolete, joined_boot))
    }
    .await;
    // Even failed entered work must have a joined owner before this test exits.
    let stopped = close(std::mem::take(&mut cluster.nodes), &cluster.hosts).await;
    let (seed, obsolete, joined_boot) = match work {
        Ok(value) => {
            stopped?;
            value
        }
        Err(error) => {
            stopped?;
            cluster.fixtures[0].domain.shutdown().await;
            return Err(error);
        }
    };
    cluster.hosts.clear();
    // The replacement's retained disk is opened through the canonical joined
    // member path, together with the two surviving initial members.
    let mut members = Vec::new();
    for i in std::iter::once(3).chain((0..3).filter(|i| *i != obsolete)) {
        let fixture = &cluster.fixtures[i];
        let node = start_node(
            cluster.roots[i].path(),
            fixture.application.clone(),
            &fixture.spec.label,
        )
        .await?;
        let (mut config, services) = fixture
            .config(&node, LogOpenMode::Resume, 44 + i as u8)
            .await?;
        archives::configure(&mut config);
        cluster
            .hosts
            .push(node.start_raft_cells(config, services).await?);
        cluster.nodes.push(node);
        members.push(i as u64 + 1);
        if i == 3 {
            let spare = &cluster.hosts[0];
            let cold = verify_cold_joined_voter(spare, cluster.history, &seed, &joined_boot).await;
            if let Err(error) = cold {
                let stopped = close(std::mem::take(&mut cluster.nodes), &cluster.hosts).await;
                fixture.domain.shutdown().await;
                return Err(
                    error.context(format!("joined-voter cold restart; shutdown={stopped:?}"))
                );
            }
        }
    }
    let resumed = async {
        let index = current_leader(&cluster.hosts, &members, cluster.history).await?;
        verify_cells(&cluster.hosts[index], &seed, 4).await?;
        ensure!(
            calls.load(Ordering::Acquire) == 8,
            "retained restart ran SQL again"
        );
        let catalog = &cluster.fixtures[0].catalog;
        let cut = catalog.read(TIMEOUT).await?;
        let group = catalog
            .authorize_group(&cut, &cluster.history, TIMEOUT)
            .await?;
        let old_permit = catalog
            .authorize_membership(&cut, &cluster.history, TIMEOUT)
            .await?;
        let target = members
            .iter()
            .position(|member| *member == 4)
            .context("joined target")?;
        for (i, history) in seed.cells.iter().enumerate() {
            let cell = cluster.hosts[target].cell(history.clone())?;
            let value = cell
                .query(
                    ProposalId::new([234 + i as u8; 16])?,
                    &cell.contract("Get")?,
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::Linearizable,
                        routing: QueryRouting::Local,
                        ..Default::default()
                    },
                )
                .await?;
            ensure!(
                value.served_by == MemberId::new(4)?
                    && value.served_by_node == cluster.hosts[target].identity()
                    && value.result == [10_i64, 14][i].to_be_bytes()
            );
        }
        let source = if index == target {
            (target + 1) % members.len()
        } else {
            index
        };
        let source_node = cluster.hosts[source]
            .observe_node(TIMEOUT)
            .await?
            .node
            .identity
            .node;
        let result = cluster.hosts[target]
            .activate_managed_learner_group(
                group,
                old_permit,
                LearnerActivation {
                    member: MemberId::new(4)?,
                    source: MemberId::new(members[source])?,
                    source_node,
                    from_s3: false,
                    timeout: TIMEOUT,
                },
            )
            .await;
        ensure!(
            matches!(
                result,
                Err(CellGroupStartupError::Refused(
                    CellGroupControllerError::Invalid("managed learner admission")
                ))
            ),
            "old-boot learner grant was not refused before submission: {result:?}"
        );
        ensure!(catalog.read(TIMEOUT).await?.revision() == cut.revision());
        for host in &cluster.hosts {
            ensure!(host.observe_node(TIMEOUT).await?.node.groups == 1);
            ensure!(host.resource_usage().retained_manifest_cells == Some(2));
        }
        Ok(())
    }
    .await;
    close(std::mem::take(&mut cluster.nodes), &cluster.hosts).await?;
    cluster.fixtures[0].domain.shutdown().await;
    resumed
}

async fn verify_cold_joined_voter(
    host: &RaftCells,
    history: GroupHistory,
    seed: &SeededCells,
    prior: &FleetBootBinding,
) -> Result<()> {
    let boot = host
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .context("retained voter boot")?;
    ensure!(
        boot.node == prior.node
            && boot.disk == prior.disk
            && boot.engine == prior.engine
            && boot.process != prior.process
            && boot.epoch > prior.epoch
    );
    let group = host.group(history)?;
    let before = group.observe(TIMEOUT).await?;
    let member = MemberId::new(4)?;
    ensure!(before.cells == 2 && before.replica.member == member && !before.status.ready);
    for (index, original) in seed.receipts.iter().enumerate() {
        let cell = host.cell(seed.cells[index].clone())?;
        let get = cell.contract("Get")?;
        let stored = cell
            .query(
                ProposalId::new([230 + index as u8; 16])?,
                &get,
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(original.version_token.clone()),
                    routing: QueryRouting::Local,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(
            stored.served_by == member
                && stored.served_by_node == host.identity()
                && stored.result == [9_i64, 13][index].to_be_bytes()
                && stored
                    .version
                    .covers(&original.version)
                    .is_ok_and(|covers| covers)
        );
        let strong = cell
            .query(
                ProposalId::new([232 + index as u8; 16])?,
                &get,
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::Linearizable,
                    routing: QueryRouting::Local,
                    timeout: Duration::from_millis(200),
                    ..Default::default()
                },
            )
            .await;
        let refusal = strong.expect_err("retained voter served a strong read without quorum");
        ensure!(
            refusal.refusal() == Some(&CommandFailure::VersionUnavailable)
                || matches!(
                    refusal.as_ref(),
                    CellDispatchError::Runtime(
                        RuntimeError::NotReady
                            | RuntimeError::NotLeader
                            | RuntimeError::DeadlineExceeded
                    )
                ),
            "unexpected cold-start read failure: {refusal:?}"
        );
    }
    let after = group.observe(TIMEOUT).await?;
    ensure!(!after.status.ready && after.status.committed == before.status.committed);
    Ok(())
}

#[path = "cell_group_managed_maintenance.rs"]
mod maintenance;
