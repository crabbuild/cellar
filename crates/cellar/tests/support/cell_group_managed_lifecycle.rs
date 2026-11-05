// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Resource-backed whole-group replacement through the public managed APIs.
use super::*;

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

async fn discover(cluster: &ManagedCluster) -> Result<()> {
    discover_survivors(cluster, None).await
}

async fn discover_survivors(cluster: &ManagedCluster, stopped: Option<usize>) -> Result<()> {
    let cut = cluster.fixtures[0].catalog.read(TIMEOUT).await?;
    for (i, host) in cluster.hosts.iter().enumerate() {
        if stopped == Some(i) {
            ensure!(host.is_closed());
            continue;
        }
        for (j, fixture) in cluster.fixtures.iter().enumerate() {
            if stopped == Some(j) {
                continue;
            }
            inspect_current(host, fixture.spec.node, cut.revision()).await?;
        }
    }
    Ok(())
}

async fn close(nodes: Vec<Node>, hosts: &[RaftCells]) -> Result<()> {
    let mut failures = Vec::new();
    for (node, host) in nodes.into_iter().zip(hosts) {
        match node.shutdown(ShutdownMode::Preserve).await {
            Ok(NodeOutcome::Stopped(_)) => (),
            result => failures.push(format!("node shutdown: {result:?}")),
        }
        match host.close().await {
            Ok(report)
                if report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers =>
            {
                ()
            }
            result => failures.push(format!("group host close: {result:?}")),
        }
    }
    ensure!(
        failures.is_empty(),
        "joined managed shutdown failed: {failures:?}"
    );
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
    let mut cluster = Box::pin(ManagedCluster::start_with_store(
        rooted,
        &[7],
        ArchiveSetup::Initialized,
        store,
    ))
    .await?;
    let work = async {
        let (seed, obsolete) = replace_with_source(&cluster, from_s3).await?;
        drain_leader(&mut cluster, &seed, obsolete).await?;
        Ok((seed, obsolete))
    }
    .await;
    // Even failed entered work must have a joined owner before this test exits.
    let stopped = close(std::mem::take(&mut cluster.nodes), &cluster.hosts).await;
    let (seed, obsolete) = match work {
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
    for (i, fixture) in cluster.fixtures.iter().enumerate() {
        if i == obsolete {
            continue;
        }
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
        config.groups.push(RaftGroupAssignment {
            history: cluster.history,
            placement: RaftGroupPlacement::Replica,
        });
        cluster
            .hosts
            .push(node.start_raft_cells(config, services).await?);
        cluster.nodes.push(node);
        members.push(i as u64 + 1);
    }
    let resumed = async {
        let index = current_leader(&cluster.hosts, &members, cluster.history).await?;
        verify_cells(&cluster.hosts[index], &seed, 4).await?;
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

async fn replace(cluster: &ManagedCluster) -> Result<(SeededCells, usize)> {
    Box::pin(replace_with_source(cluster, false)).await
}

async fn replace_with_source(
    cluster: &ManagedCluster,
    from_s3: bool,
) -> Result<(SeededCells, usize)> {
    Box::pin(replace_with_retry(cluster, from_s3, false)).await
}

async fn reserve_replacement(
    cluster: &ManagedCluster,
    from_s3: bool,
) -> Result<(
    SeededCells,
    usize,
    usize,
    MemberId,
    FleetMovementExecutionRequest,
)> {
    let seed = seed_cells(&cluster.hosts, cluster.history).await?;
    discover(cluster).await?;
    let index = leader(&cluster.hosts[..3], cluster.history).await?;
    let obsolete = (index + 1) % 3;
    let target = ReplacementTarget {
        source: index,
        obsolete,
        spare: 3,
    };
    let (obsolete_member, execution) =
        admit_replacement(cluster, target, [210, 211], from_s3).await?;
    Ok((seed, index, obsolete, obsolete_member, execution))
}

struct ReplacementTarget {
    source: usize,
    obsolete: usize,
    spare: usize,
}

async fn admit_replacement(
    cluster: &ManagedCluster,
    target: ReplacementTarget,
    ids: [u8; 2],
    from_s3: bool,
) -> Result<(MemberId, FleetMovementExecutionRequest)> {
    let f = &cluster.fixtures[0];
    let observer = &cluster.hosts[target.source];
    let membership = observer
        .group(cluster.history)?
        .quorum_membership(TIMEOUT)
        .await?;
    let local = cluster.hosts[target.spare].observe_node(TIMEOUT).await?;
    let cut = f.catalog.read(TIMEOUT).await?;
    let movement = FleetMovementRequest {
        resources: FleetResourceRequest {
            identity: request(ids[0], cut.revision()),
            group: cluster.history,
            attestation: FleetResourceAttestation {
                boot: local.fleet_boot.context("spare boot")?,
                profile_revision: 1,
                used: FleetResourceBounds {
                    engine_slots: 0,
                    durable_bytes: 1024 * 1024,
                    memory_bytes: 64 * 1024 * 1024,
                },
                evidence_digest: [71; 32],
                evidence_reference: "fixture-only/occupancy-attestation".into(),
            },
            peak: FleetResourceBounds {
                engine_slots: 1,
                durable_bytes: local.node.profile.bytes_per_group,
                memory_bytes: 8 * 1024 * 1024,
            },
        },
        source: cluster.fixtures[target.source].spec.node,
        plan: RepairPlan {
            id: ProposalId::new([ids[1]; 16])?,
            policy_revision: 1,
            expected_revision: membership.membership().revision(),
            obsolete: MemberId::new(target.obsolete as u64 + 1)?,
            replacement: JoiningMember {
                member: MemberId::new(target.spare as u64 + 1)?,
                node: cluster.fixtures[target.spare].spec.node,
                disk: cluster.fixtures[target.spare].spec.disk,
            },
        },
    };
    drop(membership);
    let accepted = f
        .catalog
        .reserve_movement(
            &cut,
            &movement,
            observer,
            std::slice::from_ref(&cluster.directory),
            TIMEOUT,
        )
        .await
        .context("reserve managed group replacement")?;
    let admitted = f.catalog.read(TIMEOUT).await?;
    ensure!(admitted.revision() == cut.revision() + 1);
    ensure!(accepted.resources().applied_revision() == admitted.revision());
    let rows = f.catalog.pending_reservations(&admitted, TIMEOUT).await?;
    ensure!(
        rows.len() == 1
            && rows[0].reservation == movement.resources.identity.id
            && rows[0].boot == movement.resources.attestation.boot
            && rows[0].peak == movement.resources.peak
            && rows[0].executable_movement
    );
    let execution = FleetMovementExecutionRequest {
        reservation: movement.resources.identity.id,
        attestation: movement.resources.attestation.clone(),
        from_s3,
    };
    Ok((movement.plan.obsolete, execution))
}

async fn replace_with_retry(
    cluster: &ManagedCluster,
    from_s3: bool,
    refuse_then_failover: bool,
) -> Result<(SeededCells, usize)> {
    let (seed, index, obsolete, obsolete_member, execution) =
        Box::pin(reserve_replacement(cluster, from_s3)).await?;
    let f = &cluster.fixtures[0];
    let mut observer = &cluster.hosts[index];
    let stopped_source = if refuse_then_failover {
        ensure!(from_s3);
        let next = Box::pin(s3::refuse_then_failover(cluster, &execution, index)).await?;
        observer = &cluster.hosts[next];
        Some(index)
    } else {
        None
    };
    let mut settled = false;
    let mut phases = Vec::new();
    let mut lose_settlement = false;
    let mut lost_settlement = false;
    for turn in 0..8 {
        discover_survivors(cluster, stopped_source).await?;
        let cut = f.catalog.read(TIMEOUT).await?;
        if stopped_source.is_none()
            && phases.last() == Some(&FleetMovementPhase::Joint)
            && !lose_settlement
        {
            // Lose the actual terminal root-CAS reply and its confirmation.
            // Background directory reads cannot consume this task-bound fault.
            f.store
                .fail_fleet_confirmation
                .store(true, Ordering::Release);
            lose_settlement = true;
        }
        let progress = match f
            .catalog
            .reconcile_movement(&cut, &execution, observer, TIMEOUT)
            .await
        {
            Ok(progress) => progress,
            Err(FleetCatalogError::Unconfirmed) => {
                let current = f.catalog.read(TIMEOUT).await?;
                let pending = f.catalog.pending_reservations(&current, TIMEOUT).await?;
                if lose_settlement && !f.store.fail_fleet_confirmation.load(Ordering::Acquire) {
                    ensure!(current.revision() == cut.revision() + 1 && pending.is_empty());
                    lost_settlement = true;
                    eprintln!("managed replacement turn {turn}: terminal CAS reply lost; receipt reconciliation required");
                } else {
                    // The learner or configuration may already be durable.
                    // A nonterminal outcome cannot release its resource debit.
                    ensure!(pending.len() == 1 && pending[0].reservation == execution.reservation);
                    eprintln!("managed replacement turn {turn}: Unconfirmed; debit retained");
                }
                continue;
            }
            Err(FleetCatalogError::Observation(
                cellar::application::consensus::InspectionError::Unreachable,
            )) if stopped_source.is_some() => {
                ensure!(f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
                Box::pin(s3::verify_blocked_settlement(
                    cluster, &seed, &execution, observer, obsolete,
                ))
                .await?;
                return Ok((seed, obsolete));
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("managed replacement turn {turn}, observed {phases:?}")
                })
            }
        };
        eprintln!(
            "managed replacement rooted={:?} phase={:?} settled={}",
            f.declaration.namespace.image_storage, progress.phase, progress.settled
        );
        phases.push(progress.phase);
        if progress.settled {
            settled = true;
            break;
        }
        ensure!(
            f.catalog
                .pending_reservations(&f.catalog.read(TIMEOUT).await?, TIMEOUT)
                .await?
                .len()
                == 1,
            "unsettled work released its debit"
        );
    }
    ensure!(
        settled && lost_settlement,
        "replacement did not reconcile its lost settlement: {phases:?}"
    );
    ensure!(
        phases.contains(&FleetMovementPhase::Seeding)
            && phases.contains(&FleetMovementPhase::Joint)
            && phases.last() == Some(&FleetMovementPhase::Complete)
    );
    let cut = f.catalog.read(TIMEOUT).await?;
    ensure!(f
        .catalog
        .pending_reservations(&cut, TIMEOUT)
        .await?
        .is_empty());
    ensure!(f.catalog.pending_movements(&cut, TIMEOUT).await?.is_empty());
    // Replaying a lost terminal reply observes the permanent receipt.
    let replay = f
        .catalog
        .reconcile_movement(&cut, &execution, observer, TIMEOUT)
        .await?;
    ensure!(replay.settled && replay.phase == FleetMovementPhase::Complete);
    let membership = observer
        .group(cluster.history)?
        .quorum_membership(TIMEOUT)
        .await?;
    let mut voters = membership.configuration().voters.clone();
    voters.sort_unstable();
    let expected = (1..=4)
        .filter(|id| *id != obsolete as u64 + 1)
        .collect::<Vec<_>>();
    ensure!(
        voters == expected
            && membership.configuration().voters_outgoing.is_empty()
            && membership.configuration().learners.is_empty()
    );
    drop(membership);
    discover_survivors(cluster, stopped_source).await?;
    // Settlement retires the move, not the old disk's physical debt.
    let removed = observer
        .inspect_peer_physical_inventory(
            cluster.fixtures[obsolete].spec.node,
            cut.revision(),
            TIMEOUT,
        )
        .await?;
    let inventory = removed
        .inventory
        .context("removed member physical inventory")?;
    ensure!(inventory.histories().len() == 1);
    let retained = inventory
        .group_history(&cluster.history)
        .context("retained removed history")?;
    ensure!(
        !retained.in_configuration()
            && retained.completed_sequence() == 1
            && retained.member() == obsolete_member
    );
    ensure!(
        cluster.hosts[obsolete]
            .resource_usage()
            .retained_manifest_cells
            == Some(2)
    );
    ensure!(cluster.hosts[3].resource_usage().retained_manifest_cells == Some(2));
    let archive_reads = cluster.fixtures[3]
        .store
        .group_payload_reads
        .load(Ordering::Acquire);
    if from_s3 {
        ensure!(
            archive_reads > 0,
            "S3 replacement never fetched archive payloads"
        );
    }
    verify_cells(observer, &seed, 2).await?;
    Ok((seed, obsolete))
}

async fn drain_leader(
    cluster: &mut ManagedCluster,
    seed: &SeededCells,
    obsolete: usize,
) -> Result<()> {
    let members = (1..=4).collect::<Vec<_>>();
    let index = current_leader(&cluster.hosts, &members, cluster.history).await?;
    ensure!(index != obsolete);
    let node = cluster.nodes.remove(index);
    let host = cluster.hosts.remove(index);
    let outcome = node.shutdown(ShutdownMode::Handoff).await?;
    let closure = host.close().await?;
    ensure!(
        closure.groups_total == 1
            && closure.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
    );
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("leader did not stop: {outcome:?}");
    };
    let raft = report.raft.context("leader shutdown Raft proof")?;
    ensure!(!raft.startup_incomplete && raft.result.is_ok());
    let drain = raft.drain.context("actual group drain proof")??;
    ensure!(
        drain.groups_examined == 1
            && drain.handoffs_accepted == 1
            && drain.leadership_released == 1
            && drain.unavailable_groups == 0
            && !drain.deadline_elapsed,
        "incomplete group handoff: {drain:?}"
    );
    let survivors = (0..4)
        .filter(|i| *i != index && *i != obsolete)
        .collect::<Vec<_>>();
    let hosts = survivors
        .iter()
        .map(|original| {
            cluster.hosts[if *original > index {
                original - 1
            } else {
                *original
            }]
            .clone()
        })
        .collect::<Vec<_>>();
    let members = survivors.iter().map(|i| *i as u64 + 1).collect::<Vec<_>>();
    let leader = current_leader(&hosts, &members, cluster.history).await?;
    verify_cells(&hosts[leader], seed, 3).await?;
    Ok(())
}

async fn current_leader(
    hosts: &[RaftCells],
    members: &[u64],
    history: GroupHistory,
) -> Result<usize> {
    Ok(tokio::time::timeout(TIMEOUT, async {
        loop {
            for (i, (host, member)) in hosts.iter().zip(members).enumerate() {
                let status = host.group(history)?.status().await?;
                if status.ready && status.leader == Some(*member) {
                    return Ok::<_, anyhow::Error>(i);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??)
}

async fn verify_cells(host: &RaftCells, seed: &SeededCells, sequence: u64) -> Result<()> {
    for (i, (cell, original)) in seed.cells.iter().zip(&seed.receipts).enumerate() {
        let handle = host.cell(cell.clone())?;
        let delta = [7i64, 11][i];
        let mut duplicate_options = CommandOptions::new(original.id, seed.retry);
        duplicate_options.durability = Durability::S3;
        let duplicate = handle
            .command(
                &handle.contract("Add")?,
                delta.to_be_bytes().to_vec(),
                duplicate_options,
            )
            .await?;
        ensure!(
            duplicate.result == original.result
                && duplicate.id == original.id
                && duplicate.durability == Durability::S3
        );
        let query = handle
            .query(
                ProposalId::new([190 + i as u8; 16])?,
                &handle.contract("Get")?,
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(original.version_token.clone()),
                    timeout: TIMEOUT,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(
            query.result == (delta + sequence as i64 - 2).to_be_bytes(),
            "duplicate changed the installed image"
        );
        let mut write_options =
            CommandOptions::new(CommandId::new(original.id.client, sequence)?, seed.retry);
        write_options.durability = Durability::S3;
        let result = handle
            .command(
                &handle.contract("Add")?,
                1i64.to_be_bytes().to_vec(),
                write_options,
            )
            .await?;
        ensure!(
            result.result == (delta + sequence as i64 - 1).to_be_bytes()
                && result.durability == Durability::S3
        );
    }
    Ok(())
}

#[path = "cell_group_managed_maintenance.rs"]
mod maintenance;
