// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Refusing S3 bytes retains the exact nonserving learner, without peer fallback.
use super::*;
use cellar::application::consensus::verification::TransportError;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_s3_seed_refusal_preserves_admission_and_retries_after_failover() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(refused_with_store(
            rooted,
            Arc::new(fault::Store::default()),
        ))
        .await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_managed_s3_seed_refusal_and_source_failover() -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(fault::Store::around(rustfs::store(
            "managed-s3-failover",
            rooted,
        )?));
        Box::pin(refused_with_store(rooted, store)).await?;
    }
    Ok(())
}

async fn refused_with_store(rooted: bool, store: Arc<fault::Store>) -> Result<()> {
    let mut cluster = Box::pin(ManagedCluster::start_with_store(
        rooted,
        &[7],
        ArchiveSetup::Initialized,
        store,
    ))
    .await?;
    let work = Box::pin(replace_with_retry(&cluster, true, true)).await;
    let stopped = close(std::mem::take(&mut cluster.nodes), &cluster.hosts).await;
    cluster.fixtures[0].domain.shutdown().await;
    let result = match (work, stopped) {
        (Ok(_), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(stopped)) => Err(anyhow::anyhow!(
            "S3 learner failure: {error:#}; joined shutdown failure: {stopped:#}"
        )),
    };
    if result.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!("retained failed managed S3 disk: {}", root.keep().display());
        }
    }
    result
}

pub(super) async fn refuse_then_failover(
    cluster: &ManagedCluster,
    execution: &FleetMovementExecutionRequest,
    source: usize,
) -> Result<usize> {
    let catalog = &cluster.fixtures[0].catalog;
    let target = &cluster.hosts[3];
    discover(cluster).await?;
    let cut = catalog.read(TIMEOUT).await?;
    let began = catalog
        .reconcile_movement(&cut, execution, &cluster.hosts[source], TIMEOUT)
        .await;
    match began {
        Ok(progress) => {
            ensure!(progress.phase == FleetMovementPhase::Reserved && !progress.settled)
        }
        Err(FleetCatalogError::Unconfirmed) => {
            // A missing response cannot prove that Begin did not commit. The
            // following fresh quorum observation must establish its actual phase.
        }
        Err(error) => return Err(error).context("begin S3 refusal fixture"),
    }
    let current = cluster.hosts[source]
        .group(cluster.history)?
        .quorum_membership(TIMEOUT)
        .await
        .context("observe committed Seeding before S3 denial")?;
    ensure!(
        current
            .membership()
            .fleet_status()
            .is_some_and(|status| status.phase
                == Some(cellar::application::consensus::FleetPermitPhase::Seeding)),
        "S3 refusal fixture did not reach committed Seeding"
    );
    drop(current);
    discover(cluster).await?;
    let cut = catalog.read(TIMEOUT).await?;
    let group = catalog
        .authorize_group(&cut, &cluster.history, TIMEOUT)
        .await?;
    let permit = catalog
        .authorize_membership(&cut, &cluster.history, TIMEOUT)
        .await?;
    let activation = LearnerActivation {
        member: MemberId::new(4)?,
        source: MemberId::new(source as u64 + 1)?,
        source_node: cluster.fixtures[source].spec.node,
        from_s3: true,
        timeout: TIMEOUT,
    };
    let store = &cluster.fixtures[3].store;
    store
        .deny_group_payload_reads
        .store(true, Ordering::Release);
    let refused = async {
        for attempt in 0..8 {
            let result = target
                .activate_managed_learner_group(group.clone(), permit.clone(), activation.clone())
                .await;
            match result {
                Err(CellGroupStartupError::Unconfirmed(CellGroupControllerError::Archive(_))) => {
                    return Ok(())
                }
                Err(CellGroupStartupError::Unconfirmed(CellGroupControllerError::Transport(
                    TransportError::Io(_) | TransportError::Timeout,
                )))
                | Err(CellGroupStartupError::Unconfirmed(CellGroupControllerError::Runtime(
                    RuntimeError::DeadlineExceeded | RuntimeError::NotReady,
                ))) => {
                    eprintln!(
                        "S3 denial attempt {attempt}: ambiguous response; exact admission retained"
                    );
                    ensure!(target.resource_usage().host.controller.groups == 0);
                    ensure!(
                        catalog
                            .pending_reservations(&catalog.read(TIMEOUT).await?, TIMEOUT)
                            .await?
                            .len()
                            == 1
                    );
                }
                result => anyhow::bail!(
                    "S3 refusal changed classification or used peer bytes: {result:?}"
                ),
            }
        }
        anyhow::bail!("S3 denial never reached the physical receiver")
    }
    .await;
    store
        .deny_group_payload_reads
        .store(false, Ordering::Release);
    refused?;
    ensure!(store.denied_group_payload_reads.load(Ordering::Acquire) > 0);
    // Node inventory includes the retained staged engine mapping. Admission is
    // proved independently by the controller and its absent runtime owner.
    ensure!(target.observe_node(TIMEOUT).await?.node.groups == 1);
    ensure!(target.resource_usage().host.controller.groups == 0);
    ensure!(
        target
            .resource_usage()
            .host
            .controller
            .pending_registrations
            == 1
    );
    ensure!(matches!(
        target.group(cluster.history)?.status().await,
        Err(RuntimeError::NotFound)
    ));
    ensure!(
        target
            .resource_usage()
            .host
            .controller
            .retained_preparations
            == 1
    );
    ensure!(
        catalog
            .pending_reservations(&catalog.read(TIMEOUT).await?, TIMEOUT)
            .await?
            .len()
            == 1
    );
    let fallback = target
        .activate_managed_learner_group(
            group,
            permit,
            LearnerActivation {
                from_s3: false,
                ..activation
            },
        )
        .await;
    ensure!(
        matches!(
            fallback,
            Err(CellGroupStartupError::Unconfirmed(
                CellGroupControllerError::Invalid("conflicting provision binding")
            ))
        ),
        "a retained S3 learner changed transfer modes: {fallback:?}"
    );
    ensure!(target.resource_usage().host.controller.groups == 0);
    eprintln!("S3 payload refusal witnessed before attachment; peer fallback refused; preparation and debit retained");
    let group = cluster.hosts[source].group(cluster.history)?;
    let status = group.replication_status(TIMEOUT).await?;
    let next = (source + 2) % 3;
    let transfer = LeadershipTransfer {
        target: MemberId::new(next as u64 + 1)?,
        expected_term: status.term,
        expected_revision: status.membership_revision,
    };
    ensure!(
        matches!(
            group.transfer_leadership(transfer, TIMEOUT).await,
            Err(RuntimeError::NotReady)
        ),
        "active repair handoff guard changed"
    );
    let report = cluster.hosts[source].close().await?;
    ensure!(report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers);
    let survivors = (0..3).filter(|i| *i != source).collect::<Vec<_>>();
    let hosts = survivors
        .iter()
        .map(|i| cluster.hosts[*i].clone())
        .collect::<Vec<_>>();
    let members = survivors.iter().map(|i| *i as u64 + 1).collect::<Vec<_>>();
    let ready = current_leader(&hosts, &members, cluster.history).await;
    if ready.is_err() {
        // The readiness deadline has already failed. Bound observations before
        // joined teardown so intermittent election/persistence failures retain
        // their actual owner state without extending or rescuing the attempt.
        for (i, host) in survivors.iter().zip(&hosts) {
            let status = match host.group(cluster.history) {
                Ok(group) => format!(
                    "{:?}",
                    tokio::time::timeout(Duration::from_millis(200), group.status()).await
                ),
                Err(error) => format!("{error:?}"),
            };
            let usage = host.resource_usage();
            eprintln!("S3 failover readiness failed: stopped={source} live={i} status={status} controller={:?} runtime={:?} sql={:?} transport={:?}",
                usage.host.controller, usage.runtime, usage.sql, usage.transport);
        }
    }
    let ready = ready.context("fresh ready successor after S3 source-service close")?;
    discover_survivors(cluster, Some(source)).await?;
    let selected = survivors[ready];
    let cut = catalog.read(TIMEOUT).await?;
    let group = catalog
        .authorize_group(&cut, &cluster.history, TIMEOUT)
        .await?;
    let permit = catalog
        .authorize_membership(&cut, &cluster.history, TIMEOUT)
        .await?;
    let retried = target
        .activate_managed_learner_group(
            group,
            permit,
            LearnerActivation {
                member: MemberId::new(4)?,
                source: MemberId::new(selected as u64 + 1)?,
                source_node: cluster.fixtures[selected].spec.node,
                from_s3: true,
                timeout: TIMEOUT,
            },
        )
        .await;
    ensure!(
        !matches!(
            retried,
            Err(CellGroupStartupError::Unconfirmed(
                CellGroupControllerError::Invalid("conflicting provision binding")
            ))
        ),
        "same boot/member/permit/mode refused a new source hint: {retried:?}"
    );
    // The other retained initial voter remains in both configurations even if
    // the temporary elected leader is the member this repair will remove.
    Ok((source + 2) % 3)
}

/// A surviving original voter is offline. Its retained physical/producer debt
/// cannot be released merely because repair finished on the remaining quorum.
pub(super) async fn verify_blocked_settlement(
    cluster: &ManagedCluster,
    seed: &SeededCells,
    execution: &FleetMovementExecutionRequest,
    observer: &RaftCells,
    obsolete: usize,
) -> Result<()> {
    let observed = observer
        .group(cluster.history)?
        .quorum_membership(TIMEOUT)
        .await?;
    let configuration = observed.configuration();
    let mut voters = configuration.voters.clone();
    voters.sort_unstable();
    ensure!(
        voters
            == (1..=4)
                .filter(|id| *id != obsolete as u64 + 1)
                .collect::<Vec<_>>()
    );
    ensure!(configuration.voters_outgoing.is_empty() && configuration.learners.is_empty());
    let status = observed
        .membership()
        .fleet_status()
        .context("repaired fleet status")?;
    ensure!(status.phase == Some(cellar::application::consensus::FleetPermitPhase::Complete));
    drop(observed);
    let catalog = &cluster.fixtures[0].catalog;
    let cut = catalog.read(TIMEOUT).await?;
    let pending = catalog.pending_reservations(&cut, TIMEOUT).await?;
    ensure!(pending.len() == 1 && pending[0].reservation == execution.reservation);
    ensure!(catalog.pending_movements(&cut, TIMEOUT).await?.len() == 1);
    ensure!(!cluster.fixtures[0]
        .store
        .fail_fleet_confirmation
        .load(Ordering::Acquire));
    let target = &cluster.hosts[3];
    ensure!(target.resource_usage().host.controller.groups == 1);
    ensure!(target.resource_usage().retained_manifest_cells == Some(2));
    for (i, (cell, receipt)) in seed.cells.iter().zip(&seed.receipts).enumerate() {
        let handle = target.cell(cell.clone())?;
        let restored = handle
            .query(
                ProposalId::new([222 + i as u8; 16])?,
                &handle.contract("Get")?,
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(receipt.version_token.clone()),
                    routing: QueryRouting::Local,
                    timeout: TIMEOUT,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(
            restored.result == [7i64, 11][i].to_be_bytes() && restored.version == receipt.version
        );
        ensure!(
            restored.served_by == MemberId::new(4)? && restored.served_by_node == target.identity()
        );
    }
    let members = voters
        .into_iter()
        .filter(|id| !cluster.hosts[*id as usize - 1].is_closed())
        .collect::<Vec<_>>();
    ensure!(members.len() == 2);
    let live = members
        .iter()
        .map(|id| cluster.hosts[*id as usize - 1].clone())
        .collect::<Vec<_>>();
    let ready = current_leader(&live, &members, cluster.history).await?;
    verify_cells(&live[ready], seed, 2).await?;
    ensure!(
        catalog
            .pending_reservations(&catalog.read(TIMEOUT).await?, TIMEOUT)
            .await?
            .len()
            == 1
    );
    eprintln!("Fresh quorum completed S3-backed repair; local replacement images/versions and S3 writes verified; offline survivor inventory blocks catalog settlement and retains debit");
    Ok(())
}
