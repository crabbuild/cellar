// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! S3 seed failure, successor and physical archive-debt oracles.

use super::{movement::*, *};
use cellar::application::consensus::verification::TransportError;

pub async fn refuse_then_failover(
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
    *store.registry_denial_on_payload_refusal.lock().unwrap() =
        Some(cluster.fixtures[source].store.deny_registry_writes.clone());
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
    wait_for_seed_debt(&cluster.hosts[source], true)
        .await
        .context("failed seed retirement did not retain observable debt")?;
    ensure!(
        cluster.fixtures[source]
            .store
            .denied_registry_writes
            .load(Ordering::Acquire)
            > 0
    );
    let journals = seed_journals(&cluster.nodes[source])?;
    let pending = journals
        .iter()
        .map(|p| seed_pending(p))
        .collect::<Result<Vec<_>>>()?;
    ensure!(pending.into_iter().any(|pending| pending));
    cluster.fixtures[source]
        .store
        .deny_registry_writes
        .store(false, Ordering::Release);
    // No seed request follows the outage recovery. The node must discover and
    // retire its abandoned generation through its own bounded cleanup owner.
    wait_for_seed_debt(&cluster.hosts[source], false)
        .await
        .context("idle node did not retry abandoned seed retirement")?;
    for journal in &journals {
        ensure!(
            !seed_pending(journal)?,
            "seed intent was not durably retired"
        );
    }
    let denied = store.denied_group_payload_reads.load(Ordering::Acquire);
    store
        .deny_group_payload_reads
        .store(true, Ordering::Release);
    let second = target
        .activate_managed_learner_group(group.clone(), permit.clone(), activation.clone())
        .await;
    store
        .deny_group_payload_reads
        .store(false, Ordering::Release);
    ensure!(
        matches!(
            second,
            Err(CellGroupStartupError::Unconfirmed(
                CellGroupControllerError::Archive(_)
            ))
        ),
        "second physical S3 refusal changed: {second:?}"
    );
    ensure!(store.denied_group_payload_reads.load(Ordering::Acquire) > denied);
    wait_for_seed_debt(&cluster.hosts[source], true).await?;
    ensure!(journals
        .iter()
        .map(|p| seed_pending(p))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .any(|pending| pending));
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
    // The original source has joined with its journal still pending. Recover
    // writes now so the surviving quorum can repair; source 0's fault wrapper
    // also surrounds the shared backend. No stopped source can retry cleanup.
    cluster.fixtures[source]
        .store
        .deny_registry_writes
        .store(false, Ordering::Release);
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

async fn wait_for_seed_debt(host: &RaftCells, pending: bool) -> Result<()> {
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let usage = host.resource_usage().host.controller;
            if usage.active_archive_seeds == 0 && (usage.archive_seed_cleanup_errors > 0) == pending
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}

fn seed_journals(node: &Node) -> Result<Vec<std::path::PathBuf>> {
    let root = node.info().data_dir.join("raft/cell-group-seed-archives");
    let journals = std::fs::read_dir(root)?
        .map(|entry| entry.map(|entry| entry.path().join("state")))
        .collect::<std::io::Result<Vec<_>>>()?;
    ensure!(!journals.is_empty(), "no physical seed journal");
    Ok(journals)
}

fn seed_pending(path: &std::path::Path) -> Result<bool> {
    let record: serde_json::Value = serde_json::from_slice(&std::fs::read(path)?)?;
    let pending = record
        .get("record")
        .and_then(|r| r.get("pending"))
        .context("seed journal pending record missing")?;
    Ok(pending != "None")
}

pub struct SeedDebt {
    source: usize,
    journals: Vec<std::path::PathBuf>,
    identities: Vec<Vec<u8>>,
}

pub fn retained_seed_debt(cluster: &ManagedCluster) -> Result<SeedDebt> {
    let sources = cluster.hosts[..3]
        .iter()
        .enumerate()
        .filter(|(_, host)| host.is_closed())
        .map(|(i, _)| i)
        .collect::<Vec<_>>();
    ensure!(
        sources.len() == 1,
        "seed retirement outage did not select one source"
    );
    let source = sources[0];
    ensure!(cluster.hosts[source].is_closed());
    let journals = seed_journals(&cluster.nodes[source])?;
    let mut identities = Vec::new();
    for journal in &journals {
        ensure!(seed_pending(journal)?, "source close discarded seed debt");
        identities.push(std::fs::read(journal.with_file_name("identity"))?);
    }
    Ok(SeedDebt {
        source,
        journals,
        identities,
    })
}

pub async fn restart_seed_cleanup(cluster: &ManagedCluster, debt: SeedDebt) -> Result<()> {
    // Every old host has joined. Reopen only the source: cleanup must not need
    // a live quorum, a learner request or new activation authority.
    ensure!(cluster.hosts.iter().all(RaftCells::is_closed));
    let f = &cluster.fixtures[debt.source];
    f.store.deny_registry_writes.store(true, Ordering::Release);
    let node = start_node(
        cluster.roots[debt.source].path(),
        f.application.clone(),
        &f.spec.label,
    )
    .await?;
    let started = async {
        let (mut config, services) = f
            .config(&node, LogOpenMode::Resume, 94 + debt.source as u8)
            .await?;
        archives::configure(&mut config);
        config.groups.push(RaftGroupAssignment {
            history: cluster.history,
            placement: RaftGroupPlacement::Replica,
        });
        node.start_raft_cells(config, services).await
    }
    .await;
    let host = match started {
        Ok(host) => host,
        Err(error) => {
            let stopped = node.shutdown(ShutdownMode::Preserve).await;
            return Err(error.context(format!(
                "seed cleanup retained startup; shutdown={stopped:?}"
            )));
        }
    };
    let work = async {
        // Inventory is available while the recovered engine is open. Closing
        // seals its I/O admission, independently of cleanup's physical joins.
        ensure!(host.resource_usage().retained_manifest_cells == Some(2));
        wait_for_seed_debt(&host, true)
            .await
            .context("restart did not discover existing seed debt")?;
        for (journal, identity) in debt.journals.iter().zip(&debt.identities) {
            ensure!(seed_pending(journal)?);
            ensure!(&std::fs::read(journal.with_file_name("identity"))? == identity);
        }
        let cut = f.catalog.read(TIMEOUT).await?;
        let gate = &f.store.group_archive_read;
        gate.armed.store(true, Ordering::Release);
        let release = ResumeSeedRead(gate);
        tokio::time::timeout(TIMEOUT, gate.entered.notified())
            .await
            .context("restart cleanup did not enter the actual archive read")?;
        let closing_host = host.clone();
        let mut closing = tokio::spawn(async move { closing_host.close().await });
        ensure!(
            tokio::time::timeout(Duration::from_millis(30), &mut closing)
                .await
                .is_err(),
            "close released an entered seed cleanup read"
        );
        ensure!(host.is_closed(), "close waiter did not seal admission");
        closing.abort();
        ensure!(matches!(closing.await, Err(error) if error.is_cancelled()));
        let usage = host.resource_usage().host.controller;
        ensure!(usage.active_archive_seed_cleanup == 1 && usage.s3_bytes >= 8192);
        f.store.deny_registry_writes.store(false, Ordering::Release);
        drop(release);
        wait_for_seed_debt(&host, false)
            .await
            .context("restart did not retire seed debt without quorum")?;
        for journal in &debt.journals {
            ensure!(!seed_pending(journal)?);
        }
        ensure!(
            f.catalog.read(TIMEOUT).await?.revision() == cut.revision(),
            "seed cleanup changed fleet authority"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let stopped = close(vec![node], std::slice::from_ref(&host)).await;
    match (work, stopped) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) | (Ok(()), Err(error)) => Err(error),
        (Err(error), Err(stopped)) => Err(anyhow::anyhow!(
            "seed restart: {error:#}; joined shutdown: {stopped:#}"
        )),
    }
}

struct ResumeSeedRead<'a>(&'a fault::RegistryGate);

impl Drop for ResumeSeedRead<'_> {
    fn drop(&mut self) {
        self.0.armed.store(false, Ordering::Release);
        self.0.resume.notify_one();
    }
}

/// A surviving original voter is offline. Its retained physical/producer debt
/// cannot be released merely because repair finished on the remaining quorum.
pub async fn verify_blocked_settlement(
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
