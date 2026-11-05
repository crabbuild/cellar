// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A repaired member returns only through rooted maintenance and fresh resume.
use super::*;
use cellar::application::consensus::verification::{
    FleetRestartGateRequest, FleetRestartHistoryProgress, FleetRestartOfflineRequest,
    FleetRestartReadyRequest, FleetRestartRequest, FleetRestartResumeCompleteRequest,
    FleetRestartResumePhase, FleetRestartResumeProgress, FleetRestartResumeRequest,
    FleetRestartReturnRequest, FleetRestartRuntimeProgress, FleetRestartStopRequest,
};

#[path = "cell_group_managed_maintenance_loss.rs"]
mod loss;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_group_maintenance_stop_return_and_resume() -> Result<()> {
    for rooted in [false, true] {
        for joined in [false, true] {
            Box::pin(maintained(rooted, joined)).await?;
        }
    }
    Ok(())
}

fn intent(id: u8, revision: u64) -> FleetRequestIdentity {
    let mut identity = request(id, revision);
    identity.operation = [230; 16];
    identity
}

async fn maintained(rooted: bool, joined: bool) -> Result<()> {
    Box::pin(maintained_with_store(
        rooted,
        joined,
        Arc::new(fault::Store::default()),
    ))
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires explicit owned RustFS endpoint, bucket and isolated prefix"]
async fn rustfs_managed_archive_maintenance_stop_return_and_resume() -> Result<()> {
    for rooted in [false, true] {
        for joined in [false, true] {
            let store = Arc::new(fault::Store::around(rustfs::store(
                &format!("managed-archive-maintenance-joined-{joined}"),
                rooted,
            )?));
            Box::pin(maintained_with_store(rooted, joined, store)).await?;
        }
    }
    Ok(())
}

async fn maintained_with_store(rooted: bool, joined: bool, store: Arc<fault::Store>) -> Result<()> {
    Box::pin(maintained_with_loss(rooted, joined, store, false)).await
}

async fn maintained_with_loss(
    rooted: bool,
    joined: bool,
    store: Arc<fault::Store>,
    lose_observation: bool,
) -> Result<()> {
    let mut cluster = Box::pin(ManagedCluster::start_with_catalog_clients(
        rooted,
        &[7],
        ArchiveSetup::Initialized,
        store,
        lose_observation,
    ))
    .await?;
    let work = async {
        let (seed, obsolete) = replace(&cluster)
            .await
            .context("maintenance joined member repair")?;
        let target = if joined {
            3
        } else {
            leader(&cluster.hosts[..3], cluster.history).await?
        };
        cycle(&mut cluster, &seed, obsolete, target, lose_observation).await
    }
    .await;
    if work.is_err() {
        // Preserve actual local status before cleanup joins the hosts. These
        // bounded read-only samples never replace or clear the original error.
        for (index, host) in cluster.hosts.iter().enumerate() {
            let usage = host.resource_usage();
            eprintln!(
                "maintenance failure: host {index} control={:?} runtime={:?} host={:?}",
                usage.control, usage.runtime, usage.host
            );
            if let Ok(group) = host.group(cluster.history) {
                let sample = tokio::time::timeout(TIMEOUT, group.status()).await;
                eprintln!("maintenance failure: host {index} status={sample:?}");
            }
        }
    }
    let joined = close(std::mem::take(&mut cluster.nodes), &cluster.hosts).await;
    cluster.fixtures[0].domain.shutdown().await;
    if work.is_err() || joined.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!(
                "retained failed managed maintenance disk: {}",
                root.keep().display()
            );
        }
    }
    joined?;
    work
}

async fn current_survivor(
    cluster: &ManagedCluster,
    obsolete: usize,
    target: usize,
) -> Result<RaftCells> {
    let originals = (0..4)
        .filter(|i| *i != obsolete && *i != target)
        .collect::<Vec<_>>();
    let hosts = originals
        .iter()
        .map(|i| cluster.hosts[*i - usize::from(*i > target)].clone())
        .collect::<Vec<_>>();
    let members = originals.iter().map(|i| *i as u64 + 1).collect::<Vec<_>>();
    let index = current_leader(&hosts, &members, cluster.history).await?;
    Ok(hosts[index].clone())
}

async fn target_leader(cluster: &ManagedCluster, target: usize) -> Result<()> {
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let status = cluster.hosts[target]
                .group(cluster.history)?
                .status()
                .await?;
            if status.ready && status.leader == Some(target as u64 + 1) {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("observe actual maintenance target leadership")??;
    Ok(())
}

pub(super) async fn handoff_to(
    cluster: &ManagedCluster,
    obsolete: usize,
    target: usize,
) -> Result<()> {
    let member = MemberId::new(target as u64 + 1)?;
    let end = tokio::time::Instant::now() + TIMEOUT;
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let target_status = cluster.hosts[target]
                .group(cluster.history)?
                .status()
                .await?;
            if target_status.ready && target_status.leader == Some(member.get()) {
                return Ok::<_, anyhow::Error>(());
            }
            // A previous survivor sample is historical. Re-observe the actual
            // leader and current term/revision before each guarded admission.
            for (index, host) in cluster.hosts.iter().enumerate() {
                if index == obsolete || index == target {
                    continue;
                }
                let remaining = end
                    .checked_duration_since(tokio::time::Instant::now())
                    .context("maintenance handoff deadline")?;
                let group = host.group(cluster.history)?;
                let progress = match group.replication_status(remaining).await {
                    Ok(progress) => progress,
                    Err(
                        RuntimeError::NotLeader | RuntimeError::NotReady | RuntimeError::Overloaded,
                    ) => continue,
                    Err(error) => return Err(error.into()),
                };
                let remaining = end
                    .checked_duration_since(tokio::time::Instant::now())
                    .context("maintenance handoff deadline")?;
                match group
                    .transfer_leadership(
                        LeadershipTransfer {
                            target: member,
                            expected_term: progress.term,
                            expected_revision: progress.membership_revision,
                        },
                        remaining,
                    )
                    .await
                {
                    Ok(())
                    | Err(
                        RuntimeError::NotReady
                        | RuntimeError::NotLeader
                        | RuntimeError::Overloaded
                        | RuntimeError::DeadlineExceeded,
                    ) => (),
                    Err(error) => return Err(error.into()),
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("observe actual maintenance target leadership")??;
    Ok(())
}

async fn paused_write_refuses(host: &RaftCells, seed: &SeededCells) -> Result<()> {
    let cell = host.cell(seed.cells[0].clone())?;
    let id = CommandId::new(ClientId::new([202; 16])?, 1)?;
    let result = cell
        .command(
            &cell.contract("Add")?,
            9i64.to_be_bytes().to_vec(),
            CommandOptions::new(id, seed.retry),
        )
        .await;
    let Err(error) = result else {
        anyhow::bail!("paused maintenance owner admitted a business write");
    };
    ensure!(
        !error.ambiguous && error.cause.refusal() == Some(&CommandFailure::Unavailable),
        "paused write was not a definite admission refusal: {error:?}"
    );
    Ok(())
}

async fn cycle(
    cluster: &mut ManagedCluster,
    seed: &SeededCells,
    obsolete: usize,
    target_index: usize,
    lose_observation: bool,
) -> Result<()> {
    let member = MemberId::new(target_index as u64 + 1)?;
    let catalog = cluster.fixtures[0].catalog.clone();
    discover(cluster).await?;
    handoff_to(cluster, obsolete, target_index).await?;
    eprintln!(
        "maintenance: target member {} leader established",
        member.get()
    );
    let target = cluster.hosts[target_index].observe_node(TIMEOUT).await?;
    let boot = target.fleet_boot.context("maintenance target boot")?;
    let node = NodeId::new(boot.node)?;
    let process = ProcessGeneration::new(boot.process)?;
    let observer_index = (0..4)
        .find(|i| *i != obsolete && *i != target_index && (!lose_observation || *i > 0))
        .context("surviving observer")?;
    let observer = cluster.hosts[observer_index].clone();
    let cut = catalog.read(TIMEOUT).await?;
    let begin = FleetRestartRequest {
        identity: intent(220, cut.revision()),
        node,
        expected_process: process,
        grace_seconds: 120,
    };
    catalog
        .begin_restart_maintenance(
            &cut,
            &begin,
            std::slice::from_ref(&cluster.directory),
            TIMEOUT,
        )
        .await
        .context("begin joined maintenance")?;
    let cut = catalog.read(TIMEOUT).await?;
    ensure!(
        catalog.authorize_host(&cut, node, TIMEOUT).await?.state()
            == FleetNodeState::RestartPending
    );
    ensure!(catalog
        .authorize_capacity(&cut, node, TIMEOUT)
        .await
        .is_err());
    let gate = FleetRestartGateRequest {
        identity: intent(221, cut.revision()),
        node,
        expected_process: process,
        gate: ProposalId::new([222; 16])?,
    };
    catalog
        .admit_restart_gate(&cut, &gate, TIMEOUT)
        .await
        .context("root joined maintenance gate")?;
    discover(cluster).await?;
    let cut = catalog.read(TIMEOUT).await?;
    let mut walk = catalog
        .prepare_restart_verification(
            &cut,
            ProposalId::new(begin.identity.operation)?,
            std::slice::from_ref(&cluster.directory),
            &observer,
            TIMEOUT,
        )
        .await
        .context("prepare joined maintenance verification")?;
    let authority = catalog
        .authorize_group(&cut, &cluster.history, TIMEOUT)
        .await?;
    for (i, fixture) in cluster
        .fixtures
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != obsolete)
    {
        let sample = observer
            .observe_peer_fleet_membership(
                fixture.spec.node,
                authority.clone(),
                cut.revision(),
                TIMEOUT,
            )
            .await?;
        eprintln!(
            "maintenance: voter {} actual node leader load {:?}",
            i + 1,
            sample.progress().leader_load
        );
        let load = sample
            .progress()
            .leader_load
            .as_ref()
            .context("missing complete shared leader load")?;
        ensure!(load.capacity == 2 && load.leaders == u32::from(i == target_index));
    }
    eprintln!("maintenance: complete target inventory, start survivor verification");
    let mut handoffs = 0;
    let mut verified = 0;
    let mut waiting = false;
    let lost = Arc::new(fault::RegistryGate::default());
    let mut armed = false;
    let mut refused = false;
    let renew = async {
        tokio::time::timeout(TIMEOUT, async {
            loop {
                match walk.reconcile_next(TIMEOUT).await {
                    Ok(Some(FleetRestartRuntimeProgress::HandoffRequested {
                        group,
                        from,
                        to,
                        ..
                    })) => {
                        ensure!(group == cluster.history && from == member && to != from);
                        handoffs += 1;
                        eprintln!("maintenance: survivor handoff requested");
                        if lose_observation && !armed {
                            // The next foreground local-member observation loses
                            // its catalog reply. It is never resumed by the test.
                            lost.armed.store(true, Ordering::Release);
                            armed = true;
                        }
                    }
                    Ok(Some(FleetRestartRuntimeProgress::Verified(
                        FleetRestartHistoryProgress::Voting {
                            group,
                            member,
                            survivors,
                        },
                    ))) => {
                        ensure!(
                            group == cluster.history
                                && member == MemberId::new(target_index as u64 + 1)?
                                && survivors[0] != survivors[1]
                                && !survivors.contains(&node)
                        );
                        verified += 1;
                    }
                    Ok(None) => break,
                    Err(
                        FleetCatalogError::Unconfirmed
                        | FleetCatalogError::Overloaded
                        | FleetCatalogError::InventoryIncomplete,
                    )
                    | Err(FleetCatalogError::Observation(
                        InspectionError::Unavailable | InspectionError::Overloaded,
                    )) => {
                        refused = true;
                        if !waiting {
                            eprintln!("maintenance: survivor proof not yet available");
                            waiting = true;
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    other => anyhow::bail!("joined maintenance verification: {other:?}"),
                }
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("renew joined maintenance survivor proof")??;
        Ok::<_, anyhow::Error>(())
    };
    if lose_observation {
        fault::gate_store_read(
            cluster.fixtures[observer_index].store.as_ref(),
            "/fleet/head".into(),
            lost.clone(),
            renew,
        )
        .await?;
        ensure!(
            armed && refused && !lost.armed.load(Ordering::Acquire),
            "lost member observation was not entered and retried"
        );
    } else {
        renew.await?;
    }
    ensure!(handoffs == 1);
    if lose_observation {
        // This regression ends at its owning renewal boundary. Full
        // stop/return/resume remains required by the original lifecycle case.
        ensure!(verified == 1);
        return Ok(());
    }
    let cut = catalog.read(TIMEOUT).await?;
    let ready = FleetRestartReadyRequest {
        identity: intent(223, cut.revision()),
        node,
        expected_process: process,
    };
    eprintln!("maintenance: survivors verified; publish readiness");
    let readiness = walk
        .publish_ready(&ready, TIMEOUT)
        .await
        .context("publish joined readiness")?;
    ensure!(
        readiness.candidate().histories() == 1
            && readiness.candidate().physical_histories() == 1
            && readiness.candidate().voting_histories() == 1
            && readiness.candidate().matched_histories() == 1
    );
    let cut = catalog.read(TIMEOUT).await?;
    paused_write_refuses(&cluster.hosts[target_index], seed)
        .await
        .context("quiesced business admission")?;
    // Preserve the nested-RPC regression: this stop originates at the actual
    // survivor leader. Its fresh matched proof inspects the stopping node while
    // the stopping node owns the stop-renewal action back to that leader.
    target_leader(cluster, observer_index).await?;
    let stop = FleetRestartStopRequest {
        identity: intent(224, cut.revision()),
        ready: ready.identity,
        node,
        expected_process: process,
        stop: ProposalId::new([225; 16])?,
    };
    let stop_admission = catalog.admit_restart_stop(&cut, &stop, TIMEOUT).await?;
    let cut = catalog.read(TIMEOUT).await?;
    let premature = FleetRestartOfflineRequest {
        identity: intent(234, cut.revision()),
        stop: stop.clone(),
    };
    ensure!(matches!(
        catalog
            .confirm_restart_offline(&cut, &premature, TIMEOUT)
            .await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    ensure!(catalog.read(TIMEOUT).await?.revision() == cut.revision());
    discover(cluster).await?;
    let cut = catalog.read(TIMEOUT).await?;
    // Overload before submission may be retried. Once proof work is queued,
    // Unconfirmed is reconciled by actual join and signed closure below, not
    // by flooding the same bounded metadata bank with more stop submissions.
    let result = tokio::time::timeout(TIMEOUT, async {
        loop {
            match catalog
                .stop_restart_maintenance(&cut, &stop, &observer, TIMEOUT)
                .await
            {
                Err(FleetCatalogError::Overloaded) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                result => break result,
            }
        }
    })
    .await
    .context("submit exact owned stop within original deadline")?;
    ensure!(
        result.is_ok()
            || matches!(
                result,
                Err(FleetCatalogError::Unconfirmed | FleetCatalogError::Closed)
            ),
        "owned maintenance stop refused: {:?}",
        result.err()
    );
    eprintln!("maintenance: owned stop submitted; join target");
    // Leave the node in the cluster's cleanup owner until its join succeeds.
    let outcome = tokio::time::timeout(TIMEOUT, cluster.nodes[target_index].finished())
        .await
        .context("join actual stopped maintenance node")??;
    let _old_node = cluster.nodes.remove(target_index);
    let old_host = cluster.hosts.remove(target_index);
    eprintln!("maintenance: target closed and joined");
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("joined owner did not stop: {outcome:?}");
    };
    let closed = report.raft.context("joined maintenance stop report")?;
    ensure!(
        !closed.startup_incomplete
            && closed.result?.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
    );
    ensure!(old_host.close().await?.groups_total == 1);
    let closure = catalog.restart_stop_closure(&stop, TIMEOUT).await?;
    ensure!(
        closure.node() == boot.node
            && closure.process() == boot.process
            && closure.operation() == begin.identity.operation
            && closure.request() == stop.stop.bytes()
    );
    let cut = catalog.read(TIMEOUT).await?;
    let offline = FleetRestartOfflineRequest {
        identity: intent(226, cut.revision()),
        stop: stop.clone(),
    };
    catalog
        .confirm_restart_offline(&cut, &offline, TIMEOUT)
        .await
        .context("confirm joined owner offline")?;
    eprintln!("maintenance: offline root confirmed");
    let survivor = current_survivor(cluster, obsolete, target_index)
        .await
        .context("find actual post-stop survivor leader")?;
    verify_cells(&survivor, seed, 3)
        .await
        .context("survivor SQL while target offline")?;
    eprintln!("maintenance: survivor writes while target offline");
    let cut = catalog.read(TIMEOUT).await?;
    let returning = FleetRestartReturnRequest {
        identity: intent(227, cut.revision()),
        offline: offline.identity,
        node,
        expected_process: process,
        next_process: ProcessGeneration::new([54; 16])?,
    };
    catalog
        .admit_restart_return(&cut, &returning, TIMEOUT)
        .await
        .context("admit joined return")?;
    let f = &cluster.fixtures[target_index];
    let resumed_node = start_node(
        cluster.roots[target_index].path(),
        f.application.clone(),
        &f.spec.label,
    )
    .await?;
    let (mut config, services) = f.config(&resumed_node, LogOpenMode::Resume, 54).await?;
    archives::configure(&mut config);
    config.groups.push(RaftGroupAssignment {
        history: cluster.history,
        placement: RaftGroupPlacement::Replica,
    });
    let resumed_host = match resumed_node.start_raft_cells(config, services).await {
        Ok(host) => host,
        Err(error) => {
            let joined = resumed_node.shutdown(ShutdownMode::Preserve).await;
            return Err(error).context(format!(
                "canonical retained maintenance return; startup cleanup={joined:?}"
            ));
        }
    };
    cluster.nodes.insert(target_index, resumed_node);
    cluster.hosts.insert(target_index, resumed_host);
    eprintln!("maintenance: retained return attached; discover new boot");
    discover(cluster)
        .await
        .context("discover returned paused node and current peers")?;
    eprintln!("maintenance: returned paused node discovered");
    let view = cluster.hosts[target_index].observe_node(TIMEOUT).await?;
    ensure!(
        view.node.profile.draining
            && view.node.storage_instance.bytes() == boot.engine
            && view.node.groups == 1
            && cluster.hosts[target_index]
                .resource_usage()
                .retained_manifest_cells
                == Some(2)
    );
    let mut planned_profile = view.node.profile.clone();
    planned_profile.draining = false;
    let attestation = FleetCapacityAttestation {
        boot: view.fleet_boot.context("returned joined boot")?,
        profile_revision: 1,
        profile_digest: fleet_capacity_profile_digest(&planned_profile)?,
        cpu_millis: 1000,
        memory_bytes: 3 * 1024 * 1024 * 1024,
        durable_bytes: planned_profile.byte_capacity.max(1024 * 1024 * 1024),
        engine_slots: 2,
        memory_swap_disabled: true,
        used_memory_bytes: 64 * 1024 * 1024,
        used_durable_bytes: 1024 * 1024,
        evidence_digest: [71; 32],
        evidence_reference: "fixture-only/deployment-attestation".into(),
    };
    ensure!(
        attestation.boot.process == returning.next_process.bytes()
            && attestation.boot.engine == boot.engine
    );
    let cut = catalog.read(TIMEOUT).await?;
    ensure!(catalog
        .authorize_capacity(&cut, node, TIMEOUT)
        .await
        .is_err());
    paused_write_refuses(&cluster.hosts[target_index], seed)
        .await
        .context("returned paused business admission")?;
    ensure!(matches!(
        observer
            .request_peer_fleet_stop(stop_admission.stop_request()?, cut.revision(), TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(
        !cluster.nodes[target_index].is_closed()
            && catalog.read(TIMEOUT).await?.revision() == cut.revision()
    );
    let resume = FleetRestartResumeRequest {
        identity: intent(228, cut.revision()),
        returning: returning.identity,
        attestation: attestation.clone(),
        resume: ProposalId::new([229; 16])?,
    };
    let mut walk = catalog
        .prepare_restart_resume(
            &cut,
            &resume,
            std::slice::from_ref(&cluster.directory),
            &observer,
            TIMEOUT,
        )
        .await
        .context("prepare joined resume")?;
    ensure!(walk.phase() == FleetRestartResumePhase::Paused);
    eprintln!("maintenance: verify returned paused voting history");
    tokio::time::timeout(TIMEOUT, async {
        loop {
            match walk.reconcile_next(TIMEOUT).await {
                Ok(Some(FleetRestartResumeProgress::Verified {
                    group,
                    current_member,
                    retained_member,
                    replaced,
                })) => ensure!(
                    group == cluster.history
                        && current_member == Some(member)
                        && retained_member.is_none()
                        && !replaced
                ),
                Ok(None) => break,
                Err(
                    FleetCatalogError::Unconfirmed
                    | FleetCatalogError::Overloaded
                    | FleetCatalogError::InventoryIncomplete,
                )
                | Err(FleetCatalogError::Observation(
                    InspectionError::Unavailable | InspectionError::Overloaded,
                )) => tokio::time::sleep(Duration::from_millis(10)).await,
                other => anyhow::bail!("joined resume verification: {other:?}"),
            }
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("renew returned paused voting proof")??;
    ensure!(walk.verified_histories() == 1);
    let consent = walk
        .admit_resume(&resume, TIMEOUT)
        .await
        .context("admit joined resume")?;
    verify_cells(&survivor, seed, 4).await?;
    discover(cluster).await?;
    let before_resume = catalog.read(TIMEOUT).await?;
    let mut wrong_child = serde_json::to_value(consent.resume_request()?)?;
    wrong_child["selector"]["request"] = serde_json::json!(vec![235; 16]);
    ensure!(matches!(
        observer
            .request_peer_fleet_stop(
                serde_json::from_value(wrong_child)?,
                before_resume.revision(),
                TIMEOUT,
            )
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    ensure!(catalog.read(TIMEOUT).await?.revision() == before_resume.revision());
    ensure!(
        cluster.hosts[target_index]
            .observe_node(TIMEOUT)
            .await?
            .node
            .profile
            .draining
    );
    let resumed = tokio::time::timeout(TIMEOUT, async {
        loop {
            match catalog
                .resume_restart_maintenance(&consent, &observer, TIMEOUT)
                .await
            {
                Err(FleetCatalogError::Overloaded) => {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                result => break result,
            }
        }
    })
    .await
    .context("submit exact owned resume within original deadline")?;
    ensure!(
        resumed.is_ok() || matches!(resumed, Err(FleetCatalogError::Unconfirmed)),
        "owned resume refused: {:?}",
        resumed.err()
    );
    // This local hint waits without retaining another peer RPC. It grants no
    // authority: the fresh Resumed walk and terminal root checks below remain
    // required before capacity or successful return is claimed.
    tokio::time::timeout(TIMEOUT, async {
        loop {
            if !cluster.hosts[target_index]
                .observe_node(TIMEOUT)
                .await?
                .node
                .profile
                .draining
            {
                break Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("observe actual owned resume barrier")??;
    let cut = catalog.read(TIMEOUT).await?;
    ensure!(
        catalog.authorize_host(&cut, node, TIMEOUT).await?.state()
            == FleetNodeState::RestartPending
            && catalog
                .authorize_capacity(&cut, node, TIMEOUT)
                .await
                .is_err()
    );
    ensure!(
        !cluster.hosts[target_index]
            .observe_node(TIMEOUT)
            .await?
            .node
            .profile
            .draining
    );
    let mut completed = catalog
        .prepare_restart_resume(
            &cut,
            &resume,
            std::slice::from_ref(&cluster.directory),
            &observer,
            TIMEOUT,
        )
        .await?;
    ensure!(completed.phase() == FleetRestartResumePhase::Resumed);
    let progress = completed
        .reconcile_next(TIMEOUT)
        .await?
        .context("resumed joined history")?;
    ensure!(
        matches!(progress, FleetRestartResumeProgress::Verified { group, current_member: Some(member), retained_member: None, replaced: false } if group == cluster.history && member == MemberId::new(target_index as u64 + 1)?)
    );
    ensure!(completed.reconcile_next(TIMEOUT).await?.is_none());
    let cut = catalog.read(TIMEOUT).await?;
    let terminal = FleetRestartResumeCompleteRequest {
        identity: intent(231, cut.revision()),
        resume: resume.identity,
    };
    let done = completed
        .complete_resume(&terminal, TIMEOUT)
        .await
        .context("settle joined resume")?;
    ensure!(done.intact() && done.retained_copies() == 0 && done.replaced_histories() == 0);
    let cut = catalog.read(TIMEOUT).await?;
    ensure!(catalog.authorize_host(&cut, node, TIMEOUT).await?.state() == FleetNodeState::Active);
    let capacity = catalog.authorize_capacity(&cut, node, TIMEOUT).await?;
    ensure!(capacity.receive_replicas() && capacity.attestation() == &attestation);
    discover(cluster).await?;
    handoff_to(cluster, obsolete, target_index).await?;
    verify_cells(&cluster.hosts[target_index], seed, 5)
        .await
        .context("actual resumed owner serves both cells")?;
    let next = FleetRestartRequest {
        identity: {
            let mut i = request(232, cut.revision());
            i.operation = [233; 16];
            i
        },
        node,
        expected_process: returning.next_process,
        grace_seconds: 120,
    };
    catalog
        .begin_restart_maintenance(
            &cut,
            &next,
            std::slice::from_ref(&cluster.directory),
            TIMEOUT,
        )
        .await
        .context("terminal resume releases disruption slot")?;
    Ok(())
}
