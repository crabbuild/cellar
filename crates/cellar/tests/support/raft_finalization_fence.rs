// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Root boot fencing uses actual managed processes, engine ownership and CAS.
//! The resulting admission supplies no physical-stop or terminal certificate.
use super::super::super::retirement::stopped;
use super::*;
use cellar::application::consensus::verification::FleetEvacuationCandidate;

fn request(operation: ProposalId, revision: u64) -> FleetRequestIdentity {
    FleetRequestIdentity {
        id: [230; 16],
        operation: operation.bytes(),
        principal: "finalization-test".into(),
        expected_revision: revision,
    }
}

async fn candidate(
    s: &Started,
) -> Result<(NamespaceCatalog, ProposalId, FleetEvacuationCandidate)> {
    let (namespace, operation, mut verification) = begin(s).await?;
    let fixed = verification
        .freeze_live(&s.observer, survivors(s), TIMEOUT)
        .await?;
    let reference = s.reference(&fixed).await?;
    verification
        .verify_live(&s.observer, survivors(s), archive(s, &reference), TIMEOUT)
        .await?;
    Ok((
        namespace,
        operation,
        verification.publish_candidate(TIMEOUT).await?,
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_boot_epoch_survives_node_state_changes() -> Result<()> {
    let f = Fixture::new().await?;
    let (_root, node, host) = f.empty_host().await?;
    let (_peer_root, peer_node, _peer) = f.peer_host(5).await?;
    let key = hex::encode([5; 16]);
    let root = f.catalog.read(TIMEOUT).await?;
    let bytes = f
        .catalog
        .get(&root, FleetTable::Nodes, &key, TIMEOUT)
        .await?
        .context("node record")?;
    let original: serde_json::Value = serde_json::from_slice(&bytes[13..bytes.len() - 32])?;
    ensure!(original["boot"].is_object());
    for (i, state) in [FleetNodeState::Active, FleetNodeState::Draining]
        .into_iter()
        .enumerate()
    {
        let revision = f.state(NodeId::new([5; 16])?, state, 70 + i as u8).await?;
        let root = f.catalog.read(TIMEOUT).await?;
        let bytes = f
            .catalog
            .get(&root, FleetTable::Nodes, &key, TIMEOUT)
            .await?
            .context("updated node")?;
        let current: serde_json::Value = serde_json::from_slice(&bytes[13..bytes.len() - 32])?;
        ensure!(current["boot"] == original["boot"]);
        ensure!(
            current["epoch"].as_u64().unwrap()
                == original["epoch"].as_u64().unwrap() + i as u64 + 1
        );
        f.catalog
            .authorize_host(&root, NodeId::new([5; 16])?, TIMEOUT)
            .await?;
        ensure!(
            f.remote(&host, NodeId::new([5; 16])?, revision)
                .await?
                .view
                .current_enrollment
                .unwrap()
                .state
                == state
        );
    }
    stopped(peer_node).await?;
    stopped(node).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_finalization_boot_fence_and_retry() -> Result<()> {
    let s = Box::pin(Started::new()).await?;
    let (namespace, operation, candidate) = candidate(&s).await?;
    let before = s.observer.observe_node(TIMEOUT).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let identity = request(operation, root.revision());
    ensure!(matches!(
        candidate
            .begin_finalization(&root, &identity, &s.nodes[0].host, &[], TIMEOUT,)
            .await,
        Err(FleetCatalogError::InventoryIncomplete)
    ));
    s.f.store.fail_fleet_after.store(true, Ordering::Release);
    let fence = candidate
        .begin_finalization(
            &root,
            &identity,
            &s.nodes[0].host,
            std::slice::from_ref(&namespace),
            TIMEOUT,
        )
        .await?;
    ensure!(
        fence.node()? == before.node.identity.node && fence.disk()? == before.node.identity.disk
    );
    ensure!(
        fence.process()? == before.node.identity.process
            && fence.storage_instance()? == before.node.storage_instance
    );
    ensure!(fence.enrollment_epoch() == before.current_enrollment.unwrap().epoch + 1);
    ensure!(
        fence.applied_revision() == root.revision() + 1
            && fence.candidate_digest() == candidate.digest()
    );
    // Using the departing host as observer would require an invalid self RPC.
    // Exact committed reconciliation must instead return the original result.
    let retry = candidate
        .begin_finalization(&root, &identity, &s.observer, &[], TIMEOUT)
        .await?;
    ensure!(
        retry.applied_revision() == fence.applied_revision()
            && retry.enrollment_epoch() == fence.enrollment_epoch()
    );
    let reopened = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let now = reopened.read(TIMEOUT).await?;
    let observed = reopened
        .evacuation_finalization(&now, operation, TIMEOUT)
        .await?;
    ensure!(
        observed.process()? == fence.process()?
            && observed.applied_revision() == fence.applied_revision()
    );
    ensure!(matches!(
        reopened.authorize_host(&now, fence.node()?, TIMEOUT).await,
        Err(FleetCatalogError::Conflict)
    ));
    let mut changed = identity.clone();
    changed.principal.push_str("-different");
    ensure!(matches!(
        candidate
            .begin_finalization(&root, &changed, &s.observer, &[], TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    refresh_trust(&s).await?;
    let current = s
        .observer
        .observe_node(TIMEOUT)
        .await?
        .current_enrollment
        .context("fenced enrollment")?;
    ensure!(
        current.state == FleetNodeState::Finalizing && current.epoch == fence.enrollment_epoch()
    );
    let second = FleetRequestIdentity {
        id: [232; 16],
        operation: [233; 16],
        principal: "second-disruption".into(),
        expected_revision: now.revision(),
    };
    ensure!(matches!(
        reopened
            .begin_evacuation(
                &now,
                &second,
                s.nodes[0].spec.node,
                std::slice::from_ref(&namespace),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::DisruptionInProgress { .. })
    ));
    write_read(&s.f, &s.cells, 1, 11, 11).await?;
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_finalization_blocks_intact_boot() -> Result<()> {
    let s = Box::pin(Started::new()).await?;
    let (namespace, operation, candidate) = candidate(&s).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let authorization =
        s.f.catalog
            .authorize_host(&root, NodeId::new([7; 16])?, TIMEOUT)
            .await?;
    let identity = request(operation, root.revision());
    let fence = candidate
        .begin_finalization(
            &root,
            &identity,
            &s.nodes[0].host,
            std::slice::from_ref(&namespace),
            TIMEOUT,
        )
        .await?;
    stopped(s.observer_node).await?;
    let spec = s.f.peer_spec(7)?;
    let node = public_node::node_named(
        s._observer_root.path(),
        s.f.definition.application.clone(),
        &spec.label,
    )
    .await?;
    let (mut config, services) = s.f.config_for(&node, authorization, &spec).await?;
    config.infrastructure.log.mode = LogOpenMode::Resume;
    config.infrastructure.log.process = ProcessGeneration::new([127; 16])?;
    config.peers[0].process = config.infrastructure.log.process;
    let refused = node.start_raft_cells(config, services).await;
    ensure!(
        refused.is_err(),
        "an intact engine booted after its rooted finalization fence"
    );
    node.shutdown(ShutdownMode::Preserve).await?;
    let now = s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        s.f.catalog
            .evacuation_finalization(&now, operation, TIMEOUT)
            .await?
            .enrollment_epoch()
            == fence.enrollment_epoch()
    );
    ensure!(s.scratch.usage()?.generations == 0);
    stop(s.nodes).await?;
    s.f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_finalization_new_boot_invalidates_candidate() -> Result<()> {
    let s = Box::pin(Started::new()).await?;
    let (namespace, operation, old_candidate) = candidate(&s).await?;
    let before = s.observer.observe_node(TIMEOUT).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let authorization =
        s.f.catalog
            .authorize_host(&root, NodeId::new([7; 16])?, TIMEOUT)
            .await?;
    // Move the actual old owner out without temporarily fabricating a Node.
    // A replacement holder is obtained from an independent fresh process below.
    let Started {
        f,
        nodes,
        cells,
        _observer_root,
        observer_node,
        observer,
        proposal,
        authorization: group,
        archive: cell_archive,
        _scratch_root,
        scratch,
        verifier,
    } = s;
    stopped(observer_node).await?;
    let spec = f.peer_spec(7)?;
    let node = public_node::node_named(
        _observer_root.path(),
        f.definition.application.clone(),
        &spec.label,
    )
    .await?;
    let (mut config, services) = f.config_for(&node, authorization, &spec).await?;
    config.infrastructure.log.mode = LogOpenMode::Resume;
    config.infrastructure.log.process = ProcessGeneration::new([127; 16])?;
    config.peers[0].process = config.infrastructure.log.process;
    let host = node.start_raft_cells(config, services).await?;
    let s = Started {
        f,
        nodes,
        cells,
        _observer_root,
        observer_node: node,
        observer: host,
        proposal,
        authorization: group,
        archive: cell_archive,
        _scratch_root,
        scratch,
        verifier,
    };
    drop(observer);
    refresh_trust(&s).await?;
    let current = s.observer.observe_node(TIMEOUT).await?;
    ensure!(current.node.identity.process != before.node.identity.process);
    ensure!(current.node.storage_instance == before.node.storage_instance);
    ensure!(
        current.current_enrollment.as_ref().unwrap().epoch
            > before.current_enrollment.unwrap().epoch
    );
    let now = s.f.catalog.read(TIMEOUT).await?;
    ensure!(matches!(
        old_candidate
            .begin_finalization(
                &now,
                &request(operation, now.revision()),
                &s.nodes[0].host,
                std::slice::from_ref(&namespace),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    let mut verification = s
        .f
        .catalog
        .prepare_evacuation_verification(&now, operation, std::slice::from_ref(&namespace), TIMEOUT)
        .await?;
    let fixed = verification
        .freeze_live(&s.observer, survivors(&s), TIMEOUT)
        .await?;
    let reference = s.reference(&fixed).await?;
    verification
        .verify_live(&s.observer, survivors(&s), archive(&s, &reference), TIMEOUT)
        .await?;
    let current_candidate = verification.publish_candidate(TIMEOUT).await?;
    let now = s.f.catalog.read(TIMEOUT).await?;
    let fence = current_candidate
        .begin_finalization(
            &now,
            &request(operation, now.revision()),
            &s.nodes[0].host,
            std::slice::from_ref(&namespace),
            TIMEOUT,
        )
        .await?;
    ensure!(fence.process()? == current.node.identity.process);
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_finalization_inflight_boot_wins_cas() -> Result<()> {
    let s = Box::pin(Started::new()).await?;
    let (namespace, operation, candidate) = candidate(&s).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let identity = request(operation, root.revision());
    let observer = s.nodes[0].host.clone();
    let reopened = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let cut = reopened.read(TIMEOUT).await?;
    let boot = reopened
        .authorize_host(&cut, NodeId::new([7; 16])?, TIMEOUT)
        .await?;
    let Started {
        f,
        nodes,
        cells,
        _observer_root,
        observer_node,
        observer: old_host,
        proposal: _,
        authorization: _,
        archive: _,
        _scratch_root,
        scratch,
        verifier: _,
    } = s;
    f.store.fleet_cas_hang.store(true, Ordering::Release);
    f.store.hold_next_fleet_cas.store(true, Ordering::Release);
    let fencing = candidate.begin_finalization(
        &root,
        &identity,
        &observer,
        std::slice::from_ref(&namespace),
        Duration::from_secs(20),
    );
    let restart = async {
        let result = async {
            f.domain
                .run(cellar::asyncrt::timeout(TIMEOUT, async {
                    while !f.store.fleet_cas_waiting.load(Ordering::Acquire) {
                        cellar::asyncrt::sleep(Duration::from_millis(5)).await;
                    }
                }))
                .await??;
            stopped(observer_node).await?;
            let spec = f.peer_spec(7)?;
            let node = public_node::node_named(
                _observer_root.path(),
                f.definition.application.clone(),
                &spec.label,
            )
            .await?;
            let (mut config, services) = f.config_for(&node, boot, &spec).await?;
            config.infrastructure.log.mode = LogOpenMode::Resume;
            config.infrastructure.log.process = ProcessGeneration::new([127; 16])?;
            config.peers[0].process = config.infrastructure.log.process;
            let host = node.start_raft_cells(config, services).await?;
            Ok::<_, anyhow::Error>((node, host))
        }
        .await;
        // Release the original request even if the restart failed. It remains
        // subject to its original deadline and conditional-write precondition.
        f.store.fleet_cas_hang.store(false, Ordering::Release);
        f.store.fleet_cas_resume.notify_waiters();
        result
    };
    let (fenced, restarted) = tokio::join!(fencing, restart);
    let (node, host) = restarted?;
    ensure!(
        matches!(fenced, Err(FleetCatalogError::Conflict)),
        "held finalization did not lose to the admitted boot"
    );
    let now = reopened.read(TIMEOUT).await?;
    ensure!(now.revision() == root.revision() + 1);
    ensure!(reopened
        .get(
            &now,
            FleetTable::Operations,
            &format!("finalizing/{}", hex::encode(operation.bytes())),
            TIMEOUT
        )
        .await?
        .is_none());
    let mut rebound = identity.clone();
    rebound.expected_revision = now.revision();
    ensure!(matches!(
        candidate
            .begin_finalization(&root, &rebound, &host, &[], TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    ensure!(scratch.usage()?.generations == 0);
    write_read(&f, &cells, 1, 5, 5).await?;
    stopped(node).await?;
    stop(nodes).await?;
    drop(old_host);
    f.domain.shutdown().await;
    Ok(())
}
