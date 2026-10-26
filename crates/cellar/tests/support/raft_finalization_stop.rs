// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real accepted stop/cancellation fences, physical joins and signed closure.
use super::*;
use cellar::sdk::consensus::FleetStopAction;
use futures_util::StreamExt;
use object_store::{path::Path, ObjectStore};
use sha2::{Digest, Sha256};

#[path = "raft_storage_fault.rs"]
#[allow(dead_code)]
mod fault;

#[path = "raft_fleet_stop_rpc.rs"]
mod remote;

async fn finished(mut node: cellar::sdk::Node, f: &Fixture) -> Result<()> {
    let outcome = f
        .domain
        .run(cellar::asyncrt::timeout(TIMEOUT, node.finished()))
        .await???;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("scoped fleet stop did not stop the node");
    };
    let raft = report.raft.context("missing joined physical Raft report")?;
    ensure!(!raft.startup_incomplete && raft.result?.unfinished.is_empty());
    Ok(())
}

async fn guarded() -> Result<(Started, Arc<fault::Gate>)> {
    let s = Box::pin(Started::new()).await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let spec = s.f.peer_spec(7)?;
    let authorization = s.f.catalog.authorize_host(&cut, spec.node, TIMEOUT).await?;
    stopped(s.observer_node).await?;
    let gate = fault::Gate::new(None);
    let node = public_node::node_declared_with_filesystem(
        s._observer_root.path(),
        s.f.definition.application.clone(),
        &spec.label,
        vec![],
        Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
    )
    .await?;
    let (mut config, services) = s.f.config_for(&node, authorization, &spec).await?;
    config.infrastructure.log.mode = LogOpenMode::Resume;
    config.infrastructure.log.process = ProcessGeneration::new([137; 16])?;
    config.peers[0].process = config.infrastructure.log.process;
    let observer = node.start_raft_cells(config, services).await?;
    Ok((
        Started {
            observer_node: node,
            observer,
            f: s.f,
            nodes: s.nodes,
            cells: s.cells,
            _observer_root: s._observer_root,
            proposal: s.proposal,
            authorization: s.authorization,
            archive: s.archive,
            _scratch_root: s._scratch_root,
            scratch: s.scratch,
            verifier: s.verifier,
        },
        gate,
    ))
}

async fn finalizing(
    s: &Started,
) -> Result<(
    ProposalId,
    cellar::application::consensus::verification::FleetFinalizationAdmission,
)> {
    let (namespace, operation, candidate) = candidate(s).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let admission = candidate
        .begin_finalization(
            &root,
            &request(operation, root.revision()),
            &s.nodes[0].host,
            std::slice::from_ref(&namespace),
            TIMEOUT,
        )
        .await?;
    Ok((operation, admission))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_enrollment_incomplete_import() -> Result<()> {
    let (s, _gate) = Box::pin(guarded()).await?;
    let (operation, admission) = finalizing(&s)
        .await
        .context("incomplete import: initial Finalizing")?;
    let stop_id = ProposalId::new([238; 16])?;
    s.observer_node
        .request_fleet_stop(admission.stop_request(stop_id)?, TIMEOUT)
        .await?;
    finished(s.observer_node, &s.f)
        .await
        .context("incomplete import: physical join")?;
    let (namespace, _) = initial_birth(&s.f)
        .await
        .context("incomplete import: namespace")?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    s.f.catalog
        .verify_physical_closure(&root, operation, stop_id, TIMEOUT)
        .await
        .context("incomplete import: signed closure")?;
    let observer = &s.nodes[0].host;
    let peers = [
        s.nodes[0].spec.node,
        s.nodes[1].spec.node,
        s.nodes[2].spec.node,
    ];
    for node in &s.nodes {
        let peer = if node.spec.node == peers[0] {
            peers[1]
        } else {
            peers[0]
        };
        s.f.remote(&node.host, peer, root.revision()).await?;
    }
    let mut verification =
        s.f.catalog
            .prepare_evacuation_verification(
                &root,
                operation,
                std::slice::from_ref(&namespace),
                TIMEOUT,
            )
            .await
            .context("incomplete import: prepare verifier")?;
    let cut = verification
        .freeze_live(observer, peers, TIMEOUT)
        .await
        .context("incomplete import: renewed quorum")?;
    let reference =
        s.f.domain
            .run(s.archive.recovery_metadata())
            .await??
            .context("terminal archive metadata")?
            .reference();
    verification
        .verify_live(
            observer,
            peers,
            FleetEvacuationArchive {
                archive: &s.archive,
                reference: &reference,
                verifier: &s.verifier,
            },
            TIMEOUT,
        )
        .await
        .context("incomplete import: verify or declare")?;
    ensure!(cut.image().is_none());
    let mut config = namespace.config().clone();
    config.namespace = NamespaceId::new([87; 16])?;
    config.managed_fleet = None;
    let extra = Catalog::new(
        s.f.store.clone(),
        CatalogOptions {
            prefix: "host-enrollment/".into(),
            ..Default::default()
        },
    )?
    .open_namespace(config)
    .await
    .context("incomplete import: extra namespace")?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    s.f.catalog
        .declare_namespace(
            &root,
            &Fixture::request(222, root.revision()),
            &extra,
            "Counter",
            TIMEOUT,
        )
        .await
        .context("incomplete import: verify or declare")?;
    let current = s.f.catalog.read(TIMEOUT).await?;
    let removal = cellar::application::consensus::verification::FleetRemovalRequest {
        identity: FleetRequestIdentity {
            id: [240; 16],
            operation: operation.bytes(),
            principal: "refuse-incomplete-removal".into(),
            expected_revision: current.revision(),
        },
        stop: stop_id,
    };
    let catalogs = [namespace.clone(), extra];
    ensure!(matches!(
        s.f.catalog
            .complete_evacuation(
                &current,
                &removal,
                Some(&mut verification),
                &catalogs,
                TIMEOUT,
            )
            .await,
        Err(FleetCatalogError::Conflict | FleetCatalogError::InventoryIncomplete)
    ));
    ensure!(matches!(
        s.f.catalog
            .prepare_evacuation_verification(&current, operation, &catalogs, TIMEOUT,)
            .await,
        Err(FleetCatalogError::InventoryIncomplete)
    ));
    let after = s.f.catalog.read(TIMEOUT).await?;
    ensure!(after.revision() == current.revision());
    ensure!(matches!(
        s.f.catalog
            .authorize_host(&after, NodeId::new([7; 16])?, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    let bytes =
        s.f.catalog
            .get(
                &after,
                cellar::application::consensus::verification::FleetTable::Nodes,
                &hex::encode([7; 16]),
                TIMEOUT,
            )
            .await?
            .context("Finalizing node row")?;
    let node: serde_json::Value = serde_json::from_slice(&bytes[13..bytes.len() - 32])?;
    ensure!(node["state"] == "Finalizing");
    ensure!(matches!(
        s.f.catalog
            .begin_evacuation(
                &after,
                &FleetRequestIdentity {
                    id: [241; 16],
                    operation: [242; 16],
                    principal: "blocked-next-disruption".into(),
                    expected_revision: after.revision(),
                },
                peers[0],
                &catalogs,
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::DisruptionInProgress { .. })
    ));
    write_read(&s.f, &s.cells, 1, 4, 4).await?;
    stop(s.nodes).await?;
    s.f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_removal_failed_survivor_keeps_slot() -> Result<()> {
    use cellar::application::consensus::verification::FleetRemovalRequest;
    let (mut s, _gate) = Box::pin(guarded()).await?;
    let (operation, admission) = finalizing(&s).await?;
    let (namespace, _) = initial_birth(&s.f).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let peers = [
        s.nodes[0].spec.node,
        s.nodes[1].spec.node,
        s.nodes[2].spec.node,
    ];
    for node in &s.nodes {
        let peer = if node.spec.node == peers[0] {
            peers[1]
        } else {
            peers[0]
        };
        s.f.remote(&node.host, peer, root.revision()).await?;
    }
    let mut proof =
        s.f.catalog
            .prepare_evacuation_verification(
                &root,
                operation,
                std::slice::from_ref(&namespace),
                TIMEOUT,
            )
            .await?;
    let observer = s.nodes[0].host.clone();
    proof.freeze_live(&observer, peers, TIMEOUT).await?;
    let reference =
        s.f.domain
            .run(s.archive.recovery_metadata())
            .await??
            .context("archive head")?
            .reference();
    proof
        .verify_live(
            &observer,
            peers,
            FleetEvacuationArchive {
                archive: &s.archive,
                reference: &reference,
                verifier: &s.verifier,
            },
            TIMEOUT,
        )
        .await?;
    let stop_id = ProposalId::new([238; 16])?;
    s.observer_node
        .request_fleet_stop(admission.stop_request(stop_id)?, TIMEOUT)
        .await?;
    finished(s.observer_node, &s.f).await?;
    // All archive and quorum verification succeeded before this later failure.
    // The old positive observations cannot authorize terminal completion.
    let failed = s.nodes.pop().context("survivor to stop")?;
    stopped(failed.node).await?;
    let request = FleetRemovalRequest {
        identity: FleetRequestIdentity {
            id: [240; 16],
            operation: operation.bytes(),
            principal: "failed-survivor-removal".into(),
            expected_revision: root.revision(),
        },
        stop: stop_id,
    };
    ensure!(matches!(
        s.f.catalog
            .complete_evacuation(
                &root,
                &request,
                Some(&mut proof),
                std::slice::from_ref(&namespace),
                TIMEOUT,
            )
            .await,
        Err(FleetCatalogError::Observation(_) | FleetCatalogError::Unconfirmed)
    ));
    let after = s.f.catalog.read(TIMEOUT).await?;
    ensure!(after.revision() == root.revision());
    s.f.catalog
        .verify_physical_closure(&after, operation, stop_id, TIMEOUT)
        .await?;
    ensure!(matches!(
        s.f.catalog
            .begin_evacuation(
                &after,
                &FleetRequestIdentity {
                    id: [241; 16],
                    operation: [242; 16],
                    principal: "blocked-next-disruption".into(),
                    expected_revision: after.revision(),
                },
                peers[0],
                std::slice::from_ref(&namespace),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::DisruptionInProgress { .. })
    ));
    let serving = s.cells.iter().take(2).cloned().collect::<Vec<_>>();
    write_read(&s.f, &serving, 1, 2, 2).await?;
    stop(s.nodes).await?;
    s.f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_removal_joined_closure_and_lost_reply() -> Result<()> {
    use cellar::application::consensus::verification::{
        FleetChange, FleetMutation, FleetRemovalRequest, FleetTable,
    };
    let (s, _gate) = Box::pin(guarded()).await?;
    let (operation, admission) = finalizing(&s).await?;
    let (namespace, _) = initial_birth(&s.f).await?;
    let root = s.f.catalog.read(TIMEOUT).await?;
    let peers = [
        s.nodes[0].spec.node,
        s.nodes[1].spec.node,
        s.nodes[2].spec.node,
    ];
    for node in &s.nodes {
        let peer = if node.spec.node == peers[0] {
            peers[1]
        } else {
            peers[0]
        };
        s.f.remote(&node.host, peer, root.revision()).await?;
    }
    let mut proof =
        s.f.catalog
            .prepare_evacuation_verification(
                &root,
                operation,
                std::slice::from_ref(&namespace),
                TIMEOUT,
            )
            .await?;
    let observer = &s.nodes[0].host;
    proof.freeze_live(observer, peers, TIMEOUT).await?;
    let reference =
        s.f.domain
            .run(s.archive.recovery_metadata())
            .await??
            .context("archive head")?
            .reference();
    proof
        .verify_live(
            observer,
            peers,
            FleetEvacuationArchive {
                archive: &s.archive,
                reference: &reference,
                verifier: &s.verifier,
            },
            TIMEOUT,
        )
        .await?;
    let stop_id = ProposalId::new([238; 16])?;
    let request = FleetRemovalRequest {
        identity: FleetRequestIdentity {
            id: [240; 16],
            operation: operation.bytes(),
            principal: "joined-removal".into(),
            expected_revision: root.revision(),
        },
        stop: stop_id,
    };
    ensure!(matches!(
        s.f.catalog
            .complete_evacuation(
                &root,
                &request,
                Some(&mut proof),
                std::slice::from_ref(&namespace),
                TIMEOUT,
            )
            .await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    ensure!(matches!(
        s.f.catalog
            .commit(
                &root,
                &FleetMutation {
                    id: [244; 16],
                    operation: [245; 16],
                    principal: "raw-release-refused".into(),
                    expected_revision: root.revision(),
                    changes: vec![FleetChange {
                        table: FleetTable::Policy,
                        key: "active-disruption".into(),
                        value: None
                    }],
                },
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Invalid(_))
    ));
    s.observer_node
        .request_fleet_stop(admission.stop_request(stop_id)?, TIMEOUT)
        .await?;
    finished(s.observer_node, &s.f).await?;
    s.f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let result =
        s.f.catalog
            .complete_evacuation(
                &root,
                &request,
                Some(&mut proof),
                std::slice::from_ref(&namespace),
                TIMEOUT,
            )
            .await;
    ensure!(matches!(
        result,
        Ok(_) | Err(FleetCatalogError::Storage(_) | FleetCatalogError::Unconfirmed)
    ));
    ensure!(!s.f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    let controller = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let current = controller.read(TIMEOUT).await?;
    ensure!(current.revision() == root.revision() + 1);
    let completed = controller
        .complete_evacuation(&current, &request, None, &[], TIMEOUT)
        .await?;
    ensure!(completed.applied_revision() == current.revision() && completed.node() == [7; 16]);
    let mut changed = request.clone();
    changed.stop = ProposalId::new([239; 16])?;
    ensure!(matches!(
        controller
            .complete_evacuation(&current, &changed, None, &[], TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    ensure!(controller.read(TIMEOUT).await?.revision() == current.revision());
    stop(s.nodes).await?;
    s.f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_stop_waiter_loss_still_joins_and_signs() -> Result<()> {
    let (s, gate) = Box::pin(guarded()).await?;
    let (operation, admission) = finalizing(&s).await?;
    let id = ProposalId::new([238; 16])?;
    let request = admission.stop_request(id)?;
    let mut wrong = serde_json::to_value(&request)?;
    wrong["selector"]["boot"]["process"] = serde_json::json!(vec![138; 16]);
    ensure!(matches!(
        s.observer_node
            .request_fleet_stop(serde_json::from_value(wrong)?, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    s.f.store.fail_closure_after.store(true, Ordering::Release);
    let release = gate.arm(fault::HOLD_SYNC);
    let mut pending = Box::pin(s.observer_node.request_fleet_stop(request, TIMEOUT));
    ensure!(futures_util::poll!(pending.as_mut()).is_pending());
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, gate.entered.notified()))
        .await??;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    ensure!(matches!(
        s.f.catalog
            .verify_physical_closure(&cut, operation, id, TIMEOUT)
            .await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    drop(pending);
    drop(release);
    // Observe and join autonomous shutdown without issuing another stop signal.
    finished(s.observer_node, &s.f).await?;
    ensure!(!s.f.store.fail_closure_after.load(Ordering::Acquire));
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let proof =
        s.f.catalog
            .verify_physical_closure(&cut, operation, id, TIMEOUT)
            .await?;
    ensure!(
        proof.node() == [7; 16] && proof.process() == [137; 16] && proof.request() == id.bytes()
    );
    ensure!(proof.operation() == operation.bytes() && proof.digest() != [0; 32]);
    // A checksum-correct bucket edit still cannot fabricate the host signature.
    let mut objects = s.f.store.inner.list(None);
    let mut closure = None;
    while let Some(object) = objects.next().await {
        let object = object?;
        if object.location.as_ref().contains("/closures/") {
            ensure!(closure.is_none());
            closure = Some(object.location);
        }
    }
    let key: Path = closure.context("missing actual closure object")?;
    let original = s.f.store.get(&key).await?.bytes().await?;
    let payload = std::str::from_utf8(&original[13..original.len() - 32])?;
    ensure!(payload.contains("\"groups\":0"));
    let payload = payload.replacen("\"groups\":0", "\"groups\":1", 1);
    let mut corrupt = original[..9].to_vec();
    corrupt.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    corrupt.extend_from_slice(payload.as_bytes());
    corrupt.extend_from_slice(&Sha256::digest(&corrupt));
    s.f.store.inner.put(&key, corrupt.into()).await?;
    ensure!(matches!(
        s.f.catalog
            .verify_physical_closure(&cut, operation, id, TIMEOUT)
            .await,
        Err(FleetCatalogError::Corrupt("closure signature"))
    ));
    s.f.store.inner.put(&key, original.into()).await?;
    ensure!(
        s.f.catalog
            .verify_physical_closure(&cut, operation, id, TIMEOUT)
            .await?
            .digest()
            == proof.digest()
    );
    write_read(&s.f, &s.cells, 1, 9, 9).await?;
    ensure!(s.scratch.usage()?.generations == 0);
    stop(s.nodes).await?;
    s.f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_stop_cancellation_fence_defeats_late_stop() -> Result<()> {
    let (s, _gate) = Box::pin(guarded()).await?;
    let (operation, admission) = finalizing(&s).await?;
    let cancellation = admission.stop_fence_request(ProposalId::new([239; 16])?)?;
    let accepted = s
        .observer_node
        .request_fleet_stop(cancellation.clone(), TIMEOUT)
        .await?;
    ensure!(accepted.action() == FleetStopAction::Fence);
    ensure!(
        s.observer_node
            .request_fleet_stop(cancellation, TIMEOUT)
            .await?
            .request()
            == [239; 16]
    );
    let id = ProposalId::new([238; 16])?;
    ensure!(matches!(
        s.observer_node
            .request_fleet_stop(admission.stop_request(id)?, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(!s.observer_node.is_closed());
    s.observer.observe_node(TIMEOUT).await?;
    stopped(s.observer_node).await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    ensure!(matches!(
        s.f.catalog
            .verify_physical_closure(&cut, operation, id, TIMEOUT)
            .await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    // The local fence did not release the rooted operation or disruption slot.
    ensure!(
        s.f.catalog
            .evacuation_finalization(&cut, operation, TIMEOUT)
            .await?
            .enrollment_epoch()
            == admission.enrollment_epoch()
    );
    stop(s.nodes).await?;
    s.f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_stop_closure_waits_for_physical_owner_join() -> Result<()> {
    let (s, gate) = Box::pin(guarded()).await?;
    let (operation, admission) = finalizing(&s).await?;
    let id = ProposalId::new([238; 16])?;
    let release = gate.arm(fault::HOLD_LOCK_DROP);
    let accepted = s
        .observer_node
        .request_fleet_stop(admission.stop_request(id)?, TIMEOUT)
        .await?;
    ensure!(accepted.action() == FleetStopAction::Stop);
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, gate.entered.notified()))
        .await??;
    ensure!(!s.observer_node.is_closed());
    // Exact accepted retries remain bounded while the driver cannot yet join
    // the physical engine. They preserve acceptance without issuing more stops.
    for _ in 0..32 {
        ensure!(
            s.observer_node
                .request_fleet_stop(admission.stop_request(id)?, TIMEOUT)
                .await?
                .request()
                == id.bytes()
        );
    }
    let cut = s.f.catalog.read(TIMEOUT).await?;
    ensure!(matches!(
        s.f.catalog
            .verify_physical_closure(&cut, operation, id, TIMEOUT)
            .await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    let late_fence = admission.stop_fence_request(ProposalId::new([239; 16])?)?;
    ensure!(matches!(
        s.observer_node
            .request_fleet_stop(late_fence, TIMEOUT)
            .await,
        Err(FleetCatalogError::Closed
            | FleetCatalogError::Conflict
            | FleetCatalogError::Unconfirmed)
    ));
    write_read(&s.f, &s.cells, 1, 3, 3).await?;
    drop(release);
    finished(s.observer_node, &s.f).await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        s.f.catalog
            .verify_physical_closure(&cut, operation, id, TIMEOUT)
            .await?
            .request()
            == id.bytes()
    );
    ensure!(s.scratch.usage()?.generations == 0);
    stop(s.nodes).await?;
    s.f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_stop_changed_inventory_refuses_admission() -> Result<()> {
    let (s, _gate) = Box::pin(guarded()).await?;
    let (operation, admission) = finalizing(&s).await?;
    let mut config = s.f.declaration.namespace.clone();
    config.namespace = NamespaceId::new([87; 16])?;
    config.managed_fleet = None;
    let extra = Catalog::new(
        s.f.store.clone(),
        CatalogOptions {
            prefix: "host-enrollment/".into(),
            ..Default::default()
        },
    )?
    .open_namespace(config)
    .await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    s.f.catalog
        .declare_namespace(
            &cut,
            &Fixture::request(222, cut.revision()),
            &extra,
            "Counter",
            TIMEOUT,
        )
        .await?;
    let id = ProposalId::new([238; 16])?;
    ensure!(matches!(
        s.observer_node
            .request_fleet_stop(admission.stop_request(id)?, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(!s.observer_node.is_closed());
    // Cancellation can narrow authority even when positive inventory inputs
    // changed. It cannot complete the rooted operation or produce closure.
    let fenced = s
        .observer_node
        .request_fleet_stop(
            admission.stop_fence_request(ProposalId::new([239; 16])?)?,
            TIMEOUT,
        )
        .await?;
    ensure!(fenced.action() == FleetStopAction::Fence);
    stopped(s.observer_node).await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    ensure!(matches!(
        s.f.catalog
            .verify_physical_closure(&cut, operation, id, TIMEOUT)
            .await,
        Err(FleetCatalogError::Unconfirmed)
    ));
    stop(s.nodes).await?;
    s.f.domain.shutdown().await;
    Ok(())
}
