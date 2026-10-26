// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Trusted fixture isolation is exercised after an actual physical join. This
//! does not qualify a production deployment's external fencing mechanism.
use super::*;
use sha2::{Digest, Sha256};

async fn isolate(
    s: &Setup,
    node: NodeId,
    boot: FleetBootBinding,
) -> Result<FleetNodeIsolationRequest> {
    let cut = s.f.catalog.read(TIMEOUT).await?;
    s.f.catalog
        .begin_evacuation(
            &cut,
            &Fixture::request(220, cut.revision()),
            node,
            std::slice::from_ref(&s.namespace),
            TIMEOUT,
        )
        .await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let request = FleetNodeIsolationRequest {
        identity: Fixture::request(221, cut.revision()),
        attestation: FleetIsolationAttestation {
            boot,
            evidence_digest: [92; 32],
            evidence_reference: "fixture/physically-joined-and-restart-disabled".into(),
        },
    };
    let mut wrong = request.clone();
    wrong.attestation.boot.process = [93; 16];
    ensure!(s
        .f
        .catalog
        .attest_node_isolation(&cut, &wrong, TIMEOUT)
        .await
        .is_err());
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    s.f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(s
        .f
        .catalog
        .attest_node_isolation(&cut, &request, TIMEOUT)
        .await
        .is_err());
    let reopened = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let root = reopened.read(TIMEOUT).await?;
    ensure!(root.revision() == cut.revision() + 1);
    let receipt = reopened
        .attest_node_isolation(&root, &request, TIMEOUT)
        .await?;
    ensure!(
        receipt.boot() == &request.attestation.boot
            && receipt.applied_revision() == root.revision()
    );
    let mut changed = request.clone();
    changed.attestation.evidence_digest = [94; 32];
    ensure!(matches!(
        reopened
            .attest_node_isolation(&root, &changed, TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    ensure!(reopened.authorize_host(&root, node, TIMEOUT).await.is_err());
    for (id, table, key) in [
        (222, FleetTable::Nodes, hex::encode(node.bytes())),
        (
            223,
            FleetTable::Operations,
            format!("node-isolation/{}", hex::encode(node.bytes())),
        ),
    ] {
        ensure!(matches!(
            reopened
                .commit(
                    &root,
                    &FleetMutation {
                        id: [id; 16],
                        operation: request.identity.operation,
                        principal: "forged-isolation-release".into(),
                        expected_revision: root.revision(),
                        changes: vec![FleetChange {
                            table,
                            key,
                            value: None
                        }],
                    },
                    TIMEOUT
                )
                .await,
            Err(FleetCatalogError::Invalid(_))
        ));
    }
    let key = hex::encode(node.bytes());
    let original = reopened
        .get(&root, FleetTable::Nodes, &key, TIMEOUT)
        .await?
        .context("isolated node")?;
    for (id, field, value) in [
        (224, "isolation", serde_json::Value::Null),
        (225, "boot", serde_json::Value::Null),
    ] {
        let mut record: serde_json::Value =
            serde_json::from_slice(&original[13..original.len() - 32])?;
        record[field] = value;
        let payload = serde_json::to_vec(&record)?;
        let mut encoded = original[..9].to_vec();
        encoded.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        encoded.extend_from_slice(&payload);
        encoded.extend_from_slice(&Sha256::digest(&encoded));
        ensure!(reopened
            .commit(
                &root,
                &FleetMutation {
                    id: [id; 16],
                    operation: request.identity.operation,
                    principal: "forged-quarantine-clear".into(),
                    expected_revision: root.revision(),
                    changes: vec![FleetChange {
                        table: FleetTable::Nodes,
                        key: key.clone(),
                        value: Some(encoded)
                    }]
                },
                TIMEOUT
            )
            .await
            .is_err());
    }
    ensure!(reopened.read(TIMEOUT).await?.revision() == root.revision());
    Ok(request)
}

async fn refresh(s: &Setup) -> Result<()> {
    let mut hosts: Vec<_> = s.nodes.iter().map(|n| &n.host).collect();
    hosts.push(&s.target);
    discover(&s.f, &hosts).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_movement_isolated_obsolete_repairs_three_histories() -> Result<()> {
    // Three actual moves exceed the fleet's two-active-move book; each must
    // settle while the obsolete node stays offline and its engine is retained.
    let mut s = Box::pin(Setup::with_histories(true, 3, 3)).await?;
    let mut groups = Vec::new();
    for proposal in &s.proposals {
        let cut = s.f.catalog.read(TIMEOUT).await?;
        let authorization =
            s.f.catalog
                .authorize_group(&cut, &proposal.scope, TIMEOUT)
                .await?;
        let cells = registered(&s.nodes, &authorization).await?;
        activate(&cells, &authorization).await?;
        write_read(&s.f, &cells, 1, 7, 7).await?;
        groups.push((cells, authorization));
    }
    let obsolete_index = (primary(&s.f, &groups[0].0).await? + 1) % 3;
    let stopped = s.nodes.remove(obsolete_index);
    let boot = stopped
        .host
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .unwrap();
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let saved =
        s.f.catalog
            .authorize_host(&cut, stopped.spec.node, TIMEOUT)
            .await?;
    // Without an admitted exclusion, external evidence has no authority.
    let premature = FleetNodeIsolationRequest {
        identity: Fixture::request(219, cut.revision()),
        attestation: FleetIsolationAttestation {
            boot: boot.clone(),
            evidence_digest: [92; 32],
            evidence_reference: "fixture/not-yet-excluded".into(),
        },
    };
    ensure!(s
        .f
        .catalog
        .attest_node_isolation(&cut, &premature, TIMEOUT)
        .await
        .is_err());
    joined(stopped.node).await?;
    let evidence = isolate(&s, stopped.spec.node, boot).await?;
    // Even a saved startup capability cannot restart the retained engine.
    let retry_node = public_node::node_named(
        stopped.root.path(),
        s.f.definition.application.clone(),
        &stopped.spec.label,
    )
    .await?;
    let (config, services) = s.f.config_for(&retry_node, saved, &stopped.spec).await?;
    ensure!(retry_node.start_raft_cells(config, services).await.is_err());
    retry_node.shutdown(ShutdownMode::Preserve).await?;
    refresh(&s).await?;
    for (index, (mut cells, authorization)) in groups.into_iter().enumerate() {
        cells.remove(obsolete_index);
        let p = primary(&s.f, &cells).await?;
        let scope = &s.proposals[index].scope;
        let cut = s.f.catalog.read(TIMEOUT).await?;
        let request = FleetMovementRequest {
            resources: s
                .request(index, 230 + index as u8 * 2, cut.revision())
                .await?,
            source: s.nodes[p].spec.node,
            plan: RepairPlan {
                id: ProposalId::new([240 + index as u8; 16])?,
                policy_revision: 1,
                expected_revision: cells[p].status().await?.membership_revision,
                obsolete: MemberId::new(obsolete_index as u64 + 1)?,
                replacement: JoiningMember {
                    member: MemberId::new(4)?,
                    node: NodeId::new([7; 16])?,
                    disk: s.f.peer_spec(7)?.disk,
                },
            },
        };
        let accepted =
            s.f.catalog
                .reserve_movement(
                    &cut,
                    &request,
                    &s.target,
                    std::slice::from_ref(&s.namespace),
                    TIMEOUT,
                )
                .await?;
        let permit = accepted.authorization();
        let target = s
            .target
            .register_managed_history(authorization, TIMEOUT)
            .await
            .context("register isolated replacement history")?;
        refresh(&s).await?;
        cells[p]
            .begin_managed_repair(permit.clone(), request.plan.clone(), TIMEOUT)
            .await
            .context("begin isolated replacement repair")?;
        let source_member = s.proposals[index]
            .voters
            .iter()
            .find(|v| v.node == s.nodes[p].spec.node)
            .unwrap()
            .member;
        target
            .activate_managed_learner(
                permit.clone(),
                LearnerActivation {
                    member: MemberId::new(4)?,
                    source: source_member,
                    source_node: s.nodes[p].spec.node,
                    from_s3: index % 2 == 0,
                    timeout: TIMEOUT,
                },
            )
            .await
            .with_context(|| format!("activate isolated replacement learner {index}"))?;
        ensure!(
            step(&s.f, &cells[p], &permit, RepairStep::PromoteAndRemove)
                .await?
                .phase
                == RepairPhase::Joint
        );
        ensure!(
            step(&s.f, &cells[p], &permit, RepairStep::FinishJoint)
                .await?
                .phase
                == RepairPhase::Complete
        );
        cells.push(target);
        write_read(&s.f, &cells, 2, 5, 12).await?;
        s.f.domain
            .run(cellar::asyncrt::timeout(TIMEOUT, async {
                loop {
                    let cut = s.f.catalog.read(TIMEOUT).await?;
                    let mut complete = true;
                    for node in &s.nodes {
                        let observed = node
                            .host
                            .inspect_peer_physical_inventory(
                                node.spec.node,
                                cut.revision(),
                                TIMEOUT,
                            )
                            .await?;
                        complete &= observed.view.seed_workers == 0
                            && observed
                                .inventory
                                .as_ref()
                                .unwrap()
                                .history(scope)?
                                .is_some_and(|h| h.completed_sequence() == 1);
                    }
                    if complete {
                        return Ok::<_, anyhow::Error>(());
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
            }))
            .await???;
        let (cut, completion) = completion(&s, &request, 231 + index as u8 * 2).await?;
        let done =
            s.f.catalog
                .complete_movement(&cut, &completion, &s.target, TIMEOUT)
                .await?;
        ensure!(done.isolated_sources() == [stopped.spec.node.bytes()]);
        let root = s.f.catalog.read(TIMEOUT).await?;
        let replay =
            s.f.catalog
                .complete_movement(&root, &completion, &s.target, TIMEOUT)
                .await?;
        ensure!(
            replay.applied_revision() == done.applied_revision()
                && replay.isolated_sources() == done.isolated_sources()
        );
        let book =
            s.f.catalog
                .get(&root, FleetTable::Policy, "resource-reservations", TIMEOUT)
                .await?;
        // Assert the floor from physical inventory, independently of completion
        // success. Completed work never turns retained engines into free slots.
        let floor =
            s.f.catalog
                .get(
                    &root,
                    FleetTable::Operations,
                    &format!("resource-floor/{}", hex::encode([7; 16])),
                    TIMEOUT,
                )
                .await?
                .context("retained target floor")?;
        let floor: serde_json::Value = serde_json::from_slice(&floor[13..floor.len() - 32])?;
        ensure!(floor["physical"]["engine_slots"] == index as u64 + 1);
        let book = book.context("retained resource book")?;
        let book: serde_json::Value = serde_json::from_slice(&book[13..book.len() - 32])?;
        ensure!(book["rows"].as_array().is_some_and(Vec::is_empty));
        refresh(&s).await?;
    }
    let root = s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        s.f.catalog
            .attest_node_isolation(&root, &evidence, TIMEOUT)
            .await?
            .boot()
            == &evidence.attestation.boot
    );
    ensure!(stopped.root.path().exists());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_movement_isolated_surviving_voter_retains_debt() -> Result<()> {
    let mut s = Box::pin(Setup::new(true)).await?;
    let r = replace(&s, false).await?;
    let stopped = s.nodes.remove(r.source);
    let boot = stopped
        .host
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .unwrap();
    joined(stopped.node).await?;
    isolate(&s, stopped.spec.node, boot).await?;
    refresh(&s).await?;
    let (cut, request) = completion(&s, &r.request, 230).await?;
    ensure!(s
        .f
        .catalog
        .complete_movement(&cut, &request, &s.target, TIMEOUT)
        .await
        .is_err());
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_movement_isolation_after_admission_preserves_completion_retry() -> Result<()> {
    let mut s = Box::pin(Setup::new(true)).await?;
    let r = replace(&s, true).await?;
    let stopped = s.nodes.remove((r.source + 1) % 3);
    let boot = stopped
        .host
        .observe_node(TIMEOUT)
        .await?
        .fleet_boot
        .unwrap();
    joined(stopped.node).await?;
    isolate(&s, stopped.spec.node, boot).await?;
    refresh(&s).await?;
    let (cut, request) = completion(&s, &r.request, 230).await?;
    s.f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(s
        .f
        .catalog
        .complete_movement(&cut, &request, &s.target, TIMEOUT)
        .await
        .is_err());
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision() + 1);
    joined(s.target_node).await?;
    let catalog = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let root = catalog.read(TIMEOUT).await?;
    let done = catalog
        .complete_movement(&root, &request, &s.nodes[0].host, TIMEOUT)
        .await?;
    ensure!(
        done.applied_revision() == cut.revision() + 1
            && done.isolated_sources() == [stopped.spec.node.bytes()]
    );
    for node in s.nodes {
        joined(node.node).await?;
    }
    s.f.domain.shutdown().await;
    Ok(())
}
