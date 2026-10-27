// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Composed resource-backed repair, physical replay and isolated terminal CAS.
//! Deployment isolation is trusted fixture evidence after actual physical join.
use super::*;
use crate::hosts::capacity_tests::{attestation, joined};
use cellar::application::consensus::verification::{
    FleetEvacuationCandidate, FleetIsolatedRemovalRequest, FleetRemovalRequest,
};
use sha2::{Digest, Sha256};

const OPERATION: [u8; 16] = [201; 16];
const ISOLATION: [u8; 16] = [202; 16];

fn request(id: u8, revision: u64) -> FleetRequestIdentity {
    FleetRequestIdentity {
        id: [id; 16],
        operation: OPERATION,
        principal: "isolated-removal-fixture".into(),
        expected_revision: revision,
    }
}

struct Repaired {
    s: Started,
    namespace: NamespaceCatalog,
    active: Vec<RaftCellHandle>,
    peers: [NodeId; 3],
    old_root: tempfile::TempDir,
    old_spec: FleetNodeSpec,
    boot: FleetBootBinding,
}

impl Repaired {
    async fn new() -> Result<Self> {
        let mut s = Box::pin(Started::new()).await?;
        let (namespace, _) = initial_birth(&s.f).await?;
        write_read(&s.f, &s.cells, 1, 7, 7).await?;
        let p = primary(&s.f, &s.cells).await?;
        let obsolete = (p + 1) % 3;
        let evidence = attestation(&s.f, &s.nodes[p].host, &s.observer).await?;
        let cut = s.f.catalog.read(TIMEOUT).await?;
        s.f.catalog
            .admit_node_capacity(
                &cut,
                &FleetCapacityRequest {
                    identity: Fixture::request(199, cut.revision()),
                    attestation: evidence,
                    action: FleetCapacityAction::Activate,
                },
                &s.nodes[p].host,
                std::slice::from_ref(&namespace),
                TIMEOUT,
            )
            .await?;
        let old = s.nodes.remove(obsolete);
        let boot = old
            .host
            .observe_node(TIMEOUT)
            .await?
            .fleet_boot
            .context("old boot")?;
        joined(old.node).await?;
        let cut = s.f.catalog.read(TIMEOUT).await?;
        s.f.catalog
            .begin_evacuation(
                &cut,
                &request(200, cut.revision()),
                old.spec.node,
                std::slice::from_ref(&namespace),
                TIMEOUT,
            )
            .await?;
        let cut = s.f.catalog.read(TIMEOUT).await?;
        s.f.catalog
            .attest_node_isolation(
                &cut,
                &FleetNodeIsolationRequest {
                    identity: request(202, cut.revision()),
                    attestation: FleetIsolationAttestation {
                        boot: boot.clone(),
                        evidence_digest: [92; 32],
                        evidence_reference: "fixture/joined-process-and-prevented-restart".into(),
                    },
                },
                TIMEOUT,
            )
            .await?;
        let mut active = s
            .cells
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != obsolete)
            .map(|(_, c)| c.clone())
            .collect::<Vec<_>>();
        let p = primary(&s.f, &active).await?;
        discover(&s.f, &[&s.nodes[0].host, &s.nodes[1].host, &s.observer]).await?;
        let cut = s.f.catalog.read(TIMEOUT).await?;
        let local = s.observer.observe_node(TIMEOUT).await?;
        let occupancy = FleetResourceAttestation {
            boot: local.fleet_boot.context("target boot")?,
            profile_revision: 1,
            used: FleetResourceBounds {
                engine_slots: 0,
                durable_bytes: 1024 * 1024,
                memory_bytes: 64 * 1024 * 1024,
            },
            evidence_digest: [71; 32],
            evidence_reference: "fixture/retained-occupancy".into(),
        };
        let movement = FleetMovementRequest {
            resources: FleetResourceRequest {
                identity: request(210, cut.revision()),
                group: s.proposal.scope.clone(),
                attestation: occupancy.clone(),
                peak: FleetResourceBounds {
                    engine_slots: 1,
                    durable_bytes: local.node.profile.bytes_per_group,
                    memory_bytes: 8 * 1024 * 1024,
                },
            },
            source: s.nodes[p].spec.node,
            plan: RepairPlan {
                id: ProposalId::new([212; 16])?,
                policy_revision: 1,
                expected_revision: active[p].status().await?.membership_revision,
                obsolete: MemberId::new(obsolete as u64 + 1)?,
                replacement: JoiningMember {
                    member: MemberId::new(4)?,
                    node: NodeId::new([7; 16])?,
                    disk: s.f.peer_spec(7)?.disk,
                },
            },
        };
        let admitted =
            s.f.catalog
                .reserve_movement(
                    &cut,
                    &movement,
                    &s.observer,
                    std::slice::from_ref(&namespace),
                    TIMEOUT,
                )
                .await?;
        let permit = admitted.authorization();
        let target = s
            .observer
            .register_managed_history(s.authorization.clone(), TIMEOUT)
            .await?;
        discover(&s.f, &[&s.nodes[0].host, &s.nodes[1].host, &s.observer]).await?;
        active[p]
            .begin_managed_repair(permit.clone(), movement.plan.clone(), TIMEOUT)
            .await?;
        let source_member = s
            .proposal
            .voters
            .iter()
            .find(|v| v.node == s.nodes[p].spec.node)
            .context("source member")?
            .member;
        target
            .activate_managed_learner(
                permit.clone(),
                LearnerActivation {
                    member: MemberId::new(4)?,
                    source: source_member,
                    source_node: s.nodes[p].spec.node,
                    from_s3: true,
                    timeout: TIMEOUT,
                },
            )
            .await?;
        ensure!(
            super::super::super::retirement::step(
                &s.f,
                &active[p],
                &permit,
                RepairStep::PromoteAndRemove
            )
            .await?
            .phase
                == RepairPhase::Joint
        );
        ensure!(
            super::super::super::retirement::step(
                &s.f,
                &active[p],
                &permit,
                RepairStep::FinishJoint
            )
            .await?
            .phase
                == RepairPhase::Complete
        );
        active.push(target);
        write_read(&s.f, &active, 2, 5, 12).await?;
        s.f.domain
            .run(cellar::asyncrt::timeout(TIMEOUT, async {
                loop {
                    let cut = s.f.catalog.read(TIMEOUT).await?;
                    let mut done = true;
                    for node in &s.nodes {
                        let sample = node
                            .host
                            .inspect_peer_physical_inventory(
                                node.spec.node,
                                cut.revision(),
                                TIMEOUT,
                            )
                            .await?;
                        done &= sample.view.seed_workers == 0
                            && sample
                                .inventory
                                .as_ref()
                                .context("physical inventory")?
                                .history(&s.proposal.scope)?
                                .is_some_and(|h| h.completed_sequence() == 1);
                    }
                    if done {
                        return Ok::<_, anyhow::Error>(());
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
            }))
            .await???;
        let cut = s.f.catalog.read(TIMEOUT).await?;
        let complete =
            s.f.catalog
                .complete_movement(
                    &cut,
                    &FleetMovementCompletionRequest {
                        identity: request(211, cut.revision()),
                        reservation: movement.resources.identity.id,
                        attestation: occupancy,
                    },
                    &s.observer,
                    TIMEOUT,
                )
                .await?;
        ensure!(complete.isolated_sources() == [old.spec.node.bytes()]);
        let peers = [
            s.nodes[0].spec.node,
            s.nodes[1].spec.node,
            NodeId::new([7; 16])?,
        ];
        Ok(Self {
            s,
            namespace,
            active,
            peers,
            old_root: old.root,
            old_spec: old.spec,
            boot,
        })
    }

    async fn refresh(&self) -> Result<()> {
        let mut hosts = self.s.nodes.iter().map(|n| &n.host).collect::<Vec<_>>();
        hosts.push(&self.s.observer);
        discover(&self.s.f, &hosts).await
    }

    async fn verification(&self, catalog: &FleetCatalog) -> Result<FleetEvacuationVerification> {
        self.refresh().await?;
        let cut = catalog.read(TIMEOUT).await?;
        let mut proof = catalog
            .prepare_evacuation_verification(
                &cut,
                ProposalId::new(OPERATION)?,
                std::slice::from_ref(&self.namespace),
                TIMEOUT,
            )
            .await?;
        let fixed = proof
            .freeze_live(&self.s.observer, self.peers, TIMEOUT)
            .await?;
        let reference = self.s.reference(&fixed).await?;
        proof
            .verify_live(
                &self.s.observer,
                self.peers,
                archive(&self.s, &reference),
                TIMEOUT,
            )
            .await?;
        Ok(proof)
    }

    async fn finalizing(&self) -> Result<FleetEvacuationCandidate> {
        let mut proof = self.verification(&self.s.f.catalog).await?;
        let candidate = proof.publish_candidate(TIMEOUT).await?;
        let cut = self.s.f.catalog.read(TIMEOUT).await?;
        let intent = request(230, cut.revision());
        ensure!(candidate
            .begin_isolated_finalization(
                &cut,
                &intent,
                ProposalId::new([203; 16])?,
                std::slice::from_ref(&self.namespace),
                TIMEOUT
            )
            .await
            .is_err());
        ensure!(candidate
            .begin_finalization(
                &cut,
                &intent,
                &self.s.observer,
                std::slice::from_ref(&self.namespace),
                TIMEOUT
            )
            .await
            .is_err());
        self.s
            .f
            .store
            .fail_fleet_confirmation
            .store(true, Ordering::Release);
        ensure!(candidate
            .begin_isolated_finalization(
                &cut,
                &intent,
                ProposalId::new(ISOLATION)?,
                std::slice::from_ref(&self.namespace),
                TIMEOUT
            )
            .await
            .is_err());
        let root = self.s.f.catalog.read(TIMEOUT).await?;
        ensure!(root.revision() == cut.revision() + 1);
        let fence = candidate
            .begin_isolated_finalization(&root, &intent, ProposalId::new(ISOLATION)?, &[], TIMEOUT)
            .await?;
        ensure!(fence.isolation_request() == Some(ISOLATION));
        ensure!(fence.stop_request(ProposalId::new([238; 16])?).is_err());
        ensure!(fence
            .stop_fence_request(ProposalId::new([238; 16])?)
            .is_err());
        ensure!(self
            .s
            .f
            .catalog
            .verify_physical_closure(
                &root,
                ProposalId::new(OPERATION)?,
                ProposalId::new([238; 16])?,
                TIMEOUT
            )
            .await
            .is_err());
        Ok(candidate)
    }

    async fn close(self) -> Result<()> {
        ensure!(self.s.scratch.usage()?.generations == 0);
        joined(self.s.observer_node).await?;
        for node in self.s.nodes {
            joined(node.node).await?;
        }
        self.s.f.domain.shutdown().await;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_isolated_removal_repairs_replays_and_releases_slot() -> Result<()> {
    let r = Box::pin(Repaired::new()).await?;
    r.finalizing().await?;
    write_read(&r.s.f, &r.active, 3, 6, 18).await?;
    let controller = Fixture::open_catalog(&r.s.f.domain, r.s.f.store.clone())?;
    let mut proof = r.verification(&controller).await?;
    let cut = controller.read(TIMEOUT).await?;
    // Cancellation must never return a permanently quarantined boot to Ready.
    ensure!(controller
        .cancel_evacuation(
            &cut,
            &cellar::application::consensus::verification::FleetEvacuationCancelRequest {
                identity: request(248, cut.revision()),
                node: r.old_spec.node,
                fence: Some(
                    cellar::application::consensus::verification::FleetEvacuationCancelFence {
                        process: ProcessGeneration::new(r.boot.process)?,
                        request: ProposalId::new([249; 16])?,
                    }
                ),
            },
            Some(&r.s.observer),
            TIMEOUT
        )
        .await
        .is_err());
    ensure!(controller.read(TIMEOUT).await?.revision() == cut.revision());
    let intent = FleetIsolatedRemovalRequest {
        identity: request(240, cut.revision()),
        isolation: ProposalId::new(ISOLATION)?,
    };
    ensure!(matches!(
        controller
            .complete_isolated_evacuation(&cut, &intent, None, &[], TIMEOUT)
            .await,
        Err(FleetCatalogError::InventoryIncomplete)
    ));
    let wrong = FleetIsolatedRemovalRequest {
        identity: intent.identity.clone(),
        isolation: ProposalId::new([203; 16])?,
    };
    ensure!(controller
        .complete_isolated_evacuation(
            &cut,
            &wrong,
            Some(&mut proof),
            std::slice::from_ref(&r.namespace),
            TIMEOUT
        )
        .await
        .is_err());
    let joined_request = FleetRemovalRequest {
        identity: intent.identity.clone(),
        stop: intent.isolation,
    };
    ensure!(controller
        .complete_evacuation(
            &cut,
            &joined_request,
            Some(&mut proof),
            std::slice::from_ref(&r.namespace),
            TIMEOUT
        )
        .await
        .is_err());
    let bytes = controller
        .get(
            &cut,
            FleetTable::Operations,
            &format!("node-isolation/{}", hex::encode(r.old_spec.node.bytes())),
            TIMEOUT,
        )
        .await?
        .context("admitted isolation")?;
    let expected: [u8; 32] = Sha256::digest(&bytes).into();
    r.s.f
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(controller
        .complete_isolated_evacuation(
            &cut,
            &intent,
            Some(&mut proof),
            std::slice::from_ref(&r.namespace),
            TIMEOUT
        )
        .await
        .is_err());
    let root = controller.read(TIMEOUT).await?;
    ensure!(root.revision() == cut.revision() + 1);
    let recreated = Fixture::open_catalog(&r.s.f.domain, r.s.f.store.clone())?;
    let root = recreated.read(TIMEOUT).await?;
    let receipt = recreated
        .complete_isolated_evacuation(&root, &intent, None, &[], TIMEOUT)
        .await?;
    ensure!(
        receipt.applied_revision() == root.revision()
            && receipt.node() == r.old_spec.node.bytes()
            && receipt.isolation_request() == ISOLATION
            && receipt.isolation_digest() == expected
    );
    ensure!(matches!(
        recreated
            .complete_evacuation(&root, &joined_request, None, &[], TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    ensure!(matches!(
        recreated
            .complete_isolated_evacuation(&root, &wrong, None, &[], TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    let bytes = recreated
        .get(
            &root,
            FleetTable::Nodes,
            &hex::encode(r.old_spec.node.bytes()),
            TIMEOUT,
        )
        .await?
        .context("removed node")?;
    let node: serde_json::Value = serde_json::from_slice(&bytes[13..bytes.len() - 32])?;
    ensure!(
        node["state"] == "Removed"
            && node["isolation"] == serde_json::to_value(ISOLATION)?
            && node["storage_instance"] == serde_json::to_value(r.boot.engine)?
    );
    ensure!(recreated
        .authorize_host(&root, r.old_spec.node, TIMEOUT)
        .await
        .is_err());
    let next = FleetRequestIdentity {
        id: [241; 16],
        operation: [242; 16],
        principal: "next-node-operation".into(),
        expected_revision: root.revision(),
    };
    let accepted = recreated
        .begin_evacuation(
            &root,
            &next,
            r.peers[0],
            std::slice::from_ref(&r.namespace),
            TIMEOUT,
        )
        .await?;
    ensure!(accepted.applied_revision > receipt.applied_revision());
    let retry = recreated
        .complete_isolated_evacuation(&accepted.snapshot, &intent, None, &[], TIMEOUT)
        .await?;
    ensure!(
        retry.applied_revision() == receipt.applied_revision()
            && retry.isolation_digest() == expected
    );
    ensure!(recreated.read(TIMEOUT).await?.revision() == accepted.snapshot.revision());
    let path = r.old_root.path().to_path_buf();
    let metadata =
        r.s.f
            .domain
            .run(async move {
                cellar::asyncrt::blocking(move || cellar::asyncrt::fs().metadata(&path)).await
            })
            .await???;
    ensure!(metadata.is_dir);
    write_read(&r.s.f, &r.active, 4, 3, 21).await?;
    r.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_isolated_removal_failed_survivor_keeps_slot() -> Result<()> {
    let mut r = Box::pin(Repaired::new()).await?;
    r.finalizing().await?;
    let mut proof = r.verification(&r.s.f.catalog).await?;
    joined(r.s.nodes.remove(0).node).await?;
    let cut = r.s.f.catalog.read(TIMEOUT).await?;
    let intent = FleetIsolatedRemovalRequest {
        identity: request(240, cut.revision()),
        isolation: ProposalId::new(ISOLATION)?,
    };
    ensure!(r
        .s
        .f
        .catalog
        .complete_isolated_evacuation(
            &cut,
            &intent,
            Some(&mut proof),
            std::slice::from_ref(&r.namespace),
            TIMEOUT
        )
        .await
        .is_err());
    ensure!(r.s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    let next = FleetRequestIdentity {
        id: [241; 16],
        operation: [242; 16],
        principal: "blocked-next-node-operation".into(),
        expected_revision: cut.revision(),
    };
    ensure!(matches!(
        r.s.f
            .catalog
            .begin_evacuation(
                &cut,
                &next,
                r.peers[1],
                std::slice::from_ref(&r.namespace),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::DisruptionInProgress { .. })
    ));
    r.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_isolated_removal_changed_inventory_keeps_slot() -> Result<()> {
    let r = Box::pin(Repaired::new()).await?;
    r.finalizing().await?;
    let mut proof = r.verification(&r.s.f.catalog).await?;
    let mut config = r.namespace.config().clone();
    config.namespace = NamespaceId::new([87; 16])?;
    config.managed_fleet = None;
    let extra = Catalog::new(
        r.s.f.store.clone(),
        CatalogOptions {
            prefix: "host-enrollment/".into(),
            ..Default::default()
        },
    )?
    .open_namespace(config)
    .await?;
    let cut = r.s.f.catalog.read(TIMEOUT).await?;
    r.s.f
        .catalog
        .declare_namespace(
            &cut,
            &request(239, cut.revision()),
            &extra,
            "Counter",
            TIMEOUT,
        )
        .await?;
    let cut = r.s.f.catalog.read(TIMEOUT).await?;
    let intent = FleetIsolatedRemovalRequest {
        identity: request(240, cut.revision()),
        isolation: ProposalId::new(ISOLATION)?,
    };
    let namespaces = [r.namespace.clone(), extra];
    ensure!(r
        .s
        .f
        .catalog
        .complete_isolated_evacuation(&cut, &intent, Some(&mut proof), &namespaces, TIMEOUT)
        .await
        .is_err());
    ensure!(matches!(
        r.s.f
            .catalog
            .prepare_evacuation_verification(
                &cut,
                ProposalId::new(OPERATION)?,
                &namespaces,
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::InventoryIncomplete)
    ));
    ensure!(r.s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    r.close().await
}
