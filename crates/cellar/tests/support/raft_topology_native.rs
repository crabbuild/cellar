// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Owned inputs -> generated movement -> native seed/joint/settlement. Bounds
//! are explicit fixture attestations, not measured deployment qualification.
use super::super::retirement::discover;
use super::*;
use cellar::application::consensus::verification::{FleetChange, FleetMutation, FleetTable};
use std::{future::Future, pin::Pin};

#[path = "raft_topology_intent_native.rs"]
mod intents;

struct Scenario {
    s: Setup,
    _spare_root: tempfile::TempDir,
    spare_node: cellar::sdk::Node,
    spare: RaftCells,
    live: Vec<Vec<(NodeId, RaftCellHandle)>>,
}

impl Scenario {
    async fn new(materialize: bool) -> Result<Self> {
        let s = Box::pin(Setup::with_capacity(true, 8))
            .await
            .context("topology base setup")?;
        let (root, node, host) = s.f.peer_host(8).await.context("topology spare setup")?;
        let mut live = Vec::new();
        if materialize {
            for proposal in &s.proposals {
                let cut = s.f.catalog.read(TIMEOUT).await?;
                let authorization =
                    s.f.catalog
                        .authorize_group(&cut, &proposal.scope, TIMEOUT)
                        .await?;
                let cells = registered(&s.nodes, &authorization)
                    .await
                    .context("topology register history")?;
                activate(&cells, &authorization)
                    .await
                    .context("topology activate history")?;
                write_read(&s.f, &cells, 1, 7, 7)
                    .await
                    .context("topology initial write")?;
                live.push(s.nodes.iter().map(|n| n.spec.node).zip(cells).collect());
            }
        }
        for (index, target) in s
            .nodes
            .iter()
            .map(|n| &n.host)
            .chain(std::iter::once(&host))
            .enumerate()
        {
            let origin = if index == 0 {
                &s.nodes[1].host
            } else {
                &s.nodes[0].host
            };
            let evidence = attestation(&s.f, origin, target)
                .await
                .context("topology qualification evidence")?;
            let cut = s.f.catalog.read(TIMEOUT).await?;
            s.f.catalog
                .admit_node_capacity(
                    &cut,
                    &FleetCapacityRequest {
                        identity: Fixture::request(225 + index as u8, cut.revision()),
                        attestation: evidence,
                        action: FleetCapacityAction::Activate,
                    },
                    origin,
                    std::slice::from_ref(&s.namespace),
                    TIMEOUT,
                )
                .await
                .context("topology capacity admission")?;
        }
        discover(
            &s.f,
            &[
                &s.nodes[0].host,
                &s.nodes[1].host,
                &s.nodes[2].host,
                &s.target,
                &host,
            ],
        )
        .await
        .context("topology discovery")?;
        Ok(Self {
            s,
            _spare_root: root,
            spare_node: node,
            spare: host,
            live,
        })
    }

    fn hosts(&self) -> Vec<&RaftCells> {
        self.s
            .nodes
            .iter()
            .map(|n| &n.host)
            .chain([&self.s.target, &self.spare])
            .collect()
    }

    async fn request(&self) -> Result<FleetTopologyRequest> {
        let mut nodes = Vec::new();
        let mut keep = Vec::new();
        let mut bytes = 0;
        for host in self.hosts() {
            let sample = host.observe_node(TIMEOUT).await?;
            keep.push(sample.node.identity.node);
            bytes = bytes.max(sample.node.profile.bytes_per_group);
            nodes.push(FleetTopologyNodeResources {
                attestation: FleetResourceAttestation {
                    boot: sample.fleet_boot.unwrap(),
                    profile_revision: 1,
                    used: FleetResourceBounds {
                        engine_slots: sample.node.groups,
                        durable_bytes: 1024 * 1024,
                        memory_bytes: 64 * 1024 * 1024,
                    },
                    evidence_digest: [71; 32],
                    evidence_reference: "fixture-only/topology-occupancy".into(),
                },
                reserve: FleetResourceBounds::default(),
            });
        }
        Ok(FleetTopologyRequest {
            keep,
            policy: FleetTopologyPolicy {
                revision: 1,
                distinct_zones: false,
                distinct_regions: false,
                max_work: 1_000_000,
            },
            nodes,
            histories: self
                .s
                .proposals
                .iter()
                .map(|p| FleetTopologyHistoryResources {
                    group: p.scope.clone(),
                    resident: FleetResourceBounds {
                        engine_slots: 1,
                        durable_bytes: bytes,
                        memory_bytes: 8 * 1024 * 1024,
                    },
                    peak: FleetResourceBounds {
                        engine_slots: 1,
                        durable_bytes: bytes * 2,
                        memory_bytes: 16 * 1024 * 1024,
                    },
                })
                .collect(),
        })
    }

    async fn collection(
        &self,
        request: &FleetTopologyRequest,
    ) -> Result<FleetTopologyVerification> {
        let cut = self.s.f.catalog.read(TIMEOUT).await?;
        self.s
            .f
            .catalog
            .prepare_topology_verification(
                &cut,
                request,
                &self.s.nodes[0].host,
                std::slice::from_ref(&self.s.namespace),
                TIMEOUT,
            )
            .await
            .context("topology prepare collection")
    }

    async fn close(self) -> Result<()> {
        joined(self.spare_node).await?;
        self.s.close().await
    }
}

struct Evidence {
    target: RaftCells,
    request: FleetMovementExecutionRequest,
}

impl FleetMovementEvidence for Evidence {
    fn refresh<'a>(
        &'a self,
        movement: &'a FleetPendingMovement,
        timeout: Duration,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = std::result::Result<FleetMovementExecutionRequest, FleetCatalogError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            if movement.reservation != self.request.reservation {
                return Err(FleetCatalogError::Conflict);
            }
            let sample = self
                .target
                .observe_node(timeout)
                .await
                .map_err(|_| FleetCatalogError::Unconfirmed)?;
            if sample.fleet_boot.as_ref() != Some(&self.request.attestation.boot) {
                return Err(FleetCatalogError::Conflict);
            }
            let mut request = self.request.clone();
            request.attestation.used.engine_slots = sample.node.groups;
            Ok(request)
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_topology_generated_moves_execute_and_recollect_retained_bindings() -> Result<()> {
    Box::pin(delivery(false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_topology_long_walk_refreshes_nodes_and_seeds_latest_sql() -> Result<()> {
    Box::pin(delivery(true)).await
}

async fn delivery(long_walk: bool) -> Result<()> {
    let mut f = Box::pin(Scenario::new(true)).await?;
    for round in 0..2 {
        discover(&f.s.f, &f.hosts()).await?;
        let request = f.request().await?;
        let mut collection = f
            .collection(&request)
            .await
            .with_context(|| format!("topology collection round {round}"))?;
        ensure!(matches!(
            collection.preview(TIMEOUT).await,
            Err(FleetCatalogError::InventoryIncomplete)
        ));
        ensure!(collection.verify_next(TIMEOUT).await?);
        if long_walk && round == 0 {
            let revision = f.s.f.catalog.read(TIMEOUT).await?.revision();
            for live in &f.live {
                write_read(
                    &f.s.f,
                    &live.iter().map(|(_, h)| h.clone()).collect::<Vec<_>>(),
                    2,
                    5,
                    12,
                )
                .await?;
            }
            // The first group's historical ReadIndex is older than the final
            // node-capacity freshness window. Data writes must not change the
            // rooted topology, and real admission/seed must include those writes.
            f.s.f
                .domain
                .run(async { cellar::asyncrt::sleep(Duration::from_millis(5100)).await })
                .await?;
            ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == revision);
        }
        while collection.verify_next(TIMEOUT).await? {}
        ensure!(collection.histories_checked() == 2);
        let preview = collection.preview(TIMEOUT).await?;
        ensure!(preview.plan().moves.len() == 2 - round);
        ensure!(preview.plan().recovery.len() == 5);
        let movement = preview
            .first_movement(Fixture::request(
                210 + round as u8 * 2,
                preview.inventory_revision(),
            ))?
            .unwrap();
        let cut = f.s.f.catalog.read(TIMEOUT).await?;
        ensure!(cut.revision() == preview.inventory_revision());
        let admission =
            f.s.f
                .catalog
                .reserve_movement(
                    &cut,
                    &movement,
                    &f.s.nodes[0].host,
                    std::slice::from_ref(&f.s.namespace),
                    TIMEOUT,
                )
                .await?;
        let other = Fixture::open_catalog(&f.s.f.domain, f.s.f.store.clone())?;
        let other_cut = other.read(TIMEOUT).await?;
        let retry = other
            .reserve_movement(
                &other_cut,
                &movement,
                &f.s.nodes[0].host,
                std::slice::from_ref(&f.s.namespace),
                TIMEOUT,
            )
            .await?;
        ensure!(retry.resources().applied_revision() == admission.resources().applied_revision());
        ensure!(matches!(
            collection.preview(TIMEOUT).await,
            Err(FleetCatalogError::Conflict)
        ));
        let blocked = f
            .collection(&request)
            .await
            .err()
            .ok_or_else(|| anyhow::anyhow!("active movement was not blocked"))?;
        ensure!(matches!(
            blocked.downcast_ref::<FleetCatalogError>(),
            Some(FleetCatalogError::Resource(LedgerError::MovementLimit))
        ));
        let target = if movement.plan.replacement.node == NodeId::new([7; 16])? {
            f.s.target.clone()
        } else {
            ensure!(movement.plan.replacement.node == NodeId::new([8; 16])?);
            f.spare.clone()
        };
        let evidence = Arc::new(Evidence {
            target: target.clone(),
            request: FleetMovementExecutionRequest {
                reservation: movement.resources.identity.id,
                attestation: movement.resources.attestation.clone(),
                from_s3: true,
            },
        });
        let service = other
            .start_movement_service(
                f.s.nodes[0].host.clone(),
                evidence,
                FleetMovementServiceOptions {
                    interval: Duration::from_millis(50),
                    retry_max_interval: Duration::from_millis(500),
                    turn_timeout: TIMEOUT,
                },
            )
            .await?;
        f.s.f
            .domain
            .run(cellar::asyncrt::timeout(TIMEOUT * 4, async {
                while service.status().settled_movements != 1 {
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
            }))
            .await??;
        service.shutdown().await;
        let index =
            f.s.proposals
                .iter()
                .position(|p| p.scope == movement.resources.group)
                .unwrap();
        let handle = target.cell(
            movement.resources.group.namespace,
            &movement.resources.group.cell,
        )?;
        let binding = f.live[index]
            .iter_mut()
            .find(|(node, _)| *node == movement.source)
            .unwrap();
        *binding = (movement.plan.replacement.node, handle);
        write_read(
            &f.s.f,
            &f.live[index]
                .iter()
                .map(|(_, h)| h.clone())
                .collect::<Vec<_>>(),
            if long_walk { 3 } else { 2 },
            if long_walk { 9 } else { 5 },
            if long_walk { 21 } else { 12 },
        )
        .await?;
    }
    discover(&f.s.f, &f.hosts()).await?;
    let request = f.request().await?;
    let mut collection = f.collection(&request).await?;
    while collection.verify_next(TIMEOUT).await? {}
    let preview = collection.preview(TIMEOUT).await?;
    ensure!(preview.plan().moves.is_empty() && preview.plan().recovery.len() == 5);
    ensure!(preview
        .first_movement(Fixture::request(214, preview.inventory_revision()))?
        .is_none());
    for live in &f.live {
        write_read(
            &f.s.f,
            &live.iter().map(|(_, h)| h.clone()).collect::<Vec<_>>(),
            if long_walk { 4 } else { 3 },
            if long_walk { 0 } else { 9 },
            21,
        )
        .await?;
    }
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_topology_incomplete_unqualified_and_changed_inputs_refuse() -> Result<()> {
    Box::pin(refusal()).await
}

async fn refusal() -> Result<()> {
    let f = Box::pin(Scenario::new(true)).await?;
    let request = f.request().await?;
    let mut missing = request.clone();
    missing.nodes.pop();
    ensure!(f.collection(&missing).await.is_err());
    let mut rebound = request.clone();
    rebound.nodes[0].attestation.boot.process = [44; 16];
    ensure!(f.collection(&rebound).await.is_err());
    let mut incomplete = request.clone();
    incomplete.histories.pop();
    let mut collector = f.collection(&incomplete).await?;
    let mut refused = false;
    loop {
        match collector.verify_next(TIMEOUT).await {
            Ok(true) => (),
            Ok(false) => break,
            Err(FleetCatalogError::InventoryIncomplete) => {
                refused = true;
                break;
            }
            Err(error) => return Err(error.into()),
        }
    }
    ensure!(refused && collector.preview(TIMEOUT).await.is_err());
    let mut collection = f.collection(&request).await?;
    ensure!(collection.verify_next(TIMEOUT).await?);
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    f.s.f
        .catalog
        .commit(
            &cut,
            &FleetMutation {
                id: [240; 16],
                operation: [241; 16],
                principal: "topology-root-change".into(),
                expected_revision: cut.revision(),
                changes: vec![FleetChange {
                    table: FleetTable::Policy,
                    key: "fixture-topology-input-change".into(),
                    value: Some(vec![1]),
                }],
            },
            TIMEOUT,
        )
        .await?;
    ensure!(matches!(
        collection.verify_next(TIMEOUT).await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(matches!(
        collection.preview(TIMEOUT).await,
        Err(FleetCatalogError::Conflict)
    ));
    f.close().await?;
    let f = Box::pin(Scenario::new(false)).await?;
    let request = f.request().await?;
    let mut collection = f.collection(&request).await?;
    ensure!(matches!(
        collection.verify_next(TIMEOUT).await,
        Err(FleetCatalogError::InventoryIncomplete)
    ));
    ensure!(collection.histories_checked() == 0);
    f.close().await
}

async fn settle_with_service(
    f: &Scenario,
    movement: &FleetMovementRequest,
    target: &RaftCells,
) -> Result<()> {
    let service =
        f.s.f
            .catalog
            .start_movement_service(
                f.s.nodes[0].host.clone(),
                Arc::new(Evidence {
                    target: target.clone(),
                    request: FleetMovementExecutionRequest {
                        reservation: movement.resources.identity.id,
                        attestation: movement.resources.attestation.clone(),
                        from_s3: true,
                    },
                }),
                FleetMovementServiceOptions {
                    interval: Duration::from_millis(50),
                    retry_max_interval: Duration::from_millis(500),
                    turn_timeout: TIMEOUT,
                },
            )
            .await?;
    f.s.f
        .domain
        .run(cellar::asyncrt::timeout(TIMEOUT * 4, async {
            while service.status().settled_movements != 1 {
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
        }))
        .await??;
    service.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_topology_retained_dynamic_binding_survives_later_move() -> Result<()> {
    Box::pin(retained_binding()).await
}

async fn fence_and_reuse(
    f: &Scenario,
    movement: FleetMovementRequest,
    permit: FleetPermitAuthorization,
    primary: &RaftCellHandle,
    target: &RaftCells,
) -> Result<(FleetMovementRequest, FleetPermitAuthorization)> {
    let empty = movement;
    let fenced = primary.fence_fleet_permit(permit, TIMEOUT).await?;
    ensure!(fenced.phase == Some(FleetPermitPhase::Fenced));
    settle_with_service(f, &empty, target).await?;
    let binding_key = format!(
        "resource-binding/{}/{:016x}/{}",
        f.s.f.catalog.group_binding(&empty.resources.group)?.key()?,
        empty.plan.replacement.member.get(),
        hex::encode(empty.plan.replacement.node.bytes())
    );
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let empty_pointer =
        f.s.f
            .catalog
            .get(&cut, FleetTable::Operations, &binding_key, TIMEOUT)
            .await?
            .unwrap();
    for (index, value) in [None, Some(vec![1])].into_iter().enumerate() {
        ensure!(f
            .s
            .f
            .catalog
            .commit(
                &cut,
                &FleetMutation {
                    id: [242 + index as u8; 16],
                    operation: [244; 16],
                    principal: "fixture-binding-tamper".into(),
                    expected_revision: cut.revision(),
                    changes: vec![FleetChange {
                        table: FleetTable::Operations,
                        key: binding_key.clone(),
                        value
                    }],
                },
                TIMEOUT
            )
            .await
            .is_err());
    }
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    discover(&f.s.f, &f.hosts()).await?;
    let request = f.request().await?;
    let mut walk = f.collection(&request).await?;
    while walk.verify_next(TIMEOUT).await? {}
    let preview = walk.preview(TIMEOUT).await?;
    let movement = preview
        .first_movement(Fixture::request(212, preview.inventory_revision()))?
        .unwrap();
    ensure!(
        movement.resources.group == empty.resources.group
            && movement.plan.replacement == empty.plan.replacement
    );
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let admitted =
        f.s.f
            .catalog
            .reserve_movement(
                &cut,
                &movement,
                &f.s.nodes[0].host,
                std::slice::from_ref(&f.s.namespace),
                TIMEOUT,
            )
            .await?;
    let permit = admitted.authorization();
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        f.s.f
            .catalog
            .get(&cut, FleetTable::Operations, &binding_key, TIMEOUT)
            .await?
            .unwrap()
            != empty_pointer
    );
    Ok((movement, permit))
}

async fn retained_binding() -> Result<()> {
    let mut f = Box::pin(Scenario::new(true)).await?;
    let request = f.request().await?;
    let mut walk = f.collection(&request).await?;
    while walk.verify_next(TIMEOUT).await? {}
    let preview = walk.preview(TIMEOUT).await?;
    let movement = preview
        .first_movement(Fixture::request(210, preview.inventory_revision()))?
        .unwrap();
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let admission =
        f.s.f
            .catalog
            .reserve_movement(
                &cut,
                &movement,
                &f.s.nodes[0].host,
                std::slice::from_ref(&f.s.namespace),
                TIMEOUT,
            )
            .await?;
    let permit = admission.authorization();
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let auth =
        f.s.f
            .catalog
            .authorize_group(&cut, &movement.resources.group, TIMEOUT)
            .await?;
    let index =
        f.s.proposals
            .iter()
            .position(|p| p.scope == movement.resources.group)
            .unwrap();
    let cells = f.live[index]
        .iter()
        .map(|(_, h)| h.clone())
        .collect::<Vec<_>>();
    let p = primary(&f.s.f, &cells).await?;
    let source = cells[p]
        .observe_fleet_membership(auth.clone(), TIMEOUT)
        .await?;
    let target = if movement.plan.replacement.node == NodeId::new([7; 16])? {
        f.s.target.clone()
    } else {
        f.spare.clone()
    };
    let (movement, permit) =
        Box::pin(fence_and_reuse(&f, movement, permit, &cells[p], &target)).await?;
    let learner = target.register_managed_history(auth, TIMEOUT).await?;
    cells[p]
        .begin_managed_repair(permit.clone(), movement.plan.clone(), TIMEOUT)
        .await?;
    learner
        .activate_managed_learner(
            permit.clone(),
            LearnerActivation {
                member: movement.plan.replacement.member,
                source: source.replica().member,
                source_node: source.replica().node,
                from_s3: true,
                timeout: TIMEOUT,
            },
        )
        .await?;
    ensure!(
        super::super::retirement::step(&f.s.f, &cells[p], &permit, RepairStep::AbortLearner)
            .await?
            .phase
            == RepairPhase::Aborted
    );
    settle_with_service(&f, &movement, &target).await?;
    ensure!(target.observe_node(TIMEOUT).await?.node.groups == 1);
    // The consumed learner is deliberately absent from the current Raft binding
    // ledger. Its rooted admission/retained receipt must supply provenance.
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let auth =
        f.s.f
            .catalog
            .authorize_group(&cut, &movement.resources.group, TIMEOUT)
            .await?;
    let proof = cells[p].observe_fleet_membership(auth, TIMEOUT).await?;
    ensure!(proof
        .joined_member(movement.plan.replacement.member)?
        .is_none());
    let (_extra_root, extra_node, extra) = f.s.f.peer_host(9).await?;
    let evidence = attestation(&f.s.f, &f.s.nodes[0].host, &extra).await?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    f.s.f
        .catalog
        .admit_node_capacity(
            &cut,
            &FleetCapacityRequest {
                identity: Fixture::request(235, cut.revision()),
                attestation: evidence,
                action: FleetCapacityAction::Activate,
            },
            &f.s.nodes[0].host,
            std::slice::from_ref(&f.s.namespace),
            TIMEOUT,
        )
        .await?;
    let all = f
        .hosts()
        .into_iter()
        .cloned()
        .chain([extra.clone()])
        .collect::<Vec<_>>();
    let references = all.iter().collect::<Vec<_>>();
    discover(&f.s.f, &references).await?;
    let mut request = f.request().await?;
    let local = extra.observe_node(TIMEOUT).await?;
    request.keep.push(local.node.identity.node);
    request.nodes.push(FleetTopologyNodeResources {
        attestation: FleetResourceAttestation {
            boot: local.fleet_boot.unwrap(),
            profile_revision: 1,
            used: FleetResourceBounds {
                engine_slots: local.node.groups,
                durable_bytes: 1024 * 1024,
                memory_bytes: 64 * 1024 * 1024,
            },
            evidence_digest: [71; 32],
            evidence_reference: "fixture-only/topology-occupancy".into(),
        },
        reserve: FleetResourceBounds::default(),
    });
    let mut walk = f.collection(&request).await?;
    while walk
        .verify_next(TIMEOUT)
        .await
        .context("collect retained aborted learner")?
    {}
    let preview = walk.preview(TIMEOUT).await?;
    ensure!(preview.plan().recovery.len() == 6);
    let next = preview
        .first_movement(Fixture::request(214, preview.inventory_revision()))?
        .unwrap();
    ensure!(next.resources.group == movement.resources.group);
    ensure!(next.plan.replacement.member.get() > movement.plan.replacement.member.get());
    ensure!(next.plan.replacement.node != movement.plan.replacement.node);
    let target2 = match next.plan.replacement.node.bytes()[0] {
        7 => f.s.target.clone(),
        8 => f.spare.clone(),
        9 => extra.clone(),
        _ => anyhow::bail!("unexpected retained-binding target"),
    };
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    f.s.f
        .catalog
        .reserve_movement(
            &cut,
            &next,
            &f.s.nodes[0].host,
            std::slice::from_ref(&f.s.namespace),
            TIMEOUT,
        )
        .await?;
    settle_with_service(&f, &next, &target2).await?;
    let binding = f.live[index]
        .iter_mut()
        .find(|(node, _)| *node == next.source)
        .unwrap();
    *binding = (
        next.plan.replacement.node,
        target2.cell(next.resources.group.namespace, &next.resources.group.cell)?,
    );
    write_read(
        &f.s.f,
        &f.live[index]
            .iter()
            .map(|(_, h)| h.clone())
            .collect::<Vec<_>>(),
        2,
        5,
        12,
    )
    .await?;
    discover(&f.s.f, &references).await?;
    // Refresh deployment occupancy for the new target while preserving the
    // same exact boot/profile; the old aborted target stays physically charged.
    for input in &mut request.nodes {
        if input.attestation.boot.node == next.plan.replacement.node.bytes() {
            input.attestation.used.engine_slots += 1;
        }
    }
    let mut walk = f.collection(&request).await?;
    while walk
        .verify_next(TIMEOUT)
        .await
        .context("collect older aborted binding after later complete")?
    {}
    let preview = walk.preview(TIMEOUT).await?;
    let retained = preview
        .plan()
        .peaks
        .iter()
        .find(|peak| peak.scope.node.node == movement.plan.replacement.node)
        .unwrap();
    ensure!(retained.resources.engine_slots >= 1);
    joined(extra_node).await?;
    f.close().await
}
