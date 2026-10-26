// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real independent catalog adapters, owned TLS probes and root-CAS faults.
//! Resource evidence is fixture attestation, not kernel/RustFS qualification.
use super::super::capacity_tests::{attestation, joined};
use super::*;
use cellar_logic::raft_fleet::{LedgerError, ResourceKind};

#[path = "raft_movement_native.rs"]
mod movement_tests;
#[path = "raft_topology_native.rs"]
mod topology_tests;

struct Setup {
    f: Fixture,
    nodes: Vec<Running>,
    _target_root: tempfile::TempDir,
    target_node: cellar::sdk::Node,
    target: RaftCells,
    namespace: NamespaceCatalog,
    proposals: Vec<BirthProposal>,
}

impl Setup {
    async fn new(qualified: bool) -> Result<Self> {
        Self::with_capacity(qualified, 1).await
    }

    async fn with_capacity(qualified: bool, capacity: u32) -> Result<Self> {
        Self::with_histories(qualified, capacity, 2).await
    }

    async fn with_histories(qualified: bool, capacity: u32, histories: u8) -> Result<Self> {
        let f = Fixture::with_managed_births(true).await?;
        let nodes = fleet(&f).await?;
        let (namespace, _) = initial_birth(&f).await?;
        let (first, _) = birth(&f).await?;
        let mut proposals = vec![first.clone()];
        for index in 1..histories {
            let mut second = first.clone();
            second.scope.cell = format!("Counter:resource-{index}");
            second.scope.group = GroupId::new([82 + index; 16])?;
            let cut = f.catalog.read(TIMEOUT).await?;
            let pending = f
                .catalog
                .reserve_birth(
                    &cut,
                    &Fixture::request(130 + index * 2, cut.revision()),
                    &namespace,
                    second.clone(),
                    TIMEOUT,
                )
                .await?;
            f.domain
                .run(namespace.resolve_managed_birth(&pending.authorization, TIMEOUT))
                .await??;
            let cut = f.catalog.read(TIMEOUT).await?;
            f.catalog
                .confirm_birth(
                    &cut,
                    &Fixture::request(131 + index * 2, cut.revision()),
                    &namespace,
                    &pending.authorization,
                    TIMEOUT,
                )
                .await?;
            proposals.push(second);
        }
        let spec = f.peer_spec(7)?;
        let cut = f.catalog.read(TIMEOUT).await?;
        let registered = f
            .catalog
            .register_node(&cut, &Fixture::request(57, cut.revision()), &spec, TIMEOUT)
            .await?;
        let authorization = f
            .catalog
            .authorize_host(&registered.snapshot, spec.node, TIMEOUT)
            .await?;
        let target_root = tempfile::tempdir()?;
        let target_node = public_node::node_named(
            target_root.path(),
            f.definition.application.clone(),
            &spec.label,
        )
        .await?;
        let (mut config, services) = f.config_for(&target_node, authorization, &spec).await?;
        let profile = config.host.node_profile.as_mut().unwrap();
        profile.group_capacity = capacity;
        profile.leader_capacity = capacity;
        let target = target_node.start_raft_cells(config, services).await?;
        if qualified {
            let evidence = attestation(&f, &nodes[0].host, &target).await?;
            let cut = f.catalog.read(TIMEOUT).await?;
            f.catalog
                .admit_node_capacity(
                    &cut,
                    &FleetCapacityRequest {
                        identity: Fixture::request(201, cut.revision()),
                        attestation: evidence,
                        action: FleetCapacityAction::Activate,
                    },
                    &nodes[0].host,
                    std::slice::from_ref(&namespace),
                    TIMEOUT,
                )
                .await?;
        } else {
            f.state(spec.node, FleetNodeState::Active, 201).await?;
        }
        let cut = f.catalog.read(TIMEOUT).await?;
        f.remote(&nodes[0].host, spec.node, cut.revision()).await?;
        Ok(Self {
            f,
            nodes,
            _target_root: target_root,
            target_node,
            target,
            namespace,
            proposals,
        })
    }

    async fn request(&self, group: usize, id: u8, revision: u64) -> Result<FleetResourceRequest> {
        let local = self.target.observe_node(TIMEOUT).await?;
        Ok(FleetResourceRequest {
            identity: Fixture::request(id, revision),
            group: self.proposals[group].scope.clone(),
            attestation: FleetResourceAttestation {
                boot: local.fleet_boot.unwrap(),
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
        })
    }

    async fn waiting(&self) -> Result<()> {
        self.f
            .domain
            .run(cellar::asyncrt::timeout(TIMEOUT, async {
                while !self.f.store.fleet_cas_waiting.load(Ordering::Acquire) {
                    cellar::asyncrt::sleep(Duration::from_millis(1)).await;
                }
            }))
            .await??;
        Ok(())
    }

    fn release(&self) {
        self.f.store.fleet_cas_hang.store(false, Ordering::Release);
        self.f.store.fleet_cas_resume.notify_waiters();
    }

    async fn close(self) -> Result<()> {
        joined(self.target_node).await?;
        for node in self.nodes {
            joined(node.node).await?;
        }
        self.f.domain.shutdown().await;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_placement_concurrent_targets() -> Result<()> {
    let s = Box::pin(Setup::new(true)).await?;
    let other = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let other_cut = other.read(TIMEOUT).await?;
    ensure!(cut.revision() == other_cut.revision());
    let first = s.request(0, 210, cut.revision()).await?;
    let second = s.request(1, 211, cut.revision()).await?;
    s.f.store.fleet_cas_hang.store(true, Ordering::Release);
    s.f.store.hold_next_fleet_cas.store(true, Ordering::Release);
    let (catalog, sender, namespace) = (
        s.f.catalog.clone(),
        s.nodes[0].host.clone(),
        s.namespace.clone(),
    );
    let pending = s.f.domain.spawn(async move {
        catalog
            .reserve_resources(&cut, &first, &sender, &[namespace], TIMEOUT)
            .await
    })?;
    s.waiting().await?;
    let winner = other
        .reserve_resources(
            &other_cut,
            &second,
            &s.nodes[0].host,
            std::slice::from_ref(&s.namespace),
            TIMEOUT,
        )
        .await;
    s.release();
    let loser = pending.await?;
    let winner = winner?;
    ensure!(matches!(loser, Err(FleetCatalogError::Conflict)));
    ensure!(winner.applied_revision() == other_cut.revision() + 1 && winner.request() == [211; 16]);
    let cut = s.f.catalog.read(TIMEOUT).await?;
    s.f.remote(&s.nodes[0].host, winner.node(), cut.revision())
        .await?;
    let retry = s.request(0, 212, cut.revision()).await?;
    let refused =
        s.f.catalog
            .reserve_resources(
                &cut,
                &retry,
                &s.nodes[0].host,
                std::slice::from_ref(&s.namespace),
                TIMEOUT,
            )
            .await;
    ensure!(matches!(
        refused,
        Err(FleetCatalogError::Resource(LedgerError::CapacityExceeded(
            ResourceKind::EngineSlots
        )))
    ));
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    ensure!(s.target.observe_node(TIMEOUT).await?.node.groups == 0);
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_resource_lost_reply_reconciles_and_cannot_reset_book() -> Result<()> {
    let s = Box::pin(Setup::new(true)).await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let request = s.request(0, 210, cut.revision()).await?;
    s.f.store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    ensure!(s
        .f
        .catalog
        .reserve_resources(
            &cut,
            &request,
            &s.nodes[0].host,
            std::slice::from_ref(&s.namespace),
            TIMEOUT
        )
        .await
        .is_err());
    let applied = cut.revision() + 1;
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == applied);
    s.f.state(NodeId::new([7; 16])?, FleetNodeState::Draining, 214)
        .await?;
    joined(s.target_node).await?;
    let reopened = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let cut = reopened.read(TIMEOUT).await?;
    let receipt = reopened
        .reserve_resources(&cut, &request, &s.nodes[0].host, &[], TIMEOUT)
        .await?;
    ensure!(receipt.applied_revision() == applied && receipt.observed_revision() == cut.revision());
    let mut changed = request.clone();
    changed.peak.memory_bytes += 1;
    ensure!(matches!(
        reopened
            .reserve_resources(&cut, &changed, &s.nodes[0].host, &[], TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    for (id, table, key) in [
        (215, FleetTable::Policy, "resource-reservations".to_owned()),
        (
            216,
            FleetTable::Operations,
            format!("resource/{}", hex::encode(request.identity.id)),
        ),
    ] {
        let mutation = FleetMutation {
            id: [id; 16],
            operation: [100; 16],
            principal: "fixture-reset".into(),
            expected_revision: cut.revision(),
            changes: vec![FleetChange {
                table,
                key,
                value: None,
            }],
        };
        ensure!(matches!(
            reopened.commit(&cut, &mutation, TIMEOUT).await,
            Err(FleetCatalogError::Invalid(_))
        ));
    }
    ensure!(reopened.read(TIMEOUT).await?.revision() == cut.revision());
    for node in s.nodes {
        joined(node.node).await?;
    }
    s.f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_resource_refuses_raw_active_and_incomplete_inputs() -> Result<()> {
    let s = Box::pin(Setup::new(false)).await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let request = s.request(0, 210, cut.revision()).await?;
    ensure!(matches!(
        s.f.catalog
            .reserve_resources(
                &cut,
                &request,
                &s.nodes[0].host,
                std::slice::from_ref(&s.namespace),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    let evidence = attestation(&s.f, &s.nodes[0].host, &s.target).await?;
    s.f.catalog
        .admit_node_capacity(
            &cut,
            &FleetCapacityRequest {
                identity: Fixture::request(202, cut.revision()),
                attestation: evidence,
                action: FleetCapacityAction::Activate,
            },
            &s.nodes[0].host,
            std::slice::from_ref(&s.namespace),
            TIMEOUT,
        )
        .await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    s.f.remote(&s.nodes[0].host, NodeId::new([7; 16])?, cut.revision())
        .await?;
    let request = s.request(0, 211, cut.revision()).await?;
    ensure!(matches!(
        s.f.catalog
            .reserve_resources(&cut, &request, &s.nodes[0].host, &[], TIMEOUT)
            .await,
        Err(FleetCatalogError::InventoryIncomplete)
    ));
    let mut unfit = request.clone();
    unfit.peak.durable_bytes -= 1;
    ensure!(matches!(
        s.f.catalog
            .reserve_resources(
                &cut,
                &unfit,
                &s.nodes[0].host,
                std::slice::from_ref(&s.namespace),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Invalid("resource learner byte bound"))
    ));
    let mut wrong = request.clone();
    wrong.attestation.boot.boot = [99; 16];
    ensure!(matches!(
        s.f.catalog
            .reserve_resources(
                &cut,
                &wrong,
                &s.nodes[0].host,
                std::slice::from_ref(&s.namespace),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    let mut memory = request;
    memory.peak.memory_bytes = 3 * 1024 * 1024 * 1024;
    ensure!(matches!(
        s.f.catalog
            .reserve_resources(
                &cut,
                &memory,
                &s.nodes[0].host,
                std::slice::from_ref(&s.namespace),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Resource(LedgerError::CapacityExceeded(
            ResourceKind::MemoryBytes
        )))
    ));
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_resource_exclusion_wins_at_cas() -> Result<()> {
    let s = Box::pin(Setup::new(true)).await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let request = s.request(0, 210, cut.revision()).await?;
    s.f.store.fleet_cas_hang.store(true, Ordering::Release);
    s.f.store.hold_next_fleet_cas.store(true, Ordering::Release);
    let (catalog, sender, namespace) = (
        s.f.catalog.clone(),
        s.nodes[0].host.clone(),
        s.namespace.clone(),
    );
    let pending = s.f.domain.spawn(async move {
        catalog
            .reserve_resources(&cut, &request, &sender, &[namespace], TIMEOUT)
            .await
    })?;
    s.waiting().await?;
    let other = Fixture::open_catalog(&s.f.domain, s.f.store.clone())?;
    let revision =
        Fixture::state_on(&other, NodeId::new([7; 16])?, FleetNodeState::Draining, 211).await;
    s.release();
    let refused = pending.await?;
    let revision = revision?;
    ensure!(matches!(refused, Err(FleetCatalogError::Conflict)));
    let cut = s.f.catalog.read(TIMEOUT).await?;
    ensure!(cut.revision() == revision);
    ensure!(s
        .f
        .catalog
        .get(&cut, FleetTable::Policy, "resource-reservations", TIMEOUT)
        .await?
        .is_none());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_resource_expired_cas_has_no_detached_writer() -> Result<()> {
    let s = Box::pin(Setup::new(true)).await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let request = s.request(0, 210, cut.revision()).await?;
    s.f.store.fleet_cas_hang.store(true, Ordering::Release);
    s.f.store.hold_next_fleet_cas.store(true, Ordering::Release);
    let refused =
        s.f.catalog
            .reserve_resources(
                &cut,
                &request,
                &s.nodes[0].host,
                std::slice::from_ref(&s.namespace),
                Duration::from_millis(500),
            )
            .await;
    ensure!(matches!(refused, Err(FleetCatalogError::Unconfirmed)));
    ensure!(!s.f.store.hold_next_fleet_cas.load(Ordering::Acquire));
    ensure!(!s.f.store.fleet_cas_waiting.load(Ordering::Acquire));
    s.release();
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    let admitted =
        s.f.catalog
            .reserve_resources(
                &cut,
                &request,
                &s.nodes[0].host,
                std::slice::from_ref(&s.namespace),
                TIMEOUT,
            )
            .await?;
    ensure!(admitted.applied_revision() == cut.revision() + 1);
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_resource_unmaterialized_genesis_is_not_free() -> Result<()> {
    let s = Box::pin(Setup::with_capacity(true, 2)).await?;
    let mut proposal = s.proposals[0].clone();
    proposal.scope.cell = "Counter:pending-resource".into();
    proposal.scope.group = GroupId::new([84; 16])?;
    proposal.voters[2].node = NodeId::new([7; 16])?;
    let genesis_scope = proposal.scope.clone();
    let cut = s.f.catalog.read(TIMEOUT).await?;
    let birth =
        s.f.catalog
            .reserve_birth(
                &cut,
                &Fixture::request(220, cut.revision()),
                &s.namespace,
                proposal,
                TIMEOUT,
            )
            .await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    s.f.remote(&s.nodes[0].host, NodeId::new([7; 16])?, cut.revision())
        .await?;
    let request = s.request(0, 221, cut.revision()).await?;
    ensure!(matches!(
        s.f.catalog
            .reserve_resources(
                &cut,
                &request,
                &s.nodes[0].host,
                std::slice::from_ref(&s.namespace),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Resource(LedgerError::Incomplete))
    ));
    s.f.domain
        .run(
            s.namespace
                .resolve_managed_birth(&birth.authorization, TIMEOUT),
        )
        .await??;
    s.f.catalog
        .confirm_birth(
            &cut,
            &Fixture::request(222, cut.revision()),
            &s.namespace,
            &birth.authorization,
            TIMEOUT,
        )
        .await?;
    let cut = s.f.catalog.read(TIMEOUT).await?;
    s.f.remote(&s.nodes[0].host, NodeId::new([7; 16])?, cut.revision())
        .await?;
    let request = s.request(0, 223, cut.revision()).await?;
    ensure!(matches!(
        s.f.catalog
            .reserve_resources(
                &cut,
                &request,
                &s.nodes[0].host,
                std::slice::from_ref(&s.namespace),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Resource(LedgerError::Incomplete))
    ));
    ensure!(s.target.observe_node(TIMEOUT).await?.node.groups == 0);
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    let authorization =
        s.f.catalog
            .authorize_group(&cut, &genesis_scope, TIMEOUT)
            .await?;
    let cell = s
        .target
        .register_managed_history(authorization.clone(), TIMEOUT)
        .await?;
    cell.activate_managed_initial_member(authorization, TIMEOUT)
        .await?;
    let observed = s.nodes[0]
        .host
        .inspect_peer_physical_inventory(NodeId::new([7; 16])?, cut.revision(), TIMEOUT)
        .await?;
    let inventory = observed.inventory.as_ref().unwrap();
    ensure!(inventory.histories().len() == 1 && inventory.initializing().is_none());
    ensure!(inventory.history(&genesis_scope)?.unwrap().member() == MemberId::new(3)?);
    // One actual durable voter with no initialized peers needs no live quorum
    // to prove occupancy. It still leaves one genuinely unused profile slot.
    ensure!(observed.view.node.groups == 1);
    let mut occupied = s.request(0, 224, cut.revision()).await?;
    occupied.group = genesis_scope;
    ensure!(matches!(
        s.f.catalog
            .reserve_resources(
                &cut,
                &occupied,
                &s.nodes[0].host,
                std::slice::from_ref(&s.namespace),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Invalid("occupied resource destination"))
    ));
    let warm = s.request(0, 225, cut.revision()).await?;
    let admitted =
        s.f.catalog
            .reserve_resources(
                &cut,
                &warm,
                &s.nodes[0].host,
                std::slice::from_ref(&s.namespace),
                TIMEOUT,
            )
            .await?;
    ensure!(admitted.applied_revision() == cut.revision() + 1);
    let cut = s.f.catalog.read(TIMEOUT).await?;
    s.f.remote(&s.nodes[0].host, NodeId::new([7; 16])?, cut.revision())
        .await?;
    let extra = s.request(1, 226, cut.revision()).await?;
    ensure!(matches!(
        s.f.catalog
            .reserve_resources(
                &cut,
                &extra,
                &s.nodes[0].host,
                std::slice::from_ref(&s.namespace),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Resource(LedgerError::CapacityExceeded(
            ResourceKind::EngineSlots
        )))
    ));
    ensure!(s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_physical_inventory_retained_reply_and_cancellation() -> Result<()> {
    let s = Box::pin(Setup::new(true)).await?;
    let revision = s.f.catalog.read(TIMEOUT).await?.revision();
    let origin = &s.nodes[0].host;
    let target = NodeId::new([7; 16])?;
    ensure!(origin
        .inspect_peer_node(target, revision, TIMEOUT)
        .await?
        .inventory
        .is_none());
    s.f.store.hold_fleet_reads.store(true, Ordering::Release);
    s.f.store.fleet_read_hang.store(true, Ordering::Release);
    s.f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            while s.f.store.active_fleet_reads.load(Ordering::Acquire) < 4 {
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await??;
    let baseline = origin.resource_usage().control.bytes;
    ensure!(baseline == 4 * 1024 * 1024);
    for deliver in [true, false] {
        let pending = origin.inspect_peer_physical_inventory(target, revision, TIMEOUT);
        s.f.domain
            .run(cellar::asyncrt::timeout(TIMEOUT, async {
                loop {
                    let usage = origin.resource_usage();
                    if usage.control.bytes >= baseline + 1024 * 1024
                        && usage.transport.is_some_and(|t| {
                            t.inspection_metadata.items == 0 && t.outgoing_connections.control == 0
                        })
                    {
                        break;
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(5)).await;
                }
            }))
            .await??;
        if deliver {
            s.f.domain
                .run(async { cellar::asyncrt::sleep(Duration::from_millis(100)).await })
                .await?;
            let observed = pending.await?;
            ensure!(observed.sample_age_ms >= 100 && observed.view.node.groups == 0);
            let inventory = observed
                .inventory
                .context("missing complete empty inventory")?;
            ensure!(inventory.histories().is_empty() && inventory.initializing().is_none());
        } else {
            drop(pending);
        }
        s.f.domain
            .run(cellar::asyncrt::timeout(TIMEOUT, async {
                loop {
                    let usage = origin.resource_usage();
                    if usage.control.bytes == baseline
                        && usage
                            .transport
                            .is_some_and(|t| t.inspection_metadata.items == 0)
                    {
                        break;
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(5)).await;
                }
            }))
            .await??;
    }
    ensure!(s.f.calls.load(Ordering::Acquire) == 0);
    s.f.store.hold_fleet_reads.store(false, Ordering::Release);
    s.f.store.fleet_read_hang.store(false, Ordering::Release);
    s.f.store.fleet_read_resume.notify_waiters();
    s.close().await
}
