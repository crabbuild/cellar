// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real five-node repair selection/admission with physical stop and joined work.
//! Isolation/resource attestations are fixture authority, not a platform driver.
use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::{future::Future, pin::Pin};

#[path = "raft_isolated_repair_fairness.rs"]
mod fairness;

struct RepairFleet {
    s: Setup,
    _spare_root: tempfile::TempDir,
    spare_node: cellar::sdk::Node,
    spare: RaftCells,
    _old_root: tempfile::TempDir,
    failed: NodeId,
    failed_boot: FleetBootBinding,
    old_engine: std::path::PathBuf,
    live: Vec<Vec<(NodeId, RaftCellHandle)>>,
}

impl RepairFleet {
    async fn new(extra: bool) -> Result<Self> {
        Box::pin(Self::with_isolation(extra, true)).await
    }

    async fn with_isolation(extra: bool, isolated: bool) -> Result<Self> {
        Box::pin(Self::with_histories(extra, isolated, 3)).await
    }

    async fn with_histories(extra: bool, isolated: bool, histories: u8) -> Result<Self> {
        let mut s = Box::pin(Setup::with_histories(true, 6, histories)).await?;
        let (spare_root, spare_node, spare) = s.f.peer_host(8).await?;
        if extra {
            let mut proposal = s.proposals[0].clone();
            proposal.scope.cell = "Counter:independent-majority".into();
            proposal.scope.group = GroupId::new([188; 16])?;
            proposal.voters = [4, 6, 7]
                .into_iter()
                .enumerate()
                .map(|(i, n)| {
                    Ok(InitialMember {
                        member: MemberId::new(i as u64 + 1)?,
                        node: NodeId::new([n; 16])?,
                    })
                })
                .collect::<Result<_>>()?;
            let cut = s.f.catalog.read(TIMEOUT).await?;
            let pending =
                s.f.catalog
                    .reserve_birth(
                        &cut,
                        &Fixture::request(170, cut.revision()),
                        &s.namespace,
                        proposal.clone(),
                        TIMEOUT,
                    )
                    .await?;
            s.f.domain
                .run(
                    s.namespace
                        .resolve_managed_birth(&pending.authorization, TIMEOUT),
                )
                .await??;
            let cut = s.f.catalog.read(TIMEOUT).await?;
            s.f.catalog
                .confirm_birth(
                    &cut,
                    &Fixture::request(171, cut.revision()),
                    &s.namespace,
                    &pending.authorization,
                    TIMEOUT,
                )
                .await?;
            s.proposals.push(proposal);
        }
        let hosts = s
            .nodes
            .iter()
            .map(|n| &n.host)
            .chain([&s.target, &spare])
            .collect::<Vec<_>>();
        discover(&s.f, &hosts).await?;
        let mut live = Vec::new();
        for proposal in &s.proposals {
            let cut = s.f.catalog.read(TIMEOUT).await?;
            let authorization =
                s.f.catalog
                    .authorize_group(&cut, &proposal.scope, TIMEOUT)
                    .await?;
            let mut cells = Vec::new();
            let mut nodes = Vec::new();
            for member in &proposal.voters {
                let host = if member.node == NodeId::new([7; 16])? {
                    &s.target
                } else {
                    &s.nodes
                        .iter()
                        .find(|n| n.spec.node == member.node)
                        .context("initial repair voter")?
                        .host
                };
                cells.push(
                    host.register_managed_history(authorization.clone(), TIMEOUT)
                        .await?,
                );
                nodes.push(member.node);
            }
            activate(&cells, &authorization).await?;
            write_read(&s.f, &cells, 1, 7, 7).await.with_context(|| {
                format!(
                    "initial SQL for isolated-repair history {}",
                    proposal.scope.cell
                )
            })?;
            live.push(nodes.into_iter().zip(cells).collect::<Vec<_>>());
        }
        for (index, host) in s.nodes.iter().map(|n| &n.host).chain([&spare]).enumerate() {
            let evidence = crate::hosts::capacity_tests::attestation(&s.f, &s.target, host).await?;
            let cut = s.f.catalog.read(TIMEOUT).await?;
            s.f.catalog
                .admit_node_capacity(
                    &cut,
                    &FleetCapacityRequest {
                        identity: Fixture::request(190 + index as u8, cut.revision()),
                        attestation: evidence,
                        action: FleetCapacityAction::Activate,
                    },
                    &s.target,
                    std::slice::from_ref(&s.namespace),
                    TIMEOUT,
                )
                .await?;
        }
        discover(&s.f, &hosts).await?;
        // Exercise a failed leader, rather than only a failed follower.
        let first = live[0].iter().map(|(_, c)| c.clone()).collect::<Vec<_>>();
        let leader = primary(&s.f, &first).await?;
        if leader != 0 {
            let before = first[leader].status().await?;
            first[leader]
                .transfer_leadership(
                    LeadershipTransfer {
                        target: MemberId::new(1)?,
                        expected_term: before.term,
                        expected_revision: before.membership_revision,
                    },
                    TIMEOUT,
                )
                .await?;
        }
        s.f.domain
            .run(cellar::asyncrt::timeout(TIMEOUT, async {
                while !first[0].status().await?.command_ready {
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
                Ok::<_, anyhow::Error>(())
            }))
            .await???;
        let old = s.nodes.remove(0);
        let failed = old.spec.node;
        let boot = old
            .host
            .observe_node(TIMEOUT)
            .await?
            .fleet_boot
            .context("failed leader boot")?;
        let old_engine = old.node.info().data_dir.join("raft");
        joined(old.node).await?;
        if isolated {
            isolate(&s, failed, boot.clone()).await?;
        } else {
            let cut = s.f.catalog.read(TIMEOUT).await?;
            s.f.catalog
                .begin_evacuation(
                    &cut,
                    &Fixture::request(220, cut.revision()),
                    failed,
                    std::slice::from_ref(&s.namespace),
                    TIMEOUT,
                )
                .await?;
        }
        for group in &mut live {
            group.retain(|(n, _)| *n != failed);
        }
        let hosts = s
            .nodes
            .iter()
            .map(|n| &n.host)
            .chain([&s.target, &spare])
            .collect::<Vec<_>>();
        discover(&s.f, &hosts).await?;
        Ok(Self {
            s,
            _spare_root: spare_root,
            spare_node,
            spare,
            _old_root: old.root,
            failed,
            failed_boot: boot,
            old_engine,
            live,
        })
    }

    async fn evidence(&self) -> Result<Arc<Inputs>> {
        let mut hosts = Vec::new();
        for host in self
            .s
            .nodes
            .iter()
            .map(|n| &n.host)
            .chain([&self.s.target, &self.spare])
        {
            hosts.push((
                host.observe_node(TIMEOUT).await?.node.identity.node,
                host.clone(),
            ));
        }
        Ok(Arc::new(Inputs {
            hosts,
            namespaces: vec![self.s.namespace.clone()],
            hold: AtomicBool::new(false),
            wrong_scope: AtomicBool::new(false),
            active: Arc::new(AtomicUsize::new(0)),
            calls: AtomicUsize::new(0),
        }))
    }

    async fn wait(
        &self,
        service: &FleetMovementService,
        ready: impl Fn(&FleetMovementServiceStatus) -> bool,
    ) -> Result<()> {
        // Three required replacements retain twenty seconds each; the extra
        // minority case gets one additional bounded history round envelope.
        self.s
            .f
            .domain
            .run(cellar::asyncrt::timeout(TIMEOUT * 16, async {
                loop {
                    let status = service.status();
                    if ready(&status) {
                        return Ok::<_, anyhow::Error>(());
                    }
                    ensure!(status.running, "{status:?}");
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
            }))
            .await??
            .with_context(|| format!("isolated repair service status: {:?}", service.status()))?;
        Ok(())
    }

    async fn verify(&mut self, index: usize) -> Result<()> {
        let scope = &self.s.proposals[index].scope;
        let mut cells = self.live[index]
            .iter()
            .map(|(_, c)| c.clone())
            .collect::<Vec<_>>();
        for host in [&self.s.target, &self.spare] {
            let node = host.observe_node(TIMEOUT).await?.node.identity.node;
            if self.live[index].iter().any(|(n, _)| *n == node) {
                continue;
            }
            if let Ok(cell) = host.cell(scope.namespace, &scope.cell) {
                cells.push(cell);
            }
        }
        ensure!(cells.len() == 3);
        let cut = self.s.f.catalog.read(TIMEOUT).await?;
        let authorization = self
            .s
            .f
            .catalog
            .authorize_group(&cut, scope, TIMEOUT)
            .await?;
        for cell in &cells {
            let proof = cell
                .observe_fleet_membership(authorization.clone(), TIMEOUT)
                .await?;
            ensure!(
                proof.configuration().voters().len() == 3
                    && proof.configuration().outgoing_voters().is_empty()
            );
            let member = proof
                .configuration()
                .voters()
                .iter()
                .find(|m| m.get() > 3)
                .context("new member")?;
            ensure!(proof.joined_member(*member)?.is_some());
        }
        write_read(&self.s.f, &cells, 2, 5, 12)
            .await
            .with_context(|| format!("first post-repair SQL for {}", scope.cell))?;
        write_read(&self.s.f, &cells, 3, 9, 21)
            .await
            .with_context(|| format!("second post-repair SQL for {}", scope.cell))?;
        Ok(())
    }

    async fn close(self) -> Result<()> {
        joined(self.spare_node).await?;
        self.s.close().await
    }
}

struct Inputs {
    hosts: Vec<(NodeId, RaftCells)>,
    namespaces: Vec<NamespaceCatalog>,
    hold: AtomicBool,
    wrong_scope: AtomicBool,
    active: Arc<AtomicUsize>,
    calls: AtomicUsize,
}

struct Callback(Arc<AtomicUsize>);

impl Drop for Callback {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn occupancy(
    view: RaftHostObservation,
) -> std::result::Result<FleetResourceAttestation, FleetCatalogError> {
    Ok(FleetResourceAttestation {
        boot: view.fleet_boot.ok_or(FleetCatalogError::Unconfirmed)?,
        profile_revision: 1,
        used: FleetResourceBounds {
            engine_slots: view.node.groups,
            durable_bytes: 1024 * 1024,
            memory_bytes: 64 * 1024 * 1024,
        },
        evidence_digest: [73; 32],
        evidence_reference: "fixture-only/isolated-repair-occupancy".into(),
    })
}

impl FleetIsolatedRepairEvidence for Inputs {
    fn refresh<'a>(
        &'a self,
        context: &'a FleetIsolatedRepairContext,
        timeout: Duration,
    ) -> Pin<
        Box<
            dyn Future<Output = std::result::Result<FleetIsolatedRepairInputs, FleetCatalogError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.active.fetch_add(1, Ordering::AcqRel);
            let _callback = Callback(self.active.clone());
            while self.hold.load(Ordering::Acquire) {
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
            let mut nodes = Vec::new();
            let mut bytes = 0;
            for (_, host) in &self.hosts {
                let view = host
                    .observe_node(timeout)
                    .await
                    .map_err(|_| FleetCatalogError::Unconfirmed)?;
                bytes = bytes.max(view.node.profile.bytes_per_group);
                nodes.push(FleetTopologyNodeResources {
                    attestation: occupancy(view)?,
                    reserve: FleetResourceBounds {
                        engine_slots: 0,
                        durable_bytes: 0,
                        memory_bytes: 8 * 1024 * 1024,
                    },
                });
            }
            let mut group = context.group.clone();
            if self.wrong_scope.load(Ordering::Acquire) {
                group.cell.push_str("-wrong");
            }
            Ok(FleetIsolatedRepairInputs {
                history: FleetTopologyHistoryResources {
                    group,
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
                },
                nodes,
                namespaces: self.namespaces.clone(),
            })
        })
    }
}

impl FleetMovementEvidence for Inputs {
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
            let host = &self
                .hosts
                .iter()
                .find(|(n, _)| *n == movement.plan.replacement.node)
                .ok_or(FleetCatalogError::InventoryIncomplete)?
                .1;
            Ok(FleetMovementExecutionRequest {
                reservation: movement.reservation,
                attestation: occupancy(
                    host.observe_node(timeout)
                        .await
                        .map_err(|_| FleetCatalogError::Unconfirmed)?,
                )?,
                from_s3: true,
            })
        })
    }
}

fn options() -> FleetMovementServiceOptions {
    FleetMovementServiceOptions {
        interval: Duration::from_millis(50),
        retry_max_interval: Duration::from_millis(500),
        turn_timeout: TIMEOUT,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_isolated_repair_service_selects_and_repairs_without_business_traffic() -> Result<()>
{
    Box::pin(automatic()).await
}

async fn automatic() -> Result<()> {
    let mut fleet = Box::pin(RepairFleet::new(false)).await?;
    let evidence = fleet.evidence().await?;
    let root = fleet.s.f.catalog.read(TIMEOUT).await?;
    evidence.hold.store(true, Ordering::Release);
    let service = fleet
        .s
        .f
        .catalog
        .start_isolated_repair_service(
            fleet.spare.clone(),
            evidence.clone(),
            evidence.clone(),
            ProposalId::new([100; 16])?,
            options(),
        )
        .await?;
    fleet
        .s
        .f
        .domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            while evidence.active.load(Ordering::Acquire) == 0 {
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
        }))
        .await??;
    ensure!(matches!(
        fleet
            .s
            .f
            .catalog
            .start_movement_service(fleet.spare.clone(), evidence.clone(), options())
            .await,
        Err(FleetCatalogError::Overloaded)
    ));
    service.shutdown().await;
    ensure!(evidence.active.load(Ordering::Acquire) == 0);
    ensure!(fleet.s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    evidence.hold.store(false, Ordering::Release);
    evidence.wrong_scope.store(true, Ordering::Release);
    let starter = ExecutionDomain::new(tokio::runtime::Handle::current());
    let caller = Fixture::open_catalog(&starter, fleet.s.f.store.clone())?;
    let service = caller
        .start_isolated_repair_service(
            fleet.spare.clone(),
            evidence.clone(),
            evidence.clone(),
            ProposalId::new([100; 16])?,
            options(),
        )
        .await?;
    starter.shutdown().await;
    fleet
        .wait(&service, |s| {
            s.isolated_repair.as_ref().is_some_and(|r| {
                r.last_blocked_failure == Some(FleetMovementServiceFailure::Rejected)
            })
        })
        .await?;
    ensure!(fleet.s.f.catalog.read(TIMEOUT).await?.revision() == root.revision());
    ensure!(
        fleet.s.target.observe_node(TIMEOUT).await?.node.groups == 0
            && fleet.spare.observe_node(TIMEOUT).await?.node.groups == 0
    );
    evidence.wrong_scope.store(false, Ordering::Release);
    fleet
        .s
        .f
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    fleet
        .wait(&service, |s| {
            s.settled_movements == 3
                && s.work.is_empty()
                && s.isolated_repair.as_ref().is_some_and(|r| {
                    r.inventory_exhausted && r.histories_checked == 3 && r.blocked_histories == 0
                })
        })
        .await?;
    ensure!(!fleet
        .s
        .f
        .store
        .fail_fleet_confirmation
        .load(Ordering::Acquire));
    service.shutdown().await;
    ensure!(evidence.active.load(Ordering::Acquire) == 0);
    let cut = fleet.s.f.catalog.read(TIMEOUT).await?;
    ensure!(fleet
        .s
        .f
        .catalog
        .pending_movements(&cut, TIMEOUT)
        .await?
        .is_empty());
    for i in 0..3 {
        fleet.verify(i).await?;
    }
    retained_engine(&fleet.s.f, fleet.old_engine.clone()).await?;
    let old = fleet
        .s
        .f
        .catalog
        .get(
            &cut,
            FleetTable::Nodes,
            &hex::encode(fleet.failed.bytes()),
            TIMEOUT,
        )
        .await?
        .context("retained isolated node")?;
    let old: serde_json::Value = serde_json::from_slice(&old[13..old.len() - 32])?;
    ensure!(old["state"] == "Draining" && !old["isolation"].is_null());
    fleet.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_isolated_repair_service_skips_minority_and_repairs_independent_majority(
) -> Result<()> {
    Box::pin(minority()).await
}

async fn minority() -> Result<()> {
    let mut fleet = Box::pin(RepairFleet::new(true)).await?;
    let second = fleet.s.nodes.remove(0);
    ensure!(second.spec.node == NodeId::new([5; 16])?);
    let second_engine = second.node.info().data_dir.join("raft");
    joined(second.node).await?;
    let evidence = fleet.evidence().await?;
    let hosts = fleet
        .s
        .nodes
        .iter()
        .map(|n| &n.host)
        .chain([&fleet.s.target, &fleet.spare])
        .collect::<Vec<_>>();
    discover(&fleet.s.f, &hosts).await?;
    let service = fleet
        .s
        .f
        .catalog
        .start_isolated_repair_service(
            fleet.spare.clone(),
            evidence.clone(),
            evidence.clone(),
            ProposalId::new([100; 16])?,
            options(),
        )
        .await?;
    fleet
        .wait(&service, |s| {
            s.settled_movements == 1
                && s.work.is_empty()
                && s.isolated_repair.as_ref().is_some_and(|r| {
                    r.inventory_exhausted && r.blocked_histories == 3 && r.histories_checked == 4
                })
        })
        .await?;
    service.shutdown().await;
    let cut = fleet.s.f.catalog.read(TIMEOUT).await?;
    for proposal in &fleet.s.proposals[..3] {
        ensure!(fleet
            .s
            .f
            .catalog
            .get(
                &cut,
                FleetTable::Admissions,
                &fleet.s.f.catalog.group_binding(&proposal.scope)?.key()?,
                TIMEOUT
            )
            .await?
            .is_none());
    }
    fleet.verify(3).await?;
    retained_engine(&fleet.s.f, second_engine).await?;
    fleet.close().await
}

async fn retained_engine(f: &Fixture, path: std::path::PathBuf) -> Result<()> {
    let retained = f
        .domain
        .run(async move {
            cellar::asyncrt::blocking(move || {
                let fs = cellar::asyncrt::fs();
                if !fs.metadata(&path)?.is_dir {
                    return Ok::<_, std::io::Error>(false);
                }
                let marker = fs.metadata(&path.join("node.json"))?;
                if !marker.is_file || marker.len == 0 {
                    return Ok(false);
                }
                for entry in fs.read_dir(&path.join("log"))? {
                    if !entry.is_dir
                        && matches!(
                            entry.path.extension().and_then(|s| s.to_str()),
                            Some("raftlog" | "rewrite")
                        )
                        && fs.metadata(&entry.path)?.len > 0
                    {
                        return Ok(true);
                    }
                }
                Ok(false)
            })
            .await
        })
        .await???;
    ensure!(retained, "stopped engine lost its retained shared-log data");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_isolated_repair_service_waits_for_trusted_isolation() -> Result<()> {
    Box::pin(before_isolation()).await
}

async fn before_isolation() -> Result<()> {
    let mut fleet = Box::pin(RepairFleet::with_isolation(false, false)).await?;
    let evidence = fleet.evidence().await?;
    let root = fleet.s.f.catalog.read(TIMEOUT).await?;
    let service = fleet
        .s
        .f
        .catalog
        .start_isolated_repair_service(
            fleet.spare.clone(),
            evidence.clone(),
            evidence.clone(),
            ProposalId::new([100; 16])?,
            options(),
        )
        .await?;
    fleet
        .wait(&service, |s| {
            s.last_round_failure == Some(FleetMovementServiceFailure::Conflict)
        })
        .await?;
    ensure!(evidence.calls.load(Ordering::Acquire) == 0);
    let cut = fleet.s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        cut.revision() == root.revision()
            && fleet
                .s
                .f
                .catalog
                .pending_movements(&cut, TIMEOUT)
                .await?
                .is_empty()
    );
    ensure!(
        fleet.s.target.observe_node(TIMEOUT).await?.node.groups == 0
            && fleet.spare.observe_node(TIMEOUT).await?.node.groups == 0
    );
    fleet
        .s
        .f
        .catalog
        .attest_node_isolation(
            &cut,
            &FleetNodeIsolationRequest {
                identity: Fixture::request(221, cut.revision()),
                attestation: FleetIsolationAttestation {
                    boot: fleet.failed_boot.clone(),
                    evidence_digest: [92; 32],
                    evidence_reference: "fixture/joined-process-and-prevented-restart".into(),
                },
            },
            TIMEOUT,
        )
        .await?;
    fleet
        .wait(&service, |s| {
            s.settled_movements == 3
                && s.work.is_empty()
                && s.isolated_repair
                    .as_ref()
                    .is_some_and(|r| r.inventory_exhausted && r.blocked_histories == 0)
        })
        .await?;
    service.shutdown().await;
    ensure!(
        evidence.calls.load(Ordering::Acquire) >= 3 && evidence.active.load(Ordering::Acquire) == 0
    );
    for i in 0..3 {
        fleet.verify(i).await?;
    }
    retained_engine(&fleet.s.f, fleet.old_engine.clone()).await?;
    fleet.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_isolated_repair_service_excludes_incompatible_spare() -> Result<()> {
    Box::pin(incompatible()).await
}

async fn incompatible() -> Result<()> {
    let mut fleet = Box::pin(RepairFleet::new(false)).await?;
    let f = &fleet.s.f;
    let mut declaration = f.declaration.clone();
    declaration.namespace = f.catalog.bind_namespace(NamespaceConfig::new(
        f.declaration.namespace.cluster,
        NamespaceId::new([93; 16])?,
        NamespaceMode::Raft,
        f.definition.schema,
    ))?;
    let namespace = Catalog::new(
        f.store.clone(),
        CatalogOptions {
            prefix: "host-enrollment/".into(),
            ..Default::default()
        },
    )?
    .open_namespace(declaration.namespace.clone())
    .await?;
    let cut = f.catalog.read(TIMEOUT).await?;
    f.catalog
        .declare_namespace(
            &cut,
            &Fixture::request(197, cut.revision()),
            &namespace,
            "Counter",
            TIMEOUT,
        )
        .await?;
    let mut spec = f.peer_spec(9)?;
    spec.capability_digest =
        RaftManagedHostOptions::capability_digest(std::slice::from_ref(&declaration))?;
    let cut = f.catalog.read(TIMEOUT).await?;
    let registered = f
        .catalog
        .register_node(&cut, &Fixture::request(59, cut.revision()), &spec, TIMEOUT)
        .await?;
    let authorization = f
        .catalog
        .authorize_host(&registered.snapshot, spec.node, TIMEOUT)
        .await?;
    let directory = tempfile::tempdir()?;
    let node = public_node::node_named(
        directory.path(),
        f.definition.application.clone(),
        &spec.label,
    )
    .await?;
    let (mut config, services) = f.config_for(&node, authorization, &spec).await?;
    config
        .host
        .managed
        .as_mut()
        .context("managed incompatible spare")?
        .namespaces = vec![declaration];
    config.host.max_groups = 64;
    let bytes = config.host.images.max_bytes;
    let profile = config.host.node_profile.as_mut().context("spare profile")?;
    profile.group_capacity = 64;
    profile.leader_capacity = 64;
    profile.byte_capacity = bytes.checked_mul(64).context("spare byte capacity")?;
    let spare = node.start_raft_cells(config, services).await?;
    let attestation = crate::hosts::capacity_tests::attestation(f, &fleet.s.target, &spare).await?;
    let cut = f.catalog.read(TIMEOUT).await?;
    let namespaces = vec![fleet.s.namespace.clone(), namespace.clone()];
    f.catalog
        .admit_node_capacity(
            &cut,
            &FleetCapacityRequest {
                identity: Fixture::request(198, cut.revision()),
                attestation,
                action: FleetCapacityAction::Activate,
            },
            &fleet.s.target,
            &namespaces,
            TIMEOUT,
        )
        .await?;
    let mut evidence = fleet.evidence().await?;
    let input = Arc::get_mut(&mut evidence).context("unshared repair evidence")?;
    input.hosts.push((spec.node, spare.clone()));
    input.namespaces.push(namespace);
    let hosts = fleet
        .s
        .nodes
        .iter()
        .map(|n| &n.host)
        .chain([&fleet.s.target, &fleet.spare, &spare])
        .collect::<Vec<_>>();
    discover(f, &hosts).await?;
    // This spare has eight times the slot capacity of ordinary peers. Its
    // different declared namespace must exclude it before any resource debit.
    let service = f
        .catalog
        .start_isolated_repair_service(
            fleet.s.target.clone(),
            evidence.clone(),
            evidence.clone(),
            ProposalId::new([100; 16])?,
            options(),
        )
        .await?;
    fleet
        .wait(&service, |s| {
            s.settled_movements == 3
                && s.work.is_empty()
                && s.isolated_repair
                    .as_ref()
                    .is_some_and(|r| r.inventory_exhausted && r.blocked_histories == 0)
        })
        .await?;
    service.shutdown().await;
    let usage = spare.observe_node(TIMEOUT).await?;
    ensure!(usage.node.groups == 0 && usage.registered_histories == 0);
    for i in 0..3 {
        fleet.verify(i).await?;
    }
    joined(node).await?;
    fleet.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_isolated_repair_service_excludes_metadata_full_spare() -> Result<()> {
    Box::pin(metadata_full()).await
}

async fn metadata_full() -> Result<()> {
    let mut fleet = Box::pin(RepairFleet::new(false)).await?;
    let f = &fleet.s.f;
    let before = fleet.s.target.observe_node(TIMEOUT).await?;
    ensure!(before.node.groups == 0 && before.registered_histories == 0);
    ensure!(before.startup_bounds.host_groups == 8);
    // Fill the registration table with unrelated, healthy birth metadata.
    // None has a physical engine, and deployment occupancy deliberately reports
    // only engines. The authenticated runtime must prevent a stranded debit.
    for index in 0_u8..8 {
        let mut proposal = fleet.s.proposals[0].clone();
        proposal.scope.cell = format!("Counter:metadata-only-{index}");
        proposal.scope.group = GroupId::new([210 + index; 16])?;
        proposal.voters = [5, 6, 7]
            .into_iter()
            .enumerate()
            .map(|(i, n)| {
                Ok(InitialMember {
                    member: MemberId::new(i as u64 + 1)?,
                    node: NodeId::new([n; 16])?,
                })
            })
            .collect::<Result<_>>()?;
        let cut = f.catalog.read(TIMEOUT).await?;
        let pending = f
            .catalog
            .reserve_birth(
                &cut,
                &Fixture::request(232 + index * 2, cut.revision()),
                &fleet.s.namespace,
                proposal.clone(),
                TIMEOUT,
            )
            .await?;
        f.domain
            .run(
                fleet
                    .s
                    .namespace
                    .resolve_managed_birth(&pending.authorization, TIMEOUT),
            )
            .await??;
        let cut = f.catalog.read(TIMEOUT).await?;
        f.catalog
            .confirm_birth(
                &cut,
                &Fixture::request(233 + index * 2, cut.revision()),
                &fleet.s.namespace,
                &pending.authorization,
                TIMEOUT,
            )
            .await?;
        let cut = f.catalog.read(TIMEOUT).await?;
        let authorization = f
            .catalog
            .authorize_group(&cut, &proposal.scope, TIMEOUT)
            .await?;
        fleet
            .s
            .target
            .register_managed_history(authorization, TIMEOUT)
            .await?;
    }
    let full = fleet.s.target.observe_node(TIMEOUT).await?;
    ensure!(full.node.groups == 0 && full.registered_histories == 8);
    let evidence = fleet.evidence().await?;
    let service = f
        .catalog
        .start_isolated_repair_service(
            fleet.s.target.clone(),
            evidence.clone(),
            evidence.clone(),
            ProposalId::new([100; 16])?,
            options(),
        )
        .await?;
    fleet
        .wait(&service, |s| {
            s.settled_movements == 3
                && s.work.is_empty()
                && s.isolated_repair.as_ref().is_some_and(|r| {
                    r.inventory_exhausted && r.blocked_histories == 0 && r.histories_checked == 11
                })
        })
        .await?;
    service.shutdown().await;
    let usage = fleet.s.target.observe_node(TIMEOUT).await?;
    ensure!(usage.node.groups == 0 && usage.registered_histories == 8);
    ensure!(fleet.spare.observe_node(TIMEOUT).await?.node.groups == 3);
    for i in 0..3 {
        fleet.verify(i).await?;
    }
    fleet.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_isolated_repair_service_fences_delayed_admission_before_replanning() -> Result<()> {
    Box::pin(delayed_admission()).await
}

async fn delayed_admission() -> Result<()> {
    let mut fleet = Box::pin(RepairFleet::new(false)).await?;
    let first_evidence = fleet.evidence().await?;
    let second_evidence = fleet.evidence().await?;
    let root = fleet.s.f.catalog.read(TIMEOUT).await?;
    fleet
        .s
        .f
        .store
        .fleet_cas_hang
        .store(true, Ordering::Release);
    fleet
        .s
        .f
        .store
        .hold_next_fleet_cas
        .store(true, Ordering::Release);
    let first = fleet
        .s
        .f
        .catalog
        .start_isolated_repair_service(
            fleet.s.target.clone(),
            first_evidence.clone(),
            first_evidence.clone(),
            ProposalId::new([100; 16])?,
            options(),
        )
        .await?;
    fleet.s.waiting().await?;
    first_evidence.hold.store(true, Ordering::Release);
    second_evidence.hold.store(true, Ordering::Release);
    let mut quick = options();
    quick.turn_timeout = Duration::from_secs(1);
    let second = fleet
        .s
        .f
        .catalog
        .start_isolated_repair_service(
            fleet.spare.clone(),
            second_evidence.clone(),
            second_evidence.clone(),
            ProposalId::new([100; 16])?,
            quick,
        )
        .await?;
    fleet
        .wait(&second, |_| {
            second_evidence.active.load(Ordering::Acquire) > 0
        })
        .await?;
    let cut = fleet.s.f.catalog.read(TIMEOUT).await?;
    ensure!(cut.revision() == root.revision() + 1);
    let fence = fleet
        .s
        .f
        .catalog
        .get(
            &cut,
            FleetTable::Operations,
            &format!("isolated-repair-fence/{}", hex::encode([100; 16])),
            TIMEOUT,
        )
        .await?
        .context("repair candidate fence")?;
    let fence: serde_json::Value = serde_json::from_slice(&fence)?;
    ensure!(
        fence[0] == 1
            && fence[1] == root.revision()
            && fence[2].as_array().is_some_and(|a| a.len() == 16)
    );
    ensure!(fleet
        .s
        .f
        .catalog
        .pending_movements(&cut, TIMEOUT)
        .await?
        .is_empty());
    ensure!(
        fleet.s.target.observe_node(TIMEOUT).await?.node.groups == 0
            && fleet.spare.observe_node(TIMEOUT).await?.node.groups == 0
    );
    fleet.s.release();
    fleet
        .wait(&first, |s| {
            s.isolated_repair.as_ref().is_some_and(|r| {
                r.last_blocked_failure == Some(FleetMovementServiceFailure::Conflict)
            }) && first_evidence.active.load(Ordering::Acquire) > 0
        })
        .await?;
    first.shutdown().await;
    second.shutdown().await;
    ensure!(
        first_evidence.active.load(Ordering::Acquire) == 0
            && second_evidence.active.load(Ordering::Acquire) == 0
    );
    let cut = fleet.s.f.catalog.read(TIMEOUT).await?;
    ensure!(
        cut.revision() == root.revision() + 1
            && fleet
                .s
                .f
                .catalog
                .pending_movements(&cut, TIMEOUT)
                .await?
                .is_empty()
    );
    second_evidence.hold.store(false, Ordering::Release);
    let service = fleet
        .s
        .f
        .catalog
        .start_isolated_repair_service(
            fleet.spare.clone(),
            second_evidence.clone(),
            second_evidence.clone(),
            ProposalId::new([100; 16])?,
            options(),
        )
        .await?;
    fleet
        .wait(&service, |s| {
            s.settled_movements == 3
                && s.work.is_empty()
                && s.isolated_repair.as_ref().is_some_and(|r| {
                    r.inventory_exhausted && r.histories_checked == 3 && r.blocked_histories == 0
                })
        })
        .await?;
    service.shutdown().await;
    for i in 0..3 {
        fleet.verify(i).await?;
    }
    retained_engine(&fleet.s.f, fleet.old_engine.clone()).await?;
    fleet.close().await
}
