// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Owned generation/admission/execution and ambiguous-root decision recovery.
use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

#[path = "raft_operator_native.rs"]
mod operator_cases;

struct Inputs {
    hosts: Vec<(NodeId, RaftCells)>,
    namespace: NamespaceCatalog,
    histories: Vec<GroupScope>,
    hold: AtomicBool,
    wrong_keep: AtomicBool,
    calls: AtomicUsize,
    active: Arc<AtomicUsize>,
    cancelled: Arc<AtomicUsize>,
}

struct Callback {
    active: Arc<AtomicUsize>,
    cancelled: Arc<AtomicUsize>,
    finished: bool,
}

impl Drop for Callback {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
        if !self.finished {
            self.cancelled.fetch_add(1, Ordering::AcqRel);
        }
    }
}

impl Inputs {
    async fn new(f: &Scenario) -> Result<Self> {
        let mut hosts = Vec::new();
        for host in f.hosts() {
            hosts.push((
                host.observe_node(TIMEOUT).await?.node.identity.node,
                host.clone(),
            ));
        }
        Ok(Self {
            hosts,
            namespace: f.s.namespace.clone(),
            histories: f.s.proposals.iter().map(|p| p.scope.clone()).collect(),
            hold: AtomicBool::new(false),
            wrong_keep: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
            active: Arc::new(AtomicUsize::new(0)),
            cancelled: Arc::new(AtomicUsize::new(0)),
        })
    }

    fn attestation(
        sample: RaftHostObservation,
    ) -> std::result::Result<FleetResourceAttestation, FleetCatalogError> {
        Ok(FleetResourceAttestation {
            boot: sample.fleet_boot.ok_or(FleetCatalogError::Unconfirmed)?,
            profile_revision: 1,
            used: FleetResourceBounds {
                engine_slots: sample.node.groups,
                durable_bytes: 1024 * 1024,
                memory_bytes: 64 * 1024 * 1024,
            },
            evidence_digest: [72; 32],
            evidence_reference: "fixture-only/owned-topology-occupancy".into(),
        })
    }
}

impl FleetTopologyEvidence for Inputs {
    fn refresh<'a>(
        &'a self,
        operation: &'a FleetTopologyOperation,
        timeout: Duration,
    ) -> Pin<
        Box<
            dyn Future<Output = std::result::Result<FleetTopologyServiceInputs, FleetCatalogError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.active.fetch_add(1, Ordering::AcqRel);
            let mut callback = Callback {
                active: self.active.clone(),
                cancelled: self.cancelled.clone(),
                finished: false,
            };
            let result = async {
                while self.hold.load(Ordering::Acquire) {
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
                let mut nodes = Vec::new();
                let mut bytes = 0;
                for (_, host) in &self.hosts {
                    let sample = host
                        .observe_node(timeout)
                        .await
                        .map_err(|_| FleetCatalogError::Unconfirmed)?;
                    bytes = bytes.max(sample.node.profile.bytes_per_group);
                    nodes.push(FleetTopologyNodeResources {
                        attestation: Self::attestation(sample)?,
                        reserve: FleetResourceBounds::default(),
                    });
                }
                let mut keep = operation.keep.clone();
                if self.wrong_keep.load(Ordering::Acquire) {
                    keep.pop();
                }
                Ok(FleetTopologyServiceInputs {
                    request: FleetTopologyRequest {
                        keep,
                        policy: operation.policy.clone(),
                        nodes,
                        histories: self
                            .histories
                            .iter()
                            .map(|group| {
                                Ok(FleetTopologyHistoryResources {
                                    group: group.clone(),
                                    resident: FleetResourceBounds {
                                        engine_slots: 1,
                                        durable_bytes: bytes,
                                        memory_bytes: 8 * 1024 * 1024,
                                    },
                                    peak: FleetResourceBounds {
                                        engine_slots: 1,
                                        durable_bytes: bytes
                                            .checked_mul(2)
                                            .ok_or(FleetCatalogError::Limit)?,
                                        memory_bytes: 16 * 1024 * 1024,
                                    },
                                })
                            })
                            .collect::<std::result::Result<Vec<_>, FleetCatalogError>>()?,
                    },
                    namespaces: vec![self.namespace.clone()],
                })
            }
            .await;
            callback.finished = true;
            result
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
            let target = self
                .hosts
                .iter()
                .find(|(node, _)| *node == movement.plan.replacement.node)
                .ok_or(FleetCatalogError::Invalid("fixture target"))?;
            let sample = target
                .1
                .observe_node(timeout)
                .await
                .map_err(|_| FleetCatalogError::Unconfirmed)?;
            Ok(FleetMovementExecutionRequest {
                reservation: movement.reservation,
                attestation: Self::attestation(sample)?,
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

async fn wait(f: &Scenario, mut ready: impl FnMut() -> bool) -> Result<()> {
    // Two serial replacements each retain the existing twenty-second fixture
    // envelope. Per-turn, quorum and physical proof deadlines are unchanged.
    f.s.f
        .domain
        .run(cellar::asyncrt::timeout(TIMEOUT * 8, async {
            while !ready() {
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
        }))
        .await??;
    Ok(())
}

async fn completed_sql(f: &mut Scenario, operation: u8) -> Result<()> {
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let status =
        f.s.f
            .catalog
            .topology_operation(&cut, ProposalId::new([operation; 16])?, TIMEOUT)
            .await?;
    ensure!(status.phase == FleetTopologyPhase::Complete && status.admitted_movements == 2);
    let mut entries =
        f.s.f
            .catalog
            .topology_movements(&cut, ProposalId::new([operation; 16])?, None, 128, TIMEOUT)
            .await?
            .entries;
    entries.sort_by_key(|e| e.admitted_revision);
    for entry in entries {
        let target = if entry.target == NodeId::new([7; 16])? {
            f.s.target.clone()
        } else {
            ensure!(entry.target == NodeId::new([8; 16])?);
            f.spare.clone()
        };
        let index =
            f.s.proposals
                .iter()
                .position(|p| p.scope == entry.group)
                .unwrap();
        let source = f.live[index]
            .iter_mut()
            .find(|(node, _)| *node == entry.source)
            .unwrap();
        *source = (
            entry.target,
            target.cell(entry.group.namespace, &entry.group.cell)?,
        );
    }
    for live in &f.live {
        let cells = live.iter().map(|(_, h)| h.clone()).collect::<Vec<_>>();
        write_read(&f.s.f, &cells, 2, 5, 12).await?;
        write_read(&f.s.f, &cells, 3, 9, 21).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_topology_service_converges_after_starter_close_and_lost_admission() -> Result<()> {
    Box::pin(automatic()).await
}

async fn automatic() -> Result<()> {
    let mut f = Box::pin(Scenario::new(true)).await?;
    let intent = desired(&f, 181, None).await?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    f.s.f
        .catalog
        .begin_topology(&cut, &intent, std::slice::from_ref(&f.s.namespace), TIMEOUT)
        .await?;
    let evidence = Arc::new(Inputs::new(&f).await?);
    let starter = ExecutionDomain::new(tokio::runtime::Handle::current());
    let caller = Fixture::open_catalog(&starter, f.s.f.store.clone())?;
    f.s.f
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let service = caller
        .start_topology_service(
            f.s.nodes[0].host.clone(),
            evidence.clone(),
            evidence.clone(),
            options(),
        )
        .await?;
    starter.shutdown().await;
    ensure!(matches!(
        f.s.f
            .catalog
            .start_movement_service(f.s.nodes[0].host.clone(), evidence.clone(), options())
            .await,
        Err(FleetCatalogError::Overloaded)
    ));
    wait(&f, || {
        service
            .status()
            .topology
            .as_ref()
            .is_some_and(|s| s.phase == FleetTopologyPhase::Complete)
    })
    .await?;
    ensure!(service.status().settled_movements == 2 && evidence.calls.load(Ordering::Acquire) >= 6);
    ensure!(!f.s.f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    service.shutdown().await;
    completed_sql(&mut f, 181).await?;
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_topology_service_supersedes_delayed_cas_before_replanning() -> Result<()> {
    Box::pin(superseded()).await
}

async fn superseded() -> Result<()> {
    let mut f = Box::pin(Scenario::new(true)).await?;
    let intent = desired(&f, 182, None).await?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    f.s.f
        .catalog
        .begin_topology(&cut, &intent, std::slice::from_ref(&f.s.namespace), TIMEOUT)
        .await?;
    let first_inputs = Arc::new(Inputs::new(&f).await?);
    let second_inputs = Arc::new(Inputs::new(&f).await?);
    f.s.f.store.fleet_cas_hang.store(true, Ordering::Release);
    f.s.f
        .store
        .hold_next_fleet_cas
        .store(true, Ordering::Release);
    let first =
        f.s.f
            .catalog
            .start_topology_service(
                f.s.nodes[0].host.clone(),
                first_inputs.clone(),
                first_inputs.clone(),
                options(),
            )
            .await?;
    f.s.waiting().await?;
    first_inputs.hold.store(true, Ordering::Release);
    second_inputs.hold.store(true, Ordering::Release);
    let mut quicker = options();
    quicker.turn_timeout = Duration::from_secs(1);
    let second =
        f.s.f
            .catalog
            .start_topology_service(
                f.s.nodes[1].host.clone(),
                second_inputs.clone(),
                second_inputs.clone(),
                quicker,
            )
            .await?;
    wait(&f, || {
        second
            .status()
            .topology
            .as_ref()
            .is_some_and(|s| s.last_superseded_admission.is_some())
            && second_inputs.active.load(Ordering::Acquire) == 1
    })
    .await?;
    f.s.release();
    wait(&f, || {
        first_inputs.calls.load(Ordering::Acquire) >= 3
            && first_inputs.active.load(Ordering::Acquire) == 1
    })
    .await?;
    ensure!(first.status().last_round_failure == Some(FleetMovementServiceFailure::Conflict));
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    let status =
        f.s.f
            .catalog
            .topology_operation(&cut, ProposalId::new([182; 16])?, TIMEOUT)
            .await?;
    ensure!(status.admitted_movements == 0 && status.last_superseded_admission.is_some());
    ensure!(f
        .s
        .f
        .catalog
        .pending_movements(&cut, TIMEOUT)
        .await?
        .is_empty());
    ensure!(
        f.s.target.observe_node(TIMEOUT).await?.node.groups == 0
            && f.spare.observe_node(TIMEOUT).await?.node.groups == 0
    );
    first.shutdown().await;
    ensure!(
        first_inputs.active.load(Ordering::Acquire) == 0
            && first_inputs.cancelled.load(Ordering::Acquire) >= 1
    );
    second_inputs.hold.store(false, Ordering::Release);
    wait(&f, || {
        second
            .status()
            .topology
            .as_ref()
            .is_some_and(|s| s.phase == FleetTopologyPhase::Complete)
    })
    .await?;
    second.shutdown().await;
    completed_sql(&mut f, 182).await?;
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_topology_service_node_shutdown_joins_collection_callback() -> Result<()> {
    Box::pin(shutdown()).await
}

async fn shutdown() -> Result<()> {
    let f = Box::pin(Scenario::new(true)).await?;
    let intent = desired(&f, 183, None).await?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    f.s.f
        .catalog
        .begin_topology(&cut, &intent, std::slice::from_ref(&f.s.namespace), TIMEOUT)
        .await?;
    let evidence = Arc::new(Inputs::new(&f).await?);
    evidence.hold.store(true, Ordering::Release);
    let service =
        f.s.f
            .catalog
            .start_topology_service(
                f.spare.clone(),
                evidence.clone(),
                evidence.clone(),
                options(),
            )
            .await?;
    wait(&f, || evidence.active.load(Ordering::Acquire) == 1).await?;
    joined(f.spare_node).await?;
    ensure!(
        evidence.active.load(Ordering::Acquire) == 0
            && evidence.cancelled.load(Ordering::Acquire) == 1
            && !service.status().running
    );
    service.shutdown().await;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    ensure!(f
        .s
        .f
        .catalog
        .pending_movements(&cut, TIMEOUT)
        .await?
        .is_empty());
    ensure!(
        f.s.f
            .catalog
            .topology_operation(&cut, ProposalId::new([183; 16])?, TIMEOUT)
            .await?
            .phase
            == FleetTopologyPhase::Running
    );
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
    f.s.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_topology_service_rejects_changed_intent_then_recovers() -> Result<()> {
    Box::pin(rejected()).await
}

async fn rejected() -> Result<()> {
    let mut f = Box::pin(Scenario::new(true)).await?;
    let intent = desired(&f, 184, None).await?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    f.s.f
        .catalog
        .begin_topology(&cut, &intent, std::slice::from_ref(&f.s.namespace), TIMEOUT)
        .await?;
    let revision = f.s.f.catalog.read(TIMEOUT).await?.revision();
    let evidence = Arc::new(Inputs::new(&f).await?);
    evidence.wrong_keep.store(true, Ordering::Release);
    let service =
        f.s.f
            .catalog
            .start_topology_service(
                f.s.nodes[0].host.clone(),
                evidence.clone(),
                evidence.clone(),
                options(),
            )
            .await?;
    wait(&f, || {
        service.status().last_round_failure == Some(FleetMovementServiceFailure::Rejected)
    })
    .await?;
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == revision);
    ensure!(
        f.s.target.observe_node(TIMEOUT).await?.node.groups == 0
            && f.spare.observe_node(TIMEOUT).await?.node.groups == 0
    );
    evidence.wrong_keep.store(false, Ordering::Release);
    wait(&f, || {
        service
            .status()
            .topology
            .as_ref()
            .is_some_and(|s| s.phase == FleetTopologyPhase::Complete)
    })
    .await?;
    service.shutdown().await;
    completed_sql(&mut f, 184).await?;
    f.close().await
}
