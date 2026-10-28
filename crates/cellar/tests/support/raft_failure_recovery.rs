// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Automatic admission and recovery against real RF3 histories. This fixture
//! has physically joined the failed host; its isolation evidence is not a
//! production deployment driver.
use super::*;
use cellar::application::consensus::verification::FleetEvacuationCancelRequest;
use sha2::{Digest, Sha256};
use std::sync::Mutex;

// A full movement book must not prevent isolation needed by admitted work.

struct BlockedMovements(AtomicUsize);

impl FleetMovementEvidence for BlockedMovements {
    fn refresh<'a>(
        &'a self,
        _: &'a FleetPendingMovement,
        _: Duration,
    ) -> Pin<
        Box<
            dyn Future<
                    Output = std::result::Result<FleetMovementExecutionRequest, FleetCatalogError>,
                > + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            self.0.fetch_add(1, Ordering::AcqRel);
            Err(FleetCatalogError::Unconfirmed)
        })
    }
}

async fn fill_book(f: &RepairFleet) -> Result<()> {
    for (index, target) in [&f.s.target, &f.spare].into_iter().enumerate() {
        let cells = f.live[index]
            .iter()
            .map(|(_, c)| c.clone())
            .collect::<Vec<_>>();
        let leader = primary(&f.s.f, &cells).await?;
        if leader != index {
            let before = cells[leader].status().await?;
            cells[leader]
                .transfer_leadership(
                    LeadershipTransfer {
                        target: MemberId::new(index as u64 + 2)?,
                        expected_term: before.term,
                        expected_revision: before.membership_revision,
                    },
                    TIMEOUT,
                )
                .await?;
        }
        f.s.f
            .domain
            .run(cellar::asyncrt::timeout(TIMEOUT, async {
                while !cells[index].status().await?.command_ready {
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
                Ok::<_, anyhow::Error>(())
            }))
            .await???;
        let cut = f.s.f.catalog.read(TIMEOUT).await?;
        let view = target.observe_node(TIMEOUT).await?;
        let mut resources =
            f.s.request(index, 240 + index as u8 * 2, cut.revision())
                .await?;
        resources.peak.durable_bytes = view.node.profile.bytes_per_group * 2;
        resources.peak.memory_bytes = 16 * 1024 * 1024;
        let target_node = view.node.identity.node;
        let target_disk = view.node.identity.disk;
        resources.attestation = occupancy(view)?;
        let request = FleetMovementRequest {
            resources,
            source: f.live[index][index].0,
            plan: RepairPlan {
                id: ProposalId::new([241 + index as u8 * 2; 16])?,
                policy_revision: 1,
                expected_revision: cells[index].status().await?.membership_revision,
                obsolete: MemberId::new(1)?,
                replacement: JoiningMember {
                    member: MemberId::new(4)?,
                    node: target_node,
                    disk: target_disk,
                },
            },
        };
        f.s.f
            .catalog
            .reserve_movement(
                &cut,
                &request,
                &f.spare,
                std::slice::from_ref(&f.s.namespace),
                TIMEOUT,
            )
            .await
            .with_context(|| format!("pre-admit full-book movement {index}"))?;
        let hosts =
            f.s.nodes
                .iter()
                .map(|n| &n.host)
                .chain([&f.s.target, &f.spare])
                .collect::<Vec<_>>();
        discover(&f.s.f, &hosts).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_failure_recovery_full_movement_book_still_isolates_without_extra_debt() -> Result<()>
{
    Box::pin(full_book()).await
}

async fn full_book() -> Result<()> {
    let mut f = Box::pin(RepairFleet::with_admission(false, None, 2)).await?;
    fill_book(&f).await?;
    let before = f.s.f.catalog.read(TIMEOUT).await?;
    let pending = f.s.f.catalog.pending_movements(&before, TIMEOUT).await?;
    ensure!(pending.len() == 2);
    let book =
        f.s.f
            .catalog
            .get(
                &before,
                FleetTable::Policy,
                "resource-reservations",
                TIMEOUT,
            )
            .await?;
    let platform = Platform::new(&f);
    platform.mode.store(2, Ordering::Release);
    let inputs = f.evidence().await?;
    let blocked = Arc::new(BlockedMovements(AtomicUsize::new(0)));
    let service =
        f.s.f
            .catalog
            .start_failure_recovery_service(
                f.spare.clone(),
                blocked.clone(),
                inputs.clone(),
                platform.clone(),
                options(),
                FleetFailureDetectionOptions::default(),
            )
            .await?;
    detection::wait_for(&f, &service, |s| {
        s.failure_recovery
            .as_ref()
            .is_some_and(|r| r.phase == FleetFailureRecoveryPhase::Repairing)
    })
    .await
    .with_context(|| format!("full-book isolation: {:?}", service.status()))?;
    let after = f.s.f.catalog.read(TIMEOUT).await?;
    ensure!(blocked.0.load(Ordering::Acquire) >= 2);
    ensure!(platform.calls.load(Ordering::Acquire) > 0);
    ensure!(inputs.calls.load(Ordering::Acquire) == 0);
    let retained = f.s.f.catalog.pending_movements(&after, TIMEOUT).await?;
    ensure!(
        retained.len() == 2
            && retained
                .iter()
                .zip(&pending)
                .all(|(a, b)| a.reservation == b.reservation
                    && a.plan == b.plan
                    && a.group == b.group)
    );
    ensure!(
        f.s.f
            .catalog
            .get(&after, FleetTable::Policy, "resource-reservations", TIMEOUT)
            .await?
            == book
    );
    service.shutdown().await;
    let service = start(&f, &f.spare, inputs, platform).await?;
    f.wait(&service, |s| {
        s.settled_movements == 2
            && s.work.is_empty()
            && s.isolated_repair
                .as_ref()
                .is_some_and(|r| r.inventory_exhausted && r.blocked_histories == 0)
    })
    .await?;
    service.shutdown().await;
    for i in 0..2 {
        f.verify(i).await?;
    }
    f.close().await
}

struct Platform {
    catalog: FleetCatalog,
    namespaces: Vec<NamespaceCatalog>,
    boot: FleetBootBinding,
    calls: AtomicUsize,
    namespace_calls: AtomicUsize,
    active: Arc<AtomicUsize>,
    mode: AtomicUsize,
    incomplete: AtomicBool,
    lose_reply: AtomicBool,
    contexts: Mutex<Vec<FleetFailureRecoveryContext>>,
}

impl Platform {
    fn new(f: &RepairFleet) -> Arc<Self> {
        Arc::new(Self {
            catalog: f.s.f.catalog.clone(),
            namespaces: vec![f.s.namespace.clone()],
            boot: f.failed_boot.clone(),
            calls: AtomicUsize::new(0),
            namespace_calls: AtomicUsize::new(0),
            active: Arc::new(AtomicUsize::new(0)),
            mode: AtomicUsize::new(0),
            incomplete: AtomicBool::new(false),
            lose_reply: AtomicBool::new(false),
            contexts: Mutex::new(Vec::new()),
        })
    }
}

impl FleetFailureRecoveryPlatform for Platform {
    fn namespaces<'a>(
        &'a self,
        _: Duration,
    ) -> Pin<
        Box<
            dyn Future<Output = std::result::Result<Vec<NamespaceCatalog>, FleetCatalogError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async {
            self.namespace_calls.fetch_add(1, Ordering::AcqRel);
            Ok(if self.incomplete.load(Ordering::Acquire) {
                Vec::new()
            } else {
                self.namespaces.clone()
            })
        })
    }

    fn isolate<'a>(
        &'a self,
        context: &'a FleetFailureRecoveryContext,
        timeout: Duration,
    ) -> Pin<
        Box<
            dyn Future<Output = std::result::Result<FleetIsolationAttestation, FleetCatalogError>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(async move {
            // The externally visible effect must follow rooted exclusion.
            let cut = self.catalog.read(timeout).await?;
            let admitted = self
                .catalog
                .evacuation_admission(&cut, context.operation(), timeout)
                .await?;
            if context.boot() != &self.boot || admitted.node().bytes() != self.boot.node {
                return Err(FleetCatalogError::Invalid("fixture isolation scope"));
            }
            self.contexts.lock().unwrap().push(context.clone());
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.active.fetch_add(1, Ordering::AcqRel);
            let _callback = Callback(self.active.clone());
            while self.mode.load(Ordering::Acquire) == 0 {
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
            if self.lose_reply.swap(false, Ordering::AcqRel) {
                return Err(FleetCatalogError::Unconfirmed);
            }
            let mut boot = self.boot.clone();
            if self.mode.load(Ordering::Acquire) == 1 {
                boot.process = [233; 16];
            }
            Ok(FleetIsolationAttestation {
                boot,
                evidence_digest: [73; 32],
                evidence_reference: "fixture-only/joined-failed-boot".into(),
            })
        })
    }
}

async fn start(
    f: &RepairFleet,
    observer: &RaftCells,
    inputs: Arc<Inputs>,
    platform: Arc<Platform>,
) -> Result<FleetMovementService> {
    Ok(f.s
        .f
        .catalog
        .start_failure_recovery_service(
            observer.clone(),
            inputs.clone(),
            inputs,
            platform,
            options(),
            FleetFailureDetectionOptions::default(),
        )
        .await?)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_failure_recovery_resumes_isolation_after_controller_loss_and_repairs_rf3(
) -> Result<()> {
    Box::pin(recover()).await
}

async fn recover() -> Result<()> {
    let mut f = Box::pin(RepairFleet::with_admission(false, None, 3)).await?;
    let inputs = f.evidence().await?;
    let platform = Platform::new(&f);
    let before = f.s.f.catalog.read(TIMEOUT).await?;
    let service = start(&f, &f.spare, inputs.clone(), platform.clone()).await?;
    detection::wait_for(&f, &service, |_| {
        platform.active.load(Ordering::Acquire) == 1
    })
    .await?;
    let context = platform.contexts.lock().unwrap()[0].clone();
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    ensure!(cut.revision() == before.revision() + 1);
    ensure!(f
        .s
        .f
        .catalog
        .pending_movements(&cut, TIMEOUT)
        .await?
        .is_empty());
    ensure!(inputs.calls.load(Ordering::Acquire) == 0);
    // Cancellation cannot release a slot while an external stop may complete.
    let cancelled =
        f.s.f
            .catalog
            .cancel_evacuation(
                &cut,
                &FleetEvacuationCancelRequest {
                    identity: FleetRequestIdentity {
                        id: [234; 16],
                        operation: context.operation().bytes(),
                        principal: "fixture".into(),
                        expected_revision: cut.revision(),
                    },
                    node: f.failed,
                    fence: None,
                },
                None,
                TIMEOUT,
            )
            .await;
    ensure!(matches!(cancelled, Err(FleetCatalogError::Invalid(_))));
    let slot =
        f.s.f
            .catalog
            .get(&cut, FleetTable::Policy, "active-disruption", TIMEOUT)
            .await?
            .context("failure slot")?;
    let forged = FleetMutation {
        id: [235; 16],
        operation: context.operation().bytes(),
        principal: "fixture-forged-failure".into(),
        expected_revision: cut.revision(),
        changes: vec![FleetChange {
            table: FleetTable::Policy,
            key: "active-disruption".into(),
            value: Some(slot),
        }],
    };
    ensure!(matches!(
        f.s.f.catalog.commit(&cut, &forged, TIMEOUT).await,
        Err(FleetCatalogError::Invalid(_))
    ));
    let key = hex::encode(f.failed.bytes());
    let original =
        f.s.f
            .catalog
            .get(&cut, FleetTable::Nodes, &key, TIMEOUT)
            .await?
            .context("failed node")?;
    // Preserve the typed record's canonical field order so the candidate
    // reaches the boot fence, rather than failing the wire decoder first.
    let payload = std::str::from_utf8(&original[13..original.len() - 32])?;
    let old_process = format!(
        "\"process\":{}",
        serde_json::to_string(&f.failed_boot.process)?
    );
    ensure!(payload.matches(&old_process).count() == 1);
    let payload = payload
        .replacen(
            &old_process,
            &format!("\"process\":{}", serde_json::to_string(&[236u8; 16])?),
            1,
        )
        .into_bytes();
    let mut encoded = original[..9].to_vec();
    encoded.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    encoded.extend_from_slice(&payload);
    encoded.extend_from_slice(&Sha256::digest(&encoded));
    let reboot = FleetMutation {
        id: [236; 16],
        changes: vec![FleetChange {
            table: FleetTable::Nodes,
            key,
            value: Some(encoded),
        }],
        ..forged
    };
    ensure!(matches!(
        f.s.f.catalog.commit(&cut, &reboot, TIMEOUT).await,
        Err(FleetCatalogError::Invalid("failure boot remains fenced"))
    ));
    service.shutdown().await;
    ensure!(platform.active.load(Ordering::Acquire) == 0);
    // A new controller must recover the original operation, reject another
    // boot's attestation and leave membership/resources unchanged.
    platform.mode.store(1, Ordering::Release);
    let service = start(&f, &f.s.target, inputs.clone(), platform.clone()).await?;
    detection::wait_for(&f, &service, |s| {
        s.last_round_failure == Some(FleetMovementServiceFailure::Rejected)
    })
    .await?;
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == cut.revision());
    ensure!(inputs.calls.load(Ordering::Acquire) == 0);
    service.shutdown().await;
    platform.mode.store(2, Ordering::Release);
    platform.lose_reply.store(true, Ordering::Release);
    f.s.f
        .store
        .fail_fleet_confirmation
        .store(true, Ordering::Release);
    let service = start(&f, &f.spare, inputs.clone(), platform.clone()).await?;
    f.wait(&service, |s| {
        s.settled_movements == 3
            && s.work.is_empty()
            && s.isolated_repair
                .as_ref()
                .is_some_and(|r| r.inventory_exhausted && r.blocked_histories == 0)
    })
    .await?;
    ensure!(!platform.lose_reply.load(Ordering::Acquire));
    ensure!(!f.s.f.store.fail_fleet_confirmation.load(Ordering::Acquire));
    ensure!(platform
        .contexts
        .lock()
        .unwrap()
        .iter()
        .all(|c| c == &context));
    service.shutdown().await;
    for i in 0..3 {
        f.verify(i).await?;
    }
    retained_engine(&f.s.f, f.old_engine.clone()).await?;
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_failure_recovery_never_adopts_operator_evacuation() -> Result<()> {
    let f = Box::pin(RepairFleet::with_isolation(false, false)).await?;
    let platform = Platform::new(&f);
    platform.mode.store(2, Ordering::Release);
    let before = f.s.f.catalog.read(TIMEOUT).await?;
    let service = start(&f, &f.spare, f.evidence().await?, platform.clone()).await?;
    detection::wait_for(&f, &service, |s| s.consecutive_round_failures >= 2).await?;
    ensure!(platform.calls.load(Ordering::Acquire) == 0);
    ensure!(service.status().failure_recovery.is_none());
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == before.revision());
    service.shutdown().await;
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_failure_recovery_incomplete_inventory_cannot_admit_or_isolate() -> Result<()> {
    let f = Box::pin(RepairFleet::with_admission(false, None, 1)).await?;
    let platform = Platform::new(&f);
    platform.incomplete.store(true, Ordering::Release);
    let before = f.s.f.catalog.read(TIMEOUT).await?;
    let inputs = f.evidence().await?;
    let service = start(&f, &f.spare, inputs.clone(), platform.clone()).await?;
    detection::wait_for(&f, &service, |s| {
        platform.namespace_calls.load(Ordering::Acquire) > 0
            && s.last_round_failure == Some(FleetMovementServiceFailure::Rejected)
    })
    .await?;
    ensure!(platform.calls.load(Ordering::Acquire) == 0);
    ensure!(inputs.calls.load(Ordering::Acquire) == 0);
    ensure!(service.status().failure_recovery.is_none());
    ensure!(f.s.f.catalog.read(TIMEOUT).await?.revision() == before.revision());
    service.shutdown().await;
    f.close().await
}
