// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Retained real Raft work must not monopolize every available fleet slot.
use super::*;

struct Stalled {
    pending: FleetPendingMovement,
    permit: FleetPermitAuthorization,
    authorization: FleetGroupAuthorization,
    book: Vec<u8>,
    row: Vec<u8>,
}

impl Stalled {
    async fn begin(fleet: &RepairFleet) -> Result<Self> {
        let f = &fleet.s.f;
        let cut = f.catalog.read(TIMEOUT).await?;
        let scope = fleet.s.proposals[0].scope.clone();
        let authorization = f.catalog.authorize_group(&cut, &scope, TIMEOUT).await?;
        let source = &fleet.live[0][0];
        ensure!(source.0 == NodeId::new([5; 16])?);
        let proof = source
            .1
            .observe_fleet_membership(authorization.clone(), TIMEOUT)
            .await
            .context("capture stalled movement baseline quorum")?;
        let target = fleet.s.target.observe_node(TIMEOUT).await?;
        let mut resources = fleet.s.request(0, 226, cut.revision()).await?;
        resources.peak.durable_bytes = target.node.profile.bytes_per_group * 2;
        resources.peak.memory_bytes = 16 * 1024 * 1024;
        let request = FleetMovementRequest {
            resources,
            source: source.0,
            plan: RepairPlan {
                id: ProposalId::new([227; 16])?,
                policy_revision: 1,
                expected_revision: proof.membership_revision(),
                obsolete: MemberId::new(1)?,
                replacement: JoiningMember {
                    member: MemberId::new(4)?,
                    node: target.node.identity.node,
                    disk: target.node.identity.disk,
                },
            },
        };
        let admitted = f
            .catalog
            .reserve_movement(
                &cut,
                &request,
                &fleet.spare,
                std::slice::from_ref(&fleet.s.namespace),
                TIMEOUT,
            )
            .await
            .context("reserve stalled movement")?;
        let cut = f.catalog.read(TIMEOUT).await?;
        confirm_cut(fleet, cut.revision())
            .await
            .context("install stalled movement trust cut")?;
        let pending = f.catalog.pending_movements(&cut, TIMEOUT).await?.remove(0);
        let execution = FleetMovementExecutionRequest {
            reservation: pending.reservation,
            attestation: occupancy(fleet.s.target.observe_node(TIMEOUT).await?)?,
            from_s3: true,
        };
        let progress = f
            .catalog
            .reconcile_movement(&cut, &execution, &fleet.spare, TIMEOUT)
            .await
            .context("begin stalled movement")?;
        ensure!(progress.phase == FleetMovementPhase::Reserved && !progress.settled);
        let proof = source
            .1
            .observe_fleet_membership(authorization.clone(), TIMEOUT)
            .await
            .context("confirm stalled movement seeding")?;
        ensure!(proof.status().phase == Some(FleetPermitPhase::Seeding));
        let book = f
            .catalog
            .get(&cut, FleetTable::Policy, "resource-reservations", TIMEOUT)
            .await?
            .context("active resource book")?;
        let row = f
            .catalog
            .get(
                &cut,
                FleetTable::Operations,
                &format!("resource/{}", hex::encode(pending.reservation)),
                TIMEOUT,
            )
            .await?
            .context("stalled resource debit")?;
        Ok(Self {
            pending,
            permit: admitted.authorization(),
            authorization,
            book,
            row,
        })
    }

    async fn assert_retained(&self, fleet: &RepairFleet) -> Result<()> {
        let f = &fleet.s.f;
        let reader = Fixture::open_catalog(&f.domain, f.store.clone())?;
        let cut = reader.read(TIMEOUT).await?;
        let pending = reader.pending_movements(&cut, TIMEOUT).await?;
        ensure!(
            pending.len() == 1
                && pending[0].reservation == self.pending.reservation
                && pending[0].group == self.pending.group
                && pending[0].plan == self.pending.plan
        );
        ensure!(
            reader
                .get(&cut, FleetTable::Policy, "resource-reservations", TIMEOUT)
                .await?
                .as_deref()
                == Some(self.book.as_slice())
        );
        ensure!(
            reader
                .get(
                    &cut,
                    FleetTable::Operations,
                    &format!("resource/{}", hex::encode(self.pending.reservation)),
                    TIMEOUT
                )
                .await?
                .as_deref()
                == Some(self.row.as_slice())
        );
        Ok(())
    }
}

async fn confirm_cut(fleet: &RepairFleet, revision: u64) -> Result<()> {
    let hosts = fleet
        .s
        .nodes
        .iter()
        .map(|n| &n.host)
        .chain([&fleet.s.target, &fleet.spare])
        .collect::<Vec<_>>();
    for origin in &hosts {
        let own = origin.observe_node(TIMEOUT).await?.node.identity.node;
        for target in &hosts {
            let node = target.observe_node(TIMEOUT).await?.node.identity.node;
            if node != own {
                fleet.s.f.remote(origin, node, revision).await?;
            }
        }
    }
    Ok(())
}

struct Blocked {
    inputs: Arc<Inputs>,
    old: GroupScope,
    block_old: AtomicBool,
    block_all: AtomicBool,
    old_calls: AtomicUsize,
}

impl Blocked {
    fn new(inputs: Arc<Inputs>, old: GroupScope, all: bool) -> Arc<Self> {
        Arc::new(Self {
            inputs,
            old,
            block_old: AtomicBool::new(true),
            block_all: AtomicBool::new(all),
            old_calls: AtomicUsize::new(0),
        })
    }
}

impl FleetMovementEvidence for Blocked {
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
            if movement.group == self.old {
                self.old_calls.fetch_add(1, Ordering::AcqRel);
                if self.block_old.load(Ordering::Acquire) {
                    return Err(FleetCatalogError::Unconfirmed);
                }
            }
            if self.block_all.load(Ordering::Acquire) {
                return Err(FleetCatalogError::Unconfirmed);
            }
            FleetMovementEvidence::refresh(self.inputs.as_ref(), movement, timeout).await
        })
    }
}

struct PayloadGate(Arc<crate::fault::Store>);

impl Drop for PayloadGate {
    fn drop(&mut self) {
        self.0.release_payload();
    }
}

struct CasGate(Arc<crate::fault::Store>);

impl Drop for CasGate {
    fn drop(&mut self) {
        self.0.fleet_cas_hang.store(false, Ordering::Release);
        self.0.fleet_cas_resume.notify_waiters();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_isolated_repair_service_recovers_with_retained_physical_seed() -> Result<()> {
    Box::pin(physical_seed()).await
}

async fn physical_seed() -> Result<()> {
    // The second group has voters 4/6/7. Node7 is both its surviving voter and
    // the pending initializer for the first group's blocked replacement.
    let mut fleet = Box::pin(RepairFleet::with_histories(true, true, 1)).await?;
    let stalled = Stalled::begin(&fleet).await?;
    let f = &fleet.s.f;
    let reader = Fixture::open_catalog(&f.domain, f.store.clone())?;
    let target = fleet
        .s
        .target
        .register_managed_history(stalled.authorization.clone(), TIMEOUT)
        .await
        .context("register held-seed target")?;
    f.store.payload_hang.store(true, Ordering::Release);
    f.store.hold_next_payload.store(true, Ordering::Release);
    let gate = PayloadGate(f.store.clone());
    let pending = target.activate_managed_learner(
        stalled.permit.clone(),
        LearnerActivation {
            member: stalled.pending.plan.replacement.member,
            source: MemberId::new(2)?,
            source_node: NodeId::new([5; 16])?,
            from_s3: true,
            timeout: TIMEOUT,
        },
    );
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            while !f.store.payload_waiting.load(Ordering::Acquire) {
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await??;
    let old_cut = reader
        .read(TIMEOUT)
        .await
        .context("read catalog with held seed")?;
    let physical = fleet
        .spare
        .inspect_peer_physical_inventory(NodeId::new([7; 16])?, old_cut.revision(), TIMEOUT)
        .await
        .context("observe held initializer")?;
    ensure!(physical
        .inventory
        .as_ref()
        .context("held initializer")?
        .initializing()
        .is_some());
    let inputs = fleet.evidence().await?;
    let blocked = Blocked::new(inputs.clone(), stalled.pending.group.clone(), false);
    let service = f
        .catalog
        .start_isolated_repair_service(
            fleet.spare.clone(),
            blocked.clone(),
            inputs,
            ProposalId::new([100; 16])?,
            options(),
        )
        .await
        .context("start independent repair controller")?;
    // Prove a second debit was admitted while the first initializer was still
    // owned, rather than after cancellation made the node appear empty.
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                let cut = reader
                    .read(TIMEOUT)
                    .await
                    .context("read concurrent repair admission")?;
                let rows = reader.pending_movements(&cut, TIMEOUT).await?;
                if rows.len() == 2 {
                    ensure!(rows.iter().any(|r| r.group == fleet.s.proposals[1].scope));
                    break;
                }
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
            Ok::<_, anyhow::Error>(())
        }))
        .await???;
    let physical = fleet
        .spare
        .inspect_peer_physical_inventory(NodeId::new([7; 16])?, old_cut.revision(), TIMEOUT)
        .await
        .context("recheck concurrent initializer")?;
    ensure!(physical
        .inventory
        .as_ref()
        .context("concurrent initializer")?
        .initializing()
        .is_some());
    // The receiver owns this exact read after the grant source has finished.
    // Losing the waiter cannot drop that owner or release the admitted debit.
    drop(pending);
    fleet
        .s
        .f
        .domain
        .run(async {
            cellar::asyncrt::sleep(Duration::from_millis(20)).await;
        })
        .await?;
    ensure!(fleet.s.f.store.held_payload_reads.load(Ordering::Acquire) == 1);
    fleet
        .wait(&service, |s| s.settled_movements == 1 && s.work.len() == 1)
        .await?;
    ensure!(f.store.payload_hang.load(Ordering::Acquire));
    ensure!(blocked.old_calls.load(Ordering::Acquire) > 1);
    stalled.assert_retained(&fleet).await?;
    fleet.verify(1).await?;
    drop(gate);
    blocked.block_old.store(false, Ordering::Release);
    fleet
        .wait(&service, |s| s.settled_movements == 2 && s.work.is_empty())
        .await?;
    service.shutdown().await;
    fleet.verify(0).await?;
    retained_engine(&fleet.s.f, fleet.old_engine.clone()).await?;
    fleet.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_isolated_repair_service_races_last_slot_and_resumes_after_restart() -> Result<()> {
    Box::pin(last_slot()).await
}

async fn last_slot() -> Result<()> {
    let mut fleet = Box::pin(RepairFleet::new(false)).await?;
    let stalled = Stalled::begin(&fleet).await?;
    let reader = Fixture::open_catalog(&fleet.s.f.domain, fleet.s.f.store.clone())?;
    let inputs = fleet.evidence().await?;
    inputs.hold.store(true, Ordering::Release);
    let blocked = Blocked::new(inputs.clone(), stalled.pending.group.clone(), true);
    let first = fleet
        .s
        .f
        .catalog
        .start_isolated_repair_service(
            fleet.s.target.clone(),
            blocked.clone(),
            inputs.clone(),
            ProposalId::new([100; 16])?,
            options(),
        )
        .await?;
    let second = fleet
        .s
        .f
        .catalog
        .start_isolated_repair_service(
            fleet.spare.clone(),
            blocked.clone(),
            inputs.clone(),
            ProposalId::new([100; 16])?,
            options(),
        )
        .await?;
    fleet
        .wait(&first, |_| inputs.active.load(Ordering::Acquire) == 2)
        .await?;
    inputs.hold.store(false, Ordering::Release);
    fleet
        .wait(&first, |s| {
            s.work.len() == 2 && second.status().work.len() == 2
        })
        .await?;
    let cut = reader.read(TIMEOUT).await?;
    let rows = reader.pending_movements(&cut, TIMEOUT).await?;
    ensure!(
        rows.len() == 2
            && rows
                .iter()
                .any(|r| r.reservation == stalled.pending.reservation)
    );
    let remaining = (1..3)
        .find(|index| {
            !rows
                .iter()
                .any(|r| r.group == fleet.s.proposals[*index].scope)
        })
        .context("third history")?;
    let request = fleet.s.request(remaining, 228, cut.revision()).await?;
    confirm_cut(&fleet, cut.revision()).await?;
    let denied = reader
        .reserve_resources(
            &cut,
            &request,
            &fleet.spare,
            std::slice::from_ref(&fleet.s.namespace),
            TIMEOUT,
        )
        .await
        .err();
    ensure!(
        matches!(
            denied.as_ref(),
            Some(FleetCatalogError::Resource(
                cellar_logic::raft_fleet::LedgerError::MovementLimit
            ))
        ),
        "third admission result: {denied:?}"
    );
    ensure!(reader.read(TIMEOUT).await?.revision() == cut.revision());
    let rounds = first.status().poll_rounds;
    fleet.wait(&first, |s| s.poll_rounds >= rounds + 4).await?;
    ensure!(reader.read(TIMEOUT).await?.revision() == cut.revision());
    first.shutdown().await;
    second.shutdown().await;
    ensure!(inputs.active.load(Ordering::Acquire) == 0);
    blocked.block_all.store(false, Ordering::Release);
    let reopened = Fixture::open_catalog(&fleet.s.f.domain, fleet.s.f.store.clone())?;
    let service = reopened
        .start_isolated_repair_service(
            fleet.spare.clone(),
            blocked.clone(),
            inputs,
            ProposalId::new([100; 16])?,
            options(),
        )
        .await?;
    fleet
        .wait(&service, |s| s.settled_movements == 2 && s.work.len() == 1)
        .await?;
    stalled.assert_retained(&fleet).await?;
    for index in 1..3 {
        fleet.verify(index).await?;
    }
    blocked.block_old.store(false, Ordering::Release);
    fleet
        .wait(&service, |s| s.settled_movements == 3 && s.work.is_empty())
        .await?;
    service.shutdown().await;
    fleet.verify(0).await?;
    fleet.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_isolated_repair_service_fences_delayed_admission_with_retained_debt() -> Result<()> {
    Box::pin(fence()).await
}

async fn fence() -> Result<()> {
    let mut fleet = Box::pin(RepairFleet::new(false)).await?;
    let stalled = Stalled::begin(&fleet).await?;
    let reader = Fixture::open_catalog(&fleet.s.f.domain, fleet.s.f.store.clone())?;
    let first_inputs = fleet.evidence().await?;
    let second_inputs = fleet.evidence().await?;
    let first_blocked = Blocked::new(first_inputs.clone(), stalled.pending.group.clone(), false);
    let second_blocked = Blocked::new(second_inputs.clone(), stalled.pending.group.clone(), false);
    let cut = reader.read(TIMEOUT).await?;
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
    let gate = CasGate(fleet.s.f.store.clone());
    let first = fleet
        .s
        .f
        .catalog
        .start_isolated_repair_service(
            fleet.s.target.clone(),
            first_blocked.clone(),
            first_inputs.clone(),
            ProposalId::new([100; 16])?,
            options(),
        )
        .await?;
    fleet.s.waiting().await?;
    first_inputs.hold.store(true, Ordering::Release);
    second_inputs.hold.store(true, Ordering::Release);
    let mut quick = options();
    quick.turn_timeout = Duration::from_secs(1);
    let second = fleet
        .s
        .f
        .catalog
        .start_isolated_repair_service(
            fleet.spare.clone(),
            second_blocked.clone(),
            second_inputs.clone(),
            ProposalId::new([100; 16])?,
            quick,
        )
        .await?;
    fleet
        .wait(&second, |_| {
            second_inputs.active.load(Ordering::Acquire) > 0
        })
        .await?;
    let fenced = reader.read(TIMEOUT).await?;
    ensure!(fenced.revision() == cut.revision() + 1);
    stalled.assert_retained(&fleet).await?;
    ensure!(reader
        .get(
            &fenced,
            FleetTable::Operations,
            &format!("isolated-repair-fence/{}", hex::encode([100; 16])),
            TIMEOUT
        )
        .await?
        .is_some());
    drop(gate);
    fleet
        .wait(&first, |s| {
            s.isolated_repair.as_ref().is_some_and(|r| {
                r.last_blocked_failure == Some(FleetMovementServiceFailure::Conflict)
            }) && first_inputs.active.load(Ordering::Acquire) > 0
        })
        .await?;
    first.shutdown().await;
    second.shutdown().await;
    ensure!(
        first_inputs.active.load(Ordering::Acquire) == 0
            && second_inputs.active.load(Ordering::Acquire) == 0
    );
    stalled.assert_retained(&fleet).await?;
    second_inputs.hold.store(false, Ordering::Release);
    let service = fleet
        .s
        .f
        .catalog
        .start_isolated_repair_service(
            fleet.spare.clone(),
            second_blocked.clone(),
            second_inputs,
            ProposalId::new([100; 16])?,
            options(),
        )
        .await?;
    fleet
        .wait(&service, |s| s.settled_movements == 2 && s.work.len() == 1)
        .await?;
    stalled.assert_retained(&fleet).await?;
    for index in 1..3 {
        fleet.verify(index).await?;
    }
    second_blocked.block_old.store(false, Ordering::Release);
    fleet
        .wait(&service, |s| s.settled_movements == 3 && s.work.is_empty())
        .await?;
    service.shutdown().await;
    fleet.verify(0).await?;
    fleet.close().await
}
