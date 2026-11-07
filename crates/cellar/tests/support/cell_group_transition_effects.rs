// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! An S3 delivery loses its receiver reply, then resumes in a different group.
use super::*;
use cellar::application::consensus::{
    RaftDeliveryRequest, RaftEffectSigningKey, RaftEffectSink, RaftEffectSourceGrant,
    RaftEffectTarget, RaftInbox, RaftOutbox, RaftOutboxLimits,
};
use futures_util::future::BoxFuture;
use std::{
    cell::RefCell,
    sync::{
        atomic::{AtomicBool, AtomicUsize},
        Mutex, Weak,
    },
};

#[path = "cell_group_transition_effect_archive.rs"]
mod archive;

tokio::task_local! { static SCENE: RefCell<Scene>; }

#[derive(Default)]
struct State {
    released: AtomicBool,
    changed: AtomicBool,
    received: AtomicUsize,
    delivered: AtomicUsize,
    arrivals: AtomicUsize,
    failure: Mutex<Option<String>>,
    request: Mutex<Option<RaftDeliveryRequest>>,
    first: Mutex<Option<CommandReceipt>>,
    endpoint: Mutex<Weak<Endpoint>>,
    committed: tokio::sync::Notify,
}

#[derive(Default)]
struct Scene {
    state: Arc<State>,
    // The compiled sink holds only a Weak endpoint, preventing host/class cycles.
    endpoint: Option<Arc<Endpoint>>,
    emitted: Option<CommandReceipt>,
    fence: Option<CommandReceipt>,
    finished: bool,
}

struct Endpoint {
    hosts: Vec<RaftCells>,
}

struct Routing(Arc<State>);

impl RaftEffectSink for Routing {
    fn deliver(
        &self,
        request: RaftDeliveryRequest,
    ) -> BoxFuture<'static, std::result::Result<Vec<u8>, CommandFailure>> {
        let state = self.0.clone();
        Box::pin(async move {
            state.arrivals.fetch_add(1, Ordering::AcqRel);
            {
                let mut first = state.request.lock().unwrap();
                if let Some(first) = &*first {
                    if first.command_id() != request.command_id()
                        || first.encoded() != request.encoded()
                    {
                        state.changed.store(true, Ordering::Release);
                        return Err(CommandFailure::CommandConflict);
                    }
                } else {
                    *first = Some(request.clone());
                }
            }
            if !state.released.load(Ordering::Acquire) && state.first.lock().unwrap().is_some() {
                return Err(CommandFailure::Unavailable);
            }
            let endpoint = state
                .endpoint
                .lock()
                .unwrap()
                .upgrade()
                .ok_or(CommandFailure::Unavailable)?;
            let receipt = match send(&endpoint, &request, &state.failure).await {
                Ok(receipt) => receipt,
                Err(error) => {
                    return Err(error);
                }
            };
            {
                let mut first = state.first.lock().unwrap();
                if let Some(first) = &*first {
                    if receipt.result != first.result {
                        state.changed.store(true, Ordering::Release);
                        return Err(CommandFailure::CommandConflict);
                    }
                } else {
                    *first = Some(receipt.clone());
                }
            }
            if !state.released.load(Ordering::Acquire) {
                state.committed.notify_one();
                return Err(CommandFailure::Unavailable); // Receiver committed; sender lost its reply.
            }
            state.delivered.fetch_add(1, Ordering::AcqRel);
            Ok(receipt.result)
        })
    }
}

async fn send(
    endpoint: &Endpoint,
    request: &RaftDeliveryRequest,
    failure: &Mutex<Option<String>>,
) -> std::result::Result<CommandReceipt, CommandFailure> {
    let host = endpoint
        .hosts
        .iter()
        .find(|host| !host.is_closed())
        .ok_or(CommandFailure::Unavailable)?;
    let handle = host
        .cell(request.target().scope().clone())
        .map_err(|_| CommandFailure::Unavailable)?;
    let mut options = CommandOptions::new(
        request.command_id(),
        preparation::retry_deadline().map_err(|_| CommandFailure::Unavailable)?,
    );
    options.durability = request.target().durability();
    let receipt = handle
        .command(
            request.target().contract(),
            request.encoded().to_vec(),
            options,
        )
        .await
        .map_err(|error| {
            *failure.lock().unwrap() = Some(format!("{error:?}"));
            error
                .cause
                .refusal()
                .cloned()
                .unwrap_or(CommandFailure::Unavailable)
        })?;
    if receipt.durability != Durability::S3 {
        return Err(CommandFailure::UnsupportedCapability);
    }
    request
        .verify_receipt(&receipt.result)
        .map_err(|_| CommandFailure::CommandConflict)?;
    Ok(receipt)
}

fn state() -> Option<Arc<State>> {
    SCENE.try_with(|scene| scene.borrow().state.clone()).ok()
}

pub(super) fn enabled() -> bool {
    state().is_some()
}

pub(crate) async fn qualify(rooted: bool, store: Arc<fault::Store>) -> Result<()> {
    SCENE
        .scope(RefCell::new(Scene::default()), async {
            Box::pin(run_plan(rooted, store, Decision::Authorize)).await?;
            ensure!(
                SCENE.with(|scene| scene.borrow().finished),
                "effect audit was not reached"
            );
            Ok(())
        })
        .await
}

pub(super) fn class() -> Result<Option<RaftClass>> {
    let Some(state) = state() else {
        return Ok(None);
    };
    let base = counter_class()?;
    let sender = Arc::new(RaftEffectSigningKey::new([211; 32])?);
    let receiver = Arc::new(RaftEffectSigningKey::new([212; 32])?);
    let receive = OperationContract {
        operation: "Receive".into(),
        kind: OperationKind::Command,
        digest: [214; 32],
    };
    let target = RaftEffectTarget::new(
        cell(1)?,
        base.definition_digest(),
        receive.clone(),
        receiver.verification_key(),
        Durability::S3,
    )?;
    let inbox = RaftInbox::new(
        vec![RaftEffectSourceGrant::new(
            ClusterId::new([1; 16])?,
            NamespaceId::new([2; 16])?,
            "Counter",
            "Events",
            [215; 32],
            sender.verification_key(),
        )?],
        receiver,
        Durability::S3,
        4,
    )?;
    let outbox = RaftOutbox::new(
        sender,
        RaftOutboxLimits {
            retry_delay_ms: 100,
            ..Default::default()
        },
    )?
    .binding(
        "Events",
        [215; 32],
        target,
        Arc::new(Routing(state.clone())),
    )?;
    Ok(Some(base.inbox("Receive", receive.digest, inbox, move |context, input| {
        let amount = i64::from_be_bytes(input.try_into()?);
        context.sql().execute_batch("CREATE TABLE IF NOT EXISTS effect_deliveries(count INTEGER NOT NULL,total INTEGER NOT NULL); INSERT INTO effect_deliveries SELECT 0,0 WHERE NOT EXISTS(SELECT 1 FROM effect_deliveries)")?;
        context.sql().execute("UPDATE effect_deliveries SET count=count+1,total=total+?1", [amount])?;
        state.received.fetch_add(1, Ordering::AcqRel);
        Ok(amount.to_be_bytes().to_vec())
    })?.outbox(outbox)?
        .command("Emit", [216; 32], |context, input| Ok(context.emit("Events", input)?.to_be_bytes().to_vec()))?
        .query("EffectProgress", [217; 32], |context, _| {
            let progress = context.outbox_progress("Events")?;
            Ok([progress.emitted.to_be_bytes(), progress.settled.to_be_bytes()].concat())
        })?
        .query("EffectStats", [218; 32], |context, _| {
            let (count,total): (i64,i64) = context.query_row("SELECT count,total FROM effect_deliveries", [], |row| Ok((row.get(0)?,row.get(1)?)))?;
            Ok([count.to_be_bytes(),total.to_be_bytes()].concat())
        })?))
}

fn cell(index: u8) -> Result<CellHistory> {
    Ok(CellHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        cell: CellId::new(format!("Counter:cold-{index}"))?,
        incarnation: IncarnationId::new([91; 16])?,
    })
}

fn endpoint(hosts: &[RaftCells]) -> Arc<Endpoint> {
    let endpoint = Arc::new(Endpoint {
        hosts: hosts.to_vec(),
    });
    SCENE.with(|scene| {
        let mut scene = scene.borrow_mut();
        *scene.state.endpoint.lock().unwrap() = Arc::downgrade(&endpoint);
        scene.endpoint = Some(endpoint.clone());
    });
    endpoint
}

async fn query(
    endpoint: &Endpoint,
    index: u8,
    operation: &str,
    id: u8,
    consistency: QueryConsistency,
) -> Result<QueryResult> {
    let host = endpoint
        .hosts
        .iter()
        .find(|host| !host.is_closed())
        .context("live endpoint")?;
    let handle = host.cell(cell(index)?)?;
    Ok(handle
        .query(
            ProposalId::new([id; 16])?,
            &handle.contract(operation)?,
            vec![],
            QueryOptions {
                consistency,
                ..Default::default()
            },
        )
        .await?)
}

// These are trusted namespace permissions, separate from voter membership.
// Inspect the rooted current boot before granting; retained startup changes it.
async fn authorize(cluster: &ManagedCluster, members: std::ops::Range<usize>) -> Result<()> {
    let cut = cluster.fixtures[0].catalog.read(TIMEOUT).await?;
    for index in members.clone() {
        let host = &cluster.hosts[index];
        for peer in &cluster.peers[members.clone()] {
            if peer.node == host.identity().node {
                continue;
            }
            crate::managed_fixture::inspect_current(host, peer.node, cut.revision()).await?;
            host.grant_command_origin(NamespaceCommandOrigin {
                cluster: cluster.history.cluster,
                namespace: cluster.history.namespace,
                revision: 1,
                peer: peer.clone(),
            })?;
        }
    }
    Ok(())
}

// Foreground probes share cells with the delivery worker. Retry only typed
// contention/readiness refusals with the exact identity, input and original cap.
// Transport loss, archive/proof errors and definitive command failures stay hard.
pub(super) async fn command_with_pending_effects(
    handle: &RaftCellHandle,
    contract: &OperationContract,
    input: Vec<u8>,
    options: CommandOptions,
) -> Result<CommandReceipt> {
    if !enabled() {
        return Ok(handle.command(contract, input, options).await?);
    }
    let deadline = tokio::time::Instant::now() + options.timeout;
    tokio::time::timeout_at(deadline, async {
        loop {
            let mut attempt = options.clone();
            attempt.timeout = deadline.saturating_duration_since(tokio::time::Instant::now());
            match handle.command(contract, input.clone(), attempt).await {
                Err(error) if retryable_probe(&error.cause) => {
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                result => return Ok::<_, anyhow::Error>(result?),
            }
        }
    })
    .await?
}

fn retryable_probe(error: &CellDispatchError) -> bool {
    matches!(
        error,
        CellDispatchError::Execution(
            cellar::application::consensus::verification::ExecutionError::Busy
        ) | CellDispatchError::Runtime(
            RuntimeError::NotReady | RuntimeError::NotLeader | RuntimeError::Overloaded
        )
    ) || matches!(
        error.refusal(),
        Some(
            CommandFailure::Unavailable
                | CommandFailure::Overloaded
                | CommandFailure::NotLeader { .. }
        )
    )
}

async fn lead_on_unrestarted_voter(cluster: &ManagedCluster, history: GroupHistory) -> Result<()> {
    let voters = &cluster.hosts[3..];
    let current = crate::managed_fixture::leader(voters, history).await?;
    if current != 1 {
        let group = voters[current].group(history)?;
        let progress = group.replication_status(TIMEOUT).await?;
        group
            .transfer_leadership(
                LeadershipTransfer {
                    target: MemberId::new(2)?,
                    expected_term: progress.term,
                    expected_revision: progress.membership_revision,
                },
                TIMEOUT,
            )
            .await?;
    }
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let status = voters[1].group(history)?.status().await?;
            if status.ready && status.leader == Some(2) {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    ensure!(voters[1].identity().process == cluster.peers[4].process);
    Ok(())
}

pub(super) async fn before_seal(cluster: &ManagedCluster, cells: &[CellHistory]) -> Result<()> {
    let Some(state) = state() else {
        return Ok(());
    };
    ensure!(cells == [cell(0)?, cell(1)?]);
    authorize(cluster, 0..3).await?;
    let endpoint = endpoint(&cluster.hosts[..3]);
    let handle = endpoint.hosts[0].cell(cell(0)?)?;
    let mut options = CommandOptions::new(
        CommandId::new(ClientId::new([155; 16])?, 1)?,
        preparation::retry_deadline()?,
    );
    options.durability = Durability::S3;
    let emitted = command_with_pending_effects(
        &handle,
        &handle.contract("Emit")?,
        17i64.to_be_bytes().to_vec(),
        options,
    )
    .await?;
    ensure!(emitted.durability == Durability::S3 && emitted.result == 1u64.to_be_bytes());
    tokio::time::timeout(TIMEOUT, state.committed.notified()).await.with_context(|| format!(
        "receiver never committed the first S3 delivery: arrivals={}, request_bytes={:?}, failure={:?}",
        state.arrivals.load(Ordering::Acquire), state.request.lock().unwrap().as_ref().map(|request| request.encoded().len()),
        state.failure.lock().unwrap()))?;
    ensure!(
        state.received.load(Ordering::Acquire) == 1 && state.delivered.load(Ordering::Acquire) == 0
    );
    let stats = query(
        &endpoint,
        1,
        "EffectStats",
        150,
        QueryConsistency::Linearizable,
    )
    .await?;
    ensure!(stats.result == [1i64.to_be_bytes(), 17i64.to_be_bytes()].concat());
    let progress = query(
        &endpoint,
        0,
        "EffectProgress",
        151,
        QueryConsistency::Linearizable,
    )
    .await?;
    ensure!(progress.result == [1u64.to_be_bytes(), 0u64.to_be_bytes()].concat());
    SCENE.with(|scene| scene.borrow_mut().emitted = Some(emitted));
    Ok(())
}

pub(super) async fn resume_children(
    cluster: &ManagedCluster,
    directory: &RangeDirectory,
) -> Result<()> {
    let Some(state) = state() else {
        return Ok(());
    };
    authorize(cluster, 3..6).await?;
    let f = &cluster.fixtures[0];
    let view = f.domain.run(directory.observe()).await??;
    let a = f
        .domain
        .run(directory.lookup(&view, &cell(0)?.cell))
        .await??;
    let b = f
        .domain
        .run(directory.lookup(&view, &cell(1)?.cell))
        .await??;
    ensure!(a.history != b.history && a.history != cluster.history && b.history != cluster.history);
    // Only voter zero reopens after archive initialization. Voter one keeps its
    // original process, so S3 settlement must discover a late binding in place.
    lead_on_unrestarted_voter(cluster, a.history).await?;
    lead_on_unrestarted_voter(cluster, b.history).await?;
    let endpoint = endpoint(&cluster.hosts[3..]);
    let progress = query(
        &endpoint,
        0,
        "EffectProgress",
        152,
        QueryConsistency::Linearizable,
    )
    .await?;
    ensure!(
        progress.result == [1u64.to_be_bytes(), 0u64.to_be_bytes()].concat(),
        "pending effect disappeared during transfer"
    );
    state.released.store(true, Ordering::Release);
    let mut last_progress = Vec::new();
    let waited = tokio::time::timeout(TIMEOUT, async {
        loop {
            let progress = query(
                &endpoint,
                0,
                "EffectProgress",
                153,
                QueryConsistency::Linearizable,
            )
            .await?;
            last_progress = progress.result.clone();
            if progress.result == [1u64.to_be_bytes(), 1u64.to_be_bytes()].concat() {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if waited.is_err() {
        let mut observed = Vec::new();
        for host in &cluster.hosts[3..] {
            let group = host.group(a.history)?;
            observed.push((
                host.identity().node,
                group.status().await,
                group.archive_status().map(|status| {
                    status.and_then(|status| status.published.map(|marker| marker.position()))
                }),
                host.resource_usage().host.controller,
            ));
        }
        anyhow::bail!("child did not settle the original delivery: progress={last_progress:?}, arrivals={}, delivered={}, changed={}, failure={:?}, observed={observed:?}",
            state.arrivals.load(Ordering::Acquire), state.delivered.load(Ordering::Acquire), state.changed.load(Ordering::Acquire), state.failure.lock().unwrap());
    }
    waited??;
    ensure!(
        state.delivered.load(Ordering::Acquire) > 0
            && state.received.load(Ordering::Acquire) == 1
            && !state.changed.load(Ordering::Acquire)
    );
    let handle = endpoint.hosts[0].cell(cell(0)?)?;
    let mut options = CommandOptions::new(
        CommandId::new(ClientId::new([155; 16])?, 2)?,
        preparation::retry_deadline()?,
    );
    options.durability = Durability::S3;
    let fence = command_with_pending_effects(
        &handle,
        &handle.contract("Add")?,
        0i64.to_be_bytes().to_vec(),
        options,
    )
    .await?;
    ensure!(fence.durability == Durability::S3 && fence.witness.group == a.history);
    SCENE.with(|scene| scene.borrow_mut().fence = Some(fence));
    Ok(())
}

pub(super) async fn after_cleanup(
    cluster: &ManagedCluster,
    directory: &RangeDirectory,
) -> Result<()> {
    let Some(state) = state() else {
        return Ok(());
    };
    let endpoint = endpoint(&cluster.hosts[3..]);
    let original = SCENE
        .with(|scene| scene.borrow().emitted.clone())
        .context("original emission")?;
    let handle = endpoint.hosts[0].cell(cell(0)?)?;
    let mut options = CommandOptions::new(original.id, preparation::retry_deadline()?);
    options.durability = Durability::S3;
    let replay = command_with_pending_effects(
        &handle,
        &handle.contract("Emit")?,
        17i64.to_be_bytes().to_vec(),
        options,
    )
    .await?;
    ensure!(
        replay.id == original.id
            && replay.result == original.result
            && replay.durability == Durability::S3
            && replay.version.history == original.version.history
            && replay.version.sequence >= original.version.sequence
    );
    let floor = query(
        &endpoint,
        0,
        "Get",
        154,
        QueryConsistency::AtLeast(original.version_token.clone()),
    )
    .await?;
    ensure!(floor.version.history == original.version.history);
    let request = state
        .request
        .lock()
        .unwrap()
        .clone()
        .context("original delivery envelope")?;
    let first = state
        .first
        .lock()
        .unwrap()
        .clone()
        .context("original S3 receiver receipt")?;
    let duplicate = send(&endpoint, &request, &state.failure)
        .await
        .map_err(|error| anyhow::anyhow!("retained receiver duplicate: {error:?}"))?;
    ensure!(duplicate.result == first.result && duplicate.durability == Durability::S3);
    let stats = query(
        &endpoint,
        1,
        "EffectStats",
        156,
        QueryConsistency::AtLeast(first.version_token.clone()),
    )
    .await?;
    ensure!(stats.result == [1i64.to_be_bytes(), 17i64.to_be_bytes()].concat());
    let progress = query(
        &endpoint,
        0,
        "EffectProgress",
        157,
        QueryConsistency::Linearizable,
    )
    .await?;
    ensure!(progress.result == [1u64.to_be_bytes(), 1u64.to_be_bytes()].concat());
    ensure!(state.received.load(Ordering::Acquire) == 1 && !state.changed.load(Ordering::Acquire));
    let fence = SCENE
        .with(|scene| scene.borrow().fence.clone())
        .context("child completion archive fence")?;
    archive::verify(cluster, directory, &first, &fence).await?;
    SCENE.with(|scene| scene.borrow_mut().finished = true);
    Ok(())
}
