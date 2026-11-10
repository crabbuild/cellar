// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! An uncertain external delivery survives managed recovery without a new ID.
use super::*;
use cellar::application::consensus::{
    RaftDeliveryRequest, RaftEffectSigningKey, RaftEffectSink, RaftEffectSourceGrant,
    RaftEffectTarget, RaftInbox, RaftOutbox, RaftOutboxLimits,
};
use futures_util::future::BoxFuture;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, AtomicUsize},
    Mutex, Weak,
};

tokio::task_local! { static SCENE: Arc<State>; }

#[derive(Default)]
pub(super) struct State {
    source: Mutex<Option<CellHistory>>,
    original: Mutex<Option<(CommandId, Vec<u8>)>>,
    attempts: AtomicUsize,
    joined: AtomicUsize,
    fenced: AtomicUsize,
    fresh: AtomicUsize,
    received: AtomicUsize,
    delivery_retry: AtomicU64,
    endpoint: Mutex<Weak<Endpoint>>,
    failure: Mutex<Option<String>>,
    changed: AtomicBool,
    checked: AtomicBool,
    entered: tokio::sync::Notify,
    fresh_entered: tokio::sync::Notify,
}

struct Endpoint {
    hosts: Vec<RaftCells>,
    group: GroupHistory,
}

struct Pending(Arc<State>);

impl Drop for Pending {
    fn drop(&mut self) {
        self.0.joined.fetch_add(1, Ordering::AcqRel);
    }
}

struct Sink {
    state: Arc<State>,
    fresh: bool,
}

impl RaftEffectSink for Sink {
    fn deliver(
        &self,
        request: RaftDeliveryRequest,
    ) -> BoxFuture<'static, std::result::Result<Vec<u8>, CommandFailure>> {
        let state = self.state.clone();
        let fresh = self.fresh;
        Box::pin(async move {
            if fresh {
                return send(&state, request).await;
            }
            state.attempts.fetch_add(1, Ordering::AcqRel);
            {
                let mut original = state.original.lock().unwrap();
                if let Some((id, bytes)) = &*original {
                    if *id != request.command_id() || bytes != request.encoded() {
                        state.changed.store(true, Ordering::Release);
                    }
                } else {
                    *original = Some((request.command_id(), request.encoded().to_vec()));
                }
            }
            // The external side accepted this identity; its response never arrives.
            // Cancellation drops the owned adapter future, not the durable intent.
            let _pending = Pending(state.clone());
            state.entered.notify_one();
            std::future::pending().await
        })
    }
}

async fn send(
    state: &Arc<State>,
    request: RaftDeliveryRequest,
) -> std::result::Result<Vec<u8>, CommandFailure> {
    let endpoint =
        { state.endpoint.lock().unwrap().upgrade() }.ok_or(CommandFailure::Unavailable)?;
    let index = leader(&endpoint.hosts, endpoint.group)
        .await
        .map_err(|_| CommandFailure::Unavailable)?;
    let handle = endpoint.hosts[index]
        .cell(request.target().scope().clone())
        .map_err(|_| CommandFailure::Unavailable)?;
    let mut options = CommandOptions::new(
        request.command_id(),
        state.delivery_retry.load(Ordering::Acquire),
    );
    options.timeout = Duration::from_millis(RaftEffectOptions::default().operation_timeout_ms);
    options.durability = request.target().durability();
    let receipt = handle
        .command(
            request.target().contract(),
            request.encoded().to_vec(),
            options,
        )
        .await
        .map_err(|error| {
            *state.failure.lock().unwrap() = Some(format!("{error:?}"));
            CommandFailure::Unavailable
        })?;
    if receipt.durability != Durability::S3 {
        return Err(CommandFailure::UnsupportedCapability);
    }
    request
        .verify_receipt(&receipt.result)
        .map_err(|_| CommandFailure::CommandConflict)?;
    state.fresh.fetch_add(1, Ordering::AcqRel);
    state.fresh_entered.notify_one();
    Ok(receipt.result)
}

pub(super) fn state() -> Option<Arc<State>> {
    SCENE.try_with(Arc::clone).ok()
}

pub(crate) async fn qualify(rooted: bool, store: Arc<fault::Store>) -> Result<()> {
    let state = Arc::new(State::default());
    SCENE
        .scope(state.clone(), async {
            Box::pin(super::qualify_disaster(
                rooted,
                store,
                RecoveryPoint::PublishedChildren,
            ))
            .await?;
            ensure!(
                state.checked.load(Ordering::Acquire),
                "recovered outbox audit was not reached"
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
    let key = Arc::new(RaftEffectSigningKey::new([219; 32])?);
    let receiver = Arc::new(RaftEffectSigningKey::new([220; 32])?);
    let receive = OperationContract {
        operation: "Receive".into(),
        kind: OperationKind::Command,
        digest: [220; 32],
    };
    let scope = |name: &str, incarnation: u8| -> Result<CellHistory> {
        Ok(CellHistory {
            cluster: ClusterId::new([1; 16])?,
            namespace: NamespaceId::new([2; 16])?,
            cell: CellId::new(name)?,
            incarnation: IncarnationId::new([incarnation; 16])?,
        })
    };
    let target = |scope| {
        RaftEffectTarget::new(
            scope,
            base.definition_digest(),
            receive.clone(),
            receiver.verification_key(),
            Durability::S3,
        )
    };
    let old_target = target(scope("Counter:external", 91)?)?;
    // The recovery fixture assigns incarnation 180 to its single replacement.
    let fresh_target = target(scope("Counter:cold-1", 180)?)?;
    let inbox = RaftInbox::new(
        vec![RaftEffectSourceGrant::new(
            ClusterId::new([1; 16])?,
            NamespaceId::new([2; 16])?,
            "Counter",
            "Fresh",
            [222; 32],
            key.verification_key(),
        )?],
        receiver,
        Durability::S3,
        4,
    )?;
    let outbox = RaftOutbox::new(key, RaftOutboxLimits::default())?
        .binding(
            "Events",
            [221; 32],
            old_target,
            Arc::new(Sink {
                state: state.clone(),
                fresh: false,
            }),
        )?
        .binding(
            "Fresh",
            [222; 32],
            fresh_target,
            Arc::new(Sink {
                state: state.clone(),
                fresh: true,
            }),
        )?;
    Ok(Some(
        base.inbox("Receive", receive.digest, inbox, move |context, input| {
            context.sql().execute_batch(
                "CREATE TABLE IF NOT EXISTS recovered_deliveries(value BLOB NOT NULL)",
            )?;
            context
                .sql()
                .execute("INSERT INTO recovered_deliveries VALUES(?1)", [input])?;
            state.received.fetch_add(1, Ordering::AcqRel);
            Ok(input.to_vec())
        })?
        .outbox(outbox)?
        .command("EmitUncertain", [223; 32], |context, input| {
            Ok(context.emit("Events", input)?.to_be_bytes().to_vec())
        })?
        .command("EmitFresh", [224; 32], |context, input| {
            Ok(context.emit("Fresh", input)?.to_be_bytes().to_vec())
        })?
        .query("OutboxState", [225; 32], |context, _| {
            let old = context.outbox_progress("Events")?;
            let fresh = context.outbox_progress("Fresh")?;
            Ok(progress_bytes(
                old.emitted,
                old.settled,
                old.requires_reconciliation,
                fresh.emitted,
                fresh.settled,
                fresh.requires_reconciliation,
            ))
        })?
        .query("FreshStats", [230; 32], |context, _| {
            let count: i64 =
                context.query_row("SELECT count(*) FROM recovered_deliveries", [], |row| {
                    row.get(0)
                })?;
            Ok(count.to_be_bytes().to_vec())
        })?,
    ))
}

fn progress_bytes(
    emitted: u64,
    settled: u64,
    blocked: bool,
    fresh_emitted: u64,
    fresh_settled: u64,
    fresh_blocked: bool,
) -> Vec<u8> {
    [
        emitted.to_be_bytes().as_slice(),
        settled.to_be_bytes().as_slice(),
        &[u8::from(blocked)],
        fresh_emitted.to_be_bytes().as_slice(),
        fresh_settled.to_be_bytes().as_slice(),
        &[u8::from(fresh_blocked)],
    ]
    .concat()
}

pub(super) async fn before_recovery(
    cluster: &ManagedCluster,
    group: GroupHistory,
    routed: &[(CellHistory, i64)],
) -> Result<()> {
    let Some(state) = state() else {
        return Ok(());
    };
    let source = routed.first().context("recovery source cell")?.0.clone();
    *state.source.lock().unwrap() = Some(source.clone());
    let host = &cluster.hosts[leader(&cluster.hosts[..3], group).await?];
    let handle = host.cell(source)?;
    let entered = state.entered.notified();
    tokio::pin!(entered);
    let receipt = handle
        .command(
            &handle.contract("EmitUncertain")?,
            b"external accepted".to_vec(),
            CommandOptions {
                durability: Durability::S3,
                timeout: TIMEOUT,
                ..CommandOptions::new(
                    CommandId::new(ClientId::new([226; 16])?, 1)?,
                    preparation::retry_deadline()?,
                )
            },
        )
        .await?;
    ensure!(receipt.result == 1u64.to_be_bytes() && receipt.durability == Durability::S3);
    tokio::time::timeout(TIMEOUT, entered)
        .await
        .context("old delivery did not reach the external adapter")?;
    Ok(())
}

pub(super) fn source_fenced(state: Option<&Arc<State>>) -> Result<()> {
    let Some(state) = state else {
        return Ok(());
    };
    let attempts = state.attempts.load(Ordering::Acquire);
    ensure!(
        attempts > 0 && state.joined.load(Ordering::Acquire) == attempts,
        "fencing did not join all entered external deliveries"
    );
    state.fenced.store(attempts, Ordering::Release);
    ensure!(!state.changed.load(Ordering::Acquire));
    Ok(())
}

pub(super) async fn verify_recovered(
    state: Option<&Arc<State>>,
    hosts: &[RaftCells],
    plan: &cellar::sdk::fleet_operations::FleetTransitionRecoveryReplacement,
) -> Result<()> {
    let Some(state) = state else {
        return Ok(());
    };
    ensure!(plan.births().len() == 1);
    let group = plan.births()[0].route().history;
    ensure!(group.incarnation == IncarnationId::new([180; 16])?);
    let old = state
        .source
        .lock()
        .unwrap()
        .clone()
        .context("uncertain source")?;
    let cell = CellHistory {
        incarnation: group.incarnation,
        ..old
    };
    let endpoint = Arc::new(Endpoint {
        hosts: hosts.to_vec(),
        group,
    });
    *state.endpoint.lock().unwrap() = Arc::downgrade(&endpoint);
    let handle = hosts[leader(hosts, group).await?].cell(cell)?;
    let query = handle
        .query(
            ProposalId::new([227; 16])?,
            &handle.contract("OutboxState")?,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                ..Default::default()
            },
        )
        .await?;
    ensure!(query.result == progress_bytes(1, 0, true, 0, 0, false));
    ensure!(
        state.received.load(Ordering::Acquire) == 0,
        "recovery replayed the inbox callback"
    );
    let retry = preparation::retry_deadline()?;
    state.delivery_retry.store(retry, Ordering::Release);
    handle
        .command(
            &handle.contract("EmitFresh")?,
            b"independent fresh lane".to_vec(),
            CommandOptions {
                durability: Durability::S3,
                timeout: TIMEOUT,
                ..CommandOptions::new(CommandId::new(ClientId::new([228; 16])?, 1)?, retry)
            },
        )
        .await?;
    let settled = tokio::time::timeout(TIMEOUT, async {
        loop {
            let query = handle
                .query(
                    ProposalId::new([229; 16])?,
                    &handle.contract("OutboxState")?,
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::Linearizable,
                        ..Default::default()
                    },
                )
                .await?;
            if query.result == progress_bytes(1, 0, true, 1, 1, false) {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    settled.with_context(|| {
        format!(
            "fresh lane did not settle, last adapter failure={:?}",
            state.failure.lock().unwrap()
        )
    })??;
    ensure!(state.fresh.load(Ordering::Acquire) > 0 && state.received.load(Ordering::Acquire) == 1);
    let receiver = hosts[leader(hosts, group).await?].cell(CellHistory {
        cluster: group.cluster,
        namespace: group.namespace,
        cell: CellId::new("Counter:cold-1")?,
        incarnation: group.incarnation,
    })?;
    let count = receiver
        .query(
            ProposalId::new([230; 16])?,
            &receiver.contract("FreshStats")?,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                ..Default::default()
            },
        )
        .await?;
    ensure!(count.result == 1i64.to_be_bytes());
    // Capture the receiver and source-completion entries in one later S3 cut.
    // The old lane remains blocked; the fresh positive control is quiescent.
    let barrier = handle
        .command(
            &handle.contract("Add")?,
            0i64.to_be_bytes().to_vec(),
            CommandOptions {
                durability: Durability::S3,
                timeout: TIMEOUT,
                ..CommandOptions::new(CommandId::new(ClientId::new([228; 16])?, 2)?, retry)
            },
        )
        .await?;
    preparation::wait_transfer_applied(hosts, &[(group, barrier.witness.position.index)]).await?;
    ensure!(
        state.attempts.load(Ordering::Acquire) == state.fenced.load(Ordering::Acquire)
            && !state.changed.load(Ordering::Acquire),
        "recovery reissued an uncertain old-origin delivery"
    );
    state.checked.store(true, Ordering::Release);
    Ok(())
}
