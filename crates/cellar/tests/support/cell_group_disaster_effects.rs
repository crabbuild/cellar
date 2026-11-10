// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! An uncertain external delivery survives managed recovery without a new ID.
use super::*;
use cellar::application::consensus::{
    RaftDeliveryRequest, RaftEffectSigningKey, RaftEffectSink, RaftEffectTarget, RaftOutbox,
    RaftOutboxLimits,
};
use futures_util::future::BoxFuture;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize},
    Mutex,
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
    changed: AtomicBool,
    checked: AtomicBool,
    entered: tokio::sync::Notify,
    fresh_entered: tokio::sync::Notify,
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
                state.fresh.fetch_add(1, Ordering::AcqRel);
                state.fresh_entered.notify_one();
                return Err(CommandFailure::Unavailable);
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
    let target = RaftEffectTarget::new(
        CellHistory {
            cluster: ClusterId::new([1; 16])?,
            namespace: NamespaceId::new([2; 16])?,
            cell: CellId::new("Counter:external")?,
            incarnation: IncarnationId::new([91; 16])?,
        },
        base.definition_digest(),
        OperationContract {
            operation: "Receive".into(),
            kind: OperationKind::Command,
            digest: [220; 32],
        },
        key.verification_key(),
        Durability::S3,
    )?;
    let outbox = RaftOutbox::new(key, RaftOutboxLimits::default())?
        .binding(
            "Events",
            [221; 32],
            target.clone(),
            Arc::new(Sink {
                state: state.clone(),
                fresh: false,
            }),
        )?
        .binding(
            "Fresh",
            [222; 32],
            target,
            Arc::new(Sink { state, fresh: true }),
        )?;
    Ok(Some(
        base.outbox(outbox)?
            .command("EmitUncertain", [223; 32], |context, input| {
                Ok(context.emit("Events", input)?.to_be_bytes().to_vec())
            })?
            .command("EmitFresh", [224; 32], |context, input| {
                Ok(context.emit("Fresh", input)?.to_be_bytes().to_vec())
            })?
            .query("OutboxState", [225; 32], |context, _| {
                let progress = context.outbox_progress("Events")?;
                let mut bytes = [
                    progress.emitted.to_be_bytes(),
                    progress.settled.to_be_bytes(),
                ]
                .concat();
                bytes.push(u8::from(progress.requires_reconciliation));
                Ok(bytes)
            })?,
    ))
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
    old: &CellHistory,
    handle: &RaftCellHandle,
) -> Result<()> {
    let Some(state) = state else {
        return Ok(());
    };
    if state.source.lock().unwrap().as_ref() != Some(old) {
        return Ok(());
    }
    let expected = [
        1u64.to_be_bytes().as_slice(),
        0u64.to_be_bytes().as_slice(),
        &[1],
    ]
    .concat();
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
    ensure!(
        query.result == expected,
        "old-origin ambiguity was not exposed after restore"
    );
    let fresh = state.fresh_entered.notified();
    tokio::pin!(fresh);
    handle
        .command(
            &handle.contract("EmitFresh")?,
            b"independent fresh lane".to_vec(),
            CommandOptions {
                durability: Durability::S3,
                timeout: TIMEOUT,
                ..CommandOptions::new(
                    CommandId::new(ClientId::new([228; 16])?, 1)?,
                    preparation::retry_deadline()?,
                )
            },
        )
        .await?;
    tokio::time::timeout(TIMEOUT, fresh)
        .await
        .context("uncertain old lane starved the fresh delivery")?;
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
    ensure!(query.result == expected && state.fresh.load(Ordering::Acquire) > 0);
    ensure!(
        state.attempts.load(Ordering::Acquire) == state.fenced.load(Ordering::Acquire)
            && !state.changed.load(Ordering::Acquire),
        "recovery reissued an uncertain old-origin delivery"
    );
    state.checked.store(true, Ordering::Release);
    Ok(())
}
