// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A provider can retain a known outcome before its caller receives the reply.
use super::*;

struct LostReturn {
    identity: BackendIdentity,
    committed: Notify,
    receipt: Mutex<Option<(ExecutionPendingOperation, ExecutionOutcome)>>,
    executions: AtomicUsize,
}

impl ExecutionControllerBackend for LostReturn {
    fn identity(&self) -> BackendIdentity {
        self.identity.clone()
    }

    fn revoke(self: Arc<Self>, _: Option<ExecutionClaim>, _: Option<ExecutionClaim>, _: Vec<ExecutionPendingOperation>) -> HandlerFuture<()> {
        Box::pin(async { anyhow::bail!("this observation fixture supplies no fence") })
    }

    fn revoke_history(self: Arc<Self>, _: CellHistory, _: CellHistory, _: Option<ExecutionClaim>, _: Vec<ExecutionPendingOperation>) -> HandlerFuture<()> {
        Box::pin(async { anyhow::bail!("this observation fixture supplies no history fence") })
    }

    fn execute(self: Arc<Self>, claim: ExecutionClaim, operation: ExecutionOperation, _: Vec<u8>) -> HandlerFuture<ExecutionOutcome> {
        Box::pin(async move {
            self.executions.fetch_add(1, Ordering::AcqRel);
            *self.receipt.lock().unwrap() = Some((ExecutionPendingOperation { claim, operation }, ExecutionOutcome::Completed(serde_json::to_vec(&json!({"format":1,"value":null}))?)));
            self.committed.notify_one();
            std::future::pending().await
        })
    }

    fn reconcile(self: Arc<Self>, current: ExecutionClaim, original: ExecutionPendingOperation) -> HandlerFuture<Option<ExecutionOutcome>> {
        Box::pin(async move {
            ensure!(current.resource_key() == original.claim.resource_key(), "foreign receipt lookup");
            let receipt = self.receipt.lock().unwrap();
            let Some((known, outcome)) = receipt.as_ref() else {return Ok(None)};
            ensure!(known == &original, "original receipt identity changed");
            Ok(Some(outcome.clone()))
        })
    }
}

pub(crate) async fn reconciled_completion_is_counted_once() -> Result<()> {
    let identity = BackendIdentity { binding:"DOCKER".into(), provider:"docker-exec".into(), configuration_digest:hex::encode([7;32]) };
    let parameters = hex::encode([8;32]);
    let resource = hex::encode([9;32]);
    let claim = crate::docker_backend::claim(&identity, 11, 3, Some(&resource), &parameters)?;
    let current = crate::docker_backend::claim(&identity, 11, 4, Some(&resource), &parameters)?;
    let body = b"original-delete".to_vec();
    let operation = ExecutionOperation::new([12;16],ExecutionOperationKind::Delete,&body,u64::MAX)?;
    let original = ExecutionPendingOperation { claim:claim.clone(), operation:operation.clone() };
    let backend = Arc::new(LostReturn { identity, committed:Notify::new(), receipt:Mutex::new(None), executions:AtomicUsize::new(0) });
    let peer = Peer::new(backend.clone(), None, false);
    let mut pending = peer.clone().execute(claim,operation,body);
    tokio::time::timeout(Duration::from_secs(1), async {
        tokio::select! {
            result = &mut pending => anyhow::bail!("lost reply unexpectedly returned: {result:?}"),
            _ = backend.committed.notified() => Ok(()),
        }
    }).await??;
    ensure!(peer.deletes.load(Ordering::Acquire)==0, "no execute reply was delivered");
    drop(pending);
    for _ in 0..3 {
        ensure!(matches!(peer.clone().reconcile(current.clone(),original.clone()).await?,Some(ExecutionOutcome::Completed(_))));
        ensure!(peer.deletes.load(Ordering::Acquire)==1, "original completed Delete was not counted exactly once after reconciliation");
    }
    ensure!(backend.executions.load(Ordering::Acquire)==1,"receipt lookup replayed provider work");
    let mut conflicting = original;
    conflicting.operation.payload_digest[0]^=1;
    ensure!(peer.clone().reconcile(current,conflicting).await.is_err());
    ensure!(peer.deletes.load(Ordering::Acquire)==1);
    Ok(())
}
