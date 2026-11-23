// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A provider can retain a known outcome before its caller receives the reply.
use super::*;

pub(crate) async fn archive_cut_uses_group_keys() -> Result<()> {
    use object_store::{path::Path, ObjectStore};

    let store = catalog_fault::Store::default();
    let parent = Path::from("managed/parent/cell-group-archives/checkpoint/group");
    *store.sandbox_parent_archive_prefix.lock().unwrap() = Some(parent.clone());
    store
        .deny_sandbox_parent_writes
        .store(true, Ordering::Release);
    // Neither ordinary extent bytes nor control records contain the old marker.
    // Every write in the selected group must still fail.
    for suffix in ["chunks/extent", "indexes/manifest", "registry"] {
        ensure!(store
            .put(&parent.child(suffix), b"opaque delta".to_vec().into())
            .await
            .is_err());
    }
    ensure!(store.denied_sandbox_parent_writes.load(Ordering::Acquire) == 3);
    let marker = b"{\"scope\":\"Counter:sandbox-floor\"}";
    let child = Path::from("managed/child/cell-group-archives/checkpoint/group/chunks/data");
    store.put(&child, marker.to_vec().into()).await?;
    let neighbor = Path::from("managed/parent/cell-group-archives/checkpoint/group-other/registry");
    store.put(&neighbor, marker.to_vec().into()).await?;
    ensure!(store.get(&child).await?.bytes().await?.as_ref() == marker);
    ensure!(store.denied_sandbox_parent_writes.load(Ordering::Acquire) == 3);
    store
        .deny_sandbox_parent_writes
        .store(false, Ordering::Release);
    let restored = parent.child("chunks/extent");
    store
        .put(&restored, b"restored delta".to_vec().into())
        .await?;
    ensure!(store.get(&restored).await?.bytes().await?.as_ref() == b"restored delta");
    Ok(())
}

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

    fn revoke(
        self: Arc<Self>,
        _: Option<ExecutionClaim>,
        _: Option<ExecutionClaim>,
        _: Vec<ExecutionPendingOperation>,
    ) -> HandlerFuture<()> {
        Box::pin(async { anyhow::bail!("this observation fixture supplies no fence") })
    }

    fn revoke_history(
        self: Arc<Self>,
        _: CellHistory,
        _: CellHistory,
        _: Option<ExecutionClaim>,
        _: Vec<ExecutionPendingOperation>,
    ) -> HandlerFuture<()> {
        Box::pin(async { anyhow::bail!("this observation fixture supplies no history fence") })
    }

    fn execute(
        self: Arc<Self>,
        claim: ExecutionClaim,
        operation: ExecutionOperation,
        _: Vec<u8>,
    ) -> HandlerFuture<ExecutionOutcome> {
        Box::pin(async move {
            self.executions.fetch_add(1, Ordering::AcqRel);
            *self.receipt.lock().unwrap() = Some((
                ExecutionPendingOperation { claim, operation },
                ExecutionOutcome::Completed(serde_json::to_vec(&json!({"format":1,"value":null}))?),
            ));
            self.committed.notify_one();
            std::future::pending().await
        })
    }

    fn reconcile(
        self: Arc<Self>,
        current: ExecutionClaim,
        original: ExecutionPendingOperation,
    ) -> HandlerFuture<Option<ExecutionOutcome>> {
        Box::pin(async move {
            ensure!(
                current.resource_key() == original.claim.resource_key(),
                "foreign receipt lookup"
            );
            let receipt = self.receipt.lock().unwrap();
            let Some((known, outcome)) = receipt.as_ref() else {
                return Ok(None);
            };
            ensure!(known == &original, "original receipt identity changed");
            Ok(Some(outcome.clone()))
        })
    }
}

struct Scenario {
    backend: Arc<LostReturn>,
    peer: Arc<Peer>,
    current: ExecutionClaim,
    original: ExecutionPendingOperation,
}

impl Scenario {
    fn new() -> Result<Self> {
        let identity = BackendIdentity {
            binding: "DOCKER".into(),
            provider: "docker-exec".into(),
            configuration_digest: hex::encode([7; 32]),
        };
        let parameters = hex::encode([8; 32]);
        let resource = hex::encode([9; 32]);
        let claim = crate::docker_backend::claim(&identity, 11, 3, Some(&resource), &parameters)?;
        let current = crate::docker_backend::claim(&identity, 11, 4, Some(&resource), &parameters)?;
        let operation = ExecutionOperation::new(
            [12; 16],
            ExecutionOperationKind::Delete,
            b"original-delete",
            u64::MAX,
        )?;
        let original = ExecutionPendingOperation { claim, operation };
        let backend = Arc::new(LostReturn {
            identity,
            committed: Notify::new(),
            receipt: Mutex::new(None),
            executions: AtomicUsize::new(0),
        });
        let peer = Peer::new(backend.clone(), None, false);
        Ok(Self {
            backend,
            peer,
            current,
            original,
        })
    }

    async fn lose_return(&self) -> Result<()> {
        let mut pending = self.peer.clone().execute(
            self.original.claim.clone(),
            self.original.operation.clone(),
            b"original-delete".to_vec(),
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            cellar::asyncrt::select! {
                result = &mut pending => anyhow::bail!("lost reply unexpectedly returned: {result:?}"),
                _ = self.backend.committed.notified() => Ok(()),
            }
        }).await??;
        ensure!(
            self.peer.deletes.load(Ordering::Acquire) == 0,
            "no execute reply was delivered"
        );
        drop(pending);
        Ok(())
    }
}

pub(crate) async fn reconciled_completion_is_counted_once() -> Result<()> {
    let s = Scenario::new()?;
    ensure!(s
        .peer
        .clone()
        .reconcile(s.current.clone(), s.original.clone())
        .await?
        .is_none());
    ensure!(s.peer.deletes.load(Ordering::Acquire) == 0);
    s.lose_return().await?;
    for _ in 0..3 {
        ensure!(matches!(
            s.peer
                .clone()
                .reconcile(s.current.clone(), s.original.clone())
                .await?,
            Some(ExecutionOutcome::Completed(_))
        ));
        ensure!(
            s.peer.deletes.load(Ordering::Acquire) == 1,
            "original completed Delete was not counted exactly once after reconciliation"
        );
    }
    ensure!(
        s.backend.executions.load(Ordering::Acquire) == 1,
        "receipt lookup replayed provider work"
    );
    let known = s.backend.receipt.lock().unwrap().clone().unwrap();
    let mut conflicting = s.original.clone();
    conflicting.operation.payload_digest[0] ^= 1;
    *s.backend.receipt.lock().unwrap() = Some((conflicting.clone(), known.1.clone()));
    ensure!(
        s.peer
            .clone()
            .reconcile(s.current.clone(), conflicting)
            .await
            .is_err(),
        "observer accepted changed original metadata"
    );
    *s.backend.receipt.lock().unwrap() = Some((
        s.original.clone(),
        ExecutionOutcome::Completed(b"changed result".to_vec()),
    ));
    ensure!(
        s.peer
            .clone()
            .reconcile(s.current.clone(), s.original.clone())
            .await
            .is_err(),
        "observer accepted changed original result bytes"
    );
    for (id, outcome) in [
        (13, ExecutionOutcome::Refused),
        (14, ExecutionOutcome::ExpiredBeforeSubmission),
    ] {
        let mut original = s.original.clone();
        original.operation.id = [id; 16];
        *s.backend.receipt.lock().unwrap() = Some((original.clone(), outcome.clone()));
        ensure!(
            s.peer
                .clone()
                .reconcile(s.current.clone(), original)
                .await?
                == Some(outcome)
        );
        ensure!(
            s.peer.deletes.load(Ordering::Acquire) == 1,
            "non-completion counted as physical work"
        );
    }
    Ok(())
}

pub(crate) async fn reconciled_first_reply_keeps_the_loss_cut() -> Result<()> {
    let mut s = Scenario::new()?;
    let store = Arc::new(catalog_fault::Store::default());
    s.peer = Peer::new(s.backend.clone(), Some(store.clone()), false);
    s.peer.lose_delete.store(true, Ordering::Release);
    s.peer.hide_delete.store(true, Ordering::Release);
    s.lose_return().await?;
    let mut pending = s
        .peer
        .clone()
        .reconcile(s.current.clone(), s.original.clone());
    tokio::time::timeout(Duration::from_secs(1),async {
        cellar::asyncrt::select! {
            result=&mut pending=>anyhow::bail!("first reconciled completion escaped the lost-reply cut: {result:?}"),
            _=s.peer.wait_for_delete_cut()=>Ok(()),
        }
    }).await??;
    drop(pending);
    ensure!(s.peer.original_delete.lock().unwrap().as_ref() == Some(&s.original));
    ensure!(s.peer.deletes.load(Ordering::Acquire) == 1);
    ensure!(store.deny_sandbox_parent_writes.load(Ordering::Acquire));
    ensure!(
        s.peer
            .clone()
            .reconcile(s.current.clone(), s.original.clone())
            .await?
            .is_none(),
        "hidden original receipt escaped its cut"
    );
    s.peer.hide_delete.store(false, Ordering::Release);
    store
        .deny_sandbox_parent_writes
        .store(false, Ordering::Release);
    for _ in 0..3 {
        ensure!(matches!(
            s.peer
                .clone()
                .reconcile(s.current.clone(), s.original.clone())
                .await?,
            Some(ExecutionOutcome::Completed(_))
        ));
        ensure!(
            s.peer.deletes.load(Ordering::Acquire) == 1,
            "original receipt was counted twice"
        );
    }
    ensure!(s.backend.executions.load(Ordering::Acquire) == 1);
    ensure!(
        !store.deny_sandbox_parent_writes.load(Ordering::Acquire),
        "receipt replay rearmed the restored archive fault"
    );
    Ok(())
}
