// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independent controller persistence/protocol tests with trusted metadata.
//! These do not establish a Raft quorum, authenticate an RPC transport, or
//! qualify the normal native Container adapter.
use anyhow::{ensure, Result};
use cellar::application::consensus::verification::cell_groups::cell_image_history_digest;
use cellar::sdk::{consensus::*, containers::*, ExecutionDomain};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::Notify;

#[path = "support/execution_controller_durability.rs"]
mod durability;

struct Provider {
    active: Mutex<Option<ExecutionClaim>>,
    results: Mutex<BTreeMap<[u8; 16], Vec<u8>>>,
    calls: AtomicUsize,
    observations: AtomicUsize,
    revocations: AtomicUsize,
    hold: AtomicBool,
    unavailable: AtomicBool,
    forget_result: AtomicBool,
    entered: Notify,
    released: Notify,
}

impl Default for Provider {
    fn default() -> Self {
        Self {
            active: Mutex::new(None),
            results: Mutex::new(BTreeMap::new()),
            calls: AtomicUsize::new(0),
            observations: AtomicUsize::new(0),
            revocations: AtomicUsize::new(0),
            hold: AtomicBool::new(false),
            unavailable: AtomicBool::new(false),
            forget_result: AtomicBool::new(false),
            entered: Notify::new(),
            released: Notify::new(),
        }
    }
}

fn backend_identity() -> BackendIdentity {
    BackendIdentity {
        binding: "TOOL".into(),
        provider: "fixture".into(),
        configuration_digest: hex::encode([17; 32]),
    }
}

impl ExecutionControllerBackend for Provider {
    fn identity(&self) -> BackendIdentity {
        backend_identity()
    }

    fn revoke(
        self: Arc<Self>,
        _previous: Option<ExecutionClaim>,
        successor: Option<ExecutionClaim>,
        _unresolved: Vec<ExecutionPendingOperation>,
    ) -> cellar::sdk::HandlerFuture<()> {
        Box::pin(async move {
            ensure!(
                !self.unavailable.load(Ordering::Acquire),
                "fixture revocation unavailable"
            );
            self.revocations.fetch_add(1, Ordering::AcqRel);
            *self.active.lock().unwrap() = successor;
            Ok(())
        })
    }

    fn revoke_history(
        self: Arc<Self>,
        _old: CellHistory,
        _successor: CellHistory,
        previous: Option<ExecutionClaim>,
        unresolved: Vec<ExecutionPendingOperation>,
    ) -> cellar::sdk::HandlerFuture<()> {
        // Trusted infrastructure model only; physical fencing needs its own drill.
        self.revoke(previous, None, unresolved)
    }

    fn execute(
        self: Arc<Self>,
        claim: ExecutionClaim,
        operation: ExecutionOperation,
        payload: Vec<u8>,
    ) -> cellar::sdk::HandlerFuture<ExecutionOutcome> {
        Box::pin(async move {
            ensure!(
                self.active.lock().unwrap().as_ref() == Some(&claim),
                "stale fixture execution"
            );
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.results
                .lock()
                .unwrap()
                .insert(operation.id, payload.clone());
            let held = self.hold.load(Ordering::Acquire);
            self.entered.notify_one();
            if held {
                self.released.notified().await;
            }
            Ok(ExecutionOutcome::Completed(payload))
        })
    }

    fn reconcile(
        self: Arc<Self>,
        current: ExecutionClaim,
        original: ExecutionPendingOperation,
    ) -> cellar::sdk::HandlerFuture<Option<ExecutionOutcome>> {
        Box::pin(async move {
            ensure!(
                self.active.lock().unwrap().as_ref() == Some(&current),
                "stale fixture reconciliation"
            );
            self.observations.fetch_add(1, Ordering::AcqRel);
            if self.forget_result.load(Ordering::Acquire) {
                return Ok(None);
            }
            Ok(self
                .results
                .lock()
                .unwrap()
                .get(&original.operation.id)
                .cloned()
                .map(ExecutionOutcome::Completed))
        })
    }
}

fn options(root: PathBuf) -> ExecutionControllerOptions {
    ExecutionControllerOptions {
        directory: root,
        controller_id: [0; 16],
        backend: backend_identity(),
        limits: ExecutionControllerLimits::default(),
    }
}

fn domain() -> Arc<ExecutionDomain> {
    Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()))
}

fn history(incarnation: u8) -> CellHistory {
    CellHistory {
        cluster: ClusterId::new([1; 16]).unwrap(),
        namespace: NamespaceId::new([2; 16]).unwrap(),
        cell: CellId::new("Counter:controller").unwrap(),
        incarnation: IncarnationId::new([incarnation; 16]).unwrap(),
    }
}

fn claim(
    identity: &BackendIdentity,
    incarnation: u8,
    sequence: u64,
    revision: u64,
) -> Result<ExecutionClaim> {
    let history = history(incarnation);
    let mut key = Sha256::new();
    key.update(b"cellar-provider-execution-resource-1");
    key.update(cell_image_history_digest(&history));
    key.update((identity.binding.len() as u16).to_be_bytes());
    key.update(identity.binding.as_bytes());
    let key: [u8; 32] = key.finalize().into();
    // The test driver represents a trusted authenticated host, not a quorum
    // proof. Production receives this metadata only after the SDK source gate.
    Ok(serde_json::from_value(serde_json::json!({
        "format":1,"cluster":history.cluster.bytes(),"namespace":history.namespace.bytes(),
        "cell":history.cell.as_str(),"incarnation":history.incarnation.bytes(),"binding":identity.binding,
        "key":key,"sequence":sequence,"owner":hex::encode([sequence as u8;32]),
        "checkpoint":{"revision":revision,"activation_epoch":1,"record":{
            "provider":identity.provider,"configuration_digest":identity.configuration_digest,
            "parameters_digest":hex::encode([18;32]),"generation":1,
            "request_id":hex::encode([19;16]),"resource_id":null,"intent":"running","observation":"pending"
        }}
    }))?)
}

async fn operation(domain: &ExecutionDomain, id: u8, payload: &[u8]) -> Result<ExecutionOperation> {
    let expires = domain
        .run(async { u64::try_from(cellar::asyncrt::wall_ms()).unwrap() + 5000 })
        .await?;
    ExecutionOperation::new([id; 16], ExecutionOperationKind::Execute, payload, expires)
}

fn root() -> Result<PathBuf> {
    let root = tempfile::tempdir()?.keep();
    eprintln!("retained independent controller root: {}", root.display());
    Ok(root.join("controller"))
}

fn failure(error: &anyhow::Error, expected: ExecutionControllerFailure) {
    assert_eq!(
        error.downcast_ref::<ExecutionControllerFailure>(),
        Some(&expected),
        "{error:#}"
    );
}

#[test]
fn execution_claim_metadata_codec_validates_identity_and_format() -> Result<()> {
    let c = claim(&backend_identity(), 3, 1, 1)?;
    let bytes = serde_json::to_vec(&c)?;
    assert_eq!(serde_json::from_slice::<ExecutionClaim>(&bytes)?, c);
    for field in ["key", "cluster", "namespace", "incarnation"] {
        let mut value = serde_json::to_value(&c)?;
        value[field] = serde_json::json!(vec![0u8; if field == "key" { 32 } else { 16 }]);
        assert!(serde_json::from_value::<ExecutionClaim>(value).is_err());
    }
    let mut value = serde_json::to_value(&c)?;
    value["format"] = serde_json::json!(2);
    assert!(serde_json::from_value::<ExecutionClaim>(value).is_err());
    let mut value = serde_json::to_value(&c)?;
    value["owner"] = serde_json::json!("untrusted");
    assert!(serde_json::from_value::<ExecutionClaim>(value).is_err());
    let mut value = serde_json::to_value(&c)?;
    value["extra"] = serde_json::json!(true);
    assert!(serde_json::from_value::<ExecutionClaim>(value).is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retired_fences_require_known_outcomes_and_never_authorize_execution() -> Result<()> {
    let d = domain();
    let backend = Arc::new(Provider::default());
    backend.hold.store(true, Ordering::Release);
    let controller =
        ExecutionController::create(d.clone(), options(root()?), backend.clone(), [42; 32]).await?;
    let resume = controller.options();
    let current = claim(&controller.identity(), 3, 1, 1)?;
    controller.clone().activate(current.clone()).await?;
    let request = operation(&d, 22, b"terminal effect").await?;
    failure(
        &controller
            .execute(
                current.clone(),
                request.clone(),
                b"terminal effect".to_vec(),
            )
            .await
            .unwrap_err(),
        ExecutionControllerFailure::UnknownOperation,
    );
    assert_eq!(backend.calls.load(Ordering::Acquire), 1);
    let original = controller.pending(current.clone()).await?.remove(0);
    let mut value = serde_json::to_value(&current)?;
    value["sequence"] = serde_json::json!(2);
    value["checkpoint"]["revision"] = serde_json::json!(2);
    value["checkpoint"]["record"]["intent"] = serde_json::json!("deleted");
    value["checkpoint"]["record"]["observation"] = serde_json::json!("confirmed");
    let retired: ExecutionClaim = serde_json::from_value(value)?;
    failure(
        &controller
            .clone()
            .activate(retired.clone())
            .await
            .unwrap_err(),
        ExecutionControllerFailure::UnknownOperation,
    );
    assert_eq!(backend.revocations.load(Ordering::Acquire), 1);
    assert_eq!(
        controller.pending(current.clone()).await?,
        vec![original.clone()]
    );
    backend.hold.store(false, Ordering::Release);
    backend.released.notify_waiters();
    assert_eq!(
        controller.reconcile(current.clone(), original).await?,
        ExecutionOutcome::Completed(b"terminal effect".to_vec())
    );
    controller.clone().activate(retired.clone()).await?;
    let forbidden = operation(&d, 23, b"forbidden").await?;
    failure(
        &controller
            .execute(retired.clone(), forbidden, b"forbidden".to_vec())
            .await
            .unwrap_err(),
        ExecutionControllerFailure::StaleClaim,
    );
    assert_eq!(backend.calls.load(Ordering::Acquire), 1);
    let mut reopened = serde_json::to_value(&current)?;
    reopened["sequence"] = serde_json::json!(3);
    reopened["checkpoint"]["revision"] = serde_json::json!(3);
    let reopened: ExecutionClaim = serde_json::from_value(reopened)?;
    failure(
        &controller.clone().activate(reopened).await.unwrap_err(),
        ExecutionControllerFailure::ConflictingRequest,
    );
    controller.shutdown().await?;
    d.shutdown().await;
    let d2 = domain();
    let restored_backend = Arc::new(Provider::default());
    let restored = ExecutionController::open(d2.clone(), resume, restored_backend.clone()).await?;
    restored.clone().activate(retired.clone()).await?;
    assert_eq!(restored_backend.revocations.load(Ordering::Acquire), 1);
    assert_eq!(
        restored_backend.active.lock().unwrap().as_ref(),
        Some(&retired)
    );
    failure(
        &restored
            .clone()
            .activate(current.clone())
            .await
            .unwrap_err(),
        ExecutionControllerFailure::StaleClaim,
    );
    let forbidden = operation(&d2, 24, b"forbidden").await?;
    failure(
        &restored
            .execute(retired.clone(), forbidden, b"forbidden".to_vec())
            .await
            .unwrap_err(),
        ExecutionControllerFailure::StaleClaim,
    );
    assert_eq!(restored_backend.calls.load(Ordering::Acquire), 0);
    let mut next = serde_json::to_value(&current)?;
    next["sequence"] = serde_json::json!(3);
    next["checkpoint"]["revision"] = serde_json::json!(3);
    next["checkpoint"]["record"]["generation"] = serde_json::json!(2);
    next["checkpoint"]["record"]["request_id"] = serde_json::json!(hex::encode([20; 16]));
    let next: ExecutionClaim = serde_json::from_value(next)?;
    restored.clone().activate(next.clone()).await?;
    let fresh = operation(&d2, 25, b"new generation").await?;
    assert_eq!(
        restored
            .execute(next, fresh, b"new generation".to_vec())
            .await?,
        ExecutionOutcome::Completed(b"new generation".to_vec())
    );
    assert_eq!(restored_backend.calls.load(Ordering::Acquire), 1);
    restored.shutdown().await?;
    d2.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_restart_retains_high_water_and_original_receipts() -> Result<()> {
    let d = domain();
    let backend = Arc::new(Provider::default());
    let controller =
        ExecutionController::create(d.clone(), options(root()?), backend.clone(), [42; 32]).await?;
    let resume = controller.options();
    assert_ne!(resume.controller_id, [0; 16]);
    assert_ne!(
        controller.identity().configuration_digest,
        backend_identity().configuration_digest
    );
    let old = claim(&controller.identity(), 3, 4, 1)?;
    controller.clone().activate(old.clone()).await?;
    let request = operation(&d, 21, b"one effect").await?;
    assert_eq!(
        controller
            .execute(old.clone(), request.clone(), b"one effect".to_vec())
            .await?,
        ExecutionOutcome::Completed(b"one effect".to_vec())
    );
    let new = claim(&controller.identity(), 3, 5, 1)?;
    controller.clone().activate(new.clone()).await?;
    failure(
        &controller.clone().activate(old.clone()).await.unwrap_err(),
        ExecutionControllerFailure::StaleClaim,
    );
    controller.shutdown().await?;
    d.shutdown().await;
    let d2 = domain();
    let restarted_backend = Arc::new(Provider::default());
    let reopened = ExecutionController::open(d2.clone(), resume, restarted_backend.clone()).await?;
    failure(
        &reopened.clone().activate(old.clone()).await.unwrap_err(),
        ExecutionControllerFailure::StaleClaim,
    );
    assert_eq!(
        reopened
            .execute(new.clone(), request.clone(), b"one effect".to_vec())
            .await?,
        ExecutionOutcome::Completed(b"one effect".to_vec())
    );
    assert_eq!(backend.calls.load(Ordering::Acquire), 1);
    let mut conflicting = request;
    conflicting.payload_digest = [31; 32];
    failure(
        &reopened
            .execute(new.clone(), conflicting, b"one effect".to_vec())
            .await
            .unwrap_err(),
        ExecutionControllerFailure::ConflictingRequest,
    );
    assert_eq!(
        restarted_backend.calls.load(Ordering::Acquire),
        0,
        "cached results must survive loss of the backend's process memory"
    );
    reopened
        .execute(
            new,
            operation(&d2, 37, b"fresh backend effect").await?,
            b"fresh backend effect".to_vec(),
        )
        .await?;
    assert_eq!(
        restarted_backend.calls.load(Ordering::Acquire),
        1,
        "startup must reassert the provider fence before new work"
    );
    assert!(controller.clone().activate(old).await.is_err());
    reopened.shutdown().await?;
    d2.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn controller_recovers_accepted_reply_loss_without_reexecution() -> Result<()> {
    let d = domain();
    let backend = Arc::new(Provider::default());
    let controller =
        ExecutionController::create(d.clone(), options(root()?), backend.clone(), [42; 32]).await?;
    let resume = controller.options();
    let c = claim(&controller.identity(), 3, 1, 1)?;
    controller.clone().activate(c.clone()).await?;
    let request = operation(&d, 22, b"accepted result").await?;
    backend.hold.store(true, Ordering::Release);
    let mut pending =
        Box::pin(controller.execute(c.clone(), request.clone(), b"accepted result".to_vec()));
    let entered = Box::pin(backend.entered.notified());
    match futures_util::future::select(pending.as_mut(), entered).await {
        futures_util::future::Either::Left((result, _)) => {
            anyhow::bail!("operation returned before held reply: {result:?}")
        }
        futures_util::future::Either::Right(_) => {}
    }
    drop(pending);
    assert_eq!(backend.calls.load(Ordering::Acquire), 1);
    assert_eq!(controller.pending(c.clone()).await?.len(), 1);
    failure(
        &controller
            .execute(c.clone(), request.clone(), b"accepted result".to_vec())
            .await
            .unwrap_err(),
        ExecutionControllerFailure::UnknownOperation,
    );
    controller.shutdown().await?;
    d.shutdown().await;
    backend.hold.store(false, Ordering::Release);
    let d2 = domain();
    let reopened = ExecutionController::open(d2.clone(), resume, backend.clone()).await?;
    assert!(reopened.pending(c.clone()).await?.is_empty());
    assert_eq!(
        reopened
            .execute(c, request, b"accepted result".to_vec())
            .await?,
        ExecutionOutcome::Completed(b"accepted result".to_vec())
    );
    assert_eq!(backend.calls.load(Ordering::Acquire), 1);
    assert_eq!(backend.observations.load(Ordering::Acquire), 1);
    reopened.shutdown().await?;
    d2.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unresolved_operations_refuse_new_mutations_until_explicit_reconciliation() -> Result<()> {
    let d = domain();
    let backend = Arc::new(Provider::default());
    let controller =
        ExecutionController::create(d.clone(), options(root()?), backend.clone(), [42; 32]).await?;
    let c = claim(&controller.identity(), 3, 1, 1)?;
    controller.clone().activate(c.clone()).await?;
    let request = operation(&d, 23, b"unknown").await?;
    backend.hold.store(true, Ordering::Release);
    let mut pending = Box::pin(controller.execute(c.clone(), request.clone(), b"unknown".to_vec()));
    match futures_util::future::select(pending.as_mut(), Box::pin(backend.entered.notified())).await
    {
        futures_util::future::Either::Left(_) => anyhow::bail!("lost-reply cut was not entered"),
        futures_util::future::Either::Right(_) => {}
    }
    drop(pending);
    backend.forget_result.store(true, Ordering::Release);
    let mut successor = serde_json::to_value(claim(&controller.identity(), 3, 2, 2)?)?;
    successor["checkpoint"]["record"]["generation"] = serde_json::json!(2);
    successor["checkpoint"]["record"]["request_id"] = serde_json::json!(hex::encode([29; 16]));
    let c: ExecutionClaim = serde_json::from_value(successor)?;
    controller.clone().activate(c.clone()).await?;
    let original = controller.pending(c.clone()).await?.pop().unwrap();
    assert_eq!(original.claim.checkpoint().record.generation, 1);
    assert_eq!(c.checkpoint().record.generation, 2);
    failure(
        &controller
            .execute(c.clone(), request.clone(), b"unknown".to_vec())
            .await
            .unwrap_err(),
        ExecutionControllerFailure::ConflictingRequest,
    );
    failure(
        &controller
            .reconcile(c.clone(), original.clone())
            .await
            .unwrap_err(),
        ExecutionControllerFailure::UnknownOperation,
    );
    let next = operation(&d, 24, b"another effect").await?;
    failure(
        &controller
            .execute(c.clone(), next.clone(), b"another effect".to_vec())
            .await
            .unwrap_err(),
        ExecutionControllerFailure::UnknownOperation,
    );
    assert_eq!(backend.calls.load(Ordering::Acquire), 1);
    backend.forget_result.store(false, Ordering::Release);
    backend.hold.store(false, Ordering::Release);
    assert_eq!(
        controller.reconcile(c.clone(), original).await?,
        ExecutionOutcome::Completed(b"unknown".to_vec())
    );
    controller
        .execute(c, next, b"another effect".to_vec())
        .await?;
    assert_eq!(backend.calls.load(Ordering::Acquire), 2);
    controller.shutdown().await?;
    d.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_registration_retains_high_water_when_backend_activation_fails() -> Result<()> {
    let d = domain();
    let backend = Arc::new(Provider::default());
    backend.unavailable.store(true, Ordering::Release);
    let controller =
        ExecutionController::create(d.clone(), options(root()?), backend.clone(), [43; 32]).await?;
    let resume = controller.options();
    let first = claim(&controller.identity(), 3, 2, 1)?;
    let stale = claim(&controller.identity(), 3, 1, 1)?;
    failure(
        &controller
            .clone()
            .activate(first.clone())
            .await
            .unwrap_err(),
        ExecutionControllerFailure::RecoveryPending,
    );
    failure(
        &controller
            .clone()
            .activate(stale.clone())
            .await
            .unwrap_err(),
        ExecutionControllerFailure::StaleClaim,
    );
    assert_eq!(backend.calls.load(Ordering::Acquire), 0);
    controller.shutdown().await?;
    d.shutdown().await;

    let unavailable = domain();
    assert!(
        ExecutionController::open(unavailable.clone(), resume.clone(), backend.clone())
            .await
            .is_err()
    );
    unavailable.shutdown().await;
    backend.unavailable.store(false, Ordering::Release);
    let recovered_domain = domain();
    let reopened =
        ExecutionController::open(recovered_domain.clone(), resume, backend.clone()).await?;
    failure(
        &reopened.clone().activate(stale).await.unwrap_err(),
        ExecutionControllerFailure::StaleClaim,
    );
    reopened.clone().activate(first.clone()).await?;
    assert_eq!(backend.revocations.load(Ordering::Acquire), 1);
    assert_eq!(backend.calls.load(Ordering::Acquire), 0);
    let payload = b"after exact fence recovery";
    assert_eq!(
        reopened
            .execute(
                first,
                operation(&recovered_domain, 33, payload).await?,
                payload.to_vec()
            )
            .await?,
        ExecutionOutcome::Completed(payload.to_vec())
    );
    assert_eq!(backend.calls.load(Ordering::Acquire), 1);
    reopened.shutdown().await?;
    recovered_domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_high_water_refuses_old_claims_across_failed_and_completed_startup() -> Result<()> {
    let d = domain();
    let backend = Arc::new(Provider::default());
    let controller =
        ExecutionController::create(d.clone(), options(root()?), backend.clone(), [42; 32]).await?;
    let resume = controller.options();
    let old = claim(&controller.identity(), 3, 1, 1)?;
    let successor = claim(&controller.identity(), 3, 2, 2)?;
    controller.clone().activate(old.clone()).await?;
    backend.unavailable.store(true, Ordering::Release);
    failure(
        &controller
            .clone()
            .activate(successor.clone())
            .await
            .unwrap_err(),
        ExecutionControllerFailure::RecoveryPending,
    );
    failure(
        &controller
            .execute(
                old.clone(),
                operation(&d, 30, b"old").await?,
                b"old".to_vec(),
            )
            .await
            .unwrap_err(),
        ExecutionControllerFailure::StaleClaim,
    );
    controller.shutdown().await?;
    d.shutdown().await;
    let failed_domain = domain();
    assert!(
        ExecutionController::open(failed_domain.clone(), resume.clone(), backend.clone())
            .await
            .is_err()
    );
    failed_domain.shutdown().await;
    backend.unavailable.store(false, Ordering::Release);
    let d2 = domain();
    let reopened = ExecutionController::open(d2.clone(), resume, backend.clone()).await?;
    failure(
        &reopened.clone().activate(old).await.unwrap_err(),
        ExecutionControllerFailure::StaleClaim,
    );
    reopened.clone().activate(successor.clone()).await?;
    reopened
        .execute(
            successor,
            operation(&d2, 31, b"new").await?,
            b"new".to_vec(),
        )
        .await?;
    assert_eq!(backend.calls.load(Ordering::Acquire), 1);
    reopened.shutdown().await?;
    d2.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fresh_incarnation_requires_persisted_permanent_revocation() -> Result<()> {
    let d = domain();
    let backend = Arc::new(Provider::default());
    let controller =
        ExecutionController::create(d.clone(), options(root()?), backend.clone(), [42; 32]).await?;
    let resume = controller.options();
    let old = claim(&controller.identity(), 3, 8, 1)?;
    controller.clone().activate(old.clone()).await?;
    let successor = claim(&controller.identity(), 4, 1, 1)?;
    failure(
        &controller
            .clone()
            .activate(successor.clone())
            .await
            .unwrap_err(),
        ExecutionControllerFailure::RevokedHistory,
    );
    failure(
        &controller
            .recover_history(history(3), history(4), [41; 32])
            .await
            .unwrap_err(),
        ExecutionControllerFailure::RevokedHistory,
    );
    backend.unavailable.store(true, Ordering::Release);
    failure(
        &controller
            .recover_history(history(3), history(4), [42; 32])
            .await
            .unwrap_err(),
        ExecutionControllerFailure::RecoveryPending,
    );
    assert!(controller.clone().activate(old.clone()).await.is_err());
    failure(
        &controller
            .clone()
            .activate(successor.clone())
            .await
            .unwrap_err(),
        ExecutionControllerFailure::RecoveryPending,
    );
    controller.shutdown().await?;
    d.shutdown().await;
    backend.unavailable.store(false, Ordering::Release);
    let d2 = domain();
    let reopened = ExecutionController::open(d2.clone(), resume, backend).await?;
    reopened.clone().activate(successor).await?;
    let very_high_old = claim(&reopened.identity(), 3, 999, 99)?;
    failure(
        &reopened.clone().activate(very_high_old).await.unwrap_err(),
        ExecutionControllerFailure::RevokedHistory,
    );
    failure(
        &reopened.clone().activate(old).await.unwrap_err(),
        ExecutionControllerFailure::RevokedHistory,
    );
    reopened
        .recover_history(history(3), history(4), [42; 32])
        .await?;
    reopened.shutdown().await?;
    d2.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn known_missing_state_and_new_controller_identity_cannot_reset_ownership() -> Result<()> {
    let directory = root()?;
    let d = domain();
    let backend = Arc::new(Provider::default());
    let controller = ExecutionController::create(
        d.clone(),
        options(directory.clone()),
        backend.clone(),
        [42; 32],
    )
    .await?;
    let resume = controller.options();
    let old = claim(&controller.identity(), 3, 1, 1)?;
    controller.clone().activate(old.clone()).await?;
    let competing_domain = domain();
    assert!(
        ExecutionController::open(competing_domain.clone(), resume.clone(), backend.clone())
            .await
            .is_err()
    );
    competing_domain.shutdown().await;
    controller.shutdown().await?;
    d.shutdown().await;
    let operator = domain();
    let cells = directory.join("cells");
    operator
        .blocking(move || {
            let fs = cellar::asyncrt::fs();
            let entries = fs.read_dir(&cells)?;
            assert_eq!(entries.len(), 1);
            fs.remove_file(&entries[0].path)
        })?
        .await??;
    let d2 = domain();
    assert!(
        ExecutionController::open(d2.clone(), resume, backend.clone())
            .await
            .is_err()
    );
    let fresh =
        ExecutionController::create(d2.clone(), options(root()?), backend, [42; 32]).await?;
    assert_ne!(fresh.identity(), controller.identity());
    failure(
        &fresh.clone().activate(old).await.unwrap_err(),
        ExecutionControllerFailure::ConflictingRequest,
    );
    fresh.shutdown().await?;
    d2.shutdown().await;
    operator.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn higher_sequences_cannot_rewind_binding_checkpoints() -> Result<()> {
    let d = domain();
    let backend = Arc::new(Provider::default());
    let controller =
        ExecutionController::create(d.clone(), options(root()?), backend, [42; 32]).await?;
    let first = claim(&controller.identity(), 3, 1, 1)?;
    controller.clone().activate(first).await?;
    let next = claim(&controller.identity(), 3, 2, 3)?;
    controller.clone().activate(next).await?;
    failure(
        &controller
            .clone()
            .activate(claim(&controller.identity(), 3, 3, 2)?)
            .await
            .unwrap_err(),
        ExecutionControllerFailure::StaleClaim,
    );
    controller.shutdown().await?;
    d.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_operations_and_capacity_refuse_before_backend_entry() -> Result<()> {
    let d = domain();
    let backend = Arc::new(Provider::default());
    let mut config = options(root()?);
    config.limits.max_operations_per_incarnation = 1;
    let controller =
        ExecutionController::create(d.clone(), config, backend.clone(), [42; 32]).await?;
    let c = claim(&controller.identity(), 3, 1, 1)?;
    controller.clone().activate(c.clone()).await?;
    let request =
        ExecutionOperation::new([26; 16], ExecutionOperationKind::Execute, b"expired", 1)?;
    assert_eq!(
        controller
            .execute(c.clone(), request.clone(), b"expired".to_vec())
            .await?,
        ExecutionOutcome::ExpiredBeforeSubmission
    );
    let mut later = request;
    later.expires_unix_ms = operation(&d, 26, b"expired").await?.expires_unix_ms;
    assert_eq!(
        controller
            .execute(c.clone(), later, b"expired".to_vec())
            .await?,
        ExecutionOutcome::ExpiredBeforeSubmission
    );
    failure(
        &controller
            .execute(c, operation(&d, 27, b"new").await?, b"new".to_vec())
            .await
            .unwrap_err(),
        ExecutionControllerFailure::Capacity,
    );
    assert_eq!(backend.calls.load(Ordering::Acquire), 0);
    controller.shutdown().await?;
    d.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn receipt_space_is_reserved_before_effects_and_corruption_refuses_open() -> Result<()> {
    let d = domain();
    let backend = Arc::new(Provider::default());
    let directory = root()?;
    let mut config = options(directory.clone());
    config.limits.max_record_bytes = 16_384;
    config.limits.max_result_bytes = 16_384;
    let controller =
        ExecutionController::create(d.clone(), config, backend.clone(), [42; 32]).await?;
    let resume = controller.options();
    let c = claim(&controller.identity(), 3, 1, 1)?;
    controller.clone().activate(c.clone()).await?;
    failure(
        &controller
            .execute(c, operation(&d, 28, b"reserve").await?, b"reserve".to_vec())
            .await
            .unwrap_err(),
        ExecutionControllerFailure::Capacity,
    );
    assert_eq!(backend.calls.load(Ordering::Acquire), 0);
    controller.shutdown().await?;
    d.shutdown().await;
    let operator = domain();
    let state = directory.join("controller");
    operator
        .blocking(move || {
            let fs = cellar::asyncrt::fs();
            let mut bytes = fs.read(&state)?;
            let last = bytes.last_mut().unwrap();
            *last ^= 1;
            fs.write(&state, &bytes)
        })?
        .await??;
    let d2 = domain();
    assert!(ExecutionController::open(d2.clone(), resume, backend)
        .await
        .is_err());
    d2.shutdown().await;
    operator.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn operation_deadline_bounds_waiting_for_another_admitted_request() -> Result<()> {
    let d = domain();
    let backend = Arc::new(Provider::default());
    let controller =
        ExecutionController::create(d.clone(), options(root()?), backend.clone(), [42; 32]).await?;
    let c = claim(&controller.identity(), 3, 1, 1)?;
    controller.clone().activate(c.clone()).await?;
    backend.hold.store(true, Ordering::Release);
    let original = operation(&d, 35, b"held reply").await?;
    let mut first = Box::pin(controller.execute(c.clone(), original, b"held reply".to_vec()));
    match futures_util::future::select(first.as_mut(), Box::pin(backend.entered.notified())).await {
        futures_util::future::Either::Left(_) => {
            anyhow::bail!("held operation returned before the admission cut")
        }
        futures_util::future::Either::Right(_) => {}
    }
    let expires = d
        .run(async { u64::try_from(cellar::asyncrt::wall_ms()).unwrap() + 100 })
        .await?;
    let queued = ExecutionOperation::new(
        [36; 16],
        ExecutionOperationKind::Read,
        b"queued read",
        expires,
    )?;
    let result = d
        .run(async {
            cellar::asyncrt::timeout(
                std::time::Duration::from_secs(1),
                controller.execute(c.clone(), queued, b"queued read".to_vec()),
            )
            .await
        })
        .await?;
    drop(first);
    let pending = controller.pending(c).await?;
    let calls = backend.calls.load(Ordering::Acquire);
    controller.shutdown().await?;
    d.shutdown().await;
    assert_eq!(calls, 1, "queued request must never enter the backend");
    assert_eq!(
        pending.len(),
        1,
        "only the original submitted operation may remain"
    );
    let result =
        result.expect("controller must honor the original deadline while waiting for admission");
    failure(
        &result.unwrap_err(),
        ExecutionControllerFailure::UnknownOperation,
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovered_incarnation_retains_original_operation_debt_and_receipts() -> Result<()> {
    let d = domain();
    let backend = Arc::new(Provider::default());
    let controller =
        ExecutionController::create(d.clone(), options(root()?), backend.clone(), [42; 32]).await?;
    let resume = controller.options();
    let old = claim(&controller.identity(), 3, 1, 1)?;
    controller.clone().activate(old.clone()).await?;
    let mut request = operation(&d, 32, b"original resource").await?;
    request = ExecutionOperation::new(
        request.id,
        ExecutionOperationKind::Create,
        b"original resource",
        request.expires_unix_ms,
    )?;
    backend.hold.store(true, Ordering::Release);
    let mut first =
        Box::pin(controller.execute(old.clone(), request.clone(), b"original resource".to_vec()));
    match futures_util::future::select(first.as_mut(), Box::pin(backend.entered.notified())).await {
        futures_util::future::Either::Left(_) => anyhow::bail!("lost-create cut was not entered"),
        futures_util::future::Either::Right(_) => {}
    }
    drop(first);
    backend.hold.store(false, Ordering::Release);
    let original = controller.pending(old).await?.pop().unwrap();
    controller
        .recover_history(history(3), history(4), [42; 32])
        .await?;
    let successor = claim(&controller.identity(), 4, 1, 1)?;
    controller.clone().activate(successor.clone()).await?;
    let inherited = controller.pending(successor.clone()).await?;
    let replay = controller
        .execute(successor.clone(), request, b"original resource".to_vec())
        .await;
    let blocked = controller
        .execute(
            successor.clone(),
            operation(&d, 33, b"new mutation").await?,
            b"new mutation".to_vec(),
        )
        .await;
    let reconciled = controller
        .reconcile(successor.clone(), original.clone())
        .await;
    let next_create = ExecutionOperation::new(
        [34; 16],
        ExecutionOperationKind::Create,
        b"another create",
        operation(&d, 34, b"another create").await?.expires_unix_ms,
    )?;
    let duplicate_create = controller
        .execute(successor.clone(), next_create, b"another create".to_vec())
        .await;
    let calls = backend.calls.load(Ordering::Acquire);
    eprintln!("fresh-incarnation ledger: inherited={}, backend_calls={calls}, replay={replay:?}, mutation={blocked:?}, reconciliation={reconciled:?}, duplicate_create={duplicate_create:?}",inherited.len());
    controller.shutdown().await?;
    d.shutdown().await;
    assert_eq!(
        inherited,
        vec![original.clone()],
        "a fresh incarnation must retain old unresolved obligations"
    );
    failure(
        &replay.unwrap_err(),
        ExecutionControllerFailure::ConflictingRequest,
    );
    failure(
        &blocked.unwrap_err(),
        ExecutionControllerFailure::UnknownOperation,
    );
    assert_eq!(
        reconciled?,
        ExecutionOutcome::Completed(b"original resource".to_vec())
    );
    failure(
        &duplicate_create.unwrap_err(),
        ExecutionControllerFailure::ConflictingRequest,
    );
    assert_eq!(
        calls, 1,
        "fencing old execution cannot authorize replay of an unknown create"
    );
    let d2 = domain();
    let reopened = ExecutionController::open(d2.clone(), resume, backend.clone()).await?;
    assert!(reopened.pending(successor.clone()).await?.is_empty());
    assert_eq!(
        reopened.reconcile(successor, original).await?,
        ExecutionOutcome::Completed(b"original resource".to_vec())
    );
    assert_eq!(backend.calls.load(Ordering::Acquire), 1);
    reopened.shutdown().await?;
    d2.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn startup_reconciles_original_operations_from_a_revoked_history() -> Result<()> {
    let d = domain();
    let backend = Arc::new(Provider::default());
    let controller =
        ExecutionController::create(d.clone(), options(root()?), backend.clone(), [42; 32]).await?;
    let resume = controller.options();
    let old = claim(&controller.identity(), 3, 1, 1)?;
    controller.clone().activate(old.clone()).await?;
    backend.hold.store(true, Ordering::Release);
    let request = operation(&d, 38, b"old accepted result").await?;
    let mut first =
        Box::pin(controller.execute(old.clone(), request, b"old accepted result".to_vec()));
    match futures_util::future::select(first.as_mut(), Box::pin(backend.entered.notified())).await {
        futures_util::future::Either::Left(_) => {
            anyhow::bail!("old operation did not reach the reply-loss cut")
        }
        futures_util::future::Either::Right(_) => {}
    }
    drop(first);
    backend.hold.store(false, Ordering::Release);
    let original = controller.pending(old).await?.pop().unwrap();
    controller
        .recover_history(history(3), history(4), [42; 32])
        .await?;
    let current = claim(&controller.identity(), 4, 1, 1)?;
    controller.clone().activate(current.clone()).await?;
    assert_eq!(
        controller.pending(current.clone()).await?,
        vec![original.clone()]
    );
    controller.shutdown().await?;
    d.shutdown().await;
    let d2 = domain();
    let reopened = ExecutionController::open(d2.clone(), resume, backend.clone()).await?;
    assert!(reopened.pending(current.clone()).await?.is_empty());
    assert_eq!(
        reopened.reconcile(current, original).await?,
        ExecutionOutcome::Completed(b"old accepted result".to_vec())
    );
    assert_eq!(backend.calls.load(Ordering::Acquire), 1);
    assert_eq!(backend.observations.load(Ordering::Acquire), 1);
    reopened.shutdown().await?;
    d2.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_observation_does_not_authorize_replay_or_block_a_new_mutation() -> Result<()> {
    let d = domain();
    let backend = Arc::new(Provider::default());
    let controller =
        ExecutionController::create(d.clone(), options(root()?), backend.clone(), [42; 32]).await?;
    let current = claim(&controller.identity(), 3, 1, 1)?;
    controller.clone().activate(current.clone()).await?;
    let deadline = operation(&d, 39, b"observation").await?.expires_unix_ms;
    let request = ExecutionOperation::new(
        [39; 16],
        ExecutionOperationKind::Inspect,
        b"observation",
        deadline,
    )?;
    backend.hold.store(true, Ordering::Release);
    let mut first =
        Box::pin(controller.execute(current.clone(), request.clone(), b"observation".to_vec()));
    match futures_util::future::select(first.as_mut(), Box::pin(backend.entered.notified())).await {
        futures_util::future::Either::Left(_) => {
            anyhow::bail!("observation did not enter the reply-loss cut")
        }
        futures_util::future::Either::Right(_) => {}
    }
    drop(first);
    backend.hold.store(false, Ordering::Release);
    controller
        .execute(
            current.clone(),
            operation(&d, 40, b"new mutation").await?,
            b"new mutation".to_vec(),
        )
        .await?;
    failure(
        &controller
            .execute(current.clone(), request, b"observation".to_vec())
            .await
            .unwrap_err(),
        ExecutionControllerFailure::UnknownOperation,
    );
    assert_eq!(controller.pending(current).await?.len(), 1);
    assert_eq!(
        backend.calls.load(Ordering::Acquire),
        2,
        "the observation must not replay"
    );
    controller.shutdown().await?;
    d.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn held_observations_leave_control_capacity_and_cannot_rewind_new_receipts() -> Result<()> {
    for replace_owner in [false, true] {
        let d = domain();
        let backend = Arc::new(Provider::default());
        let mut opts = options(root()?);
        opts.limits.max_inflight = 2;
        let controller =
            ExecutionController::create(d.clone(), opts, backend.clone(), [42; 32]).await?;
        let old = claim(&controller.identity(), 3, 1, 1)?;
        controller.clone().activate(old.clone()).await?;
        let expiry = operation(&d, 51, b"old observation").await?.expires_unix_ms;
        let read = ExecutionOperation::new(
            [51; 16],
            ExecutionOperationKind::Read,
            b"old observation",
            expiry,
        )?;
        backend.hold.store(true, Ordering::Release);
        let mut held =
            Box::pin(controller.execute(old.clone(), read.clone(), b"old observation".to_vec()));
        match futures_util::future::select(held.as_mut(), Box::pin(backend.entered.notified()))
            .await
        {
            futures_util::future::Either::Left(_) => {
                anyhow::bail!("observation did not reach hold cut")
            }
            futures_util::future::Either::Right(_) => {}
        }
        backend.hold.store(false, Ordering::Release);
        let current = if replace_owner {
            claim(&controller.identity(), 3, 2, 2)?
        } else {
            old.clone()
        };
        let write = operation(&d, 52, b"new mutation").await?;
        let control = d
            .run(async {
                cellar::asyncrt::timeout(std::time::Duration::from_secs(1), async {
                    controller.clone().activate(current.clone()).await?;
                    assert_eq!(controller.pending(current.clone()).await?.len(), 1);
                    assert!(controller
                        .lifecycle(current.clone(), ExecutionOperationKind::Delete)
                        .await?
                        .is_none());
                    controller
                        .execute(current.clone(), write.clone(), b"new mutation".to_vec())
                        .await?;
                    controller
                        .reconcile(
                            current.clone(),
                            ExecutionPendingOperation {
                                claim: current.clone(),
                                operation: write.clone(),
                            },
                        )
                        .await
                })
                .await
            })
            .await?;
        backend.released.notify_one();
        let observed = held.await;
        assert_eq!(
            control
                .expect("held observations cannot block control")
                .unwrap(),
            ExecutionOutcome::Completed(b"new mutation".to_vec())
        );
        if replace_owner {
            failure(
                &observed.unwrap_err(),
                ExecutionControllerFailure::UnknownOperation,
            );
            let original = controller.pending(current.clone()).await?.pop().unwrap();
            assert_eq!(original.operation, read);
            controller.reconcile(current.clone(), original).await?;
        } else {
            assert_eq!(
                observed?,
                ExecutionOutcome::Completed(b"old observation".to_vec())
            );
        }
        assert_eq!(
            controller
                .execute(current, write, b"new mutation".to_vec())
                .await?,
            ExecutionOutcome::Completed(b"new mutation".to_vec())
        );
        assert_eq!(
            backend.calls.load(Ordering::Acquire),
            2,
            "observation completion must retain the concurrent mutation receipt"
        );
        controller.shutdown().await?;
        d.shutdown().await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_lookup_recovers_completed_receipts_in_a_new_incarnation() -> Result<()> {
    let d = domain();
    let backend = Arc::new(Provider::default());
    let controller =
        ExecutionController::create(d.clone(), options(root()?), backend.clone(), [42; 32]).await?;
    let old = claim(&controller.identity(), 3, 1, 1)?;
    controller.clone().activate(old.clone()).await?;
    let expiry = operation(&d, 53, b"original create").await?.expires_unix_ms;
    let create = ExecutionOperation::new(
        [53; 16],
        ExecutionOperationKind::Create,
        b"original create",
        expiry,
    )?;
    controller
        .execute(old.clone(), create.clone(), b"original create".to_vec())
        .await?;
    controller
        .recover_history(history(3), history(4), [42; 32])
        .await?;
    let new = claim(&controller.identity(), 4, 1, 1)?;
    controller.clone().activate(new.clone()).await?;
    assert!(controller.pending(new.clone()).await?.is_empty());
    let original = controller
        .lifecycle(new.clone(), ExecutionOperationKind::Create)
        .await?
        .unwrap();
    assert_eq!(original.claim, old);
    assert_eq!(original.operation, create);
    assert_eq!(
        controller.reconcile(new, original).await?,
        ExecutionOutcome::Completed(b"original create".to_vec())
    );
    assert_eq!(backend.calls.load(Ordering::Acquire), 1);
    controller.shutdown().await?;
    d.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sealing_absent_operations_refuses_late_submission_and_preserves_entered_work() -> Result<()>
{
    let d = domain();
    let backend = Arc::new(Provider::default());
    let controller =
        ExecutionController::create(d.clone(), options(root()?), backend.clone(), [42; 32]).await?;
    let resume = controller.options();
    let current = claim(&controller.identity(), 3, 1, 1)?;
    controller.clone().activate(current.clone()).await?;
    let live = operation(&d, 54, b"late call").await?;
    let mut tombstone = live.clone();
    tombstone.expires_unix_ms = 1;
    let sealed = controller
        .seal_unsubmitted(current.clone(), tombstone.clone())
        .await?;
    assert_eq!(
        controller
            .execute(current.clone(), live, b"late call".to_vec())
            .await?,
        ExecutionOutcome::Refused
    );
    let entered = operation(&d, 55, b"accepted call").await?;
    controller
        .execute(current.clone(), entered.clone(), b"accepted call".to_vec())
        .await?;
    let mut expired = entered.clone();
    expired.expires_unix_ms = 1;
    let retained = controller
        .seal_unsubmitted(current.clone(), expired)
        .await?;
    assert_eq!(
        retained.operation, entered,
        "sealing cannot rewrite an entered request's deadline"
    );
    assert_eq!(
        controller
            .reconcile(current.clone(), sealed.clone())
            .await?,
        ExecutionOutcome::Refused
    );
    controller.shutdown().await?;
    d.shutdown().await;
    let d2 = domain();
    let reopened = ExecutionController::open(d2.clone(), resume, backend.clone()).await?;
    assert_eq!(
        reopened.reconcile(current, sealed).await?,
        ExecutionOutcome::Refused
    );
    assert_eq!(backend.calls.load(Ordering::Acquire), 1);
    reopened.shutdown().await?;
    d2.shutdown().await;
    Ok(())
}
