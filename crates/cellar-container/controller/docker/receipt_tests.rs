// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real durable receipt/admission logic with an unopened Engine transport.
//! These tests qualify metadata concurrency, not physical process fencing.
use super::*;
use cellar::sdk::consensus::{CellId, ClusterId, IncarnationId, NamespaceId};
use serde_json::json;
use std::time::Duration;

struct RefuseHistory;

impl DockerHistoryFence for RefuseHistory {
    fn fence(self: Arc<Self>, _: CellHistory, _: CellHistory) -> HandlerFuture<()> {
        Box::pin(async { anyhow::bail!("receipt fixture has no infrastructure fence") })
    }
}

pub(super) struct Fixture {
    pub(super) backend: Arc<DockerBackend>,
    pub(super) current: ExecutionClaim,
    known: ExecutionPendingOperation,
    unknown: ExecutionPendingOperation,
}

async fn fixture() -> Result<Fixture> {
    fixture_in_domain(Arc::new(ExecutionDomain::new(
        tokio::runtime::Handle::current(),
    )))
    .await
}

pub(super) async fn fixture_in_domain(domain: Arc<ExecutionDomain>) -> Result<Fixture> {
    let root = tempfile::tempdir()?.keep();
    eprintln!("retained Docker receipt concurrency: {}", root.display());
    let options = DockerOptions {
        directory: root.join("backend"),
        socket: root.join("unopened-engine.sock"),
        binding: "DOCKER".into(),
        owner: "a".repeat(32),
        image: format!("sha256:{}", "b".repeat(64)),
        memory_bytes: 128 * 1024 * 1024,
        nano_cpus: 250_000_000,
        pids: 64,
        max_resources: 8,
        max_operations: 256,
        max_inflight: 8,
        max_body_bytes: 4096,
        max_operation_ms: 5000,
    };
    let identity = options.identity()?;
    let history = CellHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        cell: CellId::new("Counter:receipt")?,
        incarnation: IncarnationId::new([3; 16])?,
    };
    let mut hash = Sha256::new();
    hash.update(b"cellar-provider-execution-resource-1");
    hash.update(
        cellar::application::consensus::verification::cell_groups::cell_image_history_digest(
            &history,
        ),
    );
    hash.update((identity.binding.len() as u16).to_be_bytes());
    hash.update(identity.binding.as_bytes());
    let key: [u8; 32] = hash.finalize().into();
    let current: ExecutionClaim = serde_json::from_value(json!({
        "format":1,"cluster":history.cluster.bytes(),"namespace":history.namespace.bytes(),"cell":history.cell.as_str(),"incarnation":history.incarnation.bytes(),
        "binding":identity.binding,"key":key,"sequence":1,"owner":"d".repeat(64),
        "checkpoint":{"revision":1,"activation_epoch":1,"record":{"provider":identity.provider,"configuration_digest":identity.configuration_digest,"parameters_digest":"e".repeat(64),"generation":1,"request_id":"f".repeat(32),"resource_id":null,"intent":"running","observation":"pending"}}
    }))?;
    let fs = domain.run(async { cellar::asyncrt::fs() }).await?;
    let policy = options.clone();
    let journal = domain
        .blocking(move || Journal::create(policy, fs, "receipt-fixture-engine".into()))?
        .await??;
    let backend = Arc::new(DockerBackend {
        engine: Engine::new(&options),
        identity,
        domain,
        journal,
        admission: Admission::default(),
        capacity: Arc::new(Semaphore::new(8)),
        observations: Arc::new(Semaphore::new(7)),
        options,
        jobs: Mutex::new(BTreeMap::new()),
        changed: Notify::new(),
        closed: AtomicBool::new(false),
        history_fence: Arc::new(RefuseHistory),
    });
    let known = ExecutionPendingOperation {
        claim: current.clone(),
        operation: ExecutionOperation::new(
            [1; 16],
            ExecutionOperationKind::Execute,
            b"original",
            1,
        )?,
    };
    let unknown = ExecutionPendingOperation {
        claim: current.clone(),
        operation: ExecutionOperation::new(
            [2; 16],
            ExecutionOperationKind::Execute,
            b"entered-unknown",
            1,
        )?,
    };
    let claim = current.clone();
    let operations = [
        (
            &known,
            Some(ExecutionOutcome::Completed(b"original".to_vec())),
        ),
        (&unknown, None),
    ]
    .into_iter()
    .map(|(op, outcome)| {
        (
            hex(&op.operation.id),
            PhysicalOperation {
                claim: op.claim.clone(),
                operation: op.operation.clone(),
                outcome,
            },
        )
    })
    .collect();
    backend
        .update(move |state| {
            state.incarnations.insert(
                journal::logical(claim.history(), claim.binding())?,
                journal::history(&claim)?,
            );
            state.slots.insert(
                journal::key(&claim),
                Slot {
                    high: claim,
                    active: true,
                    transition: 1,
                    operations,
                },
            );
            Ok(())
        })
        .await?;
    Ok(Fixture {
        backend,
        current,
        known,
        unknown,
    })
}

#[tokio::test]
async fn retained_receipts_do_not_wait_for_physical_admission() -> Result<()> {
    let f = fixture().await?;
    let domain = f.backend.domain.clone();
    let result = async {
        let _admission = f.backend.admission.exclusive().await;
        let known = domain
            .run(async {
                cellar::asyncrt::timeout(
                    Duration::from_millis(500),
                    f.backend.clone().reconcile(f.current.clone(), f.known),
                )
                .await
            })
            .await?
            .context("retained known receipt waited for physical admission")??;
        ensure!(known == Some(ExecutionOutcome::Completed(b"original".to_vec())));
        let unknown = domain
            .run(async {
                cellar::asyncrt::timeout(
                    Duration::from_millis(500),
                    f.backend.clone().reconcile(f.current, f.unknown),
                )
                .await
            })
            .await?
            .context("entered unknown receipt waited for physical admission")??;
        ensure!(
            unknown.is_none(),
            "expiry rewrote an entered unknown operation"
        );
        Ok(())
    }
    .await;
    // No Engine work was started. The domain joins all durable journal work.
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn absent_expiry_receipts_require_physical_admission() -> Result<()> {
    let f = fixture().await?;
    let domain = f.backend.domain.clone();
    let result = async {
        let original = ExecutionPendingOperation {
            claim: f.current.clone(),
            operation: ExecutionOperation::new(
                [3; 16],
                ExecutionOperationKind::Delete,
                b"absent-delete",
                1,
            )?,
        };
        let admission = f.backend.admission.exclusive().await;
        let attempts = f.backend.journal.attempts.load(Ordering::Acquire);
        let blocked = domain
            .run(async {
                cellar::asyncrt::timeout(
                    Duration::from_millis(50),
                    f.backend
                        .clone()
                        .reconcile(f.current.clone(), original.clone()),
                )
                .await
            })
            .await?;
        ensure!(
            blocked.is_err(),
            "absence was sealed without excluding queued physical submission"
        );
        ensure!(
            f.backend.journal.attempts.load(Ordering::Acquire) == attempts,
            "absence entered journal persistence before physical admission"
        );
        ensure!(f
            .backend
            .journal
            .view(|state| Ok(state.original_operation(&f.current, &original)?.is_none()))?);
        drop(admission);
        ensure!(
            f.backend
                .clone()
                .reconcile(f.current.clone(), original.clone())
                .await?
                == Some(ExecutionOutcome::ExpiredBeforeSubmission)
        );
        ensure!(
            f.backend.journal.view(|state| Ok(state
                .original_operation(&f.current, &original)?
                .context("durable expiry receipt missing")?
                .outcome
                .clone()))?
                == Some(ExecutionOutcome::ExpiredBeforeSubmission)
        );
        Ok(())
    }
    .await;
    domain.shutdown().await;
    result
}

async fn other_binding(f: &Fixture) -> Result<ExecutionClaim> {
    let mut history = f.current.history().clone();
    history.cell = CellId::new("Counter:independent-receipt")?;
    let identity = f.backend.identity();
    let mut hash = Sha256::new();
    hash.update(b"cellar-provider-execution-resource-1");
    hash.update(
        cellar::application::consensus::verification::cell_groups::cell_image_history_digest(
            &history,
        ),
    );
    hash.update((identity.binding.len() as u16).to_be_bytes());
    hash.update(identity.binding.as_bytes());
    let key: [u8; 32] = hash.finalize().into();
    let mut encoded = serde_json::to_value(&f.current)?;
    encoded["cell"] = json!(history.cell.as_str());
    encoded["key"] = json!(key);
    let claim: ExecutionClaim = serde_json::from_value(encoded)?;
    let installed = claim.clone();
    f.backend
        .update(move |state| {
            state.incarnations.insert(
                journal::logical(installed.history(), installed.binding())?,
                journal::history(&installed)?,
            );
            state.slots.insert(
                journal::key(&installed),
                Slot {
                    high: installed,
                    active: true,
                    transition: 1,
                    operations: BTreeMap::new(),
                },
            );
            Ok(())
        })
        .await?;
    Ok(claim)
}

#[tokio::test]
async fn independent_binding_durable_expiry_does_not_wait_for_another_fence() -> Result<()> {
    let f = fixture().await?;
    let domain = f.backend.domain.clone();
    let result = async {
        let other = other_binding(&f).await?;
        let original = ExecutionPendingOperation {
            claim: other.clone(),
            operation: ExecutionOperation::new(
                [4; 16],
                ExecutionOperationKind::Delete,
                b"independent-absent-delete",
                1,
            )?,
        };
        // Hold exactly the admission used while this binding physically stops
        // and joins its old process tree. The other binding is independent.
        let _physical = f
            .backend
            .admission
            .resource(&journal::key(&f.current))
            .await?;
        let outcome = domain
            .run(async {
                cellar::asyncrt::timeout(
                    Duration::from_secs(5),
                    f.backend.clone().reconcile(other.clone(), original.clone()),
                )
                .await
            })
            .await?
            .context("independent binding waited behind another resource fence")??;
        ensure!(outcome == Some(ExecutionOutcome::ExpiredBeforeSubmission));
        ensure!(
            f.backend.journal.view(|state| Ok(state
                .original_operation(&other, &original)?
                .context("independent durable receipt missing")?
                .outcome
                .clone()))?
                == Some(ExecutionOutcome::ExpiredBeforeSubmission)
        );
        // An entered unknown operation in the fenced binding is untouched.
        ensure!(f.backend.journal.view(|state| Ok(state
            .original_operation(&f.current, &f.unknown)?
            .context("original unknown operation missing")?
            .outcome
            .is_none()))?);
        Ok(())
    }
    .await;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn absent_expiry_waits_for_its_own_binding_fence() -> Result<()> {
    let f = fixture().await?;
    let domain = f.backend.domain.clone();
    let result = async {
        let original = ExecutionPendingOperation {
            claim: f.current.clone(),
            operation: ExecutionOperation::new(
                [5; 16],
                ExecutionOperationKind::Delete,
                b"same-binding-absent-delete",
                1,
            )?,
        };
        let physical = f
            .backend
            .admission
            .resource(&journal::key(&f.current))
            .await?;
        let attempts = f.backend.journal.attempts.load(Ordering::Acquire);
        let blocked = domain
            .run(async {
                cellar::asyncrt::timeout(
                    Duration::from_millis(50),
                    f.backend
                        .clone()
                        .reconcile(f.current.clone(), original.clone()),
                )
                .await
            })
            .await?;
        ensure!(
            blocked.is_err(),
            "same-binding absence bypassed its physical fence"
        );
        ensure!(
            f.backend.journal.attempts.load(Ordering::Acquire) == attempts,
            "same-binding absence entered persistence behind its fence"
        );
        ensure!(f
            .backend
            .journal
            .view(|state| Ok(state.original_operation(&f.current, &original)?.is_none()))?);
        drop(physical);
        ensure!(
            f.backend
                .clone()
                .reconcile(f.current.clone(), original)
                .await?
                == Some(ExecutionOutcome::ExpiredBeforeSubmission)
        );
        Ok(())
    }
    .await;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn queued_revocation_cannot_reactivate_after_shutdown_closes_admission() -> Result<()> {
    let f = fixture().await?;
    let domain = f.backend.domain.clone();
    let result = async {
        let mut encoded = serde_json::to_value(&f.current)?;
        encoded["sequence"] = json!(2);
        encoded["checkpoint"]["revision"] = json!(2);
        let successor = serde_json::from_value(encoded)?;
        let exclusive = f.backend.admission.exclusive().await;
        let before = f.backend.admission.attempts();
        let backend = f.backend.clone();
        let queued = domain.spawn(async move { backend.revoke_to(successor).await })?;
        domain
            .run(async {
                cellar::asyncrt::timeout(Duration::from_secs(5), async {
                    while f.backend.admission.attempts() == before {
                        cellar::asyncrt::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await
            })
            .await?
            .context("revocation never reached physical admission")?;
        // Shutdown closes this flag before acquiring its exclusive gate.
        // The revocation already passed its initial check, but has not entered.
        f.backend.closed.store(true, Ordering::Release);
        drop(exclusive);
        let error = queued
            .await?
            .expect_err("closed backend admitted queued revocation");
        ensure!(
            matches!(
                error.downcast_ref::<ExecutionControllerFailure>(),
                Some(ExecutionControllerFailure::Unavailable)
            ),
            "queued revocation reached Engine I/O after shutdown: {error:#}"
        );
        ensure!(
            f.backend.journal.view(|state| Ok(state
                .slots
                .get(&journal::key(&f.current))
                .context("original binding missing")?
                .high
                == f.current))?,
            "late revocation changed the durable high-water mark"
        );
        Ok(())
    }
    .await;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn durable_reservation_rechecks_the_current_claim() -> Result<()> {
    let f = fixture().await?;
    let domain = f.backend.domain.clone();
    let result = async {
        let mut encoded = serde_json::to_value(&f.current)?;
        encoded["sequence"] = json!(2);
        encoded["checkpoint"]["revision"] = json!(2);
        let successor = serde_json::from_value(encoded)?;
        let key = journal::key(&f.current);
        f.backend
            .update(move |state| {
                state
                    .slots
                    .get_mut(&key)
                    .context("original slot missing")?
                    .high = successor;
                Ok(())
            })
            .await?;
        let pending = PhysicalOperation {
            claim: f.current.clone(),
            operation: ExecutionOperation::new(
                [6; 16],
                ExecutionOperationKind::Delete,
                b"stale-reservation",
                1,
            )?,
            outcome: None,
        };
        let error = f
            .backend
            .update(move |state| state.reserve(pending))
            .await
            .expect_err("obsolete snapshot admitted a stale reservation");
        ensure!(
            error.downcast_ref::<ExecutionControllerFailure>()
                == Some(&ExecutionControllerFailure::StaleClaim)
        );
        ensure!(
            f.backend
                .journal
                .view(|state| Ok(state.slots[&journal::key(&f.current)].operations.len()))?
                == 2
        );
        Ok(())
    }
    .await;
    domain.shutdown().await;
    result
}

#[tokio::test]
async fn duplicate_reservation_cannot_erase_original_outcome() -> Result<()> {
    let f = fixture().await?;
    let domain = f.backend.domain.clone();
    let result = async {
        let repeated = PhysicalOperation {
            claim: f.known.claim.clone(),
            operation: f.known.operation.clone(),
            outcome: None,
        };
        let error = f
            .backend
            .update(move |state| state.reserve(repeated))
            .await
            .expect_err("duplicate reservation erased an immutable outcome");
        ensure!(
            error.downcast_ref::<ExecutionControllerFailure>()
                == Some(&ExecutionControllerFailure::UnknownOperation)
        );
        ensure!(
            f.backend.journal.view(|state| Ok(state
                .original_operation(&f.current, &f.known)?
                .context("original receipt missing")?
                .outcome
                .clone()))?
                == Some(ExecutionOutcome::Completed(b"original".to_vec()))
        );
        Ok(())
    }
    .await;
    domain.shutdown().await;
    result
}
