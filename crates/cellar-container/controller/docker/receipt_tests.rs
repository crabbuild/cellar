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

struct Fixture {
    backend: Arc<DockerBackend>,
    current: ExecutionClaim,
    known: ExecutionPendingOperation,
    unknown: ExecutionPendingOperation,
}

async fn fixture() -> Result<Fixture> {
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
    let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
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
        admission: AsyncMutex::new(()),
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
        let _admission = f.backend.admission.lock().await;
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
        let admission = f.backend.admission.lock().await;
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
