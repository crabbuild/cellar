// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A controller can durably submit before its backend future is first polled.
//! Expired absence needs a durable backend seal, not an absence-based retry.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

struct DelayedDelete {
    backend: Arc<DockerBackend>,
    entered: AtomicBool,
}

impl ExecutionControllerBackend for DelayedDelete {
    fn identity(&self) -> BackendIdentity {
        self.backend.identity()
    }

    fn revoke(
        self: Arc<Self>,
        previous: Option<ExecutionClaim>,
        successor: Option<ExecutionClaim>,
        pending: Vec<ExecutionPendingOperation>,
    ) -> HandlerFuture<()> {
        self.backend.clone().revoke(previous, successor, pending)
    }

    fn revoke_history(
        self: Arc<Self>,
        old: CellHistory,
        successor: CellHistory,
        previous: Option<ExecutionClaim>,
        pending: Vec<ExecutionPendingOperation>,
    ) -> HandlerFuture<()> {
        self.backend
            .clone()
            .revoke_history(old, successor, previous, pending)
    }

    fn execute(
        self: Arc<Self>,
        claim: ExecutionClaim,
        operation: ExecutionOperation,
        payload: Vec<u8>,
    ) -> HandlerFuture<ExecutionOutcome> {
        Box::pin(async move {
            if operation.kind == ExecutionOperationKind::Delete {
                self.entered.store(true, Ordering::Release);
                std::future::pending::<()>().await;
            }
            self.backend
                .clone()
                .execute(claim, operation, payload)
                .await
        })
    }

    fn reconcile(
        self: Arc<Self>,
        current: ExecutionClaim,
        original: ExecutionPendingOperation,
    ) -> HandlerFuture<Option<ExecutionOutcome>> {
        self.backend.clone().reconcile(current, original)
    }
}

pub async fn qualify() -> Result<()> {
    let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
    let result = run(domain.clone()).await;
    domain.shutdown().await;
    result
}

async fn run(domain: Arc<ExecutionDomain>) -> Result<()> {
    let root = tempfile::tempdir()?.keep();
    eprintln!(
        "retained Docker submission-seal qualification: {}",
        root.display()
    );
    let options = DockerOptions {
        directory: root.join("backend"),
        socket: PathBuf::from(std::env::var("CELLAR_DOCKER_BACKEND_SOCKET")?),
        image: std::env::var("CELLAR_DOCKER_BACKEND_IMAGE")?,
        binding: "DOCKER".into(),
        owner: hex::encode(&Sha256::digest(root.as_os_str().as_encoded_bytes())[..16]),
        memory_bytes: 128 * 1024 * 1024,
        nano_cpus: 250_000_000,
        pids: 64,
        max_resources: 8,
        max_operations: 256,
        max_inflight: 8,
        max_body_bytes: 4096,
        max_operation_ms: 5000,
    };
    let backend =
        DockerBackend::create(domain.clone(), options.clone(), Arc::new(RefuseHistory)).await?;
    let delayed = Arc::new(DelayedDelete {
        backend: backend.clone(),
        entered: AtomicBool::new(false),
    });
    let controller = ExecutionController::create(
        domain.clone(),
        ExecutionControllerOptions {
            directory: root.join("controller"),
            controller_id: [0; 16],
            backend: backend.identity(),
            limits: ExecutionControllerLimits::default(),
        },
        delayed.clone(),
        [33; 32],
    )
    .await?;
    let configuration = controller.options();
    let checkpoint = async {
        let start = payload(Operation::Start, json!({"options":{"entrypoint":null,"env":[],"labels":[],"enable_internet":false}}))?;
        let parameters = hex::encode(ExecutionOperation::new([1;16], ExecutionOperationKind::Create, &start, 1)?.payload_digest);
        let first = claim(&controller.identity(), 3, 1, None, &parameters)?;
        controller.clone().activate(first.clone()).await?;
        let resource = result(&controller, &first, operation(&domain, 1, ExecutionOperationKind::Create, &start).await?, start).await?.resource_id;
        let mut value = serde_json::to_value(claim(&controller.identity(), 3, 2, Some(&resource), &parameters)?)?;
        value["checkpoint"]["record"]["intent"] = json!("deleted");
        value["checkpoint"]["record"]["observation"] = json!("pending");
        let current: ExecutionClaim = serde_json::from_value(value)?;
        controller.clone().activate(current.clone()).await?;
        let body = payload(Operation::Destroy, json!({}))?;
        let request = operation(&domain, 2, ExecutionOperationKind::Delete, &body).await?;
        let error = controller.execute(current.clone(), request.clone(), body.clone()).await.err().context("delayed Delete escaped its original five-second deadline")?;
        ensure!(error.downcast_ref::<ExecutionControllerFailure>() == Some(&ExecutionControllerFailure::UnknownOperation));
        ensure!(delayed.entered.load(Ordering::Acquire), "controller did not reach its submitted/backend-delivery boundary");
        let original = ExecutionPendingOperation { claim: current.clone(), operation: request.clone() };
        ensure!(controller.pending(current.clone()).await? == vec![original.clone()], "original submitted Delete identity was not retained");
        let before = process_state(&domain, &options, &resource).await?;
        ensure!(before["Pid"] == 0, "deleted-intent fence did not join the original scaffold");
        let receipt = backend.clone().reconcile(current.clone(), original.clone()).await?;
        ensure!(receipt == Some(ExecutionOutcome::ExpiredBeforeSubmission), "expired controller submission remained permanently unknown before physical backend entry: {receipt:?}");
        let mut conflicting = original.clone();
        conflicting.operation.payload_digest[0] ^= 1;
        let error = backend.clone().reconcile(current.clone(), conflicting).await.err().context("sealed operation accepted changed original metadata")?;
        ensure!(error.downcast_ref::<ExecutionControllerFailure>() == Some(&ExecutionControllerFailure::ConflictingRequest));
        ensure!(controller.reconcile(current.clone(), original.clone()).await? == ExecutionOutcome::ExpiredBeforeSubmission);
        Ok::<_,anyhow::Error>((current,resource,original,body))
    }.await;
    let closed = controller.shutdown().await;
    let stopped = backend.shutdown().await;
    drop(controller);
    drop(delayed);
    drop(backend);
    let (current, resource, original, body) = checkpoint?;
    closed?;
    stopped?;

    let backend =
        DockerBackend::open(domain.clone(), options.clone(), Arc::new(RefuseHistory)).await?;
    let controller =
        ExecutionController::open(domain.clone(), configuration, backend.clone()).await?;
    let checked = async {
        ensure!(backend.clone().reconcile(current.clone(), original.clone()).await? == Some(ExecutionOutcome::ExpiredBeforeSubmission));
        ensure!(backend.clone().execute(original.claim.clone(), original.operation.clone(), body.clone()).await? == ExecutionOutcome::ExpiredBeforeSubmission, "late original submission escaped its durable seal");
        let not_expired = ExecutionPendingOperation { claim: current.clone(), operation: operation(&domain, 4, ExecutionOperationKind::Delete, &body).await? };
        ensure!(backend.clone().reconcile(current.clone(), not_expired).await?.is_none(), "live absent submission was sealed before its original deadline");
        let retry = operation(&domain, 3, ExecutionOperationKind::Delete, &body).await?;
        let reply = result(&controller,&current,retry.clone(),body.clone()).await?;
        ensure!(reply.value.is_null());
        let duplicate = result(&controller,&current,retry,body).await?;
        ensure!(duplicate.value == reply.value && duplicate.resource_id == reply.resource_id);
        let socket = options.socket.clone();
        let removed = domain.run(async move { cellar::docker::Docker::new(socket).call_bounded("GET",&format!("/containers/{resource}/json"),None,1024*1024).await }).await??;
        ensure!(removed.status.as_u16() == 404, "new Delete did not remove the exact old physical resource");
        eprintln!("docker_submission_seal {{\"original_request_ms\":5000,\"original_expired_receipt\":true,\"retained_restart\":true,\"late_original_refused\":true,\"live_absence_uncertain\":true,\"replacement_delete_completed\":true}}");
        Ok::<_,anyhow::Error>(())
    }.await;
    let closed = controller.shutdown().await;
    let stopped = backend.shutdown().await;
    checked?;
    closed?;
    stopped
}
