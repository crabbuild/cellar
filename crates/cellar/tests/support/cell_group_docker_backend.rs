// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical backend restart/fence checks using trusted host metadata. The
//! separate public-driver cases establish the actual Raft source admission.
use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::{
    application::consensus::verification::cell_groups::cell_image_history_digest,
    sdk::{consensus::*, containers::*, ExecutionDomain, HandlerFuture},
};
use cellar_container::{
    controller::{
        docker::{DockerBackend, DockerHistoryFence, DockerOptions},
        Reply, Request,
    },
    Operation,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc, time::Duration};

struct RefuseHistory;

impl DockerHistoryFence for RefuseHistory {
    fn fence(self: Arc<Self>, _: CellHistory, _: CellHistory) -> HandlerFuture<()> {
        Box::pin(async {
            anyhow::bail!("no infrastructure fence is supplied by this standalone fixture")
        })
    }
}

fn payload(method: Operation, input: Value) -> Result<Vec<u8>> {
    let mut input = input;
    input.sort_all_objects();
    Ok(serde_json::to_vec(&Request {
        format: 1,
        operation: method.name().into(),
        input,
    })?)
}

fn claim(
    identity: &BackendIdentity,
    incarnation: u8,
    sequence: u64,
    resource: Option<&str>,
    parameters: &str,
) -> Result<ExecutionClaim> {
    let history = CellHistory {
        cluster: ClusterId::new([1; 16])?,
        namespace: NamespaceId::new([2; 16])?,
        cell: CellId::new("Counter:physical")?,
        incarnation: IncarnationId::new([incarnation; 16])?,
    };
    let mut key = Sha256::new();
    key.update(b"cellar-provider-execution-resource-1");
    key.update(cell_image_history_digest(&history));
    key.update((identity.binding.len() as u16).to_be_bytes());
    key.update(identity.binding.as_bytes());
    let key: [u8; 32] = key.finalize().into();
    Ok(serde_json::from_value(
        json!({"format":1,"cluster":history.cluster.bytes(),"namespace":history.namespace.bytes(),"cell":history.cell.as_str(),"incarnation":history.incarnation.bytes(),
        "binding":identity.binding,"key":key,"sequence":sequence,"owner":hex::encode([31;32]),
        "checkpoint":{"revision":if resource.is_some(){2}else{1},"activation_epoch":1,"record":{
            "provider":identity.provider,"configuration_digest":identity.configuration_digest,"parameters_digest":parameters,"generation":1,"request_id":hex::encode([32;16]),
            "resource_id":resource,"intent":"running","observation":if resource.is_some(){"confirmed"}else{"pending"}
        }}}),
    )?)
}

async fn operation(
    domain: &ExecutionDomain,
    id: u8,
    kind: ExecutionOperationKind,
    body: &[u8],
) -> Result<ExecutionOperation> {
    let expires = domain
        .run(async { u64::try_from(cellar::asyncrt::wall_ms()).unwrap() + 5000 })
        .await?;
    ExecutionOperation::new([id; 16], kind, body, expires)
}

async fn result(
    controller: &Arc<ExecutionController>,
    claim: &ExecutionClaim,
    operation: ExecutionOperation,
    body: Vec<u8>,
) -> Result<Reply> {
    let outcome = controller.execute(claim.clone(), operation, body).await?;
    let ExecutionOutcome::Completed(bytes) = outcome else {
        anyhow::bail!("physical operation refused: {outcome:?}");
    };
    Ok(serde_json::from_slice(&bytes)?)
}

async fn read(
    controller: &Arc<ExecutionController>,
    domain: &ExecutionDomain,
    claim: &ExecutionClaim,
    id: u8,
    path: &str,
) -> Result<Vec<u8>> {
    let body = payload(Operation::ReadFile, json!({"path":path,"max_bytes":4096}))?;
    let operation = operation(domain, id, ExecutionOperationKind::Inspect, &body).await?;
    let reply = result(controller, claim, operation, body).await?;
    Ok(STANDARD.decode(
        reply
            .value
            .as_str()
            .context("physical file result missing")?,
    )?)
}

async fn process_state(
    domain: &ExecutionDomain,
    options: &DockerOptions,
    resource: &str,
) -> Result<Value> {
    ensure!(resource.len() == 64 && resource.bytes().all(|byte| byte.is_ascii_hexdigit()));
    let resource = resource.to_owned();
    let options = options.clone();
    domain
        .run(async move {
            let docker = cellar::docker::Docker::new(options.socket);
            let reply = docker
                .call_bounded(
                    "GET",
                    &format!("/containers/{resource}/json"),
                    None,
                    1024 * 1024,
                )
                .await?;
            ensure!(
                reply.status.is_success(),
                "Docker process observation refused"
            );
            let value = reply.json()?;
            ensure!(
                value["Id"].as_str() == Some(&resource)
                    && value["Config"]["Labels"]["cellar.execution.owner"].as_str()
                        == Some(&options.owner)
                    && value["Config"]["Image"].as_str() == Some(&options.image),
                "Docker shutdown fixture ownership differs"
            );
            Ok::<_, anyhow::Error>(value["State"].clone())
        })
        .await?
}

pub async fn qualify_failed_shutdown() -> Result<()> {
    let root = tempfile::tempdir()?.keep();
    eprintln!("retained Docker shutdown fault: {}", root.display());
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
    let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
    let backend =
        DockerBackend::create(domain.clone(), options.clone(), Arc::new(RefuseHistory)).await?;
    let controller = ExecutionController::create(
        domain.clone(),
        ExecutionControllerOptions {
            directory: root.join("controller"),
            controller_id: [0; 16],
            backend: backend.identity(),
            limits: ExecutionControllerLimits::default(),
        },
        backend.clone(),
        [33; 32],
    )
    .await?;
    let start = payload(
        Operation::Start,
        json!({"options":{"entrypoint":null,"env":[],"labels":[],"enable_internet":false}}),
    )?;
    let parameters = hex::encode(
        ExecutionOperation::new([1; 16], ExecutionOperationKind::Create, &start, 1)?.payload_digest,
    );
    let first = claim(&controller.identity(), 3, 1, None, &parameters)?;
    controller.clone().activate(first.clone()).await?;
    let create = operation(&domain, 1, ExecutionOperationKind::Create, &start).await?;
    let resource = result(&controller, &first, create, start)
        .await?
        .resource_id;
    controller.shutdown().await?;
    let before = process_state(&domain, &options, &resource).await?;
    ensure!(before["Running"] == true && before["Pid"].as_u64().is_some_and(|pid| pid > 0));
    let fault = options.directory.join("state.next");
    domain
        .blocking(move || {
            let fs = cellar::asyncrt::fs();
            ensure!(matches!(fs.symlink_metadata(&fault), Err(error) if error.kind() == std::io::ErrorKind::NotFound));
            fs.create_dir_all(&fault)?;
            Ok::<_, anyhow::Error>(())
        })?
        .await??;
    let shutdown = backend.shutdown().await;
    let observed = process_state(&domain, &options, &resource).await?;
    let fenced = observed["Running"] == false && observed["Pid"] == 0;
    // Preserve the actual pre-cleanup oracle. The failing source still needs
    // an exact-owner kill/join so qualification never leaves its process alive.
    if !fenced {
        let socket = options.socket.clone();
        let resource = resource.clone();
        domain
            .run(async move {
                let docker = cellar::docker::Docker::new(socket);
                for suffix in ["kill?signal=KILL", "wait?condition=not-running"] {
                    let reply = docker
                        .call_bounded(
                            "POST",
                            &format!("/containers/{resource}/{suffix}"),
                            None,
                            1024 * 1024,
                        )
                        .await?;
                    ensure!(
                        reply.status.is_success(),
                        "owned shutdown-fault cleanup refused"
                    );
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
        let cleaned = process_state(&domain, &options, &resource).await?;
        ensure!(cleaned["Running"] == false && cleaned["Pid"] == 0);
    }
    domain.shutdown().await;
    let error = shutdown
        .err()
        .context("journal failure was silently acknowledged")?;
    ensure!(
        error.chain().any(|cause| cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::IsADirectory)),
        "shutdown did not retain the injected journal error: {error:#}"
    );
    eprintln!(
        "docker_shutdown_failure_observations {}",
        json!({"journal_error_retained":true,"fenced_before_test_cleanup":fenced,"process_state":observed,"forced_test_cleanup":!fenced})
    );
    ensure!(
        fenced,
        "failed durable deactivation left Docker process running"
    );
    Ok(())
}

pub async fn qualify() -> Result<()> {
    let root = tempfile::tempdir()?.keep();
    eprintln!("retained physical Docker qualification: {}", root.display());
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
    let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
    let backend =
        DockerBackend::create(domain.clone(), options.clone(), Arc::new(RefuseHistory)).await?;
    let controller = ExecutionController::create(
        domain.clone(),
        ExecutionControllerOptions {
            directory: root.join("controller"),
            controller_id: [0; 16],
            backend: backend.identity(),
            limits: ExecutionControllerLimits::default(),
        },
        backend.clone(),
        [33; 32],
    )
    .await?;
    let configuration = controller.options();
    let identity = controller.identity();
    let start = payload(
        Operation::Start,
        json!({"options":{"entrypoint":null,"env":[],"labels":[],"enable_internet":false}}),
    )?;
    let parameters = hex::encode(
        ExecutionOperation::new([1; 16], ExecutionOperationKind::Create, &start, 1)?.payload_digest,
    );
    let first = claim(&identity, 3, 1, None, &parameters)?;
    controller.clone().activate(first.clone()).await?;
    let create = operation(&domain, 1, ExecutionOperationKind::Create, &start).await?;
    let resource = result(&controller, &first, create, start)
        .await?
        .resource_id;
    let current = claim(&identity, 3, 2, Some(&resource), &parameters)?;
    controller.clone().activate(current.clone()).await?;
    let limits = payload(
        Operation::ExecOutput,
        json!({"options":{"cmd":["/bin/sh","-c","cat /sys/fs/cgroup/cpu.max /sys/fs/cgroup/memory.max /sys/fs/cgroup/memory.swap.max /sys/fs/cgroup/pids.max"]},"max_bytes":4096}),
    )?;
    let limits = result(
        &controller,
        &current,
        operation(&domain, 12, ExecutionOperationKind::Execute, &limits).await?,
        limits,
    )
    .await?;
    ensure!(limits.value["exit_code"].as_i64() == Some(0));
    let limits = STANDARD.decode(
        limits.value["stdout"]
            .as_str()
            .context("kernel cgroup observation missing")?,
    )?;
    ensure!(
        limits == b"25000 100000\n134217728\n0\n64\n",
        "kernel did not enforce the selected Docker resource limits"
    );
    eprintln!("physical Docker cgroups: {}", String::from_utf8(limits)?);
    let command = payload(
        Operation::ExecOutput,
        json!({"options":{"cmd":["/bin/sh","-c","printf x >> /workspace/once; printf original"]},"max_bytes":4096}),
    )?;
    let request = operation(&domain, 2, ExecutionOperationKind::Execute, &command).await?;
    let original = result(&controller, &current, request.clone(), command.clone()).await?;
    ensure!(
        STANDARD.decode(
            original.value["stdout"]
                .as_str()
                .context("physical stdout missing")?
        )? == b"original"
    );
    ensure!(read(&controller, &domain, &current, 3, "/workspace/once").await? == b"x");
    controller.shutdown().await?;
    backend.shutdown().await?;
    drop(controller);
    drop(backend);

    let backend = DockerBackend::open(domain.clone(), options, Arc::new(RefuseHistory)).await?;
    let controller =
        ExecutionController::open(domain.clone(), configuration, backend.clone()).await?;
    let replay = result(&controller, &current, request, command).await?;
    ensure!(replay.value == original.value && replay.resource_id == resource);
    ensure!(
        read(&controller, &domain, &current, 4, "/workspace/once").await? == b"x",
        "controller/backend restart replayed a physical execution"
    );
    let detached = payload(
        Operation::ExecOutput,
        json!({"options":{"cmd":["/bin/sh","-c","(while :; do printf x >> /workspace/heartbeat; sleep 0.05; done) </dev/null >/dev/null 2>&1 & while [ ! -s /workspace/heartbeat ]; do sleep 0.01; done; printf spawned"]},"max_bytes":4096}),
    )?;
    result(
        &controller,
        &current,
        operation(&domain, 5, ExecutionOperationKind::Execute, &detached).await?,
        detached,
    )
    .await?;
    let successor = claim(&identity, 3, 3, Some(&resource), &parameters)?;
    controller.clone().activate(successor.clone()).await?;
    let before = read(&controller, &domain, &successor, 6, "/workspace/heartbeat").await?;
    ensure!(!before.is_empty());
    domain
        .run(async { cellar::asyncrt::sleep(Duration::from_millis(250)).await })
        .await?;
    ensure!(
        read(&controller, &domain, &successor, 7, "/workspace/heartbeat").await? == before,
        "old physical execution survived a fence"
    );
    let body = payload(
        Operation::ExecOutput,
        json!({"options":{"cmd":["/bin/sh","-c","printf stale >> /workspace/heartbeat"]},"max_bytes":4096}),
    )?;
    let old = backend
        .clone()
        .execute(
            current.clone(),
            operation(&domain, 8, ExecutionOperationKind::Execute, &body).await?,
            body,
        )
        .await
        .err()
        .context("old backend claim accepted")?;
    ensure!(
        old.downcast_ref::<ExecutionControllerFailure>()
            == Some(&ExecutionControllerFailure::StaleClaim)
    );
    let slow = payload(
        Operation::ExecOutput,
        json!({"options":{"cmd":["/bin/sh","-c","printf started; while :; do sleep 1; done"]},"max_bytes":4096}),
    )?;
    let slow_request = operation(&domain, 9, ExecutionOperationKind::Execute, &slow).await?;
    let timed = controller
        .execute(successor.clone(), slow_request.clone(), slow)
        .await
        .err()
        .context("long command escaped its original deadline")?;
    ensure!(
        timed.downcast_ref::<ExecutionControllerFailure>()
            == Some(&ExecutionControllerFailure::UnknownOperation)
    );
    let original = ExecutionPendingOperation {
        claim: successor.clone(),
        operation: slow_request,
    };
    let completed = domain
        .run(async {
            cellar::asyncrt::timeout(Duration::from_secs(60), async {
                loop {
                    match controller
                        .reconcile(successor.clone(), original.clone())
                        .await
                    {
                        Ok(ExecutionOutcome::Completed(bytes)) => {
                            return Ok::<_, anyhow::Error>(serde_json::from_slice::<Reply>(&bytes)?)
                        }
                        Ok(other) => {
                            anyhow::bail!("entered process lost its actual outcome: {other:?}")
                        }
                        Err(error)
                            if matches!(
                                error.downcast_ref::<ExecutionControllerFailure>(),
                                Some(
                                    ExecutionControllerFailure::UnknownOperation
                                        | ExecutionControllerFailure::RecoveryPending
                                )
                            ) => {}
                        Err(error) => return Err(error),
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(20)).await;
                }
            })
            .await?
        })
        .await??;
    ensure!(
        completed.value["exit_code"]
            .as_i64()
            .is_some_and(|code| code != 0)
            && STANDARD.decode(
                completed.value["stdout"]
                    .as_str()
                    .context("original process output missing")?
            )? == b"started",
        "deadline recovery did not retain the joined original process outcome"
    );
    ensure!(read(&controller, &domain, &successor, 10, "/workspace/once").await? == b"x");
    let healthy = payload(
        Operation::ExecOutput,
        json!({"options":{"cmd":["/bin/cat","/workspace/once"]},"max_bytes":4096}),
    )?;
    let healthy = result(
        &controller,
        &successor,
        operation(&domain, 11, ExecutionOperationKind::Execute, &healthy).await?,
        healthy,
    )
    .await?;
    ensure!(
        healthy.value["exit_code"].as_i64() == Some(0)
            && STANDARD.decode(
                healthy.value["stdout"]
                    .as_str()
                    .context("follow-up output missing")?
            )? == b"x"
    );
    let overflow = payload(
        Operation::ExecOutput,
        json!({"options":{"cmd":["/bin/sh","-c","printf overflow > /workspace/overflow; dd if=/dev/zero bs=4096 count=2; while :; do sleep 1; done"]},"max_bytes":16}),
    )?;
    ensure!(
        controller
            .execute(
                successor.clone(),
                operation(&domain, 13, ExecutionOperationKind::Execute, &overflow).await?,
                overflow
            )
            .await
            .is_err(),
        "output overflow did not refuse its original operation"
    );
    let socket = PathBuf::from(std::env::var("CELLAR_DOCKER_BACKEND_SOCKET")?);
    let docker = cellar::docker::Docker::new(socket);
    let inspected = domain
        .run(async {
            let reply = docker
                .call_bounded(
                    "GET",
                    &format!("/containers/{resource}/json"),
                    None,
                    1024 * 1024,
                )
                .await?;
            ensure!(reply.status.is_success());
            reply.json()
        })
        .await??;
    ensure!(
        inspected["State"]["Running"].as_bool() == Some(false)
            && inspected["State"]["Pid"].as_u64() == Some(0),
        "failed output collection released a still-running process tree"
    );
    let pending = claim(&identity, 3, 4, Some(&resource), &parameters)?;
    ensure!(
        backend
            .clone()
            .revoke(Some(successor.clone()), Some(pending), vec![])
            .await
            .is_err(),
        "an unresolved physical mutation admitted a successor"
    );
    let fresh = claim(&identity, 4, 5, Some(&resource), &parameters)?;
    ensure!(
        backend
            .clone()
            .revoke(None, Some(fresh.clone()), vec![])
            .await
            .is_err(),
        "fresh incarnation bypassed old-history fencing"
    );
    ensure!(
        backend
            .clone()
            .revoke_history(
                successor.history().clone(),
                fresh.history().clone(),
                Some(successor.clone()),
                vec![]
            )
            .await
            .is_err(),
        "missing infrastructure authority accepted recovery"
    );
    ensure!(backend
        .clone()
        .revoke(None, Some(successor), vec![])
        .await
        .is_err());
    ensure!(backend
        .clone()
        .revoke(None, Some(fresh), vec![])
        .await
        .is_err());
    let closed = controller.shutdown().await;
    let stopped = backend.shutdown().await;
    domain.shutdown().await;
    closed?;
    stopped?;
    Ok(())
}
