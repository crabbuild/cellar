// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Normal native Container API, real processes and retained provider bytes.
use super::{catalog_fault, configuration, managed_fixture, rustfs};
use anyhow::{ensure, Context as _, Result};
use cellar::{
    engine_api::RpcData,
    sdk::{consensus::*, containers::*, *},
};
use cellar_container::{
    controller::{
        docker::{DockerBackend, DockerHistoryFence, DockerOptions},
        ControllerProvider, NativeContainerAdapter,
    },
    Limits,
};
use managed_fixture::{ArchiveSetup, FirstGroupSetup, Fixture, ManagedCluster, TIMEOUT};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

const CONTENT: &[u8] = b"native\0\xffquorum";

pub(super) struct NoDisasterAuthority;

impl DockerHistoryFence for NoDisasterAuthority {
    fn fence(self: Arc<Self>, _: CellHistory, _: CellHistory) -> HandlerFuture<()> {
        Box::pin(async {
            anyhow::bail!("this fixture does not authorize disaster history admission")
        })
    }
}

struct LostReply {
    backend: Arc<DockerBackend>,
    lose: AtomicBool,
    hide: AtomicBool,
    original: Mutex<Option<ExecutionPendingOperation>>,
    submissions: AtomicUsize,
    hidden_lookups: AtomicUsize,
    latest: Mutex<Option<ExecutionClaim>>,
}

impl ExecutionControllerBackend for LostReply {
    fn identity(&self) -> BackendIdentity {
        self.backend.identity()
    }

    fn revoke(
        self: Arc<Self>,
        previous: Option<ExecutionClaim>,
        successor: Option<ExecutionClaim>,
        unresolved: Vec<ExecutionPendingOperation>,
    ) -> HandlerFuture<()> {
        Box::pin(async move {
            self.backend
                .clone()
                .revoke(previous, successor.clone(), unresolved)
                .await?;
            *self.latest.lock().unwrap() = successor;
            Ok(())
        })
    }

    fn revoke_history(
        self: Arc<Self>,
        old: CellHistory,
        new: CellHistory,
        previous: Option<ExecutionClaim>,
        unresolved: Vec<ExecutionPendingOperation>,
    ) -> HandlerFuture<()> {
        self.backend
            .clone()
            .revoke_history(old, new, previous, unresolved)
    }

    fn execute(
        self: Arc<Self>,
        claim: ExecutionClaim,
        operation: ExecutionOperation,
        payload: Vec<u8>,
    ) -> HandlerFuture<ExecutionOutcome> {
        Box::pin(async move {
            let outcome = self
                .backend
                .clone()
                .execute(claim.clone(), operation.clone(), payload)
                .await?;
            if operation.kind == ExecutionOperationKind::Execute {
                *self.latest.lock().unwrap() = Some(claim.clone());
                self.submissions.fetch_add(1, Ordering::AcqRel);
                if self.lose.swap(false, Ordering::AcqRel) {
                    *self.original.lock().unwrap() =
                        Some(ExecutionPendingOperation { claim, operation });
                    // Real command and durable backend receipt already completed.
                    // Only the reply into the controller is lost at this cut.
                    return std::future::pending().await;
                }
            }
            Ok(outcome)
        })
    }

    fn reconcile(
        self: Arc<Self>,
        current: ExecutionClaim,
        original: ExecutionPendingOperation,
    ) -> HandlerFuture<Option<ExecutionOutcome>> {
        Box::pin(async move {
            if self.hide.load(Ordering::Acquire)
                && self.original.lock().unwrap().as_ref() == Some(&original)
            {
                self.hidden_lookups.fetch_add(1, Ordering::AcqRel);
                return Ok(None);
            }
            self.backend.clone().reconcile(current, original).await
        })
    }
}

#[derive(Default)]
struct Track {
    prepared: AtomicUsize,
    factories: Mutex<std::collections::BTreeMap<String, usize>>,
}

struct PreparedAdapter {
    inner: NativeContainerAdapter,
    track: Arc<Track>,
}

impl ContainerAdapter for PreparedAdapter {
    fn identity(&self) -> BackendIdentity {
        self.inner.identity()
    }

    fn max_body_bytes(&self) -> usize {
        self.inner.max_body_bytes()
    }

    fn prepare<'a>(
        &'a self,
        context: &'a Context,
        request: ContainerRequest,
        lifecycle: Option<&'a BindingLifecycleGuard>,
    ) -> Result<PreparedContainerCall<'a>> {
        let start = matches!(request, ContainerRequest::Start(_));
        let call = self.inner.prepare(context, request, lifecycle)?;
        if start {
            self.track.prepared.fetch_add(1, Ordering::AcqRel);
        }
        Ok(call)
    }
}

struct Root {
    storage: Storage,
    container: Container,
}

impl Cell for Root {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc {
                args: RpcData::Json(args),
                ..
            } = invocation
            else {
                anyhow::bail!("native Docker RPC required");
            };
            let operation: String = serde_json::from_str(&args)?;
            match operation.as_str() {
                "Create" => {
                    self.storage.put_json("business", &json!(11))?;
                    self.container
                        .start(
                            &context,
                            StartOptions {
                                env: vec![("VALUE".into(), "configured".into())],
                                ..Default::default()
                            },
                        )
                        .await?;
                }
                "Write" => {
                    self.storage.put_json("business", &json!(12))?;
                    self.container
                        .write_file(
                            &context,
                            FilePath::new("/workspace/value")?,
                            CONTENT.to_vec(),
                            0o644,
                        )
                        .await?;
                }
                "Execute" => {
                    let output = self.container.exec_output(&context,ExecOptions{
                        cmd:vec!["/bin/sh".into(),"-c".into(),"printf '%s' \"$VALUE\"; printf x >> /workspace/once; cat /workspace/value; printf diagnostic >&2".into()],
                        env:vec![("VALUE".into(),"request".into())],..Default::default()
                    },4096).await?;
                    ensure!(
                        output.exit_code == 0
                            && output.stdout == [b"request".as_slice(), CONTENT].concat()
                            && output.stderr == b"diagnostic"
                    );
                }
                "Detach" => {
                    let output=self.container.exec_output(&context,ExecOptions{
                        cmd:vec!["/bin/sh".into(),"-c".into(),"(while :; do printf x >> /workspace/heartbeat; sleep 0.05; done) </dev/null >/dev/null 2>&1 & while [ ! -s /workspace/heartbeat ]; do sleep 0.01; done; printf spawned".into()],
                        ..Default::default()
                    },4096).await?;
                    ensure!(output.exit_code == 0 && output.stdout == b"spawned");
                }
                "Fence" => {
                    let path = FilePath::new("/workspace/heartbeat")?;
                    let before = self
                        .container
                        .read_file(&context, path.clone(), 4096)
                        .await?;
                    ensure!(
                        !before.is_empty(),
                        "detached process never entered execution"
                    );
                    cellar::asyncrt::sleep(std::time::Duration::from_millis(250)).await;
                    ensure!(
                        self.container.read_file(&context, path, 4096).await? == before,
                        "old detached execution survived revocation"
                    );
                }
                "Read" => {
                    ensure!(
                        self.storage.get_json("business")? == Some(json!(12)),
                        "business state lost"
                    );
                    ensure!(
                        self.container
                            .read_file(&context, FilePath::new("/workspace/value")?, 4096)
                            .await?
                            == CONTENT,
                        "provider bytes lost"
                    );
                    let once = self
                        .container
                        .read_file(&context, FilePath::new("/workspace/once")?, 16)
                        .await?;
                    ensure!(
                        once == b"xx",
                        "uncertain command replayed or prior work was lost"
                    );
                    let metadata = self
                        .container
                        .stat_file(&context, FilePath::new("/workspace/value")?)
                        .await?;
                    ensure!(metadata.size == CONTENT.len() as u64 && metadata.mode == 0o644);
                    let entries = self
                        .container
                        .list_files(&context, FilePath::new("/workspace")?, 4096, 16)
                        .await?;
                    ensure!(
                        entries.iter().any(|entry| entry.name == "value")
                            && entries.iter().any(|entry| entry.name == "once")
                    );
                }
                "Status" => {
                    let _ = self.container.running(&context)?;
                }
                "Destroy" => self.container.destroy(&context).await?,
                _ => anyhow::bail!("unknown Docker fixture operation"),
            }
            Ok(Output::Rpc(RpcData::Json(
                json!({"operation":operation}).to_string(),
            )))
        })
    }
}

async fn start(
    rooted: bool,
    minimum: Durability,
    store: Arc<catalog_fault::Store>,
    controller: Arc<ExecutionController>,
    track: Arc<Track>,
) -> Result<ManagedCluster> {
    let provider = Arc::new(ControllerProvider::new(controller)?);
    let adapter = Arc::new(PreparedAdapter {
        inner: NativeContainerAdapter::new(
            provider,
            Limits {
                max_body_bytes: 4096,
            },
        )?,
        track: track.clone(),
    });
    let class = RaftClass::new("Counter", 1, [201; 32])?
        .initializer([202; 32], |_, _| Ok(()))?
        .json_cell_rpc("Run", [203; 32])?
        .container_adapter(adapter)?;
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |context, identity| {
                *track
                    .factories
                    .lock()
                    .unwrap()
                    .entry(identity.scope.clone())
                    .or_default() += 1;
                Ok(Arc::new(Root {
                    storage: context.storage()?,
                    container: context.container()?,
                }))
            },
        )?,
    );
    let mut options = configuration::options("docker-execution");
    options.do_classes = vec!["Counter".into()];
    application.validate_deployment(&configuration::deployment(options.clone()))?;
    let definition = application.resolve_raft_class("Counter")?;
    let mut fixture =
        Fixture::with_definition_policies(rooted, vec![(definition, minimum)], application, store)
            .await?;
    fixture.configuration = Some(options);
    fixture.max_input_bytes = 64 * 1024;
    fixture.sql_workers = 1;
    fixture.effects = Some(RaftEffectOptions {
        callback_timeout_ms: 5000,
        ..Default::default()
    });
    Box::pin(ManagedCluster::start_with_applications(
        fixture,
        &[],
        ArchiveSetup::Initialized,
        false,
        None,
        FirstGroupSetup::Normal,
    ))
    .await
}

fn options(sequence: u64, minimum: Durability) -> Result<CommandOptions> {
    Ok(CommandOptions {
        durability: minimum,
        ..CommandOptions::new(
            CommandId::new(ClientId::new([204; 16])?, sequence)?,
            u64::try_from(cellar::asyncrt::wall_ms())? + 60_000,
        )
    })
}

async fn call(
    cluster: &ManagedCluster,
    history: &CellHistory,
    minimum: Durability,
    sequence: u64,
    operation: &str,
) -> Result<CommandReceipt> {
    let leader = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
    let cell = cluster.hosts[leader].cell(history.clone())?;
    let started_unix_ms = cellar::asyncrt::wall_ms();
    let started = std::time::Instant::now();
    let result = cell
        .command(
            &cell.contract("Run")?,
            serde_json::to_vec(operation)?,
            options(sequence, minimum)?,
        )
        .await;
    eprintln!(
        "docker_native_request {}",
        json!({"cell":history.cell.as_str(),"sequence":sequence,"operation":operation,"minimum":format!("{minimum:?}"),"started_unix_ms":started_unix_ms,"finished_unix_ms":cellar::asyncrt::wall_ms(),"elapsed_ms":started.elapsed().as_millis(),"success":result.is_ok()})
    );
    Ok(result?)
}

pub(super) async fn restart(cluster: &mut ManagedCluster) -> Result<()> {
    let old = std::mem::take(&mut cluster.hosts);
    managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &old).await?;
    cluster.peers.clear();
    for (index, fixture) in cluster.fixtures.iter().enumerate() {
        let node = fixture.start_node(cluster.roots[index].path()).await?;
        let (mut config, services) = fixture
            .config(&node, LogOpenMode::Resume, 104 + index as u8)
            .await?;
        managed_fixture::archives::configure(&mut config);
        let peer = config.peers[0].clone();
        let host = match node.start_raft_cells(config, services).await {
            Ok(host) => host,
            Err(error) => {
                node.shutdown(ShutdownMode::Preserve).await?;
                return Err(error);
            }
        };
        cluster.nodes.push(node);
        cluster.hosts.push(host);
        cluster.peers.push(peer);
    }
    Ok(())
}

pub(super) async fn physical_resources(
    domain: &ExecutionDomain,
    policy: &DockerOptions,
) -> Result<usize> {
    let docker = cellar::docker::Docker::new(policy.socket.clone());
    let filter = serde_json::to_string(
        &json!({"label":[format!("cellar.execution.owner={}",policy.owner)]}),
    )?;
    let query = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("all", "true")
        .append_pair("filters", &filter)
        .finish();
    domain
        .run(async move {
            let result = docker
                .call_bounded(
                    "GET",
                    &format!("/containers/json?{query}"),
                    None,
                    1024 * 1024,
                )
                .await?;
            ensure!(result.status.is_success());
            Ok(result
                .json()?
                .as_array()
                .context("physical inventory")?
                .len())
        })
        .await?
}

pub async fn qualify(outage: bool) -> Result<()> {
    let socket = PathBuf::from(std::env::var("CELLAR_DOCKER_BACKEND_SOCKET")?);
    let image = std::env::var("CELLAR_DOCKER_BACKEND_IMAGE")?;
    let mut failures = Vec::new();
    for rooted in [false, true] {
        for minimum in [Durability::Quorum, Durability::S3] {
            let root = tempfile::tempdir()?.keep();
            eprintln!("retained Docker backend and controller: {}", root.display());
            let owner = hex::encode(&Sha256::digest(root.as_os_str().as_encoded_bytes())[..16]);
            let policy = DockerOptions {
                directory: root.join("backend"),
                socket: socket.clone(),
                image: image.clone(),
                binding: "DOCKER".into(),
                owner,
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
            let backend = DockerBackend::create(
                domain.clone(),
                policy.clone(),
                Arc::new(NoDisasterAuthority),
            )
            .await?;
            let fault = Arc::new(LostReply {
                backend: backend.clone(),
                lose: AtomicBool::new(false),
                hide: AtomicBool::new(false),
                original: Mutex::new(None),
                submissions: AtomicUsize::new(0),
                hidden_lookups: AtomicUsize::new(0),
                latest: Mutex::new(None),
            });
            let controller = ExecutionController::create(
                domain.clone(),
                ExecutionControllerOptions {
                    directory: root.join("controller"),
                    controller_id: [0; 16],
                    backend: backend.identity(),
                    limits: ExecutionControllerLimits::default(),
                },
                fault.clone(),
                [205; 32],
            )
            .await?;
            let case = format!(
                "docker-native-{}-{}",
                if outage { "outage" } else { "recovery" },
                if minimum == Durability::S3 {
                    "s3"
                } else {
                    "quorum"
                }
            );
            let store = Arc::new(catalog_fault::Store::around(rustfs::store(&case, rooted)?));
            let track = Arc::new(Track::default());
            let mut cluster = match start(
                rooted,
                minimum,
                store.clone(),
                controller.clone(),
                track.clone(),
            )
            .await
            {
                Ok(cluster) => cluster,
                Err(error) => {
                    controller.shutdown().await?;
                    backend.shutdown().await?;
                    domain.shutdown().await;
                    failures.push(format!("{case} rooted={rooted}: startup {error:#}"));
                    continue;
                }
            };
            let execution = cluster.fixtures[0].domain.clone();
            let result=execution.run(async {
            let history=|name:&str|->Result<CellHistory>{Ok(CellHistory{cluster:cluster.history.cluster,namespace:cluster.history.namespace,
                cell:CellId::new(format!("Counter:{name}"))?,incarnation:IncarnationId::new([206;16])?})};
            let a=history("docker-a")?;let b=history("docker-b")?;
            for (index,h) in [&a,&b].iter().enumerate() { let leader=managed_fixture::leader(&cluster.hosts,cluster.history).await?;
                cluster.hosts[leader].cell((*h).clone())?.enroll(CellEnrollmentRequest{creation:ProposalId::new([207+index as u8;16])?,durability:minimum},TIMEOUT).await?; }
            if outage {
                let before=store.denied_group_archive_writes.load(Ordering::Acquire);store.deny_group_archive_writes.store(true,Ordering::Release);
                let created=call(&cluster,&a,minimum,1,"Create").await;
                ensure!(track.prepared.load(Ordering::Acquire)==1,"archive cut did not reach private native Start preparation");
                if minimum==Durability::S3 { let error=created.err().context("native Docker escaped source S3 gate")?;
                    let original=options(1,minimum)?.id;
                    ensure!(error.downcast_ref::<Arc<CellCommandError>>().is_some_and(|error|error.ambiguous && error.command==original));
                    ensure!(store.denied_group_archive_writes.load(Ordering::Acquire)>before);
                    ensure!(physical_resources(&domain,&policy).await?==0,"Docker entered before source proof");
                } else { ensure!(created?.durability>=minimum);ensure!(physical_resources(&domain,&policy).await?==1); }
                store.deny_group_archive_writes.store(false,Ordering::Release);
                if minimum==Durability::Quorum {call(&cluster,&a,minimum,2,"Destroy").await?;}
            } else {
                call(&cluster,&a,minimum,1,"Create").await?;call(&cluster,&b,minimum,1,"Create").await?;
                call(&cluster,&a,minimum,2,"Write").await?;call(&cluster,&a,minimum,3,"Execute").await?;
                fault.lose.store(true,Ordering::Release);fault.hide.store(true,Ordering::Release);
                let error=call(&cluster,&a,minimum,4,"Execute").await.err().context("physical exec reply cut was not reached")?;
                let original=options(4,minimum)?.id;
                ensure!(error.downcast_ref::<Arc<CellCommandError>>().is_some_and(|error|error.ambiguous && error.command==original));
                ensure!(fault.submissions.load(Ordering::Acquire)==2 && fault.original.lock().unwrap().is_some());
                call(&cluster,&b,minimum,2,"Status").await?;
                restart(&mut cluster).await?;
                let constructed=track.factories.lock().unwrap().get(a.cell.as_str()).copied().unwrap_or(0);
                let leader=managed_fixture::leader(&cluster.hosts,cluster.history).await?;
                let blocked=cluster.hosts[leader].cell(a.clone())?;
                let refused=blocked.command(&blocked.contract("Run")?,serde_json::to_vec("Read")?,CommandOptions{
                    durability:minimum,..CommandOptions::new(CommandId::new(ClientId::new([208;16])?,1)?,
                        u64::try_from(cellar::asyncrt::wall_ms())?+60_000)
                }).await;
                ensure!(refused.is_err(),"unknown original exec admitted business construction");
                ensure!(fault.hidden_lookups.load(Ordering::Acquire)>0,"unknown original receipt cut was not observed");
                ensure!(track.factories.lock().unwrap().get(a.cell.as_str()).copied().unwrap_or(0)==constructed,"business constructor entered while original execution was unknown");
                call(&cluster,&b,minimum,3,"Status").await?;
                fault.hide.store(false,Ordering::Release);
                call(&cluster,&a,minimum,5,"Read").await?;
                ensure!(fault.submissions.load(Ordering::Acquire)==2,"recovery replayed an original physical command");
                call(&cluster,&a,minimum,6,"Detach").await?;
                let old=fault.latest.lock().unwrap().clone().context("original Docker execution claim missing")?;
                let leader=managed_fixture::leader(&cluster.hosts,cluster.history).await?;
                let status=cluster.hosts[leader].group(cluster.history)?.replication_status(TIMEOUT).await?;
                let next=(leader+1)%3;
                cluster.hosts[leader].group(cluster.history)?.transfer_leadership(LeadershipTransfer{
                    target:MemberId::new(next as u64+1)?,expected_term:status.term,expected_revision:status.membership_revision
                },TIMEOUT).await?;
                cellar::asyncrt::timeout(TIMEOUT,async {
                    while managed_fixture::leader(&cluster.hosts,cluster.history).await?!=next {cellar::asyncrt::sleep(std::time::Duration::from_millis(10)).await;}
                    Ok::<_,anyhow::Error>(())
                }).await??;
                call(&cluster,&a,minimum,7,"Fence").await?;
                let current=fault.latest.lock().unwrap().clone().context("successor Docker execution claim missing")?;
                ensure!(current.history()==old.history() && current.sequence()>old.sequence());
                let payload=serde_json::to_vec(&cellar_container::controller::Request{format:1,operation:cellar_container::Operation::ExecOutput.name().into(),
                    input:json!({"options":{"cmd":["/bin/sh","-c","printf stale >> /workspace/heartbeat"]},"max_bytes":4096})})?;
                let operation=ExecutionOperation::new([209;16],ExecutionOperationKind::Execute,&payload,u64::try_from(cellar::asyncrt::wall_ms())?+5000)?;
                let stale=backend.clone().execute(old,operation,payload).await.err().context("old Docker claim entered execution")?;
                ensure!(stale.downcast_ref::<ExecutionControllerFailure>()==Some(&ExecutionControllerFailure::StaleClaim));
                ensure!(fault.submissions.load(Ordering::Acquire)==3);
                call(&cluster,&a,minimum,8,"Destroy").await?;call(&cluster,&b,minimum,4,"Destroy").await?;
                ensure!(physical_resources(&domain,&policy).await?==0);
            }
            eprintln!("docker_native_configuration {}",json!({"extents":rooted,"minimum":format!("{minimum:?}"),"archive_outage":outage,"original_request_ms":5000,"real_engine":true}));
            Ok::<_,anyhow::Error>(())
        }).await.map_err(anyhow::Error::from).and_then(std::convert::identity);
            store
                .deny_group_archive_writes
                .store(false, Ordering::Release);
            let closed = managed_fixture::close_group_nodes(
                std::mem::take(&mut cluster.nodes),
                &cluster.hosts,
            )
            .await;
            for root in std::mem::take(&mut cluster.roots) {
                eprintln!("retained Docker voter disk: {}", root.keep().display());
            }
            execution.shutdown().await;
            let stopped = controller.shutdown().await;
            let backend_stopped = backend.shutdown().await;
            domain.shutdown().await;
            if let Err(error) = result.and(closed).and(stopped).and(backend_stopped) {
                failures.push(format!("{case} rooted={rooted}: {error:#}"));
            }
        }
    }
    ensure!(
        failures.is_empty(),
        "Docker configurations failed: {}",
        failures.join("; ")
    );
    Ok(())
}
