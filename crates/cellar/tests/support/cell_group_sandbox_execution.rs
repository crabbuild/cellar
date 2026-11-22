// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public named Sandbox calls, actual processes and autonomous cleanup.
use super::{
    catalog_fault, configuration, docker_execution, managed_fixture, named_service, rustfs,
};
use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::{
    engine_api::RpcData,
    sdk::{consensus::*, containers::*, *},
};
use cellar_container::{
    controller::{
        docker::{DockerBackend, DockerOptions},
        ControllerProvider, NativeContainerAdapter,
    },
    Limits,
};
use managed_fixture::{ArchiveSetup, FirstGroupSetup, Fixture, ManagedCluster, TIMEOUT};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::Notify;

const ORDINARY_COMMAND_SLOTS: usize = 3;

struct TimingAdapter(NativeContainerAdapter);

impl ContainerAdapter for TimingAdapter {
    fn identity(&self) -> BackendIdentity {
        self.0.identity()
    }

    fn max_body_bytes(&self) -> usize {
        self.0.max_body_bytes()
    }

    fn supports_bounded_sandbox(&self) -> bool {
        self.0.supports_bounded_sandbox()
    }

    fn prepare<'a>(
        &'a self,
        context: &'a Context,
        request: ContainerRequest,
        lifecycle: Option<&'a BindingLifecycleGuard>,
    ) -> Result<PreparedContainerCall<'a>> {
        let kind = match &request {
            ContainerRequest::Start(_) => "Start",
            ContainerRequest::Observe => "Observe",
            ContainerRequest::Destroy => "Destroy",
            _ => "other",
        };
        let scope = context
            .identity()
            .context("timed child identity")?
            .scope
            .clone();
        eprintln!(
            "sandbox_start_adapter {}",
            json!({"scope":scope,"kind":kind,"phase":"prepare-enter","at_unix_ms":cellar::asyncrt::wall_ms()})
        );
        let call = self.0.prepare(context, request, lifecycle)?;
        eprintln!(
            "sandbox_start_adapter {}",
            json!({"scope":scope,"kind":kind,"phase":"prepare-complete","at_unix_ms":cellar::asyncrt::wall_ms()})
        );
        Ok(Box::new(move || {
            eprintln!(
                "sandbox_start_adapter {}",
                json!({"scope":scope,"kind":kind,"phase":"source-proved","at_unix_ms":cellar::asyncrt::wall_ms()})
            );
            let future = call();
            Box::pin(async move {
                let result = future.await;
                eprintln!(
                    "sandbox_start_adapter {}",
                    json!({"scope":scope,"kind":kind,"phase":"complete","at_unix_ms":cellar::asyncrt::wall_ms(),"success":result.is_ok()})
                );
                result
            })
        }))
    }
}

struct Peer {
    backend: Arc<DockerBackend>,
    retirement_cut: Option<Arc<catalog_fault::Store>>,
    creates: AtomicUsize,
    executes: AtomicUsize,
    writes: AtomicUsize,
    deletes: AtomicUsize,
    lose_exec: AtomicBool,
    hide_exec: AtomicBool,
    lose_delete: AtomicBool,
    hide_delete: AtomicBool,
    original_exec: Mutex<Option<ExecutionPendingOperation>>,
    original_delete: Mutex<Option<ExecutionPendingOperation>>,
    lookups: AtomicUsize,
    changed: Notify,
    trace: bool,
}

impl ExecutionControllerBackend for Peer {
    fn identity(&self) -> BackendIdentity {
        self.backend.identity()
    }

    fn revoke(
        self: Arc<Self>,
        old: Option<ExecutionClaim>,
        new: Option<ExecutionClaim>,
        pending: Vec<ExecutionPendingOperation>,
    ) -> HandlerFuture<()> {
        self.backend.clone().revoke(old, new, pending)
    }

    fn revoke_history(
        self: Arc<Self>,
        old: CellHistory,
        new: CellHistory,
        prior: Option<ExecutionClaim>,
        pending: Vec<ExecutionPendingOperation>,
    ) -> HandlerFuture<()> {
        self.backend
            .clone()
            .revoke_history(old, new, prior, pending)
    }

    fn execute(
        self: Arc<Self>,
        claim: ExecutionClaim,
        operation: ExecutionOperation,
        payload: Vec<u8>,
    ) -> HandlerFuture<ExecutionOutcome> {
        Box::pin(async move {
            let started = cellar::asyncrt::wall_ms();
            let result = self
                .backend
                .clone()
                .execute(claim.clone(), operation.clone(), payload)
                .await;
            if self.trace {
                eprintln!(
                    "sandbox_start_physical {}",
                    json!({"kind":format!("{:?}",operation.kind),"started_unix_ms":started,"finished_unix_ms":cellar::asyncrt::wall_ms(),"success":result.is_ok()})
                );
            }
            let outcome = result?;
            if matches!(outcome, ExecutionOutcome::Completed(_)) {
                let pending = ExecutionPendingOperation {
                    claim,
                    operation: operation.clone(),
                };
                let lose = match operation.kind {
                    ExecutionOperationKind::Create => {
                        self.creates.fetch_add(1, Ordering::AcqRel);
                        false
                    }
                    ExecutionOperationKind::Write => {
                        self.writes.fetch_add(1, Ordering::AcqRel);
                        false
                    }
                    ExecutionOperationKind::Execute => {
                        self.executes.fetch_add(1, Ordering::AcqRel);
                        let lose = self.lose_exec.swap(false, Ordering::AcqRel);
                        if lose {
                            *self.original_exec.lock().unwrap() = Some(pending);
                        }
                        lose
                    }
                    ExecutionOperationKind::Delete => {
                        self.deletes.fetch_add(1, Ordering::AcqRel);
                        if let Some(store) = &self.retirement_cut {
                            store
                                .deny_sandbox_parent_writes
                                .store(true, Ordering::Release);
                        }
                        let lose = self.lose_delete.swap(false, Ordering::AcqRel);
                        if lose {
                            *self.original_delete.lock().unwrap() = Some(pending);
                        }
                        lose
                    }
                    _ => false,
                };
                self.changed.notify_waiters();
                if lose {
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
            let hidden = (self.hide_exec.load(Ordering::Acquire)
                && self.original_exec.lock().unwrap().as_ref() == Some(&original))
                || (self.hide_delete.load(Ordering::Acquire)
                    && self.original_delete.lock().unwrap().as_ref() == Some(&original));
            if hidden {
                self.lookups.fetch_add(1, Ordering::AcqRel);
                return Ok(None);
            }
            self.backend.clone().reconcile(current, original).await
        })
    }
}

#[derive(Default)]
struct Track {
    factories: AtomicUsize,
    refuse: AtomicBool,
    calls: AtomicUsize,
    old: Mutex<Option<Context>>,
}

struct Parent {
    storage: Storage,
    track: Arc<Track>,
}

fn parameters() -> Value {
    json!({"options":{"entrypoint":null,"env":[["VALUE","sandbox"]],"labels":[],"enable_internet":false}})
}

impl Cell for Parent {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc {
                method,
                args: RpcData::Json(args),
            } = invocation
            else {
                anyhow::bail!("sandbox parent requires JSON RPC")
            };
            ensure!(method == "Run");
            let operation: String = serde_json::from_str(&args)?;
            self.track.calls.fetch_add(1, Ordering::AcqRel);
            self.track
                .old
                .lock()
                .unwrap()
                .get_or_insert_with(|| context.clone());
            let catalog = SandboxCatalog::open(&context)?;
            let value = match operation.as_str() {
                "Create" | "Spare" => serde_json::to_value(
                    catalog
                        .start_child(
                            &context,
                            if operation == "Spare" { "spare" } else { "job" },
                            "SANDBOX",
                            parameters(),
                        )
                        .await?,
                )?,
                "Exec" => {
                    self.storage.put_json("business", &json!(12))?;
                    let allocation = catalog
                        .get(&context, "job")?
                        .context("sandbox allocation")?;
                    catalog.execute_child(&context,&allocation,"container.exec_output",json!({"options":{"cmd":["/bin/sh","-c","printf x >> /workspace/once; printf \"$VALUE\""],"env":[],"cwd":null,"user":null,"combined":false,"pty":null},"max_bytes":4096})).await?
                }
                "Read" => {
                    ensure!(self.storage.get_json("business")? == Some(json!(12)));
                    let allocation = catalog
                        .get(&context, "job")?
                        .context("sandbox allocation")?;
                    let result = catalog
                        .execute_child(
                            &context,
                            &allocation,
                            "container.file.read",
                            json!({"path":"/workspace/once","max_bytes":4096}),
                        )
                        .await?;
                    ensure!(
                        STANDARD.decode(result.as_str().context("sandbox file result")?)? == b"xx",
                        "sandbox execution replayed or lost its workspace"
                    );
                    result
                }
                "FileWrite" | "FileRead" | "FileStat" | "FileList" => {
                    let allocation = catalog
                        .get(&context, "job")?
                        .context("sandbox allocation")?;
                    let (method, input) = match operation.as_str() {
                        "FileWrite" => (
                            "container.file.write",
                            json!({"path":"/workspace/bridge","bytes":STANDARD.encode(b"bridge"),"mode":384}),
                        ),
                        "FileRead" => (
                            "container.file.read",
                            json!({"path":"/workspace/bridge","max_bytes":4096}),
                        ),
                        "FileStat" => ("container.file.stat", json!({"path":"/workspace/bridge"})),
                        _ => (
                            "container.file.list",
                            json!({"path":"/workspace","max_bytes":4096,"max_entries":16}),
                        ),
                    };
                    catalog
                        .execute_child(&context, &allocation, method, input)
                        .await?
                }
                "Forge" => {
                    let allocation = catalog
                        .get(&context, "job")?
                        .context("sandbox allocation")?;
                    ensure!(
                        context
                            .namespace("SANDBOX")?
                            .get_by_name(&allocation.execution_name)
                            .rpc(&context, "__sandboxExecute", RpcData::Json("{}".into()))
                            .await
                            .is_err(),
                        "ordinary RPC forged sandbox authority"
                    );
                    json!(true)
                }
                "DestroyJob" => {
                    let allocation = catalog
                        .get(&context, "job")?
                        .context("sandbox allocation")?;
                    serde_json::to_value(catalog.destroy_child(&context, &allocation).await?)?
                }
                "Retire" => serde_json::to_value(catalog.begin_retirement(&context).await?)?,
                "Status" => Value::Null,
                _ => anyhow::bail!("unknown sandbox parent operation"),
            };
            Ok(Output::Rpc(RpcData::Json(serde_json::to_string(&value)?)))
        })
    }
}

async fn start(
    rooted: bool,
    minimum: Durability,
    target_minimum: Durability,
    store: Arc<catalog_fault::Store>,
    controller: Arc<ExecutionController>,
    track: Arc<Track>,
    trace: bool,
) -> Result<ManagedCluster> {
    let adapter = NativeContainerAdapter::new(
        Arc::new(ControllerProvider::new(controller)?),
        Limits {
            max_body_bytes: 4096,
        },
    )?;
    let adapter: Arc<dyn ContainerAdapter> = if trace {
        Arc::new(TimingAdapter(adapter))
    } else {
        Arc::new(adapter)
    };
    let parent=RaftClass::new("Counter",1,[220;32])?.initializer([230;32],|_,_|Ok(()))?.json_cell_rpc("Run",[221;32])?.sandbox_catalog()?
        .query("SandboxState",[222;32],|query,_|Ok(serde_json::to_vec(&json!({"parent":query.sandbox_parent()?,"cleanup_pending":query.sandbox_cleanup_pending()?,"job":query.sandbox_allocation("job")?,"spare":query.sandbox_allocation("spare")?}))?))?;
    let child = RaftClass::new("Sandbox", 1, [223; 32])?
        .container_adapter(adapter)?
        .sandbox_execution()?;
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
            .raft_cell(parent, move |context, _| {
                ensure!(
                    !track.refuse.load(Ordering::Acquire),
                    "sandbox parent constructor is unavailable"
                );
                track.factories.fetch_add(1, Ordering::AcqRel);
                Ok(Arc::new(Parent {
                    storage: context.storage()?,
                    track: track.clone(),
                }))
            })?
            .raft_class(child)?,
    );
    let mut options = configuration::options("native-sandbox-qualification");
    options.do_classes = vec!["Counter".into(), "Sandbox".into()];
    options.bindings = vec![("SANDBOX".into(), "Sandbox".into())];
    application.validate_deployment(&configuration::deployment(options.clone()))?;
    let mut fixture = Fixture::with_definition_policies(
        rooted,
        vec![
            (application.resolve_raft_class("Counter")?, minimum),
            (application.resolve_raft_class("Sandbox")?, target_minimum),
        ],
        application,
        store,
    )
    .await?;
    fixture.configuration = Some(options);
    // Private authenticated event envelopes require at least 4 KiB. Match the
    // fixture controller's existing 64 KiB command bound without changing
    // provider body bounds, concurrency or original execution deadlines.
    fixture.max_input_bytes = 64 * 1024;
    fixture.sql_workers = 1;
    // An interrupted parent and its private child retain two admissions. The
    // independent second-cell command needs one bounded slot of its own, as in
    // the shared binding progress fixtures; keep the same byte and SQL limits.
    fixture.ordinary_admission = Some((ORDINARY_COMMAND_SLOTS, 64 * 1024));
    fixture.effects = Some(RaftEffectOptions {
        callback_timeout_ms: 3000,
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
            CommandId::new(ClientId::new([224; 16])?, sequence)?,
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
    call_options(cluster, history, options(sequence, minimum)?, operation).await
}

async fn call_options(
    cluster: &ManagedCluster,
    history: &CellHistory,
    options: CommandOptions,
    operation: &str,
) -> Result<CommandReceipt> {
    let leader = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
    let cell = cluster.hosts[leader].cell(history.clone())?;
    let minimum = options.durability;
    let sequence = options.id.sequence();
    let started = cellar::asyncrt::wall_ms();
    let result = cell
        .command(
            &cell.contract("Run")?,
            serde_json::to_vec(operation)?,
            options,
        )
        .await;
    eprintln!(
        "sandbox_native_request {}",
        json!({"cell":history.cell.as_str(),"sequence":sequence,"operation":operation,"minimum":format!("{minimum:?}"),"started_unix_ms":started,"finished_unix_ms":cellar::asyncrt::wall_ms(),"success":result.is_ok()})
    );
    let receipt = result?;
    ensure!(
        receipt.durability >= minimum,
        "Sandbox command returned a weaker proof"
    );
    Ok(receipt)
}

async fn state(cluster: &ManagedCluster, history: &CellHistory, id: u64) -> Result<Value> {
    let leader = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
    let cell = cluster.hosts[leader].cell(history.clone())?;
    let mut proposal = [225; 16];
    proposal[..8].copy_from_slice(&id.to_be_bytes());
    let reply = cell
        .query(
            ProposalId::new(proposal)?,
            &cell.contract("SandboxState")?,
            vec![],
            QueryOptions::default(),
        )
        .await?;
    Ok(serde_json::from_slice(&reply.result)?)
}

fn unknown(error: &anyhow::Error, id: CommandId) -> Result<()> {
    ensure!(
        error
            .downcast_ref::<Arc<CellCommandError>>()
            .is_some_and(|e| e.ambiguous && e.command == id),
        "sandbox lost original ambiguous identity: {error:#}"
    );
    Ok(())
}

pub async fn qualify(outage: bool) -> Result<()> {
    run(if outage {
        Scenario::Outage
    } else {
        Scenario::Recovery
    })
    .await
}

pub async fn qualify_minimum_custody() -> Result<()> {
    run(Scenario::MinimumCustody).await
}

pub async fn profile_start() -> Result<()> {
    run(Scenario::StartProfile).await
}

pub async fn qualify_delete_input() -> Result<()> {
    run(Scenario::DeleteInput).await
}

enum Scenario {
    Recovery,
    Outage,
    MinimumCustody,
    StartProfile,
    DeleteInput,
}

async fn run(scenario: Scenario) -> Result<()> {
    let outage = matches!(scenario, Scenario::Outage);
    let minimum_custody = matches!(scenario, Scenario::MinimumCustody);
    let profile = matches!(scenario, Scenario::StartProfile);
    let delete_input = matches!(scenario, Scenario::DeleteInput);
    let _ = tracing_subscriber::fmt()
        .with_env_filter(if profile { "cellar::sandbox::execution=debug,cellar::raft::checkpoint=debug,cellar::raft::group_archive=debug,cellar::raft::sql_publish=trace" } else { "cellar::sandbox::execution=debug" })
        .with_test_writer()
        .try_init();
    let socket = PathBuf::from(std::env::var("CELLAR_DOCKER_BACKEND_SOCKET")?);
    let image = std::env::var("CELLAR_DOCKER_BACKEND_IMAGE")?;
    let mut failures = Vec::new();
    for rooted in [false, true] {
        for minimum in [Durability::Quorum, Durability::S3] {
            if profile && (!rooted || minimum != Durability::S3) {
                continue;
            }
            if minimum_custody && minimum != Durability::S3 {
                continue;
            }
            if delete_input && minimum != Durability::Quorum {
                continue;
            }
            let store = Arc::new(catalog_fault::Store::around(rustfs::store(
                &format!(
                    "native-sandbox-{}-{minimum:?}",
                    if minimum_custody {
                        "floor"
                    } else if outage {
                        "proof"
                    } else {
                        "recovery"
                    }
                ),
                rooted,
            )?));
            let root = tempfile::tempdir()?.keep();
            eprintln!(
                "retained shared Sandbox controller/backend: {}",
                root.display()
            );
            let policy = DockerOptions {
                directory: root.join("backend"),
                socket: socket.clone(),
                image: image.clone(),
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
            let backend = DockerBackend::create(
                domain.clone(),
                policy.clone(),
                Arc::new(docker_execution::NoDisasterAuthority),
            )
            .await?;
            let peer = Arc::new(Peer {
                backend: backend.clone(),
                retirement_cut: minimum_custody.then(|| store.clone()),
                creates: AtomicUsize::new(0),
                executes: AtomicUsize::new(0),
                writes: AtomicUsize::new(0),
                deletes: AtomicUsize::new(0),
                lose_exec: AtomicBool::new(false),
                hide_exec: AtomicBool::new(false),
                lose_delete: AtomicBool::new(false),
                hide_delete: AtomicBool::new(false),
                original_exec: Mutex::new(None),
                original_delete: Mutex::new(None),
                lookups: AtomicUsize::new(0),
                changed: Notify::new(),
                trace: profile,
            });
            let controller = ExecutionController::create(
                domain.clone(),
                ExecutionControllerOptions {
                    directory: root.join("controller"),
                    controller_id: [0; 16],
                    backend: backend.identity(),
                    limits: ExecutionControllerLimits::default(),
                },
                peer.clone(),
                [226; 32],
            )
            .await?;
            let track = Arc::new(Track::default());
            let mut cluster = match start(
                rooted,
                if minimum_custody {
                    Durability::Quorum
                } else {
                    minimum
                },
                if outage || minimum_custody {
                    Durability::S3
                } else {
                    minimum
                },
                store.clone(),
                controller.clone(),
                track.clone(),
                profile,
            )
            .await
            {
                Ok(cluster) => cluster,
                Err(error) => {
                    let a = controller.shutdown().await;
                    let b = backend.shutdown().await;
                    domain.shutdown().await;
                    failures.push(format!("Sandbox startup extents={rooted} minimum={minimum:?}: {error:#}; cleanup={a:?}/{b:?}"));
                    continue;
                }
            };
            let execution = cluster.fixtures[0].domain.clone();
            let result=execution.run(async {
            // The generic fixture selects only the first namespace. Provision
            // the execution namespace and its independent RF3/archive authority
            // before any public Sandbox call or archive fault is admitted.
            named_service::activate(&cluster,"sandbox-archive").await?;
            ensure!(cluster.hosts.iter().all(|host|host.resource_usage().host.controller.groups==2),"both Sandbox namespaces must have a live managed group");
            let history=|name:&str|->Result<CellHistory>{Ok(CellHistory{cluster:cluster.history.cluster,namespace:cluster.history.namespace,cell:CellId::new(format!("Counter:{name}"))?,incarnation:IncarnationId::new([227;16])?})};
            let a=history(if minimum_custody {"sandbox-floor"}else{"sandbox-parent"})?;let b=history("sandbox-independent")?;
            for (index,h) in [&a,&b].iter().enumerate() {let leader=managed_fixture::leader(&cluster.hosts,cluster.history).await?;cluster.hosts[leader].cell((*h).clone())?.enroll(CellEnrollmentRequest{creation:ProposalId::new([228+index as u8;16])?,durability:minimum},TIMEOUT).await?;}
            if outage {
                let before=store.denied_group_archive_writes.load(Ordering::Acquire);store.deny_group_archive_writes.store(true,Ordering::Release);
                let error=call(&cluster,&a,minimum,1,"Create").await.err().context("Sandbox creation bypassed required archive proof")?;
                unknown(&error,options(1,minimum)?.id)?;
                let prepared=state(&cluster,&a,0).await?;
                ensure!(prepared["job"]["state"]=="reserved" && prepared["job"]["generation"]==1,"archive refusal did not retain the original Sandbox reservation: {prepared}");
                ensure!(track.calls.load(Ordering::Acquire)==1 && store.denied_group_archive_writes.load(Ordering::Acquire)>before,"archive cut did not enter the public Sandbox callback/proof");
                ensure!(peer.creates.load(Ordering::Acquire)==0 && peer.executes.load(Ordering::Acquire)==0 && docker_execution::physical_resources(&domain,&policy).await?==0,"Sandbox entered physical work without source/target S3 proof");
                store.deny_group_archive_writes.store(false,Ordering::Release);
            } else if delete_input {
                let created=call(&cluster,&a,Durability::Quorum,1,"Create").await?;
                let deleted=call(&cluster,&a,Durability::Quorum,2,"DestroyJob").await?;
                let cuts=deleted.version.sequence.get()-created.version.sequence.get();
                eprintln!("sandbox_delete_input_checkpoints {}",json!({"extents":rooted,"before":created.version.sequence.get(),"after":deleted.version.sequence.get(),"cuts":cuts,"required_cuts":3}));
                ensure!(peer.creates.load(Ordering::Acquire)==1 && peer.deletes.load(Ordering::Acquire)==1);
                let floor=history("sandbox-floor")?;
                let leader=managed_fixture::leader(&cluster.hosts,cluster.history).await?;
                cluster.hosts[leader].cell(floor.clone())?.enroll(CellEnrollmentRequest{creation:ProposalId::new([230;16])?,durability:Durability::Quorum},TIMEOUT).await?;
                call(&cluster,&floor,Durability::Quorum,1,"Create").await?;
                let denied=store.denied_sandbox_parent_writes.load(Ordering::Acquire);
                store.deny_sandbox_parent_writes.store(true,Ordering::Release);
                let error=call(&cluster,&floor,Durability::S3,2,"DestroyJob").await.err().context("Sandbox Delete bypassed its source S3 input proof")?;
                unknown(&error,options(2,Durability::S3)?.id)?;
                let pending=state(&cluster,&floor,0).await?;
                ensure!(pending["job"]["state"]=="deleting" && pending["cleanup_pending"]==true,"Delete did not retain exact cleanup debt: {pending}");
                ensure!(store.denied_sandbox_parent_writes.load(Ordering::Acquire)>denied,"Delete input archive cut was not reached");
                ensure!(peer.creates.load(Ordering::Acquire)==2 && peer.deletes.load(Ordering::Acquire)==1,"Delete entered before its parent input proof");
                track.refuse.store(true,Ordering::Release);
                let factories=track.factories.load(Ordering::Acquire);
                named_service::restart(&mut cluster).await?;
                ensure!(peer.deletes.load(Ordering::Acquire)==1,"retained restart downgraded original S3 Delete input");
                store.deny_sandbox_parent_writes.store(false,Ordering::Release);
                cellar::asyncrt::timeout(Duration::from_secs(60),async {let mut id=1;loop {
                    let status=state(&cluster,&floor,id).await?;id+=1;
                    if status["job"]["state"]=="deleted" && status["cleanup_pending"]==false {break;}
                    cellar::asyncrt::sleep(Duration::from_millis(50)).await;
                }Ok::<_,anyhow::Error>(())}).await.context("Delete did not recover autonomously after source archive restoration")??;
                ensure!(track.factories.load(Ordering::Acquire)==factories,"Delete recovery constructed unavailable parent");
                ensure!(peer.creates.load(Ordering::Acquire)==2 && peer.deletes.load(Ordering::Acquire)==2 && peer.executes.load(Ordering::Acquire)==0,"Delete recovery replayed or lost a physical mutation");
                ensure!(docker_execution::physical_resources(&domain,&policy).await?==0);
                ensure!(cuts==3,"Sandbox Delete must combine deletion intent and child-call intent before target I/O, then prove terminal metadata and its original public response; got {cuts} source cuts");
            } else if minimum_custody {
                call(&cluster,&a,Durability::S3,1,"Create").await?;
                let retirement=call(&cluster,&a,Durability::S3,2,"Retire").await;
                if let Err(error)=retirement {unknown(&error,options(2,Durability::S3)?.id)?;}
                cellar::asyncrt::timeout(Duration::from_secs(60),async {loop {
                    if peer.deletes.load(Ordering::Acquire)==1 && store.denied_sandbox_parent_writes.load(Ordering::Acquire)>0 {break;}
                    cellar::asyncrt::sleep(Duration::from_millis(50)).await;
                }}).await.context("actual Delete/parent S3 archive cut not reached")?;
                track.refuse.store(true,Ordering::Release);let factories=track.factories.load(Ordering::Acquire);
                named_service::restart(&mut cluster).await?;
                let start=std::time::Instant::now();let mut id=1;
                while std::time::Instant::now().duration_since(start)<Duration::from_secs(10) {
                    let status=state(&cluster,&a,id).await?;id+=1;
                    ensure!(status["cleanup_pending"]==true,"explicit S3 retirement lost proof debt during archive outage in Quorum namespace: {status}");
                    cellar::asyncrt::sleep(Duration::from_millis(50)).await;
                }
                store.deny_sandbox_parent_writes.store(false,Ordering::Release);
                cellar::asyncrt::timeout(Duration::from_secs(60),async {loop {
                    let status=state(&cluster,&a,id).await?;id+=1;
                    if status["parent"]["state"]=="deleted" && status["job"]["state"]=="deleted" && status["cleanup_pending"]==false {break;}
                    cellar::asyncrt::sleep(Duration::from_millis(50)).await;
                }Ok::<_,anyhow::Error>(())}).await.context("S3 retirement did not autonomously confirm after RustFS restoration")??;
                ensure!(track.factories.load(Ordering::Acquire)==factories,"cleanup constructed unavailable parent");
                ensure!(peer.creates.load(Ordering::Acquire)==1 && peer.deletes.load(Ordering::Acquire)==1 && peer.executes.load(Ordering::Acquire)==0,"minimum custody replayed physical mutation");
                ensure!(docker_execution::physical_resources(&domain,&policy).await?==0);
            } else {
                let first=call(&cluster,&a,minimum,1,"Create").await?;let allocation:SandboxAllocation=serde_json::from_slice(&first.result)?;
                ensure!(allocation.generation==1 && allocation.state==SandboxAllocationState::Ready);
                if profile {
                    ensure!(peer.creates.load(Ordering::Acquire)==1 && peer.executes.load(Ordering::Acquire)==0,"diagnostic must use the original first Start");
                    eprintln!("sandbox_start_profile {{\"extents\":true,\"minimum\":\"S3\",\"sequence\":1,\"original_request_ms\":5000}}");
                    return Ok(());
                }
                let output=call(&cluster,&a,minimum,2,"Exec").await?;let output:Value=serde_json::from_slice(&output.result)?;ensure!(STANDARD.decode(output["stdout"].as_str().context("Sandbox stdout")?)?==b"sandbox");
                peer.lose_exec.store(true,Ordering::Release);peer.hide_exec.store(true,Ordering::Release);
                let lost=call(&cluster,&a,minimum,3,"Exec").await.err().context("Sandbox exec reply loss not reached")?;unknown(&lost,options(3,minimum)?.id)?;
                ensure!(peer.executes.load(Ordering::Acquire)==2 && peer.original_exec.lock().unwrap().is_some());
                call(&cluster,&b,minimum,1,"Status").await?;named_service::restart(&mut cluster).await?;
                ensure!(call(&cluster,&a,minimum,4,"Read").await.is_err(),"hidden original execution receipt admitted Sandbox work");
                ensure!(peer.lookups.load(Ordering::Acquire)>0,"original Sandbox receipt was not consulted");
                peer.hide_exec.store(false,Ordering::Release);call(&cluster,&a,minimum,5,"Read").await?;
                ensure!(peer.executes.load(Ordering::Acquire)==2,"Sandbox recovery replayed compute");
                // Separate identities belong to new file operations; the
                // original execution/recovery command IDs remain unchanged.
                for (index,operation) in ["FileWrite","FileRead","FileStat","FileList"].into_iter().enumerate() {
                    let sequence=index as u64+1;
                    let request=CommandOptions { id:CommandId::new(ClientId::new([229;16])?,sequence)?,..options(sequence,minimum)? };
                    let result=call_options(&cluster,&a,request,operation).await?;
                    let value:Value=serde_json::from_slice(&result.result)?;
                    match operation {
                        "FileRead"=>ensure!(STANDARD.decode(value.as_str().context("Sandbox file bytes")?)?==b"bridge"),
                        "FileStat"=>ensure!(value["size"]==6 && value["mode"]==384,"Sandbox file metadata differs: {value}"),
                        "FileList"=>ensure!(value.as_array().context("Sandbox listing")?.iter().any(|entry|entry["name"]=="bridge"),"Sandbox listing lost admitted file"),
                        _=>ensure!(value.is_null()),
                    }
                }
                ensure!(peer.writes.load(Ordering::Acquire)==1,"Sandbox bridge duplicated file write");
                call(&cluster,&a,minimum,6,"Forge").await?;ensure!(peer.executes.load(Ordering::Acquire)==2);
                call(&cluster,&a,minimum,7,"Spare").await?;call(&cluster,&a,minimum,8,"DestroyJob").await?;
                let next=call(&cluster,&a,minimum,9,"Create").await?;let next:SandboxAllocation=serde_json::from_slice(&next.result)?;
                ensure!(next.generation==2 && next.execution_scope!=allocation.execution_scope,"Sandbox generation reused an old child identity");
                peer.lose_delete.store(true,Ordering::Release);peer.hide_delete.store(true,Ordering::Release);
                call(&cluster,&a,minimum,10,"Retire").await?;
                cellar::asyncrt::timeout(Duration::from_secs(60),async {loop {let changed=peer.changed.notified();if peer.original_delete.lock().unwrap().is_some(){break;}changed.await;}}).await.context("autonomous Sandbox Delete never entered")?;
                track.refuse.store(true,Ordering::Release);let factories=track.factories.load(Ordering::Acquire);
                named_service::restart(&mut cluster).await?;peer.hide_delete.store(false,Ordering::Release);
                cellar::asyncrt::timeout(Duration::from_secs(60),async {let mut id=1;loop {let status=state(&cluster,&a,id).await?;id+=1;if status["parent"]["state"]=="deleted" && status["cleanup_pending"]==false && status["job"]["state"]=="deleted" && status["spare"]["state"]=="deleted" {break;}cellar::asyncrt::sleep(Duration::from_millis(50)).await;}Ok::<_,anyhow::Error>(())}).await.context("retained Sandbox cleanup did not prove terminal metadata")??;
                ensure!(track.factories.load(Ordering::Acquire)==factories,"autonomous Sandbox cleanup constructed business code");
                ensure!(peer.creates.load(Ordering::Acquire)==3 && peer.deletes.load(Ordering::Acquire)==3 && peer.executes.load(Ordering::Acquire)==2,"Sandbox cleanup replayed a mutation or missed an exact generation");
                ensure!(docker_execution::physical_resources(&domain,&policy).await?==0);
                let old=track.old.lock().unwrap().clone().context("retired parent Context")?;ensure!(SandboxCatalog::open(&old).is_err(),"old Sandbox Context borrowed successor authority");
                track.refuse.store(false,Ordering::Release);ensure!(call(&cluster,&a,minimum,11,"Create").await.is_err(),"retired Sandbox parent reopened admission");
                ensure!(peer.creates.load(Ordering::Acquire)==3);
            }
            if delete_input {
                eprintln!("sandbox_delete_input_configuration {}",json!({"extents":rooted,"namespace_minimum":"Quorum","delete_minimum":"S3","original_request_ms":5000,"real_engine":true,"ordinary_command_slots":ORDINARY_COMMAND_SLOTS,"sql_workers":1}));
            } else if minimum_custody {
                eprintln!("sandbox_floor_configuration {}",json!({"extents":rooted,"minimum":"S3","namespace_minimum":"Quorum","target_minimum":"S3","original_request_ms":5000,"real_engine":true,"denied_parent_archive_writes":store.denied_sandbox_parent_writes.load(Ordering::Acquire),"ordinary_command_slots":ORDINARY_COMMAND_SLOTS,"sql_workers":1}));
            } else {
            eprintln!("sandbox_native_configuration {}",json!({"extents":rooted,"minimum":format!("{minimum:?}"),"target_minimum":if outage{"S3"}else{if minimum==Durability::S3{"S3"}else{"Quorum"}},"archive_outage":outage,"original_request_ms":5000,"real_engine":true,"business_constructor_cleanup":false,"ordinary_command_slots":ORDINARY_COMMAND_SLOTS,"sql_workers":1}));
            }
            Ok::<_,anyhow::Error>(())
        }).await.map_err(anyhow::Error::from).and_then(std::convert::identity);
            store
                .deny_group_archive_writes
                .store(false, Ordering::Release);
            store
                .deny_sandbox_parent_writes
                .store(false, Ordering::Release);
            peer.hide_exec.store(false, Ordering::Release);
            peer.hide_delete.store(false, Ordering::Release);
            let closed = managed_fixture::close_group_nodes_with_groups(
                std::mem::take(&mut cluster.nodes),
                &cluster.hosts,
                2,
            )
            .await;
            for root in std::mem::take(&mut cluster.roots) {
                eprintln!(
                    "retained shared Sandbox voter disk: {}",
                    root.keep().display()
                );
            }
            execution.shutdown().await;
            let a = controller.shutdown().await;
            let b = backend.shutdown().await;
            domain.shutdown().await;
            if let Err(error) = result.and(closed).and(a).and(b) {
                failures.push(format!(
                    "Sandbox extents={rooted} minimum={minimum:?} outage={outage}: {error:#}; physical creates={} executes={} writes={} deletes={} original-lookups={} parent-calls={}",peer.creates.load(Ordering::Acquire),peer.executes.load(Ordering::Acquire),peer.writes.load(Ordering::Acquire),peer.deletes.load(Ordering::Acquire),peer.lookups.load(Ordering::Acquire),track.calls.load(Ordering::Acquire)
                ));
            }
        }
    }
    ensure!(
        failures.is_empty(),
        "Shared Sandbox configurations failed: {}",
        failures.join("; ")
    );
    Ok(())
}
