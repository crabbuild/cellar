// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual ordinary SDK sessions, RF3 and independent controller storage. The
//! controlled Linux-protocol peer is not physical provider/process fencing.
use super::*;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::sdk::containers::*;
use cellar_container::{
    controller::{Reply, Request},
    ControllerProvider, Operation,
};
use sha2::Digest;
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicBool, RwLock},
};

#[path = "cell_group_provider_admission.rs"]
mod admission;
#[path = "cell_group_provider_delete_intent.rs"]
mod delete_intent;
#[path = "cell_group_provider_lifecycle_wait.rs"]
mod lifecycle_wait;
#[path = "cell_group_native_container.rs"]
mod native_container;
#[path = "cell_group_provider_remote.rs"]
mod remote;
#[path = "cell_group_provider_retirement.rs"]
mod retirement;

#[derive(Default)]
struct Resource {
    id: String,
    exists: bool,
    files: BTreeMap<String, Vec<u8>>,
}

struct PeerReceipt {
    original: ExecutionPendingOperation,
    bytes: Vec<u8>,
}

#[derive(Default)]
struct Peer {
    active: Mutex<Option<ExecutionClaim>>,
    resource: Mutex<Resource>,
    receipts: Mutex<BTreeMap<[u8; 16], PeerReceipt>>,
    creates: AtomicUsize,
    deletes: AtomicUsize,
    calls: AtomicUsize,
    writes: AtomicUsize,
    executions: AtomicUsize,
    lose_create: AtomicBool,
    lose_delete: AtomicBool,
    lose_write: AtomicBool,
    lose_exec: AtomicBool,
    lost_data: Mutex<Option<ExecutionPendingOperation>>,
    recovered_data: Mutex<Option<ExecutionPendingOperation>>,
    hide_data_receipt: AtomicBool,
    data_lookups: AtomicUsize,
    data_changed: Notify,
    expire_activation: AtomicBool,
    refuse_retirement_archive: Mutex<Option<Arc<catalog_fault::Store>>>,
    hold_retirement_fence: AtomicBool,
    retirement_fences: AtomicUsize,
    retirement_fence_changed: Notify,
}

impl ExecutionControllerBackend for Peer {
    fn identity(&self) -> BackendIdentity {
        BackendIdentity {
            binding: "TOOL".into(),
            provider: "native-protocol-fixture".into(),
            configuration_digest: hex::encode([240; 32]),
        }
    }

    fn revoke(
        self: Arc<Self>,
        _previous: Option<ExecutionClaim>,
        successor: Option<ExecutionClaim>,
        _unresolved: Vec<ExecutionPendingOperation>,
    ) -> HandlerFuture<()> {
        Box::pin(async move {
            if successor
                .as_ref()
                .is_some_and(|claim| claim.checkpoint().record.is_retired())
            {
                self.retirement_fences.fetch_add(1, Ordering::AcqRel);
                self.retirement_fence_changed.notify_waiters();
                loop {
                    let changed = self.retirement_fence_changed.notified();
                    if !self.hold_retirement_fence.load(Ordering::Acquire) {
                        break;
                    }
                    changed.await;
                }
            }
            if self.expire_activation.swap(false, Ordering::AcqRel) {
                // Admission has already proved the intent. Cross its unchanged
                // 500 ms retention cap before allowing provider dispatch.
                cellar::asyncrt::sleep(Duration::from_millis(600)).await;
            }
            *self.active.lock().unwrap() = successor;
            Ok(())
        })
    }

    fn revoke_history(
        self: Arc<Self>,
        _old: CellHistory,
        _successor: CellHistory,
        _previous: Option<ExecutionClaim>,
        _unresolved: Vec<ExecutionPendingOperation>,
    ) -> HandlerFuture<()> {
        Box::pin(async {
            anyhow::bail!("physical infrastructure fencing is outside this controlled peer")
        })
    }

    fn execute(
        self: Arc<Self>,
        claim: ExecutionClaim,
        operation: ExecutionOperation,
        payload: Vec<u8>,
    ) -> HandlerFuture<ExecutionOutcome> {
        Box::pin(async move {
            eprintln!(
                "controller_provider_backend_start kind={:?} id={} remaining_ms={}",
                operation.kind,
                hex::encode(operation.id),
                operation
                    .expires_unix_ms
                    .saturating_sub(cellar::asyncrt::wall_ms().max(0) as u64)
            );
            ensure!(
                self.active.lock().unwrap().as_ref() == Some(&claim),
                "stale provider request"
            );
            let request = Request::decode(
                &claim,
                &operation,
                &payload,
                &cellar_container::Limits {
                    max_body_bytes: 4096,
                },
            )?;
            let method = Operation::parse(&request.operation)?;
            let bytes = {
                let mut resource = self.resource.lock().unwrap();
                if method != Operation::Start {
                    ensure!(
                        claim.checkpoint().record.resource_id.as_deref()
                            == Some(resource.id.as_str()),
                        "request targets a foreign resource"
                    );
                }
                let value = match method {
                    Operation::Start => {
                        ensure!(!resource.exists, "create was replayed");
                        resource.id = hex::encode(sha2::Sha256::digest(
                            claim.checkpoint().record.request_id.as_bytes(),
                        ));
                        resource.exists = true;
                        resource.files.clear();
                        self.creates.fetch_add(1, Ordering::AcqRel);
                        json!(claim.checkpoint().record.generation.to_string())
                    }
                    Operation::Destroy => {
                        ensure!(
                            resource.exists
                                && claim.checkpoint().record.resource_id.as_deref()
                                    == Some(resource.id.as_str()),
                            "delete targets another resource"
                        );
                        resource.exists = false;
                        self.deletes.fetch_add(1, Ordering::AcqRel);
                        serde_json::Value::Null
                    }
                    Operation::Running => json!(resource.exists),
                    Operation::WriteFile => {
                        ensure!(resource.exists);
                        self.writes.fetch_add(1, Ordering::AcqRel);
                        resource.files.insert(
                            request.input["path"].as_str().unwrap().into(),
                            STANDARD.decode(request.input["data"].as_str().unwrap())?,
                        );
                        serde_json::Value::Null
                    }
                    Operation::ReadFile => json!(STANDARD.encode(
                        resource
                            .files
                            .get(request.input["path"].as_str().unwrap())
                            .context("fixture file missing")?
                    )),
                    Operation::ExecOutput => {
                        self.executions.fetch_add(1, Ordering::AcqRel);
                        json!({"exit_code":0,"stdout":STANDARD.encode(b"controlled result"),"stderr":""})
                    }
                    _ => anyhow::bail!("unexpected fixture operation"),
                };
                serde_json::to_vec(&Reply {
                    format: 1,
                    resource_id: resource.id.clone(),
                    value,
                })?
            };
            self.calls.fetch_add(1, Ordering::AcqRel);
            ensure!(
                self.receipts
                    .lock()
                    .unwrap()
                    .insert(
                        operation.id,
                        PeerReceipt {
                            original: ExecutionPendingOperation {
                                claim: claim.clone(),
                                operation: operation.clone(),
                            },
                            bytes: bytes.clone(),
                        }
                    )
                    .is_none(),
                "provider submission replayed an operation ID"
            );
            eprintln!(
                "controller_provider_backend_completed kind={:?} id={}",
                operation.kind,
                hex::encode(operation.id)
            );
            if method == Operation::Destroy {
                if let Some(store) = self.refuse_retirement_archive.lock().unwrap().take() {
                    store
                        .deny_group_archive_writes
                        .store(true, Ordering::Release);
                }
            }
            if (method == Operation::Start && self.lose_create.swap(false, Ordering::AcqRel))
                || (method == Operation::Destroy && self.lose_delete.swap(false, Ordering::AcqRel))
            {
                // The independent peer already owns an exact outcome. Cancellation
                // loses its reply; recovery observes the original receipt only.
                std::future::pending::<()>().await;
            }
            if (method == Operation::WriteFile && self.lose_write.swap(false, Ordering::AcqRel))
                || (method == Operation::ExecOutput && self.lose_exec.swap(false, Ordering::AcqRel))
            {
                *self.lost_data.lock().unwrap() =
                    Some(ExecutionPendingOperation { claim, operation });
                std::future::pending::<()>().await;
            }
            Ok(ExecutionOutcome::Completed(bytes))
        })
    }

    fn reconcile(
        self: Arc<Self>,
        current: ExecutionClaim,
        original: ExecutionPendingOperation,
    ) -> HandlerFuture<Option<ExecutionOutcome>> {
        Box::pin(async move {
            ensure!(
                self.active.lock().unwrap().as_ref() == Some(&current),
                "stale receipt lookup"
            );
            let receipts = self.receipts.lock().unwrap();
            match receipts.get(&original.operation.id) {
                Some(receipt) => {
                    ensure!(
                        receipt.original == original,
                        "original provider receipt differs"
                    );
                    if self.lost_data.lock().unwrap().as_ref() == Some(&original) {
                        let hidden = self.hide_data_receipt.load(Ordering::Acquire);
                        self.data_lookups.fetch_add(1, Ordering::AcqRel);
                        self.data_changed.notify_waiters();
                        if hidden {
                            return Ok(None);
                        }
                        *self.recovered_data.lock().unwrap() = Some(original);
                    }
                    Ok(Some(ExecutionOutcome::Completed(receipt.bytes.clone())))
                }
                None => Ok(None),
            }
        })
    }
}

// Supervision retains its authority at registration. Model a stable host-owned
// endpoint, not an old closed controller handle, across independent restart.
// Only the endpoint changes: every supplied claim remains the original claim.
struct Authority(Arc<RwLock<Arc<ControllerProvider>>>);

impl ExecutionFenceAuthority for Authority {
    fn identity(&self) -> BackendIdentity {
        self.0.read().unwrap().identity()
    }

    fn activate(self: Arc<Self>, claim: ExecutionClaim) -> HandlerFuture<()> {
        let authority = self.0.read().unwrap().authority();
        authority.activate(claim)
    }
}

struct Route {
    provider: Arc<RwLock<Arc<ControllerProvider>>>,
    factory_refused: AtomicBool,
    retired: AtomicBool,
    changed: Notify,
    staged: AtomicUsize,
    store: Arc<catalog_fault::Store>,
    old_context: Mutex<Option<Context>>,
    wait: Arc<lifecycle_wait::Wait>,
}

impl Route {
    fn selected(&self) -> Arc<ControllerProvider> {
        self.provider.read().unwrap().clone()
    }
}

impl FencedMaintenanceBackend for Route {
    fn identity(&self) -> BackendIdentity {
        self.selected().identity()
    }

    fn authority(&self) -> Arc<dyn ExecutionFenceAuthority> {
        Arc::new(Authority(self.provider.clone()))
    }

    fn prepare(
        &self,
        expected: &BindingCheckpoint,
        action: MaintenanceAction,
    ) -> Result<MaintenancePreparation> {
        self.selected().prepare(expected, action)
    }

    fn maintain(
        self: Arc<Self>,
        context: Context,
        expected: BindingCheckpoint,
        action: MaintenanceAction,
        grant: ExecutionGrant,
    ) -> HandlerFuture<()> {
        let provider = self.selected();
        Box::pin(async move {
            let confirmation = expected.record.is_retired().then(|| expected.clone());
            if expected.record.is_retired() {
                let entered = Arc::new(AtomicBool::new(false));
                let observe = entered.clone();
                let refused = grant
                    .dispatch(&context, move |_| async move {
                        observe.store(true, Ordering::Release);
                        Ok(())
                    })
                    .await;
                ensure!(
                    refused
                        .err()
                        .is_some_and(|error| error.downcast_ref::<ExecutionControllerFailure>()
                            == Some(&ExecutionControllerFailure::StaleClaim))
                        && !entered.load(Ordering::Acquire),
                    "retired grant admitted provider I/O"
                );
            }
            provider
                .maintain(context.clone(), expected, action, grant)
                .await?;
            // A Delete turn can prove retired metadata before the separate
            // framework turn fences the controller. Report only confirmation
            // entered with that exact already-fenced retired checkpoint.
            if let Some(expected) = confirmation {
                ensure!(
                    BindingJournal::open(&context, "TOOL")?
                        .load(&context)?
                        .as_ref()
                        == Some(&expected),
                    "retirement confirmation checkpoint changed"
                );
                self.retired.store(true, Ordering::Release);
                self.changed.notify_waiters();
            }
            Ok(())
        })
    }
}

struct Root(Storage, Arc<Route>);

impl Cell for Root {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc {
                args: RpcData::Json(input),
                ..
            } = invocation
            else {
                anyhow::bail!("JSON RPC required")
            };
            let operation: String = serde_json::from_str(&input)?;
            let mut bindings = cellar_container::Bindings::default();
            bindings.insert("TOOL", self.1.selected())?;
            let sessions = bindings.sessions();
            let session = sessions.get("TOOL")?;
            let limits = cellar_container::Limits {
                max_body_bytes: 4096,
            };
            let journal = BindingJournal::open(&context, "TOOL")?;
            match operation.as_str() {
                "HoldLifecycle" => lifecycle_wait::hold(&context, &journal, &self.1).await?,
                "WaitLifecycle" | "WaitDirtyLifecycle" => {
                    lifecycle_wait::wait(
                        &context,
                        &journal,
                        &self.1,
                        operation == "WaitDirtyLifecycle",
                    )
                    .await?;
                }
                "Create" => {
                    self.0.put_json("business", &json!(17))?;
                    *self.1.old_context.lock().unwrap() = Some(context.clone());
                    ensure!(
                        session
                            .execute(&context, Operation::Start, json!({"options":{}}), &limits)
                            .await?
                            == json!("1")
                    );
                }
                "Data" => {
                    let input =
                        json!({"path":"/tmp/value","data":STANDARD.encode([0,255,42]),"mode":416});
                    session
                        .execute(&context, Operation::WriteFile, input, &limits)
                        .await?;
                    let bytes = session
                        .execute(
                            &context,
                            Operation::ReadFile,
                            json!({"path":"/tmp/value","max_bytes":3}),
                            &limits,
                        )
                        .await?;
                    ensure!(bytes == json!(STANDARD.encode([0, 255, 42])));
                    // A second session in this same invocation must not reuse
                    // the first session's automatic operation ordinal.
                    let more = bindings.sessions();
                    more.get("TOOL")?.execute(&context,Operation::WriteFile,json!({"path":"/tmp/second","data":STANDARD.encode(b"second"),"mode":384}),&limits).await?;
                    let output = session
                        .execute(
                            &context,
                            Operation::ExecOutput,
                            json!({"options":{"cmd":["controlled"]},"max_bytes":4096}),
                            &limits,
                        )
                        .await?;
                    ensure!(output["stdout"] == STANDARD.encode(b"controlled result"));
                }
                "StageDeleteIntent" => {
                    let _guard = journal.lock_lifecycle(&context).await?;
                    let current = journal.load(&context)?.context("binding missing")?;
                    let mut record = current.record;
                    record.intent = BindingIntent::Deleted;
                    record.observation = BindingObservation::Pending;
                    // Model loss after this exact intent is durable, before
                    // any controller Delete reservation or submission.
                    journal
                        .compare_exchange(&context, Some(current.revision), record)
                        .await?;
                }
                "MaintainDeleteIntent" => {
                    let guard = journal.lock_lifecycle(&context).await?;
                    let expected = journal.load(&context)?.context("binding missing")?;
                    let provider = self.1.selected();
                    let grant = guard
                        .fence_execution(&context, provider.authority())
                        .await?;
                    ensure!(grant.claim().checkpoint() == &expected);
                    // The generic coordinator selects Inspect for an already
                    // persisted deletion intent. Exercise that exact adapter.
                    provider
                        .maintain(context.clone(), expected, MaintenanceAction::Inspect, grant)
                        .await?;
                }
                "MaintainRetirement" => {
                    let entered = self
                        .0
                        .get_json("destroy-entered")?
                        .and_then(|value| value.as_u64())
                        .unwrap_or(0);
                    self.0.put_json("destroy-entered", &json!(entered + 1))?;
                    let guard = journal.lock_lifecycle(&context).await?;
                    let current = journal.load(&context)?.context("binding missing")?;
                    let mut record = current.record;
                    record.intent = BindingIntent::Deleted;
                    record.observation = BindingObservation::Pending;
                    let expected =
                        journal.prepare_exchange(&context, Some(current.revision), record)?;
                    let provider = self.1.selected();
                    let grant = guard
                        .fence_execution(&context, provider.authority())
                        .await?;
                    provider
                        .maintain(context.clone(), expected, MaintenanceAction::Inspect, grant)
                        .await?;
                }
                "Destroy" => {
                    session
                        .execute(&context, Operation::Destroy, json!({}), &limits)
                        .await?;
                }
                "Reconcile" => {
                    session
                        .execute(&context, Operation::Reconcile, json!({}), &limits)
                        .await?;
                }
                "StageOutput" => {
                    let current = journal.load(&context)?.context("binding missing")?;
                    journal.prepare_exchange(&context, Some(current.revision), current.record)?;
                    self.1.staged.fetch_add(1, Ordering::AcqRel);
                    self.1
                        .store
                        .deny_group_archive_writes
                        .store(true, Ordering::Release);
                }
                "Read" => ensure!(self.0.get_json("business")? == Some(json!(17))),
                "ReadRetirement" => {
                    ensure!(self.0.get_json("business")? == Some(json!(17)));
                    ensure!(self.0.get_json("destroy-entered")? == Some(json!(1)));
                    ensure!(journal
                        .load(&context)?
                        .is_some_and(|row| row.record.is_retired()));
                }
                _ => anyhow::bail!("unknown operation"),
            }
            let current = journal.load(&context)?.context("binding missing")?;
            Ok(Output::Rpc(RpcData::Json(
                json!({"revision":current.revision,"record":current.record}).to_string(),
            )))
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scenario {
    Basic,
    Interrupted,
    Expired,
    LostWrite,
    LostExec,
}

async fn start(
    rooted: bool,
    minimum: Durability,
    route: Arc<Route>,
    scenario: Scenario,
) -> Result<ManagedCluster> {
    let identity = route.selected().identity();
    let class = RaftClass::new("Counter", 1, [241; 32])?
        .initializer([242; 32], |_, _| Ok(()))?
        .json_cell_rpc("Run", [243; 32])?
        .query("Version", [244; 32], |_, _| Ok(vec![]))?
        .provider_binding(
            identity.binding,
            identity.provider,
            hex::decode(identity.configuration_digest)?
                .try_into()
                .map_err(|_| anyhow::anyhow!("invalid controller identity"))?,
        )?;
    let factory = route.clone();
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |context, _| {
                ensure!(
                    !factory.factory_refused.load(Ordering::Acquire),
                    "business constructor unavailable"
                );
                Ok(Arc::new(Root(context.storage()?, factory.clone())))
            },
        )?,
    );
    let application: Arc<dyn Application> = if scenario != Scenario::Basic {
        let mut supervision = Supervision::default();
        supervision.register_fenced(
            route.clone(),
            MaintenancePolicy {
                inspect_interval_ms: 100,
                maximum_lifetime_ms: if scenario == Scenario::Expired {
                    500
                } else {
                    10_000
                },
                attempt_timeout_ms: 3000,
                idle_timeout_ms: None,
            },
        )?;
        supervise(application, supervision)
    } else {
        application
    };
    let mut options = configuration::options("controller-provider");
    options.do_classes = vec!["Counter".into()];
    application.validate_deployment(&configuration::deployment(options.clone()))?;
    let definition = application.resolve_raft_class("Counter")?;
    let mut fixture = Fixture::with_definition_policies(
        rooted,
        vec![(definition, minimum)],
        application,
        route.store.clone(),
    )
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

async fn call(
    cluster: &ManagedCluster,
    history: &CellHistory,
    minimum: Durability,
    seq: u64,
    operation: &str,
) -> Result<CommandReceipt> {
    let leader = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
    let cell = cluster.hosts[leader].cell(history.clone())?;
    Ok(cell
        .command(
            &cell.contract("Run")?,
            serde_json::to_vec(operation)?,
            bindings::options(seq, minimum)?,
        )
        .await?)
}

pub async fn qualify(real: bool, recovery: bool) -> Result<()> {
    selected(
        real,
        if recovery {
            Scenario::Interrupted
        } else {
            Scenario::Basic
        },
    )
    .await
}

pub async fn qualify_native_container(real: bool, outage: bool) -> Result<()> {
    native_container::qualify(real, outage).await
}

pub async fn qualify_native_restart(real: bool, archive: bool) -> Result<()> {
    native_container::qualify_restart(real, archive).await
}

pub async fn qualify_expiry(real: bool) -> Result<()> {
    selected(real, Scenario::Expired).await
}

pub async fn qualify_data_recovery(real: bool, write: bool) -> Result<()> {
    selected(
        real,
        if write {
            Scenario::LostWrite
        } else {
            Scenario::LostExec
        },
    )
    .await
}

pub async fn qualify_lifecycle_wait(real: bool, dirty: bool) -> Result<()> {
    lifecycle_wait::qualify(real, dirty).await
}

pub async fn profile_extent_create(real: bool) -> Result<()> {
    lifecycle_wait::profile_create(real).await
}

pub async fn qualify_delete_intent(real: bool, maintenance: bool) -> Result<()> {
    delete_intent::qualify(real, maintenance).await
}

pub async fn qualify_remote(real: bool) -> Result<()> {
    delete_intent::qualify_remote(real).await
}

pub async fn qualify_retirement_confirmation(real: bool) -> Result<()> {
    retirement::qualify(real).await
}

pub async fn qualify_retirement_order(real: bool) -> Result<()> {
    retirement::qualify_order(real).await
}

pub async fn qualify_admission(real: bool, archive_outage: bool) -> Result<()> {
    admission::qualify(real, archive_outage).await
}

async fn selected(real: bool, scenario: Scenario) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("cellar::raft::checkpoint=debug,cellar::raft::resource_read=debug")
        .with_ansi(false)
        .try_init();
    let recovery = scenario != Scenario::Basic;
    let expiry = scenario == Scenario::Expired;
    let lost_write = scenario == Scenario::LostWrite;
    let lost_exec = scenario == Scenario::LostExec;
    let data_loss = lost_write || lost_exec;
    for rooted in [false, true] {
        for minimum in [Durability::Quorum, Durability::S3] {
            let peer = Arc::new(Peer::default());
            let root = tempfile::tempdir()?.keep();
            eprintln!("retained controller-provider state: {}", root.display());
            let mut controller_domain = Arc::new(cellar::sdk::ExecutionDomain::new(
                tokio::runtime::Handle::current(),
            ));
            let mut controller = ExecutionController::create(
                controller_domain.clone(),
                ExecutionControllerOptions {
                    directory: root.join("controller"),
                    controller_id: [0; 16],
                    backend: peer.identity(),
                    limits: ExecutionControllerLimits::default(),
                },
                peer.clone(),
                [245; 32],
            )
            .await?;
            let resume = controller.options();
            let store = Arc::new(if real {
                catalog_fault::Store::around(rustfs::store(
                    match (scenario, minimum) {
                        (Scenario::Expired, Durability::S3) => "controller-provider-expiry-s3",
                        (Scenario::Expired, _) => "controller-provider-expiry-quorum",
                        (Scenario::Interrupted, Durability::S3) => {
                            "controller-provider-recovery-s3"
                        }
                        (Scenario::Interrupted, _) => "controller-provider-recovery-quorum",
                        (Scenario::LostWrite, Durability::S3) => "controller-provider-write-s3",
                        (Scenario::LostWrite, _) => "controller-provider-write-quorum",
                        (Scenario::LostExec, Durability::S3) => "controller-provider-exec-s3",
                        (Scenario::LostExec, _) => "controller-provider-exec-quorum",
                        (Scenario::Basic, Durability::S3) => "controller-provider-s3",
                        (Scenario::Basic, _) => "controller-provider-quorum",
                    },
                    rooted,
                )?)
            } else {
                catalog_fault::Store::default()
            });
            let route = Arc::new(Route {
                provider: Arc::new(RwLock::new(Arc::new(ControllerProvider::new(
                    controller.clone(),
                )?))),
                factory_refused: AtomicBool::new(false),
                retired: AtomicBool::new(false),
                changed: Notify::new(),
                staged: AtomicUsize::new(0),
                store,
                old_context: Mutex::new(None),
                wait: Arc::new(lifecycle_wait::Wait::default()),
            });
            peer.lose_create
                .store(scenario == Scenario::Interrupted, Ordering::Release);
            peer.lose_delete
                .store(scenario == Scenario::Interrupted, Ordering::Release);
            peer.lose_write.store(lost_write, Ordering::Release);
            peer.lose_exec.store(lost_exec, Ordering::Release);
            peer.hide_data_receipt.store(lost_write, Ordering::Release);
            peer.expire_activation.store(expiry, Ordering::Release);
            let mut cluster = start(rooted, minimum, route.clone(), scenario).await?;
            let domain = cluster.fixtures[0].domain.clone();
            let result=domain.run(async {
            let group=cluster.history;let history=CellHistory{cluster:group.cluster,namespace:group.namespace,cell:CellId::new("Counter:controller-provider")?,incarnation:IncarnationId::new([246;16])?};
            let leader=managed_fixture::leader(&cluster.hosts,group).await?;let cell=cluster.hosts[leader].cell(history.clone())?;
            cell.enroll(CellEnrollmentRequest{creation:ProposalId::new([247;16])?,durability:minimum},TIMEOUT).await?;
            let before=cell.query(ProposalId::new([248;16])?,&cell.contract("Version")?,vec![],QueryOptions::default()).await?;
            let create=call(&cluster,&history,minimum,1,"Create").await;
            if recovery && !data_loss {
                let error=create.err().context("create unexpectedly returned a result")?;
                let original=bindings::options(1,minimum)?.id;
                ensure!(error.downcast_ref::<Arc<CellCommandError>>().is_some_and(|error|error.ambiguous && error.command==original));
            } else {
                let receipt=create?;
                ensure!(receipt.version.sequence.get()==before.version.sequence.get()+2,"public provider must retain exactly combined entry/intent/claim and result proofs");
                let data=call(&cluster,&history,minimum,2,"Data").await;
                if data_loss {
                    let error=data.err().context("data mutation unexpectedly returned a result")?;
                    let original=bindings::options(2,minimum)?.id;
                    ensure!(error.downcast_ref::<Arc<CellCommandError>>().is_some_and(|error|error.ambiguous && error.command==original),"lost data reply lost its original ambiguous command identity");
                    ensure!(peer.lost_data.lock().unwrap().is_some(),"data loss cut was never entered; writes={} execs={} source_error={error:#}",peer.writes.load(Ordering::Acquire),peer.executions.load(Ordering::Acquire));
                    if lost_write {
                        // The controller remains live. Cleanup must settle the
                        // original write without constructing a business cell.
                        route.factory_refused.store(true,Ordering::Release);
                        cellar::asyncrt::timeout(Duration::from_secs(60),async {
                            // An unavailable original receipt is not refusal:
                            // it must block Delete and remain runnable. Releasing
                            // lookup later must settle this same original ID.
                            loop {let changed=peer.data_changed.notified();if peer.data_lookups.load(Ordering::Acquire)>0{break;}changed.await;}
                            ensure!(peer.deletes.load(Ordering::Acquire)==0 && peer.resource.lock().unwrap().exists,"unknown write receipt allowed resource deletion");
                            peer.hide_data_receipt.store(false,Ordering::Release);
                            loop {let changed=route.changed.notified();if route.retired.load(Ordering::Acquire){break;}changed.await;}
                            Ok::<_,anyhow::Error>(())
                        }).await.context("live write cleanup did not finish")??;
                        route.factory_refused.store(false,Ordering::Release);
                    } else {
                        // Explicit reconciliation must settle the original exec,
                        // not just report Running while deletion stays blocked.
                        call(&cluster,&history,minimum,3,"Reconcile").await?;
                        call(&cluster,&history,minimum,4,"Destroy").await?;
                    }
                    ensure!(*peer.lost_data.lock().unwrap()==*peer.recovered_data.lock().unwrap(),"live controller did not reconcile the exact original data operation");
                    ensure!(peer.data_lookups.load(Ordering::Acquire)>=if lost_write {2}else{1},"original data receipt was not observed through the selected outage");
                    ensure!(peer.writes.load(Ordering::Acquire)==if lost_write {1}else{2});
                    ensure!(peer.executions.load(Ordering::Acquire)==usize::from(lost_exec));
                    ensure!(peer.deletes.load(Ordering::Acquire)==1,"live recovery failed to retire the resource");
                } else {
                    let data=data?;
                    ensure!(data.version.sequence.get()==receipt.version.sequence.get()+2,"prepared execution claims must share the proven data checkpoint: before={} actual={}",receipt.version.sequence.get(),data.version.sequence.get());
                    let destroyed=call(&cluster,&history,minimum,3,"Destroy").await?;
                    ensure!(destroyed.version.sequence.get()==data.version.sequence.get()+2,"public delete must retain exactly combined entry/delete-intent/claim and result proofs: before={} actual={}",data.version.sequence.get(),destroyed.version.sequence.get());
                }
            }
            ensure!(peer.creates.load(Ordering::Acquire)==usize::from(!expiry),"create did not reach its selected admission/expiry cut");
            route.factory_refused.store(recovery && !data_loss,Ordering::Release);
            let old_hosts=std::mem::take(&mut cluster.hosts);
            managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes),&old_hosts).await?;
            controller.shutdown().await?;controller_domain.shutdown().await;
            controller_domain=Arc::new(cellar::sdk::ExecutionDomain::new(tokio::runtime::Handle::current()));
            controller=ExecutionController::open(controller_domain.clone(),resume,peer.clone()).await?;
            *route.provider.write().unwrap()=Arc::new(ControllerProvider::new(controller.clone())?);
            bindings::restart(&mut cluster).await?;
            if recovery && !data_loss {
                cellar::asyncrt::timeout(Duration::from_secs(60),async {
                    loop {let changed=route.changed.notified();if route.retired.load(Ordering::Acquire){break;}changed.await;}
                }).await.context("autonomous controller cleanup did not finish")?;
                route.factory_refused.store(false,Ordering::Release);
            }
            let read_sequence=match scenario {Scenario::Basic=>4,Scenario::LostWrite=>3,Scenario::LostExec=>5,_=>2};
            let observed=call(&cluster,&history,minimum,read_sequence,"Read").await?;let value:serde_json::Value=serde_json::from_slice(&observed.result)?;
            ensure!(value["record"]["intent"]=="deleted" && value["record"]["observation"]=="confirmed");
            ensure!(peer.creates.load(Ordering::Acquire)==usize::from(!expiry) && peer.deletes.load(Ordering::Acquire)==usize::from(!expiry),"controller recovery replayed compute");
            let old=route.old_context.lock().unwrap().clone().context("original Context missing")?;
            let calls_before=peer.calls.load(Ordering::Acquire);
            let stale=cellar_container::Adapter::new(route.selected()).session().execute(&old,Operation::Running,json!({}),&cellar_container::Limits{max_body_bytes:4096}).await;
            ensure!(stale.is_err(),"old Context borrowed the replacement controller's grant");
            ensure!(peer.calls.load(Ordering::Acquire)==calls_before,"stale Context entered the provider");
            let leader=managed_fixture::leader(&cluster.hosts,group).await?;let source=cluster.hosts[leader].cell(history.clone())?;
            let contract=source.contract("Run")?;let input=serde_json::to_vec("StageOutput")?;let options=bindings::options(read_sequence+1,minimum)?;
            let denied_before=route.store.denied_group_archive_writes.load(Ordering::Acquire);
            let staged=source.command(&contract,input.clone(),options.clone()).await;route.store.deny_group_archive_writes.store(false,Ordering::Release);
            if minimum==Durability::S3 {let error=staged.err().context("prepared output escaped S3 minimum")?;ensure!(error.ambiguous && error.command==options.id);ensure!(route.store.denied_group_archive_writes.load(Ordering::Acquire)>denied_before,"fault refused no actual archive write");}
            else {ensure!(staged?.durability>=minimum);}
            let receipt=source.receipt(CellReceiptRequest{id:options.id,contract,input,durability:minimum},TIMEOUT).await?;
            let mut expected=value;expected["revision"]=json!(expected["revision"].as_u64().context("observed revision missing")?+1);
            let expected=serde_json::to_vec(&expected)?;
            ensure!(receipt.durability()>=minimum && receipt.receipt().is_some_and(|row|row.result()==expected.as_slice()) && route.staged.load(Ordering::Acquire)==1,"receipt lookup replayed the callback or changed its result");
            eprintln!("controller_provider_observations {}",json!({"extents":rooted,"minimum":format!("{minimum:?}"),"recovery":recovery,"expiry":expiry,"lost_write":lost_write,"lost_exec":lost_exec,"writes":peer.writes.load(Ordering::Acquire),"executions":peer.executions.load(Ordering::Acquire),"creates":peer.creates.load(Ordering::Acquire),"deletes":peer.deletes.load(Ordering::Acquire),"backend_calls":peer.calls.load(Ordering::Acquire),"staged":route.staged.load(Ordering::Acquire)}));
            Ok::<_,anyhow::Error>(())
        }).await.map_err(anyhow::Error::from).and_then(std::convert::identity);
            let closed = managed_fixture::close_group_nodes(
                std::mem::take(&mut cluster.nodes),
                &cluster.hosts,
            )
            .await;
            for root in std::mem::take(&mut cluster.roots) {
                eprintln!(
                    "retained controller-provider voter disk: {}",
                    root.keep().display()
                );
            }
            domain.shutdown().await;
            controller.shutdown().await?;
            controller_domain.shutdown().await;
            result.with_context(|| {
                format!(
                    "controller provider extents={rooted} minimum={minimum:?} recovery={recovery}"
                )
            })?;
            closed?;
        }
    }
    Ok(())
}
