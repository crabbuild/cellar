// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public fenced supervision, including journal refresh and lost-delete recovery.
//! The fixture controller survives voter restart; independent controller crashes
//! and durable controller high-water recovery remain separate qualification.
use super::*;
use cellar::sdk::containers::{
    supervise, BackendIdentity, BindingCheckpoint, BindingIntent, BindingJournal,
    BindingObservation, BindingRecord, ExecutionClaim, ExecutionFenceAuthority, ExecutionGrant,
    FencedMaintenanceBackend, MaintenanceAction, MaintenancePolicy, Supervision,
};
use std::sync::atomic::AtomicBool;

fn identity() -> BackendIdentity {
    BackendIdentity {
        binding: "TOOL".into(),
        provider: "fenced-maintenance".into(),
        configuration_digest: hex::encode([226; 32]),
    }
}

fn pending() -> BindingRecord {
    BindingRecord {
        provider: identity().provider,
        configuration_digest: identity().configuration_digest,
        parameters_digest: hex::encode([227; 32]),
        generation: 1,
        request_id: hex::encode([228; 16]),
        resource_id: None,
        intent: BindingIntent::Running,
        observation: BindingObservation::Pending,
    }
}

#[derive(Default)]
struct State {
    active: Option<ExecutionClaim>,
    resource: Option<String>,
    exists: bool,
    running: bool,
}

#[derive(Default)]
struct Control {
    provider_sessions: bool,
    session_admissions: AtomicUsize,
    session_unadmitted: AtomicUsize,
    state: tokio::sync::Mutex<State>,
    docker: Option<Docker>,
    refuse: AtomicBool,
    wrong_identity: AtomicBool,
    refused: AtomicBool,
    factory_refused: AtomicBool,
    delete_entered: AtomicBool,
    release_delete: AtomicBool,
    lost_delete: AtomicBool,
    retired: AtomicBool,
    calls: AtomicUsize,
    creates: AtomicUsize,
    deletes: AtomicUsize,
    refreshes: AtomicUsize,
    staged_observations: AtomicUsize,
    archive: Mutex<Option<Arc<catalog_fault::Store>>>,
    changed: Notify,
    released: Notify,
    claims: Mutex<Vec<ExecutionClaim>>,
}

impl ExecutionFenceAuthority for Control {
    fn identity(&self) -> BackendIdentity {
        let mut value = identity();
        if self.wrong_identity.load(Ordering::Acquire) {
            value.provider = "wrong-provider".into();
        }
        value
    }

    fn activate(self: Arc<Self>, claim: ExecutionClaim) -> HandlerFuture<()> {
        Box::pin(async move {
            if self.refuse.load(Ordering::Acquire) {
                self.refused.store(true, Ordering::Release);
                self.changed.notify_waiters();
                anyhow::bail!("fixture fencing authority unavailable");
            }
            tracing::debug!(target: "cellar::raft::provider_qualification", phase = "fence-activate", sequence = claim.sequence(), revision = claim.checkpoint().revision);
            let mut state = self.state.lock().await;
            if let Some(previous) = &state.active {
                ensure!(
                    previous.history() == claim.history()
                        && previous.binding() == claim.binding()
                        && previous.resource_key() == claim.resource_key(),
                    "controller history mismatch"
                );
                ensure!(
                    claim.sequence() >= previous.sequence(),
                    "stale maintenance claim"
                );
                if claim.sequence() == previous.sequence() {
                    ensure!(previous == &claim, "conflicting maintenance claim");
                    return Ok(());
                }
                // The controller serializes accepted operations with token
                // replacement. On an owner change it also terminates the old
                // real executor; maintenance never allocates a replacement.
                if previous.owner_digest() != claim.owner_digest() && state.exists && state.running
                {
                    if let Some(docker) = &self.docker {
                        docker
                            .stop(state.resource.as_deref().context("executor ID")?)
                            .await?;
                    }
                    state.running = false;
                }
            }
            self.claims.lock().unwrap().push(claim.clone());
            state.active = Some(claim);
            Ok(())
        })
    }
}

impl Control {
    fn check(state: &State, claim: &ExecutionClaim) -> Result<()> {
        ensure!(
            state.active.as_ref() == Some(claim),
            "stale provider request"
        );
        ensure!(
            claim.checkpoint().record.request_id == pending().request_id
                && claim.checkpoint().record.generation == 1,
            "provider resource generation mismatch"
        );
        Ok(())
    }

    async fn create(&self, claim: &ExecutionClaim) -> Result<String> {
        let mut state = self.state.lock().await;
        Self::check(&state, claim)?;
        ensure!(state.resource.is_none(), "unknown create was replayed");
        let id = match &self.docker {
            Some(docker) => docker.create(claim).await?,
            None => "fixture/fenced-resource/1".into(),
        };
        state.resource = Some(id.clone());
        state.exists = true;
        state.running = true;
        self.creates.fetch_add(1, Ordering::AcqRel);
        Ok(id)
    }

    async fn inspect(&self, claim: &ExecutionClaim) -> Result<bool> {
        let state = self.state.lock().await;
        Self::check(&state, claim)?;
        ensure!(
            state.resource.as_deref() == claim.checkpoint().record.resource_id.as_deref(),
            "wrong exact resource"
        );
        if let Some(docker) = &self.docker {
            ensure!(
                docker
                    .exists(state.resource.as_deref().context("known resource")?)
                    .await?
                    == state.exists,
                "controller resource ledger disagrees with Docker"
            );
        }
        Ok(state.exists)
    }

    async fn delete(&self, claim: &ExecutionClaim) -> Result<()> {
        let mut state = self.state.lock().await;
        Self::check(&state, claim)?;
        ensure!(
            claim.checkpoint().record.intent == BindingIntent::Deleted
                && state.resource.as_deref() == claim.checkpoint().record.resource_id.as_deref(),
            "delete lacks exact durable intent"
        );
        if state.exists {
            if let Some(docker) = &self.docker {
                docker
                    .delete(state.resource.as_deref().context("known resource")?)
                    .await?;
            }
            state.exists = false;
            state.running = false;
            self.deletes.fetch_add(1, Ordering::AcqRel);
            if self.lost_delete.swap(false, Ordering::AcqRel) {
                // Pause the reply after the provider accepted DELETE. The
                // original callback is cancelled by retained voter restart;
                // its successor must inspect absence, never repeat DELETE.
                drop(state);
                self.delete_entered.store(true, Ordering::Release);
                self.changed.notify_waiters();
                loop {
                    let released = self.released.notified();
                    if self.release_delete.load(Ordering::Acquire) {
                        break;
                    }
                    released.await;
                }
                anyhow::bail!("fixture lost accepted delete reply");
            }
        }
        Ok(())
    }
}

struct Backend(Arc<Control>);

pub fn contracts() -> Result<()> {
    use cellar::sdk::containers::MaintenanceBackend;

    struct Direct;

    impl MaintenanceBackend for Direct {
        fn identity(&self) -> BackendIdentity {
            identity()
        }

        fn maintain(
            self: Arc<Self>,
            _: Context,
            _: BindingCheckpoint,
            _: MaintenanceAction,
        ) -> HandlerFuture<()> {
            Box::pin(async { anyhow::bail!("registration must not call provider") })
        }
    }
    let control = Arc::new(Control::default());
    let backend: Arc<dyn FencedMaintenanceBackend> = Arc::new(Backend(control.clone()));
    let policy = MaintenancePolicy {
        inspect_interval_ms: 1000,
        maximum_lifetime_ms: 10_000,
        attempt_timeout_ms: 3000,
        idle_timeout_ms: None,
    };
    let class = RaftClass::new("Counter", 1, [229; 32])?
        .initializer([230; 32], |_, _| Ok(()))?
        .json_cell_rpc("Run", [231; 32])?
        .provider_binding("TOOL", "fenced-maintenance", [226; 32])?;
    let application: Arc<dyn Application> = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |context, _| {
                Ok(Arc::new(Root(
                    context.storage()?,
                    Arc::new(Control::default()),
                )))
            },
        )?,
    );
    let mut direct = Supervision::default();
    direct.register(Arc::new(Direct), policy)?;
    let direct = supervise(application.clone(), direct).resolve_raft_class("Counter")?;
    let registry = || -> Result<Supervision> {
        let mut r = Supervision::default();
        r.register_fenced(backend.clone(), policy)?;
        Ok(r)
    };
    let wrapped = supervise(application.clone(), registry()?);
    let fenced = wrapped.resolve_raft_class("Counter")?;
    ensure!(
        direct.schema_digest() != fenced.schema_digest(),
        "fencing requirement absent from schema"
    );
    ensure!(
        supervise(wrapped, registry()?)
            .resolve_raft_class("Counter")?
            .schema_digest()
            == fenced.schema_digest()
    );
    let mut duplicate = registry()?;
    ensure!(duplicate.register_fenced(backend.clone(), policy).is_err());
    control.wrong_identity.store(true, Ordering::Release);
    ensure!(
        registry().is_err(),
        "mismatched controller identity was accepted"
    );
    ensure!(
        control.calls.load(Ordering::Acquire) == 0 && control.creates.load(Ordering::Acquire) == 0
    );
    Ok(())
}

impl FencedMaintenanceBackend for Backend {
    fn identity(&self) -> BackendIdentity {
        identity()
    }

    fn authority(&self) -> Arc<dyn ExecutionFenceAuthority> {
        self.0.clone()
    }

    fn maintain(
        self: Arc<Self>,
        context: Context,
        expected: BindingCheckpoint,
        action: MaintenanceAction,
        grant: ExecutionGrant,
    ) -> HandlerFuture<()> {
        Box::pin(async move {
            tracing::debug!(target: "cellar::raft::provider_qualification", phase = "maintenance-enter", action = ?action, revision = expected.revision);
            self.0.calls.fetch_add(1, Ordering::AcqRel);
            ensure!(grant.claim().checkpoint() == &expected);
            grant.check(&context)?;
            let journal = BindingJournal::open(&context, "TOOL")?;
            ensure!(journal.load(&context)?.as_ref() == Some(&expected));
            grant.before_effect(&context).await?;
            let exists = self.0.inspect(grant.claim()).await?;
            if !exists {
                ensure!(
                    expected.record.intent == BindingIntent::Deleted,
                    "absence cannot retire an unknown create"
                );
                let mut record = expected.record;
                record.observation = BindingObservation::Confirmed;
                journal
                    .compare_exchange(&context, Some(expected.revision), record)
                    .await?;
                tracing::debug!(target: "cellar::raft::provider_qualification", phase = "retirement-proven");
                self.0.retired.store(true, Ordering::Release);
                self.0.changed.notify_waiters();
                return Ok(());
            }
            if action == MaintenanceAction::Inspect {
                return Ok(());
            }
            let grant = if expected.record.intent == BindingIntent::Running {
                let old = grant.claim().clone();
                let mut record = expected.record;
                record.intent = BindingIntent::Deleted;
                record.observation = BindingObservation::Pending;
                journal.prepare_exchange(&context, Some(expected.revision), record)?;
                ensure!(
                    grant.check(&context).is_err(),
                    "binding change left old grant valid"
                );
                let refreshed = grant.refresh(&context).await?;
                ensure!(
                    refreshed.claim().sequence() > old.sequence()
                        && refreshed.claim().resource_key() == old.resource_key()
                        && refreshed.claim().owner_digest() == old.owner_digest(),
                    "refresh replaced ownership or failed to fence checkpoint"
                );
                self.0.refreshes.fetch_add(1, Ordering::AcqRel);
                refreshed
            } else {
                grant
            };
            grant.before_effect(&context).await?;
            self.0.delete(grant.claim()).await
        })
    }
}

// The ordinary named-provider adapter owns the lifecycle slot. This backend
// must receive that exact admission rather than reacquire its own lock.
struct SessionProvider(Arc<Control>);

impl cellar_container::Provider for SessionProvider {
    fn name(&self) -> &str {
        "fenced-maintenance"
    }

    fn binding_name(&self) -> Option<&str> {
        Some("TOOL")
    }

    fn fencing(&self) -> cellar_container::Fencing {
        cellar_container::Fencing::ProviderEnforced
    }

    fn operations(&self) -> &[cellar_container::Operation] {
        &[cellar_container::Operation::Start]
    }

    fn open(&self) -> Box<dyn cellar_container::ProviderSession> {
        Box::new(Self(self.0.clone()))
    }
}

impl cellar_container::ProviderSession for SessionProvider {
    fn execute<'a>(
        &'a self,
        _context: &'a Context,
        _operation: cellar_container::Operation,
        _input: serde_json::Value,
        _limits: &'a cellar_container::Limits,
    ) -> cellar_container::ProviderFuture<'a> {
        Box::pin(async move {
            self.0.session_unadmitted.fetch_add(1, Ordering::AcqRel);
            anyhow::bail!("fenced provider control requires existing lifecycle admission")
        })
    }

    fn execute_admitted<'a>(
        &'a self,
        context: &'a Context,
        operation: cellar_container::Operation,
        _input: serde_json::Value,
        _limits: &'a cellar_container::Limits,
        lifecycle: Option<&'a cellar::sdk::containers::BindingLifecycleGuard>,
    ) -> cellar_container::ProviderFuture<'a> {
        Box::pin(async move {
            ensure!(operation == cellar_container::Operation::Start);
            let lifecycle = lifecycle.context("adapter lost lifecycle admission")?;
            let journal = BindingJournal::open(context, "TOOL")?;
            journal.prepare_exchange(context, None, pending())?;
            let grant = lifecycle.fence_execution(context, self.0.clone()).await?;
            journal.check_retention(context)?;
            grant.before_effect(context).await?;
            self.0.session_admissions.fetch_add(1, Ordering::AcqRel);
            let id = self.0.create(grant.claim()).await?;
            let current = journal
                .load(context)?
                .context("reserved public provider resource")?;
            let mut record = current.record;
            record.resource_id = Some(id);
            record.observation = BindingObservation::Confirmed;
            journal.prepare_exchange(context, Some(current.revision), record)?;
            self.0.refuse.store(true, Ordering::Release);
            Ok(json!("1"))
        })
    }
}

struct Root(Storage, Arc<Control>);

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
            let op: String = serde_json::from_str(&input)?;
            tracing::debug!(target: "cellar::raft::provider_qualification", phase = "root-enter", operation = %op);
            let journal = BindingJournal::open(&context, "TOOL")?;
            let lifecycle = if self.1.provider_sessions && op == "Create" {
                None
            } else {
                Some(journal.lock_lifecycle(&context).await?)
            };
            tracing::debug!(target: "cellar::raft::provider_qualification", phase = "root-lifecycle-selected", operation = %op);
            let result = match op.as_str() {
                "Create" => {
                    self.0.put_json("business", &json!(17))?;
                    if self.1.provider_sessions {
                        let mut bindings = cellar_container::Bindings::default();
                        bindings.insert("TOOL", Arc::new(SessionProvider(self.1.clone())))?;
                        let sessions = bindings.sessions();
                        let result = sessions
                            .get("TOOL")?
                            .execute(
                                &context,
                                cellar_container::Operation::Start,
                                json!({"options":{}}),
                                &cellar_container::Limits {
                                    max_body_bytes: 4096,
                                },
                            )
                            .await?;
                        ensure!(result == json!("1"));
                        journal
                            .load(&context)?
                            .context("public provider confirmation")?
                    } else {
                        let lifecycle = lifecycle.context("direct lifecycle admission")?;
                        tracing::debug!(target: "cellar::raft::provider_qualification", phase = "reserve-begin");
                        journal.prepare_exchange(&context, None, pending())?;
                        tracing::debug!(target: "cellar::raft::provider_qualification", phase = "reserve-prepared");
                        let grant = journal
                            .fence_execution(&context, lifecycle, self.1.clone())
                            .await?;
                        tracing::debug!(target: "cellar::raft::provider_qualification", phase = "fence-proven");
                        journal.check_retention(&context)?;
                        grant.before_effect(&context).await?;
                        tracing::debug!(target: "cellar::raft::provider_qualification", phase = "create-admitted");
                        let id = self.1.create(grant.claim()).await?;
                        tracing::debug!(target: "cellar::raft::provider_qualification", phase = "create-accepted");
                        let current = journal.load(&context)?.context("reserved resource")?;
                        let mut record = current.record;
                        record.resource_id = Some(id);
                        record.observation = BindingObservation::Confirmed;
                        let confirmed =
                            journal.prepare_exchange(&context, Some(current.revision), record)?;
                        tracing::debug!(target: "cellar::raft::provider_qualification", phase = "create-observation-prepared");
                        self.1.refuse.store(true, Ordering::Release);
                        confirmed
                    }
                }
                "Read" => {
                    ensure!(self.0.get_json("business")? == Some(json!(17)));
                    journal.load(&context)?.context("retained resource")?
                }
                "StageOutput" => {
                    let prior = journal.load(&context)?.context("retired resource")?;
                    ensure!(prior.record.is_retired());
                    let prepared =
                        journal.prepare_exchange(&context, Some(prior.revision), prior.record)?;
                    self.1.staged_observations.fetch_add(1, Ordering::AcqRel);
                    // Refuse archival only after the business callback has staged
                    // its complete result. The original final output proof must
                    // still refuse S3 success; this does not submit provider I/O.
                    self.1
                        .archive
                        .lock()
                        .unwrap()
                        .as_ref()
                        .context("output proof backend")?
                        .deny_group_archive_writes
                        .store(true, Ordering::Release);
                    prepared
                }
                _ => anyhow::bail!("unknown operation"),
            };
            Ok(Output::Rpc(RpcData::Json(
                json!({"revision":result.revision,"record":result.record}).to_string(),
            )))
        })
    }
}

async fn start(
    rooted: bool,
    minimum: Durability,
    store: Arc<catalog_fault::Store>,
    control: Arc<Control>,
) -> Result<ManagedCluster> {
    let class = RaftClass::new("Counter", 1, [229; 32])?
        .initializer([230; 32], |_, _| Ok(()))?
        .json_cell_rpc("Run", [231; 32])?
        .query("Version", [234; 32], |_, _| Ok(vec![]))?
        .provider_binding("TOOL", "fenced-maintenance", [226; 32])?;
    let factory_control = control.clone();
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |context, _| {
                ensure!(
                    !factory_control.factory_refused.load(Ordering::Acquire),
                    "business constructor unavailable"
                );
                Ok(Arc::new(Root(context.storage()?, factory_control.clone())))
            },
        )?,
    );
    let mut supervision = Supervision::default();
    supervision.register_fenced(
        Arc::new(Backend(control)),
        MaintenancePolicy {
            inspect_interval_ms: 1000,
            maximum_lifetime_ms: 10_000,
            attempt_timeout_ms: 3000,
            idle_timeout_ms: None,
        },
    )?;
    let application = supervise(application, supervision);
    let mut options = configuration::options("fenced-provider-maintenance");
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

async fn call(
    cluster: &ManagedCluster,
    history: &CellHistory,
    minimum: Durability,
    seq: u64,
    op: &str,
) -> Result<CommandReceipt> {
    let leader = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
    let cell = cluster.hosts[leader].cell(history.clone())?;
    tracing::debug!(target: "cellar::raft::provider_qualification", phase = "request", sequence = seq, minimum = ?minimum, operation = op);
    Ok(cell
        .command(
            &cell.contract("Run")?,
            serde_json::to_vec(op)?,
            bindings::options(seq, minimum)?,
        )
        .await?)
}

async fn wait(control: &Control, flag: &AtomicBool, stage: &str) -> Result<()> {
    cellar::asyncrt::timeout(Duration::from_secs(60), async {
        loop {
            let changed = control.changed.notified();
            if flag.load(Ordering::Acquire) {
                break;
            }
            changed.await;
        }
    })
    .await
    .with_context(|| format!("autonomous fenced maintenance {stage} did not finish"))
}

pub async fn qualify(real_rustfs: bool, docker: bool) -> Result<()> {
    selected(real_rustfs, docker, None, false).await
}

pub async fn qualify_sessions(real_rustfs: bool, docker: bool) -> Result<()> {
    selected(real_rustfs, docker, None, true).await
}

pub async fn trace(rooted: bool, real_rustfs: bool, docker: bool) -> Result<()> {
    selected(real_rustfs, docker, Some((rooted, Durability::S3)), false).await
}

async fn selected(
    real_rustfs: bool,
    docker: bool,
    only: Option<(bool, Durability)>,
    provider_sessions: bool,
) -> Result<()> {
    for rooted in [false, true] {
        for minimum in [Durability::Quorum, Durability::S3] {
            if only.is_some_and(|selected| selected != (rooted, minimum)) {
                continue;
            }
            let engine = if docker {
                Some(Docker::configured(rooted, minimum)?)
            } else {
                None
            };
            let control = Arc::new(Control {
                provider_sessions,
                docker: engine,
                ..Default::default()
            });
            control.lost_delete.store(true, Ordering::Release);
            let store = Arc::new(if real_rustfs {
                catalog_fault::Store::around(rustfs::store(
                    if minimum == Durability::S3 {
                        "fenced-maintenance-s3"
                    } else {
                        "fenced-maintenance-quorum"
                    },
                    rooted,
                )?)
            } else {
                catalog_fault::Store::default()
            });
            *control.archive.lock().unwrap() = Some(store.clone());
            let mut cluster = start(rooted, minimum, store.clone(), control.clone()).await?;
            let domain = cluster.fixtures[0].domain.clone();
            let work = domain
                .run(async {
                    let group = cluster.history;
                    let history = CellHistory {
                        cluster: group.cluster,
                        namespace: group.namespace,
                        cell: CellId::new("Counter:fenced-maintenance")?,
                        incarnation: IncarnationId::new([232; 16])?,
                    };
                    let leader = managed_fixture::leader(&cluster.hosts, group).await?;
                    cluster.hosts[leader]
                        .cell(history.clone())?
                        .enroll(
                            CellEnrollmentRequest {
                                creation: ProposalId::new([233; 16])?,
                                durability: minimum,
                            },
                            TIMEOUT,
                        )
                        .await?;
                    let cell = cluster.hosts[leader].cell(history.clone())?;
                    let before = cell
                        .query(
                            ProposalId::new([235; 16])?,
                            &cell.contract("Version")?,
                            vec![],
                            QueryOptions::default(),
                        )
                        .await?;
                    let first = call(&cluster, &history, minimum, 1, "Create").await?;
                    ensure!(
                        first.version.sequence.get() == before.version.sequence.get() + if provider_sessions { 3 } else { 2 },
                        "provider creation must retain the required adapter entry, intent/claim and receipt proofs: before={} actual={}",
                        before.version.sequence.get(),
                        first.version.sequence.get()
                    );
                    ensure!(control.session_unadmitted.load(Ordering::Acquire) == 0);
                    ensure!(control.session_admissions.load(Ordering::Acquire) == usize::from(provider_sessions));
                    wait(&control, &control.refused, "authority refusal").await?;
                    ensure!(
                        control.calls.load(Ordering::Acquire) == 0,
                        "unfenced backend fallback after refusal"
                    );
                    control.refuse.store(false, Ordering::Release);
                    wait(
                        &control,
                        &control.delete_entered,
                        "refresh/delete admission",
                    )
                    .await?;
                    ensure!(
                        control.refreshes.load(Ordering::Acquire) == 1
                            && control.deletes.load(Ordering::Acquire) == 1
                    );
                    let old = control
                        .claims
                        .lock()
                        .unwrap()
                        .last()
                        .cloned()
                        .context("old admitted claim")?;
                    control.factory_refused.store(true, Ordering::Release);
                    bindings::restart(&mut cluster).await?;
                    control.release_delete.store(true, Ordering::Release);
                    control.released.notify_waiters();
                    wait(&control, &control.retired, "restart/lost-delete recovery").await?;
                    control.factory_refused.store(false, Ordering::Release);
                    let observed = call(&cluster, &history, minimum, 2, "Read").await?;
                    let value: serde_json::Value = serde_json::from_slice(&observed.result)?;
                    ensure!(
                        value["record"]["intent"] == "deleted"
                            && value["record"]["observation"] == "confirmed"
                            && value["record"]["request_id"] == pending().request_id
                            && value["record"]["generation"] == 1
                    );
                    ensure!(
                        control.creates.load(Ordering::Acquire) == 1
                            && control.deletes.load(Ordering::Acquire) == 1,
                        "cleanup replayed create or accepted delete"
                    );
                    ensure!(
                        call(&cluster, &history, minimum, 1, "Create").await?.result
                            == first.result
                    );
                    ensure!(
                        control.clone().activate(old).await.is_err(),
                        "old maintenance grant regained ownership"
                    );
                    // Final observation preparation does not acknowledge S3.
                    // Preserve the same command and read its stored receipt after
                    // restoring the archive; never rerun the application body.
                    let leader = managed_fixture::leader(&cluster.hosts, group).await?;
                    let source = cluster.hosts[leader].cell(history.clone())?;
                    let contract = source.contract("Run")?;
                    let input = serde_json::to_vec("StageOutput")?;
                    let options = bindings::options(3, minimum)?;
                    let denied_before = store.denied_group_archive_writes.load(Ordering::Acquire);
                    let staged = source
                        .command(&contract, input.clone(), options.clone())
                        .await;
                    let denied_output_writes = store
                        .denied_group_archive_writes
                        .load(Ordering::Acquire)
                        .saturating_sub(denied_before);
                    store.deny_group_archive_writes.store(false, Ordering::Release);
                    ensure!(control.staged_observations.load(Ordering::Acquire) == 1);
                    if minimum == Durability::S3 {
                        let error = staged.err().context("prepared observation escaped S3 output gate")?;
                        ensure!(error.command == options.id && error.ambiguous);
                        ensure!(denied_output_writes > 0, "output fault refused no actual archive write");
                        eprintln!("retained prepared provider output command: {:?}", options.id);
                    } else {
                        ensure!(staged?.durability >= minimum);
                    }
                    let mut expected = value.clone();
                    expected["revision"] = json!(value["revision"].as_u64().context("observed revision")? + 1);
                    let expected = serde_json::to_vec(&expected)?;
                    let receipt = source
                        .receipt(
                            CellReceiptRequest {
                                id: options.id,
                                contract,
                                input,
                                durability: minimum,
                            },
                            TIMEOUT,
                        )
                        .await?;
                    ensure!(
                        receipt.command() == options.id
                            && receipt.durability() >= minimum
                            && receipt.receipt().is_some_and(|row| row.result() == expected.as_slice())
                    );
                    ensure!(
                        control.staged_observations.load(Ordering::Acquire) == 1
                            && control.creates.load(Ordering::Acquire) == 1
                            && control.deletes.load(Ordering::Acquire) == 1,
                        "receipt-only recovery replayed business or provider work"
                    );
                    eprintln!("fenced_maintenance_observations {}", json!({
                        "extents": rooted, "minimum": format!("{minimum:?}"),
                        "create_committed_versions": first.version.sequence.get() - before.version.sequence.get(),
                        "provider_sessions": provider_sessions,
                        "session_admissions": control.session_admissions.load(Ordering::Acquire),
                        "session_unadmitted": control.session_unadmitted.load(Ordering::Acquire),
                        "creates": control.creates.load(Ordering::Acquire),
                        "deletes": control.deletes.load(Ordering::Acquire),
                        "refreshes": control.refreshes.load(Ordering::Acquire),
                        "staged_observations": control.staged_observations.load(Ordering::Acquire),
                        "denied_output_archive_writes": denied_output_writes,
                        "resource": control.state.lock().await.resource.clone(),
                        "claim_sequences": control.claims.lock().unwrap().iter().map(ExecutionClaim::sequence).collect::<Vec<_>>()
                    }));
                    Ok::<_, anyhow::Error>(())
                })
                .await
                .map_err(anyhow::Error::from)
                .and_then(std::convert::identity);
            let closed = managed_fixture::close_group_nodes(
                std::mem::take(&mut cluster.nodes),
                &cluster.hosts,
            )
            .await;
            for root in std::mem::take(&mut cluster.roots) {
                eprintln!(
                    "retained fenced maintenance disk: {}",
                    root.keep().display()
                );
            }
            domain.shutdown().await;
            work.with_context(|| {
                format!("fenced maintenance extents={rooted} minimum={minimum:?} docker={docker}")
            })?;
            closed?;
        }
    }
    Ok(())
}

struct Docker {
    engine: cellar::docker::Docker,
    image: String,
    owner: String,
}

impl Docker {
    fn configured(rooted: bool, minimum: Durability) -> Result<Self> {
        let socket = std::env::var("CELLAR_CG17_FENCE_DOCKER_SOCKET")?;
        let image = std::env::var("CELLAR_CG17_FENCE_DOCKER_IMAGE")?;
        let owner = std::env::var("CELLAR_CG17_FENCE_DOCKER_OWNER")?;
        ensure!(
            image.contains("@sha256:")
                && !owner.is_empty()
                && owner.len() <= 80
                && owner
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
            "unowned/unpinned Docker fixture"
        );
        Ok(Self {
            engine: cellar::docker::Docker::new(socket),
            image,
            owner: format!(
                "{owner}-fm-{}-{}",
                u8::from(rooted),
                if minimum == Durability::S3 {
                    "s3"
                } else {
                    "quorum"
                }
            ),
        })
    }

    async fn create(&self, claim: &ExecutionClaim) -> Result<String> {
        let name = format!("{}-{}", self.owner, hex::encode(claim.resource_key()));
        let result=self.engine.expect("POST",&format!("/containers/create?name={name}"),Some(json!({
            "Image":self.image,"Cmd":["/bin/sh","-c","while :; do sleep 1; done"],"NetworkDisabled":true,
            "Labels":{"cellar.cg17.owner":self.owner,"cellar.cg17.request":claim.checkpoint().record.request_id},
            "HostConfig":{"Memory":67108864,"MemorySwap":67108864,"NanoCpus":100000000,"RestartPolicy":{"Name":"no"}}
        })),"create managed fixture resource").await?.json()?;
        let id = cellar::docker::ContainerId::new(
            result["Id"].as_str().context("created resource ID")?,
        )?;
        self.engine
            .expect(
                "POST",
                &format!("/containers/{id}/start"),
                None,
                "start managed fixture resource",
            )
            .await?;
        eprintln!(
            "fenced maintenance created Docker resource: {} {id}",
            self.owner
        );
        Ok(id.as_str().into())
    }

    async fn exists(&self, id: &str) -> Result<bool> {
        let id = cellar::docker::ContainerId::new(id)?;
        let r = self
            .engine
            .call("GET", &format!("/containers/{id}/json"), None)
            .await?;
        if r.status.as_u16() == 404 {
            return Ok(false);
        }
        ensure!(r.status.is_success(), "resource inspection failed");
        let r = r.json()?;
        ensure!(
            r["Id"].as_str() == Some(id.as_str())
                && r["Config"]["Labels"]["cellar.cg17.owner"].as_str() == Some(self.owner.as_str())
                && r["Config"]["Labels"]["cellar.cg17.request"].as_str()
                    == Some(pending().request_id.as_str()),
            "foreign resource metadata"
        );
        let limits = &r["HostConfig"];
        ensure!(
            limits["NanoCpus"].as_u64() == Some(100_000_000)
                && limits["Memory"].as_u64() == Some(67108864)
                && limits["MemorySwap"].as_u64() == Some(67108864)
                && limits["RestartPolicy"]["Name"].as_str() == Some("no")
                && r["Config"]["NetworkDisabled"].as_bool() == Some(true),
            "managed fixture resource limits changed"
        );
        eprintln!(
            "fenced_maintenance_docker_observation {}",
            json!({
                "id": r["Id"], "owner": self.owner,
                "image": r["Image"], "state": r["State"],
                "limits": {
                    "cpus": limits["NanoCpus"], "memory": limits["Memory"],
                    "memory_swap": limits["MemorySwap"],
                    "restart": limits["RestartPolicy"]["Name"],
                    "network_disabled": r["Config"]["NetworkDisabled"]
                }
            })
        );
        Ok(true)
    }

    async fn stop(&self, id: &str) -> Result<()> {
        ensure!(
            self.exists(id).await?,
            "fenced resource missing before owner revocation"
        );
        let id = cellar::docker::ContainerId::new(id)?;
        self.engine
            .expect(
                "POST",
                &format!("/containers/{id}/kill?signal=SIGKILL"),
                None,
                "revoke old managed executor",
            )
            .await?;
        let exit = self
            .engine
            .expect(
                "POST",
                &format!("/containers/{id}/wait?condition=not-running"),
                None,
                "observe managed executor exit",
            )
            .await?
            .json()?;
        ensure!(
            exit["StatusCode"].as_i64() == Some(137),
            "old executor did not exit from revocation"
        );
        eprintln!(
            "fenced maintenance revoked Docker executor: {} {id}",
            self.owner
        );
        Ok(())
    }

    async fn delete(&self, id: &str) -> Result<()> {
        ensure!(
            self.exists(id).await?,
            "cannot delete unauthenticated resource"
        );
        let id = cellar::docker::ContainerId::new(id)?;
        self.engine
            .expect(
                "DELETE",
                &format!("/containers/{id}?force=1"),
                None,
                "delete exact managed resource",
            )
            .await?;
        ensure!(
            !self.exists(id.as_str()).await?,
            "delete did not retire exact resource"
        );
        eprintln!(
            "fenced maintenance removed Docker resource: {} {id}",
            self.owner
        );
        Ok(())
    }
}
