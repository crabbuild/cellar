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
    lose_create: AtomicBool,
    lose_delete: AtomicBool,
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
            if (method == Operation::Start && self.lose_create.swap(false, Ordering::AcqRel))
                || (method == Operation::Destroy && self.lose_delete.swap(false, Ordering::AcqRel))
            {
                // The independent peer already owns an exact outcome. Cancellation
                // loses its reply; recovery observes the original receipt only.
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
                    Ok(Some(ExecutionOutcome::Completed(receipt.bytes.clone())))
                }
                None => Ok(None),
            }
        })
    }
}

struct Route {
    provider: RwLock<Arc<ControllerProvider>>,
    factory_refused: AtomicBool,
    retired: AtomicBool,
    changed: Notify,
    staged: AtomicUsize,
    store: Arc<catalog_fault::Store>,
    old_context: Mutex<Option<Context>>,
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
        self.selected().authority()
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
            provider
                .maintain(context.clone(), expected, action, grant)
                .await?;
            // The production maintenance adapter returns only after proving its
            // observation. Inspect that proof's state; add no harness checkpoint.
            if BindingJournal::open(&context, "TOOL")?
                .load(&context)?
                .is_some_and(|row| row.record.is_retired())
            {
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
                _ => anyhow::bail!("unknown operation"),
            }
            let current = journal.load(&context)?.context("binding missing")?;
            Ok(Output::Rpc(RpcData::Json(
                json!({"revision":current.revision,"record":current.record}).to_string(),
            )))
        })
    }
}

async fn start(
    rooted: bool,
    minimum: Durability,
    route: Arc<Route>,
    cleanup: bool,
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
    let application: Arc<dyn Application> = if cleanup {
        let mut supervision = Supervision::default();
        supervision.register_fenced(
            route.clone(),
            MaintenancePolicy {
                inspect_interval_ms: 100,
                maximum_lifetime_ms: 500,
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
                    match (recovery, minimum) {
                        (true, Durability::S3) => "controller-provider-recovery-s3",
                        (true, _) => "controller-provider-recovery-quorum",
                        (false, Durability::S3) => "controller-provider-s3",
                        (false, _) => "controller-provider-quorum",
                    },
                    rooted,
                )?)
            } else {
                catalog_fault::Store::default()
            });
            let route = Arc::new(Route {
                provider: RwLock::new(Arc::new(ControllerProvider::new(controller.clone())?)),
                factory_refused: AtomicBool::new(false),
                retired: AtomicBool::new(false),
                changed: Notify::new(),
                staged: AtomicUsize::new(0),
                store,
                old_context: Mutex::new(None),
            });
            peer.lose_create.store(recovery, Ordering::Release);
            peer.lose_delete.store(recovery, Ordering::Release);
            let mut cluster = start(rooted, minimum, route.clone(), recovery).await?;
            let domain = cluster.fixtures[0].domain.clone();
            let result=domain.run(async {
            let group=cluster.history;let history=CellHistory{cluster:group.cluster,namespace:group.namespace,cell:CellId::new("Counter:controller-provider")?,incarnation:IncarnationId::new([246;16])?};
            let leader=managed_fixture::leader(&cluster.hosts,group).await?;let cell=cluster.hosts[leader].cell(history.clone())?;
            cell.enroll(CellEnrollmentRequest{creation:ProposalId::new([247;16])?,durability:minimum},TIMEOUT).await?;
            let before=cell.query(ProposalId::new([248;16])?,&cell.contract("Version")?,vec![],QueryOptions::default()).await?;
            let create=call(&cluster,&history,minimum,1,"Create").await;
            if recovery {let error=create.err().context("accepted create lost no reply")?;ensure!(error.downcast_ref::<Arc<CellCommandError>>().is_some_and(|error|error.ambiguous));}
            else {let receipt=create?;ensure!(receipt.version.sequence.get()==before.version.sequence.get()+3,"public provider must retain exactly entry, intent/claim and result proofs");call(&cluster,&history,minimum,2,"Data").await?;call(&cluster,&history,minimum,3,"Destroy").await?;}
            ensure!(peer.creates.load(Ordering::Acquire)==1);
            route.factory_refused.store(recovery,Ordering::Release);
            let old_hosts=std::mem::take(&mut cluster.hosts);
            managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes),&old_hosts).await?;
            controller.shutdown().await?;controller_domain.shutdown().await;
            controller_domain=Arc::new(cellar::sdk::ExecutionDomain::new(tokio::runtime::Handle::current()));
            controller=ExecutionController::open(controller_domain.clone(),resume,peer.clone()).await?;
            *route.provider.write().unwrap()=Arc::new(ControllerProvider::new(controller.clone())?);
            bindings::restart(&mut cluster).await?;
            if recovery {
                cellar::asyncrt::timeout(Duration::from_secs(60),async {
                    loop {let changed=route.changed.notified();if route.retired.load(Ordering::Acquire){break;}changed.await;}
                }).await.context("autonomous controller cleanup did not finish")?;
                route.factory_refused.store(false,Ordering::Release);
            }
            let observed=call(&cluster,&history,minimum,4,"Read").await?;let value:serde_json::Value=serde_json::from_slice(&observed.result)?;
            ensure!(value["record"]["intent"]=="deleted" && value["record"]["observation"]=="confirmed");
            ensure!(peer.creates.load(Ordering::Acquire)==1 && peer.deletes.load(Ordering::Acquire)==1,"controller recovery replayed compute");
            let old=route.old_context.lock().unwrap().clone().context("original Context missing")?;
            let calls_before=peer.calls.load(Ordering::Acquire);
            let stale=cellar_container::Adapter::new(route.selected()).session().execute(&old,Operation::Running,json!({}),&cellar_container::Limits{max_body_bytes:4096}).await;
            ensure!(stale.is_err(),"old Context borrowed the replacement controller's grant");
            ensure!(peer.calls.load(Ordering::Acquire)==calls_before,"stale Context entered the provider");
            let leader=managed_fixture::leader(&cluster.hosts,group).await?;let source=cluster.hosts[leader].cell(history.clone())?;
            let contract=source.contract("Run")?;let input=serde_json::to_vec("StageOutput")?;let options=bindings::options(5,minimum)?;
            let denied_before=route.store.denied_group_archive_writes.load(Ordering::Acquire);
            let staged=source.command(&contract,input.clone(),options.clone()).await;route.store.deny_group_archive_writes.store(false,Ordering::Release);
            if minimum==Durability::S3 {let error=staged.err().context("prepared output escaped S3 minimum")?;ensure!(error.ambiguous && error.command==options.id);ensure!(route.store.denied_group_archive_writes.load(Ordering::Acquire)>denied_before,"fault refused no actual archive write");}
            else {ensure!(staged?.durability>=minimum);}
            let receipt=source.receipt(CellReceiptRequest{id:options.id,contract,input,durability:minimum},TIMEOUT).await?;
            let mut expected=value;expected["revision"]=json!(expected["revision"].as_u64().context("observed revision missing")?+1);
            let expected=serde_json::to_vec(&expected)?;
            ensure!(receipt.durability()>=minimum && receipt.receipt().is_some_and(|row|row.result()==expected.as_slice()) && route.staged.load(Ordering::Acquire)==1,"receipt lookup replayed the callback or changed its result");
            eprintln!("controller_provider_observations {}",json!({"extents":rooted,"minimum":format!("{minimum:?}"),"recovery":recovery,"creates":peer.creates.load(Ordering::Acquire),"deletes":peer.deletes.load(Ordering::Acquire),"backend_calls":peer.calls.load(Ordering::Acquire),"staged":route.staged.load(Ordering::Acquire)}));
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
