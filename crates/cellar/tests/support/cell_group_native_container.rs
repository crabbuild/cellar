// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Normal Context.container calls, independent cells and deferred construction.
use super::*;
use cellar::sdk::containers::{Container, FilePath, StartOptions};

#[derive(Default)]
struct Peers(Mutex<BTreeMap<String, (String, Arc<Peer>)>>);

impl Peers {
    fn select(&self, claim: &ExecutionClaim) -> Arc<Peer> {
        self.0
            .lock()
            .unwrap()
            .entry(hex::encode(claim.resource_key()))
            .or_insert_with(|| {
                (
                    claim.history().cell.as_str().into(),
                    Arc::new(Peer::default()),
                )
            })
            .1
            .clone()
    }

    fn cell(&self, scope: &str) -> Result<Arc<Peer>> {
        self.0
            .lock()
            .unwrap()
            .values()
            .find(|(name, _)| name == scope)
            .map(|(_, peer)| peer.clone())
            .context("native container peer missing")
    }

    fn calls(&self) -> usize {
        self.0
            .lock()
            .unwrap()
            .values()
            .map(|(_, peer)| peer.calls.load(Ordering::Acquire))
            .sum()
    }
}

impl ExecutionControllerBackend for Peers {
    fn identity(&self) -> BackendIdentity {
        Peer::default().identity()
    }

    fn revoke(
        self: Arc<Self>,
        previous: Option<ExecutionClaim>,
        successor: Option<ExecutionClaim>,
        unresolved: Vec<ExecutionPendingOperation>,
    ) -> HandlerFuture<()> {
        match successor.as_ref().or(previous.as_ref()) {
            Some(claim) => self.select(claim).revoke(previous, successor, unresolved),
            None => Box::pin(async { anyhow::bail!("controlled peer has no revocation identity") }),
        }
    }

    fn revoke_history(
        self: Arc<Self>,
        _: CellHistory,
        _: CellHistory,
        _: Option<ExecutionClaim>,
        _: Vec<ExecutionPendingOperation>,
    ) -> HandlerFuture<()> {
        Box::pin(async {
            anyhow::bail!("independent execution fencing is outside this controlled peer")
        })
    }

    fn execute(
        self: Arc<Self>,
        claim: ExecutionClaim,
        operation: ExecutionOperation,
        payload: Vec<u8>,
    ) -> HandlerFuture<ExecutionOutcome> {
        self.select(&claim).execute(claim, operation, payload)
    }

    fn reconcile(
        self: Arc<Self>,
        current: ExecutionClaim,
        original: ExecutionPendingOperation,
    ) -> HandlerFuture<Option<ExecutionOutcome>> {
        if current.resource_key() != original.claim.resource_key() {
            return Box::pin(async { anyhow::bail!("foreign native container receipt") });
        }
        self.select(&current).reconcile(current, original)
    }
}

#[derive(Default)]
struct Track {
    constructed: Mutex<BTreeMap<String, usize>>,
    reads: AtomicUsize,
    old: Mutex<Option<(Container, Context)>>,
    restarted: AtomicBool,
}

struct NativeRoot {
    storage: Storage,
    container: Container,
    peers: Arc<Peers>,
    track: Arc<Track>,
}

impl Cell for NativeRoot {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc {
                args: RpcData::Json(args),
                ..
            } = invocation
            else {
                anyhow::bail!("unexpected native invocation")
            };
            let operation: String = serde_json::from_str(&args)?;
            let mut output = json!({"running":self.container.running(&context)?});
            match operation.as_str() {
                "Create" => {
                    ensure!(!self.container.running(&context)?);
                    self.storage.put_json("business", &json!(17))?;
                    let run = self
                        .container
                        .start(&context, StartOptions::default())
                        .await?;
                    ensure!(run == 1 && self.container.running(&context)?);
                    let mut old = self.track.old.lock().unwrap();
                    if old.is_none() {
                        *old = Some((self.container.clone(), context.clone()));
                    }
                    output = json!({"run":run});
                }
                "Write" => {
                    self.storage.put_json("business", &json!(99))?;
                    self.container
                        .write_file(
                            &context,
                            FilePath::new("/tmp/value")?,
                            vec![0, 255, 42],
                            0o640,
                        )
                        .await?;
                }
                "Read" => {
                    self.track.reads.fetch_add(1, Ordering::AcqRel);
                    ensure!(self.container.running(&context)?);
                    let value = self
                        .container
                        .read_file(&context, FilePath::new("/tmp/value")?, 4096)
                        .await?;
                    ensure!(value == [0, 255, 42]);
                    output = json!({"bytes":STANDARD.encode(value)});
                }
                "Invalid" => {
                    let before = self.peers.calls();
                    ensure!(self
                        .container
                        .start(
                            &context,
                            StartOptions {
                                env: vec![("KEY".into(), "x".repeat(128 * 1024))],
                                ..Default::default()
                            }
                        )
                        .await
                        .is_err());
                    ensure!(self
                        .container
                        .write_file(&context, FilePath::new("/tmp/value")?, vec![7], 0o1777)
                        .await
                        .is_err());
                    ensure!(self
                        .container
                        .write_file(&context, FilePath::new("/tmp/value")?, vec![7; 4097], 0o600)
                        .await
                        .is_err());
                    ensure!(self
                        .container
                        .exec(
                            &context,
                            cellar::sdk::containers::ExecOptions {
                                cmd: vec!["true".into()],
                                ..Default::default()
                            }
                        )
                        .await
                        .is_err());
                    ensure!(self.container.port(&context, 80).is_err());
                    ensure!(
                        self.peers.calls() == before,
                        "rejected native API reached provider"
                    );
                }
                "Destroy" => {
                    self.container.destroy(&context).await?;
                    ensure!(!self.container.running(&context)?);
                }
                "Status" => {}
                _ => anyhow::bail!("unknown native operation"),
            }
            let old = self.track.old.lock().unwrap().clone();
            if let Some((old_container, old_context)) = old {
                let before = self.peers.calls();
                if operation != "Create"
                    || context.identity().unwrap().scope != old_context.identity().unwrap().scope
                {
                    ensure!(
                        self.container
                            .write_file(&old_context, FilePath::new("/tmp/old")?, vec![9], 0o600)
                            .await
                            .is_err(),
                        "native container borrowed an earlier invocation"
                    );
                }
                if self.track.restarted.load(Ordering::Acquire)
                    || context.identity().unwrap().scope != old_context.identity().unwrap().scope
                {
                    ensure!(
                        old_container
                            .write_file(&context, FilePath::new("/tmp/old")?, vec![9], 0o600)
                            .await
                            .is_err(),
                        "native container borrowed another activation or cell"
                    );
                }
                ensure!(
                    self.peers.calls() == before,
                    "stale/foreign native Context reached provider"
                );
            }
            Ok(Output::Rpc(RpcData::Json(output.to_string())))
        })
    }
}

async fn start_native(
    rooted: bool,
    minimum: Durability,
    store: Arc<catalog_fault::Store>,
    controller: Arc<ExecutionController>,
    peers: Arc<Peers>,
    track: Arc<Track>,
) -> Result<ManagedCluster> {
    let provider = Arc::new(ControllerProvider::new(controller)?);
    let adapter = Arc::new(cellar_container::controller::NativeContainerAdapter::new(
        provider,
        cellar_container::Limits {
            max_body_bytes: 4096,
        },
    )?);
    let class = RaftClass::new("Counter", 1, [221; 32])?
        .initializer([222; 32], |_, _| Ok(()))?
        .json_cell_rpc("Run", [223; 32])?
        .query("Version", [224; 32], |_, _| Ok(vec![]))?
        .container_adapter(adapter)?;
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |context, identity| {
                let container = context.container()?;
                let running = container.running(&context)?;
                if let Ok(peer) = peers.cell(&identity.scope) {
                    let original = peer.lost_data.lock().unwrap().clone();
                    if original.is_some() {
                        ensure!(
                            original == *peer.recovered_data.lock().unwrap(),
                            "constructor entered before original operation reconciliation"
                        );
                    }
                }
                if track.restarted.load(Ordering::Acquire) {
                    ensure!(running, "constructor lost reconciled running observation");
                }
                *track
                    .constructed
                    .lock()
                    .unwrap()
                    .entry(identity.scope.clone())
                    .or_default() += 1;
                Ok(Arc::new(NativeRoot {
                    storage: context.storage()?,
                    container,
                    peers: peers.clone(),
                    track: track.clone(),
                }))
            },
        )?,
    );
    let mut options = configuration::options("native-container");
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

pub(super) async fn qualify(real: bool, outage: bool) -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("cellar::raft::checkpoint=debug")
        .with_ansi(false)
        .try_init();
    for rooted in [false, true] {
        for minimum in [Durability::Quorum, Durability::S3] {
            let root = tempfile::tempdir()?.keep();
            eprintln!("retained native-container controller: {}", root.display());
            let peers = Arc::new(Peers::default());
            let track = Arc::new(Track::default());
            let domain = Arc::new(cellar::sdk::ExecutionDomain::new(
                tokio::runtime::Handle::current(),
            ));
            let controller = ExecutionController::create(
                domain.clone(),
                ExecutionControllerOptions {
                    directory: root.join("controller"),
                    controller_id: [0; 16],
                    backend: peers.identity(),
                    limits: ExecutionControllerLimits::default(),
                },
                peers.clone(),
                [220; 32],
            )
            .await?;
            let store = Arc::new(if real {
                catalog_fault::Store::around(rustfs::store(
                    match (outage, minimum) {
                        (true, Durability::S3) => "native-container-outage-s3",
                        (true, _) => "native-container-outage-quorum",
                        (false, Durability::S3) => "native-container-s3",
                        (false, _) => "native-container-quorum",
                    },
                    rooted,
                )?)
            } else {
                catalog_fault::Store::default()
            });
            let mut cluster = match start_native(
                rooted,
                minimum,
                store.clone(),
                controller.clone(),
                peers.clone(),
                track.clone(),
            )
            .await
            {
                Ok(cluster) => cluster,
                Err(error) => {
                    controller.shutdown().await?;
                    domain.shutdown().await;
                    return Err(error.context("native container startup"));
                }
            };
            let execution = cluster.fixtures[0].domain.clone();
            let result=execution.run(async {
                let history=|name:&str|->Result<CellHistory>{Ok(CellHistory{cluster:cluster.history.cluster,namespace:cluster.history.namespace,cell:CellId::new(format!("Counter:{name}"))?,incarnation:IncarnationId::new([219;16])?})};
                let a=history("native-a")?;let b=history("native-b")?;
                for (i,h) in [&a,&b].iter().enumerate() {
                    let leader=managed_fixture::leader(&cluster.hosts,cluster.history).await?;
                    cluster.hosts[leader].cell((*h).clone())?.enroll(CellEnrollmentRequest{creation:ProposalId::new([218-i as u8;16])?,durability:minimum},TIMEOUT).await?;
                    if outage { break; }
                }
                if outage {
                    let denied=store.denied_group_archive_writes.load(Ordering::Acquire);
                    store.deny_group_archive_writes.store(true,Ordering::Release);
                    let result=call(&cluster,&a,minimum,1,"Create").await;
                    if minimum==Durability::S3 {
                        let error=result.err().context("native provider escaped S3 gate")?;
                        let original=bindings::options(1,minimum)?.id;
                        ensure!(error.downcast_ref::<Arc<CellCommandError>>().is_some_and(|error|error.command==original && error.ambiguous));
                        ensure!(store.denied_group_archive_writes.load(Ordering::Acquire)>denied, "native archive refusal cut was not entered");
                        ensure!(peers.0.lock().unwrap().is_empty(),"native controller activated before S3 proof");
                    } else {
                        ensure!(result?.durability>=minimum);ensure!(peers.cell(a.cell.as_str())?.creates.load(Ordering::Acquire)==1);
                    }
                    store.deny_group_archive_writes.store(false,Ordering::Release);
                } else {
                    for h in [&a,&b] {
                        let leader=managed_fixture::leader(&cluster.hosts,cluster.history).await?;
                        let cell=cluster.hosts[leader].cell(h.clone())?;
                        let before=cell.query(ProposalId::new([217;16])?,&cell.contract("Version")?,vec![],QueryOptions::default()).await?;
                        let created=call(&cluster,h,minimum,1,"Create").await?;
                        ensure!(created.version.sequence.get()==before.version.sequence.get()+2,"native first Start added a pre-constructor proof");
                    }
                    ensure!(peers.0.lock().unwrap().len()==2,"shared cells borrowed one provider resource");
                    let first=peers.cell(a.cell.as_str())?;
                    first.lose_write.store(true,Ordering::Release);first.hide_data_receipt.store(true,Ordering::Release);
                    let error=call(&cluster,&a,minimum,2,"Write").await.err().context("lost native Write returned success")?;
                    let original=bindings::options(2,minimum)?.id;
                    ensure!(error.downcast_ref::<Arc<CellCommandError>>().is_some_and(|error|error.ambiguous && error.command==original));
                    ensure!(first.writes.load(Ordering::Acquire)==1 && first.lost_data.lock().unwrap().is_some(),"native Write loss cut was not entered");
                    track.restarted.store(true,Ordering::Release);
                    bindings::restart(&mut cluster).await?;
                    let lookups=first.data_lookups.load(Ordering::Acquire);
                    let failed=call(&cluster,&a,minimum,3,"Read").await;
                    ensure!(failed.is_err() && first.data_lookups.load(Ordering::Acquire)>lookups,"unresolved native prologue cut was not entered");
                    ensure!(track.reads.load(Ordering::Acquire)==0 && track.constructed.lock().unwrap().get(a.cell.as_str())==Some(&1),"unresolved native operation entered business construction");
                    ensure!(peers.cell(b.cell.as_str())?.creates.load(Ordering::Acquire)==1);
                    call(&cluster,&b,minimum,2,"Status").await?;
                    first.hide_data_receipt.store(false,Ordering::Release);
                    call(&cluster,&a,minimum,4,"Read").await?;
                    call(&cluster,&a,minimum,5,"Invalid").await?;
                    ensure!(first.writes.load(Ordering::Acquire)==1 && first.executions.load(Ordering::Acquire)==0 && *first.lost_data.lock().unwrap()==*first.recovered_data.lock().unwrap(),"native recovery replayed or changed original work");
                    call(&cluster,&a,minimum,6,"Destroy").await?;
                    call(&cluster,&b,minimum,3,"Destroy").await?;
                    ensure!(peers.0.lock().unwrap().values().all(|(_,peer)|peer.creates.load(Ordering::Acquire)==1 && peer.deletes.load(Ordering::Acquire)==1));
                    ensure!(track.constructed.lock().unwrap().values().all(|count|*count==2));
                }
                eprintln!("native_container_observations {}",json!({"extents":rooted,"minimum":format!("{minimum:?}"),"archive_outage":outage,"cells":if outage{1}else{2},"normal_context_api":true,"original_request_budget_ms":5000}));
                Ok::<_,anyhow::Error>(())
            }).await;
            store
                .deny_group_archive_writes
                .store(false, Ordering::Release);
            let closed = managed_fixture::close_group_nodes(
                std::mem::take(&mut cluster.nodes),
                &cluster.hosts,
            )
            .await;
            for root in std::mem::take(&mut cluster.roots) {
                eprintln!("retained native-container voter: {}", root.keep().display());
            }
            execution.shutdown().await;
            controller.shutdown().await?;
            domain.shutdown().await;
            result
                .map_err(anyhow::Error::from)
                .and_then(std::convert::identity)
                .with_context(|| {
                    format!("native container extents={rooted} minimum={minimum:?} outage={outage}")
                })?;
            closed?;
        }
    }
    Ok(())
}
