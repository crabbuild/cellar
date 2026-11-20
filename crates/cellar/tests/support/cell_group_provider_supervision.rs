// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public supervision on managed RF3. The backend is an identity-checking fixture,
//! not proof of a real provider's execution fencing or persistent volume recovery.
use super::*;
use cellar::sdk::containers::{
    supervise, BackendIdentity, BindingCheckpoint, BindingIntent, BindingJournal,
    BindingObservation, BindingRecord, DeleteSafety, MaintenanceAction, MaintenanceBackend,
    MaintenancePolicy, Supervision,
};
use std::sync::atomic::AtomicBool;

#[derive(Default)]
struct Probe {
    inspections: AtomicUsize,
    deletes: AtomicUsize,
    confirmed: AtomicBool,
    retired: AtomicBool,
    exists: AtomicBool,
    fail_inspection: AtomicBool,
    lose_delete_reply: AtomicBool,
    hold_delete: AtomicBool,
    delete_entered: AtomicBool,
    refuse_factory: AtomicBool,
    changed: Notify,
    release: Notify,
    observations: Mutex<Vec<(String, u64, String)>>,
}

fn pending() -> BindingRecord {
    BindingRecord {
        provider: "supervised-fixture".into(),
        configuration_digest: hex::encode([191; 32]),
        parameters_digest: hex::encode([192; 32]),
        generation: 1,
        request_id: hex::encode([193; 16]),
        resource_id: None,
        intent: BindingIntent::Running,
        observation: BindingObservation::Pending,
    }
}

struct Backend(Arc<Probe>);

impl MaintenanceBackend for Backend {
    fn identity(&self) -> BackendIdentity {
        BackendIdentity {
            binding: "TOOL".into(),
            provider: "supervised-fixture".into(),
            configuration_digest: hex::encode([191; 32]),
        }
    }

    fn delete_safety(&self) -> DeleteSafety {
        DeleteSafety::RevalidateExactResource
    }

    fn maintain(
        self: Arc<Self>,
        context: Context,
        expected: BindingCheckpoint,
        action: MaintenanceAction,
    ) -> HandlerFuture<()> {
        Box::pin(async move {
            let authority = context.container_provider_authority()?;
            authority.check(&context)?;
            let journal = BindingJournal::open(&context, "TOOL")?;
            ensure!(journal.load(&context)?.as_ref() == Some(&expected));
            ensure!(expected.record.provider == "supervised-fixture");
            ensure!(expected.record.configuration_digest == hex::encode([191; 32]));
            ensure!(expected.record.request_id == hex::encode([193; 16]));
            ensure!(expected.record.generation == 1);
            self.0.observations.lock().unwrap().push((
                context.identity().context("provider owner")?.scope.clone(),
                expected.record.generation,
                expected.record.request_id.clone(),
            ));
            if action == MaintenanceAction::Inspect {
                self.0.inspections.fetch_add(1, Ordering::AcqRel);
                if self.0.fail_inspection.swap(false, Ordering::AcqRel) {
                    anyhow::bail!("fixture inspection failed without changing the reservation");
                }
            } else {
                ensure!(expected.record.resource_id.as_deref() == Some("fixture/resource/1"));
                self.0.delete_entered.store(true, Ordering::Release);
                self.0.changed.notify_waiters();
                loop {
                    let released = self.0.release.notified();
                    if !self.0.hold_delete.load(Ordering::Acquire) {
                        break;
                    }
                    released.await;
                }
                authority.check(&context)?;
                ensure!(journal.load(&context)?.as_ref() == Some(&expected));
                // Revalidation models authenticated absence after a lost reply.
                // Never submit another delete for an already absent resource.
                if self.0.exists.swap(false, Ordering::AcqRel) {
                    self.0.deletes.fetch_add(1, Ordering::AcqRel);
                    if self.0.lose_delete_reply.swap(false, Ordering::AcqRel) {
                        anyhow::bail!("fixture delete committed but its reply was lost");
                    }
                }
            }
            let mut record = expected.record;
            if self.0.exists.load(Ordering::Acquire) {
                record.resource_id = Some("fixture/resource/1".into());
                record.observation = BindingObservation::Confirmed;
            } else {
                record.intent = BindingIntent::Deleted;
                record.observation = BindingObservation::Confirmed;
            }
            let checkpoint = journal
                .compare_exchange(&context, Some(expected.revision), record)
                .await?;
            if checkpoint.record.is_retired() {
                self.0.retired.store(true, Ordering::Release);
            } else {
                self.0.confirmed.store(true, Ordering::Release);
            }
            self.0.changed.notify_waiters();
            Ok(())
        })
    }
}

struct Root(Storage);

pub fn contracts() -> Result<()> {
    let probe = Arc::new(Probe::default());
    let backend: Arc<dyn MaintenanceBackend> = Arc::new(Backend(probe.clone()));
    let application = |digest| -> Result<Arc<dyn Application>> {
        let class = RaftClass::new("Counter", 1, [194; 32])?
            .initializer([195; 32], |_, _| Ok(()))?
            .json_cell_rpc("Run", [196; 32])?
            .provider_binding("TOOL", "supervised-fixture", digest)?;
        Ok(Arc::new(
            Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
                .raft_cell(class, |context, _| Ok(Arc::new(Root(context.storage()?))))?,
        ))
    };
    let policy = MaintenancePolicy {
        inspect_interval_ms: 250,
        maximum_lifetime_ms: 300_000,
        attempt_timeout_ms: 3000,
        idle_timeout_ms: None,
    };
    let registry = |policy| -> Result<Supervision> {
        let mut registry = Supervision::default();
        registry.register(backend.clone(), policy)?;
        Ok(registry)
    };
    let plain = application([191; 32])?;
    let supervised = supervise(plain.clone(), registry(policy)?);
    let definition = supervised.resolve_raft_class("Counter")?;
    ensure!(definition.schema_digest() != plain.resolve_raft_class("Counter")?.schema_digest());
    ensure!(definition.contract("__sys_raft_native_work").is_none());
    for changed in [
        MaintenancePolicy {
            inspect_interval_ms: 251,
            ..policy
        },
        MaintenancePolicy {
            maximum_lifetime_ms: 300_001,
            ..policy
        },
        MaintenancePolicy {
            attempt_timeout_ms: 3001,
            ..policy
        },
        MaintenancePolicy {
            idle_timeout_ms: Some(10),
            ..policy
        },
    ] {
        ensure!(
            supervise(plain.clone(), registry(changed)?)
                .resolve_raft_class("Counter")?
                .schema_digest()
                != definition.schema_digest()
        );
    }
    ensure!(
        supervise(supervised, registry(policy)?)
            .resolve_raft_class("Counter")?
            .schema_digest()
            == definition.schema_digest()
    );
    ensure!(supervise(application([190; 32])?, registry(policy)?)
        .resolve_raft_class("Counter")
        .is_err());
    ensure!(
        probe.inspections.load(Ordering::Acquire) == 0
            && probe.deletes.load(Ordering::Acquire) == 0
    );
    Ok(())
}

impl Cell for Root {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc {
                args: RpcData::Json(input),
                ..
            } = invocation
            else {
                anyhow::bail!("supervision fixture requires JSON RPC");
            };
            let operation: String = serde_json::from_str(&input)?;
            let journal = BindingJournal::open(&context, "TOOL")?;
            let _lifecycle = journal.lock_lifecycle(&context).await?;
            let checkpoint = match operation.as_str() {
                "Create" => {
                    self.0.put_json("business", &json!(7))?;
                    Some(journal.compare_exchange(&context, None, pending()).await?)
                }
                "Read" => {
                    ensure!(self.0.get_json("business")? == Some(json!(7)));
                    journal.load(&context)?
                }
                "Delete" => {
                    let old = journal.load(&context)?.context("confirmed resource")?;
                    ensure!(old.record.observation == BindingObservation::Confirmed);
                    let mut record = old.record;
                    record.intent = BindingIntent::Deleted;
                    record.observation = BindingObservation::Pending;
                    Some(
                        journal
                            .compare_exchange(&context, Some(old.revision), record)
                            .await?,
                    )
                }
                "Absent" => {
                    ensure!(journal.load(&context)?.is_none());
                    None
                }
                _ => anyhow::bail!("unknown supervision fixture method"),
            };
            let output =
                checkpoint.map(|value| json!({"revision":value.revision,"record":value.record}));
            Ok(Output::Rpc(RpcData::Json(serde_json::to_string(&output)?)))
        })
    }
}

async fn started(
    rooted: bool,
    minimum: Durability,
    backend: Arc<catalog_fault::Store>,
    probe: Arc<Probe>,
) -> Result<ManagedCluster> {
    let factory_probe = probe.clone();
    let class = RaftClass::new("Counter", 1, [194; 32])?
        .initializer([195; 32], |_, _| Ok(()))?
        .json_cell_rpc("Run", [196; 32])?
        .provider_binding("TOOL", "supervised-fixture", [191; 32])?;
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |context, _| {
                ensure!(
                    !factory_probe.refuse_factory.load(Ordering::Acquire),
                    "fixture business constructor is unavailable"
                );
                Ok(Arc::new(Root(context.storage()?)))
            },
        )?,
    );
    let mut supervision = Supervision::default();
    supervision.register(
        Arc::new(Backend(probe)),
        MaintenancePolicy {
            inspect_interval_ms: 250,
            maximum_lifetime_ms: 300_000,
            attempt_timeout_ms: 3000,
            idle_timeout_ms: None,
        },
    )?;
    let application = supervise(application, supervision);
    let mut options = configuration::options("shared-provider-supervision");
    options.do_classes = vec!["Counter".into()];
    application.validate_deployment(&configuration::deployment(options.clone()))?;
    let definition = application.resolve_raft_class("Counter")?;
    let mut fixture = Fixture::with_definition_policies(
        rooted,
        vec![(definition, minimum)],
        application,
        backend,
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
    cell: &CellHistory,
    minimum: Durability,
    sequence: u64,
    op: &str,
) -> Result<CommandReceipt> {
    let leader = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
    let cell = cluster.hosts[leader].cell(cell.clone())?;
    Ok(cell
        .command(
            &cell.contract("Run")?,
            serde_json::to_vec(op)?,
            bindings::options(sequence, minimum)?,
        )
        .await?)
}

async fn wait(probe: &Probe, flag: &AtomicBool, stage: &str) -> Result<()> {
    cellar::asyncrt::timeout(Duration::from_secs(60), async {
        loop {
            let changed = probe.changed.notified();
            if flag.load(Ordering::Acquire) {
                break;
            }
            changed.await;
        }
    })
    .await
    .with_context(|| format!("autonomous provider {stage} required application traffic"))
}

pub async fn qualify(real_rustfs: bool) -> Result<()> {
    for rooted in [false, true] {
        for minimum in [Durability::Quorum, Durability::S3] {
            let backend = Arc::new(if real_rustfs {
                catalog_fault::Store::around(rustfs::store(
                    if minimum == Durability::S3 {
                        "provider-supervision-s3"
                    } else {
                        "provider-supervision-quorum"
                    },
                    rooted,
                )?)
            } else {
                catalog_fault::Store::default()
            });
            let probe = Arc::new(Probe::default());
            probe.exists.store(true, Ordering::Release);
            probe.fail_inspection.store(true, Ordering::Release);
            probe.lose_delete_reply.store(true, Ordering::Release);
            let mut cluster = started(rooted, minimum, backend, probe.clone()).await?;
            let domain = cluster.fixtures[0].domain.clone();
            let work = domain
                .run(async {
                    let group = cluster.history;
                    let history = CellHistory {
                        cluster: group.cluster,
                        namespace: group.namespace,
                        cell: CellId::new("Counter:supervised-provider")?,
                        incarnation: IncarnationId::new([197; 16])?,
                    };
                    let leader = managed_fixture::leader(&cluster.hosts, group).await?;
                    cluster.hosts[leader]
                        .cell(history.clone())?
                        .enroll(
                            CellEnrollmentRequest {
                                creation: ProposalId::new([198; 16])?,
                                durability: minimum,
                            },
                            TIMEOUT,
                        )
                        .await?;
                    let first = call(&cluster, &history, minimum, 1, "Create").await?;
                    // No application request or polling read may drive due work.
                    wait(&probe, &probe.confirmed, "inspection/retry").await?;
                    ensure!(probe.inspections.load(Ordering::Acquire) >= 2);
                    let observed = call(&cluster, &history, minimum, 2, "Read").await?;
                    ensure!(
                        serde_json::from_slice::<serde_json::Value>(&observed.result)?["record"]
                            ["observation"]
                            == "confirmed"
                    );
                    let other = CellHistory {
                        cell: CellId::new("Counter:supervised-other")?,
                        incarnation: IncarnationId::new([199; 16])?,
                        ..history.clone()
                    };
                    let leader = managed_fixture::leader(&cluster.hosts, group).await?;
                    cluster.hosts[leader]
                        .cell(other.clone())?
                        .enroll(
                            CellEnrollmentRequest {
                                creation: ProposalId::new([200; 16])?,
                                durability: minimum,
                            },
                            TIMEOUT,
                        )
                        .await?;
                    ensure!(call(&cluster, &other, minimum, 1, "Absent").await?.result == b"null");
                    probe.hold_delete.store(true, Ordering::Release);
                    call(&cluster, &history, minimum, 3, "Delete").await?;
                    wait(&probe, &probe.delete_entered, "delete admission").await?;
                    ensure!(probe.deletes.load(Ordering::Acquire) == 0);
                    probe.refuse_factory.store(true, Ordering::Release);
                    bindings::restart(&mut cluster).await?;
                    probe.hold_delete.store(false, Ordering::Release);
                    probe.release.notify_waiters();
                    wait(&probe, &probe.retired, "restart/lost-delete reconciliation").await?;
                    probe.refuse_factory.store(false, Ordering::Release);
                    let retired = call(&cluster, &history, minimum, 4, "Read").await?;
                    let value: serde_json::Value = serde_json::from_slice(&retired.result)?;
                    ensure!(
                        value["record"]["intent"] == "deleted"
                            && value["record"]["observation"] == "confirmed"
                    );
                    ensure!(
                        value["record"]["request_id"] == hex::encode([193; 16])
                            && value["record"]["generation"] == 1
                    );
                    ensure!(
                        probe.deletes.load(Ordering::Acquire) == 1,
                        "lost delete reply repeated the mutation"
                    );
                    ensure!(
                        call(&cluster, &history, minimum, 1, "Create").await?.result
                            == first.result
                    );
                    ensure!(probe.observations.lock().unwrap().iter().all(
                        |(cell, generation, id)| cell == history.cell.as_str()
                            && *generation == 1
                            && *id == hex::encode([193; 16])
                    ));
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
                    "retained supervised provider disk: {}",
                    root.keep().display()
                );
            }
            domain.shutdown().await;
            work.with_context(|| {
                format!("provider supervision extents={rooted} minimum={minimum:?}")
            })?;
            closed?;
        }
    }
    Ok(())
}
