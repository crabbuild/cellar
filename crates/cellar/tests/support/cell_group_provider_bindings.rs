// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The public provider journal over actual managed RF3 command/checkpoint owners.
//! This qualifies local journal/proof authority, not remote provider fencing.
use super::*;
use cellar::sdk::containers::{
    BindingIntent, BindingJournal, BindingObservation, BindingRecord, ProviderAuthority,
};

const SCRIPT: &str = "shared-provider-journal";

#[derive(Default)]
struct Probe {
    journal: Mutex<Option<BindingJournal>>,
    authority: Mutex<Option<Arc<ProviderAuthority>>>,
    context: Mutex<Option<Context>>,
    effects: AtomicUsize,
}

struct Root(Storage, Arc<Probe>);

fn pending(generation: u64) -> BindingRecord {
    BindingRecord {
        provider: "docker".into(),
        configuration_digest: hex::encode([19; 32]),
        parameters_digest: hex::encode([20; 32]),
        generation,
        request_id: hex::encode([20 + generation as u8; 16]),
        resource_id: None,
        intent: BindingIntent::Running,
        observation: BindingObservation::Pending,
    }
}

impl Cell for Root {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc {
                args: RpcData::Json(input),
                ..
            } = invocation
            else {
                anyhow::bail!("provider test requires JSON RPC");
            };
            let operation: String = serde_json::from_str(&input)?;
            if let Some(prior) = self.1.journal.lock().unwrap().as_ref() {
                ensure!(
                    prior.load(&context).is_err(),
                    "journal crossed invocation ownership"
                );
            }
            if let Some(prior) = self.1.authority.lock().unwrap().as_ref() {
                ensure!(
                    prior.check(&context).is_err(),
                    "provider authority crossed invocations"
                );
            }
            let journal = BindingJournal::open(&context, "TOOL")?;
            let _lifecycle = journal.lock_lifecycle(&context).await?;
            let result = match operation.as_str() {
                "Create" => {
                    ensure!(journal.load(&context)?.is_none());
                    self.0.put_json("before-provider", &json!(true))?;
                    let checkpoint = journal.compare_exchange(&context, None, pending(1)).await?;
                    ensure!(checkpoint.revision == 1 && checkpoint.record == pending(1));
                    ensure!(journal
                        .compare_exchange(&context, None, pending(1))
                        .await
                        .is_err());
                    ensure!(self
                        .0
                        .sql("SELECT record FROM __sys_container_bindings", &[])
                        .is_err());
                    self.0.delete_all()?;
                    ensure!(
                        journal.load(&context)?.as_ref() == Some(&checkpoint),
                        "deleteAll erased host provider identity"
                    );
                    *self.1.journal.lock().unwrap() = Some(journal.clone());
                    *self.1.context.lock().unwrap() = Some(context.clone());
                    *self.1.authority.lock().unwrap() =
                        Some(Arc::new(context.container_provider_authority()?));
                    json!({"revision":checkpoint.revision,"record":checkpoint.record})
                }
                "Confirm" => {
                    let prior = journal.load(&context)?.context("pending provider")?;
                    let authority = context.container_provider_authority()?;
                    self.0.put_json("before-effect", &json!(true))?;
                    authority.before_effect(&context).await?;
                    self.1.effects.fetch_add(1, Ordering::AcqRel);
                    let mut record = prior.record;
                    record.resource_id = Some("docker/run/1".into());
                    record.observation = BindingObservation::Confirmed;
                    let checkpoint = journal
                        .compare_exchange(&context, Some(prior.revision), record)
                        .await?;
                    journal.activity(&context, 1).await?;
                    journal.check_retention(&context)?;
                    json!({"revision":checkpoint.revision,"record":checkpoint.record})
                }
                "Refusals" => {
                    ensure!(BindingJournal::open(&context, "UNDECLARED").is_err());
                    ensure!(cellar::sdk::containers::SandboxCatalog::open(&context).is_err());
                    ensure!(context.container().is_err());
                    let prior = journal.load(&context)?.context("confirmed provider")?;
                    for kind in 0..3 {
                        let mut record = prior.record.clone();
                        match kind {
                            0 => record.provider = "another-provider".into(),
                            1 => record.configuration_digest = hex::encode([99; 32]),
                            _ => record.resource_id = Some("different-resource".into()),
                        }
                        ensure!(journal
                            .compare_exchange(&context, Some(prior.revision), record)
                            .await
                            .is_err());
                    }
                    self.0.transaction_sync(|_| {
                        ensure!(BindingJournal::open(&context, "TOOL").is_err());
                        Ok(())
                    })?;
                    ensure!(journal.load(&context)?.as_ref() == Some(&prior));
                    json!(true)
                }
                "Delete" => {
                    let prior = journal.load(&context)?.context("running provider")?;
                    let mut record = prior.record;
                    record.intent = BindingIntent::Deleted;
                    record.observation = BindingObservation::Pending;
                    let checkpoint = journal
                        .compare_exchange(&context, Some(prior.revision), record)
                        .await?;
                    ensure!(
                        journal
                            .compare_exchange(&context, Some(checkpoint.revision), pending(2))
                            .await
                            .is_err(),
                        "pending deletion authorized a new generation"
                    );
                    json!({"revision":checkpoint.revision,"record":checkpoint.record})
                }
                "Retire" => {
                    let prior = journal.load(&context)?.context("pending delete")?;
                    let mut record = prior.record;
                    record.observation = BindingObservation::Confirmed;
                    let checkpoint = journal
                        .compare_exchange(&context, Some(prior.revision), record)
                        .await?;
                    ensure!(checkpoint.record.is_retired());
                    json!({"revision":checkpoint.revision,"record":checkpoint.record})
                }
                "Recreate" => {
                    let prior = journal.load(&context)?.context("retired provider")?;
                    let checkpoint = journal
                        .compare_exchange(&context, Some(prior.revision), pending(2))
                        .await?;
                    json!({"revision":checkpoint.revision,"record":checkpoint.record})
                }
                "Inspect" => {
                    let checkpoint = journal.load(&context)?.context("provider checkpoint")?;
                    json!({"revision":checkpoint.revision,"record":checkpoint.record})
                }
                "Absent" => {
                    ensure!(
                        journal.load(&context)?.is_none(),
                        "provider journal crossed logical cells"
                    );
                    json!(null)
                }
                "Gate" => {
                    let authority = context.container_provider_authority()?;
                    self.0.put_json("provider-gate-attempt", &json!(true))?;
                    authority.before_effect(&context).await?;
                    self.1.effects.fetch_add(1, Ordering::AcqRel);
                    json!(true)
                }
                _ => anyhow::bail!("unknown provider test operation"),
            };
            Ok(Output::Rpc(RpcData::Json(result.to_string())))
        })
    }
}

pub fn declarations() -> Result<()> {
    let class = || RaftClass::new("Counter", 1, [211; 32]);
    let first = class()?.provider_binding("TOOL", "docker", [19; 32])?;
    ensure!(first.schema_digest() != class()?.schema_digest());
    ensure!(
        first.schema_digest()
            != class()?
                .provider_binding("TOOL", "docker", [18; 32])?
                .schema_digest()
    );
    ensure!(class()?
        .provider_binding("TOOL", "docker", [19; 32])?
        .provider_binding("TOOL", "docker", [19; 32])
        .is_err());
    for (name, provider, digest) in [
        ("bad/name", "docker", [19; 32]),
        ("TOOL", "bad/name", [19; 32]),
        ("TOOL", "docker", [0; 32]),
    ] {
        ensure!(class()?.provider_binding(name, provider, digest).is_err());
    }
    let forward = class()?
        .provider_binding("A", "docker", [19; 32])?
        .provider_binding("B", "docker", [18; 32])?;
    let reverse = class()?
        .provider_binding("B", "docker", [18; 32])?
        .provider_binding("A", "docker", [19; 32])?;
    ensure!(forward.schema_digest() == reverse.schema_digest());
    let mut full = class()?;
    for index in 0..cellar::sdk::containers::MAX_BINDINGS {
        full = full.provider_binding(format!("tool-{index}"), "docker", [19; 32])?;
    }
    ensure!(full
        .provider_binding("overflow", "docker", [19; 32])
        .is_err());
    Ok(())
}

async fn start(
    rooted: bool,
    minimum: Durability,
    backend: Arc<catalog_fault::Store>,
    probe: Arc<Probe>,
    max_input_bytes: usize,
) -> Result<ManagedCluster> {
    let class = RaftClass::new("Counter", 1, [211; 32])?
        .initializer([212; 32], |_, tx| {
            tx.execute_batch(
                "CREATE TABLE counter(value INTEGER NOT NULL); INSERT INTO counter VALUES(0)",
            )?;
            Ok(())
        })?
        .json_cell_rpc("Run", [213; 32])?
        .provider_binding("TOOL", "docker", [19; 32])?;
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |context, _| {
                ensure!(
                    BindingJournal::open(&context, "TOOL").is_err(),
                    "constructor acquired invocation authority"
                );
                Ok(Arc::new(Root(context.storage()?, probe.clone())))
            },
        )?,
    );
    let mut configuration = configuration::options(SCRIPT);
    configuration.do_classes = vec!["Counter".into()];
    application.validate_deployment(&configuration::deployment(configuration.clone()))?;
    let definition = application.resolve_raft_class("Counter")?;
    let mut fixture = Fixture::with_definition_policies(
        rooted,
        vec![(definition, minimum)],
        application,
        backend,
    )
    .await?;
    fixture.configuration = Some(configuration);
    fixture.max_input_bytes = max_input_bytes;
    fixture.sql_workers = 1;
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
    sequence: u64,
    operation: &str,
) -> Result<CommandReceipt> {
    let leader = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
    let cell = cluster.hosts[leader].cell(history.clone())?;
    Ok(cell
        .command(
            &cell.contract("Run")?,
            serde_json::to_vec(operation)?,
            bindings::options(sequence, minimum)?,
        )
        .await?)
}

pub async fn qualify(real_rustfs: bool) -> Result<()> {
    qualify_with_input_limit(real_rustfs, 64 * 1024).await
}

pub async fn qualify_small_input() -> Result<()> {
    // Caller input and framework-generated SQLite checkpoints have independent
    // bounds. A small request must not require a 64-KiB caller allowance.
    qualify_with_input_limit(false, 1024).await
}

async fn qualify_with_input_limit(real_rustfs: bool, max_input_bytes: usize) -> Result<()> {
    for rooted in [false, true] {
        for minimum in [Durability::Quorum, Durability::S3] {
            let backend = if real_rustfs {
                Arc::new(catalog_fault::Store::around(rustfs::store(
                    if minimum == Durability::S3 {
                        "provider-journal-s3"
                    } else {
                        "provider-journal-quorum"
                    },
                    rooted,
                )?))
            } else {
                Arc::new(catalog_fault::Store::default())
            };
            let probe = Arc::new(Probe::default());
            let mut cluster = start(
                rooted,
                minimum,
                backend.clone(),
                probe.clone(),
                max_input_bytes,
            )
            .await?;
            let domain = cluster.fixtures[0].domain.clone();
            let work = domain
                .run(async {
                    let group = cluster.history;
                    let history = CellHistory {
                        cluster: group.cluster,
                        namespace: group.namespace,
                        cell: CellId::new("Counter:provider-journal")?,
                        incarnation: IncarnationId::new([214; 16])?,
                    };
                    let leader = managed_fixture::leader(&cluster.hosts, group).await?;
                    cluster.hosts[leader]
                        .cell(history.clone())?
                        .enroll(
                            CellEnrollmentRequest {
                                creation: ProposalId::new([215; 16])?,
                                durability: minimum,
                            },
                            TIMEOUT,
                        )
                        .await?;
                    let first = call(&cluster, &history, minimum, 1, "Create").await?;
                    let old_context = probe
                        .context
                        .lock()
                        .unwrap()
                        .clone()
                        .context("captured invocation")?;
                    ensure!(
                        probe
                            .journal
                            .lock()
                            .unwrap()
                            .as_ref()
                            .context("captured journal")?
                            .load(&old_context)
                            .is_err(),
                        "journal escaped its executing invocation"
                    );
                    let old_authority = probe
                        .authority
                        .lock()
                        .unwrap()
                        .clone()
                        .context("captured authority")?;
                    ensure!(
                        old_authority.before_effect(&old_context).await.is_err(),
                        "retired invocation authorized provider work"
                    );
                    let replay = call(&cluster, &history, minimum, 1, "Create").await?;
                    ensure!(
                        replay.result == first.result,
                        "journal creation replay changed its receipt"
                    );
                    let other = CellHistory {
                        cell: CellId::new("Counter:provider-other")?,
                        incarnation: IncarnationId::new([216; 16])?,
                        ..history.clone()
                    };
                    let owner = managed_fixture::leader(&cluster.hosts, group).await?;
                    cluster.hosts[owner]
                        .cell(other.clone())?
                        .enroll(
                            CellEnrollmentRequest {
                                creation: ProposalId::new([217; 16])?,
                                durability: minimum,
                            },
                            TIMEOUT,
                        )
                        .await?;
                    ensure!(call(&cluster, &other, minimum, 1, "Absent").await?.result == b"null");
                    let confirmed = call(&cluster, &history, minimum, 2, "Confirm").await?;
                    ensure!(
                        serde_json::from_slice::<serde_json::Value>(&confirmed.result)?["revision"]
                            == 2
                    );
                    ensure!(probe.effects.load(Ordering::Acquire) == 1);
                    call(&cluster, &history, minimum, 3, "Refusals").await?;
                    bindings::restart(&mut cluster).await?;
                    ensure!(
                        call(&cluster, &history, minimum, 1, "Create").await?.result
                            == first.result
                    );
                    let current = call(&cluster, &history, minimum, 4, "Inspect").await?;
                    ensure!(
                        current.result == confirmed.result,
                        "retained restart changed the provider identity"
                    );
                    for (sequence, operation, revision) in
                        [(5, "Delete", 3), (6, "Retire", 4), (7, "Recreate", 5)]
                    {
                        let value = call(&cluster, &history, minimum, sequence, operation).await?;
                        ensure!(
                            serde_json::from_slice::<serde_json::Value>(&value.result)?["revision"]
                                == revision
                        );
                    }
                    ensure!(
                        probe.effects.load(Ordering::Acquire) == 1,
                        "journal replay repeated a provider effect"
                    );
                    let owner = managed_fixture::leader(&cluster.hosts, group).await?;
                    let source = cluster.hosts[owner].cell(history.clone())?;
                    let contract = source.contract("Run")?;
                    let input = serde_json::to_vec("Gate")?;
                    let options = bindings::options(8, minimum)?;
                    backend
                        .deny_group_archive_writes
                        .store(true, Ordering::Release);
                    let response = source
                        .command(&contract, input.clone(), options.clone())
                        .await;
                    backend
                        .deny_group_archive_writes
                        .store(false, Ordering::Release);
                    if minimum == Durability::Quorum {
                        ensure!(response?.result == b"true");
                        ensure!(probe.effects.load(Ordering::Acquire) == 2);
                    } else {
                        let error = response
                            .err()
                            .context("S3 provider effect escaped archive refusal")?;
                        ensure!(
                            error.command == options.id && error.ambiguous,
                            "lost provider response changed its reconciliation identity: {error}"
                        );
                        ensure!(
                            probe.effects.load(Ordering::Acquire) == 1,
                            "unproven source entered provider work"
                        );
                        eprintln!("retained provider gate unknown command: {:?}", options.id);
                    }
                    // This reads only the original receipt. Absence stays ambiguous;
                    // it never authorizes replay of the application/provider body.
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
                        .await;
                    if minimum == Durability::Quorum {
                        let receipt = receipt?;
                        ensure!(
                            receipt.command() == options.id
                                && receipt.durability() >= minimum
                                && receipt.receipt().is_some_and(|row| row.result() == b"true")
                        );
                    } else {
                        // The denied output checkpoint can already be committed
                        // to quorum when the original invocation expires. That
                        // partial RPC must remain uncertain: backend restoration
                        // cannot fabricate its final result or rerun its body.
                        let error = receipt
                            .err()
                            .context("partial provider RPC became complete")?;
                        ensure!(
                            matches!(
                                error.as_ref(),
                                CellDispatchError::Execution(ExecutionError::Uncertain)
                            ),
                            "original provider checkpoint lost its typed uncertainty: {error}"
                        );
                    }
                    ensure!(
                        probe.effects.load(Ordering::Acquire)
                            == if minimum == Durability::Quorum { 2 } else { 1 },
                        "receipt observation redispatched provider work"
                    );
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
                eprintln!("retained provider journal disk: {}", root.keep().display());
            }
            domain.shutdown().await;
            work.with_context(|| format!("provider journal extents={rooted} minimum={minimum:?}"))?;
            closed?;
        }
    }
    Ok(())
}
