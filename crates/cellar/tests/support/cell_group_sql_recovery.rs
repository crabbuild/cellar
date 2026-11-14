// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A durable SQL child survives a parent whose completion was never published.

use super::*;

#[derive(Default)]
struct Stage {
    checkpoint: Notify,
    attempts: AtomicUsize,
}

struct InterruptedSql {
    storage: Storage,
    database: cellar::sdk::sql::Database,
    stage: Arc<Stage>,
}

impl Cell for InterruptedSql {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc { method, args } = invocation else {
                anyhow::bail!("SQL recovery requires RPC");
            };
            match method.as_str() {
                "Interrupt" => {
                    let RpcData::Json(input) = args else {
                        anyhow::bail!("SQL recovery requires JSON");
                    };
                    let failed: bool = serde_json::from_str(&input)?;
                    self.stage.attempts.fetch_add(1, Ordering::AcqRel);
                    // The binding intent checkpoint must publish this write.
                    self.storage.put_json("before", &json!(1))?;
                    if failed {
                        sql_failure(
                            self.database
                                .batch(
                                    &context,
                                    &[
                                        self.database.prepare("INSERT INTO recovered VALUES(999)"),
                                        self.database.prepare("SELECT * FROM missing_table"),
                                    ],
                                )
                                .await,
                            &["no such table: missing_table"],
                        )?;
                    } else {
                        self.database
                            .exec(&context, "INSERT INTO recovered VALUES(1)")
                            .await?;
                    }
                    // The SQL reply is independently durable, but its source
                    // resolution and this write remain private. No sync, new
                    // effect or parent result may publish them before restart.
                    self.storage.put_json("after", &json!(1))?;
                    self.stage.checkpoint.notify_one();
                    std::future::pending().await
                }
                "RecoveryProbe" => {
                    let mut clear = false;
                    let result: Result<()> = self.storage.transaction_sync(|tx| {
                        tx.delete_all()?;
                        clear = true;
                        anyhow::bail!("roll back the recovery probe");
                    });
                    ensure!(result.is_err());
                    Ok(Output::Rpc(RpcData::Json(json!(clear).to_string())))
                }
                "Continue" => {
                    let started = cellar::asyncrt::mono_ms();
                    eprintln!("SQL recovery fresh parent entered");
                    self.database
                        .exec(&context, "INSERT INTO recovered VALUES(2)")
                        .await?;
                    eprintln!(
                        "SQL recovery fresh child returned after {} ms",
                        cellar::asyncrt::mono_ms().saturating_sub(started)
                    );
                    Ok(Output::Rpc(RpcData::Json("true".into())))
                }
                _ => anyhow::bail!("unknown SQL recovery operation"),
            }
        })
    }
}

fn application(
    stage: Arc<Stage>,
) -> Result<(Arc<Registry>, cellar::engine_api::WorkerConfigOptions)> {
    let class = RaftClass::new("Counter", 1, [205; 32])?
        .initializer([206; 32], |_, tx| {
            let storage = tx.storage()?;
            storage.put_json("before", &json!(0))?;
            storage.put_json("after", &json!(0))
        })?
        .json_cell_rpc("Interrupt", [207; 32])?
        .json_cell_rpc("RecoveryProbe", [208; 32])?
        .json_cell_rpc("Continue", [209; 32])?
        .query("SourceState", [210; 32], |query, _| {
            Ok(serde_json::to_vec(&json!({
                "before":query.storage().get_json("before")?,
                "after":query.storage().get_json("after")?,
            }))?)
        })?;
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |context, _| {
                Ok(Arc::new(InterruptedSql {
                    storage: context.storage()?,
                    database: context.sql("DB")?,
                    stage: stage.clone(),
                }))
            },
        )?,
    );
    let mut declared = configuration::options(SCRIPT);
    declared.do_classes = vec!["Counter".into(), cellar::deploy::SQL_CLASS.into()];
    declared.sql_bindings = vec![("DB".into(), DATABASE.into())];
    application.validate_deployment(&configuration::deployment(declared.clone()))?;
    Ok((application, declared))
}

pub async fn qualify(real_rustfs: bool) -> Result<()> {
    logging();
    let mut failures = Vec::new();
    for rooted in [false, true] {
        for durability in [Durability::Quorum, Durability::S3] {
            for failed in [false, true] {
                let result = case(real_rustfs, rooted, durability, failed).await;
                let status = if result.is_ok() { "PASS" } else { "FAILED" };
                eprintln!("SQL recovery RustFS={real_rustfs} extents={rooted} source={durability:?} error={failed}: {status}");
                if let Err(error) = result {
                    eprintln!("SQL recovery failure: {error:#}");
                    failures.push(format!(
                        "extents={rooted} source={durability:?} error={failed}: {error:#}"
                    ));
                }
            }
        }
    }
    ensure!(failures.is_empty(), "SQL recovery: {}", failures.join("; "));
    Ok(())
}

/// The exact post-recovery configuration that failed the initial matrix.
pub async fn extent_error_progress() -> Result<()> {
    logging();
    case(false, true, Durability::S3, true).await
}

fn logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("cellar::raft::cell_groups::controller::effects=debug,cellar::raft::query_connection=debug")
        .with_test_writer()
        .try_init();
}

async fn case(real_rustfs: bool, rooted: bool, durability: Durability, failed: bool) -> Result<()> {
    let store = if real_rustfs {
        Arc::new(catalog_fault::Store::around(rustfs::store(
            &format!("sql-recovery-{durability:?}-error-{failed}"),
            rooted,
        )?))
    } else {
        Arc::new(catalog_fault::Store::default())
    };
    Box::pin(run(rooted, durability, failed, store)).await
}

async fn rows(cluster: &ManagedCluster) -> Result<serde_json::Value> {
    let (status, body) = operator::send(
        cluster,
        1,
        json!({"exec":{"sql":"SELECT v FROM recovered ORDER BY v","rows":true}}),
    )
    .await?;
    ensure!(status == 200, "SQL recovery inspection: {status}: {body}");
    Ok(body["result"]["results"][0]["rows"].clone())
}

async fn source_state(source: &RaftCellHandle, proposal: u8) -> Result<QueryResult> {
    Ok(source
        .query(
            ProposalId::new([proposal; 16])?,
            &source.contract("SourceState")?,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                ..Default::default()
            },
        )
        .await?)
}

async fn run(
    rooted: bool,
    durability: Durability,
    failed: bool,
    store: Arc<catalog_fault::Store>,
) -> Result<()> {
    let stage = Arc::new(Stage::default());
    let (application, declared) = application(stage.clone())?;
    let mut fixture = Fixture::with_namespaces(
        rooted,
        &["Counter", cellar::deploy::SQL_CLASS],
        application,
        store,
    )
    .await?;
    fixture.configuration = Some(declared);
    fixture.sql_workers = 1;
    fixture.max_input_bytes = 16 * 1024;
    fixture.max_client_identities = 4;
    let domain = fixture.domain.clone();
    let started = Box::pin(ManagedCluster::start_with_applications(
        fixture,
        &[],
        ArchiveSetup::Initialized,
        false,
        None,
        FirstGroupSetup::Normal,
    ))
    .await;
    let mut cluster = match started {
        Ok(cluster) => cluster,
        Err(error) => {
            domain.shutdown().await;
            return Err(error);
        }
    };
    let work = domain
        .run(async {
            let sql_group = database_group(&cluster).await?;
            let group = cluster.history;
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            let sql_leader = managed_fixture::leader(&cluster.hosts, sql_group).await?;
            let target = (leader + 1) % 3;
            if sql_leader != target {
                let handle = cluster.hosts[sql_leader].group(sql_group)?;
                let status = handle.replication_status(TIMEOUT).await?;
                handle
                    .transfer_leadership(
                        LeadershipTransfer {
                            target: MemberId::new(target as u64 + 1)?,
                            expected_term: status.term,
                            expected_revision: status.membership_revision,
                        },
                        TIMEOUT,
                    )
                    .await?;
            }
            cellar::asyncrt::timeout(TIMEOUT, async {
                while managed_fixture::leader(&cluster.hosts, sql_group).await? != target {
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
            let (status, body) = operator::send(
                &cluster,
                0,
                json!({"exec":{"sql":"CREATE TABLE recovered(v INTEGER)","rows":true}}),
            )
            .await?;
            ensure!(status == 200, "SQL recovery setup: {status}: {body}");
            let namespace = cellar_native::cell_id::Namespace::new(
                "Counter",
                format!("cells:v1:{}:{SCRIPT}:Counter", SCRIPT.len()),
            );
            let source = cluster.hosts[leader].cell(CellHistory {
                cluster: group.cluster,
                namespace: group.namespace,
                cell: CellId::new(namespace.id_from_name("recovery-source").scope())?,
                incarnation: IncarnationId::new([211; 16])?,
            })?;
            source
                .enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([212; 16])?,
                        durability,
                    },
                    TIMEOUT,
                )
                .await?;
            let original = bindings::options(1, durability)?;
            ensure!(original.timeout == TIMEOUT);
            let input = serde_json::to_vec(&failed)?;
            let hints_before = applied_hints(&cluster)?;
            let call = source
                .command(
                    &source.contract("Interrupt")?,
                    input.clone(),
                    original.clone(),
                )
                .boxed();
            let call = cellar::asyncrt::timeout(TIMEOUT, async {
                match futures_util::future::select(call, stage.checkpoint.notified().boxed()).await
                {
                    futures_util::future::Either::Left((result, _)) => {
                        anyhow::bail!("SQL parent completed before interruption: {result:?}")
                    }
                    futures_util::future::Either::Right(((), call)) => Ok(call),
                }
            })
            .await
            .context("SQL child did not complete within the original five-second budget")??;
            eprintln!("SQL recovery target replied: source={durability:?} error={failed}");
            ensure!(stage.attempts.load(Ordering::Acquire) == 1);
            let observed = source_state(&source, 213).await?;
            ensure!(
                serde_json::from_slice::<serde_json::Value>(&observed.result)?
                    == json!({"before":1,"after":0}),
                "source must publish its intent prefix and retain later writes privately"
            );
            let hints = applied_hints(&cluster)?
                .checked_sub(hints_before)
                .context("SQL hints regressed")?;
            ensure!(hints <= 1);
            ensure!(
                observed.version.sequence.get() == if hints == 1 { 2 } else { 3 },
                "parent published an extra completion/resolution checkpoint before interruption"
            );
            let history = source.history().clone();
            eprintln!(
                "SQL recovery source prefix: sequence={} applied_hints={hints}",
                observed.version.sequence.get()
            );
            let (restarted, interrupted) =
                futures_util::future::join(Box::pin(restart(&mut cluster)), call).await;
            restarted?;
            eprintln!("SQL recovery retained restart joined");
            let error = interrupted
                .err()
                .context("interrupted SQL parent cannot complete")?;
            ensure!(error.ambiguous, "SQL parent intent was durable: {error}");
            drop(source);
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            managed_fixture::leader(&cluster.hosts, sql_group).await?;
            let source = cluster.hosts[leader].cell(history)?;
            let expected = if failed { json!([]) } else { json!([[1]]) };
            ensure!(
                rows(&cluster).await? == expected,
                "SQL child result/rollback did not survive restart"
            );
            // No retry or explicit maintenance call starts recovery. Its committed
            // due index and node-owned worker must settle the original obligation.
            cellar::asyncrt::sleep(Duration::from_secs(2)).await;
            let until = cellar::asyncrt::mono_ms() + 60_000;
            let mut sequence = 2;
            loop {
                let probe = source
                    .command(
                        &source.contract("RecoveryProbe")?,
                        b"null".to_vec(),
                        bindings::options(sequence, durability)?,
                    )
                    .await?;
                sequence += 1;
                if probe.result == b"true" {
                    break;
                }
                ensure!(probe.result == b"false");
                ensure!(
                    cellar::asyncrt::mono_ms() < until,
                    "SQL binding recovery did not settle its source lane"
                );
                cellar::asyncrt::sleep(Duration::from_millis(200)).await;
            }
            eprintln!(
                "SQL recovery obligation settled after {} probes",
                sequence - 2
            );
            let retry = source
                .command(&source.contract("Interrupt")?, input, original)
                .await;
            ensure!(
                retry
                    .err()
                    .context("SQL parent retry fabricated completion")?
                    .ambiguous
            );
            ensure!(
                stage.attempts.load(Ordering::Acquire) == 1,
                "recovery replayed the interrupted SQL parent"
            );
            ensure!(
                rows(&cluster).await? == expected,
                "recovery replayed a committed/failed SQL mutation"
            );
            let observed = source_state(&source, 214).await?;
            ensure!(
                serde_json::from_slice::<serde_json::Value>(&observed.result)?
                    == json!({"before":1,"after":0}),
                "private post-child writes or recovery probes escaped rollback"
            );
            let next = source
                .command(
                    &source.contract("Continue")?,
                    b"null".to_vec(),
                    bindings::options(sequence, durability)?,
                )
                .await;
            if let Err(error) = &next {
                eprintln!("SQL recovery fresh parent failed: {error}");
                eprintln!(
                    "SQL recovery after failure source: {:?}",
                    source_state(&source, 215).await
                );
                eprintln!(
                    "SQL recovery after failure rows: {:?}",
                    rows(&cluster).await
                );
                for (node, host) in cluster.hosts.iter().enumerate() {
                    eprintln!(
                        "SQL recovery performance node={node}: {}",
                        serde_json::to_string(&host.performance())?
                    );
                }
            }
            let next = next?;
            ensure!(next.result == b"true");
            let expected = if failed {
                json!([[2]])
            } else {
                json!([[1], [2]])
            };
            ensure!(
                rows(&cluster).await? == expected,
                "fresh SQL binding did not advance after recovery"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(std::convert::identity);
    let joined = managed_fixture::close_group_nodes_with_groups(
        std::mem::take(&mut cluster.nodes),
        &cluster.hosts,
        2,
    )
    .await;
    if work.is_err() || joined.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!("retained SQL recovery disk: {}", root.keep().display());
        }
    }
    domain.shutdown().await;
    work?;
    joined
}
