// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Existing named targets remain discoverable while mutation admission is full.
use super::*;

const SCRIPT: &str = "binding-admission-service";

#[derive(Default)]
struct Control {
    entered: Notify,
    proceed: Notify,
    initialized: AtomicUsize,
}

struct Bound {
    storage: Storage,
    control: Arc<Control>,
}

impl Cell for Bound {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc { method, .. } = invocation else {
                anyhow::bail!("RPC required");
            };
            let value = if method == "Run" {
                self.control.entered.notify_one();
                self.control.proceed.notified().await;
                let reply = context
                    .namespace("COUNTERS")?
                    .get_by_name("target")
                    .rpc(&context, "Child", RpcData::Json("[]".into()))
                    .await?;
                let RpcData::Json(reply) = reply else {
                    anyhow::bail!("JSON child reply required");
                };
                let value: serde_json::Value = serde_json::from_str(&reply)?;
                self.storage.put_json("value", &value)?;
                value
            } else {
                self.storage.tx(|tx| {
                    let value = tx
                        .get_json("value")?
                        .context("child value")?
                        .as_i64()
                        .context("integer child")?
                        + 1;
                    tx.put_json("value", &json!(value))?;
                    Ok(json!(value))
                })?
            };
            Ok(Output::Rpc(RpcData::Json(value.to_string())))
        })
    }
}

struct Release {
    control: Arc<Control>,
    gate: Arc<catalog_fault::NamespaceReadGate>,
}

impl Drop for Release {
    fn drop(&mut self) {
        self.control.proceed.notify_one();
        self.gate.release();
    }
}

async fn run(rooted: bool, store: Arc<catalog_fault::Store>) -> Result<()> {
    let control = Arc::new(Control::default());
    let parent = RaftClass::new("Parent", 1, [201; 32])?
        .initializer([202; 32], |_, tx| {
            tx.storage()?.put_json("value", &json!(0))
        })?
        .json_cell_rpc("Run", [203; 32])?;
    let initialize = control.clone();
    let child = RaftClass::new("Counter", 1, [204; 32])?
        .initializer([205; 32], move |_, tx| {
            initialize.initialized.fetch_add(1, Ordering::AcqRel);
            tx.storage()?.put_json("value", &json!(0))
        })?
        .json_cell_rpc("Child", [206; 32])?
        .query("Peek", [207; 32], |q, _| {
            Ok(serde_json::to_vec(&q.storage().get_json("value")?)?)
        })?;
    let parent_control = control.clone();
    let child_control = control.clone();
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
            .raft_cell(parent, move |context, _| {
                Ok(Arc::new(Bound {
                    storage: context.storage()?,
                    control: parent_control.clone(),
                }))
            })?
            .raft_cell(child, move |context, _| {
                Ok(Arc::new(Bound {
                    storage: context.storage()?,
                    control: child_control.clone(),
                }))
            })?,
    );
    let mut declared = configuration::options(SCRIPT);
    declared.do_classes = vec!["Parent".into(), "Counter".into()];
    declared.bindings = vec![("COUNTERS".into(), "Counter".into())];
    application.validate_deployment(&configuration::deployment(declared.clone()))?;
    let mut fixture = Fixture::with_definition_policies(
        rooted,
        vec![
            (
                application.resolve_raft_class("Parent")?,
                Durability::Quorum,
            ),
            (
                application.resolve_raft_class("Counter")?,
                Durability::Quorum,
            ),
        ],
        application,
        store.clone(),
    )
    .await?;
    fixture.configuration = Some(declared);
    fixture.sql_workers = 1;
    let mut cluster = Box::pin(ManagedCluster::start_with_applications(
        fixture,
        &[],
        ArchiveSetup::Initialized,
        false,
        None,
        FirstGroupSetup::Normal,
    ))
    .await?;
    let work = cluster.fixtures[0]
        .domain
        .run(async {
            named_service::grant(&cluster).await?;
            let child_group = named_service::activate_at(&cluster, "binding-admission", 0).await?;
            let source_node = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
            let target_node = (source_node + 1) % cluster.hosts.len();
            let leader = managed_fixture::leader(&cluster.hosts, child_group).await?;
            if leader != target_node {
                cluster.hosts[leader]
                    .group(child_group)?
                    .transfer_leader(MemberId::new(target_node as u64 + 1)?)
                    .await?;
            }
            cellar::asyncrt::timeout(TIMEOUT, async {
                loop {
                    if managed_fixture::leader(&cluster.hosts, child_group).await? == target_node {
                        return Ok::<_, anyhow::Error>(());
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(5)).await;
                }
            })
            .await??;
            let source_history = CellHistory {
                cluster: cluster.history.cluster,
                namespace: cluster.history.namespace,
                cell: CellId::new("Parent:admission-source")?,
                incarnation: IncarnationId::new([208; 16])?,
            };
            let namespace = cellar_native::cell_id::Namespace::new(
                "Counter",
                format!("cells:v1:{}:{SCRIPT}:Counter", SCRIPT.len()),
            );
            let target_history = CellHistory {
                cluster: child_group.cluster,
                namespace: child_group.namespace,
                cell: CellId::new(namespace.id_from_name("target").scope())?,
                incarnation: IncarnationId::new([209; 16])?,
            };
            cluster.hosts[target_node]
                .cell(target_history.clone())?
                .enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([210; 16])?,
                        durability: Durability::Quorum,
                    },
                    TIMEOUT,
                )
                .await?;
            cluster.hosts[source_node]
                .cell(source_history.clone())?
                .enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([211; 16])?,
                        durability: Durability::Quorum,
                    },
                    TIMEOUT,
                )
                .await?;
            let source = cluster.hosts[source_node].cell(source_history)?;
            let target = cluster.hosts[source_node].cell(target_history.clone())?;
            let gate =
                catalog_fault::NamespaceReadGate::new(hex::encode(child_group.namespace.bytes()));
            let _release = Release {
                control: control.clone(),
                gate: gate.clone(),
            };
            let retry = u64::try_from(cellar::asyncrt::wall_ms())? + 60_000;
            let options = CommandOptions::new(CommandId::new(ClientId::new([212; 16])?, 1)?, retry);
            let (reply, observed) = futures_util::future::join(
                source.command(&source.contract("Run")?, b"[]".to_vec(), options.clone()),
                async {
                    cellar::asyncrt::timeout(TIMEOUT, control.entered.notified()).await?;
                    // These real public requests acquire eight namespace permits,
                    // but are never polled, routed or executed. No limit is raised.
                    let held = (1..=8)
                        .map(|sequence| {
                            target.command(
                                &target.contract("Child").unwrap(),
                                b"[]".to_vec(),
                                CommandOptions::new(
                                    CommandId::new(ClientId::new([213; 16]).unwrap(), sequence)
                                        .unwrap(),
                                    retry,
                                ),
                            )
                        })
                        .collect::<Vec<_>>();
                    let refused = cluster.hosts[source_node]
                        .cell(CellHistory {
                            incarnation: IncarnationId::new([214; 16])?,
                            ..target_history.clone()
                        })?
                        .enroll(
                            CellEnrollmentRequest {
                                creation: ProposalId::new([215; 16])?,
                                durability: Durability::Quorum,
                            },
                            TIMEOUT,
                        )
                        .await
                        .err()
                        .context("namespace mutation bank was not full")?;
                    ensure!(
                        !refused.ambiguous
                            && matches!(
                                refused.cause.as_ref(),
                                CellDispatchError::Runtime(RuntimeError::Overloaded)
                            )
                    );
                    let before = cluster.hosts[source_node].resource_usage().control.items;
                    *store.binding_namespace_reads.lock().unwrap() = Some(gate.clone());
                    control.proceed.notify_one();
                    let discovered = cellar::asyncrt::timeout(Duration::from_secs(2), async {
                        loop {
                            if gate.entered.load(Ordering::Acquire) > 0
                                && cluster.hosts[source_node].resource_usage().control.items
                                    > before
                            {
                                return;
                            }
                            cellar::asyncrt::sleep(Duration::from_millis(1)).await;
                        }
                    })
                    .await
                    .context("refused binding did not attempt read-only name discovery");
                    drop(held);
                    gate.release();
                    discovered?;
                    Ok::<_, anyhow::Error>(())
                },
            )
            .await;
            observed?;
            let reply = reply?;
            ensure!(reply.id == options.id && reply.durability == Durability::Quorum);
            ensure!(reply.result == b"1", "parent reply: {:?}", reply.result);
            let replay = source
                .command(&source.contract("Run")?, b"[]".to_vec(), options)
                .await?;
            ensure!(replay.result == reply.result && replay.version == reply.version);
            let result = target
                .query(
                    ProposalId::new([216; 16])?,
                    &target.contract("Peek")?,
                    vec![],
                    QueryOptions::default(),
                )
                .await?;
            ensure!(
                result.result == b"1",
                "unpolled requests executed or child was repeated"
            );
            ensure!(
                control.initialized.load(Ordering::Acquire) == 1,
                "existing child was initialized again"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(std::convert::identity);
    control.proceed.notify_one();
    if let Some(gate) = store.binding_namespace_reads.lock().unwrap().take() {
        gate.release();
    }
    let joined = managed_fixture::close_group_nodes_with_groups(
        std::mem::take(&mut cluster.nodes),
        &cluster.hosts,
        2,
    )
    .await;
    if work.is_err() || joined.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!("retained binding-admission disk: {}", root.keep().display());
        }
    }
    cluster.fixtures[0].domain.shutdown().await;
    work?;
    joined
}

pub async fn qualify(real: bool) -> Result<()> {
    for rooted in [false, true] {
        let store = if real {
            Arc::new(catalog_fault::Store::around(rustfs::store(
                "binding-enrollment-admission",
                rooted,
            )?))
        } else {
            Arc::new(catalog_fault::Store::default())
        };
        Box::pin(run(rooted, store))
            .await
            .with_context(|| format!("binding enrollment extents={rooted}"))?;
    }
    Ok(())
}
