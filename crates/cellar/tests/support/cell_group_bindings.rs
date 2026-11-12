// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Ordinary declared bindings route through durable shared-group commands.

use super::*;
use cellar::sdk::clone::{Object, StructuredValue, Value};

const SCRIPT: &str = "named-binding-service";

struct BoundCounter(Storage);

fn graph() -> StructuredValue {
    StructuredValue {
        root: Value::Reference(0),
        objects: vec![Object::DenseArray {
            elements: vec![Value::Reference(0), Value::String(vec![0xd800])],
            properties: vec![],
        }],
    }
}

impl Cell for BoundCounter {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc { method, args, .. } = invocation else {
                anyhow::bail!("cell RPC required");
            };
            if method == "Echo" {
                return Ok(Output::Rpc(args));
            }
            let RpcData::Json(input) = args else {
                anyhow::bail!("JSON input required")
            };
            let input: serde_json::Value = serde_json::from_str(&input)?;
            let result = match method.as_str() {
                "Add" => json!({
                    "value": update(&self.0, input.as_i64().context("delta")?)?,
                    "name": context.identity().context("identity")?.name,
                }),
                "Hold" => {
                    update(&self.0, 1)?;
                    self.0.sync().await?;
                    // The durable target checkpoint survives the lost response.
                    // The original kernel deadline cancels this frame.
                    return std::future::pending().await;
                }
                "Abandon" => {
                    context
                        .namespace("COUNTERS")?
                        .get_by_name("held-target")
                        .rpc(&context, "Hold", RpcData::Json("{}".into()))
                        .await?;
                    anyhow::bail!("held target must never return a business response")
                }
                "Again" => {
                    let reply = context
                        .namespace("COUNTERS")?
                        .get_by_name("held-target")
                        .rpc(&context, "Add", RpcData::Json("2".into()))
                        .await?;
                    return Ok(Output::Rpc(reply));
                }
                "RecoveryProbe" => {
                    let mut clear = false;
                    let attempted: Result<()> = self.0.transaction_sync(|tx| {
                        tx.delete_all()?;
                        clear = true;
                        anyhow::bail!("probe rolls back all business and framework changes")
                    });
                    ensure!(attempted.is_err());
                    json!(clear)
                }
                "Relay" => {
                    update(&self.0, 1)?;
                    let namespace = context.namespace("COUNTERS")?;
                    let stub = namespace.get_by_name(input["name"].as_str().context("name")?);
                    let reply = stub.rpc(&context, "Add", RpcData::Json("2".into())).await?;
                    let RpcData::Json(reply) = reply else {
                        anyhow::bail!("JSON reply")
                    };
                    let reply: serde_json::Value = serde_json::from_str(&reply)?;
                    ensure!(context.namespace("MISSING").is_err());
                    // A cyclic argument array and an unpaired UTF-16 surrogate
                    // cannot pass through JSON without losing their meaning.
                    ensure!(stub.call(&context, "Echo", &graph()).await? == graph());
                    json!({"source": self.0.get_json("value")?, "target": reply})
                }
                "Recursive" => {
                    let before = self.0.get_json("value")?.context("value")?;
                    let mut cursor = self.0.sql_cursor(
                        "SELECT value FROM counter UNION ALL SELECT value FROM counter",
                        &[],
                    )?;
                    let reply = context
                        .namespace("COUNTERS")?
                        .get_by_name("source")
                        .rpc(&context, "Add", RpcData::Json("2".into()))
                        .await?;
                    let RpcData::Json(reply) = reply else {
                        anyhow::bail!("JSON reply")
                    };
                    let reply: serde_json::Value = serde_json::from_str(&reply)?;
                    let old = vec![cellar::sdk::storage::SqlValue::Integer(
                        before.as_i64().context("integer")?,
                    )];
                    ensure!(cursor.next_raw()? == Some(old.clone()));
                    ensure!(cursor.next_raw()? == Some(old));
                    ensure!(cursor.next_raw()?.is_none());
                    ensure!(self.0.get_json("value")? == Some(reply["value"].clone()));
                    reply
                }
                "Cancel" => {
                    let controller = cellar::native::web::AbortController::new();
                    let call = context
                        .namespace("COUNTERS")?
                        .get_by_name("cancelled-target")
                        .rpc_with_signal(
                            &context,
                            "Add",
                            RpcData::Json("2".into()),
                            controller.signal(),
                        );
                    controller.abort(None)?;
                    let error = call
                        .await
                        .err()
                        .context("cancelled call preserves admitted identity")?;
                    let error = error
                        .downcast_ref::<cellar::sdk::cells::CallError>()
                        .context("typed admitted call error")?;
                    ensure!(error.ambiguous && error.command.sequence() > 0);
                    json!(true)
                }
                "Atomic" => {
                    let inside = context.clone();
                    let result: Result<()> = self
                        .0
                        .transaction(&context, move |tx| async move {
                            update(&tx, 1000)?;
                            let call = inside
                                .namespace("COUNTERS")?
                                .get_by_name("must-not-exist")
                                .rpc(&inside, "Add", RpcData::Json("2".into()));
                            ensure!(call.await.is_err());
                            anyhow::bail!("rollback refused atomic dispatch")
                        })
                        .await;
                    ensure!(result.is_err());
                    json!(self.0.get_json("value")?)
                }
                _ => anyhow::bail!("unknown method {method}"),
            };
            Ok(Output::Rpc(RpcData::Json(result.to_string())))
        })
    }
}

pub(super) fn identity(name: &str) -> Result<CellId> {
    let namespace = cellar_native::cell_id::Namespace::new(
        "Counter",
        format!("cells:v1:{}:{SCRIPT}:Counter", SCRIPT.len()),
    );
    Ok(CellId::new(namespace.id_from_name(name).scope())?)
}

pub(super) fn options(sequence: u64, durability: Durability) -> Result<CommandOptions> {
    Ok(CommandOptions {
        durability,
        ..CommandOptions::new(
            CommandId::new(ClientId::new([121; 16])?, sequence)?,
            u64::try_from(cellar::asyncrt::wall_ms())? + 60_000,
        )
    })
}

/// Exercise every configuration even when an earlier one fails. Any failure
/// still fails the combined case with its original error and deadline.
pub async fn qualify(real_rustfs: bool) -> Result<()> {
    qualify_case(real_rustfs, false).await
}

pub async fn qualify_recovery(real_rustfs: bool) -> Result<()> {
    qualify_case(real_rustfs, true).await
}

async fn qualify_case(real_rustfs: bool, recovery: bool) -> Result<()> {
    let mut failures = Vec::new();
    for rooted in [false, true] {
        for durability in [Durability::Quorum, Durability::S3] {
            let result = async {
                let store = if real_rustfs {
                    Arc::new(catalog_fault::Store::around(rustfs::store(
                        if recovery && durability == Durability::S3 {
                            "binding-recovery-s3"
                        } else if recovery {
                            "binding-recovery-quorum"
                        } else if durability == Durability::S3 {
                            "named-bindings-s3"
                        } else {
                            "named-bindings-quorum"
                        },
                        rooted,
                    )?))
                } else {
                    Arc::new(catalog_fault::Store::default())
                };
                if recovery {
                    Box::pin(super::binding_recovery::run(rooted, durability, store)).await
                } else {
                    Box::pin(run(rooted, durability, store)).await
                }
            }
            .await;
            match result {
                Ok(()) => eprintln!(
                    "named binding configuration recovery={recovery} extents={rooted} source={durability:?}: PASS"
                ),
                Err(error) => {
                    let error = error.context(format!(
                        "named binding configuration recovery={recovery} RustFS={real_rustfs} extents={rooted} source={durability:?}"
                    ));
                    eprintln!("named binding configuration recovery={recovery} extents={rooted} source={durability:?}: FAILED: {error:?}");
                    failures.push(error);
                }
            }
        }
    }
    let count = failures.len();
    match failures.into_iter().next() {
        Some(error) => Err(error.context(format!(
            "{count} of 4 named binding configurations failed; all outcomes reported"
        ))),
        None => Ok(()),
    }
}

pub(super) async fn start(
    rooted: bool,
    store: Arc<catalog_fault::Store>,
) -> Result<ManagedCluster> {
    let class = RaftClass::new("Counter", 1, [111; 32])?
        .initializer([112; 32], |_, tx| {
            tx.execute_batch(
                "CREATE TABLE counter(value INTEGER NOT NULL); INSERT INTO counter VALUES(0);",
            )?;
            tx.storage()?.put_json("value", &json!(0))
        })?
        .json_cell_rpc("Add", [113; 32])?
        .json_cell_rpc("Relay", [114; 32])?
        .json_cell_rpc("Recursive", [115; 32])?
        .json_cell_rpc("Atomic", [116; 32])?
        .cell_rpc("Echo", [117; 32])?
        .json_cell_rpc("Cancel", [123; 32])?
        .json_cell_rpc("Hold", [125; 32])?
        .json_cell_rpc("Abandon", [126; 32])?
        .json_cell_rpc("Again", [127; 32])?
        .json_cell_rpc("RecoveryProbe", [128; 32])?
        .query("Get", [118; 32], |context, _| {
            Ok(serde_json::to_vec(&context.storage().get_json("value")?)?)
        })?;
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
            .raft_cell(class, |context, _| {
                Ok(Arc::new(BoundCounter(context.storage()?)))
            })?,
    );
    let mut declared = configuration::options(SCRIPT);
    declared.do_classes = vec!["Counter".into()];
    declared.bindings = vec![("COUNTERS".into(), "Counter".into())];
    application.validate_deployment(&configuration::deployment(declared.clone()))?;
    let mut fixture = Fixture::with_application(rooted, "Counter", application, store).await?;
    fixture.configuration = Some(declared);
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

pub async fn run(
    rooted: bool,
    durability: Durability,
    store: Arc<catalog_fault::Store>,
) -> Result<()> {
    let mut cluster = Box::pin(start(rooted, store)).await?;
    let domain = cluster.fixtures[0].domain.clone();
    let work = domain
        .run(async {
            let group = cluster.history;
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            let owner = &cluster.hosts[leader];
            let source = owner.cell(CellHistory {
                cluster: group.cluster,
                namespace: group.namespace,
                cell: identity("source")?,
                incarnation: IncarnationId::new([119; 16])?,
            })?;
            source
                .enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([120; 16])?,
                        durability,
                    },
                    TIMEOUT,
                )
                .await?;
            let relay = source.contract("Relay")?;
            let input = json!({"name":"target"}).to_string().into_bytes();
            let first_options = options(1, durability)?;
            let first = source
                .command(&relay, input.clone(), first_options.clone())
                .await?;
            ensure!(
                serde_json::from_slice::<serde_json::Value>(&first.result)?
                    == json!({"source":1,"target":{"value":2,"name":"target"}})
            );
            let replay = source
                .command(&relay, input.clone(), first_options.clone())
                .await?;
            ensure!(replay.result == first.result);
            for sequence in 2..=3 {
                let receipt = source
                    .command(&relay, input.clone(), options(sequence, durability)?)
                    .await?;
                ensure!(
                    serde_json::from_slice::<serde_json::Value>(&receipt.result)?
                        == json!({"source":sequence,"target":{"value":sequence*2,"name":"target"}})
                );
            }
            let target = owner
                .resolve_cell_history(
                    group.cluster,
                    group.namespace,
                    identity("target")?,
                    QueryOptions::default(),
                )
                .await?
                .context("auto-enrolled target")?;
            let target = owner.cell(target)?;
            let value = target
                .query(
                    ProposalId::new([122; 16])?,
                    &target.contract("Get")?,
                    vec![],
                    QueryOptions::default(),
                )
                .await?;
            ensure!(value.result == b"6");
            let recursive = source
                .command(
                    &source.contract("Recursive")?,
                    b"{}".to_vec(),
                    options(4, durability)?,
                )
                .await?;
            ensure!(
                serde_json::from_slice::<serde_json::Value>(&recursive.result)?
                    == json!({"value":5,"name":"source"})
            );
            let atomic = source
                .command(
                    &source.contract("Atomic")?,
                    b"{}".to_vec(),
                    options(5, durability)?,
                )
                .await?;
            ensure!(atomic.result == b"5");
            ensure!(owner
                .resolve_cell_history(
                    group.cluster,
                    group.namespace,
                    identity("must-not-exist")?,
                    QueryOptions::default()
                )
                .await?
                .is_none());
            let cancelled = source
                .command(
                    &source.contract("Cancel")?,
                    b"{}".to_vec(),
                    options(6, durability)?,
                )
                .await?;
            ensure!(cancelled.result == b"true");
            let cancelled_target = owner
                .resolve_cell_history(
                    group.cluster,
                    group.namespace,
                    identity("cancelled-target")?,
                    QueryOptions::default(),
                )
                .await?
                .context("cancelled response still owns admitted work")?;
            let cancelled_target = owner.cell(cancelled_target)?;
            let cancelled_value = cancelled_target
                .query(
                    ProposalId::new([124; 16])?,
                    &cancelled_target.contract("Get")?,
                    vec![],
                    QueryOptions::default(),
                )
                .await?;
            ensure!(cancelled_value.result == b"2");
            let source_history = source.history().clone();
            Box::pin(restart(&mut cluster)).await?;
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            let current = cluster.hosts[leader].cell(source_history)?;
            let replay = current
                .command(&current.contract("Relay")?, input.clone(), first_options)
                .await?;
            ensure!(replay.result == first.result);
            // Lane counters and target business names survive all voter owners.
            let receipt = current
                .command(&current.contract("Relay")?, input, options(7, durability)?)
                .await?;
            ensure!(
                serde_json::from_slice::<serde_json::Value>(&receipt.result)?
                    == json!({"source":6,"target":{"value":8,"name":"target"}})
            );
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(std::convert::identity);
    let joined =
        managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
            .await;
    if work.is_err() || joined.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!("retained named binding disk: {}", root.keep().display());
        }
    }
    cluster.fixtures[0].domain.shutdown().await;
    work?;
    joined
}

pub(super) async fn restart(cluster: &mut ManagedCluster) -> Result<()> {
    let old_hosts = std::mem::take(&mut cluster.hosts);
    managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &old_hosts).await?;
    cluster.peers.clear();
    for (index, fixture) in cluster.fixtures.iter().enumerate() {
        let node = fixture.start_node(cluster.roots[index].path()).await?;
        let started = async {
            let (mut config, services) = fixture
                .config(&node, LogOpenMode::Resume, 104 + index as u8)
                .await?;
            managed_fixture::archives::configure(&mut config);
            let peer = config.peers[0].clone();
            node.start_raft_cells(config, services)
                .await
                .map(|host| (host, peer))
        }
        .await;
        let (host, peer) = match started {
            Ok(started) => started,
            Err(error) => {
                node.shutdown(cellar::sdk::ShutdownMode::Preserve).await?;
                return Err(error).context("retained named binding node");
            }
        };
        cluster.nodes.push(node);
        cluster.hosts.push(host);
        cluster.peers.push(peer);
    }
    Ok(())
}
