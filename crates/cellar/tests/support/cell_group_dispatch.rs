// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Dependent SDK dispatch waits for its source's exact durability proof.

use super::*;

struct Dependent {
    storage: Storage,
    target: Arc<Mutex<Option<RaftCellHandle>>>,
    gate: Arc<RpcGate>,
    calls: Arc<AtomicUsize>,
}

impl Cell for Dependent {
    fn invoke(self: Arc<Self>, _context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc {
                args: RpcData::Json(input),
                ..
            } = invocation
            else {
                anyhow::bail!("expected JSON RPC");
            };
            let input: serde_json::Value = serde_json::from_str(&input)?;
            let result = if input["target"] == true {
                self.calls.fetch_add(1, Ordering::AcqRel);
                let result = update(&self.storage, 2)?;
                self.gate.entered.notify_one();
                let _waiting = Waiting(&self.gate.dropped);
                self.gate.release.notified().await;
                json!(result)
            } else {
                update(&self.storage, 1)?;
                // The caller persists the original target request identity and
                // input before dispatch. The framework must prove these writes.
                self.storage.put_json("pending-target", &input)?;
                let target = self
                    .target
                    .lock()
                    .unwrap()
                    .clone()
                    .context("target handle")?;
                let receipt = target
                    .command(
                        &target.contract("Run")?,
                        br#"{"target":true}"#.to_vec(),
                        CommandOptions::new(
                            CommandId::new(ClientId::new([221; 16])?, 1)?,
                            input["retry_until_ms"].as_u64().context("retry horizon")?,
                        ),
                    )
                    .await?;
                json!({"source": update(&self.storage, 10)?, "target": serde_json::from_slice::<i64>(&receipt.result)?})
            };
            Ok(Output::Rpc(RpcData::Json(result.to_string())))
        })
    }
}

fn command(client: u8, durability: Durability) -> Result<CommandOptions> {
    Ok(CommandOptions {
        durability,
        ..CommandOptions::new(
            CommandId::new(ClientId::new([client; 16])?, 1)?,
            u64::try_from(cellar::asyncrt::wall_ms())? + 60_000,
        )
    })
}

async fn value(cell: &RaftCellHandle, invocation: u8) -> Result<i64> {
    let result = cell
        .query(
            ProposalId::new([invocation; 16])?,
            &cell.contract("Get")?,
            Vec::new(),
            QueryOptions::default(),
        )
        .await?;
    Ok(serde_json::from_slice(&result.result)?)
}

pub async fn run(
    rooted: bool,
    durability: Durability,
    store: Arc<catalog_fault::Store>,
) -> Result<()> {
    let target = Arc::new(Mutex::new(None::<RaftCellHandle>));
    let gate = Arc::new(RpcGate::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let factory_target = target.clone();
    let factory_gate = gate.clone();
    let factory_calls = calls.clone();
    let class = RaftClass::new("Counter", 1, [171; 32])?
        .initializer([172; 32], |_, tx| {
            tx.execute_batch(
                "CREATE TABLE counter(value INTEGER NOT NULL); INSERT INTO counter VALUES(0);",
            )?;
            tx.storage()?.put_json("value", &json!(0))
        })?
        .json_cell_rpc("Run", [173; 32])?
        .query("Get", [174; 32], |context, _| {
            let storage = context.storage();
            let (_, rows, _) = storage.sql("SELECT value FROM counter", &[])?;
            ensure!(storage.get_json("value")? == Some(rows[0][0].clone()));
            Ok(serde_json::to_vec(&rows[0][0])?)
        })?;
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |context, _| {
                Ok(Arc::new(Dependent {
                    storage: context.storage()?,
                    target: factory_target.clone(),
                    gate: factory_gate.clone(),
                    calls: factory_calls.clone(),
                }))
            },
        )?,
    );
    let declared = configuration::options("dependent-service");
    application.validate_deployment(&configuration::deployment(declared.clone()))?;
    let mut fixture = Fixture::with_application(rooted, "Counter", application, store).await?;
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
            let group = cluster.history;
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            let mut cells = Vec::new();
            for (name, id) in [("source", 175), ("target", 176)] {
                let cell = cluster.hosts[leader].cell(CellHistory {
                    cluster: group.cluster,
                    namespace: group.namespace,
                    cell: CellId::new(format!("Counter:dispatch-{name}"))?,
                    incarnation: IncarnationId::new([id; 16])?,
                })?;
                cell.enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([id; 16])?,
                        durability,
                    },
                    TIMEOUT,
                )
                .await?;
                cells.push(cell);
            }
            *target.lock().unwrap() = Some(cells[1].clone());
            let mut options = command(177, durability)?;
            options.timeout = Duration::from_secs(20);
            let input = json!({"retry_until_ms": options.retry_until_ms})
                .to_string()
                .into_bytes();
            let (result, observed) = tokio::join!(
                cells[0].command(&cells[0].contract("Run")?, input.clone(), options.clone()),
                async {
                    cellar::asyncrt::timeout(TIMEOUT, gate.entered.notified()).await?;
                    let source = value(&cells[0], 178).await?;
                    let target = value(&cells[1], 179).await?;
                    gate.release.notify_one();
                    ensure!(
                        source == 1,
                        "target executed before source proof: observed {source}"
                    );
                    ensure!(target == 0, "target private writes escaped");
                    Ok::<_, anyhow::Error>(())
                }
            );
            observed?;
            let result = result?;
            ensure!(
                serde_json::from_slice::<serde_json::Value>(&result.result)?
                    == json!({"source":11,"target":2})
            );
            ensure!(value(&cells[0], 180).await? == 11 && value(&cells[1], 181).await? == 2);
            ensure!(
                cells[0]
                    .command(&cells[0].contract("Run")?, input, options)
                    .await?
                    .result
                    == result.result
            );
            ensure!(calls.load(Ordering::Acquire) == 1);
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
            eprintln!("retained dispatch disk: {}", root.keep().display());
        }
    }
    target.lock().unwrap().take();
    cluster.fixtures[0].domain.shutdown().await;
    work?;
    joined
}
