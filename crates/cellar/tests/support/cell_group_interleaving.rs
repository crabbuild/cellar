// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Ordinary same-cell RPCs share an activation between proven transaction roots.

use super::*;

struct Interleaved {
    storage: Storage,
    gates: Arc<[RpcGate; 3]>,
    calls: Arc<AtomicUsize>,
}

impl Cell for Interleaved {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::AcqRel);
            ensure!(context.var("OWNER") == Some("shared"));
            let Invocation::CellRpc {
                args: RpcData::Json(input),
                ..
            } = invocation
            else {
                anyhow::bail!("expected JSON RPC");
            };
            let input: serde_json::Value = serde_json::from_str(&input)?;
            let kind = input["kind"].as_str().context("RPC kind")?;
            let value = match kind {
                "wait" => {
                    self.storage
                        .transaction(&context, |tx| async move { update(&tx, 1) })
                        .await?;
                    let gate = usize::try_from(input["gate"].as_u64().context("gate")?)?;
                    self.gates[gate].entered.notify_one();
                    let _waiting = Waiting(&self.gates[gate].dropped);
                    self.gates[gate].release.notified().await;
                    // The original continuation must see another invocation's
                    // committed image, through its original constructor handle.
                    let before = self.storage.get_json("value")?.context("value")?;
                    let after = self
                        .storage
                        .transaction(&context, |tx| async move { update(&tx, 10) })
                        .await?;
                    return Ok(Output::Rpc(RpcData::Json(
                        json!([before, after]).to_string(),
                    )));
                }
                "private" | "schema" => {
                    self.storage.sync().await?;
                    if kind == "schema" {
                        self.storage
                            .sql("CREATE TABLE private_schema(value INTEGER)", &[])?;
                    } else {
                        update(&self.storage, 1000)?;
                    }
                    self.gates[1].entered.notify_one();
                    let _waiting = Waiting(&self.gates[1].dropped);
                    self.gates[1].release.notified().await;
                    anyhow::bail!("roll back the private continuation")
                }
                "atomic" => {
                    self.storage.sync().await?;
                    let gate = self.gates.clone();
                    self.storage
                        .transaction(&context, move |tx| async move {
                            update(&tx, 1000)?;
                            gate[1].entered.notify_one();
                            let _waiting = Waiting(&gate[1].dropped);
                            gate[1].release.notified().await;
                            anyhow::bail!("roll back the held atomic transaction")
                        })
                        .await?
                }
                "write" => update(&self.storage, 2)?,
                _ => anyhow::bail!("unknown RPC kind"),
            };
            Ok(Output::Rpc(RpcData::Json(json!(value).to_string())))
        })
    }
}

fn command(client: u8, sequence: u64, durability: Durability) -> Result<CommandOptions> {
    let now = u64::try_from(cellar::asyncrt::wall_ms())?;
    Ok(CommandOptions {
        durability,
        ..CommandOptions::new(
            CommandId::new(ClientId::new([client; 16])?, sequence)?,
            now + 60_000,
        )
    })
}

fn input(kind: &str, gate: usize) -> Vec<u8> {
    json!({"kind": kind, "gate": gate}).to_string().into_bytes()
}

async fn value(cell: &RaftCellHandle, proposal: u8) -> Result<i64> {
    let result = cell
        .query(
            ProposalId::new([proposal; 16])?,
            &cell.contract("Get")?,
            Vec::new(),
            QueryOptions::default(),
        )
        .await?;
    serde_json::from_slice(&result.result).map_err(Into::into)
}

pub async fn run(
    rooted: bool,
    durability: Durability,
    store: Arc<catalog_fault::Store>,
) -> Result<()> {
    let constructors = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let gates: Arc<[RpcGate; 3]> = Arc::new(std::array::from_fn(|_| RpcGate::default()));
    let counted = constructors.clone();
    let called = calls.clone();
    let waiting = gates.clone();
    let class = RaftClass::new("Counter", 1, [181; 32])?
        .initializer([182; 32], |_, tx| {
            tx.execute_batch(
                "CREATE TABLE counter(value INTEGER NOT NULL); INSERT INTO counter VALUES(0);",
            )?;
            tx.storage()?.put_json("value", &json!(0))
        })?
        .json_cell_rpc("Run", [183; 32])?
        .query("Get", [184; 32], |context, _| {
            let storage = context.storage();
            let (_, rows, _) = storage.sql("SELECT value FROM counter", &[])?;
            ensure!(storage.get_json("value")? == Some(rows[0][0].clone()));
            Ok(serde_json::to_vec(&rows[0][0])?)
        })?;
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |context, _| {
                counted.fetch_add(1, Ordering::AcqRel);
                Ok(Arc::new(Interleaved {
                    storage: context.storage()?,
                    gates: waiting.clone(),
                    calls: called.clone(),
                }))
            },
        )?,
    );
    let mut declared = configuration::options("interleaved-service");
    declared.vars = vec![("OWNER".into(), "shared".into())];
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
            let cell = cluster.hosts[leader].cell(CellHistory {
                cluster: group.cluster,
                namespace: group.namespace,
                cell: CellId::new("Counter:interleaved")?,
                incarnation: IncarnationId::new([185; 16])?,
            })?;
            cell.enroll(
                CellEnrollmentRequest {
                    creation: ProposalId::new([186; 16])?,
                    durability,
                },
                TIMEOUT,
            )
            .await?;
            let run = cell.contract("Run")?;
            let mut first = command(187, 1, durability)?;
            first.timeout = Duration::from_secs(20);
            let first_input = input("wait", 0);
            let second = command(187, 2, durability)?;
            let (first_result, second_result) = tokio::join!(
                cell.command(&run, first_input.clone(), first.clone()),
                async {
                    cellar::asyncrt::timeout(TIMEOUT, gates[0].entered.notified()).await?;
                    ensure!(value(&cell, 188).await? == 1);
                    let duplicate = cell
                        .command(
                            &run,
                            first_input.clone(),
                            CommandOptions {
                                timeout: Duration::from_secs(1),
                                ..first.clone()
                            },
                        )
                        .await;
                    ensure!(duplicate.is_err() && calls.load(Ordering::Acquire) == 1);
                    let written = cell
                        .command(&run, input("write", 0), second.clone())
                        .await?;
                    ensure!(written.result == b"3" && value(&cell, 189).await? == 3);
                    ensure!(
                        calls.load(Ordering::Acquire) == 2
                            && constructors.load(Ordering::Acquire) == 1
                    );
                    let usage = cluster.hosts[leader]
                        .resource_usage()
                        .sql
                        .context("shared activation")?;
                    ensure!(usage.retained_activations == 1 && usage.idle_activations == 0);
                    gates[0].release.notify_one();
                    Ok::<_, anyhow::Error>(written)
                }
            );
            let second_result = second_result?;
            let first_result = first_result?;
            ensure!(first_result.result == b"[3,13]" && value(&cell, 190).await? == 13);
            ensure!(
                first_result.durability == durability && second_result.durability == durability
            );
            ensure!(constructors.load(Ordering::Acquire) == 1);
            ensure!(cell.command(&run, first_input, first).await?.result == first_result.result);
            ensure!(
                cell.command(&run, input("write", 0), second).await?.result == second_result.result
            );
            ensure!(calls.load(Ordering::Acquire) == 2);

            // Open async transactions, private DML and private schema changes must
            // all preserve their writer after a prior independently proven root.
            let mut baseline = 13;
            for (index, kind) in ["atomic", "private", "schema"].into_iter().enumerate() {
                let client = 191 + index as u8 * 2;
                let mut atomic = command(client, 1, durability)?;
                atomic.timeout = Duration::from_secs(20);
                let blocked = command(client + 1, 1, durability)?;
                let before = calls.load(Ordering::Acquire);
                let (atomic_result, observed) =
                    tokio::join!(cell.command(&run, input(kind, 1), atomic), async {
                        cellar::asyncrt::timeout(TIMEOUT, gates[1].entered.notified()).await?;
                        ensure!(value(&cell, 201).await? == baseline);
                        let refused = cell
                            .command(
                                &run,
                                input("write", 0),
                                CommandOptions {
                                    timeout: Duration::from_secs(1),
                                    ..blocked.clone()
                                },
                            )
                            .await;
                        ensure!(refused.is_err() && calls.load(Ordering::Acquire) == before + 1);
                        ensure!(value(&cell, 202).await? == baseline);
                        gates[1].release.notify_one();
                        Ok::<_, anyhow::Error>(())
                    });
                observed?;
                ensure!(atomic_result.is_err() && value(&cell, 203).await? == baseline);
                baseline += 2;
                let result = cell.command(&run, input("write", 0), blocked).await?;
                ensure!(serde_json::from_slice::<i64>(&result.result)? == baseline);
                ensure!(constructors.load(Ordering::Acquire) == index + 2);
            }

            // Fencing a parked continuation preserves its first root and original
            // uncertain identity, without authorizing the second callback to run.
            let mut interrupted = command(204, 1, durability)?;
            interrupted.timeout = Duration::from_secs(20);
            let held = input("wait", 2);
            let (interrupted_result, moved) = tokio::join!(
                cell.command(&run, held.clone(), interrupted.clone()),
                async {
                    cellar::asyncrt::timeout(TIMEOUT, gates[2].entered.notified()).await?;
                    ensure!(value(&cell, 197).await? == baseline + 1);
                    let target = (leader + 1) % 3;
                    let handle = cluster.hosts[leader].group(group)?;
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
                    cellar::asyncrt::timeout(TIMEOUT, async {
                        loop {
                            let status = cluster.hosts[target].group(group)?.status().await?;
                            if status.ready && status.leader == Some(target as u64 + 1) {
                                break Ok::<_, anyhow::Error>(());
                            }
                            cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                        }
                    })
                    .await??;
                    gates[2].release.notify_one();
                    Ok::<_, anyhow::Error>(target)
                }
            );
            let target = moved?;
            ensure!(interrupted_result.is_err());
            let recovered = cluster.hosts[target].cell(cell.history().clone())?;
            ensure!(value(&recovered, 198).await? == baseline + 1);
            let invoked = calls.load(Ordering::Acquire);
            ensure!(recovered.command(&run, held, interrupted).await.is_err());
            ensure!(calls.load(Ordering::Acquire) == invoked);
            ensure!(
                recovered
                    .command(&run, input("write", 0), command(204, 2, durability)?)
                    .await?
                    .result
                    == json!(baseline + 3).to_string().as_bytes()
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
            eprintln!("retained interleaving disk: {}", root.keep().display());
        }
    }
    cluster.fixtures[0].domain.shutdown().await;
    work?;
    joined
}
