// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Dependent SDK dispatch waits for its source's exact durability proof.

use super::*;

type ChildCall = futures_util::future::BoxFuture<
    'static,
    std::result::Result<CommandReceipt, Arc<CellCommandError>>,
>;

struct Dependent {
    storage: Storage,
    target: Arc<Mutex<Option<RaftCellHandle>>>,
    gates: Arc<[RpcGate; 6]>,
    escaped: Arc<Mutex<Option<ChildCall>>>,
    calls: Arc<AtomicUsize>,
}

impl Cell for Dependent {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
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
                if input["hold"] != false {
                    let gate = input["gate"].as_u64().unwrap_or(0) as usize;
                    if gate == 4 {
                        // Leave a durable unfinished target receipt before
                        // losing the source leader. Its callback cannot replay.
                        self.storage.sync().await?;
                    }
                    let gate = self.gates.get(gate).context("target gate")?;
                    gate.entered.notify_one();
                    let _waiting = Waiting(&gate.dropped);
                    gate.release.notified().await;
                }
                json!(result)
            } else {
                let target = self
                    .target
                    .lock()
                    .unwrap()
                    .clone()
                    .context("target handle")?;
                let horizon = input["retry_until_ms"].as_u64().context("retry horizon")?;
                let kind = input["kind"].as_str().unwrap_or("nested");
                if kind == "busy" {
                    self.calls.fetch_add(1, Ordering::AcqRel);
                    update(&self.storage, 1000)?;
                    anyhow::bail!(ExecutionError::Busy)
                }
                if kind == "oversized" {
                    update(&self.storage, 1000)?;
                    let error = target
                        .command(
                            &target.contract("Run")?,
                            vec![b' '; 1024 * 1024],
                            CommandOptions::new(
                                CommandId::new(ClientId::new([231; 16])?, 1)?,
                                horizon,
                            ),
                        )
                        .now_or_never()
                        .context("oversized input must refuse before source proof")?
                        .expect_err("target input bound");
                    ensure!(
                        !error.ambiguous
                            && matches!(
                                error.cause.as_ref(),
                                CellDispatchError::Runtime(RuntimeError::Overloaded)
                            )
                    );
                    anyhow::bail!("roll back oversized child admission")
                }
                if kind == "atomic" {
                    return self
                        .storage
                        .transaction(&context, move |tx| async move {
                            update(&tx, 1000)?;
                            refused(&target, horizon, 223)?;
                            anyhow::bail!("roll back the refused atomic dispatch")
                        })
                        .await;
                }
                let client = match kind {
                    "drop" => 222,
                    "deadline" => 224,
                    "fence" => 225,
                    "recursive" => 226,
                    _ => 221,
                };
                let id = CommandId::new(ClientId::new([client; 16])?, 1)?;
                let mut options = CommandOptions::new(id, horizon);
                if input["target_s3"] == true {
                    options.durability = Durability::S3;
                }
                let target_durability = options.durability;
                let gate = match kind {
                    "drop" => 2,
                    "fence" => 4,
                    _ => 0,
                };
                let child_input = json!({"target":true,"gate":gate,"hold":kind != "recursive"})
                    .to_string()
                    .into_bytes();
                if kind == "deadline" {
                    let gates = self.gates.clone();
                    drop(self.storage.transaction(&context, move |tx| async move {
                        update(&tx, 1)?;
                        gates[3].entered.notify_one();
                        let _waiting = Waiting(&gates[3].dropped);
                        gates[3].release.notified().await;
                        Ok(())
                    }));
                    options.timeout = Duration::from_secs(1);
                } else {
                    update(&self.storage, 1)?;
                }
                // The caller owns this stable target identity, input and policy.
                // The source barrier must prove it before the target can run.
                self.storage.put_json(
                    "pending-target",
                    &json!({
                        "history": {"cluster":target.history().cluster.bytes(),
                            "namespace":target.history().namespace.bytes(),
                            "cell":target.history().cell.as_str(),
                            "incarnation":target.history().incarnation.bytes()},
                        "client":client, "sequence":1,
                        "input":serde_json::from_slice::<serde_json::Value>(&child_input)?,
                        "retry_until_ms":horizon,"durability":format!("{target_durability:?}")
                    }),
                )?;
                let receipt_input = child_input.clone();
                let call = target.command(&target.contract("Run")?, child_input, options);
                match kind {
                    "drop" => {
                        drop(call);
                        json!(self.storage.get_json("value")?.context("source value")?)
                    }
                    "deadline" => {
                        let error = call.await.expect_err("queued target deadline must expire");
                        ensure!(
                            !error.ambiguous
                                && matches!(
                                    error.cause.as_ref(),
                                    CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)
                                )
                        );
                        json!({"expired":true})
                    }
                    "fence" => {
                        *self.escaped.lock().unwrap() = Some(Box::pin(call));
                        let _waiting = Waiting(&self.gates[5].dropped);
                        self.gates[5].release.notified().await;
                        anyhow::bail!("fenced handler unexpectedly resumed")
                    }
                    "recursive" => {
                        let receipt = call.await?;
                        let result: i64 = serde_json::from_slice(&receipt.result)?;
                        ensure!(self.storage.get_json("value")? == Some(json!(result)));
                        json!({"recursive":result})
                    }
                    _ => {
                        let receipt = call.await?;
                        ensure!(receipt.durability == target_durability);
                        let source = update(&self.storage, 10)?;
                        let read = target
                            .query(
                                ProposalId::new([230; 16])?,
                                &target.contract("Get")?,
                                Vec::new(),
                                QueryOptions::default(),
                            )
                            .await?;
                        ensure!(read.result == receipt.result);
                        self.gates[1].entered.notify_one();
                        let _waiting = Waiting(&self.gates[1].dropped);
                        self.gates[1].release.notified().await;
                        let inspected = target
                            .receipt(
                                CellReceiptRequest {
                                    id,
                                    contract: target.contract("Run")?,
                                    input: receipt_input,
                                    durability: target_durability,
                                },
                                TIMEOUT,
                            )
                            .await?;
                        ensure!(
                            inspected.receipt().context("target receipt")?.result()
                                == receipt.result.as_slice()
                        );
                        let enrolled = target
                            .enroll(
                                CellEnrollmentRequest {
                                    creation: ProposalId::new([176; 16])?,
                                    durability: Durability::Quorum,
                                },
                                TIMEOUT,
                            )
                            .await?;
                        ensure!(enrolled.record().cell() == target.history());
                        json!({"source":source,"target":serde_json::from_slice::<i64>(&receipt.result)?})
                    }
                }
            };
            Ok(Output::Rpc(RpcData::Json(result.to_string())))
        })
    }
}

fn refused(target: &RaftCellHandle, horizon: u64, client: u8) -> Result<Vec<u8>> {
    let error = target
        .command(
            &target.contract("Run")?,
            br#"{"target":true,"hold":false}"#.to_vec(),
            CommandOptions::new(CommandId::new(ClientId::new([client; 16])?, 1)?, horizon),
        )
        .now_or_never()
        .context("callback dispatch must refuse synchronously")?
        .expect_err("synchronous callback cannot dispatch");
    ensure!(
        !error.ambiguous && error.cause.refusal() == Some(&CommandFailure::UnsupportedCapability)
    );
    Ok(Vec::new())
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
    let gates: Arc<[RpcGate; 6]> = Arc::new(std::array::from_fn(|_| RpcGate::default()));
    let escaped = Arc::new(Mutex::new(None::<ChildCall>));
    let horizon = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let factory_target = target.clone();
    let factory_gates = gates.clone();
    let factory_escaped = escaped.clone();
    let command_target = target.clone();
    let query_target = target.clone();
    let command_horizon = horizon.clone();
    let query_horizon = horizon.clone();
    let factory_calls = calls.clone();
    let class = RaftClass::new("Counter", 1, [171; 32])?
        .initializer([172; 32], |_, tx| {
            tx.execute_batch(
                "CREATE TABLE counter(value INTEGER NOT NULL); INSERT INTO counter VALUES(0);",
            )?;
            tx.storage()?.put_json("value", &json!(0))
        })?
        .json_cell_rpc("Run", [173; 32])?
        .command("Probe", [168; 32], move |_, _| {
            refused(
                &command_target
                    .lock()
                    .unwrap()
                    .clone()
                    .context("probe target")?,
                command_horizon.load(Ordering::Acquire),
                228,
            )
        })?
        .query("ProbeRead", [169; 32], move |_, _| {
            refused(
                &query_target
                    .lock()
                    .unwrap()
                    .clone()
                    .context("probe target")?,
                query_horizon.load(Ordering::Acquire),
                229,
            )
        })?
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
                    gates: factory_gates.clone(),
                    escaped: factory_escaped.clone(),
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
            // Routing is not permission. Grant each current peer boot explicitly.
            for host in &cluster.hosts {
                for peer in &cluster.peers {
                    if peer.node != host.identity().node {
                        host.grant_query_origin(NamespaceQueryOrigin {
                            cluster: group.cluster,
                            namespace: group.namespace,
                            revision: 1,
                            peer: peer.clone(),
                        })?;
                        host.grant_command_origin(NamespaceCommandOrigin {
                            cluster: group.cluster,
                            namespace: group.namespace,
                            revision: 1,
                            peer: peer.clone(),
                        })?;
                    }
                }
            }
            // The nested handle originates at a follower, exercising authenticated
            // command/query/receipt/enrollment forwarding through the canonical path.
            *target.lock().unwrap() =
                Some(cluster.hosts[(leader + 1) % 3].cell(cells[1].history().clone())?);
            // The parent and forwarded child retain both leader controller slots.
            // Observe on the unused voter without increasing that admission bound;
            // default linearizable reads still require a fresh quorum barrier.
            let observer = &cluster.hosts[(leader + 2) % 3];
            let observed_cells = [
                observer.cell(cells[0].history().clone())?,
                observer.cell(cells[1].history().clone())?,
            ];
            let mut options = command(177, durability)?;
            options.timeout = Duration::from_secs(20);
            horizon.store(options.retry_until_ms, Ordering::Release);
            let input = json!({"retry_until_ms": options.retry_until_ms,"target_s3":durability == Durability::Quorum})
                .to_string()
                .into_bytes();
            let (result, observed) = tokio::join!(
                cells[0].command(&cells[0].contract("Run")?, input.clone(), options.clone()),
                async {
                    cellar::asyncrt::timeout(TIMEOUT, gates[0].entered.notified()).await.context("SDK gate 0 entered")?;
                    let source = value(&observed_cells[0], 178).await?;
                    let target = value(&observed_cells[1], 179).await?;
                    gates[0].release.notify_one();
                    ensure!(
                        source == 1,
                        "target executed before source proof: observed {source}"
                    );
                    ensure!(target == 0, "target private writes escaped");
                    cellar::asyncrt::timeout(TIMEOUT, gates[1].entered.notified()).await.context("SDK gate 1 entered")?;
                    let source = value(&observed_cells[0], 182).await?;
                    gates[1].release.notify_one();
                    ensure!(source == 11, "query escaped its source barrier: {source}");
                    Ok::<_, anyhow::Error>(())
                }
            );
            observed.with_context(|| format!(
                "SDK nested observer; command error: {:?}",
                result.as_ref().err().map(ToString::to_string),
            ))?;
            let result = result.context("SDK nested command")?;
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

            // Synchronous callbacks and open async transactions cannot enqueue SDK work.
            cells[0]
                .command(
                    &cells[0].contract("Probe")?,
                    Vec::new(),
                    command(183, durability)?,
                )
                .await?;
            cells[0]
                .query(
                    ProposalId::new([184; 16])?,
                    &cells[0].contract("ProbeRead")?,
                    Vec::new(),
                    QueryOptions::default(),
                )
                .await?;
            let request = |kind: &str, options: &CommandOptions| {
                json!({"kind":kind,"retry_until_ms":options.retry_until_ms})
                    .to_string()
                    .into_bytes()
            };
            let mut atomic = command(185, durability)?;
            atomic.timeout = Duration::from_secs(20);
            ensure!(cells[0]
                .command(
                    &cells[0].contract("Run")?,
                    request("atomic", &atomic),
                    atomic
                )
                .await
                .is_err());
            ensure!(calls.load(Ordering::Acquire) == 1 && value(&cells[0], 186).await? == 11);

            // Dropping the child receiver leaves admitted work owned and parent output gated.
            let mut dropped = command(187, durability)?;
            dropped.timeout = Duration::from_secs(20);
            let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let completed = done.clone();
            let (output, observed) = tokio::join!(
                async {
                    let result = cells[0]
                        .command(
                            &cells[0].contract("Run")?,
                            request("drop", &dropped),
                            dropped,
                        )
                        .await;
                    completed.store(true, Ordering::Release);
                    result.map_err(anyhow::Error::from)
                },
                async {
                    cellar::asyncrt::timeout(TIMEOUT, gates[2].entered.notified()).await.context("SDK gate 2 entered")?;
                    ensure!(
                        value(&observed_cells[0], 188).await? == 12
                            && value(&observed_cells[1], 189).await? == 2
                    );
                    cellar::asyncrt::sleep(Duration::from_millis(100)).await;
                    let premature = done.load(Ordering::Acquire);
                    gates[2].release.notify_one();
                    ensure!(!premature, "parent output escaped a dropped child response");
                    Ok::<_, anyhow::Error>(())
                }
            );
            observed?;
            ensure!(output?.result == b"12");
            ensure!(value(&cells[1], 190).await? == 4 && calls.load(Ordering::Acquire) == 2);

            // Waiting behind an admitted root cannot restart the target's one-second budget.
            let mut queued = command(191, durability)?;
            queued.timeout = Duration::from_secs(20);
            let (expired, observed) = tokio::join!(
                cells[0].command(
                    &cells[0].contract("Run")?,
                    request("deadline", &queued),
                    queued
                ),
                async {
                    cellar::asyncrt::timeout(TIMEOUT, gates[3].entered.notified()).await.context("SDK gate 3 entered")?;
                    ensure!(value(&observed_cells[0], 192).await? == 12);
                    cellar::asyncrt::sleep(Duration::from_millis(1100)).await;
                    gates[3].release.notify_one();
                    Ok::<_, anyhow::Error>(())
                }
            );
            observed?;
            let expired = expired?;
            ensure!(expired.result == br#"{"expired":true}"#);
            ensure!(value(&cells[0], 193).await? == 13 && calls.load(Ordering::Acquire) == 2);

            // A recursive child uses the same native instance between proven roots.
            let mut recursive = command(194, durability)?;
            recursive.timeout = Duration::from_secs(20);
            let result = cells[1]
                .command(
                    &cells[1].contract("Run")?,
                    request("recursive", &recursive),
                    recursive,
                )
                .await?;
            ensure!(result.result == br#"{"recursive":7}"#);
            ensure!(value(&cells[1], 195).await? == 7 && calls.load(Ordering::Acquire) == 3);

            let oversized = command(201, durability)?;
            ensure!(cells[0]
                .command(
                    &cells[0].contract("Run")?,
                    request("oversized", &oversized),
                    oversized
                )
                .await
                .is_err());
            ensure!(value(&cells[0], 202).await? == 13 && calls.load(Ordering::Acquire) == 3);

            // Fence an admitted child. Its escaped receiver retains uncertainty and its ID.
            let mut fencing = command(196, durability)?;
            fencing.timeout = Duration::from_secs(20);
            let held = request("fence", &fencing);
            let (fenced, moved) = tokio::join!(
                cells[0].command(&cells[0].contract("Run")?, held.clone(), fencing.clone()),
                async {
                    cellar::asyncrt::timeout(TIMEOUT, gates[4].entered.notified()).await.context("SDK gate 4 entered")?;
                    ensure!(
                        value(&observed_cells[0], 197).await? == 14
                            && value(&observed_cells[1], 198).await? == 9
                    );
                    let next = (leader + 1) % 3;
                    let handle = cluster.hosts[leader].group(group)?;
                    let status = handle.replication_status(TIMEOUT).await?;
                    handle
                        .transfer_leadership(
                            LeadershipTransfer {
                                target: MemberId::new(next as u64 + 1)?,
                                expected_term: status.term,
                                expected_revision: status.membership_revision,
                            },
                            TIMEOUT,
                        )
                        .await?;
                    cellar::asyncrt::timeout(TIMEOUT, async {
                        loop {
                            let status = cluster.hosts[next].group(group)?.status().await?;
                            if status.ready && status.leader == Some(next as u64 + 1) {
                                break Ok::<_, anyhow::Error>(());
                            }
                            cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                        }
                    })
                    .await??;
                    cellar::asyncrt::timeout(TIMEOUT, gates[5].dropped.notified()).await.context("SDK gate 5 dropped")?;
                    Ok::<_, anyhow::Error>(next)
                }
            );
            let next = moved?;
            ensure!(fenced.is_err());
            let pending = escaped
                .lock()
                .unwrap()
                .take()
                .context("escaped child receiver")?;
            let error = pending
                .await
                .expect_err("obsolete source cannot settle a child");
            ensure!(
                error.ambiguous && error.command == CommandId::new(ClientId::new([225; 16])?, 1)?
            );
            let source = cluster.hosts[next].cell(cells[0].history().clone())?;
            let target = cluster.hosts[next].cell(cells[1].history().clone())?;
            ensure!(value(&source, 199).await? == 14 && value(&target, 200).await? == 9);
            ensure!(source
                .command(&source.contract("Run")?, held, fencing)
                .await
                .is_err());
            ensure!(calls.load(Ordering::Acquire) == 4);
            ensure!(
                target
                    .command(
                        &target.contract("Run")?,
                        br#"{"target":true,"hold":false}"#.to_vec(),
                        command(227, durability)?
                    )
                    .await?
                    .result
                    == b"11"
            );

            // An admitted callback's Busy error must never become an admission retry.
            let busy = command(232, durability)?;
            let error = source
                .command(&source.contract("Run")?, request("busy", &busy), busy)
                .await
                .expect_err("admitted callback Busy");
            ensure!(matches!(
                error.cause.as_ref(),
                CellDispatchError::Execution(ExecutionError::Busy)
            ));
            ensure!(calls.load(Ordering::Acquire) == 6);
            ensure!(value(&source, 233).await? == 14);

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
