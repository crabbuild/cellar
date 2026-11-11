// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public name discovery selects an exact history through the shared runtime.

use super::*;

#[derive(Clone)]
struct Target {
    owner: std::sync::Weak<RaftCells>,
    history: CellHistory,
    member: MemberId,
}

impl Target {
    fn options(&self) -> QueryOptions {
        QueryOptions {
            routing: QueryRouting::Replicas {
                members: vec![self.member],
                attempt_timeout: TIMEOUT,
                leader_fallback: false,
            },
            ..Default::default()
        }
    }
}

struct Resolver {
    storage: Storage,
    target: Arc<Mutex<Option<Target>>>,
    gate: Arc<RpcGate>,
    calls: Arc<AtomicUsize>,
}

fn lookup(
    target: &Target,
) -> impl std::future::Future<
    Output = std::result::Result<Option<CellHistory>, Arc<CellDispatchError>>,
> + Send
       + 'static {
    target
        .owner
        .upgrade()
        .expect("live fixture owner")
        .resolve_cell_history(
            target.history.cluster,
            target.history.namespace,
            target.history.cell.clone(),
            target.options(),
        )
}

fn refused(target: &Target) -> Result<()> {
    let error = lookup(target)
        .now_or_never()
        .context("callback lookup must refuse before scheduling")?
        .expect_err("synchronous callback lookup");
    ensure!(error.refusal() == Some(&CommandFailure::UnsupportedCapability));
    let owner = target.owner.upgrade().context("live fixture owner")?;
    let error = owner
        .cell(target.history.clone())
        .err()
        .context("callback handle construction must refuse")?;
    ensure!(error.refusal() == Some(&CommandFailure::UnsupportedCapability));
    Ok(())
}

impl Cell for Resolver {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc {
                args: RpcData::Json(input),
                ..
            } = invocation
            else {
                anyhow::bail!("JSON RPC")
            };
            let input: serde_json::Value = serde_json::from_str(&input)?;
            self.calls.fetch_add(1, Ordering::AcqRel);
            let result = if input["target"] == true {
                json!(update(&self.storage, 2)?)
            } else {
                let target = self
                    .target
                    .lock()
                    .unwrap()
                    .clone()
                    .context("target binding")?;
                if input["atomic"] == true {
                    self.storage
                        .transaction(&context, move |storage| async move {
                            update(&storage, 1000)?;
                            refused(&target)?;
                            anyhow::bail!("atomic rollback")
                        })
                        .await?;
                    anyhow::bail!("atomic callback unexpectedly completed")
                }
                update(&self.storage, 1)?;
                let selected = lookup(&target).await?.context("enrolled target")?;
                ensure!(selected == target.history);
                self.gate.entered.notify_one();
                self.gate.release.notified().await;
                let owner = target.owner.upgrade().context("live target owner")?;
                // Pure handle construction is permitted only in this ordinary
                // RPC's current frame; actual command dispatch proves its root.
                let cell = owner.cell(selected.clone())?;
                let horizon = input["retry_until_ms"].as_u64().context("retry horizon")?;
                let id = CommandId::new(ClientId::new([243; 16])?, 1)?;
                self.storage.put_json(
                    "pending-target",
                    &json!({
                        "cell":selected.cell.as_str(),"incarnation":selected.incarnation.bytes(),
                        "client":243,"sequence":1,"retry_until_ms":horizon
                    }),
                )?;
                let receipt = cell
                    .command(
                        &cell.contract("Run")?,
                        br#"{"target":true}"#.to_vec(),
                        CommandOptions::new(id, horizon),
                    )
                    .await?;
                json!({"source":self.storage.get_json("value")?.context("source value")?,"target":serde_json::from_slice::<i64>(&receipt.result)?})
            };
            Ok(Output::Rpc(RpcData::Json(result.to_string())))
        })
    }
}

pub async fn run(
    rooted: bool,
    durability: Durability,
    store: Arc<catalog_fault::Store>,
) -> Result<()> {
    let target = Arc::new(Mutex::new(None::<Target>));
    let gate = Arc::new(RpcGate::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let factory_target = target.clone();
    let factory_gate = gate.clone();
    let factory_calls = calls.clone();
    let probe_target = target.clone();
    let read_target = target.clone();
    let class = RaftClass::new("Counter", 1, [201; 32])?
        .initializer([202; 32], |_, tx| {
            tx.execute_batch(
                "CREATE TABLE counter(value INTEGER NOT NULL); INSERT INTO counter VALUES(0);",
            )?;
            tx.storage()?.put_json("value", &json!(0))
        })?
        .json_cell_rpc("Run", [203; 32])?
        .command("Probe", [204; 32], move |ctx, _| {
            update(&ctx.storage()?, 1000)?;
            refused(
                probe_target
                    .lock()
                    .unwrap()
                    .as_ref()
                    .context("probe target")?,
            )?;
            anyhow::bail!("probe rollback")
        })?
        .query("ProbeRead", [205; 32], move |_, _| {
            refused(
                read_target
                    .lock()
                    .unwrap()
                    .as_ref()
                    .context("probe target")?,
            )?;
            Ok(Vec::new())
        })?
        .query("Get", [206; 32], |ctx, _| {
            Ok(serde_json::to_vec(&ctx.storage().get_json("value")?)?)
        })?;
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |ctx, _| {
                Ok(Arc::new(Resolver {
                    storage: ctx.storage()?,
                    target: factory_target.clone(),
                    gate: factory_gate.clone(),
                    calls: factory_calls.clone(),
                }))
            },
        )?,
    );
    let declared = configuration::options("name-discovery");
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
            let origin = (leader + 1) % 3;
            let owner = Arc::new(cluster.hosts[origin].clone());
            let source = cluster.hosts[leader].cell(CellHistory {
                cluster: group.cluster,
                namespace: group.namespace,
                cell: CellId::new("Counter:name-source")?,
                incarnation: IncarnationId::new([211; 16])?,
            })?;
            let destination = cluster.hosts[leader].cell(CellHistory {
                cluster: group.cluster,
                namespace: group.namespace,
                cell: CellId::new("Counter:name-target")?,
                incarnation: IncarnationId::new([212; 16])?,
            })?;
            for (cell, id) in [(&source, 211), (&destination, 212)] {
                cell.enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([id; 16])?,
                        durability,
                    },
                    TIMEOUT,
                )
                .await?;
            }
            let binding = Target {
                owner: Arc::downgrade(&owner),
                history: destination.history().clone(),
                member: MemberId::new(leader as u64 + 1)?,
            };
            *target.lock().unwrap() = Some(binding.clone());
            ensure!(
                lookup(&binding).await.is_err(),
                "name lookup ignored its namespace read grant"
            );
            for host in &cluster.hosts {
                for peer in &cluster.peers {
                    if peer.node != host.identity().node {
                        host.grant_query_origin(NamespaceQueryOrigin {
                            cluster: group.cluster,
                            namespace: group.namespace,
                            revision: 1,
                            peer: peer.clone(),
                        })?;
                    }
                }
            }
            ensure!(lookup(&binding).await? == Some(destination.history().clone()));
            ensure!(
                calls.load(Ordering::Acquire) == 0,
                "name lookup constructed an application"
            );
            ensure!(owner
                .resolve_cell_history(
                    group.cluster,
                    group.namespace,
                    CellId::new("Counter:absent")?,
                    binding.options()
                )
                .await?
                .is_none());
            let stale = owner
                .resolve_cell_history(
                    group.cluster,
                    group.namespace,
                    destination.history().cell.clone(),
                    QueryOptions {
                        consistency: QueryConsistency::Stale,
                        ..Default::default()
                    },
                )
                .now_or_never()
                .context("stale discovery refuses before work")?
                .expect_err("fresh names require quorum");
            ensure!(stale.refusal() == Some(&CommandFailure::UnsupportedConsistency));
            let delayed = owner.resolve_cell_history(
                group.cluster,
                group.namespace,
                destination.history().cell.clone(),
                QueryOptions {
                    timeout: Duration::from_millis(1),
                    ..Default::default()
                },
            );
            cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            ensure!(
                delayed.await.is_err(),
                "unpolled lookup restarted its deadline"
            );
            for host in &cluster.hosts {
                for peer in &cluster.peers {
                    if peer.node != host.identity().node {
                        host.grant_command_origin(NamespaceCommandOrigin {
                            cluster: group.cluster,
                            namespace: group.namespace,
                            revision: 1,
                            peer: peer.clone(),
                        })?;
                    }
                }
            }
            let horizon = u64::try_from(cellar::asyncrt::wall_ms())?
                .checked_add(60_000)
                .context("retry horizon")?;
            let options = CommandOptions {
                durability,
                timeout: Duration::from_secs(20),
                ..CommandOptions::new(CommandId::new(ClientId::new([242; 16])?, 1)?, horizon)
            };
            let input = json!({"retry_until_ms":horizon}).to_string().into_bytes();
            let observer = cluster.hosts[(leader + 2) % 3].cell(source.history().clone())?;
            let (response, observed) = tokio::join!(
                source.command(&source.contract("Run")?, input.clone(), options.clone()),
                async {
                    cellar::asyncrt::timeout(TIMEOUT, gate.entered.notified()).await?;
                    let value = observer
                        .query(
                            ProposalId::new([241; 16])?,
                            &observer.contract("Get")?,
                            Vec::new(),
                            QueryOptions::default(),
                        )
                        .await?;
                    gate.release.notify_one();
                    ensure!(value.result == b"1", "name lookup escaped its source proof");
                    Ok::<_, anyhow::Error>(())
                }
            );
            observed?;
            let response = response?;
            ensure!(response.result == br#"{"source":1,"target":2}"#);
            ensure!(
                source
                    .command(&source.contract("Run")?, input, options)
                    .await?
                    .result
                    == response.result
            );
            ensure!(calls.load(Ordering::Acquire) == 2);
            let mut control =
                CommandOptions::new(CommandId::new(ClientId::new([244; 16])?, 1)?, horizon);
            control.durability = durability;
            ensure!(source
                .command(&source.contract("Probe")?, Vec::new(), control.clone())
                .await
                .is_err());
            source
                .query(
                    ProposalId::new([245; 16])?,
                    &source.contract("ProbeRead")?,
                    Vec::new(),
                    QueryOptions::default(),
                )
                .await?;
            control.id = CommandId::new(ClientId::new([246; 16])?, 1)?;
            ensure!(source
                .command(
                    &source.contract("Run")?,
                    br#"{"atomic":true}"#.to_vec(),
                    control
                )
                .await
                .is_err());
            ensure!(calls.load(Ordering::Acquire) == 3);
            let value = source
                .query(
                    ProposalId::new([247; 16])?,
                    &source.contract("Get")?,
                    Vec::new(),
                    QueryOptions::default(),
                )
                .await?;
            ensure!(
                value.result == b"1",
                "refused lookup published private changes"
            );
            let old = cluster.hosts[leader].group(group)?;
            let status = old.replication_status(TIMEOUT).await?;
            old.transfer_leadership(
                LeadershipTransfer {
                    target: MemberId::new(origin as u64 + 1)?,
                    expected_term: status.term,
                    expected_revision: status.membership_revision,
                },
                TIMEOUT,
            )
            .await?;
            cellar::asyncrt::timeout(TIMEOUT, async {
                loop {
                    let status = cluster.hosts[origin].group(group)?.status().await?;
                    if status.ready && status.leader == Some(origin as u64 + 1) {
                        break Ok::<_, anyhow::Error>(());
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
            })
            .await??;
            ensure!(
                lookup(&binding).await? == Some(destination.history().clone()),
                "follower name lookup lost the current incarnation"
            );
            let mut stopped = Vec::new();
            let mut stopped_hosts = Vec::new();
            let mut kept = Vec::new();
            for (n, node) in std::mem::take(&mut cluster.nodes).into_iter().enumerate() {
                if n == origin {
                    kept.push(node)
                } else {
                    stopped.push(node);
                    stopped_hosts.push(cluster.hosts[n].clone());
                }
            }
            managed_fixture::close_group_nodes(stopped, &stopped_hosts).await?;
            cluster.nodes = kept;
            cluster.hosts = vec![(*owner).clone()];
            ensure!(
                owner
                    .resolve_cell_history(
                        group.cluster,
                        group.namespace,
                        destination.history().cell.clone(),
                        QueryOptions {
                            timeout: Duration::from_secs(1),
                            max_attempts: 1,
                            routing: QueryRouting::Local,
                            ..Default::default()
                        }
                    )
                    .await
                    .is_err(),
                "name returned from retained metadata without quorum"
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
            eprintln!("retained name-discovery disk: {}", root.keep().display());
        }
    }
    target.lock().unwrap().take();
    cluster.fixtures[0].domain.shutdown().await;
    work?;
    joined
}
