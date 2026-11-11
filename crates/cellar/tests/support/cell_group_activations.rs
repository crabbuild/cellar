// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public native activation lifetime, node capacity and fencing checks.

use super::*;
use cellar::sdk::ShutdownMode;

struct WarmCell {
    storage: Storage,
    turns: AtomicUsize,
    previous: Mutex<Option<Storage>>,
}

impl Cell for WarmCell {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            ensure!(
                matches!(invocation, Invocation::CellRpc { ref method, .. } if method == "Tick")
            );
            ensure!(context.var("COUNT_STEP") == Some("1"));
            let current = context.storage()?;
            if let Some(previous) = self.previous.lock().unwrap().replace(current) {
                ensure!(
                    previous.get_json("visits").is_err(),
                    "old invocation Storage was adopted"
                );
            }
            let builds = self
                .storage
                .get_json("builds")?
                .and_then(|value| value.as_u64())
                .context("build count")?;
            let visits = self
                .storage
                .get_json("visits")?
                .and_then(|value| value.as_u64())
                .context("visit count")?
                + 1;
            self.storage.put_json("visits", &json!(visits))?;
            self.storage.sync().await?;
            let memory = self.turns.fetch_add(1, Ordering::AcqRel) + 1;
            Ok(Output::Rpc(RpcData::Json(
                json!([builds, visits, memory]).to_string(),
            )))
        })
    }
}

fn options(client: u8) -> Result<CommandOptions> {
    let now = u64::try_from(cellar::asyncrt::wall_ms())?;
    Ok(CommandOptions::new(
        CommandId::new(ClientId::new([client; 16])?, 1)?,
        now.checked_add(60_000).context("retry horizon")?,
    ))
}

async fn tick(
    cell: &RaftCellHandle,
    client: u8,
    expected: serde_json::Value,
) -> Result<CommandOptions> {
    let options = options(client)?;
    let receipt = cell
        .command(&cell.contract("Tick")?, b"{}".to_vec(), options.clone())
        .await?;
    ensure!(receipt.durability == Durability::Quorum);
    ensure!(serde_json::from_slice::<serde_json::Value>(&receipt.result)? == expected);
    Ok(options)
}

fn usage(host: &RaftCells, idle: usize, retained: usize) -> Result<()> {
    let sql = host.resource_usage().sql.context("SQL activation sample")?;
    ensure!(
        sql.idle_activations == idle && sql.retained_activations == retained,
        "activation usage idle={} retained={}, expected {idle}/{retained}",
        sql.idle_activations,
        sql.retained_activations
    );
    ensure!(
        sql.activation_capacity == sql.limits.max_pending_cells
            && retained <= sql.activation_capacity
    );
    Ok(())
}

pub async fn run(rooted: bool, store: Arc<catalog_fault::Store>) -> Result<()> {
    let constructors = Arc::new(AtomicUsize::new(0));
    let counted = constructors.clone();
    let workers = Arc::new(Mutex::new(std::collections::HashSet::new()));
    let observed_workers = workers.clone();
    let class = RaftClass::new("Counter", 1, [170; 32])?
        .initializer([171; 32], |_, transaction| {
            let storage = transaction.storage()?;
            storage.put_json("builds", &json!(0))?;
            storage.put_json("visits", &json!(0))
        })?
        .json_cell_rpc("Tick", [172; 32])?;
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |context, _| {
                ensure!(context.identity().context("configured identity")?.class == "Counter");
                let step: u64 = context
                    .var("COUNT_STEP")
                    .context("configured step")?
                    .parse()?;
                counted.fetch_add(1, Ordering::AcqRel);
                observed_workers
                    .lock()
                    .unwrap()
                    .insert(std::thread::current().id());
                let storage = context.storage()?;
                let builds = storage
                    .get_json("builds")?
                    .and_then(|value| value.as_u64())
                    .context("prior builds")?;
                storage.put_json("builds", &json!(builds + step))?;
                Ok(Arc::new(WarmCell {
                    storage,
                    turns: AtomicUsize::new(0),
                    previous: Mutex::new(None),
                }))
            },
        )?,
    );
    let mut deployment_options = configuration::options("warm-cell-service");
    deployment_options.vars = vec![("COUNT_STEP".into(), "1".into())];
    application.validate_deployment(&configuration::deployment(deployment_options))?;
    let fixture = Fixture::with_application(rooted, "Counter", application, store).await?;
    ensure!(fixture.sql_workers == 2);
    let mut cluster = Box::pin(ManagedCluster::start_with_applications(
        fixture,
        &[],
        ArchiveSetup::Initialized,
        false,
        None,
        FirstGroupSetup::Normal,
    ))
    .await?;
    let domain = cluster.fixtures[0].domain.clone();
    let work = domain
        .run(async {
            let group = cluster.history;
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            let capacity = cluster.hosts[leader]
                .resource_usage()
                .sql
                .context("cache capacity")?
                .activation_capacity;
            ensure!(capacity == 8);
            let mut cells = Vec::new();
            for index in 0..=capacity {
                let cell = cluster.hosts[leader].cell(CellHistory {
                    cluster: group.cluster,
                    namespace: group.namespace,
                    cell: CellId::new(format!("Counter:cached-{index}"))?,
                    incarnation: IncarnationId::new([174; 16])?,
                })?;
                cell.enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([u8::try_from(index)? + 180; 16])?,
                        durability: Durability::Quorum,
                    },
                    TIMEOUT,
                )
                .await?;
                cells.push(cell);
            }
            usage(&cluster.hosts[leader], 0, 0)?;
            let first_options = options(190)?;
            let first = cells[0]
                .command(
                    &cells[0].contract("Tick")?,
                    b"{}".to_vec(),
                    first_options.clone(),
                )
                .await?;
            ensure!(
                serde_json::from_slice::<serde_json::Value>(&first.result)? == json!([1, 1, 1])
            );
            let replay = cells[0]
                .command(
                    &cells[0].contract("Tick")?,
                    b"{}".to_vec(),
                    first_options.clone(),
                )
                .await?;
            ensure!(replay.result == first.result && replay.version == first.version);
            tick(&cells[0], 191, json!([1, 2, 2])).await?;
            ensure!(constructors.load(Ordering::Acquire) == 1);
            usage(&cluster.hosts[leader], 1, 1)?;
            for cell in cells.iter().skip(1) {
                tick(cell, 190, json!([1, 1, 1])).await?;
            }
            ensure!(constructors.load(Ordering::Acquire) == capacity + 1);
            ensure!(
                workers.lock().unwrap().len() == 2,
                "both SQL workers must own native instances"
            );
            usage(&cluster.hosts[leader], capacity, capacity)?;
            tick(&cells[0], 192, json!([2, 3, 1])).await?;
            ensure!(constructors.load(Ordering::Acquire) == capacity + 2);
            usage(&cluster.hosts[leader], capacity, capacity)?;

            let handle = cluster.hosts[leader].group(group)?;
            let status = handle.replication_status(TIMEOUT).await?;
            let target = (leader + 1) % 3;
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
                while cluster.hosts[leader]
                    .resource_usage()
                    .sql
                    .context("retired cache")?
                    .retained_activations
                    != 0
                {
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
            usage(&cluster.hosts[leader], 0, 0)?;
            // Transfer admission revokes the old cache before the target has
            // necessarily established its fresh-term applied quorum barrier.
            cellar::asyncrt::timeout(TIMEOUT, async {
                loop {
                    let elected = cluster.hosts[target].group(group)?.status().await?;
                    if elected.ready && elected.leader == Some(target as u64 + 1) {
                        return Ok::<_, anyhow::Error>(());
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
            })
            .await??;
            let current = cluster.hosts[target].cell(cells[0].history().clone())?;
            let replay = current
                .command(&current.contract("Tick")?, b"{}".to_vec(), first_options)
                .await?;
            ensure!(replay.result == first.result);
            ensure!(constructors.load(Ordering::Acquire) == capacity + 2);
            usage(&cluster.hosts[target], 0, 0)?;
            tick(&current, 193, json!([3, 4, 1])).await?;
            usage(&cluster.hosts[target], 1, 1)?;
            ensure!(constructors.load(Ordering::Acquire) == capacity + 3);

            // Real idle expiry on the worker's existing clock, without a client
            // request or changing any RPC/lease deadline.
            cellar::asyncrt::sleep(Duration::from_secs(60)).await;
            cellar::asyncrt::timeout(TIMEOUT, async {
                while cluster.hosts[target]
                    .resource_usage()
                    .sql
                    .context("expired cache")?
                    .retained_activations
                    != 0
                {
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
            usage(&cluster.hosts[target], 0, 0)?;
            let retained_options = tick(&current, 194, json!([4, 5, 1])).await?;
            ensure!(constructors.load(Ordering::Acquire) == capacity + 4);
            usage(&cluster.hosts[target], 1, 1)?;

            let old_hosts = std::mem::take(&mut cluster.hosts);
            let identities = old_hosts
                .iter()
                .map(RaftCells::identity)
                .collect::<Vec<_>>();
            managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &old_hosts)
                .await?;
            cluster.peers.clear();
            for (index, fixture) in cluster.fixtures.iter().enumerate() {
                let node = managed_fixture::start_node(
                    cluster.roots[index].path(),
                    fixture.application.clone(),
                    &fixture.spec.label,
                )
                .await?;
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
                        node.shutdown(ShutdownMode::Preserve).await?;
                        return Err(error).context("retained activation node");
                    }
                };
                ensure!(
                    host.identity().node == identities[index].node
                        && host.identity().process != identities[index].process
                );
                usage(&host, 0, 0)?;
                cluster.nodes.push(node);
                cluster.hosts.push(host);
                cluster.peers.push(peer);
            }
            // Canonical Resume startup reopens the retained groups. New-group
            // admission is separate from recovery of these existing voters.
            let restarted = managed_fixture::leader(&cluster.hosts, group).await?;
            let current = cluster.hosts[restarted].cell(cells[0].history().clone())?;
            let replay = current
                .command(&current.contract("Tick")?, b"{}".to_vec(), retained_options)
                .await?;
            ensure!(
                serde_json::from_slice::<serde_json::Value>(&replay.result)? == json!([4, 5, 1])
            );
            ensure!(constructors.load(Ordering::Acquire) == capacity + 4);
            usage(&cluster.hosts[restarted], 0, 0)?;
            tick(&current, 195, json!([5, 6, 1])).await?;
            ensure!(constructors.load(Ordering::Acquire) == capacity + 5);
            usage(&cluster.hosts[restarted], 1, 1)?;
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
            eprintln!("retained activation disk: {}", root.keep().display());
        }
    }
    cluster.fixtures[0].domain.shutdown().await;
    work?;
    joined
}
