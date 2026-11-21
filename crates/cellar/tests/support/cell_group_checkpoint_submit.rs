// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Checkpoint replication starts before unrelated queued SQL occupies its worker.

use super::*;
use std::{future::Future, sync::Condvar};

#[derive(Default)]
struct Gate {
    entered: Notify,
    released: Mutex<bool>,
    wake: Condvar,
}

impl Gate {
    fn hold(&self) {
        self.entered.notify_one();
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.wake.wait(released).unwrap();
        }
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}

struct Release(Arc<Gate>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct SyncCell {
    storage: Storage,
    first_poll: Arc<Gate>,
    calls: Arc<AtomicUsize>,
}

impl Cell for SyncCell {
    fn invoke(self: Arc<Self>, _: Context, invocation: Invocation) -> HandlerFuture<Output> {
        let gate = self.first_poll.clone();
        let mut first = true;
        let mut future = Box::pin(async move {
            ensure!(
                matches!(invocation, Invocation::CellRpc { ref method, .. } if method == "Run")
            );
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.storage.put_json("value", &json!(1))?;
            self.storage.sync().await?;
            self.storage.put_json("value", &json!(2))?;
            Ok(Output::Rpc(RpcData::Json("2".into())))
        });
        Box::pin(futures_util::future::poll_fn(move |cx| {
            let result = future.as_mut().poll(cx);
            if first && result.is_pending() {
                first = false;
                // Hold the actual worker before its framework checkpoint work.
                gate.hold();
            }
            result
        }))
    }
}

async fn read(cell: &RaftCellHandle, proposal: u8) -> Result<i64> {
    let reply = cell
        .query(
            ProposalId::new([proposal; 16])?,
            &cell.contract("Get")?,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Stale,
                routing: QueryRouting::Local,
                ..Default::default()
            },
        )
        .await?;
    serde_json::from_slice(&reply.result).map_err(Into::into)
}

pub async fn qualify() -> Result<()> {
    let mut failures = Vec::new();
    for rooted in [false, true] {
        for durability in [Durability::Quorum, Durability::S3] {
            if let Err(error) = run(rooted, durability).await {
                failures.push(format!(
                    "rooted={rooted} durability={durability:?}: {error:#}"
                ));
            }
        }
    }
    ensure!(
        failures.is_empty(),
        "checkpoint submission qualification: {failures:?}"
    );
    Ok(())
}

async fn run(rooted: bool, durability: Durability) -> Result<()> {
    let store = Arc::new(catalog_fault::Store::default());
    let result = run_with_store(rooted, durability, store.clone()).await;
    managed_fixture::memory::preserve_result(&store.inner, "checkpoint-submit", result).await
}

async fn run_with_store(
    rooted: bool,
    durability: Durability,
    store: Arc<catalog_fault::Store>,
) -> Result<()> {
    let first_poll = Arc::new(Gate::default());
    let query_gate = Arc::new(Gate::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let hold = query_gate.clone();
    let class = RaftClass::new("Counter", 1, [211; 32])?
        .initializer([212; 32], |_, tx| {
            tx.storage()?.put_json("value", &json!(0))
        })?
        .json_cell_rpc("Run", [213; 32])?
        .query("Get", [214; 32], |query, _| {
            Ok(serde_json::to_vec(&query.storage().get_json("value")?)?)
        })?
        .query("Hold", [215; 32], move |_, _| {
            hold.hold();
            Ok(vec![])
        })?;
    let poll_gate = first_poll.clone();
    let invoked = calls.clone();
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |context, _| {
                Ok(Arc::new(SyncCell {
                    storage: context.storage()?,
                    first_poll: poll_gate.clone(),
                    calls: invoked.clone(),
                }))
            },
        )?,
    );
    let configuration = configuration::options("checkpoint-submit-service");
    application.validate_deployment(&configuration::deployment(configuration.clone()))?;
    let mut fixture = Fixture::with_application(rooted, "Counter", application, store).await?;
    fixture.configuration = Some(configuration);
    fixture.sql_workers = 1;
    let log_gate = storage_fault::Gate::new(None);
    fixture.filesystem = Arc::new(storage_fault::FaultFileSystem {
        gate: log_gate.clone(),
    });
    let mut cluster = Box::pin(ManagedCluster::start_with_applications(
        fixture,
        &[],
        ArchiveSetup::Initialized,
        false,
        None,
        FirstGroupSetup::Normal,
    ))
    .await?;
    let work = cluster.fixtures[0].domain.run(async {
        let group = cluster.history;
        let leader = managed_fixture::leader(&cluster.hosts, group).await?;
        if leader != 0 {
            let handle = cluster.hosts[leader].group(group)?;
            let status = handle.replication_status(TIMEOUT).await?;
            handle.transfer_leadership(LeadershipTransfer { target: MemberId::new(1)?, expected_term: status.term, expected_revision: status.membership_revision }, TIMEOUT).await?;
        }
        cellar::asyncrt::timeout(TIMEOUT, async {
            while managed_fixture::leader(&cluster.hosts, group).await? != 0 { cellar::asyncrt::sleep(Duration::from_millis(10)).await; }
            Ok::<_, anyhow::Error>(())
        }).await??;
        let source = cluster.hosts[0].cell(CellHistory { cluster: group.cluster, namespace: group.namespace, cell: CellId::new("Counter:checkpoint-submit-source")?, incarnation: IncarnationId::new([216; 16])? })?;
        let other = cluster.hosts[0].cell(CellHistory { cell: CellId::new("Counter:checkpoint-submit-other")?, incarnation: IncarnationId::new([217; 16])?, ..source.history().clone() })?;
        for (cell, tag) in [(&source, 216), (&other, 217)] {
            cell.enroll(CellEnrollmentRequest { creation: ProposalId::new([tag; 16])?, durability }, TIMEOUT).await?;
        }
        let peers = [cluster.hosts[1].cell(source.history().clone())?, cluster.hosts[2].cell(source.history().clone())?];
        // Drain initial replication before freezing the single source worker.
        cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                if read(&peers[0], 218).await? == 0 && read(&peers[1], 219).await? == 0 { return Ok::<_, anyhow::Error>(()); }
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
        }).await??;
        let before = cluster.hosts[0].group(group)?.status().await?.applied;
        let run = source.contract("Run")?;
        let hold = other.contract("Hold")?;
        let options = interleaving::command(220, 1, durability)?;
        let completed = std::sync::atomic::AtomicBool::new(false);
        let (result, observed) = tokio::join!(
            async {
                let result = source.command(&run, b"{}".to_vec(), options.clone()).await;
                if let Err(error) = &result { eprintln!("CHECKPOINT_SUBMIT_RESPONSE {}", json!({"ambiguous":error.ambiguous,"command":format!("{:?}",error.command),"error":format!("{error:#}")})); }
                completed.store(true, Ordering::Release);
                result
            },
            async {
                let _release_poll = Release(first_poll.clone());
                let _release_query = Release(query_gate.clone());
                cellar::asyncrt::timeout(TIMEOUT, first_poll.entered.notified()).await.context("first source poll")?;
                let (query, observed) = tokio::join!(
                    other.query(ProposalId::new([221; 16])?, &hold, vec![], QueryOptions { consistency: QueryConsistency::Stale, routing: QueryRouting::Local, ..Default::default() }),
                    async {
                        let _release_poll = Release(first_poll.clone());
                        let _release_query = Release(query_gate.clone());
                        // Real queued query admission, before releasing source poll.
                        cellar::asyncrt::timeout(TIMEOUT, async {
                            while cluster.hosts[0].resource_usage().sql.context("SQL queue accounting")?.queued_or_running == 0 { cellar::asyncrt::sleep(Duration::from_millis(1)).await; }
                            Ok::<_, anyhow::Error>(())
                        }).await??;
                        // Prevent a fast checkpoint apply from overtaking the queued query.
                        let release_log = log_gate.arm(storage_fault::HOLD_SYNC);
                        first_poll.release();
                        let checked = async {
                            cellar::asyncrt::timeout(TIMEOUT, query_gate.entered.notified()).await.context("queued SQL query entered")?;
                            drop(release_log);
                            // Leave the original five-second command budget for final apply.
                            let progress = cellar::asyncrt::timeout(Duration::from_secs(1), async {
                                loop {
                                    if read(&peers[0], 222).await? == 1 && read(&peers[1], 223).await? == 1 { return Ok::<_, anyhow::Error>(()); }
                                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                                }
                            }).await;
                            let replicated = matches!(progress, Ok(Ok(())));
                            let status = cluster.hosts[0].group(group)?.status().await?;
                            ensure!(status.applied == before, "leader applied while SQL owner was held");
                            ensure!(!completed.load(Ordering::Acquire), "caller acknowledged without leader apply");
                            Ok::<_, anyhow::Error>(replicated)
                        }.await;
                        query_gate.release();
                        checked
                    },
                );
                query_gate.release();
                if let Err(error) = &query { eprintln!("CHECKPOINT_SUBMIT_QUERY_RESPONSE {error:#}"); }
                let replicated = observed?;
                query?;
                Ok::<_, anyhow::Error>(replicated)
            },
        );
        let receipt = result?;
        ensure!(receipt.result == b"2" && receipt.durability == durability);
        let replay = source.command(&run, b"{}".to_vec(), options).await;
        if let Err(error) = &replay { eprintln!("CHECKPOINT_SUBMIT_REPLAY_RESPONSE {}", json!({"ambiguous":error.ambiguous,"command":format!("{:?}",error.command),"error":format!("{error:#}")})); }
        let replay = replay?;
        ensure!(replay.id == receipt.id && replay.result == receipt.result && replay.durability == durability && replay.version.history == receipt.version.history && replay.version.sequence.get() >= receipt.version.sequence.get());
        ensure!(calls.load(Ordering::Acquire) == 1 && read(&source, 224).await? == 2);
        observed
    }).await.map_err(anyhow::Error::from).and_then(std::convert::identity);
    first_poll.release();
    query_gate.release();
    let joined =
        managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
            .await;
    for root in std::mem::take(&mut cluster.roots) {
        eprintln!(
            "retained checkpoint submission disk: {}",
            root.keep().display()
        );
    }
    if let Err(error) = &joined {
        eprintln!("checkpoint submission joined cleanup: {error:#}");
    }
    eprintln!(
        "CHECKPOINT_SUBMISSION {}",
        json!({"rooted":rooted,"durability":format!("{durability:?}"),"replicatedWhileSqlHeld":work.as_ref().ok(),"valuesAndReplayVerified":work.is_ok(),"joinedClose":joined.is_ok()})
    );
    cluster.fixtures[0].domain.shutdown().await;
    let replicated = work?;
    joined?;
    ensure!(
        replicated,
        "prepared checkpoint did not replicate before unrelated SQL finished"
    );
    Ok(())
}
