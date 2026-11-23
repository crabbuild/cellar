// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Checkpoint proof observation progresses while unrelated SQL holds its owner.

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
    resumed: Arc<AtomicUsize>,
    holder_ready: Arc<Notify>,
    holder_start: Arc<Notify>,
    holder_gate: Arc<Gate>,
    holder_calls: Arc<AtomicUsize>,
}

impl Cell for SyncCell {
    fn invoke(self: Arc<Self>, _: Context, invocation: Invocation) -> HandlerFuture<Output> {
        if matches!(&invocation, Invocation::CellRpc { method, .. } if method == "HoldLater") {
            return Box::pin(async move {
                self.holder_calls.fetch_add(1, Ordering::AcqRel);
                self.holder_ready.notify_one();
                self.holder_start.notified().await;
                self.holder_gate.hold();
                Ok(Output::Rpc(RpcData::Json("0".into())))
            });
        }
        let gate = self.first_poll.clone();
        let mut first = true;
        let mut future = Box::pin(async move {
            ensure!(
                matches!(invocation, Invocation::CellRpc { ref method, .. } if method == "Run")
            );
            self.calls.fetch_add(1, Ordering::AcqRel);
            self.storage.put_json("value", &json!(1))?;
            self.storage.sync().await?;
            self.resumed.fetch_add(1, Ordering::AcqRel);
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

fn stage(rooted: bool, durability: Durability, phase: &str) {
    eprintln!(
        "CHECKPOINT_PROOF_STAGE {}",
        json!({"rooted":rooted,"durability":format!("{durability:?}"),"phase":phase})
    );
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
        "checkpoint proof qualification: {failures:?}"
    );
    Ok(())
}

async fn run(rooted: bool, durability: Durability) -> Result<()> {
    let store = Arc::new(catalog_fault::Store::default());
    let result = run_with_store(rooted, durability, store.clone()).await;
    managed_fixture::memory::preserve_result(&store.inner, "checkpoint-proof", result).await
}

async fn run_with_store(
    rooted: bool,
    durability: Durability,
    store: Arc<catalog_fault::Store>,
) -> Result<()> {
    let first_poll = Arc::new(Gate::default());
    let query_gate = Arc::new(Gate::default());
    let calls = Arc::new(AtomicUsize::new(0));
    let resumed = Arc::new(AtomicUsize::new(0));
    let holder_ready = Arc::new(Notify::new());
    let holder_start = Arc::new(Notify::new());
    let holder_calls = Arc::new(AtomicUsize::new(0));
    let class = RaftClass::new("Counter", 1, [211; 32])?
        .initializer([212; 32], |_, tx| {
            tx.storage()?.put_json("value", &json!(0))
        })?
        .json_cell_rpc("Run", [213; 32])?
        .json_cell_rpc("HoldLater", [215; 32])?
        .query("Get", [214; 32], |query, _| {
            Ok(serde_json::to_vec(&query.storage().get_json("value")?)?)
        })?;
    let poll_gate = first_poll.clone();
    let invoked = calls.clone();
    let continuations = resumed.clone();
    let holding_ready = holder_ready.clone();
    let holding_start = holder_start.clone();
    let holding_gate = query_gate.clone();
    let holding_calls = holder_calls.clone();
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |context, _| {
                Ok(Arc::new(SyncCell {
                    storage: context.storage()?,
                    first_poll: poll_gate.clone(),
                    calls: invoked.clone(),
                    resumed: continuations.clone(),
                    holder_ready: holding_ready.clone(),
                    holder_start: holding_start.clone(),
                    holder_gate: holding_gate.clone(),
                    holder_calls: holding_calls.clone(),
                }))
            },
        )?,
    );
    let configuration = configuration::options("checkpoint-proof-service");
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
        let source = cluster.hosts[0].cell(CellHistory { cluster: group.cluster, namespace: group.namespace, cell: CellId::new("Counter:checkpoint-proof-source")?, incarnation: IncarnationId::new([216; 16])? })?;
        let other = cluster.hosts[0].cell(CellHistory { cell: CellId::new("Counter:checkpoint-proof-other")?, incarnation: IncarnationId::new([217; 16])?, ..source.history().clone() })?;
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
        let run = source.contract("Run")?;
        let hold = other.contract("HoldLater")?;
        let options = interleaving::command(220, 1, durability)?;
        let hold_options = interleaving::command(221, 1, durability)?;
        let completed = std::sync::atomic::AtomicBool::new(false);
        // Admit this invocation while routing is ready. Its first await leaves
        // the SQL worker free for the checkpoint's physical apply preparation.
        // Waking it later requires no new query admission behind the held fence.
        let (held, source_work) = tokio::join!(
            async {
                let result = other.command(&hold, b"{}".to_vec(), hold_options.clone()).await;
                if let Err(error) = &result { eprintln!("CHECKPOINT_PROOF_HOLDER_RESPONSE {}", json!({"ambiguous":error.ambiguous,"command":format!("{:?}",error.command),"error":format!("{error:#}")})); }
                result
            },
            async {
                cellar::asyncrt::timeout(TIMEOUT, holder_ready.notified()).await.context("unrelated invocation admitted before checkpoint")?;
                stage(rooted, durability, "holder-admitted");
                let before = cluster.hosts[0].group(group)?.status().await?.applied;
                stage(rooted, durability, "initial-drain");
                let initial = cluster.hosts[0].performance();
                ensure!(initial.available && !initial.saturated, "proof observations unavailable");
                let preparation = initial.operations.iter().find(|op| op.name == "sql_prepare")
                    .context("apply preparation observation")?;
                ensure!(preparation.started == preparation.succeeded, "initial apply has not drained");
                let prepared_before = preparation.succeeded;
                let proof_before = initial.operations.iter().find(|op| op.name == "cell_checkpoint_proof")
                    .context("checkpoint proof observation")?.succeeded;
                let (result, observed) = tokio::join!(
                    async {
                        let result = source.command(&run, b"{}".to_vec(), options.clone()).await;
                        if let Err(error) = &result { eprintln!("CHECKPOINT_PROOF_RESPONSE {}", json!({"ambiguous":error.ambiguous,"command":format!("{:?}",error.command),"error":format!("{error:#}")})); }
                        completed.store(true, Ordering::Release);
                        result
                    },
                    async {
                        let _release_poll = Release(first_poll.clone());
                        let _release_holder = Release(query_gate.clone());
                        cellar::asyncrt::timeout(TIMEOUT, first_poll.entered.notified()).await.context("first source poll")?;
                        stage(rooted, durability, "source-first-poll");
                        // Own selector cleanup before retaining the host. Arm
                        // only after its complete source/phase selector exists.
                        let release_log = log_gate.arm(storage_fault::OFF);
                        *log_gate.log_sync_prefix.lock().unwrap() = Some(cluster.nodes[0].info().data_dir.clone());
                        let source_host = cluster.hosts[0].clone();
                        *log_gate.sync_when.lock().unwrap() = Some(Arc::new(move || {
                            let observation = source_host.performance();
                            observation.available && !observation.saturated && observation.operations.iter()
                                .any(|op| op.name == "sql_prepare" && op.succeeded > prepared_before)
                        }));
                        log_gate.mode.store(storage_fault::HOLD_SYNC, Ordering::Release);
                        first_poll.release();
                        cellar::asyncrt::timeout(TIMEOUT, log_gate.entered.notified()).await.context("source engine fence after checkpoint apply preparation")?;
                        stage(rooted, durability, "source-engine-held-after-apply");
                        holder_start.notify_one();
                        cellar::asyncrt::timeout(TIMEOUT, query_gate.entered.notified()).await.context("admitted holder occupies SQL before root publication")?;
                        stage(rooted, durability, "sql-owner-held");
                        ensure!(cluster.hosts[0].group(group)?.status().await?.applied == before,
                            "checkpoint applied before its selected engine fence was released");
                        ensure!(!completed.load(Ordering::Acquire) && resumed.load(Ordering::Acquire) == 0,
                            "checkpoint continued or acknowledged before root publication");
                        drop(release_log);
                        stage(rooted, durability, "source-engine-released");
                        let progress = cellar::asyncrt::timeout(Duration::from_secs(1), async {
                            loop {
                                let applied = cluster.hosts[0].group(group)?.status().await?.applied;
                                let observation = cluster.hosts[0].performance();
                                ensure!(observation.available && !observation.saturated, "proof observations unavailable");
                                let proof = observation.operations.iter().find(|op| op.name == "cell_checkpoint_proof")
                                    .context("checkpoint proof observation")?;
                                if applied.index > before.index && proof.succeeded > proof_before
                                    && read(&peers[0], 222).await? == 1 && read(&peers[1], 223).await? == 1 {
                                    return Ok::<_, anyhow::Error>(());
                                }
                                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                            }
                        }).await;
                        let proven = matches!(progress, Ok(Ok(())));
                        stage(rooted, durability, "proof-progress-window-ended");
                        ensure!(!completed.load(Ordering::Acquire) && resumed.load(Ordering::Acquire) == 0,
                            "proof observation resumed or acknowledged while SQL owner was held");
                        query_gate.release();
                        Ok::<_, anyhow::Error>(proven)
                    },
                );
                Ok::<_, anyhow::Error>((result, observed))
            },
        );
        let (result, observed) = source_work?;
        if let Err(error) = &observed {
            eprintln!("CHECKPOINT_PROOF_OBSERVER_FAILURE {error:#}");
        }
        let receipt = result?;
        let held = held?;
        ensure!(held.result == b"0" && held.durability == durability);
        ensure!(receipt.result == b"2" && receipt.durability == durability);
        let replay = source.command(&run, b"{}".to_vec(), options).await;
        if let Err(error) = &replay { eprintln!("CHECKPOINT_PROOF_REPLAY_RESPONSE {}", json!({"ambiguous":error.ambiguous,"command":format!("{:?}",error.command),"error":format!("{error:#}")})); }
        let replay = replay?;
        ensure!(replay.id == receipt.id && replay.result == receipt.result && replay.durability == durability && replay.version.history == receipt.version.history && replay.version.sequence.get() >= receipt.version.sequence.get());
        ensure!(calls.load(Ordering::Acquire) == 1 && resumed.load(Ordering::Acquire) == 1
            && read(&source, 224).await? == 2);
        let held_replay = other.command(&hold, b"{}".to_vec(), hold_options).await?;
        ensure!(held_replay.id == held.id && held_replay.result == held.result
            && held_replay.durability == held.durability && holder_calls.load(Ordering::Acquire) == 1
            && read(&other, 225).await? == 0);
        stage(rooted, durability, "final-replay");
        observed
    }).await.map_err(anyhow::Error::from).and_then(std::convert::identity);
    first_poll.release();
    query_gate.release();
    let joined =
        managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
            .await;
    for root in std::mem::take(&mut cluster.roots) {
        eprintln!("retained checkpoint proof disk: {}", root.keep().display());
    }
    if let Err(error) = &joined {
        eprintln!("checkpoint proof joined cleanup: {error:#}");
    }
    eprintln!(
        "CHECKPOINT_PROOF_PROGRESS {}",
        json!({"rooted":rooted,"durability":format!("{durability:?}"),"proofObservedWhileSqlHeld":work.as_ref().ok(),"valuesAndReplayVerified":work.is_ok(),"joinedClose":joined.is_ok()})
    );
    cluster.fixtures[0].domain.shutdown().await;
    let proof_observed = work?;
    joined?;
    ensure!(
        proof_observed,
        "checkpoint proof did not progress before unrelated SQL finished"
    );
    Ok(())
}
