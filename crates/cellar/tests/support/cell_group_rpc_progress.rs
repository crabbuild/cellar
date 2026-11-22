// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A proven callback is independent of later apply work for another cell.

use super::*;
use sha2::{Digest, Sha256};
use std::sync::Condvar;

#[path = "raft_ltx_fault.rs"]
#[allow(dead_code)]
pub(super) mod physical;

type ArmPreparation = Arc<Mutex<Option<Box<dyn FnOnce() + Send>>>>;

struct PausedCell {
    storage: Storage,
    preparation: ArmPreparation,
    gates: Arc<[RpcGate; 2]>,
    calls: Arc<AtomicUsize>,
    captures: Arc<AtomicUsize>,
    sql_thread: Arc<Mutex<Option<std::thread::ThreadId>>>,
}

impl Cell for PausedCell {
    fn invoke(self: Arc<Self>, _: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            if matches!(invocation, Invocation::CellRpc { ref method, .. } if method == "Prepare") {
                self.calls.fetch_add(1, Ordering::AcqRel);
                let value = update(&self.storage, 1)?;
                self.gates[0].entered.notify_one();
                self.gates[0].release.notified().await;
                return Ok(Output::Rpc(RpcData::Json(json!(value).to_string())));
            }
            if matches!(invocation, Invocation::CellRpc { ref method, .. } if method == "Add") {
                self.captures.fetch_add(1, Ordering::AcqRel);
                let value = update(&self.storage, 2)?;
                arm_preparation(&self.preparation);
                return Ok(Output::Rpc(RpcData::Json(json!(value).to_string())));
            }
            if matches!(invocation, Invocation::CellRpc { ref method, .. } if method == "Sync") {
                self.captures.fetch_add(1, Ordering::AcqRel);
                let value = update(&self.storage, 2)?;
                arm_preparation(&self.preparation);
                self.storage.sync().await?;
                return Ok(Output::Rpc(RpcData::Json(json!(value).to_string())));
            }
            self.calls.fetch_add(1, Ordering::AcqRel);
            *self.sql_thread.lock().unwrap() = Some(std::thread::current().id());
            update(&self.storage, 1)?;
            self.storage.sync().await?;
            self.gates[0].entered.notify_one();
            self.gates[0].release.notified().await;
            ensure!(self.storage.get_json("value")? == Some(json!(1)));
            self.gates[1].entered.notify_one();
            self.gates[1].release.notified().await;
            let value = update(&self.storage, 10)?;
            Ok(Output::Rpc(RpcData::Json(json!(value).to_string())))
        })
    }
}

struct Release(Option<std::sync::mpsc::SyncSender<()>>);

impl Drop for Release {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

fn arm_preparation(preparation: &ArmPreparation) {
    let arm = preparation.lock().unwrap().take();
    if let Some(arm) = arm {
        arm();
    }
}

fn hold_private_preparation(
    faults: Arc<physical::Faults>,
    preparation: &ArmPreparation,
) -> (Release, tokio::sync::oneshot::Receiver<()>) {
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, held) = std::sync::mpsc::sync_channel(1);
    // Opening an extent workspace uses the same inventory directory. Arm only
    // after SQL has run so that this gate selects immutable image persistence.
    *preparation.lock().unwrap() = Some(Box::new(move || {
        *faults.before_directory_sync.lock().unwrap() = Some(Box::new(move || {
            let _ = entered.send(());
            let _ = held.recv_timeout(Duration::from_secs(15));
        }));
    }));
    (Release(Some(release)), started)
}

#[derive(Default)]
struct ReadGate {
    entered: Notify,
    released: Mutex<bool>,
    wake: Condvar,
}

impl ReadGate {
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

struct ReleaseRead(Arc<ReadGate>);

impl Drop for ReleaseRead {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct DenyReopen(Arc<std::sync::atomic::AtomicBool>);

impl Drop for DenyReopen {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

fn hold_apply(faults: &physical::Faults) -> (Release, tokio::sync::oneshot::Receiver<()>) {
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, held) = std::sync::mpsc::sync_channel(1);
    *faults.before_directory_sync.lock().unwrap() = Some(Box::new(move || {
        let _ = entered.send(());
        let _ = held.recv_timeout(Duration::from_secs(15));
    }));
    (Release(Some(release)), started)
}

pub async fn run(rooted: bool, store: Arc<catalog_fault::Store>) -> Result<()> {
    run_mode(rooted, store, Mode::Continuation).await
}

pub async fn checkpoint(rooted: bool, store: Arc<catalog_fault::Store>) -> Result<()> {
    run_mode(rooted, store, Mode::Checkpoint).await
}

pub async fn preparation(rooted: bool, store: Arc<catalog_fault::Store>) -> Result<()> {
    run_mode(rooted, store, Mode::Preparation).await
}

pub async fn preparation_one_worker(rooted: bool, store: Arc<catalog_fault::Store>) -> Result<()> {
    run_mode_with_workers(rooted, store, Mode::Preparation, 1).await
}

pub async fn private_preparation(
    rooted: bool,
    store: Arc<catalog_fault::Store>,
    checkpoint: bool,
) -> Result<()> {
    run_mode_with_workers(
        rooted,
        store,
        if checkpoint {
            Mode::CheckpointPreview
        } else {
            Mode::Preview
        },
        1,
    )
    .await
    .with_context(|| format!("private image rooted={rooted} checkpoint={checkpoint}"))
}

pub async fn registered_preparation(rooted: bool, store: Arc<catalog_fault::Store>) -> Result<()> {
    run_mode_with_workers(rooted, store, Mode::RegisteredPreview, 1).await
}

pub async fn cancelled_preparation(
    rooted: bool,
    store: Arc<catalog_fault::Store>,
    checkpoint: bool,
) -> Result<()> {
    run_mode_with_workers(
        rooted,
        store,
        if checkpoint {
            Mode::CancelledCheckpointPreview
        } else {
            Mode::CancelledPreview
        },
        1,
    )
    .await
    .with_context(|| format!("cancelled private image rooted={rooted} checkpoint={checkpoint}"))
}

enum Mode {
    Continuation,
    Checkpoint,
    Preparation,
    Preview,
    CheckpointPreview,
    RegisteredPreview,
    CancelledPreview,
    CancelledCheckpointPreview,
    FailedPreview,
    FailedCheckpointPreview,
}

pub async fn failed_preparation(rooted: bool, checkpoint: bool) -> Result<()> {
    run_mode_with_workers(
        rooted,
        Arc::new(catalog_fault::Store::default()),
        if checkpoint {
            Mode::FailedCheckpointPreview
        } else {
            Mode::FailedPreview
        },
        1,
    )
    .await
}

async fn run_mode(rooted: bool, store: Arc<catalog_fault::Store>, mode: Mode) -> Result<()> {
    run_mode_with_workers(rooted, store, mode, 2).await
}

async fn run_mode_with_workers(
    rooted: bool,
    store: Arc<catalog_fault::Store>,
    mode: Mode,
    workers: usize,
) -> Result<()> {
    let checkpoint = matches!(mode, Mode::Checkpoint);
    let preview = matches!(
        mode,
        Mode::Preview
            | Mode::CheckpointPreview
            | Mode::RegisteredPreview
            | Mode::CancelledPreview
            | Mode::CancelledCheckpointPreview
            | Mode::FailedPreview
            | Mode::FailedCheckpointPreview
    );
    let preparation: ArmPreparation = Arc::new(Mutex::new(None));
    let calls = Arc::new(AtomicUsize::new(0));
    let captures = Arc::new(AtomicUsize::new(0));
    let capture_calls = captures.clone();
    let gates: Arc<[RpcGate; 2]> = Arc::new(std::array::from_fn(|_| RpcGate::default()));
    let reader = Arc::new(ReadGate::default());
    let query_reader = reader.clone();
    let sql_thread = Arc::new(Mutex::new(None));
    let source_thread = sql_thread.clone();
    let cell_preparation = preparation.clone();
    let deny_reopen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let reopened = Arc::new(AtomicUsize::new(0));
    let probes = calls.clone();
    let class = RaftClass::new("Counter", 1, [181; 32])?
        .initializer([182; 32], |_, tx| {
            tx.execute_batch("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(0)")?;
            tx.storage()?.put_json("value", &json!(0))
        })?
        .json_cell_rpc("Run", [183; 32])?
        .json_cell_rpc("Add", [184; 32])?
        .json_cell_rpc("Prepare", [186; 32])?
        .json_cell_rpc("Sync", [187; 32])?
        .query("Probe", [188; 32], move |query, _| {
            probes.fetch_add(1, Ordering::AcqRel);
            Ok(query
                .storage()
                .get_json("value")?
                .context("probe value")?
                .to_string()
                .into_bytes())
        })?
        .query("Hold", [185; 32], move |_, _| {
            query_reader.hold();
            Ok(vec![])
        })?;
    let registered_preparation = preparation.clone();
    let class = if matches!(mode, Mode::RegisteredPreview) {
        class.command("Registered", [189; 32], move |context, _| {
            let value = update(&context.storage()?, 2)?;
            arm_preparation(&registered_preparation);
            Ok(value.to_string().into_bytes())
        })?
    } else {
        class
    };
    let called = calls.clone();
    let waiting = gates.clone();
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |context, _| {
                Ok(Arc::new(PausedCell {
                    storage: context.storage()?,
                    preparation: cell_preparation.clone(),
                    gates: waiting.clone(),
                    calls: called.clone(),
                    captures: capture_calls.clone(),
                    sql_thread: source_thread.clone(),
                }))
            },
        )?,
    );
    let declared = configuration::options("rpc-progress-service");
    application.validate_deployment(&configuration::deployment(declared.clone()))?;
    let mut fixture = Fixture::with_application(rooted, "Counter", application, store).await?;
    fixture.configuration = Some(declared);
    // The one-worker variant requires physical group apply to leave SQL
    // available. Existing variants retain their original two-worker profile.
    fixture.sql_workers = workers;
    if checkpoint {
        // The physical worker gate is a third, fully admitted request beside
        // the two commands. Keep the original byte bank and all public profiles.
        fixture.ordinary_admission = Some((3, 64 * 1024));
    }
    let roots = (0..3)
        .map(|_| tempfile::tempdir())
        .collect::<std::io::Result<Vec<_>>>()?;
    let pages = roots[0]
        .path()
        .join("raft-cells/groups")
        .join(hex::encode(fixture.declaration.namespace.cluster.bytes()))
        .join(hex::encode(fixture.declaration.namespace.namespace.bytes()))
        .join(hex::encode([3; 16]))
        .join(hex::encode([7; 16]))
        .join("pages");
    let faults = physical::Faults::new(
        if preview {
            roots[0].path().join(if rooted {
                "raft-cells/extent-inventory"
            } else {
                "raft-cells/images"
            })
        } else {
            pages
        },
        None,
    );
    let images = roots[0].path().join("raft-cells/images");
    let forbidden = deny_reopen.clone();
    let observed_opens = reopened.clone();
    *faults.read_open.lock().unwrap() = Some(Arc::new(move |path| {
        if forbidden.load(Ordering::Acquire)
            && path.starts_with(&images)
            && *sql_thread.lock().unwrap() == Some(std::thread::current().id())
        {
            observed_opens.fetch_add(1, Ordering::AcqRel);
            return Err(std::io::Error::other("checkpoint reopened a proven image"));
        }
        Ok(())
    }));
    fixture.filesystem = Arc::new(physical::FaultFileSystem {
        faults: faults.clone(),
    });
    let mut cluster = Box::pin(ManagedCluster::start_with_roots(fixture, roots)).await?;
    let mut expected_close = None;
    let work = cluster.fixtures[0]
        .domain
        .run(async {
            let group = cluster.history;
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            if leader != 0 {
                let handle = cluster.hosts[leader].group(group)?;
                let status = handle.replication_status(TIMEOUT).await?;
                handle
                    .transfer_leadership(
                        LeadershipTransfer {
                            target: MemberId::new(1)?,
                            expected_term: status.term,
                            expected_revision: status.membership_revision,
                        },
                        TIMEOUT,
                    )
                    .await?;
            }
            cellar::asyncrt::timeout(TIMEOUT, async {
                while managed_fixture::leader(&cluster.hosts, group).await? != 0 {
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
            let source_history = CellHistory {
                cluster: group.cluster,
                namespace: group.namespace,
                cell: CellId::new("Counter:progress-0")?,
                incarnation: IncarnationId::new([185; 16])?,
            };
            let mut hash = Sha256::new();
            for id in [
                group.cluster.bytes(),
                group.namespace.bytes(),
                group.group.bytes(),
                group.incarnation.bytes(),
            ] {
                hash.update(id);
            }
            let group_hash: [u8; 32] = hash.finalize().into();
            let source_hash =
                cellar::sdk::consensus::verification::cell_groups::cell_image_history_digest(
                    &source_history,
                );
            ensure!(
                workers == 1 || u64::from_le_bytes(group_hash[..8].try_into()?) % workers as u64
                    != u64::from_le_bytes(source_hash[..8].try_into()?) % workers as u64,
                "physical gate must not occupy source worker"
            );
            let source = cluster.hosts[0].cell(source_history.clone())?;
            // Its command must also avoid the physically blocked source worker.
            let other_history = (0..64)
                .map(|n| CellHistory {
                    cell: CellId::new(format!("Counter:unrelated-{n}")).unwrap(),
                    ..source_history.clone()
                })
                .find(|history| {
                    let hash = cellar::sdk::consensus::verification::cell_groups::cell_image_history_digest(history);
                    u64::from_le_bytes(hash[..8].try_into().unwrap()) % workers as u64
                        == u64::from_le_bytes(group_hash[..8].try_into().unwrap()) % workers as u64
                })
                .context("unrelated worker")?;
            let other = cluster.hosts[0].cell(other_history)?;
            for (cell, id) in [(&source, 186), (&other, 187)] {
                cell.enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([id; 16])?,
                        durability: Durability::Quorum,
                    },
                    TIMEOUT,
                )
                .await?;
            }
            let options = interleaving::command(188, 1, Durability::Quorum)?;
            let run = source.contract("Run")?;
            let add = other.contract(match mode {
                Mode::CheckpointPreview | Mode::CancelledCheckpointPreview | Mode::FailedCheckpointPreview => "Sync",
                Mode::RegisteredPreview => "Registered",
                _ => "Add",
            })?;
            if matches!(mode, Mode::FailedPreview | Mode::FailedCheckpointPreview) {
                let fault = faults.clone();
                *preparation.lock().unwrap() = Some(Box::new(move || {
                    *fault.before_directory_sync.lock().unwrap() = Some(Box::new(|| {
                        panic!("injected RPC image capture task failure");
                    }));
                }));
                let error = other.command(&add, b"{}".to_vec(), interleaving::command(189, 1, Durability::Quorum)?).await.err().context("capture task failure acknowledged")?;
                ensure!(error.ambiguous && error.command == interleaving::command(189, 1, Durability::Quorum)?.id, "lost original capture identity: {error:?}");
                ensure!(matches!(error.cause.as_ref(), CellDispatchError::Execution(ExecutionError::Refused(CommandFailure::Unavailable))), "capture failure remained serving: {error:?}");
                ensure!(captures.load(Ordering::Acquire) == 1, "capture task failure replayed callback");
                let closed = cellar::asyncrt::timeout(TIMEOUT, cluster.hosts[0].close()).await?.err().context("fatal capture failure lost close error")?;
                ensure!(matches!(closed.controller.as_deref(), Some(CellGroupControllerError::Command(_))), "capture failure lost controller custody: {closed:?}");
                let sql = cluster.hosts[0].resource_usage().sql.context("closed SQL owner")?;
                ensure!(sql.running_workers == 0 && sql.work_reservations == 0 && sql.queue_closed, "capture failure retained SQL work: {sql:?}");
                expected_close = Some(closed);
                return Ok(());
            }
            if matches!(mode, Mode::CancelledPreview | Mode::CancelledCheckpointPreview) {
                let (release, started) = hold_private_preparation(faults.clone(), &preparation);
                let mut command = Box::pin(other.command(
                    &add, b"{}".to_vec(), interleaving::command(189, 1, Durability::Quorum)?,
                ));
                cellar::asyncrt::select! {
                    result = &mut command => anyhow::bail!("private command escaped persistence barrier: {result:?}"),
                    entered = cellar::asyncrt::timeout(TIMEOUT, started) => { entered??; }
                }
                drop(command);
                let status = cluster.hosts[0].group(group)?.status().await?;
                ensure!(status.applied.index == status.committed, "cancelled private image was submitted");
                let usage = cluster.hosts[0].resource_usage().sql.context("SQL custody")?;
                ensure!(usage.active_cells == 1 && usage.retained_bytes != 0, "cancellation released entered image I/O");
                let mut close = Box::pin(cluster.hosts[0].close());
                ensure!(cellar::asyncrt::timeout(Duration::from_millis(30), &mut close).await.is_err(), "host close detached entered image I/O");
                let usage = cluster.hosts[0].resource_usage().sql.context("closing SQL custody")?;
                ensure!(usage.active_cells == 1 && usage.retained_bytes != 0, "close released physical custody before join");
                drop(release);
                let report = cellar::asyncrt::timeout(TIMEOUT, close).await??;
                ensure!(report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers);
                return Ok(());
            }
            if preview {
                let (release, started) = hold_private_preparation(faults.clone(), &preparation);
                let finished = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let completed = finished.clone();
                let write_options = interleaving::command(189, 1, Durability::Quorum)?;
                let (written, read) = tokio::join!(
                    async {
                        let result = other.command(&add, b"{}".to_vec(), write_options.clone()).await;
                        completed.store(true, Ordering::Release);
                        result
                    },
                    async {
                        cellar::asyncrt::timeout(TIMEOUT, started).await??;
                        let status = cluster.hosts[0].group(group)?.status().await?;
                        ensure!(status.applied.index == status.committed, "private image reached Raft before its persistence barrier");
                        let response = cellar::asyncrt::timeout(Duration::from_secs(1), source.query(
                            ProposalId::new([193; 16])?, &source.contract("Probe")?, vec![], QueryOptions::default(),
                        )).await.context("SQL query blocked by unrelated private image preparation")??;
                        ensure!(response.result == b"0" && calls.load(Ordering::Acquire) == 1);
                        ensure!(!finished.load(Ordering::Acquire), "write completed before its private image persistence barrier");
                        drop(release);
                        Ok::<_, anyhow::Error>(())
                    },
                );
                read?;
                let written = written?;
                ensure!(written.result == b"2" && written.durability == Durability::Quorum);
                let replay = other.command(&add, b"{}".to_vec(), write_options).await?;
                ensure!(replay.version == written.version && replay.result == written.result);
                return Ok(());
            }
            if matches!(mode, Mode::Preparation) {
                let (release, started) = hold_apply(&faults);
                let (other_result, prepared) = tokio::join!(
                    other.command(
                        &add,
                        b"{}".to_vec(),
                        interleaving::command(189, 1, Durability::Quorum)?,
                    ),
                    async {
                        let observed = async {
                            cellar::asyncrt::timeout(TIMEOUT, started).await??;
                            let status = cluster.hosts[0].group(group)?.status().await?;
                            ensure!(status.applied.index < status.committed, "held unrelated committed apply");
                            let contract = source.contract("Prepare")?;
                            let mut command = Box::pin(source.command(&contract, b"{}".to_vec(), options.clone()));
                            cellar::asyncrt::select! {
                                result = &mut command => anyhow::bail!("command completed before preparation: {result:?}"),
                                result = cellar::asyncrt::timeout(Duration::from_secs(1), gates[0].entered.notified()) => {
                                    result.context("new cell preparation blocked by unrelated committed apply")?;
                                }
                            }
                            ensure!(calls.load(Ordering::Acquire) == 1);
                            let status = cluster.hosts[0].group(group)?.status().await?;
                            ensure!(status.applied.index < status.committed, "preparation waited for group catch-up");
                            gates[0].release.notify_one();
                            ensure!(
                                cellar::asyncrt::timeout(Duration::from_millis(30), &mut command).await.is_err(),
                                "private preparation acknowledged before contiguous root publication"
                            );
                            drop(release);
                            let result = command.await?;
                            ensure!(result.durability == Durability::Quorum && result.result == b"1");
                            let replay = source.command(&contract, b"{}".to_vec(), options.clone()).await?;
                            ensure!(replay.version == result.version && replay.result == result.result);
                            ensure!(calls.load(Ordering::Acquire) == 1, "replay reran preparation");
                            Ok::<_, anyhow::Error>(())
                        }.await;
                        gates[0].release.notify_one();
                        observed
                    },
                );
                prepared?;
                other_result?;
                return Ok(());
            }
            let initial_apply = checkpoint.then(|| hold_apply(&faults));
            let (result, observed) = tokio::join!(
                source.command(&run, b"{}".to_vec(), options.clone()),
                async {
                    if let Some((release, started)) = initial_apply {
                        // Hold the source worker on an immutable stale query while
                        // its own checkpoint becomes applied. No callback poll can
                        // race ahead and obtain whole-group execution authority.
                        cellar::asyncrt::timeout(TIMEOUT, started).await??;
                        let (query, observed) = tokio::join!(
                            source.query(
                                ProposalId::new([190; 16])?,
                                &source.contract("Hold")?,
                                vec![],
                                QueryOptions {
                                    consistency: QueryConsistency::Stale,
                                    routing: QueryRouting::Local,
                                    ..Default::default()
                                },
                            ),
                            async {
                                let _release_read = ReleaseRead(reader.clone());
                                cellar::asyncrt::timeout(TIMEOUT, reader.entered.notified()).await?;
                                drop(release);
                                cellar::asyncrt::timeout(TIMEOUT, async {
                                    loop {
                                        let status = cluster.hosts[0].group(group)?.status().await?;
                                        if status.ready && status.applied.index == status.committed {
                                            break Ok::<_, anyhow::Error>(());
                                        }
                                        cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                                    }
                                }).await??;
                                let (release, started) = hold_apply(&faults);
                                let (other_result, resumed) = tokio::join!(
                                    other.command(
                                        &add,
                                        b"{}".to_vec(),
                                        interleaving::command(189, 1, Durability::Quorum)?,
                                    ),
                                    async {
                                        let observed = async {
                                            cellar::asyncrt::timeout(TIMEOUT, started).await??;
                                            let status = cluster.hosts[0].group(group)?.status().await?;
                                            ensure!(status.applied.index < status.committed, "held unrelated committed apply");
                                            // The retained writer already contains this exact
                                            // captured checkpoint. Its settlement must not
                                            // open another materialized receipt reader.
                                            deny_reopen.store(!rooted, Ordering::Release);
                                            let _no_reader = DenyReopen(deny_reopen.clone());
                                            reader.release();
                                            cellar::asyncrt::timeout(
                                                Duration::from_secs(1),
                                                gates[0].entered.notified(),
                                            ).await.with_context(|| format!("applied checkpoint settlement stalled; immutable receipt reopens={}", reopened.load(Ordering::Acquire)))?;
                                            ensure!(reopened.load(Ordering::Acquire) == 0, "checkpoint reopened an immutable receipt reader");
                                            let status = cluster.hosts[0].group(group)?.status().await?;
                                            ensure!(status.applied.index < status.committed, "checkpoint waited for group catch-up");
                                            Ok::<_, anyhow::Error>(())
                                        }.await;
                                        drop(release);
                                        reader.release();
                                        gates[0].release.notify_one();
                                        gates[1].release.notify_one();
                                        observed
                                    },
                                );
                                other_result?;
                                resumed
                            },
                        );
                        reader.release();
                        observed?;
                        query.context("joined physical source query")?;
                        return Ok(());
                    }
                    cellar::asyncrt::timeout(TIMEOUT, gates[0].entered.notified()).await?;
                    let (entered, started) = tokio::sync::oneshot::channel();
                    let (release, held) = std::sync::mpsc::sync_channel(1);
                    let release = Release(Some(release));
                    *faults.before_directory_sync.lock().unwrap() = Some(Box::new(move || {
                        let _ = entered.send(());
                        let _ = held.recv_timeout(Duration::from_secs(15));
                    }));
                    let (other_result, resumed) = tokio::join!(
                        other.command(
                            &add,
                            b"{}".to_vec(),
                            interleaving::command(189, 1, Durability::Quorum)?
                        ),
                        async {
                            let observed = async {
                                cellar::asyncrt::timeout(TIMEOUT, started).await??;
                                let status = cluster.hosts[0].group(group)?.status().await?;
                                ensure!(
                                    status.applied.index < status.committed,
                                    "held committed tail"
                                );
                                gates[0].release.notify_one();
                                cellar::asyncrt::timeout(
                                    Duration::from_secs(1),
                                    gates[1].entered.notified(),
                                )
                                .await
                                .context("proven callback blocked by unrelated unapplied entry")?;
                                let status = cluster.hosts[0].group(group)?.status().await?;
                                ensure!(
                                    status.applied.index < status.committed,
                                    "callback waited for group catch-up"
                                );
                                Ok::<_, anyhow::Error>(())
                            }
                            .await;
                            drop(release);
                            gates[0].release.notify_one();
                            gates[1].release.notify_one();
                            observed
                        }
                    );
                    other_result?;
                    resumed
                }
            );
            observed?;
            let result = result?;
            ensure!(result.durability == Durability::Quorum && result.result == b"11");
            let replay = source.command(&run, b"{}".to_vec(), options).await?;
            ensure!(replay.result == result.result && replay.version == result.version);
            ensure!(
                calls.load(Ordering::Acquire) == 1,
                "receipt replay reran callback"
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
            eprintln!("retained RPC progress disk: {}", root.keep().display());
        }
    }
    cluster.fixtures[0].domain.shutdown().await;
    work?;
    if let Some(expected) = expected_close {
        let error = joined.err().context("fatal owner cleanup hid failure")?;
        ensure!(
            error
                .downcast_ref::<Arc<RaftCloseError>>()
                .is_some_and(|actual| Arc::ptr_eq(actual, &expected)),
            "cleanup lost original close failure: {error:#}"
        );
        Ok(())
    } else {
        joined
    }
}
