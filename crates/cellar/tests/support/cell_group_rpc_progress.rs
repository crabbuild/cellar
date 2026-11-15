// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A proven callback is independent of later apply work for another cell.

use super::*;
use sha2::{Digest, Sha256};

#[path = "raft_ltx_fault.rs"]
#[allow(dead_code)]
mod physical;

struct PausedCell {
    storage: Storage,
    gates: Arc<[RpcGate; 2]>,
    calls: Arc<AtomicUsize>,
}

impl Cell for PausedCell {
    fn invoke(self: Arc<Self>, _: Context, _: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::AcqRel);
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

pub async fn run(rooted: bool, store: Arc<catalog_fault::Store>) -> Result<()> {
    let calls = Arc::new(AtomicUsize::new(0));
    let gates: Arc<[RpcGate; 2]> = Arc::new(std::array::from_fn(|_| RpcGate::default()));
    let class = RaftClass::new("Counter", 1, [181; 32])?
        .initializer([182; 32], |_, tx| {
            tx.execute_batch("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(0)")?;
            tx.storage()?.put_json("value", &json!(0))
        })?
        .json_cell_rpc("Run", [183; 32])?
        .command("Add", [184; 32], |context, _| {
            update(&context.storage()?, 2)?;
            Ok(b"2".to_vec())
        })?;
    let called = calls.clone();
    let waiting = gates.clone();
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |context, _| {
                Ok(Arc::new(PausedCell {
                    storage: context.storage()?,
                    gates: waiting.clone(),
                    calls: called.clone(),
                }))
            },
        )?,
    );
    let mut fixture = Fixture::with_application(rooted, "Counter", application, store).await?;
    // Group apply and the source callback must use different workers: this
    // test holds synchronous physical I/O, not the whole SQL executor.
    fixture.sql_workers = 2;
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
    let faults = physical::Faults::new(pages, None);
    fixture.filesystem = Arc::new(physical::FaultFileSystem {
        faults: faults.clone(),
    });
    let mut cluster = Box::pin(ManagedCluster::start_with_roots(fixture, roots)).await?;
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
                u64::from_le_bytes(group_hash[..8].try_into()?) % 2
                    != u64::from_le_bytes(source_hash[..8].try_into()?) % 2,
                "physical gate must not occupy source worker"
            );
            let source = cluster.hosts[0].cell(source_history.clone())?;
            let other = cluster.hosts[0].cell(CellHistory {
                cell: CellId::new("Counter:unrelated")?,
                ..source_history
            })?;
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
            let add = other.contract("Add")?;
            let (result, observed) = tokio::join!(
                source.command(&run, b"{}".to_vec(), options.clone()),
                async {
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
                            vec![],
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
    joined
}
