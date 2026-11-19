// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! An admitted callback keeps its exact checkpoint while S3 output slots are held.

use super::*;

#[derive(Clone, Copy, Debug)]
pub enum End {
    Release,
    Deadline,
    Cancel,
    Close,
}

struct CheckpointCell {
    storage: Storage,
    entered: Arc<Notify>,
    calls: Arc<AtomicUsize>,
    continued: Arc<AtomicUsize>,
}

impl Cell for CheckpointCell {
    fn invoke(self: Arc<Self>, _: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc { method, .. } = invocation else {
                anyhow::bail!("unexpected checkpoint invocation")
            };
            if method == "Checkpoint" {
                self.calls.fetch_add(1, Ordering::AcqRel);
                self.storage.put_json("value", &json!(1))?;
                self.entered.notify_one();
                self.storage.sync().await?;
                self.continued.fetch_add(1, Ordering::AcqRel);
            } else {
                ensure!(method == "HoldProof");
            }
            Ok(Output::Rpc(RpcData::Json(
                self.storage.get_json("value")?.unwrap().to_string(),
            )))
        })
    }
}

fn options(sequence: u64) -> Result<CommandOptions> {
    let mut options = CommandOptions::new(
        CommandId::new(ClientId::new([236; 16])?, sequence)?,
        u64::try_from(cellar::asyncrt::wall_ms())? + 600_000,
    );
    options.durability = Durability::S3;
    Ok(options)
}

fn expired(error: &CellDispatchError) -> bool {
    match error {
        CellDispatchError::Runtime(RuntimeError::DeadlineExceeded) => true,
        CellDispatchError::Execution(ExecutionError::Storage(error)) => error
            .downcast_ref::<CellDispatchError>()
            .is_some_and(expired),
        _ => false,
    }
}

async fn retired(host: &RaftCells) -> Result<()> {
    cellar::asyncrt::timeout(TIMEOUT, async {
        loop {
            let usage = host.resource_usage();
            if usage.sql.as_ref().is_some_and(|sql| sql.active_cells == 0)
                && usage.host.controller.archive_settlement.items == 1
            {
                break;
            }
            cellar::asyncrt::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("checkpoint retained admission after its waiter ended")
}

pub async fn qualify(rooted: bool, real: bool, end: End) -> Result<()> {
    let entered = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let continued = Arc::new(AtomicUsize::new(0));
    let class = RaftClass::new("Counter", 1, [230; 32])?
        .initializer([231; 32], |_, tx| {
            tx.storage()?.put_json("value", &json!(0))
        })?
        .json_cell_rpc("HoldProof", [232; 32])?
        .json_cell_rpc("Checkpoint", [233; 32])?
        .query("Read", [234; 32], |query, _| {
            Ok(query
                .storage()
                .get_json("value")?
                .unwrap()
                .to_string()
                .into_bytes())
        })?;
    let captured = (entered.clone(), calls.clone(), continued.clone());
    let app = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |context, _| {
                Ok(Arc::new(CheckpointCell {
                    storage: context.storage()?,
                    entered: captured.0.clone(),
                    calls: captured.1.clone(),
                    continued: captured.2.clone(),
                }))
            },
        )?,
    );
    let store = if real {
        Arc::new(catalog_fault::Store::around(rustfs::store(
            &format!("checkpoint-admission-{end:?}"),
            rooted,
        )?))
    } else {
        Arc::new(catalog_fault::Store::default())
    };
    let declared = configuration::options("checkpoint-admission-service");
    app.validate_deployment(&configuration::deployment(declared.clone()))?;
    let mut fixture = Fixture::with_application(rooted, "Counter", app, store).await?;
    fixture.configuration = Some(declared);
    fixture.sql_workers = 1;
    fixture.ordinary_admission = Some((2, 64 * 1024));
    let roots = (0..3)
        .map(|_| tempfile::tempdir())
        .collect::<std::io::Result<Vec<_>>>()?;
    let mut cluster = Box::pin(ManagedCluster::start_with_roots(fixture, roots)).await?;
    let work = cluster.fixtures[0].domain.run(async {
        let leader = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
        let host = &cluster.hosts[leader];
        let history = CellHistory {
            cluster: cluster.history.cluster,
            namespace: cluster.history.namespace,
            cell: CellId::new("Counter:checkpoint")?,
            incarnation: IncarnationId::new([235; 16])?,
        };
        let source = host.cell(history.clone())?;
        let holder = host.cell(CellHistory {
            cell: CellId::new("Counter:proof-holder")?,
            ..history
        })?;
        for (cell, creation) in [(&source, 237), (&holder, 238)] {
            cell.enroll(CellEnrollmentRequest {
                creation: ProposalId::new([creation; 16])?,
                durability: Durability::Quorum,
            }, TIMEOUT).await?;
        }
        let hold = holder.contract("HoldProof")?;
        for sequence in [1, 2] {
            holder.command(&hold, b"{}".to_vec(), options(sequence)?).await?;
        }
        // The SDK's copied command value releases its output proof. Receipt
        // observations deliberately retain their physical S3 proof through
        // caller consumption, so hold those to fill the real two-slot bank.
        let request_for = |sequence| -> Result<_> {
            Ok(CellReceiptRequest {
                id: options(sequence)?.id,
                contract: hold.clone(),
                input: b"{}".to_vec(),
                durability: Durability::S3,
            })
        };
        let first = holder.receipt(request_for(1)?, TIMEOUT).await?;
        let second = holder.receipt(request_for(2)?, TIMEOUT).await?;
        ensure!(first.receipt().is_some() && second.receipt().is_some());
        let usage = host.resource_usage().host.controller;
        ensure!(usage.archive_settlement.items == 2 && usage.archive_settlement.max_items == 2);
        eprintln!("checkpoint admission: extents={rooted} end={end:?} held_proofs=2");
        let request = options(1)?;
        let checkpoint = source.contract("Checkpoint")?;
        let mut pending = Box::pin(source.command(&checkpoint, b"{}".to_vec(), request.clone()));
        let observed = async {
            cellar::asyncrt::timeout(TIMEOUT, entered.notified()).await?;
            loop {
                let response = source.query(ProposalId::new([239; 16])?, &source.contract("Read")?, vec![], QueryOptions::default()).await?;
                if response.result == b"1" {
                    break;
                }
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, anyhow::Error>(())
        };
        cellar::asyncrt::select! {
            result = &mut pending => anyhow::bail!("admitted checkpoint abandoned S3 capacity before release: {result:?}"),
            result = cellar::asyncrt::timeout(TIMEOUT, observed) => { result??; }
        }
        ensure!(calls.load(Ordering::Acquire) == 1 && continued.load(Ordering::Acquire) == 0);
        ensure!(host.resource_usage().host.controller.archive_settlement.items == 2);
        eprintln!("checkpoint admission: partial root committed, callback_calls=1 continued=0");
        match end {
            End::Release => {
                drop(first);
                let result = pending.await.context("held checkpoint after S3 capacity release")?;
                ensure!(result.result == b"1" && result.durability == Durability::S3);
                drop(result);
                let replay = source.command(&checkpoint, b"{}".to_vec(), request).await?;
                ensure!(replay.result == b"1" && replay.durability == Durability::S3);
                ensure!(calls.load(Ordering::Acquire) == 1 && continued.load(Ordering::Acquire) == 1, "checkpoint retry replayed native code");
                drop(replay);
            }
            End::Deadline => {
                let error = pending.await.expect_err("checkpoint escaped its original deadline");
                ensure!(error.ambiguous, "committed intermediate root became known non-commit");
                ensure!(expired(error.cause.as_ref()), "checkpoint lost its typed original deadline: {error}");
                ensure!(continued.load(Ordering::Acquire) == 0);
                drop(first);
                retired(host).await?;
                ensure!(continued.load(Ordering::Acquire) == 0, "expired checkpoint resumed after healing");
            }
            End::Cancel => {
                drop(pending);
                drop(first);
                retired(host).await?;
                ensure!(continued.load(Ordering::Acquire) == 0, "cancelled callback resumed");
            }
            End::Close => {
                let (closed, refused) = tokio::join!(host.close(), async {
                    let result = pending.await;
                    ensure!(result.is_err(), "closing owner produced S3 output");
                    ensure!(continued.load(Ordering::Acquire) == 0, "closed callback resumed");
                    // Consumer proofs can outlive the node. Release them only
                    // after its admitted callback has refused continuation.
                    drop(first);
                    drop(second);
                    Ok::<_, anyhow::Error>(())
                });
                closed?;
                refused?;
                return Ok(());
            }
        }
        drop(second);
        Ok::<_, anyhow::Error>(())
    }).await.map_err(anyhow::Error::from).and_then(std::convert::identity);
    let joined =
        managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
            .await;
    if work.is_err() || joined.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!(
                "retained checkpoint admission disk: {}",
                root.keep().display()
            );
        }
    }
    cluster.fixtures[0].domain.shutdown().await;
    work?;
    joined
}
