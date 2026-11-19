// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A live S3 invocation coalesces intermediate same-group background cuts.

use super::*;

#[derive(Default)]
struct Control {
    entered: Notify,
    release: Notify,
    finished: Notify,
    calls: AtomicUsize,
}

struct Release(Arc<Control>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.release.notify_one();
    }
}

struct Counter(Storage, Arc<Control>);

impl Cell for Counter {
    fn invoke(self: Arc<Self>, _: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc { method, .. } = invocation else {
                anyhow::bail!("RPC required")
            };
            self.0.put_json("value", &json!(1))?;
            if method == "Run" {
                self.1.calls.fetch_add(1, Ordering::AcqRel);
                self.0.sync().await?;
                self.1.entered.notify_one();
                self.1.release.notified().await;
                self.0.put_json("value", &json!(2))?;
            } else {
                ensure!(method == "Touch");
            }
            Ok(Output::Rpc(RpcData::Json(
                self.0.get_json("value")?.unwrap().to_string(),
            )))
        })
    }
}

fn options(sequence: u64, durability: Durability) -> Result<CommandOptions> {
    let mut options = CommandOptions::new(
        CommandId::new(ClientId::new([246; 16])?, sequence)?,
        u64::try_from(cellar::asyncrt::wall_ms())? + 60_000,
    );
    options.durability = durability;
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

pub async fn qualify(rooted: bool, real: bool, expire: bool) -> Result<()> {
    let control = Arc::new(Control::default());
    let class = RaftClass::new("Counter", 1, [240; 32])?
        .initializer([241; 32], |_, tx| {
            tx.storage()?.put_json("value", &json!(0))
        })?
        .json_cell_rpc("Run", [242; 32])?
        .json_cell_rpc("Touch", [243; 32])?
        .query("Read", [244; 32], |q, _| {
            Ok(q.storage()
                .get_json("value")?
                .unwrap()
                .to_string()
                .into_bytes())
        })?;
    let captured = control.clone();
    let app = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
            .raft_cell(class, move |context, _| {
                Ok(Arc::new(Counter(context.storage()?, captured.clone())))
            })?,
    );
    let store = Arc::new(if real {
        catalog_fault::Store::around(rustfs::store(&format!("s3-foreground-{expire}"), rooted)?)
    } else {
        catalog_fault::Store::default()
    });
    let declared = configuration::options("s3-foreground-service");
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
        let node = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
        let host = &cluster.hosts[node];
        let group = host.group(cluster.history)?;
        let source = host.cell(CellHistory {
            cluster: cluster.history.cluster,
            namespace: cluster.history.namespace,
            cell: CellId::new("Counter:s3-foreground")?,
            incarnation: IncarnationId::new([245; 16])?,
        })?;
        let neighbor = host.cell(CellHistory {
            cell: CellId::new("Counter:s3-neighbor")?,
            ..source.history().clone()
        })?;
        for (cell, id) in [(&source, 247), (&neighbor, 248)] {
            cell.enroll(CellEnrollmentRequest {
                creation: ProposalId::new([id; 16])?,
                durability: Durability::Quorum,
            }, TIMEOUT).await?;
        }
        let request = options(1, Durability::S3)?;
        let contract = source.contract("Run")?;
        let mut changed = None;
        let (result, observed) = futures_util::future::join(
            async {
                let result = source.command(&contract, b"{}".to_vec(), request.clone()).await;
                control.finished.notify_one();
                result
            },
            async {
                let _release = Release(control.clone());
                cellar::asyncrt::timeout(TIMEOUT, control.entered.notified()).await?;
                let published = group.archive_status()?.and_then(|s| s.published)
                    .context("S3 checkpoint has no published cut")?.position().index;
                let receipt = neighbor.command(&neighbor.contract("Touch")?, b"{}".to_vec(), options(1, Durability::Quorum)?).await?;
                let index = receipt.witness.position.index;
                ensure!(index > published, "neighbor did not advance the group");
                changed = Some(index);
                let until = cellar::asyncrt::mono_ms() + 1000;
                while cellar::asyncrt::mono_ms() < until {
                    let current = group.archive_status()?.and_then(|s| s.published)
                        .context("published cut disappeared")?.position().index;
                    ensure!(current == published,
                        "background archived an intermediate prefix during a live S3 invocation: {published:?} -> {current:?}");
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
                if expire {
                    cellar::asyncrt::timeout(TIMEOUT, control.finished.notified()).await?;
                }
                Ok::<_, anyhow::Error>(())
            },
        ).await;
        observed?;
        if expire {
            let error = result.expect_err("parked S3 invocation escaped its original deadline");
            ensure!(error.ambiguous, "committed checkpoint became known non-commit");
            ensure!(expired(error.cause.as_ref()), "original typed deadline was lost: {error}");
        } else {
            let result = result?;
            ensure!(result.result == b"2" && result.durability == Durability::S3);
            let replay = source.command(&contract, b"{}".to_vec(), request).await?;
            ensure!(replay.result == b"2" && control.calls.load(Ordering::Acquire) == 1);
        }
        // Drop or original deadline expiry must unblock existing dirty work.
        // The observer performs no mutation or S3 read to create demand.
        let changed = if expire {
            changed.context("neighbor did not commit")?
        } else {
            neighbor.command(&neighbor.contract("Touch")?, b"{}".to_vec(), options(2, Durability::Quorum)?).await?.witness.position.index
        };
        cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                if group.archive_status()?.and_then(|s| s.published)
                    .is_some_and(|m| m.position().index >= changed) { break; }
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, anyhow::Error>(())
        }).await??;
        let observed = source.query(ProposalId::new([249; 16])?, &source.contract("Read")?, vec![], QueryOptions::default()).await?;
        ensure!(observed.result == if expire { b"1" } else { b"2" });
        ensure!(control.calls.load(Ordering::Acquire) == 1);
        eprintln!("S3 foreground RustFS={real} extents={rooted} expire={expire}: PASS");
        Ok::<_, anyhow::Error>(())
    }).await.map_err(anyhow::Error::from).and_then(std::convert::identity);
    let joined =
        managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
            .await;
    if work.is_err() || joined.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!("retained S3 foreground disk: {}", root.keep().display());
        }
    }
    cluster.fixtures[0].domain.shutdown().await;
    work?;
    joined
}
