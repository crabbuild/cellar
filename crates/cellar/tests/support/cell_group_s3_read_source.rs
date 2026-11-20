// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A physically verified sync can cover unchanged dependent reads, never writes.

use super::*;
use std::sync::{atomic::AtomicBool, Condvar};

#[derive(Default)]
struct Gate {
    armed: AtomicBool,
    entered: Notify,
    released: Mutex<bool>,
    wake: Condvar,
}

impl Gate {
    fn arm(&self) {
        *self.released.lock().unwrap() = false;
        self.armed.store(true, Ordering::Release);
    }

    fn read(&self) {
        if self.armed.swap(false, Ordering::AcqRel) {
            self.entered.notify_one();
            let mut released = self.released.lock().unwrap();
            while !*released {
                released = self.wake.wait(released).unwrap();
            }
        }
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}

struct Release(Arc<Vec<Arc<Gate>>>);

impl Drop for Release {
    fn drop(&mut self) {
        for gate in self.0.iter() {
            gate.release();
        }
    }
}

struct Reader {
    storage: Storage,
    target: Arc<Mutex<Option<RaftCellHandle>>>,
}

impl Cell for Reader {
    fn invoke(self: Arc<Self>, _: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc {
                method,
                args: RpcData::Json(input),
                ..
            } = invocation
            else {
                anyhow::bail!("JSON read RPC required");
            };
            if method == "Bump" {
                self.storage.put_json("value", &json!(1))?;
                return Ok(Output::Rpc(RpcData::Json("1".into())));
            }
            ensure!(method == "Read", "read-source operation");
            let mode: u8 = serde_json::from_str(&input)?;
            ensure!(mode <= 3, "read-source mode");
            // This original sync must obtain its own physical S3 proof before
            // any target query. The final parent still needs a separate receipt.
            self.storage.sync().await?;
            let target = self.target.lock().unwrap().clone().context("read target")?;
            for index in 0..2 {
                if mode == 1 && index == 0 {
                    self.storage.put_json("value", &json!(11))?;
                } else if mode == 2 && index == 1 {
                    self.storage.put_json("value", &json!(22))?;
                }
                let selected = target
                    .query(
                        ProposalId::new([210 + mode * 2 + index; 16])?,
                        &target.contract("Blocked")?,
                        vec![index],
                        QueryOptions {
                            routing: QueryRouting::Local,
                            ..Default::default()
                        },
                    )
                    .await?;
                ensure!(
                    selected.result == if mode == 3 && index == 1 { b"1" } else { b"0" },
                    "target read changed"
                );
            }
            Ok(Output::Rpc(RpcData::Json("0".into())))
        })
    }
}

async fn run(rooted: bool, store: Arc<catalog_fault::Store>) -> Result<()> {
    let gates = Arc::new(vec![Arc::new(Gate::default()), Arc::new(Gate::default())]);
    let query_gates = gates.clone();
    let target = Arc::new(Mutex::new(None));
    let factory_target = target.clone();
    let class = RaftClass::new("Counter", 1, [241; 32])?
        .initializer([242; 32], |_, tx| {
            tx.storage()?.put_json("value", &json!(0))
        })?
        .json_cell_rpc("Read", [243; 32])?
        .json_cell_rpc("Bump", [246; 32])?
        .query("Peek", [244; 32], |query, _| {
            Ok(serde_json::to_vec(&query.storage().get_json("value")?)?)
        })?
        .query("Blocked", [245; 32], move |query, input| {
            let index = usize::from(*input.first().context("read index")?);
            query_gates.get(index).context("bounded read index")?.read();
            Ok(serde_json::to_vec(&query.storage().get_json("value")?)?)
        })?;
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |context, _| {
                Ok(Arc::new(Reader {
                    storage: context.storage()?,
                    target: factory_target.clone(),
                }))
            },
        )?,
    );
    let declared = configuration::options("s3-read-source-service");
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
    let work = cluster.fixtures[0].domain.run(async {
        named_service::grant(&cluster).await?;
        let group = cluster.history;
        let leader = managed_fixture::leader(&cluster.hosts, group).await?;
        let target_history = CellHistory {
            cluster: group.cluster, namespace: group.namespace,
            cell: CellId::new("Counter:s3-read-target")?, incarnation: IncarnationId::new([219;16])?,
        };
        cluster.hosts[leader].cell(target_history.clone())?.enroll(
            CellEnrollmentRequest { creation: ProposalId::new([219;16])?, durability: Durability::S3 }, TIMEOUT,
        ).await?;
        *target.lock().unwrap() = Some(cluster.hosts[(leader+1)%3].cell(target_history.clone())?);
        let target_writer = cluster.hosts[leader].cell(target_history)?;
        for mode in 0_u8..4 {
            let history = CellHistory {
                cluster: group.cluster, namespace: group.namespace,
                cell: CellId::new(format!("Counter:s3-read-source-{mode}"))?,
                incarnation: IncarnationId::new([220+mode*2;16])?,
            };
            let source = cluster.hosts[leader].cell(history.clone())?;
            source.enroll(CellEnrollmentRequest {
                creation: ProposalId::new([220+mode*2;16])?, durability: Durability::S3,
            }, TIMEOUT).await?;
            let observer = cluster.hosts[(leader+2)%3].cell(history)?;
            let before = observer.query(ProposalId::new([230;16])?, &observer.contract("Peek")?,
                vec![], QueryOptions::default()).await?;
            let before = before.version.clone();
            for gate in gates.iter() { gate.arm(); }
            let mut options = CommandOptions::new(
                CommandId::new(ClientId::new([231;16])?, u64::from(mode)+1)?,
                u64::try_from(cellar::asyncrt::wall_ms())?+60_000,
            );
            options.durability = Durability::S3;
            let (response, observed) = futures_util::future::join(
                source.command(&source.contract("Read")?, serde_json::to_vec(&mode)?, options),
                async {
                    let _release = Release(gates.clone());
                    let mut last = before.sequence;
                    for (index, gate) in gates.iter().enumerate() {
                        cellar::asyncrt::timeout(TIMEOUT, gate.entered.notified()).await
                            .context("target read did not enter after S3 sync")?;
                        let during = observer.query(ProposalId::new([232+index as u8;16])?,
                            &observer.contract("Peek")?, vec![], QueryOptions::default()).await?;
                        let dirty = mode == 1 || (mode == 2 || mode == 3) && index == 1;
                        let expected = before.sequence.get()+1+u64::from(dirty);
                        ensure!(during.version.history == before.history);
                        ensure!(during.version.sequence.get() == expected,
                            "S3 source read added a redundant checkpoint or skipped dirty proof: mode={mode} read={index} expected={expected} actual={}", during.version.sequence.get());
                        ensure!(during.result == if mode == 1 { b"11".as_slice() }
                            else if mode == 2 && index == 1 { b"22".as_slice() } else { b"0".as_slice() });
                        last = during.version.sequence;
                        drop(during);
                        if mode == 3 && index == 0 {
                            // A new S3 cut must evict the idle source proof, not
                            // deadlock on its one-slot verification workspace.
                            let mut write = CommandOptions::new(
                                CommandId::new(ClientId::new([234;16])?, 1)?,
                                u64::try_from(cellar::asyncrt::wall_ms())?+60_000,
                            );
                            write.durability = Durability::S3;
                            let receipt = target_writer.command(&target_writer.contract("Bump")?, b"0".to_vec(), write).await?;
                            ensure!(receipt.result == b"1" && receipt.durability == Durability::S3);
                            drop(receipt);
                        }
                        gate.release();
                    }
                    Ok::<_,anyhow::Error>(last)
                },
            ).await;
            let last = observed?;
            let response = response?;
            ensure!(response.result == b"0" && response.durability == Durability::S3);
            ensure!(response.version.sequence > last, "source checkpoint substituted for final receipt");
        }
        Ok::<_,anyhow::Error>(())
    }).await.map_err(anyhow::Error::from).and_then(std::convert::identity);
    for gate in gates.iter() {
        gate.release();
    }
    let joined =
        managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
            .await;
    if work.is_err() || joined.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!("retained S3-read-source disk: {}", root.keep().display());
        }
    }
    cluster.fixtures[0].domain.shutdown().await;
    work?;
    joined
}

pub async fn qualify(real: bool) -> Result<()> {
    for rooted in [false, true] {
        let store = if real {
            Arc::new(catalog_fault::Store::around(rustfs::store(
                "s3-read-source",
                rooted,
            )?))
        } else {
            Arc::new(catalog_fault::Store::default())
        };
        Box::pin(run(rooted, store))
            .await
            .with_context(|| format!("verified S3 read source RustFS={real} extents={rooted}"))?;
    }
    Ok(())
}
