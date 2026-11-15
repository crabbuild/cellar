// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A clean first Quorum read needs no synthetic source mutation. Dirty and S3
//! sources still prove their state before entering an independent target read.

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

struct Release(Arc<Gate>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
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
                args: RpcData::Json(input),
                ..
            } = invocation
            else {
                anyhow::bail!("read RPC required");
            };
            let dirty: bool = serde_json::from_str(&input)?;
            if dirty {
                self.storage.put_json("value", &json!(1))?;
            }
            let target = self.target.lock().unwrap().clone().context("read target")?;
            let result = target
                .query(
                    ProposalId::new([213; 16])?,
                    &target.contract("Blocked")?,
                    vec![],
                    QueryOptions {
                        routing: QueryRouting::Local,
                        ..Default::default()
                    },
                )
                .await?;
            Ok(Output::Rpc(RpcData::Json(String::from_utf8(
                result.result,
            )?)))
        })
    }
}

async fn run(rooted: bool, store: Arc<catalog_fault::Store>) -> Result<()> {
    let gate = Arc::new(Gate::default());
    let query_gate = gate.clone();
    let target = Arc::new(Mutex::new(None));
    let factory_target = target.clone();
    let class = RaftClass::new("Counter", 1, [208; 32])?
        .initializer([209; 32], |_, tx| {
            tx.storage()?.put_json("value", &json!(0))
        })?
        .json_cell_rpc("Read", [210; 32])?
        .query("Peek", [211; 32], |query, _| {
            Ok(serde_json::to_vec(&query.storage().get_json("value")?)?)
        })?
        .query("Blocked", [212; 32], move |query, _| {
            query_gate.read();
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
    let declared = configuration::options("initial-read-service");
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
            named_service::grant(&cluster).await?;
            let group = cluster.history;
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            let source_history = CellHistory {
                cluster: group.cluster,
                namespace: group.namespace,
                cell: CellId::new("Counter:initial-read-source")?,
                incarnation: IncarnationId::new([214; 16])?,
            };
            let target_history = CellHistory {
                cell: CellId::new("Counter:initial-read-target")?,
                incarnation: IncarnationId::new([215; 16])?,
                ..source_history.clone()
            };
            let source = cluster.hosts[leader].cell(source_history.clone())?;
            for (history, id) in [(source_history.clone(), 214), (target_history.clone(), 215)] {
                cluster.hosts[leader]
                    .cell(history)?
                    .enroll(
                        CellEnrollmentRequest {
                            creation: ProposalId::new([id; 16])?,
                            durability: Durability::S3,
                        },
                        TIMEOUT,
                    )
                    .await?;
            }
            // The blocked physical reader lives on a follower; the third voter can
            // observe the source with fresh quorum while its leader owns the RPC.
            *target.lock().unwrap() = Some(cluster.hosts[(leader + 1) % 3].cell(target_history)?);
            let observer = cluster.hosts[(leader + 2) % 3].cell(source_history)?;
            for (index, (dirty, durability)) in [
                (false, Durability::Quorum),
                (true, Durability::Quorum),
                (false, Durability::S3),
            ]
            .into_iter()
            .enumerate()
            {
                let before = observer
                    .query(
                        ProposalId::new([216; 16])?,
                        &observer.contract("Peek")?,
                        vec![],
                        QueryOptions::default(),
                    )
                    .await?;
                let version = before.version.clone();
                drop(before);
                gate.arm();
                let mut options = CommandOptions::new(
                    CommandId::new(ClientId::new([217; 16])?, index as u64 + 1)?,
                    u64::try_from(cellar::asyncrt::wall_ms())? + 60_000,
                );
                options.durability = durability;
                let (response, observed) = futures_util::future::join(
                    source.command(
                        &source.contract("Read")?,
                        serde_json::to_vec(&dirty)?,
                        options,
                    ),
                    async {
                        let _release = Release(gate.clone());
                        cellar::asyncrt::timeout(TIMEOUT, gate.entered.notified())
                            .await
                            .context("target query did not enter")?;
                        let during = observer
                            .query(
                                ProposalId::new([218; 16])?,
                                &observer.contract("Peek")?,
                                vec![],
                                QueryOptions::default(),
                            )
                            .await?;
                        if !dirty && durability == Durability::Quorum {
                            ensure!(
                                during.version == version,
                                "clean first Quorum read created a source checkpoint"
                            );
                        } else {
                            ensure!(
                                during.version.sequence > version.sequence,
                                "dirty or S3 source skipped its checkpoint"
                            );
                        }
                        ensure!(during.result == if dirty || index == 2 { b"1" } else { b"0" });
                        Ok::<_, anyhow::Error>(())
                    },
                )
                .await;
                observed?;
                let response = response?;
                ensure!(response.result == b"0" && response.durability == durability);
                ensure!(
                    response.version.sequence > version.sequence,
                    "final parent receipt was not committed"
                );
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(std::convert::identity);
    gate.release();
    let joined =
        managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
            .await;
    if work.is_err() || joined.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!("retained initial-read disk: {}", root.keep().display());
        }
    }
    cluster.fixtures[0].domain.shutdown().await;
    work?;
    joined
}

pub async fn qualify(real: bool) -> Result<()> {
    for rooted in [false, true] {
        let store = if real {
            Arc::new(catalog_fault::Store::around(rustfs::store("initial-read", rooted)?))
        } else {
            Arc::new(catalog_fault::Store::default())
        };
        Box::pin(run(rooted, store))
            .await
            .with_context(|| format!("initial read extents={rooted}"))?;
    }
    Ok(())
}
