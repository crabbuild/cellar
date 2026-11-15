// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A blocked writer reacquisition must not hide another cell's ready work.

use super::*;
use futures_util::future::BoxFuture;
use std::sync::{atomic::AtomicBool, Condvar};

type Call = BoxFuture<'static, std::result::Result<CommandReceipt, Arc<CellCommandError>>>;

struct PausedCell {
    storage: Storage,
    gates: Arc<[RpcGate; 3]>,
    progressed: Arc<AtomicBool>,
}

impl Cell for PausedCell {
    fn invoke(self: Arc<Self>, _: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc { method, .. } = invocation else {
                anyhow::bail!("expected cell RPC");
            };
            let index = match method.as_str() {
                "Park" => 0,
                "HoldA" => 1,
                "HoldB" => 2,
                _ => anyhow::bail!("unexpected RPC"),
            };
            update(&self.storage, if index == 0 { 1 } else { 10 })?;
            if index == 0 {
                self.storage.sync().await?;
            }
            // Park has an applied root and can release its writer. The two
            // Hold invocations retain private writes and their writer leases.
            self.gates[index].entered.notify_one();
            self.gates[index].release.notified().await;
            if index == 2 {
                self.progressed.store(true, Ordering::Release);
            }
            let value = self.storage.get_json("value")?.context("cell value")?;
            Ok(Output::Rpc(RpcData::Json(value.to_string())))
        })
    }
}

#[derive(Default)]
struct PhysicalGate {
    entered: Notify,
    released: Mutex<bool>,
    wake: Condvar,
}

impl PhysicalGate {
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

struct Release(Arc<PhysicalGate>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

async fn entered(call: Call, gate: &RpcGate) -> Result<Call> {
    cellar::asyncrt::timeout(TIMEOUT, async {
        match futures_util::future::select(call, gate.entered.notified().boxed()).await {
            futures_util::future::Either::Left((result, _)) => {
                anyhow::bail!("RPC completed before its gate: {result:?}")
            }
            futures_util::future::Either::Right(((), call)) => Ok(call),
        }
    })
    .await?
}

pub async fn run(rooted: bool, store: Arc<catalog_fault::Store>) -> Result<()> {
    let gates: Arc<[RpcGate; 3]> = Arc::new(std::array::from_fn(|_| RpcGate::default()));
    let progressed = Arc::new(AtomicBool::new(false));
    let physical = Arc::new(PhysicalGate::default());
    let blocked = physical.clone();
    let observed = progressed.clone();
    let class = RaftClass::new("Counter", 1, [91; 32])?
        .initializer([92; 32], |_, tx| {
            tx.execute_batch("CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES(0)")?;
            tx.storage()?.put_json("value", &json!(0))
        })?
        .json_cell_rpc("Park", [93; 32])?
        .json_cell_rpc("HoldA", [94; 32])?
        .json_cell_rpc("HoldB", [95; 32])?
        .query("Block", [96; 32], move |_, _| {
            blocked.hold();
            Ok(vec![])
        })?
        .query("Observe", [97; 32], move |_, _| {
            Ok(vec![u8::from(observed.load(Ordering::Acquire))])
        })?;
    let waiting = gates.clone();
    let progress = progressed.clone();
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |context, _| {
                Ok(Arc::new(PausedCell {
                    storage: context.storage()?,
                    gates: waiting.clone(),
                    progressed: progress.clone(),
                }))
            },
        )?,
    );
    let declared = configuration::options("resume-progress-service");
    application.validate_deployment(&configuration::deployment(declared.clone()))?;
    let mut fixture = Fixture::with_application(rooted, "Counter", application, store).await?;
    fixture.configuration = Some(declared);
    fixture.sql_workers = 1;
    // Three retained RPCs and two physical observations share the original
    // byte bank. Existing public fixture admissions are unchanged.
    fixture.ordinary_admission = Some((5, 64 * 1024));
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
            let host = &cluster.hosts[leader];
            let mut cells = Vec::new();
            for (name, id) in [("a", 98), ("b", 99)] {
                let cell = host.cell(CellHistory {
                    cluster: group.cluster,
                    namespace: group.namespace,
                    cell: CellId::new(format!("Counter:resume-{name}"))?,
                    incarnation: IncarnationId::new([id; 16])?,
                })?;
                cell.enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([id; 16])?,
                        durability: Durability::Quorum,
                    },
                    TIMEOUT,
                )
                .await?;
                cells.push(cell);
            }
            let call = |cell: &RaftCellHandle, method: &str, id| -> Result<Call> {
                Ok(cell
                    .command(
                        &cell.contract(method)?,
                        b"{}".to_vec(),
                        interleaving::command(id, 1, Durability::Quorum)?,
                    )
                    .boxed())
            };
            let parked = entered(call(&cells[0], "Park", 100)?, &gates[0]).await?;
            cellar::asyncrt::timeout(TIMEOUT, async {
                while host.resource_usage().sql.context("SQL usage")?.active_cells != 0 {
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
            let hold_a = entered(call(&cells[0], "HoldA", 101)?, &gates[1]).await?;
            let hold_b = entered(call(&cells[1], "HoldB", 102)?, &gates[2]).await?;
            let options = QueryOptions {
                consistency: QueryConsistency::Stale,
                routing: QueryRouting::Local,
                ..Default::default()
            };
            let (blocked, checked) = tokio::join!(
                cells[1].query(
                    ProposalId::new([103; 16])?,
                    &cells[1].contract("Block")?,
                    vec![],
                    options.clone(),
                ),
                async {
                    let _release = Release(physical.clone());
                    cellar::asyncrt::timeout(TIMEOUT, physical.entered.notified()).await?;
                    let (result, armed) = tokio::join!(
                        cells[1].query(
                            ProposalId::new([104; 16])?,
                            &cells[1].contract("Observe")?,
                            vec![],
                            options,
                        ),
                        async {
                            cellar::asyncrt::timeout(TIMEOUT, async {
                                while host
                                    .resource_usage()
                                    .sql
                                    .context("SQL usage")?
                                    .queued_or_running
                                    < 2
                                {
                                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                                }
                                Ok::<_, anyhow::Error>(())
                            })
                            .await??;
                            ensure!(!progressed.load(Ordering::Acquire));
                            // Both callbacks are ready before the worker leaves
                            // Block. Park cannot regain A's lease, but B's dirty
                            // writer can run immediately. Observe is already
                            // queued: no idle tick or new wake may hide the order.
                            gates[0].release.notify_one();
                            gates[2].release.notify_one();
                            physical.release();
                            Ok::<_, anyhow::Error>(())
                        },
                    );
                    armed?;
                    ensure!(
                        result?.result == [1],
                        "ready cell work postponed behind a blocked writer resume"
                    );
                    Ok::<_, anyhow::Error>(())
                },
            );
            for gate in gates.iter() {
                gate.release.notify_one();
            }
            let receipts = futures_util::future::join3(parked, hold_a, hold_b).await;
            blocked?;
            checked?;
            ensure!(receipts.0?.result == b"11");
            ensure!(receipts.1?.result == b"11");
            ensure!(receipts.2?.result == b"10");
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(std::convert::identity);
    physical.release();
    let joined =
        managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
            .await;
    if work.is_err() || joined.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!("retained resume progress disk: {}", root.keep().display());
        }
    }
    cluster.fixtures[0].domain.shutdown().await;
    work?;
    joined
}
