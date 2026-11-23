// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real private cleanup must retain its reply and admission without blocking
//! an unrelated cell's fresh linearizable query on the single SQL worker.

use super::rpc_progress::physical;
use super::*;
use std::{path::PathBuf, sync::atomic::AtomicBool, sync::Condvar};

#[path = "cell_group_private_cleanup_lifecycle.rs"]
mod lifecycle;

pub async fn qualify_cancel() -> Result<()> {
    lifecycle::qualify_cancel().await
}

pub async fn qualify_failure() -> Result<()> {
    lifecycle::qualify_failure().await
}

#[derive(Default)]
struct Control {
    prefix: Mutex<Option<PathBuf>>,
    directory: Mutex<Option<PathBuf>>,
    armed: AtomicBool,
    entered: Notify,
    released: Mutex<bool>,
    wake: Condvar,
    calls: AtomicUsize,
    sql_thread: Mutex<Option<std::thread::ThreadId>>,
    cleanup_on_sql_thread: AtomicBool,
}

impl Control {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }

    fn hold(&self) {
        self.cleanup_on_sql_thread.store(
            *self.sql_thread.lock().unwrap() == Some(std::thread::current().id()),
            Ordering::Release,
        );
        self.entered.notify_one();
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.wake.wait(released).unwrap();
        }
    }
}

struct Release(Arc<Control>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}

struct Counter {
    storage: Storage,
    control: Arc<Control>,
}

impl Cell for Counter {
    fn invoke(self: Arc<Self>, _: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            ensure!(
                matches!(invocation, Invocation::CellRpc { ref method, .. } if method == "Run")
            );
            self.control.calls.fetch_add(1, Ordering::AcqRel);
            self.storage.put_json("value", &json!(1))?;
            *self.control.sql_thread.lock().unwrap() = Some(std::thread::current().id());
            // Arm after opening and invoking this exact private writer. Initial
            // scratch cleanup, other voters and immutable apply are excluded.
            self.control.armed.store(true, Ordering::Release);
            Ok(Output::Rpc(RpcData::Json("1".into())))
        })
    }
}

async fn read(cell: &RaftCellHandle, tag: u8) -> Result<i64> {
    let reply = cell
        .query(
            ProposalId::new([tag; 16])?,
            &cell.contract("Get")?,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
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
            let store = Arc::new(catalog_fault::Store::default());
            let result = run(rooted, durability, store.clone()).await;
            if let Err(error) =
                managed_fixture::memory::preserve_result(&store.inner, "private-cleanup", result)
                    .await
            {
                failures.push(format!(
                    "rooted={rooted} durability={durability:?}: {error:#}"
                ));
            }
        }
    }
    ensure!(
        failures.is_empty(),
        "private cleanup qualification: {failures:?}"
    );
    Ok(())
}

async fn run(rooted: bool, durability: Durability, store: Arc<catalog_fault::Store>) -> Result<()> {
    let control = Arc::new(Control::default());
    let factory = control.clone();
    let class = RaftClass::new("Counter", 1, [224; 32])?
        .initializer([225; 32], |_, tx| {
            tx.storage()?.put_json("value", &json!(0))
        })?
        .json_cell_rpc("Run", [226; 32])?
        .query("Get", [227; 32], |query, _| {
            Ok(serde_json::to_vec(&query.storage().get_json("value")?)?)
        })?;
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |context, _| {
                Ok(Arc::new(Counter {
                    storage: context.storage()?,
                    control: factory.clone(),
                }))
            },
        )?,
    );
    let configuration = configuration::options("private-cleanup-service");
    application.validate_deployment(&configuration::deployment(configuration.clone()))?;
    let mut fixture = Fixture::with_application(rooted, "Counter", application, store).await?;
    fixture.configuration = Some(configuration);
    fixture.sql_workers = 1;
    let faults = physical::Faults::new("unused-private-cleanup-selection".into(), None);
    let opening = control.clone();
    *faults.lock_open.lock().unwrap() = Some(Arc::new(move |path| {
        if path.file_name().is_some_and(|name| name == "working.lock")
            && opening
                .prefix
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|prefix| path.starts_with(prefix))
            && !opening.armed.load(Ordering::Acquire)
        {
            *opening.directory.lock().unwrap() = path.parent().map(PathBuf::from);
        }
        Ok(())
    }));
    let cleaning = control.clone();
    *faults.private_cleanup.lock().unwrap() = Some(Arc::new(move |path| {
        let selected = cleaning
            .directory
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|directory| {
                if rooted {
                    path == directory
                } else {
                    path == directory.join("working.sqlite")
                }
            });
        if selected && cleaning.armed.swap(false, Ordering::AcqRel) {
            cleaning.hold();
        }
        Ok(())
    }));
    fixture.filesystem = Arc::new(physical::FaultFileSystem { faults });
    let mut cluster = Box::pin(ManagedCluster::start_with_applications(
        fixture,
        &[],
        ArchiveSetup::Initialized,
        false,
        None,
        FirstGroupSetup::Normal,
    ))
    .await?;
    let _release = Release(control.clone());
    *control.prefix.lock().unwrap() = Some(cluster.nodes[0].info().data_dir.clone());
    let work = cluster.fixtures[0].domain.run(async {
        let group = cluster.history;
        let leader = managed_fixture::leader(&cluster.hosts, group).await?;
        if leader != 0 {
            cluster.hosts[leader].group(group)?.transfer_leader(MemberId::new(1)?).await?;
        }
        cellar::asyncrt::timeout(TIMEOUT, async {
            while managed_fixture::leader(&cluster.hosts, group).await? != 0 {
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
            Ok::<_, anyhow::Error>(())
        }).await??;
        let source = cluster.hosts[0].cell(CellHistory {
            cluster: group.cluster, namespace: group.namespace,
            cell: CellId::new("Counter:private-cleanup-source")?, incarnation: IncarnationId::new([228; 16])?,
        })?;
        let other = cluster.hosts[0].cell(CellHistory {
            cell: CellId::new("Counter:private-cleanup-other")?, incarnation: IncarnationId::new([229; 16])?,
            ..source.history().clone()
        })?;
        for (cell, tag) in [(&source, 228), (&other, 229)] {
            cell.enroll(CellEnrollmentRequest { creation: ProposalId::new([tag; 16])?, durability }, TIMEOUT).await?;
        }
        ensure!(read(&other, 230).await? == 0);
        let before = cluster.hosts[0].resource_usage();
        ensure!(before.sql.context("SQL observer")?.active_cells == 0);
        if rooted {
            ensure!(before.serving_images.extents.context("extent observer")?.workspaces == 0);
        }
        let options = interleaving::command(231, 1, durability)?;
        let contract = source.contract("Run")?;
        let completed = AtomicBool::new(false);
        let (result, observed) = tokio::join!(
            async {
                let result = source.command(&contract, b"{}".to_vec(), options.clone()).await;
                if let Err(error) = &result {
                    eprintln!("PRIVATE_CLEANUP_RESPONSE {}", json!({"ambiguous":error.ambiguous,"command":format!("{:?}",error.command),"error":format!("{error:#}")}));
                }
                completed.store(true, Ordering::Release);
                result
            },
            async {
                let _release = Release(control.clone());
                cellar::asyncrt::timeout(TIMEOUT, control.entered.notified()).await.context("selected private cleanup")?;
                ensure!(!completed.load(Ordering::Acquire), "reply preceded joined cleanup");
                let held = cluster.hosts[0].resource_usage();
                let sql = held.sql.context("held SQL observer")?;
                ensure!(sql.active_cells == 1 && sql.retained_bytes > 0, "private cleanup released admission early");
                if rooted {
                    let extents = held.serving_images.extents.context("held extent observer")?;
                    ensure!(extents.workspaces > 0 && extents.workspace_bytes > 0 && !extents.quarantined,
                        "physical workspace was refunded before joined cleanup");
                }
                // Timeout borrows this exact read future. Its original request
                // remains alive and is joined after release; it is never reset.
                let mut query = Box::pin(read(&other, 232));
                let progress = cellar::asyncrt::timeout(Duration::from_secs(1), query.as_mut()).await;
                let progressed = progress.is_ok();
                ensure!(!completed.load(Ordering::Acquire), "reply preceded physical cleanup release");
                control.release();
                let value = match progress { Ok(result) => result?, Err(_) => query.await? };
                ensure!(value == 0, "unrelated cell changed");
                Ok::<_, anyhow::Error>(progressed)
            },
        );
        let receipt = result?;
        ensure!(receipt.result == b"1" && receipt.durability == durability);
        let replay = source.command(&contract, b"{}".to_vec(), options).await?;
        ensure!(receipt.id == replay.id && receipt.result == replay.result && receipt.version == replay.version
            && replay.durability == durability && control.calls.load(Ordering::Acquire) == 1);
        ensure!(read(&source, 233).await? == 1);
        let settled = cluster.hosts[0].resource_usage();
        ensure!(settled.sql.context("settled SQL observer")?.active_cells == 0);
        if rooted {
            let extents = settled.serving_images.extents.context("settled extent observer")?;
            ensure!(extents.workspaces == 0 && extents.workspace_bytes == 0 && !extents.quarantined);
        }
        observed
    }).await.map_err(anyhow::Error::from).and_then(std::convert::identity);
    control.release();
    let joined =
        managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
            .await;
    for root in std::mem::take(&mut cluster.roots) {
        eprintln!("retained private cleanup disk: {}", root.keep().display());
    }
    if let Err(error) = &joined {
        eprintln!("private cleanup joined close: {error:#}");
    }
    eprintln!(
        "PRIVATE_CLEANUP_PROGRESS {}",
        json!({"rooted":rooted,"durability":format!("{durability:?}"),
        "queryProgressedBeforeRelease":work.as_ref().ok(),"valuesReplayAndAdmissionVerified":work.is_ok(),
        "cleanupOnSqlOwner":control.cleanup_on_sql_thread.load(Ordering::Acquire),"joinedClose":joined.is_ok()})
    );
    cluster.fixtures[0].domain.shutdown().await;
    let progressed = work?;
    joined?;
    ensure!(
        progressed,
        "private cleanup blocked unrelated SQL beyond the progress window"
    );
    Ok(())
}
