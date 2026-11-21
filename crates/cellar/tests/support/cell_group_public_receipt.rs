// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public command settlement must reuse only its exact frozen result writer.

use super::rpc_progress::physical;
use super::*;
use std::sync::atomic::AtomicBool;

#[derive(Default)]
struct Control {
    calls: AtomicUsize,
    thread: Mutex<Option<std::thread::ThreadId>>,
    deny: AtomicBool,
    reopens: AtomicUsize,
}

struct Counter {
    storage: Storage,
    control: Arc<Control>,
}

impl Cell for Counter {
    fn invoke(self: Arc<Self>, _: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            ensure!(matches!(invocation, Invocation::CellRpc { .. }));
            self.control.calls.fetch_add(1, Ordering::AcqRel);
            self.storage.put_json("value", &json!(1))?;
            self.storage.sync().await?;
            self.storage.put_json("value", &json!(2))?;
            *self.control.thread.lock().unwrap() = Some(std::thread::current().id());
            // Final preparation and physical apply retain their real I/O.
            // Only the owning SQL worker's immutable image reopen is denied.
            // There is no await or further application work after arming.
            self.control.deny.store(true, Ordering::Release);
            Ok(Output::Rpc(RpcData::Json("2".into())))
        })
    }
}

pub(super) async fn qualify(rooted: bool, real_rustfs: bool) -> Result<()> {
    let control = Arc::new(Control::default());
    let factory = control.clone();
    let class = RaftClass::new("Counter", 1, [236; 32])?
        .initializer([237; 32], |_, tx| {
            tx.storage()?.put_json("value", &json!(0))
        })?
        .json_cell_rpc("Run", [238; 32])?
        .query("Peek", [239; 32], |query, _| {
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
    let declared = configuration::options("public-receipt-service");
    application.validate_deployment(&configuration::deployment(declared.clone()))?;
    let store = Arc::new(if real_rustfs {
        catalog_fault::Store::around(rustfs::store("public-receipt", rooted)?)
    } else {
        catalog_fault::Store::default()
    });
    let mut fixture = Fixture::with_application(rooted, "Counter", application, store).await?;
    fixture.configuration = Some(declared);
    fixture.sql_workers = 1;
    let faults = physical::Faults::new("unused-public-receipt-selection".into(), None);
    let observed = control.clone();
    *faults.read_open.lock().unwrap() = Some(Arc::new(move |path| {
        let immutable = path.file_name().is_some_and(|name| {
            let name = name.to_string_lossy();
            name.starts_with("e-")
                || name.strip_suffix(".sqlite").is_some_and(|stem| {
                    stem.len() == 32 && stem.bytes().all(|byte| byte.is_ascii_hexdigit())
                })
        });
        if immutable
            && observed.deny.load(Ordering::Acquire)
            && *observed.thread.lock().unwrap() == Some(std::thread::current().id())
        {
            observed.reopens.fetch_add(1, Ordering::AcqRel);
            return Err(std::io::Error::other(
                "public settlement reopened its frozen result image",
            ));
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
    let work = cluster.fixtures[0]
        .domain
        .run(async {
            named_service::grant(&cluster).await?;
            let group = cluster.history;
            let leader = managed_fixture::leader(&cluster.hosts, group).await?;
            if leader != 0 {
                cluster.hosts[leader]
                    .group(group)?
                    .transfer_leader(MemberId::new(1)?)
                    .await?;
            }
            cellar::asyncrt::timeout(TIMEOUT, async {
                loop {
                    if managed_fixture::leader(&cluster.hosts, group).await? == 0 {
                        return Ok::<_, anyhow::Error>(());
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(5)).await;
                }
            })
            .await??;
            let history = CellHistory {
                cluster: group.cluster,
                namespace: group.namespace,
                cell: CellId::new("Counter:public-receipt")?,
                incarnation: IncarnationId::new([240; 16])?,
            };
            let source = cluster.hosts[0].cell(history)?;
            source
                .enroll(
                    CellEnrollmentRequest {
                        creation: ProposalId::new([241; 16])?,
                        durability: Durability::Quorum,
                    },
                    TIMEOUT,
                )
                .await?;
            let options = CommandOptions::new(
                CommandId::new(ClientId::new([242; 16])?, 1)?,
                u64::try_from(cellar::asyncrt::wall_ms())? + 60_000,
            );
            // This is the normal public driver, including Action::Reconcile;
            // it does not call the low-level executor.finish shortcut.
            let reply = source
                .command(&source.contract("Run")?, b"[]".to_vec(), options.clone())
                .await;
            control.deny.store(false, Ordering::Release);
            let reopens = control.reopens.load(Ordering::Acquire);
            let reply = reply.with_context(|| {
                format!("public final receipt extents={rooted}, denied image reopens={reopens}")
            })?;
            ensure!(reopens == 0, "settlement reopened its frozen image");
            ensure!(reply.result == b"2" && reply.durability == Durability::Quorum);
            let replay = source
                .command(&source.contract("Run")?, b"[]".to_vec(), options)
                .await?;
            ensure!(replay.result == reply.result && replay.version == reply.version);
            ensure!(
                control.calls.load(Ordering::Acquire) == 1,
                "callback replayed"
            );
            let current = source
                .query(
                    ProposalId::new([243; 16])?,
                    &source.contract("Peek")?,
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::Linearizable,
                        ..Default::default()
                    },
                )
                .await?;
            ensure!(current.result == b"2", "final mutation lost");
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(std::convert::identity);
    control.deny.store(false, Ordering::Release);
    let joined =
        managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
            .await;
    if work.is_err() || joined.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!("retained public-receipt disk: {}", root.keep().display());
        }
    }
    cluster.fixtures[0].domain.shutdown().await;
    work?;
    joined
}
