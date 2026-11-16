// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A remote binding does not reopen an uncontended, already proven writer.

use super::*;
use std::sync::atomic::AtomicBool;

#[path = "raft_ltx_fault.rs"]
mod physical;

const SCRIPT: &str = "binding-writer-service";

#[derive(Default)]
struct Control {
    child: Notify,
    release: Notify,
    source_thread: Mutex<Option<std::thread::ThreadId>>,
    deny: AtomicBool,
    reopens: AtomicUsize,
}

struct Release(Arc<Control>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.deny.store(false, Ordering::Release);
        self.0.release.notify_one();
    }
}

struct Bound {
    storage: Storage,
    control: Arc<Control>,
}

impl Cell for Bound {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc { method, .. } = invocation else {
                anyhow::bail!("RPC required");
            };
            let value = if method == "Run" {
                *self.control.source_thread.lock().unwrap() = Some(std::thread::current().id());
                let reply = context
                    .namespace("COUNTERS")?
                    .get_by_name("target")
                    .rpc(&context, "Child", RpcData::Json("[]".into()))
                    .await?;
                // The callback has its writer again. Final result capture and
                // immutable receipt verification keep their ordinary I/O.
                self.control.deny.store(false, Ordering::Release);
                let RpcData::Json(reply) = reply else {
                    anyhow::bail!("JSON child result required")
                };
                let value: serde_json::Value = serde_json::from_str(&reply)?;
                self.storage.put_json("value", &value)?;
                value
            } else {
                self.control.child.notify_one();
                self.control.release.notified().await;
                self.storage.tx(|tx| {
                    let value = tx
                        .get_json("value")?
                        .context("child value")?
                        .as_i64()
                        .context("integer child")?
                        + 1;
                    tx.put_json("value", &json!(value))?;
                    Ok(json!(value))
                })?
            };
            Ok(Output::Rpc(RpcData::Json(value.to_string())))
        })
    }
}

async fn run(rooted: bool, store: Arc<catalog_fault::Store>) -> Result<()> {
    let control = Arc::new(Control::default());
    let parent = RaftClass::new("Parent", 1, [221; 32])?
        .initializer([222; 32], |_, tx| {
            tx.storage()?.put_json("value", &json!(0))
        })?
        .json_cell_rpc("Run", [223; 32])?
        .query("Peek", [224; 32], |q, _| {
            Ok(serde_json::to_vec(&q.storage().get_json("value")?)?)
        })?;
    let child = RaftClass::new("Counter", 1, [225; 32])?
        .initializer([226; 32], |_, tx| {
            tx.storage()?.put_json("value", &json!(0))
        })?
        .json_cell_rpc("Child", [227; 32])?
        .query("Peek", [228; 32], |q, _| {
            Ok(serde_json::to_vec(&q.storage().get_json("value")?)?)
        })?;
    let parent_control = control.clone();
    let child_control = control.clone();
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
            .raft_cell(parent, move |context, _| {
                Ok(Arc::new(Bound {
                    storage: context.storage()?,
                    control: parent_control.clone(),
                }))
            })?
            .raft_cell(child, move |context, _| {
                Ok(Arc::new(Bound {
                    storage: context.storage()?,
                    control: child_control.clone(),
                }))
            })?,
    );
    let mut declared = configuration::options(SCRIPT);
    declared.do_classes = vec!["Parent".into(), "Counter".into()];
    declared.bindings = vec![("COUNTERS".into(), "Counter".into())];
    application.validate_deployment(&configuration::deployment(declared.clone()))?;
    let mut fixture = Fixture::with_definition_policies(
        rooted,
        vec![
            (
                application.resolve_raft_class("Parent")?,
                Durability::Quorum,
            ),
            (
                application.resolve_raft_class("Counter")?,
                Durability::Quorum,
            ),
        ],
        application,
        store,
    )
    .await?;
    fixture.configuration = Some(declared);
    fixture.sql_workers = 1;
    let faults = physical::Faults::new(
        std::path::PathBuf::from("unused-writer-fault-selection"),
        None,
    );
    let observed = control.clone();
    *faults.lock_open.lock().unwrap() = Some(Arc::new(move |path| {
        if path.file_name().is_some_and(|name| name == "working.lock")
            && observed.deny.load(Ordering::Acquire)
            && *observed.source_thread.lock().unwrap() == Some(std::thread::current().id())
        {
            observed.reopens.fetch_add(1, Ordering::AcqRel);
            return Err(std::io::Error::other(
                "uncontended binding reopened source writer",
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
            let _release = Release(control.clone());
            named_service::grant(&cluster).await?;
            let child_group = named_service::activate_at(&cluster, "binding-writer", 0).await?;
            let source_node = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
            let target_node = (source_node + 1) % cluster.hosts.len();
            let leader = managed_fixture::leader(&cluster.hosts, child_group).await?;
            if leader != target_node {
                cluster.hosts[leader]
                    .group(child_group)?
                    .transfer_leader(MemberId::new(target_node as u64 + 1)?)
                    .await?;
            }
            cellar::asyncrt::timeout(TIMEOUT, async {
                loop {
                    if managed_fixture::leader(&cluster.hosts, child_group).await? == target_node {
                        return Ok::<_, anyhow::Error>(());
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(5)).await;
                }
            })
            .await??;
            let source_history = CellHistory {
                cluster: cluster.history.cluster,
                namespace: cluster.history.namespace,
                cell: CellId::new("Parent:binding-writer")?,
                incarnation: IncarnationId::new([229; 16])?,
            };
            let namespace = cellar_native::cell_id::Namespace::new(
                "Counter",
                format!("cells:v1:{}:{SCRIPT}:Counter", SCRIPT.len()),
            );
            let target_history = CellHistory {
                cluster: child_group.cluster,
                namespace: child_group.namespace,
                cell: CellId::new(namespace.id_from_name("target").scope())?,
                incarnation: IncarnationId::new([230; 16])?,
            };
            for (node, history, id) in [
                (source_node, source_history.clone(), 231),
                (target_node, target_history.clone(), 232),
            ] {
                cluster.hosts[node]
                    .cell(history)?
                    .enroll(
                        CellEnrollmentRequest {
                            creation: ProposalId::new([id; 16])?,
                            durability: Durability::Quorum,
                        },
                        TIMEOUT,
                    )
                    .await?;
            }
            let source = cluster.hosts[source_node].cell(source_history.clone())?;
            let target = cluster.hosts[target_node].cell(target_history)?;
            let options = CommandOptions::new(
                CommandId::new(ClientId::new([233; 16])?, 1)?,
                u64::try_from(cellar::asyncrt::wall_ms())? + 60_000,
            );
            let (reply, observed) = futures_util::future::join(
                source.command(&source.contract("Run")?, b"[]".to_vec(), options.clone()),
                async {
                    cellar::asyncrt::timeout(TIMEOUT, control.child.notified())
                        .await
                        .context("child did not enter")?;
                    // Serve an actual immutable query on the source's sole SQL
                    // worker while its child is held. This joins the worker turn
                    // that could park the parent; it adds no contending writer.
                    let before = source
                        .query(
                            ProposalId::new([234; 16])?,
                            &source.contract("Peek")?,
                            vec![],
                            QueryOptions {
                                routing: QueryRouting::Local,
                                ..Default::default()
                            },
                        )
                        .await?;
                    ensure!(before.result == b"0");
                    drop(before);
                    control.deny.store(true, Ordering::Release);
                    control.release.notify_one();
                    Ok::<_, anyhow::Error>(())
                },
            )
            .await;
            observed?;
            let reply = reply.with_context(|| {
                format!(
                    "source writer reopened {} times",
                    control.reopens.load(Ordering::Acquire)
                )
            })?;
            ensure!(reply.result == b"1" && reply.durability == Durability::Quorum);
            ensure!(control.reopens.load(Ordering::Acquire) == 0);
            let replay = source
                .command(&source.contract("Run")?, b"[]".to_vec(), options)
                .await?;
            ensure!(replay.result == reply.result && replay.version == reply.version);
            let child = target
                .query(
                    ProposalId::new([235; 16])?,
                    &target.contract("Peek")?,
                    vec![],
                    QueryOptions::default(),
                )
                .await?;
            ensure!(child.result == b"1", "child effect repeated or lost");
            Ok::<_, anyhow::Error>(())
        })
        .await;
    let cleanup = named_service::close(&mut cluster).await;
    cluster.fixtures[0].domain.shutdown().await;
    work?;
    ensure!(cleanup.is_empty(), "binding writer cleanup: {cleanup:?}");
    Ok(())
}

pub(super) async fn qualify(real_rustfs: bool) -> Result<()> {
    for rooted in [false, true] {
        let store = Arc::new(if real_rustfs {
            catalog_fault::Store::around(rustfs::store("binding-writer", rooted)?)
        } else {
            catalog_fault::Store::default()
        });
        Box::pin(run(rooted, store))
            .await
            .with_context(|| format!("uncontended binding writer extents={rooted}"))?;
    }
    Ok(())
}
