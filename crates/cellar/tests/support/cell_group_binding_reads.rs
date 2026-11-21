// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A completed child changes only private receipt bookkeeping before a query.

use super::*;
use std::sync::{atomic::AtomicBool, Condvar};

const SCRIPT: &str = "binding-read-service";

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

#[derive(Default)]
struct Control {
    child: Notify,
    proceed: Notify,
    query: Gate,
}

struct Release(Arc<Control>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.proceed.notify_one();
        self.0.query.release();
    }
}

struct Reader {
    storage: Storage,
    target: Arc<Mutex<Option<RaftCellHandle>>>,
    control: Arc<Control>,
}

impl Cell for Reader {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc {
                method,
                args: RpcData::Json(input),
                ..
            } = invocation
            else {
                anyhow::bail!("JSON RPC required");
            };
            if method == "Child" {
                let value = self.storage.tx(|tx| {
                    let prior = tx.get_json("value")?.context("child value")?;
                    let next = prior.as_i64().context("integer child value")? + 1;
                    tx.put_json("value", &json!(next))?;
                    Ok(next)
                })?;
                return Ok(Output::Rpc(RpcData::Json(value.to_string())));
            }
            let dirty: bool = serde_json::from_str(&input)?;
            let reply = context
                .namespace("COUNTERS")?
                .get_by_name("target")
                .rpc(&context, "Child", RpcData::Json("[]".into()))
                .await?;
            ensure!(cellar::application::decode_rpc_result(&reply)?
                .to_json()?
                .as_i64()
                .is_some());
            drop(reply);
            // The child receipt is verified and its private completion is now
            // recorded. Let a third voter capture the exact proven source cut.
            self.control.child.notify_one();
            self.control.proceed.notified().await;
            if dirty {
                self.storage.put_json("value", &json!(9))?;
            }
            let target = self.target.lock().unwrap().clone().context("read target")?;
            let result = target
                .query(
                    ProposalId::new([181; 16])?,
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
    let control = Arc::new(Control::default());
    let query_control = control.clone();
    let target = Arc::new(Mutex::new(None));
    let factory_target = target.clone();
    let factory_control = control.clone();
    let class = RaftClass::new("Counter", 1, [182; 32])?
        .initializer([183; 32], |_, tx| {
            tx.storage()?.put_json("value", &json!(0))
        })?
        .json_cell_rpc("Run", [184; 32])?
        .json_cell_rpc("Child", [185; 32])?
        .query("Peek", [186; 32], |q, _| {
            Ok(serde_json::to_vec(&q.storage().get_json("value")?)?)
        })?
        .query("Blocked", [187; 32], move |q, _| {
            query_control.query.read();
            Ok(serde_json::to_vec(&q.storage().get_json("value")?)?)
        })?;
    let application = Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
            class,
            move |context, _| {
                Ok(Arc::new(Reader {
                    storage: context.storage()?,
                    target: factory_target.clone(),
                    control: factory_control.clone(),
                }))
            },
        )?,
    );
    let mut declared = configuration::options(SCRIPT);
    declared.do_classes = vec!["Counter".into()];
    declared.bindings = vec![("COUNTERS".into(), "Counter".into())];
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
        let source_history = CellHistory {
            cluster:group.cluster, namespace:group.namespace,
            cell:CellId::new("Counter:binding-read-source")?, incarnation:IncarnationId::new([188;16])?,
        };
        let namespace = cellar_native::cell_id::Namespace::new(
            "Counter", format!("cells:v1:{}:{SCRIPT}:Counter", SCRIPT.len()),
        );
        let target_history = CellHistory {
            cell:CellId::new(namespace.id_from_name("target").scope())?,
            incarnation:IncarnationId::new([189;16])?, ..source_history.clone()
        };
        for (history,id) in [(source_history.clone(),188), (target_history.clone(),189)] {
            cluster.hosts[leader].cell(history)?.enroll(
                CellEnrollmentRequest { creation:ProposalId::new([id;16])?, durability:Durability::S3 }, TIMEOUT,
            ).await?;
        }
        let source = cluster.hosts[leader].cell(source_history.clone())?;
        let target_observer = cluster.hosts[(leader+2)%3].cell(target_history.clone())?;
        *target.lock().unwrap() = Some(cluster.hosts[(leader+1)%3].cell(target_history)?);
        let observer = cluster.hosts[(leader+2)%3].cell(source_history)?;
        for (index,(dirty,durability)) in [
            (false,Durability::Quorum), (true,Durability::Quorum), (false,Durability::S3),
        ].into_iter().enumerate() {
            control.query.arm();
            let mut options = CommandOptions::new(
                CommandId::new(ClientId::new([190;16])?, index as u64+1)?,
                u64::try_from(cellar::asyncrt::wall_ms())?+60_000,
            );
            options.durability = durability;
            let (reply, observed) = futures_util::future::join(
                source.command(&source.contract("Run")?, serde_json::to_vec(&dirty)?, options),
                async {
                    let _release = Release(control.clone());
                    cellar::asyncrt::timeout(TIMEOUT, control.child.notified()).await
                        .context("child receipt did not return")?;
                    let before = observer.query(ProposalId::new([191;16])?, &observer.contract("Peek")?,
                        vec![], QueryOptions::default()).await?;
                    let version = before.version.clone(); drop(before);
                    control.proceed.notify_one();
                    cellar::asyncrt::timeout(TIMEOUT, control.query.entered.notified()).await
                        .context("following target query did not enter")?;
                    let during = observer.query(ProposalId::new([192;16])?, &observer.contract("Peek")?,
                        vec![], QueryOptions::default()).await?;
                    if !dirty && durability==Durability::Quorum {
                        ensure!(during.version==version,
                            "private binding completion created a redundant source read checkpoint");
                    } else {
                        ensure!(during.version.sequence>version.sequence,
                            "dirty or S3 source skipped its required read checkpoint");
                    }
                    ensure!(during.result==if index==0 {b"0"} else {b"9"});
                    Ok::<_,anyhow::Error>(version)
                },
            ).await;
                let reply = reply.context("parent invocation before following-read observation")?;
                let before = observed?;
            ensure!(reply.result==(index+1).to_string().as_bytes() && reply.durability==durability);
            ensure!(reply.version.sequence>before.sequence, "final parent receipt was not committed");
            let child = target_observer.query(ProposalId::new([193;16])?, &target_observer.contract("Peek")?,
                vec![], QueryOptions::default()).await?;
            ensure!(child.result==(index+1).to_string().as_bytes(), "child effect was lost or repeated");
        }
        Ok::<_,anyhow::Error>(())
    }).await.map_err(anyhow::Error::from).and_then(std::convert::identity);
    control.proceed.notify_one();
    control.query.release();
    let joined =
        managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
            .await;
    if work.is_err() || joined.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!("retained binding-read disk: {}", root.keep().display());
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
                "binding-read",
                rooted,
            )?))
        } else {
            Arc::new(catalog_fault::Store::default())
        };
        Box::pin(run(rooted, store))
            .await
            .with_context(|| format!("binding read extents={rooted}"))?;
    }
    Ok(())
}
