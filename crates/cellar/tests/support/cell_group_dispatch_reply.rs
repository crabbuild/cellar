// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A queued SDK value cannot move into a later invocation of the same live cell.

use super::*;
use std::task::{Context as TaskContext, Wake, Waker};

#[derive(Default)]
struct Control {
    target: Mutex<Option<RaftCellHandle>>,
    constructors: AtomicUsize,
    queries: AtomicUsize,
    refused: AtomicUsize,
    escaped: AtomicUsize,
    fresh: AtomicUsize,
}

#[derive(Default)]
struct Queued(Notify);

impl Wake for Queued {
    fn wake(self: Arc<Self>) {
        self.0.notify_one();
    }
}

struct Holder {
    control: Arc<Control>,
    saved: Mutex<Option<HandlerFuture<Vec<u8>>>>,
}

impl Cell for Holder {
    fn invoke(self: Arc<Self>, _: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::CellRpc { method, .. } = invocation else {
                anyhow::bail!("ordinary RPC required")
            };
            let target = self
                .control
                .target
                .lock()
                .unwrap()
                .clone()
                .context("query target")?;
            match method.as_str() {
                "Hold" => {
                    // Construct the SDK future now so it captures this invocation.
                    // Poll only to register a receiver wake; never consume its value.
                    let query = target.query(
                        ProposalId::new([216; 16])?,
                        &target.contract("Value")?,
                        vec![],
                        QueryOptions::default(),
                    );
                    let mut held: HandlerFuture<Vec<u8>> =
                        Box::pin(async move { Ok(query.await?.result) });
                    let queued = Arc::new(Queued::default());
                    let wake = Waker::from(queued.clone());
                    ensure!(
                        held.as_mut()
                            .poll(&mut TaskContext::from_waker(&wake))
                            .is_pending(),
                        "query must wait for its owned I/O before the custody cut"
                    );
                    *self.saved.lock().unwrap() = Some(held);
                    queued.0.notified().await;
                    ensure!(
                        self.control.queries.load(Ordering::Acquire) == 1,
                        "target query did not run before source completion"
                    );
                }
                "Consume" => {
                    let saved = self
                        .saved
                        .lock()
                        .unwrap()
                        .take()
                        .context("held query future")?;
                    match saved.await {
                        Ok(value) => {
                            ensure!(value == b"42", "unexpected held query value");
                            self.control.escaped.fetch_add(1, Ordering::AcqRel);
                            return Ok(Output::Rpc(RpcData::Json("\"escaped\"".into())));
                        }
                        Err(error) => ensure!(
                            format!("{error:#}")
                                .contains("storage belongs to another Raft command"),
                            "held query failed for another reason: {error:#}"
                        ),
                    }
                    self.control.refused.fetch_add(1, Ordering::AcqRel);
                    let current = target
                        .query(
                            ProposalId::new([217; 16])?,
                            &target.contract("Value")?,
                            vec![],
                            QueryOptions::default(),
                        )
                        .await?;
                    ensure!(current.result == b"42", "current query changed value");
                    self.control.fresh.fetch_add(1, Ordering::AcqRel);
                }
                _ => anyhow::bail!("unknown reply lifetime method"),
            }
            Ok(Output::Rpc(RpcData::Json("\"checked\"".into())))
        })
    }
}

pub async fn qualify() -> Result<()> {
    for rooted in [false, true] {
        let control = Arc::new(Control::default());
        let queries = control.clone();
        let class = RaftClass::new("Counter", 1, [210; 32])?
            .initializer([211; 32], |_, tx| {
                tx.storage()?.put_json("value", &json!(42))
            })?
            .json_cell_rpc("Hold", [212; 32])?
            .json_cell_rpc("Consume", [213; 32])?
            .query("Value", [214; 32], move |query, _| {
                queries.queries.fetch_add(1, Ordering::AcqRel);
                Ok(query
                    .storage()
                    .get_json("value")?
                    .context("target value")?
                    .to_string()
                    .into_bytes())
            })?;
        let instances = control.clone();
        let app = Arc::new(
            Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).raft_cell(
                class,
                move |_, _| {
                    instances.constructors.fetch_add(1, Ordering::AcqRel);
                    Ok(Arc::new(Holder {
                        control: instances.clone(),
                        saved: Mutex::new(None),
                    }))
                },
            )?,
        );
        let config = configuration::options("sdk-reply-lifetime");
        app.validate_deployment(&configuration::deployment(config.clone()))?;
        let mut fixture = Fixture::with_application(
            rooted,
            "Counter",
            app,
            Arc::new(catalog_fault::Store::default()),
        )
        .await?;
        fixture.configuration = Some(config);
        fixture.sql_workers = 1;
        // The parent and the deliberately retained target output share this
        // bounded bank; a fresh current query is the third admitted operation.
        fixture.ordinary_admission = Some((3, 64 * 1024));
        let domain = fixture.domain.clone();
        let mut cluster = Box::pin(ManagedCluster::start_with_applications(
            fixture,
            &[],
            ArchiveSetup::Initialized,
            false,
            None,
            FirstGroupSetup::Normal,
        ))
        .await?;
        let work = domain.run(async {
            let leader = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
            let history = CellHistory {
                cluster: cluster.history.cluster, namespace: cluster.history.namespace,
                cell: CellId::new("Counter:reply-source")?, incarnation: IncarnationId::new([210; 16])?,
            };
            let source = cluster.hosts[leader].cell(history.clone())?;
            let target = cluster.hosts[leader].cell(CellHistory {
                cell: CellId::new("Counter:reply-target")?, ..history
            })?;
            for (cell, id) in [(&source, 211), (&target, 212)] {
                cell.enroll(CellEnrollmentRequest { creation: ProposalId::new([id; 16])?, durability: Durability::Quorum }, TIMEOUT).await?;
            }
            *control.target.lock().unwrap() = Some(target);
            source.command(&source.contract("Hold")?, b"{}".to_vec(), interleaving::command(215, 1, Durability::Quorum)?).await?;
            let options = interleaving::command(215, 2, Durability::Quorum)?;
            let consumed = source.command(&source.contract("Consume")?, b"{}".to_vec(), options.clone()).await;
            eprintln!("queued SDK reply extents={rooted}: constructors={} queries={} refused={} escaped={} fresh={}",
                control.constructors.load(Ordering::Acquire), control.queries.load(Ordering::Acquire),
                control.refused.load(Ordering::Acquire), control.escaped.load(Ordering::Acquire), control.fresh.load(Ordering::Acquire));
            let consumed = consumed?;
            ensure!(control.constructors.load(Ordering::Acquire) == 1, "test lost the cached source activation");
            ensure!(control.refused.load(Ordering::Acquire) == 1 && control.escaped.load(Ordering::Acquire) == 0
                && control.fresh.load(Ordering::Acquire) == 1 && control.queries.load(Ordering::Acquire) == 2,
                "queued SDK reply crossed its invocation, or current query did not progress");
            let replay = source.command(&source.contract("Consume")?, b"{}".to_vec(), options).await?;
            ensure!(replay.result == consumed.result && replay.version == consumed.version);
            ensure!(control.queries.load(Ordering::Acquire) == 2 && control.refused.load(Ordering::Acquire) == 1,
                "receipt replay repeated SDK work");
            Ok::<_, anyhow::Error>(())
        }).await.map_err(anyhow::Error::from).and_then(std::convert::identity);
        control.target.lock().unwrap().take();
        let joined =
            managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
                .await;
        if work.is_err() || joined.is_err() {
            for root in std::mem::take(&mut cluster.roots) {
                eprintln!("retained queued SDK reply disk: {}", root.keep().display());
            }
        }
        domain.shutdown().await;
        work?;
        joined?;
    }
    Ok(())
}
