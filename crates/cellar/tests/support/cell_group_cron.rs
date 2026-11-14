// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The public cron declaration arms itself, checkpoints before handler entry,
//! and resumes the same occurrence after every voter has joined shutdown.

use super::*;
use cellar::sdk::ShutdownMode;

const SCRIPT: &str = "shared-cron-service";
const RETRY: &str = "* * * * *";
const NO_RETRY: &str = "*/1 * * * *";

#[derive(Default)]
struct Observed {
    calls: Mutex<Vec<(String, i64)>>,
    changed: Notify,
    dropped: Notify,
    previous: Mutex<Option<Context>>,
}

struct Dropped(Arc<Observed>);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.dropped.notify_one();
    }
}

struct Unused;

impl Cell for Unused {
    fn invoke(self: Arc<Self>, _: Context, _: Invocation) -> HandlerFuture<Output> {
        Box::pin(async { anyhow::bail!("the cron fixture has no ordinary cell requests") })
    }
}

async fn entries(
    observed: &Observed,
    count: usize,
    timeout: Duration,
) -> Result<Vec<(String, i64)>> {
    cellar::asyncrt::timeout(timeout, async {
        loop {
            let changed = observed.changed.notified();
            let calls = observed.calls.lock().unwrap().clone();
            if calls.len() >= count {
                return calls;
            }
            changed.await;
        }
    })
    .await
    .map_err(Into::into)
}

async fn page(cell: &RaftCellHandle, serial: u8) -> Result<serde_json::Value> {
    let result = cell
        .query(
            ProposalId::new([serial; 16])?,
            &cell.contract("__cronHistory")?,
            b"{}".to_vec(),
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                ..Default::default()
            },
        )
        .await?;
    Ok(serde_json::from_slice(&result.result)?)
}

pub async fn qualify(real: bool) -> Result<()> {
    for rooted in [false, true] {
        for minimum in [Durability::Quorum, Durability::S3] {
            case(real, rooted, minimum).await.with_context(|| {
                format!("cron RustFS={real} extents={rooted} minimum={minimum:?}")
            })?;
        }
    }
    Ok(())
}

async fn case(real: bool, rooted: bool, minimum: Durability) -> Result<()> {
    let backend = real
        .then(|| {
            rustfs::StoreConfig::load(
                if minimum == Durability::S3 {
                    "cron-s3"
                } else {
                    "cron-quorum"
                },
                rooted,
            )
        })
        .transpose()?;
    let store = Arc::new(match &backend {
        Some(config) => catalog_fault::Store::around(config.open(None)?),
        None => catalog_fault::Store::default(),
    });
    let observed = Arc::new(Observed::default());
    let captured = observed.clone();
    let application = Arc::new(
        Registry::new(move |context, invocation| {
            let observed = captured.clone();
            Box::pin(async move {
                let Invocation::Scheduled(controller) = invocation else {
                    anyhow::bail!("expected scheduled invocation")
                };
                ensure!(context.identity().is_none() && context.storage().is_err());
                ensure!(context.var("CRON_VALUE") == Some("admitted"));
                if let Some(previous) = observed.previous.lock().unwrap().replace(context) {
                    ensure!(
                        previous.wait_until(async { Ok(()) }).is_err(),
                        "retired scheduled Context survived"
                    );
                }
                let attempt = {
                    let mut calls = observed.calls.lock().unwrap();
                    calls.push((controller.cron.clone(), controller.scheduled_ms));
                    calls
                        .iter()
                        .filter(|(expression, _)| expression == RETRY)
                        .count()
                };
                observed.changed.notify_one();
                if controller.cron == NO_RETRY {
                    controller.no_retry();
                    anyhow::bail!("this expression declines retry");
                }
                ensure!(controller.cron == RETRY);
                match attempt {
                    1 => {
                        let _dropped = Dropped(observed);
                        std::future::pending::<Result<Output>>().await
                    }
                    2 => anyhow::bail!("retry the interrupted occurrence's known handler error"),
                    _ => Ok(Output::Done),
                }
            })
        })
        .raft_cell(
            RaftClass::new("Counter", 1, [237; 32])?
                .initializer([238; 32], |_, _| Ok(()))?
                .json_cell_rpc("Unused", [239; 32])?,
            |_, _| Ok(Arc::new(Unused)),
        )?,
    );
    let mut options = configuration::options(SCRIPT);
    options.vars.push(("CRON_VALUE".into(), "admitted".into()));
    let crons = vec![RETRY.into(), NO_RETRY.into()];
    let mut deployment = managed_fixture::graph::graph(SCRIPT, "", Vec::new()).primary;
    deployment.options = options.clone();
    deployment.crons = crons.clone();
    application.validate_deployment(&deployment)?;
    let class = application.resolve_raft_class(cellar_logic::cron::RESERVED_CLASS)?;
    let mut fixture =
        Fixture::with_definition_policies(rooted, vec![(class, minimum)], application, store)
            .await?;
    fixture.configuration = Some(options);
    fixture.crons = crons;
    fixture.sql_workers = 1;
    fixture.effects = Some(RaftEffectOptions {
        callback_timeout_ms: 30_000,
        ..Default::default()
    });
    let mut cluster = Box::pin(ManagedCluster::start_with_applications(
        fixture,
        &[],
        ArchiveSetup::Initialized,
        false,
        None,
        FirstGroupSetup::Normal,
    ))
    .await?;
    let domain = cluster.fixtures[0].domain.clone();
    let work = domain
        .run(async {
            // No application traffic initializes the schedule or causes discovery.
            let first = entries(&observed, 1, Duration::from_secs(75)).await?;
            let leader = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
            let id = CellId::new(cellar_logic::cron::reserved_cell(SCRIPT))?;
            let history = cluster.hosts[leader]
                .resolve_cell_history(
                    cluster.history.cluster,
                    cluster.history.namespace,
                    id.clone(),
                    QueryOptions {
                        consistency: QueryConsistency::Linearizable,
                        ..Default::default()
                    },
                )
                .await?
                .context("autonomous cron enrollment")?;
            let cell = cluster.hosts[leader].cell(history.clone())?;
            let started = page(&cell, 240).await?;
            ensure!(
                started["items"]
                    .as_array()
                    .context("cron history")?
                    .iter()
                    .any(|row| { row["scheduled_at"] == first[0].1 && row["status"] == "started" }),
                "handler entry preceded committed occurrence/history"
            );
            let hosts = std::mem::take(&mut cluster.hosts);
            managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &hosts).await?;
            cellar::asyncrt::timeout(TIMEOUT, observed.dropped.notified()).await?;
            ensure!(
                observed.calls.lock().unwrap().len() == 1,
                "old quorum retried before joined shutdown"
            );
            cluster.peers.clear();
            for (index, fixture) in cluster.fixtures.iter().enumerate() {
                let node = fixture.start_node(cluster.roots[index].path()).await?;
                let (mut config, services) = fixture
                    .config(&node, LogOpenMode::Resume, 141 + index as u8)
                    .await?;
                managed_fixture::archives::configure(&mut config);
                let peer = config.peers[0].clone();
                let started = node.start_raft_cells(config, services).await;
                match started {
                    Ok(host) => {
                        cluster.nodes.push(node);
                        cluster.hosts.push(host);
                        cluster.peers.push(peer);
                    }
                    Err(error) => {
                        node.shutdown(ShutdownMode::Preserve).await?;
                        return Err(error);
                    }
                }
            }
            let calls = entries(&observed, 4, Duration::from_secs(45)).await?;
            ensure!(
                calls[..4]
                    .iter()
                    .all(|(_, occurrence)| *occurrence == first[0].1),
                "restart changed the owed occurrence"
            );
            ensure!(
                calls[..4]
                    .iter()
                    .filter(|(expression, _)| expression == NO_RETRY)
                    .count()
                    == 1
            );
            ensure!(
                calls[..4]
                    .iter()
                    .filter(|(expression, _)| expression == RETRY)
                    .count()
                    == 3
            );
            let leader = managed_fixture::leader(&cluster.hosts, cluster.history).await?;
            let cell = cluster.hosts[leader].cell(history)?;
            cellar::asyncrt::timeout(TIMEOUT, async {
                loop {
                    let history = page(&cell, 241).await?;
                    let rows = history["items"].as_array().context("cron history")?;
                    let completed = rows.iter().any(|row| {
                        row["scheduled_at"] == first[0].1
                            && row["expression"] == RETRY
                            && row["status"] == "completed"
                            && row["attempt"] == 3
                    });
                    let declined = rows.iter().any(|row| {
                        row["scheduled_at"] == first[0].1
                            && row["expression"] == NO_RETRY
                            && row["status"] == "failed_no_retry"
                    });
                    if completed && declined {
                        return Ok::<_, anyhow::Error>(());
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
            })
            .await??;
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(std::convert::identity);
    let joined =
        managed_fixture::close_group_nodes(std::mem::take(&mut cluster.nodes), &cluster.hosts)
            .await;
    if work.is_err() || joined.is_err() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!("retained failed cron disk: {}", root.keep().display());
        }
    }
    domain.shutdown().await;
    work?;
    joined
}
