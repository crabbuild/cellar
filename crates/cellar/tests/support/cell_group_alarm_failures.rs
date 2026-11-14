// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Returned errors roll back private writes before durable retry publication.
use super::*;
use std::sync::atomic::AtomicI64;

#[derive(Default)]
pub(super) struct Control {
    remaining: AtomicUsize,
    gate: AtomicBool,
    waiting: AtomicBool,
    entered: Notify,
    release: Notify,
    failed_at: AtomicI64,
    entries: Mutex<Vec<i64>>,
}

impl Control {
    fn arm(&self, failures: usize) {
        self.remaining.store(failures, Ordering::Release);
        self.failed_at.store(0, Ordering::Release);
        self.gate.store(true, Ordering::Release);
    }

    async fn wait_gate(&self) -> Result<()> {
        cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                let changed = self.entered.notified();
                if self.waiting.load(Ordering::Acquire) {
                    break;
                }
                changed.await;
            }
        })
        .await
        .context("failing callback did not reach its proven checkpoint")?;
        Ok(())
    }
}

pub(super) fn snapshot(storage: &Storage) -> Result<serde_json::Value> {
    Ok(json!({"alarm":storage.get_alarm()?,
        "checkpoint":storage.get_json("failure_checkpoint")?,
        "unsynced":storage.get_json("failure_unsynced")?,
        "completed":storage.get_json("failure_completed")?}))
}

pub(super) async fn invoke(cell: &AlarmCell, storage: &Storage, label: &str) -> Result<Output> {
    let control = &cell.observed.failure;
    control
        .entries
        .lock()
        .unwrap()
        .push(cellar::asyncrt::wall_ms());
    control.entered.notify_waiters();
    let failing = control
        .remaining
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
        .is_ok();
    if failing {
        storage.put_json("failure_checkpoint", &json!(1))?;
        if label == "failure/replace" {
            storage.set_alarm(cellar::asyncrt::wall_ms() + 60_000)?;
        } else if label == "failure/cancel" {
            storage.delete_alarm()?;
        }
        storage.sync().await?;
        storage.put_json("failure_unsynced", &json!(1))?;
        if control.gate.swap(false, Ordering::AcqRel) {
            control.waiting.store(true, Ordering::Release);
            control.entered.notify_waiters();
            control.release.notified().await;
            control.waiting.store(false, Ordering::Release);
        }
        cell.poisoned.store(true, Ordering::Release);
        control
            .failed_at
            .store(cellar::asyncrt::wall_ms(), Ordering::Release);
        anyhow::bail!("intended ordinary alarm handler failure");
    }
    ensure!(
        storage.get_json("failure_unsynced")?.is_none(),
        "failed application writes committed"
    );
    ensure!(
        storage.get_json("failure_checkpoint")? == Some(json!(1)),
        "earlier explicit checkpoint was lost"
    );
    storage.put_json("failure_completed", &json!(true))?;
    Ok(Output::Done)
}

async fn state(cluster: &ManagedCluster, history: &CellHistory) -> Result<serde_json::Value> {
    let cell = cluster.hosts[0].cell(history.clone())?;
    let value = cell
        .query(
            ProposalId::new([75; 16])?,
            &cell.contract("FailureState")?,
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                ..Default::default()
            },
        )
        .await?;
    Ok(serde_json::from_slice(&value.result)?)
}

async fn backoff(
    cluster: &ManagedCluster,
    history: &CellHistory,
    control: &Control,
    lease: i64,
) -> Result<serde_json::Value> {
    cellar::asyncrt::timeout(TIMEOUT, async {
        loop {
            let value = state(cluster, history).await?;
            let failed_at = control.failed_at.load(Ordering::Acquire);
            if let Some(due) = value["alarm"].as_i64().filter(|due| {
                failed_at > 0
                    && *due < lease
                    && *due >= failed_at + 2_000
                    && *due <= failed_at + 2_500
            }) {
                ensure!(value["checkpoint"] == 1 && value["unsynced"].is_null());
                eprintln!(
                    "ALARM_BACKOFF {}",
                    json!({"lease":lease,"failedAt":failed_at,"retryAt":due})
                );
                return Ok(value);
            }
            cellar::asyncrt::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("returned handler error did not persist its two-second backoff")?
}

pub async fn qualify(real: bool, external: bool) -> Result<()> {
    qualify_cases(real, external, false).await
}

pub async fn qualify_exhaustion(real: bool) -> Result<()> {
    qualify_cases(real, false, true).await
}

async fn qualify_cases(real: bool, external: bool, exhaust: bool) -> Result<()> {
    let mut failures = vec![];
    for rooted in [false, true] {
        for minimum in [Durability::Quorum, Durability::S3] {
            let result = case(real, external, rooted, minimum, exhaust).await;
            eprintln!("Alarm failure RustFS={real} Axum={external} extents={rooted} minimum={minimum:?} exhaustion={exhaust}: {}",if result.is_ok(){"PASS"}else{"FAILED"});
            if let Err(error) = result {
                failures.push(format!("extents={rooted} minimum={minimum:?}: {error:#}"));
            }
        }
    }
    ensure!(
        failures.is_empty(),
        "alarm failure qualification: {}",
        failures.join("; ")
    );
    Ok(())
}

async fn case(
    real: bool,
    external: bool,
    rooted: bool,
    minimum: Durability,
    exhaust: bool,
) -> Result<()> {
    let backend = real
        .then(|| {
            rustfs::StoreConfig::load(
                &format!("alarm-failures-{minimum:?}-axum-{external}-exhaust-{exhaust}"),
                rooted,
            )
        })
        .transpose()?;
    let store = Arc::new(match &backend {
        Some(config) => catalog_fault::Store::around(config.open(None)?),
        None => catalog_fault::Store::default(),
    });
    let observed = Arc::new(Observed::default());
    let (application, declared) = application(observed.clone())?;
    let class = application.resolve_raft_class("Counter")?;
    let mut fixture =
        Fixture::with_definition_policies(rooted, vec![(class, minimum)], application, store)
            .await?;
    fixture.configuration = Some(declared);
    fixture.sql_workers = 1;
    fixture.effects = Some(RaftEffectOptions {
        callback_timeout_ms: 5000,
        ..Default::default()
    });
    let http = external.then(|| Arc::new(managed_fixture::http::Owner::default()));
    fixture.external_http = http.clone();
    let domain = fixture.domain.clone();
    let started = Box::pin(ManagedCluster::start_with_applications(
        fixture,
        &[],
        ArchiveSetup::Initialized,
        false,
        None,
        FirstGroupSetup::Normal,
    ))
    .await;
    let mut cluster = match started {
        Ok(cluster) => cluster,
        Err(error) => {
            if let Some(http) = http {
                http.close().await?;
            }
            domain.shutdown().await;
            return Err(error);
        }
    };
    let work = domain
        .run(async {
            named_service::grant(&cluster).await?;
            managed_fixture::leader(&cluster.hosts, cluster.history).await?;
            request(&cluster, "Read", json!(null)).await?;
            let namespace = cellar_native::cell_id::Namespace::new(
                "Counter",
                format!("cells:v1:{}:{SCRIPT}:Counter", SCRIPT.len()),
            );
            let group = cluster.history;
            let history = cluster.hosts[0]
                .resolve_cell_history(
                    group.cluster,
                    group.namespace,
                    CellId::new(namespace.id_from_name("alarm").scope())?,
                    QueryOptions::default(),
                )
                .await?
                .context("public alarm history")?;
            let control = &observed.failure;
            control.arm(if exhaust { usize::MAX } else { 1 });
            request(
                &cluster,
                "Arm",
                json!({"label":"failure/checkpoint","after":100}),
            )
            .await?;
            control.wait_gate().await?;
            let before = state(&cluster, &history).await?;
            ensure!(before["checkpoint"] == 1 && before["unsynced"].is_null());
            let lease = before["alarm"]
                .as_i64()
                .context("committed alarm claim lease")?;
            ensure!(
                lease - cellar::asyncrt::wall_ms() > 2500,
                "test must release before lease and retry deadlines can coincide"
            );
            control.release.notify_one();
            backoff(&cluster, &history, control, lease).await?;
            // This mutation needs a current instance, before restart can retire it.
            request(&cluster, "Read", json!(null)).await?;
            bindings::restart(&mut cluster).await?;
            // No client request discovers the next installation after restart.
            let window = if exhaust {
                Duration::from_millis(126_000 + 7 * 5000)
            } else {
                Duration::from_secs(20)
            };
            cellar::asyncrt::timeout(window, async {
                loop {
                    let changed = control.entered.notified();
                    if control.entries.lock().unwrap().len() >= if exhaust { 7 } else { 2 } {
                        break;
                    }
                    changed.await;
                }
            })
            .await
            .context("alarm retries did not resume autonomously")?;
            cellar::asyncrt::timeout(TIMEOUT, async {
                loop {
                    let value = state(&cluster, &history).await?;
                    if value["alarm"].is_null() {
                        ensure!(value["checkpoint"] == 1 && value["unsynced"].is_null());
                        if exhaust {
                            ensure!(value["completed"].is_null());
                        } else {
                            ensure!(value["completed"] == true);
                        }
                        break Ok::<_, anyhow::Error>(());
                    }
                    cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .context("alarm retry final state")??;
            let times = control.entries.lock().unwrap().clone();
            ensure!(times.len() == if exhaust { 7 } else { 2 });
            for (index, pair) in times.windows(2).enumerate() {
                ensure!(
                    pair[1] - pair[0] >= 2_000 * (1_i64 << index),
                    "alarm retried before its backoff"
                );
            }
            if !exhaust {
                for label in ["failure/replace", "failure/cancel"] {
                    control.arm(1);
                    request(&cluster, "Arm", json!({"label":label,"after":100})).await?;
                    control.wait_gate().await?;
                    let replacement = state(&cluster, &history).await?["alarm"].clone();
                    control.release.notify_one();
                    request(&cluster, "Read", json!(null)).await?;
                    let value = state(&cluster, &history).await?;
                    ensure!(
                        value["alarm"] == replacement && value["unsynced"].is_null(),
                        "old failed attempt changed a replacement or committed private writes"
                    );
                    request(&cluster, "Cancel", json!(null)).await?;
                }
            }
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(std::convert::identity);
    let mut cleanup = Vec::new();
    for node in std::mem::take(&mut cluster.nodes) {
        match node.shutdown(cellar::sdk::ShutdownMode::Preserve).await {
            Ok(cellar::sdk::NodeOutcome::Stopped(_)) => (),
            result => cleanup.push(format!("alarm node shutdown: {result:?}")),
        }
    }
    for host in &cluster.hosts {
        match host.close().await {
            Ok(report)
                if report.groups_total == 1
                    && report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers => (),
            result => cleanup.push(format!("alarm host closure: {result:?}")),
        }
    }
    if let Some(http) = http {
        if let Err(error) = http.close().await {
            cleanup.push(error.to_string());
        }
    }
    if work.is_err() || !cleanup.is_empty() {
        for root in std::mem::take(&mut cluster.roots) {
            eprintln!("retained alarm failure disk: {}", root.keep().display());
        }
    }
    domain.shutdown().await;
    work?;
    ensure!(cleanup.is_empty(), "alarm failure cleanup: {cleanup:?}");
    Ok(())
}
