//! A committed queue lease survives a lost store receipt and owner recovery.
use super::fault_gates::{Gate, Release, Store};
use super::*;
use std::sync::atomic::Ordering;

#[derive(Debug)]
struct Lease {
    epoch: u64,
    message: Value,
    deadline: u64,
}

async fn snapshot(path: &Path) -> Result<Value> {
    let path = path.to_owned();
    Ok(tokio::task::spawn_blocking(move || super::queue_observation::capture(&path)).await?)
}

fn lease(snapshot: &Value) -> Result<Option<Lease>> {
    ensure!(
        snapshot["truncated"] == false,
        "truncated queue snapshot: {snapshot}"
    );
    ensure!(
        snapshot["discovery_errors"] == json!([]),
        "queue discovery failed: {snapshot}"
    );
    let mut latest = None;
    for database in snapshot["databases"]
        .as_array()
        .context("queue databases")?
    {
        let state = &database["state"];
        // Recovery removes the predecessor database. Its old epoch directory
        // may still appear in the bounded observer's discovery results.
        if state.get("error").is_some() {
            continue;
        }
        for name in ["messages", "alarms", "wakes"] {
            ensure!(
                state[name]["truncated"] == false,
                "truncated {name}: {snapshot}"
            );
        }
        let messages = state["messages"]["rows"].as_array().context("queue rows")?;
        if messages.is_empty() {
            continue;
        }
        ensure!(
            messages.len() == 1,
            "unexpected queue population: {snapshot}"
        );
        let message = &messages[0];
        let Some(deadline) = message["leased_until"].as_u64() else {
            continue;
        };
        ensure!(
            message["lease_id"].is_string(),
            "missing lease identity: {snapshot}"
        );
        let alarms = state["alarms"]["rows"].as_array().context("alarm rows")?;
        let wakes = state["wakes"]["rows"].as_array().context("wake rows")?;
        ensure!(
            alarms.len() <= 1 && wakes.len() <= 1,
            "invalid singleton rows"
        );
        // Leasing and rearming use separate native transactions. Only release
        // the held fault after observing the complete message/alarm/wake state;
        // the caller's finite deadline fails if it never becomes ready.
        if alarms
            .first()
            .is_none_or(|alarm| alarm["at_ms"] != deadline)
            || wakes.first().is_none_or(|wake| wake["at_ms"] != deadline)
        {
            continue;
        }
        let epoch = u64::from_str_radix(
            state["wakes"]["rows"][0]["epoch"]
                .as_str()
                .context("wake epoch")?,
            16,
        )?;
        if latest
            .as_ref()
            .is_none_or(|current: &Lease| current.epoch < epoch)
        {
            latest = Some(Lease {
                epoch,
                message: message.clone(),
                deadline,
            });
        }
    }
    Ok(latest)
}

async fn value(client: &reqwest::Client, url: &str, route: &str) -> Result<Value> {
    Ok(client
        .get(format!("{url}{route}"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

async fn empty(client: &reqwest::Client, url: &str) -> Result<bool> {
    let response = client.get(format!("{url}/metrics")).send().await?;
    // These are read-only observations while native recovery refuses admission.
    if matches!(response.status().as_u16(), 500 | 503) {
        return Ok(false);
    }
    let metrics: Value = response.error_for_status()?.json().await?;
    Ok(metrics["count"] == 0.0 && metrics["bytes"] == 0.0)
}

async fn drain(client: &reqwest::Client, url: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !empty(client, url).await? {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("queue did not drain")?
}

pub(super) fn child() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let report = run(
        Options::default(),
        Source::new(include_str!("queue_lost_ack.py"), "queue_lost_ack.py")
            .events([EventKind::Fetch, EventKind::Queue]),
        move |application, monitor| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let application: Arc<dyn Application> = Arc::new(delivery::Observed(application));
                let gate = Arc::new(Gate::default());
                let _release = Release(gate.clone(), Arc::new(Gate::default()));
                let configure = |config: &mut NodeConfig, services: &mut NodeServices, graph: &mut DeploymentGraph| {
                    config.runtime.handler_budget = Duration::from_secs(15);
                    config.actor.lease_ttl_ms = 5_000;
                    config.startup.recovery_retry_backoff = Duration::from_secs(5);
                    graph.primary.options.bindings.clear();
                    graph.primary.options.do_classes = vec![cellar::deploy::SQL_CLASS.into(), cellar::deploy::QUEUE_CLASS.into()];
                    graph.primary.options.sql_bindings.push(("DB".into(), "lost-ack-db".into()));
                    graph.primary.options.queue_bindings.push(cellar::engine_api::QueueBinding {
                        environment: "JOBS".into(), queue: "lost-ack-queue".into(), delivery_delay: 0,
                    });
                    graph.primary.options.queue_consumers.push(cellar::protocol::QueueConsumerConfig {
                        queue: "lost-ack-queue".into(), max_batch_size: 1, max_batch_timeout: 0,
                        max_retries: 2, dead_letter_queue: None, max_concurrency: Some(1), retry_delay: Some(0),
                    });
                    let fleet = services.fleet.as_mut().expect("lost-ack fleet");
                    let store = Arc::new(Store { inner: fleet.bucket.store.clone(), gate: gate.clone() });
                    fleet.replica_storage = ReplicaStorage::new(store.clone(), ReplicaStorageOptions::default()).expect("replica configuration");
                    fleet.bucket.store = store.clone();
                    fleet.bucket.cas_store = store;
                };
                let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
                let data = directory.path().join("node");
                let (node, url) = start_graph(&data, application.clone(), "queue-lost-ack", Some(bucket.clone()), configure).await?;
                let client = reqwest::Client::builder().timeout(Duration::from_secs(20)).build()?;
                ensure!(value(&client, &url, "/setup").await? == true);
                drain(&client, &url).await?;

                gate.hold();
                let mut pending = Box::pin(client.get(format!("{url}/send")).send());
                let prepared = Box::pin(tokio::time::timeout(Duration::from_millis(1500), async {
                    loop {
                        if let Some(lease) = lease(&snapshot(&data).await?)? {
                            return Ok::<_, anyhow::Error>(lease);
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }));
                let original = match futures_util::future::select(pending.as_mut(), prepared).await {
                    futures_util::future::Either::Left((response, _)) => anyhow::bail!("producer escaped held proof: {:?}", response.map(|r| r.status())),
                    futures_util::future::Either::Right((result, _)) => result.context("queue did not install its held lease")??,
                };
                ensure!(original.message["attempts"] == 0 && original.message["lease_generation"] == 1);
                ensure!(original.deadline - original.message["enqueued_at"].as_u64().context("enqueue time")? >= 21_000);
                gate.lose_ack();
                let response = pending.await;
                gate.open();
                let response: Value = response?.error_for_status()?.json().await?;
                ensure!(response == json!({"code":"Ambiguous", "outcome":"unknown"}), "lost receipt became success: {response}");
                ensure!(gate.lost_acks.load(Ordering::Acquire) > 0, "fault never committed a write");

                let recovered = tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        // Drive only read admission. No producer or settlement is replayed.
                        ensure!(!empty(&client, &url).await?, "recovery discarded the committed message");
                        if let Some(current) = lease(&snapshot(&data).await?)? {
                            if current.epoch > original.epoch { return Ok::<_, anyhow::Error>(current); }
                        }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                }).await.context("lost receipt did not recover ownership")??;
                ensure!(recovered.message == original.message, "recovery replaced or extended the lease: {original:?} -> {recovered:?}");
                let progress = value(&client, &url, "/progress").await?;
                ensure!(progress["calls"] == 1 && progress["deliveries"].as_array().context("deliveries")?.len() == 1,
                    "consumer escaped failed proof or producer replayed: {progress}");

                // A valid recovered lease must cover admission + handler + settlement.
                // Observe its actual persisted deadline, then bound recovery after expiry.
                // The original binding-fault test keeps its separate 10-second assertion.
                let observed = snapshot(&data).await?["observed_unix_ms"].as_u64().context("snapshot time")?;
                ensure!(recovered.deadline > observed + 10_000, "insufficient live-lease observation window");
                tokio::time::timeout(Duration::from_millis(recovered.deadline - observed + 5_000), async {
                    loop {
                        let progress = value(&client, &url, "/progress").await?;
                        ensure!(progress["calls"] == 1, "producer replayed after unknown outcome");
                        for event in progress["deliveries"].as_array().context("deliveries")? {
                            if event["command"] == "lost-ack" {
                                if let Some(at) = event["at_ms"].as_u64() {
                                    ensure!(at >= recovered.deadline, "consumer ran before lease expiry: {event}");
                                }
                            }
                        }
                        if empty(&client, &url).await? { return Ok::<_, anyhow::Error>(()); }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                }).await.context("recovered lease did not expire and settle")??;
                let progress = value(&client, &url, "/progress").await?;
                let events = progress["deliveries"].as_array().context("deliveries")?;
                ensure!(events.len() == 2, "unexpected consumer deliveries: {progress}");
                ensure!(events[1]["id"] == original.message["id"] && events[1]["attempt"] == 2);
                ensure!(events[1]["at_ms"].as_u64().context("delivery time")? >= recovered.deadline);
                let receipts = value(&client, &url, "/state").await?;
                ensure!(receipts.as_array().context("receipts")?.len() == 2);
                ensure!(receipts[0] == json!({"command":"lost-ack", "message":original.message["id"], "attempt":2}));
                // Reconciliation uses the business key, not a fabricated exactly-once producer.
                ensure!(value(&client, &url, "/send").await? == true);
                drain(&client, &url).await?;
                ensure!(value(&client, &url, "/state").await? == receipts, "reconciliation duplicated the business effect");
                stop(node, &monitor).await?;
                let (node, url) = start_graph(&data, application, "queue-lost-ack", Some(bucket), configure).await?;
                drain(&client, &url).await?;
                ensure!(value(&client, &url, "/state").await? == receipts, "acknowledged receipts changed after restart");
                stop(node, &monitor).await
            })
        },
    )?;
    ensure!(
        report.complete()
            && report.native_outcomes.len() == 2
            && report.bridge.operations.settled(),
        "lost-ack recovery did not join: {report:?}"
    );
    Ok(())
}
