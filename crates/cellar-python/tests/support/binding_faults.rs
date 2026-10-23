//! Independent cancellation, failed proof and replay matrices per binding.
use super::fault_gates::{Gate, Release, Store};
use super::*;
use std::sync::atomic::Ordering;

async fn value(client: &reqwest::Client, url: &str, route: &str) -> Result<Value> {
    Ok(client
        .get(format!("{url}{route}"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

async fn wait_phase(
    client: &reqwest::Client,
    url: &str,
    command: &str,
    phase: &str,
) -> Result<Value> {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let progress = value(client, url, "/progress").await?;
            if progress["progress"][command]["phase"] == phase {
                return Ok(progress);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .with_context(|| format!("{command} did not reach {phase}"))?
}

async fn drained(client: &reqwest::Client, url: &str, phase: &str, node: &Path) -> Result<Value> {
    let mut refused = 0;
    let mut last_metrics = None;
    let mut last_status = None;
    let mut probe = "metrics";
    let mut observations = 0;
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            // Reset is asynchronous after failed proof. These two fixed
            // observations contain only metrics, KV reads and SELECTs; retry
            // their temporary refusal, never a mutation/attempt endpoint.
            probe = "metrics";
            let metrics = client.get(format!("{url}/metrics")).send().await?;
            last_status = Some(metrics.status().as_u16());
            observations += 1;
            if matches!(metrics.status().as_u16(), 500 | 503) {
                refused += 1;
            } else {
                let metrics: Value = metrics.error_for_status()?.json().await?;
                let empty = metrics["count"].as_f64() == Some(0.0);
                last_metrics = Some(metrics);
                if empty {
                    probe = "state";
                    let state = client.get(format!("{url}/state")).send().await?;
                    last_status = Some(state.status().as_u16());
                    if matches!(state.status().as_u16(), 500 | 503) {
                        refused += 1;
                    } else {
                        return Ok(state.error_for_status()?.json().await?);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if result.is_err() {
        // Capture only after the original deadline fails: passing fault timing
        // and mutation behavior stay unchanged. This observer has no SDK handle.
        let node = node.to_owned();
        match tokio::task::spawn_blocking(move || super::queue_observation::capture(&node)).await {
            Ok(snapshot) => eprintln!(
                "[binding-queue-recovery] {}",
                json!({"phase": phase, "snapshot": snapshot})
            ),
            Err(error) => eprintln!("[binding-queue-recovery] observation task failed: {error}"),
        }
    }
    result.with_context(|| {
        format!("binding queue did not recover/drain during {phase}; {refused} read-only probes refused; observations={observations}, probe={probe}, last_status={last_status:?}, last_metrics={last_metrics:?}")
    })?
}

pub(super) fn child() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let report = run(
        Options::default(),
        Source::new(include_str!("binding_faults.py"), "binding_faults.py")
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
                graph.primary.options.do_classes = vec![cellar::deploy::KV_CLASS.into(), cellar::deploy::SQL_CLASS.into(), cellar::deploy::QUEUE_CLASS.into()];
                graph.primary.options.kv_bindings.push(("CACHE".into(), "fault-kv".into()));
                graph.primary.options.sql_bindings.push(("DB".into(), "fault-db".into()));
                graph.primary.options.queue_bindings.push(cellar::engine_api::QueueBinding {
                    environment: "JOBS".into(), queue: "fault-queue".into(), delivery_delay: 0,
                });
                graph.primary.options.queue_consumers.push(cellar::protocol::QueueConsumerConfig {
                    queue: "fault-queue".into(), max_batch_size: 1, max_batch_timeout: 0, max_retries: 2,
                    dead_letter_queue: None, max_concurrency: Some(1), retry_delay: Some(0),
                });
                let fleet = services.fleet.as_mut().expect("binding fault fleet");
                let store = Arc::new(Store { inner: fleet.bucket.store.clone(), gate: gate.clone() });
                fleet.replica_storage = ReplicaStorage::new(store.clone(), ReplicaStorageOptions::default()).expect("valid replica configuration");
                fleet.bucket.store = store.clone();
                fleet.bucket.cas_store = store;
            };
            let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
            let data = directory.path().join("node");
            let (node, url) = start_graph(&data, application.clone(), "binding-faults", Some(bucket.clone()), configure).await?;
            let client = reqwest::Client::builder().timeout(Duration::from_secs(20)).build()?;
            ensure!(value(&client, &url, "/setup").await? == true);
            drained(&client, &url, "setup", &data).await?;
            for binding in ["kv", "sql", "queue"] {
                ensure!(value(&client, &url, &format!("/attempt/{binding}/before")).await? == json!({"phase":"cancelled"}));
                for mode in ["cancel", "failure"] {
                    let command = format!("{binding}-{mode}");
                    let entered = gate.entered.load(Ordering::Acquire);
                    gate.hold();
                    let mut pending = Box::pin(client.get(format!("{url}/attempt/{binding}/{mode}")).send());
                    tokio::time::timeout(Duration::from_secs(5), async {
                        loop {
                            if let futures_util::future::Either::Left((result, _)) = futures_util::future::select(
                                pending.as_mut(), Box::pin(tokio::time::sleep(Duration::from_millis(5)))).await {
                                anyhow::bail!("{command} returned before held data proof: {:?}", result.map(|reply|reply.status()));
                            }
                            if gate.entered.load(Ordering::Acquire) > entered &&
                                value(&client, &url, "/progress").await?["progress"][&command]["phase"] == "pending" { return Ok::<_,anyhow::Error>(()); }
                        }
                    }).await.with_context(||format!("{command} did not reach pending data proof"))??;
                    if mode == "cancel" {
                        ensure!(value(&client, &url, &format!("/cancel/{command}")).await? == true);
                    } else {
                        gate.fail();
                    }
                    // Continue polling the original request while observing a
                    // separate, SDK-free Python handler on the same interpreter.
                    let observe = wait_phase(&client, &url, &command, if mode == "cancel" {"cancelled"} else {"failed"});
                    let release = async {
                        let progress = observe.await?;
                        if mode == "failure" {
                            ensure!(progress["progress"][&command] == json!({"phase":"failed","code":"Ambiguous","outcome":"unknown"}),
                                "binding fabricated rollback: {progress}");
                        }
                        ensure!(progress["calls"][&command] == 1, "adapter replayed a submitted mutation: {progress}");
                        gate.open();
                        Ok::<_,anyhow::Error>(())
                    };
                    let (response, released) = futures_util::future::join(&mut pending, release).await;
                    released?;
                    // A cancelled/failed inner mutation does not establish its
                    // output proof. The outer request is not an effect receipt.
                    let _response = response?;
                    let state = drained(&client, &url, &command, &data).await?;
                    let present = match binding {
                        "kv" => state["kv"][mode] == json!({"receipt":command}),
                        "sql" => state["sql"].as_array().context("SQL receipts")?.iter().any(|row|row["command"] == command),
                        _ => state["queue"].as_array().context("queue receipts")?.iter().any(|row|row["command"] == command),
                    };
                    if mode == "cancel" {
                        ensure!(present, "cancelled {command} did not leave its in-flight effect: {state}");
                    }
                    // Failed proof may retain or discard unacknowledged local
                    // work. Neither outcome changes the unknown SDK verdict.
                    // Reconciliation must converge in both cases.
                    ensure!(value(&client, &url, &format!("/reconcile/{binding}/{mode}")).await? == true);
                    drained(&client, &url, &format!("reconcile {command}"), &data).await?;
                }
                ensure!(value(&client, &url, &format!("/burst/{binding}")).await? == json!({"accepted":1,"refused":31}));
                drained(&client, &url, &format!("burst {binding}"), &data).await?;
            }
            let before = drained(&client, &url, "before restart", &data).await?;
            ensure!(before["kv"]["before"].is_null(), "cancel-before-submit wrote KV: {before}");
            ensure!(before["sql"] == json!([{"command":"sql-burst"},{"command":"sql-cancel"},{"command":"sql-failure"}]), "SQL replay duplicated effects: {before}");
            ensure!(before["queue"] == json!([{"command":"queue-burst"},{"command":"queue-cancel"},{"command":"queue-failure"},{"command":"seed"}]), "queue replay duplicated effects: {before}");
            stop(node, &monitor).await?;
            let (node, url) = start_graph(&data, application, "binding-faults", Some(bucket), configure).await?;
            ensure!(drained(&client, &url, "after restart", &data).await? == before, "binding effects changed across preserving restart");
            stop(node, &monitor).await
        })
        },
    )?;
    ensure!(
        report.complete()
            && report.native_outcomes.len() == 2
            && report.bridge.operations.settled(),
        "binding faults leaked work: {report:?}"
    );
    Ok(())
}
