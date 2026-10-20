//! Force a real lease mismatch while Python holds a live cell context, recover
//! acknowledged state in a new domain, and resume the noncooperative old task.
use super::*;

pub(super) fn child() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let failure = run(
        Options::default(),
        Source::new(include_str!("../bridge.py"), "bridge.py")
            .events([EventKind::Fetch])
            .cell_events("Counter", [EventKind::Fetch, EventKind::Alarm]),
        move |application, monitor| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let bucket =
                    cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
                let (node, url) = start_configured(
                    &directory.path().join("old"),
                    application.clone(),
                    "python-fenced",
                    Some(bucket.clone()),
                    |config| {
                        config.runtime.handler_budget = Duration::from_secs(15);
                        config.actor.lease_ttl_ms = 5_000;
                        config.startup.recovery_retry_backoff = Duration::from_secs(5);
                    },
                )
                .await?;
                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(20))
                    .build()?;
                let seed: Value = client
                    .post(format!("{url}/counter"))
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                ensure!(seed["count"] == 1);
                let held_client = client.clone();
                let held_url = url.clone();
                let held = tokio::spawn(async move {
                    held_client
                        .get(format!("{held_url}/fence-write"))
                        .send()
                        .await
                });
                wait_value(&client, &url, "/fence-waiting", json!(true)).await?;
                let domain =
                    cellar::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
                let fault = domain
                    .run(async {
                        let key = "nodes/python-fenced.json";
                        let (bytes, _) = bucket.get(key).await?.expect("published node lease");
                        let mut lease: Value = serde_json::from_slice(&bytes)?;
                        lease["generation"] = json!("replacement-process");
                        bucket.put(key, serde_json::to_vec(&lease)?).await?;
                        Ok::<_, anyhow::Error>(())
                    })
                    .await;
                domain.shutdown().await;
                fault??;
                let outcome = tokio::time::timeout(
                    Duration::from_secs(10),
                    node.run_until(std::future::pending()),
                )
                .await??;
                ensure!(
                    outcome
                        == NodeOutcome::Fenced {
                            code: 3,
                            reason: FenceReason::NodeLeaseMismatch
                        },
                    "expected a real ownership fence, got {outcome:?}"
                );
                monitor.record_node(outcome);
                let _ = held.await?;
                // The synthetic replacement generation has no renewal task.
                // Honor its published lease before asking a successor to take
                // ownership; fencing is not permission to steal a live lease.
                tokio::time::sleep(Duration::from_secs(5)).await;
                let (successor, successor_url) = start_configured(
                    &directory.path().join("new"),
                    application,
                    "python-successor",
                    Some(bucket),
                    |config| {
                        config.runtime.handler_budget = Duration::from_secs(15);
                        config.actor.lease_ttl_ms = 5_000;
                        config.startup.recovery_retry_backoff = Duration::from_secs(5);
                    },
                )
                .await?;
                // The old cancellation-suppressing frame still owns its
                // instance. Retirement must revoke authority without freeing
                // an object that live Python code is still using.
                wait_value(&client, &successor_url, "/instance-count", json!(1)).await?;
                client
                    .get(format!("{successor_url}/fence-release"))
                    .send()
                    .await?
                    .error_for_status()?;
                wait_value(
                    &client,
                    &successor_url,
                    "/fence-result",
                    json!("ContextExpired"),
                )
                .await?;
                // No successor cell has been invoked yet. Observe real weak
                // collection of the fenced object before creating its replacement.
                wait_value(&client, &successor_url, "/instance-count", json!(0)).await?;
                let recovered = client
                    .get(format!("{successor_url}/fence-state"))
                    .send()
                    .await?;
                let status = recovered.status();
                let body = recovered.text().await?;
                ensure!(
                    status.is_success(),
                    "successor recovery refused ({status}): {body}"
                );
                let state: Value = serde_json::from_str(&body)?;
                ensure!(
                    state == json!({"count":1,"late":false}),
                    "successor state after stale task resumed: {state}"
                );
                stop(successor, &monitor).await
            })
        },
    )
    .expect_err("a fenced native outcome must remain incomplete");
    let incomplete = failure
        .downcast_ref::<cellar_python::Incomplete>()
        .ok_or_else(|| anyhow::anyhow!("missing combined fenced report: {failure}"))?;
    ensure!(
        !incomplete.report.host_failed && !incomplete.report.runtime_failed,
        "fence qualification failed: {failure:?}"
    );
    ensure!(
        incomplete.report.pending_tasks == 0
            && incomplete.report.pending_objects == 0
            && incomplete.report.pending_threads == 0
            && incomplete.report.bridge.pending_reservations == 0,
        "fenced host leaked managed work: {failure}"
    );
    ensure!(incomplete.report.native_outcomes.len() == 2);
    ensure!(
        incomplete.report.objects_created > 0
            && incomplete.report.objects_created == incomplete.report.objects_retired
    );
    Ok(())
}

async fn wait_value(
    client: &reqwest::Client,
    url: &str,
    path: &str,
    expected: Value,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let value: Value = client
                .get(format!("{url}{path}"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            if value == expected {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await??;
    Ok(())
}
