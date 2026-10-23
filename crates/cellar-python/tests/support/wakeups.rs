//! Readiness, cancellation and retirement must not wait for housekeeping.
use super::*;

pub(super) fn child() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let started = std::time::Instant::now();
    let report = run(
        Options {
            poll_interval: Duration::from_secs(30),
            telemetry_interval: Duration::from_secs(30),
            ..Options::default()
        },
        Source::new(include_str!("wakeups.py"), "wakeups.py")
            .events([EventKind::Fetch])
            .cell("Counter"),
        move |application, monitor| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let (node, url) =
                    start_configured(directory.path(), application, "wakeups", None, |config| {
                        config.actor.lease_ttl_ms = 5_000;
                        config.startup.recovery_retry_backoff = Duration::from_secs(5);
                        config.runtime.handler_budget = Duration::from_secs(2);
                    })
                    .await?;
                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(5))
                    .build()?;
                // Wait until the pump is idle. Admission and each nested host
                // reply must wake it before the deliberately long timer.
                tokio::time::sleep(Duration::from_millis(100)).await;
                for _ in 0..3 {
                    let response: Value = client
                        .get(format!("{url}/counter"))
                        .send()
                        .await?
                        .error_for_status()
                        .context("admission/reply waited for housekeeping")?
                        .json()
                        .await?;
                    ensure!(response["value"] == 42);
                }
                let identity: Value = client
                    .get(format!("{url}/counter"))
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                tokio::time::sleep(Duration::from_millis(100)).await;
                // A resident object needs only its owner's lifetime, not its
                // first request's sender, caller scope or event-loop reference.
                let idle = tokio::time::timeout(
                    Duration::from_secs(2),
                    python_control::observe(|v| v["bridges"] == 0),
                )
                .await
                .context("resident Python cell retained a completed invocation bridge")??;
                ensure!(idle["created"] == 1 && idle["retired"] == 0);
                lifetimes::evict(&client, &node, &identity).await?;
                // Inspect flags directly: sending another HTTP request would
                // mask a missing native-retirement or cancellation wakeup.
                tokio::time::timeout(
                    Duration::from_secs(2),
                    python_control::observe(|v| v["retired"] == 1),
                )
                .await
                .context("retirement waited for housekeeping")??;
                let response = client.get(format!("{url}/held")).send().await?;
                ensure!(
                    response.status().is_server_error(),
                    "held invocation escaped native deadline"
                );
                tokio::time::timeout(
                    Duration::from_secs(2),
                    python_control::observe(|v| v["cancelled"] == 1),
                )
                .await
                .context("cancellation waited for housekeeping")??;
                client
                    .get(format!("{url}/lag"))
                    .send()
                    .await?
                    .error_for_status()?;
                stop(node, &monitor).await
            })
        },
    )?;
    ensure!(report.complete(), "wakeup fixture did not join: {report:?}");
    ensure!(
        report.interpreter.loop_delay.maximum_ns >= 40_000_000,
        "long housekeeping interval hid a blocking handler"
    );
    ensure!(
        started.elapsed() < Duration::from_secs(15),
        "host completion waited for housekeeping"
    );
    Ok(())
}
