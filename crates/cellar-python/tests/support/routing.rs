//! Real core residency refusal, retaining its typed cause through Python.
use super::python_control::{call as control, observe};
use super::*;

pub(super) fn child() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let report = run(
        Options::default(),
        Source::new(include_str!("routing.py"), "routing.py")
            .events([EventKind::Fetch])
            .cell("Counter"),
        move |application, monitor| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let bucket =
                    cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
                let (node, url) = start_configured(
                    directory.path(),
                    application,
                    "routing",
                    Some(bucket),
                    |config| {
                        config.runtime.handler_budget = Duration::from_secs(10);
                        config.actor.core.max_resident = 1;
                        config.actor.core.max_activations = 1;
                        config.actor.core.operation_deadline_ms = Some(1_000);
                        config.actor.lease_ttl_ms = 5_000;
                        config.startup.recovery_retry_backoff = Duration::from_secs(5);
                    },
                )
                .await?;
                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(15))
                    .build()?;
                let held_client = client.clone();
                let held_url = url.clone();
                let held = tokio::spawn(async move {
                    held_client.get(format!("{held_url}/hold")).send().await
                });
                observe(|view| view["entered"] == true).await?;
                let refusal: Value = client
                    .get(format!("{url}/probe"))
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                ensure!(
                    refusal == json!({"code":"Overloaded","outcome":"unknown"}),
                    "core routing cause lost: {refusal}"
                );
                ensure!(
                    control("snapshot")?["cold_visits"] == 0,
                    "refused handler entered Python"
                );
                control("release")?;
                ensure!(held.await??.error_for_status()?.json::<bool>().await?);
                // Explicit new read-only request after capacity becomes free;
                // the adapter never replays the refused call automatically.
                ensure!(
                    client
                        .get(format!("{url}/after"))
                        .send()
                        .await?
                        .error_for_status()?
                        .json::<bool>()
                        .await?
                );
                ensure!(control("snapshot")?["cold_visits"] == 1);
                stop(node, &monitor).await
            })
        },
    )?;
    ensure!(
        report.complete()
            && report.bridge.operations.settled()
            && report.bridge.pending_reservations == 0
    );
    Ok(())
}
