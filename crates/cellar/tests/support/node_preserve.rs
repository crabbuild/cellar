//! A retaining restart must work after its last log witness has stopped.
use super::*;

async fn paced_node(
    name: &str,
    directory: &Path,
    core: &Executor,
    bucket: &Bucket,
) -> Result<Node> {
    let (config, mut services) = inputs(name, directory, core, Some(bucket.clone()), 0).await?;
    // Most public-node fixtures disable pacing. Exercise the production bundle
    // path: follower acknowledgements and bucket bundles precede per-cell sync.
    services.fleet.as_mut().unwrap().replication.flush_interval = Duration::from_millis(300);
    Node::start(config, services, graph(), applications("preserved", None)?).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retaining_stop_seals_bundled_writes_before_the_last_node_reloads() -> Result<()> {
    let observer = ExecutionDomain::new(tokio::runtime::Handle::current());
    let outcome = observer
        .run(async {
            let directory = tempfile::tempdir()?;
            let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
            let core = Executor::new("preserve-core")?;
            let path = directory.path().join("leader");
            let leader = paced_node("leader", &path, &core, &bucket).await?;
            let follower = paced_node(
                "follower",
                &directory.path().join("follower"),
                &core,
                &bucket,
            )
            .await?;
            ready(&leader).await?;
            ready(&follower).await?;
            let client = client()?;
            // Wait for a real recruited ensemble before writing. A bucket-only write
            // would bypass the ordering defect this test must exercise.
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let (body, _) = bucket
                        .get("nodes/leader.json")
                        .await?
                        .context("leader lease")?;
                    let record: serde_json::Value = serde_json::from_slice(&body)?;
                    if record["log"]["state"] == "open"
                        && record["log"]["ensemble"]
                            .as_array()
                            .is_some_and(|members| !members.is_empty())
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
            ensure!(
                call(&client, &leader, reqwest::Method::POST, "/write").await? == "preserved:1"
            );
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if bucket
                        .list("log/leader/")
                        .await?
                        .iter()
                        .any(|entry| entry.location.as_ref().contains("/bundle/"))
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await
            .context("write never reached a retained bundle")??;
            let (body, _) = bucket
                .get("nodes/leader.json")
                .await?
                .context("leader lease")?;
            let record: serde_json::Value = serde_json::from_slice(&body)?;
            ensure!(
                record["log"]["may_have_fleet_acks"] == true,
                "fixture did not activate fleet acknowledgements"
            );
            stopped(follower).await?;
            let outcome = leader.run_until(async { ShutdownMode::Preserve }).await?;
            let NodeOutcome::Stopped(report) = outcome else {
                anyhow::bail!("unexpected preserving stop {outcome:?}");
            };
            ensure!(
                report.clean_reload_prepared
                    && report.log_quiesced
                    && report.local_durability_closed
                    && report.ingress_drained
                    && report.cells_drained
                    && report.application_work_drained
                    && !report.deadline_elapsed
                    && !report.drain_timed_out,
                "incomplete preserving stop {report:?}"
            );
            let (body, _) = bucket
                .get("nodes/leader.json")
                .await?
                .context("stopped leader lease")?;
            let record: serde_json::Value = serde_json::from_slice(&body)?;
            ensure!(
                record["log"]["state"] == "sealed",
                "retaining stop left a bundled predecessor open after its witness stopped: {}",
                record["log"]
            );
            let replacement = paced_node("leader", &path, &core, &bucket).await?;
            ready(&replacement).await?;
            ensure!(
                call(&client, &replacement, reqwest::Method::GET, "/read").await? == "preserved:1",
                "solo replacement lost its acknowledged write"
            );
            stopped(replacement).await?;
            Ok::<_, anyhow::Error>(())
        })
        .await;
    observer.shutdown().await;
    outcome?
}
