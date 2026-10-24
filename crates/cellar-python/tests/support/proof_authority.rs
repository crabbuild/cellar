//! Revoke authority after a native commit, while sync/output proof is withheld.
use super::fault_gates::{Gate, Peer, Release, Store};
use super::python_control::{call as control, observe};
use super::*;
use std::sync::atomic::Ordering;

#[path = "proof_recovery.rs"]
mod recovery;

async fn recovered_state(
    client: &reqwest::Client,
    url: &str,
    mut response: Option<reqwest::Response>,
) -> Result<(Value, usize)> {
    let mut refused = 0;
    loop {
        let response = match response.take() {
            Some(response) => response,
            None => client.get(format!("{url}/state")).send().await?,
        };
        if response.status() != reqwest::StatusCode::SERVICE_UNAVAILABLE {
            return Ok((response.error_for_status()?.json().await?, refused));
        }
        // A proof failure resets the activation asynchronously. Requests
        // queued during that reset are refused even after transport recovery.
        // This fixed route contains only reads; never repeat /failure, /seed
        // or any business mutation. The enclosing original audit budget wins.
        refused += 1;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub(super) fn child(kind: &str, fence: bool) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let recovering = kind == "recovery";
    let kind = if recovering { "failure" } else { kind }.to_owned();
    let result = run(
        Options::default(),
        Source::new(include_str!("proof_authority.py"), "proof_authority.py")
            .events([EventKind::Fetch])
            .cell("Counter"),
        move |application, monitor| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let reset = Arc::new(recovery::Reset::default());
                let _reset_release = recovery::Release(reset.clone());
                let application: Arc<dyn Application> = if recovering {
                    Arc::new(recovery::Observed { application, reset: reset.clone() })
                } else {
                    application
                };
                let bucket =
                    cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
                let peers = Arc::new(Gate::default());
                let data = Arc::new(Gate::default());
                let _release = Release(peers.clone(), data.clone());
                let mut nodes = Vec::new();
                let proof_timeout = if kind == "failure" {
                    Duration::from_secs(2)
                } else {
                    Duration::from_secs(30)
                };
                for name in [
                    "authority-proof-b",
                    "authority-proof-c",
                    "authority-proof-a",
                ] {
                    let peers = peers.clone();
                    let data = data.clone();
                    let (node, url) = start_services(
                        &directory.path().join(name),
                        application.clone(),
                        name,
                        Some(bucket.clone()),
                        move |config, services| {
                            config.runtime.handler_budget = Duration::from_secs(10);
                            config.actor.core.operation_deadline_ms = Some(30_000);
                            config.actor.lease_ttl_ms = 5_000;
                            config.startup.recovery_retry_backoff = Duration::from_secs(5);
                            config.maintenance.fleet_sample_period = Duration::from_millis(50);
                            let fleet = services.fleet.as_mut().expect("proof fleet");
                            fleet.fleet_durability = true;
                            // Failure classification needs a live caller when
                            // proof fails; cancellation cases use the reverse
                            // deadline order so cancellation wins first.
                            fleet.replication.durability_timeout = proof_timeout;
                            fleet.log.record_publish_timeout = proof_timeout;
                            let store = Arc::new(Store {
                                inner: fleet.bucket.store.clone(),
                                gate: data,
                            });
                            fleet.replica_storage = ReplicaStorage::new(
                                store.clone(),
                                ReplicaStorageOptions::default(),
                            )
                            .expect("proof replica options");
                            fleet.bucket.store = store.clone();
                            fleet.bucket.cas_store = store;
                            fleet.log_transport = Arc::new(Peer {
                                inner: fleet.log_transport.clone(),
                                gate: peers,
                            });
                        },
                    )
                    .await?;
                    tokio::time::timeout(Duration::from_secs(10), async {
                        while !node.is_ready().await? {
                            tokio::time::sleep(Duration::from_millis(20)).await;
                        }
                        Ok::<_, anyhow::Error>(())
                    })
                    .await??;
                    nodes.push((name, Some(node), url));
                }
                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(20))
                    .build()?;
                let seed_url = nodes.last().context("proof nodes")?.2.clone();
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        ensure!(
                            client
                                .get(format!("{seed_url}/seed"))
                                .send()
                                .await?
                                .error_for_status()?
                                .json::<bool>()
                                .await?
                        );
                        if peers.members.lock().unwrap().len() >= 2 {
                            return Ok::<_, anyhow::Error>(());
                        }
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                })
                .await??;
                let owner = control("snapshot")?["owner"]
                    .as_str()
                    .context("seed owner")?
                    .to_owned();
                let index = nodes
                    .iter()
                    .position(|row| row.0 == owner)
                    .context("actual Python cell owner")?;
                let url = nodes[index].2.clone();
                let before_peer = peers.entered.load(Ordering::Acquire);
                let before_data = data.entered.load(Ordering::Acquire);
                peers.hold();
                data.hold_and_drain().await;
                let held_client = client.clone();
                let address = format!("{url}/{kind}");
                let mut held = tokio::spawn(async move { held_client.get(address).send().await });
                observe(|view| {
                    view["committed"] == true
                        && (kind == "sync"
                            || view["returned"] == true && view["late_entered"] == true)
                        && peers.entered.load(Ordering::Acquire) > before_peer
                        && data.entered.load(Ordering::Acquire) > before_data
                })
                .await?;
                ensure!(
                    tokio::time::timeout(Duration::from_millis(150), &mut held)
                        .await
                        .is_err(),
                    "proof-held mutation returned before cancellation/fence"
                );
                if kind == "failure" {
                    reset.armed.store(recovering, Ordering::Release);
                    peers.fail();
                    data.fail();
                } else if fence {
                    let node = nodes[index].1.take().context("proof owner node")?;
                    let outcome = fencing::force_node_address(&bucket, node, &owner).await?;
                    ensure!(
                        outcome
                            == NodeOutcome::Fenced {
                                code: 3,
                                reason: FenceReason::NodeLeaseMismatch
                            },
                        "proof owner did not actually fence: {outcome:?}"
                    );
                    monitor.record_node(outcome);
                }
                let response = held.await?;
                if kind == "failure" {
                    let response = response?.error_for_status().with_context(|| {
                        format!(
                            "proof failure lost its typed reply: {:?}",
                            control("snapshot")
                        )
                    })?;
                    let value: Value = response.json().await?;
                    ensure!(
                        value == json!({"code":"Ambiguous","outcome":"unknown"}),
                        "native proof classification changed: {value}"
                    );
                } else if fence {
                    if let Ok(response) = response {
                        ensure!(
                            response.status().is_server_error(),
                            "fenced proof returned success"
                        );
                    }
                } else {
                    ensure!(
                        response?.status().is_server_error(),
                        "deadline proof returned success"
                    );
                }
                observe(|view| {
                    view["active"] == false && (kind != "sync" || view["cancelled"] == true)
                })
                .await?;
                // Opening real transports after revocation cannot resurrect a
                // successful response or the original Python SDK authority.
                peers.open();
                data.open();
                control("release")?;
                let final_view = observe(|view| view["late_done"] == true).await?;
                ensure!(
                    final_view["attempts"] == 1,
                    "adapter/native routing replayed the business handler: {final_view}"
                );
                control("clear")?;
                if fence {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                let remaining_url = nodes
                    .iter()
                    .find(|row| row.1.is_some())
                    .context("remaining proof node")?
                    .2
                    .clone();
                let value: Value = tokio::time::timeout(Duration::from_secs(20), async {
                    let response = if recovering {
                        tokio::time::timeout(Duration::from_secs(1), reset.entered.acquire()).await??.forget();
                        let reader = client.clone();
                        let url = remaining_url.clone();
                        let mut reading = recovery::Reading(tokio::spawn(async move { reader.get(format!("{url}/state")).send().await }));
                        observe(|view| view["state_reads"] == 1).await?;
                        ensure!(tokio::time::timeout(Duration::from_millis(150), &mut reading.0).await.is_err(),
                            "state read completed before the held native reset");
                        reset.open();
                        ensure!(!reset.timed_out.load(Ordering::Acquire), "reset barrier timed out");
                        let response = (&mut reading.0).await??;
                        eprintln!("RESET_READ_EVIDENCE status={} attempts={}", response.status(), control("snapshot")?["attempts"]);
                        response
                    } else {
                        client.get(format!("{remaining_url}/state")).send().await?
                    };
                    let (value, refused) = recovered_state(&client, &remaining_url, Some(response)).await?;
                    if recovering {
                        let snapshot = control("snapshot")?;
                        ensure!(refused >= 1 && snapshot["attempts"] == 1 && snapshot["state_reads"].as_u64().is_some_and(|reads| reads >= 2),
                            "reset recovery did not retain the refused read and singular business effect: {snapshot}");
                        eprintln!("RECOVERED_READ_EVIDENCE refused={refused} attempts={} reads={} reset_timeout={}", snapshot["attempts"], snapshot["state_reads"], reset.timed_out.load(Ordering::Acquire));
                    }
                    Ok::<_, anyhow::Error>(value)
                })
                .await
                .context("read-only proof recovery exceeded its original HTTP budget")??;
                ensure!(
                    value["seed"] == 7 && value["late"].is_null() && value["late-raw"].is_null(),
                    "acknowledged/stale state changed: {value}"
                );
                // The pending write lacked acknowledgement. Recovery may keep
                // it or omit it; neither outcome establishes rollback or replay.
                ensure!(
                    value["pending"].is_null() || value["pending"] == 1,
                    "unacknowledged effect duplicated: {value}"
                );
                for (_, node, _) in nodes.into_iter().rev() {
                    if let Some(node) = node {
                        stop(node, &monitor).await?;
                    }
                }
                Ok(())
            })
        },
    );
    let report = if fence {
        let error = result.expect_err("fencing must retain its incomplete native outcome");
        let report = error
            .downcast_ref::<cellar_python::Incomplete>()
            .context("combined proof fence report")?
            .report
            .clone();
        ensure!(
            !report.runtime_failed && !report.host_failed,
            "proof authority failure: {error:?}"
        );
        report
    } else {
        let report = result?;
        ensure!(report.complete());
        report
    };
    ensure!(
        report.native_outcomes.len() == 3
            && report.pending_tasks == 0
            && report.pending_objects == 0
            && report.pending_threads == 0
            && report.bridge.pending_reservations == 0
            && report.bridge.operations.settled(),
        "proof authority leaked work: {report:?}"
    );
    Ok(())
}
