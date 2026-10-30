// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real fleet and bucket proof faults through Python's original native turns.
use super::fault_gates::{Gate, Peer, Release, Store};
use super::*;
use std::sync::atomic::Ordering;

async fn recovered_state(client: &reqwest::Client, url: &str) -> Result<Value> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let response = client.get(format!("{url}/proof/state")).send().await?;
            if response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                // A read admitted during asynchronous activation reset is
                // refused. Retry only this fixed, read-only route; a failed
                // proof never authorizes replay of /failure or another write.
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
            let status = response.status();
            let body = response.text().await?;
            ensure!(
                status.is_success(),
                "recovered proof read failed: {status}: {body}"
            );
            return Ok::<_, anyhow::Error>(serde_json::from_str(&body)?);
        }
    })
    .await
    .context("read-only proof recovery exceeded its original budget")?
}

pub(super) fn child() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let report = run(
        Options::default(),
        Source::new(include_str!("../bridge.py"), "bridge.py")
            .events([EventKind::Fetch])
            .cell_events("Counter", [EventKind::Fetch, EventKind::Alarm]),
        move |application, monitor| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let bucket =
                    cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
                let peers = Arc::new(Gate::default());
                let data = Arc::new(Gate::default());
                let _release = Release(peers.clone(), data.clone());
                let mut nodes = Vec::new();
                for name in ["proof-b", "proof-c", "proof-a"] {
                    let peers = peers.clone();
                    let data = data.clone();
                    let (node, url) = start_services(
                        &directory.path().join(name),
                        application.clone(),
                        name,
                        Some(bucket.clone()),
                        move |config, services| {
                            config.runtime.handler_budget = Duration::from_secs(15);
                            config.actor.lease_ttl_ms = 5_000;
                            config.startup.recovery_retry_backoff = Duration::from_secs(5);
                            config.maintenance.fleet_sample_period = Duration::from_millis(50);
                            let fleet = services.fleet.as_mut().unwrap();
                            fleet.fleet_durability = true;
                            let store = Arc::new(Store {
                                inner: fleet.bucket.store.clone(),
                                gate: data,
                            });
                            fleet.replica_storage = ReplicaStorage::new(
                                store.clone(),
                                ReplicaStorageOptions::default(),
                            )
                            .unwrap();
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
                    nodes.push((node, url));
                }
                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(10))
                    .build()?;
                let url = nodes.last().unwrap().1.clone();
                tokio::time::timeout(Duration::from_secs(10), async {
                    loop {
                        client
                            .get(format!("{url}/proof/seed"))
                            .send()
                            .await?
                            .error_for_status()?;
                        if peers.members.lock().unwrap().len() >= 2 {
                            return Ok::<_, anyhow::Error>(());
                        }
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                })
                .await??;
                // Explicit sync and final output are independent native proof
                // boundaries. Neither may acknowledge while all proofs stall.
                for kind in ["sync", "output"] {
                    client
                        .get(format!("{url}/proof/reset"))
                        .send()
                        .await?
                        .error_for_status()?;
                    let before_peer = peers.entered.load(Ordering::Acquire);
                    let before_data = data.entered.load(Ordering::Acquire);
                    peers.hold();
                    data.hold();
                    ensure!(
                        peers.is_held() && data.is_held(),
                        "both proof gates must be held before dispatch"
                    );
                    let mut pending = Box::pin(client.get(format!("{url}/proof/{kind}")).send());
                    tokio::time::timeout(Duration::from_secs(5), async {
                        loop {
                            if let futures_util::future::Either::Left((result, _)) =
                                futures_util::future::select(
                                    pending.as_mut(),
                                    Box::pin(tokio::time::sleep(Duration::from_millis(5))),
                                )
                                .await
                            {
                                anyhow::bail!(
                                    "{kind} returned before proof: {:?}",
                                    result.map(|r| r.status())
                                );
                            }
                            if peers.entered.load(Ordering::Acquire) > before_peer
                                && data.entered.load(Ordering::Acquire) > before_data
                            {
                                break;
                            }
                        }
                        Ok::<_, anyhow::Error>(())
                    })
                    .await??;
                    ensure!(
                        tokio::time::timeout(Duration::from_millis(150), &mut pending)
                            .await
                            .is_err(),
                        "{kind} escaped a withheld proof"
                    );
                    let progress: Value = client
                        .get(format!("{url}/proof/progress"))
                        .send()
                        .await?
                        .error_for_status()?
                        .json()
                        .await?;
                    ensure!(
                        progress["sync"] == false && progress["returned"] == (kind == "output"),
                        "Python progressed past a stalled sync or did not reach output: {progress}"
                    );
                    peers.open();
                    // A real fleet proof may release the response while bucket
                    // uploads remain held; no fake acknowledgement is injected.
                    let result =
                        tokio::time::timeout(Duration::from_secs(2), &mut pending).await??;
                    ensure!(
                        result.status().is_success(),
                        "released fleet proof failed: {:?}",
                        result.status()
                    );
                    data.open();
                }
                peers.hold();
                data.hold();
                let mut failed = Box::pin(client.get(format!("{url}/proof/failure")).send());
                let committed = async {
                    loop {
                        let progress: Value = client
                            .get(format!("{url}/proof/progress"))
                            .send().await?.error_for_status()?.json().await?;
                        if progress["failure_committed"] == true {
                            return Ok::<_, anyhow::Error>(());
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                };
                cellar::asyncrt::select! {
                    response = &mut failed => anyhow::bail!("failure request escaped its held proof: {response:?}"),
                    result = tokio::time::timeout(Duration::from_secs(5), committed) => result??,
                }
                // Admit the read against the unproven write before failing its
                // proof. Reopening transports cannot turn that old read into
                // a newly admitted recovery request.
                let mut reading = Box::pin(client.get(format!("{url}/proof/state")).send());
                let entered = async {
                    loop {
                        let progress: Value = client
                            .get(format!("{url}/proof/progress"))
                            .send().await?.error_for_status()?.json().await?;
                        if progress["state_reads"] == 1 {
                            return Ok::<_, anyhow::Error>(());
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                };
                cellar::asyncrt::select! {
                    response = &mut failed => anyhow::bail!("failure request settled before read admission: {response:?}"),
                    response = &mut reading => anyhow::bail!("read escaped the unproven write: {response:?}"),
                    result = tokio::time::timeout(Duration::from_secs(5), entered) => result??,
                }
                peers.fail();
                data.fail();
                let failed = failed.await?;
                ensure!(
                    failed.status().is_server_error(),
                    "unproven mutation was acknowledged: {:?}",
                    failed.status()
                );
                let refused = reading.await?;
                ensure!(
                    refused.status().is_server_error(),
                    "read acknowledged an unproven write: {:?}",
                    refused.status()
                );
                peers.open();
                data.open();
                let attempts: Value = client
                    .get(format!("{url}/proof/attempts"))
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                ensure!(
                    attempts["failure"] == 1,
                    "an unproven mutation handler was blindly replayed: {attempts}"
                );
                let state = recovered_state(&client, &url).await?;
                ensure!(
                    state["sync"] == 1 && state["output"] == 1,
                    "restored proof access lost previously acknowledged state: {state}"
                );
                // Failure is not rollback evidence. A local commit may remain
                // even though its caller received no durability acknowledgement.
                ensure!(
                    state["failure"].is_null() || state["failure"] == 1,
                    "failed proof duplicated the local effect: {state}"
                );
                for (node, _) in nodes.into_iter().rev() {
                    stop(node, &monitor).await?;
                }
                Ok(())
            })
        },
    )?;
    ensure!(report.complete() && report.native_outcomes.len() == 3);
    ensure!(report.bridge.pending_reservations == 0 && report.bridge.queued_bytes == 0);
    Ok(())
}
