// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Revoke authority after a native commit, while sync/output proof is withheld.
use super::fault_gates::{Gate, Peer, Release, Store};
use super::python_control::{call as control, observe};
use super::*;
use sha2::{Digest, Sha256};
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
            let status = response.status();
            let body = response.text().await?;
            ensure!(
                status.is_success(),
                "proof state read failed: {status} {body}"
            );
            return Ok((serde_json::from_str(&body)?, refused));
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
    scenario(kind, fence, None)
}

pub(super) fn filesystem(barrier: &str, fence: bool) -> Result<()> {
    ensure!(matches!(barrier, "fsync" | "fdatasync" | "sync" | "output"));
    scenario(
        if barrier == "output" {
            "output"
        } else {
            "sync"
        },
        fence,
        Some(barrier),
    )
}

fn configure(
    config: &mut NodeConfig,
    services: &mut NodeServices,
    peers: Arc<Gate>,
    data: Arc<Gate>,
    proof_timeout: Duration,
    filesystem: bool,
) {
    config.runtime.handler_budget = Duration::from_secs(10);
    config.actor.core.operation_deadline_ms = Some(30_000);
    config.actor.lease_ttl_ms = 5_000;
    config.startup.recovery_retry_backoff = Duration::from_secs(5);
    config.maintenance.fleet_sample_period = Duration::from_millis(50);
    if filesystem {
        config.cell_limits.filesystem.max_host_handles = 4;
    }
    let fleet = services.fleet.as_mut().expect("proof fleet");
    fleet.fleet_durability = true;
    fleet.replication.durability_timeout = proof_timeout;
    fleet.log.record_publish_timeout = proof_timeout;
    let store = Arc::new(Store {
        inner: fleet.bucket.store.clone(),
        gate: data,
    });
    fleet.replica_storage = ReplicaStorage::new(store.clone(), ReplicaStorageOptions::default())
        .expect("proof replica options");
    fleet.bucket.store = store.clone();
    fleet.bucket.cas_store = store;
    fleet.log_transport = Arc::new(Peer {
        inner: fleet.log_transport.clone(),
        gate: peers,
    });
}

fn verify_filesystem(value: &Value, generation: u8) -> Result<()> {
    let mut bytes = vec![generation; 3 * 65536 + 17];
    bytes[65535] = generation + 8;
    bytes[131072] = generation + 16;
    ensure!(
        value["generation"] == generation && value["size"] == bytes.len(),
        "filesystem generation/size mismatch: {value}"
    );
    ensure!(
        value["sha256"] == format!("{:x}", Sha256::digest(bytes)),
        "filesystem content hash mismatch: {value}"
    );
    for field in ["inode", "version"] {
        ensure!(
            value[field]
                .as_str()
                .context("filesystem identity field")?
                .parse::<u64>()?
                > 0
        );
    }
    Ok(())
}

fn scenario(kind: &str, fence: bool, barrier: Option<&str>) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let recovering = kind == "recovery";
    let kind = if recovering { "failure" } else { kind }.to_owned();
    let filesystem = barrier.is_some();
    let symbol = barrier
        .map(serde_json::to_string)
        .transpose()?
        .unwrap_or_else(|| "None".to_owned());
    let source = Source::new(
        format!(
            "FILESYSTEM_BARRIER = {symbol}\n{}\n{}",
            include_str!("proof_filesystem.py"),
            include_str!("proof_authority.py")
        ),
        "proof_authority.py",
    )
    .events([EventKind::Fetch])
    .cell("Counter");
    let source = if filesystem {
        source.filesystem("Counter", cellar::sdk::filesystem::Policy::default())?
    } else {
        source
    };
    let evidence = Arc::new(std::sync::Mutex::new(None));
    let recorded = evidence.clone();
    let barrier = barrier.map(str::to_owned);
    let result = run(Options::default(), source, move |application, monitor| {
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
                            // Failure classification needs a live caller when
                            // proof fails; cancellation cases use the reverse
                            // deadline order so cancellation wins first.
                            configure(config, services, peers, data, proof_timeout, filesystem);
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
                        && (!filesystem || kind != "sync" || view["filesystem"]["barrier_entered"] == true)
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
                if filesystem {
                    ensure!(final_view["filesystem"]["refusals"] == 32, "retired filesystem proof admitted work: {final_view}");
                } else {
                    control("clear")?;
                }
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
                .context("read-only proof recovery exceeded its original HTTP budget")?
                .context("warm proof state readback")?;
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
                if filesystem {
                    verify_filesystem(&value["filesystem"], if value["pending"].is_null() { 1 } else { 2 })?;
                }
                for (_, node, _) in nodes.into_iter().rev() {
                    if let Some(node) = node {
                        stop(node, &monitor).await?;
                    }
                }
                if filesystem {
                    // Preserve leaves published ownership in place. New logical
                    // identities must honor the original five-second lease.
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let mut cold = Vec::new();
                    for name in ["authority-proof-cold-b", "authority-proof-cold-c", "authority-proof-cold-a"] {
                        let peers = peers.clone();
                        let data = data.clone();
                        let (node, url) = start_services(&directory.path().join(name), application.clone(), name, Some(bucket.clone()), move |config, services| {
                            configure(config, services, peers, data, proof_timeout, true);
                        }).await?;
                        tokio::time::timeout(Duration::from_secs(10), async {
                            while !node.is_ready().await? {
                                tokio::time::sleep(Duration::from_millis(20)).await;
                            }
                            Ok::<_, anyhow::Error>(())
                        }).await??;
                        cold.push((node, url));
                    }
                    let address = &cold.last().context("cold filesystem node")?.1;
                    let (restored, _) = tokio::time::timeout(Duration::from_secs(20), recovered_state(&client, address, None)).await.context("cold proof state deadline")?.context("cold proof state readback")?;
                    ensure!(restored == value, "cold file/state/receipt recovery changed the proven readback: {restored} vs {value}");
                    let written: Value = client.get(format!("{address}/post-recovery")).send().await?.error_for_status()?.json().await?;
                    verify_filesystem(&written, 3)?;
                    ensure!(written["inode"] == restored["filesystem"]["inode"]
                        && written["version"].as_str().context("new content version")?.parse::<u64>()?
                            > restored["filesystem"]["version"].as_str().context("restored content version")?.parse::<u64>()?,
                        "fresh recovery write did not advance the existing file: {written}");
                    let (after, _) = recovered_state(&client, address, None).await?;
                    ensure!(after["filesystem"] == written && after["seed"] == value["seed"]
                        && after["pending"] == value["pending"]
                        && after["late"].is_null() && after["late-raw"].is_null());
                    for (node, _) in cold.into_iter().rev() { stop(node, &monitor).await?; }
                    ensure!(control("snapshot")?["attempts"] == 1, "business handler replayed during recovery");
                    *recorded.lock().unwrap() = Some(json!({
                        "format": 1, "barrier": barrier,
                        "termination": if fence { "node_lease_mismatch" } else { "handler_deadline" },
                        "scope": "supplied development bucket and authenticated loopback peer transport",
                        "proof_held_after_local_commit": true,
                        "peer_entries_after_hold": peers.entered.load(Ordering::Acquire) - before_peer,
                        "bucket_entries_after_hold": data.entered.load(Ordering::Acquire) - before_data,
                        "expired_context": final_view,
                        "warm_recovery": value, "cold_recovery": restored,
                        "post_recovery_write": written,
                        "cold_local_storage": "new directories after Preserve and original lease expiry",
                        "retained_guest_wrappers": true, "host_slots_reused": 4,
                        "business_handler_attempts": 1,
                    }));
                    control("clear")?;
                }
                Ok(())
            })
    });
    let report = if fence {
        let error = result.expect_err("fencing must retain its incomplete native outcome");
        let report = error
            .downcast_ref::<cellar_python::Incomplete>()
            .context("combined proof fence report")?
            .report
            .clone();
        // The exact fence was asserted when joining the native owner. Every
        // other native outcome and every interpreter/bridge cleanup invariant
        // must still satisfy the normal complete-report contract.
        let mut cleanup = report.clone();
        cleanup
            .native_outcomes
            .retain(|outcome| outcome["complete"] == true);
        ensure!(
            cleanup.native_outcomes.len() + 1 == report.native_outcomes.len() && cleanup.complete(),
            "proof authority failure beyond the expected fence: {error:?}"
        );
        report
    } else {
        let report = result?;
        ensure!(report.complete());
        report
    };
    ensure!(
        report.native_outcomes.len() == if filesystem { 6 } else { 3 }
            && report.pending_tasks == 0
            && report.pending_objects == 0
            && report.pending_threads == 0
            && report.bridge.pending_reservations == 0
            && report.bridge.operations.settled(),
        "proof authority leaked work: {report:?}"
    );
    if let Some(mut evidence) = evidence.lock().unwrap().take() {
        evidence["joined_native_outcomes"] = json!(report.native_outcomes.len());
        evidence["interpreter_and_bridge_settled"] = json!(true);
        evidence["shutdown_report"] = serde_json::to_value(&report)?;
        evidence["gil_enabled"] = evidence["expired_context"]["filesystem"]["gil_enabled"].clone();
        if let Some(path) = std::env::var_os("CELLAR_FS_PROOF_REPORT") {
            std::fs::write(
                std::path::PathBuf::from(path),
                serde_json::to_vec_pretty(&evidence)?,
            )?;
        }
    }
    Ok(())
}
