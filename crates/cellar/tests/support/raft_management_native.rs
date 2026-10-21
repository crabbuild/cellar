//! Real authenticated management stops with a registered public Raft cell host.
use super::{faults, log_fault, public_node, registration, wait_status};
use anyhow::{ensure, Context as _, Result};
use cellar::management::{FleetClient, NodeAction, RaftStopProof, StopMode, StopProof};
use cellar::sdk::{consensus::*, ExecutionDomain, NodeOutcome, ShutdownMode, ShutdownPolicy};
use std::{
    sync::{atomic::AtomicUsize, atomic::Ordering, Arc},
    time::Duration,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Scenario {
    PendingSql,
    ArchiveLag,
    Ambiguous,
    Cancelled,
    CloseError,
    JoinDeadline,
}

pub async fn cancelled_start() -> Result<()> {
    let root = tempfile::tempdir()?;
    let management = ExecutionDomain::new(tokio::runtime::Handle::current());
    let bucket = management
        .run(async {
            let bucket = cellar::bucket::Bucket::open_dev(&root.path().join("fleet.sqlite"))?;
            bucket
                .put(
                    "fleet/peer-auth.json",
                    serde_json::to_vec(&serde_json::json!({"version":1,"key":"0b".repeat(32)}))?,
                )
                .await?;
            Ok::<_, anyhow::Error>(bucket)
        })
        .await??;
    let (sql_gate, _) = public_node::Gate::new();
    sql_gate.release();
    let calls = Arc::new(AtomicUsize::new(0));
    let definition = public_node::counter(calls.clone(), sql_gate)?;
    let registration = registration(definition.schema)?;
    let log_gate = log_fault::Gate::new(None);
    let mut node = Some(
        public_node::node_with_fleet(
            &root.path().join("node"),
            definition.application.clone(),
            ShutdownPolicy {
                total_ms: 3000,
                token_wait_ms: 0,
                no_progress_ms: 1500,
            },
            bucket.clone(),
            Arc::new(log_fault::FaultFileSystem {
                gate: log_gate.clone(),
            }),
        )
        .await?,
    );
    let mut release = Some(log_gate.arm(log_fault::HOLD_SYNC));
    let result = async {
        let mut starting = Box::pin(public_node::cells(
            node.as_ref().context("node")?,
            definition.application,
            Arc::new(faults::Store::default()),
            registration,
            false,
        ));
        tokio::time::timeout(Duration::from_secs(3), async {
            cellar::asyncrt::select! {
                _=log_gate.entered.notified()=>Ok::<_,anyhow::Error>(()),
                result=&mut starting=>{
                    result?;
                    anyhow::bail!("startup completed before held opening fsync")
                },
            }
        })
        .await??;
        drop(starting);
        ensure!(calls.load(Ordering::Acquire) == 0);
        let generation = node
            .as_ref()
            .context("node")?
            .info()
            .lease_generation
            .clone();
        let client = FleetClient::new(bucket.clone(), false);
        management
            .run(async {
                let accepted = client
                    .node_action(
                        "raft-host",
                        &generation,
                        NodeAction::Stop {
                            operation_id: "cancelled-start".into(),
                            mode: StopMode::Preserve,
                        },
                    )
                    .await?;
                ensure!(accepted["receipt"]["domain_joined"] == false);
                Ok::<_, anyhow::Error>(())
            })
            .await??;
        let owner = node.take().context("node")?;
        let mut stopping = tokio::spawn(owner.run_until(std::future::pending::<ShutdownMode>()));
        ensure!(
            tokio::time::timeout(Duration::from_millis(100), &mut stopping)
                .await
                .is_err(),
            "cancelled startup abandoned a running opening fsync"
        );
        management
            .run(async {
                let pending = client
                    .stop_receipt("raft-host", &generation, "cancelled-start")
                    .await?
                    .context("pending receipt")?;
                ensure!(!pending.domain_joined && pending.phase != "complete");
                if let Some(proof) = pending.proof {
                    ensure!(
                        matches!(
                            proof.raft,
                            RaftStopProof::Failed {
                                startup_incomplete: true
                            }
                        ) && !proof.complete(StopMode::Preserve)
                    );
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
        drop(release.take());
        let outcome = tokio::time::timeout(Duration::from_secs(10), stopping).await???;
        let NodeOutcome::Stopped(report) = outcome else {
            anyhow::bail!("unexpected cancelled-start outcome: {outcome:?}")
        };
        let actual = report
            .raft
            .as_ref()
            .context("cancelled-start Raft report")?;
        ensure!(actual.startup_incomplete && actual.result.is_err());
        management
            .run(async {
                let joined = client
                    .stop_receipt("raft-host", &generation, "cancelled-start")
                    .await?
                    .context("joined receipt")?;
                ensure!(joined.domain_joined && joined.phase == "incomplete");
                let proof = joined.proof.context("joined proof")?;
                ensure!(
                    matches!(
                        proof.raft,
                        RaftStopProof::Failed {
                            startup_incomplete: true
                        }
                    ) && !proof.complete(StopMode::Preserve)
                );
                ensure!(
                    serde_json::to_value(proof)? == serde_json::to_value(StopProof::from(&report))?
                );
                Ok::<_, anyhow::Error>(())
            })
            .await??;
        ensure!(calls.load(Ordering::Acquire) == 0);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    drop(release.take());
    if let Some(node) = node {
        node.cancel().await?;
    }
    management.shutdown().await;
    result
}

pub async fn run(scenario: Scenario) -> Result<()> {
    let root = tempfile::tempdir()?;
    let management = ExecutionDomain::new(tokio::runtime::Handle::current());
    let bucket = management
        .run(async {
            let bucket = cellar::bucket::Bucket::open_dev(&root.path().join("fleet.sqlite"))?;
            bucket
                .put(
                    "fleet/peer-auth.json",
                    serde_json::to_vec(&serde_json::json!({"version":1,"key":"0b".repeat(32)}))?,
                )
                .await?;
            Ok::<_, anyhow::Error>(bucket)
        })
        .await??;
    let (gate, mut entered) = public_node::Gate::new();
    let definition = public_node::counter(Arc::new(AtomicUsize::new(0)), gate.clone())?;
    let registration = registration(definition.schema)?;
    let log_gate = log_fault::Gate::new(None);
    let filesystem: Arc<dyn cellar_ltx::FileSystem> =
        if matches!(scenario, Scenario::CloseError | Scenario::JoinDeadline) {
            Arc::new(log_fault::FaultFileSystem {
                gate: log_gate.clone(),
            })
        } else {
            Arc::new(cellar_ltx::DirectFileSystem)
        };
    let mut node = Some(
        public_node::node_with_fleet(
            &root.path().join("node"),
            definition.application.clone(),
            ShutdownPolicy {
                total_ms: if scenario == Scenario::JoinDeadline {
                    300
                } else {
                    3000
                },
                token_wait_ms: 0,
                no_progress_ms: if matches!(scenario, Scenario::Ambiguous | Scenario::JoinDeadline)
                {
                    200
                } else {
                    1500
                },
            },
            bucket.clone(),
            filesystem,
        )
        .await?,
    );
    let store = Arc::new(faults::Store::default());
    let result = async {
        let service = public_node::cells(
            node.as_ref().context("node")?,
            definition.application.clone(),
            store.clone(),
            registration.clone(),
            false,
        )
        .await
        .context("start managed Raft cells")?;
        let cell = service
            .cell(
                registration.namespace.namespace,
                &registration.birth.scope.cell,
            )
            .context("managed cell handle")?;
        public_node::ready(&cell)
            .await
            .context("managed cell readiness")?;
        let first = cell
            .command(
                definition.increment.clone(),
                1i64.to_be_bytes().to_vec(),
                public_node::options(1)?,
            )
            .await
            .context("first managed command")?;
        wait_status(&cell, |status| {
            status.command_ready && status.archived >= first.version.position.index
        })
        .await
        .context("first managed archive coverage")?;
        let held_sql = matches!(scenario, Scenario::PendingSql | Scenario::Cancelled);
        if !held_sql {
            gate.release();
        }
        if scenario == Scenario::ArchiveLag {
            store.archive_unavailable.store(true, Ordering::Release);
        }
        let mut options = public_node::options(2)?;
        let id = options.id;
        let attempts = store.archive_attempts.load(Ordering::Acquire);
        if scenario == Scenario::Ambiguous {
            store.archive_hang.store(true, Ordering::Release);
            options.durability = Durability::S3;
        }
        let pending = tokio::spawn(cell.command(
            definition.increment.clone(),
            2i64.to_be_bytes().to_vec(),
            options,
        ));
        if held_sql {
            tokio::time::timeout(Duration::from_secs(2), entered.recv())
                .await?
                .context("SQL entry signal")?;
        } else {
            wait_status(&cell, |status| {
                status.applied.index > first.version.position.index
                    && (scenario != Scenario::Ambiguous
                        || store.archive_attempts.load(Ordering::Acquire) > attempts)
            })
            .await
            .context("second managed apply")?;
        }
        let mut log_release = if matches!(scenario, Scenario::CloseError | Scenario::JoinDeadline) {
            wait_status(&cell, |status| {
                status.command_ready && status.archived >= status.applied.index
            })
            .await
            .context("pre-fault managed archive coverage")?;
            Some(log_gate.arm(if scenario == Scenario::CloseError {
                log_fault::FAIL_SYNC
            } else {
                log_fault::HOLD_SYNC
            }))
        } else {
            None
        };
        let generation = node
            .as_ref()
            .context("node")?
            .info()
            .lease_generation
            .clone();
        let client = FleetClient::new(bucket.clone(), false);
        management
            .run(async {
                let accepted = client
                    .node_action(
                        "raft-host",
                        &generation,
                        NodeAction::Stop {
                            operation_id: "raft-stop".into(),
                            mode: StopMode::Preserve,
                        },
                    )
                    .await?;
                ensure!(
                    accepted["receipt"]["phase"] == "accepted"
                        && accepted["receipt"]["domain_joined"] == false
                        && accepted["receipt"]["proof"].is_null()
                );
                Ok::<_, anyhow::Error>(())
            })
            .await??;
        if held_sql {
            // The regression cut: the management stop must seal new Raft work
            // before its blocked private callback can return or physical close.
            tokio::time::timeout(Duration::from_millis(500), async {
                loop {
                    if matches!(
                        cell.query(definition.get.clone(), vec![], QueryOptions::default())
                            .await,
                        Err(CommandFailure::Unavailable)
                    ) {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .context("management stop did not seal Raft admission")?;
            // is_closed() includes admission closure. The pending callback and
            // the actual Node join below are the physical ownership oracles.
            ensure!(!pending.is_finished(), "held SQL command already returned");
            management
                .run(async {
                    let receipt = client
                        .stop_receipt("raft-host", &generation, "raft-stop")
                        .await?
                        .context("pending stop receipt")?;
                    ensure!(
                        !receipt.domain_joined
                            && receipt.proof.is_none()
                            && receipt.phase != "complete"
                    );
                    Ok::<_, anyhow::Error>(())
                })
                .await??;
        }
        let owner = node.take().context("node")?;
        let mut stopping = tokio::spawn(async move {
            if scenario == Scenario::Cancelled {
                owner.cancel().await
            } else {
                owner
                    .run_until(std::future::pending::<ShutdownMode>())
                    .await
            }
        });
        if held_sql {
            tokio::time::sleep(Duration::from_millis(30)).await;
            ensure!(
                !stopping.is_finished(),
                "Node stop returned while private SQL was held"
            );
            gate.release();
        }
        if scenario == Scenario::JoinDeadline {
            tokio::time::timeout(Duration::from_secs(2), log_gate.entered.notified()).await?;
            ensure!(
                tokio::time::timeout(Duration::from_millis(500), &mut stopping)
                    .await
                    .is_err(),
                "held fsync did not retain physical ownership"
            );
            drop(log_release.take());
        }
        let outcome = tokio::time::timeout(Duration::from_secs(10), stopping).await???;
        let pending = pending.await?;
        ensure!(service.is_closed());
        management
            .run(async {
                let receipt = client
                    .stop_receipt("raft-host", &generation, "raft-stop")
                    .await?
                    .context("joined stop receipt")?;
                ensure!(receipt.domain_joined);
                let encoded = serde_json::to_value(&receipt)?;
                if scenario == Scenario::Cancelled {
                    ensure!(matches!(outcome, NodeOutcome::Cancelled));
                    ensure!(receipt.phase == "failed" && receipt.proof.is_none());
                    ensure!(pending.is_err());
                    return Ok::<_, anyhow::Error>(());
                }
                let NodeOutcome::Stopped(report) = outcome else {
                    anyhow::bail!("unexpected stop: {outcome:?}")
                };
                let proof = receipt.proof.context("Raft stop proof")?;
                ensure!(
                    serde_json::to_value(&proof)?
                        == serde_json::to_value(StopProof::from(&report))?,
                    "durable proof differs from actual Node result"
                );
                if scenario == Scenario::CloseError {
                    ensure!(report.raft.as_ref().context("Raft report")?.result.is_err());
                    ensure!(matches!(
                        proof.raft,
                        RaftStopProof::Failed {
                            startup_incomplete: false
                        }
                    ));
                    ensure!(receipt.phase == "incomplete" && !proof.complete(StopMode::Preserve));
                    ensure!(pending?.result == 3i64.to_be_bytes());
                    ensure!(
                        log_gate.mode.load(Ordering::Acquire) == log_fault::OFF,
                        "sync failure was not exercised"
                    );
                    return Ok::<_, anyhow::Error>(());
                }
                let RaftStopProof::Closed { owners, .. } = &proof.raft else {
                    anyhow::bail!("missing actual Raft close proof")
                };
                let closed = report
                    .raft
                    .as_ref()
                    .context("actual Raft report")?
                    .result
                    .as_ref()
                    .map_err(|e| anyhow::anyhow!(e.clone()))?;
                ensure!(
                    owners.node == closed.node.node.bytes()
                        && owners.disk == closed.node.disk.bytes()
                        && owners.process == closed.node.process.bytes()
                        && owners.engine == closed.storage_instance.bytes()
                );
                ensure!(owners.groups == 1 && owners.groups_with_unknown_archive == 0);
                if scenario == Scenario::Ambiguous {
                    ensure!(receipt.phase == "incomplete" && !proof.complete(StopMode::Preserve));
                    ensure!(owners.unfinished_commands == 1 && owners.groups_with_archive_lag == 1);
                    ensure!(
                        closed.unfinished
                            == vec![UnfinishedCommand {
                                scope: registration.birth.scope.clone(),
                                id
                            }]
                    );
                    ensure!(
                        matches!(pending,Err(CommandFailure::Ambiguous{command}) if command==id)
                    );
                } else if scenario == Scenario::JoinDeadline {
                    ensure!(
                        report.deadline_elapsed,
                        "physical Raft close hid the elapsed original Node deadline"
                    );
                    ensure!(receipt.phase == "incomplete" && !proof.complete(StopMode::Preserve));
                    ensure!(
                        owners.unfinished_commands == 0 && pending?.result == 3i64.to_be_bytes()
                    );
                } else {
                    ensure!(
                        receipt.phase == "complete" && proof.complete(StopMode::Preserve),
                        "unexpected incomplete stop: {proof:?}"
                    );
                    ensure!(
                        owners.unfinished_commands == 0 && pending?.result == 3i64.to_be_bytes()
                    );
                    if scenario == Scenario::ArchiveLag {
                        ensure!(owners.groups_with_archive_lag == 1);
                    }
                }
                if scenario == Scenario::ArchiveLag {
                    let key = format!("management/stops/raft-host/{generation}/raft-stop.json");
                    for corruption in [
                        "missing_raft",
                        "zero_engine",
                        "impossible_counts",
                        "hidden_ambiguity",
                    ] {
                        let mut invalid = encoded.clone();
                        match corruption {
                            "missing_raft" => {
                                invalid["proof"]
                                    .as_object_mut()
                                    .context("proof object")?
                                    .remove("raft");
                            }
                            "zero_engine" => {
                                invalid["proof"]["raft"]["owners"]["engine"] =
                                    serde_json::json!(vec![0; 16])
                            }
                            "impossible_counts" => {
                                invalid["proof"]["raft"]["owners"]["groups_with_archive_lag"] =
                                    serde_json::json!(2)
                            }
                            "hidden_ambiguity" => {
                                invalid["proof"]["raft"]["owners"]["unfinished_commands"] =
                                    serde_json::json!(1)
                            }
                            _ => unreachable!(),
                        }
                        bucket.put(&key, serde_json::to_vec(&invalid)?).await?;
                        ensure!(
                            client
                                .stop_receipt("raft-host", &generation, "raft-stop")
                                .await
                                .is_err(),
                            "reader accepted {corruption}"
                        );
                    }
                    bucket.put(&key, serde_json::to_vec(&encoded)?).await?;
                    ensure!(client
                        .stop_receipt("raft-host", &generation, "raft-stop")
                        .await?
                        .is_some());
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    gate.release();
    store.archive_hang.store(false, Ordering::Release);
    store.archive_resume.notify_waiters();
    if let Some(node) = node {
        node.cancel().await?;
    }
    management.shutdown().await;
    result
}
