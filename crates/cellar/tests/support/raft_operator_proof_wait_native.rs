// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A real TCP fault interrupts proof traffic while the same voter stays alive.
use super::*;
use crate::hosts::initial_tests::retirement::stopped;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize};

struct Proxy {
    address: SocketAddr,
    blocked: Arc<AtomicBool>,
    dropped: Arc<AtomicUsize>,
    stop: oneshot::Sender<()>,
    joined: tokio::task::JoinHandle<Result<()>>,
}

impl Proxy {
    async fn start(target: SocketAddr) -> Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let blocked = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicUsize::new(0));
        let fault = blocked.clone();
        let failures = dropped.clone();
        let (stop, mut stopped) = oneshot::channel();
        let joined = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            let result = {
                let accepting = async {
                    loop {
                        let (mut incoming, _) = match listener.accept().await {
                            Ok(pair) => pair,
                            Err(error) => return Err(anyhow::Error::from(error)),
                        };
                        while connections.try_join_next().is_some() {}
                        if fault.load(Ordering::Acquire) || connections.len() >= 64 {
                            failures.fetch_add(1, Ordering::AcqRel);
                            continue;
                        }
                        connections.spawn(async move {
                            if let Ok(mut outgoing) = tokio::net::TcpStream::connect(target).await {
                                let _ = tokio::io::copy_bidirectional(&mut incoming, &mut outgoing)
                                    .await;
                            }
                        });
                    }
                };
                cellar::asyncrt::select! {
                    _ = &mut stopped => Ok(()),
                    result = accepting => result,
                }
            };
            connections.abort_all();
            while connections.join_next().await.is_some() {}
            result
        });
        Ok(Self {
            address,
            blocked,
            dropped,
            stop,
            joined,
        })
    }

    async fn close(self) -> Result<()> {
        let _ = self.stop.send(());
        self.joined.await?
    }
}

async fn proxied_voter(f: &Fixture, id: u8) -> Result<(Running, Proxy)> {
    let spec = f.peer_spec(id)?;
    let root = f.catalog.read(TIMEOUT).await?;
    let commit = f
        .catalog
        .register_node(
            &root,
            &Fixture::request(id + 50, root.revision()),
            &spec,
            TIMEOUT,
        )
        .await?;
    let auth = f
        .catalog
        .authorize_host(&commit.snapshot, spec.node, TIMEOUT)
        .await?;
    let root = tempfile::tempdir()?;
    let node =
        public_node::node_named(root.path(), f.definition.application.clone(), &spec.label).await?;
    let (mut config, services) = f.config_for(&node, auth, &spec).await?;
    let proxy = Proxy::start(services.listeners.control.local_addr()?).await?;
    config.peers[0].control_address = proxy.address;
    let host = node.start_raft_cells(config, services).await?;
    Ok((
        Running {
            root,
            node,
            host,
            spec,
        },
        proxy,
    ))
}

async fn published(f: &Fixture, cells: &[RaftCellHandle]) -> Result<()> {
    let p = primary(f, cells).await?;
    let position = cells[p]
        .status()
        .await?
        .version
        .context("written version")?
        .position;
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            loop {
                let mut ready = true;
                for cell in cells {
                    ready &=
                        cell.inspect().await?.member.is_some_and(|m| {
                            m.published.is_some_and(|p| p.index >= position.index)
                        });
                }
                if ready {
                    return Ok::<_, anyhow::Error>(());
                }
                cellar::asyncrt::sleep(Duration::from_millis(10)).await;
            }
        }))
        .await??
}

async fn wait_case(change_root: bool) -> Result<()> {
    let f = Fixture::with_managed_births(true).await?;
    let (root, node, host) = f.empty_host().await?;
    let mut nodes = vec![Running {
        root,
        node,
        host,
        spec: f.spec.clone(),
    }];
    let (root, node, host) = f.peer_host(5).await?;
    nodes.push(Running {
        root,
        node,
        host,
        spec: f.peer_spec(5)?,
    });
    let (voter, proxy) = proxied_voter(&f, 6).await?;
    nodes.push(voter);
    for (i, node) in nodes.iter().enumerate() {
        f.state(node.spec.node, FleetNodeState::Active, 110 + i as u8)
            .await?;
    }
    discover(&f, &[&nodes[0].host, &nodes[1].host, &nodes[2].host]).await?;
    let (namespace, _) = initial_birth(&f).await?;
    let (proposal, authorization) = birth(&f).await?;
    let cells = registered(&nodes, &authorization)
        .await
        .context("register proxied fleet")?;
    activate(&cells, &authorization)
        .await
        .context("activate proxied fleet")?;
    write_read(&f, &cells, 1, 7, 7)
        .await
        .context("initialize proxied counter")?;
    let (_spare_root, mut spare_node, spare) = f.peer_host(7).await?;
    let mut spare_joined = false;
    f.state(NodeId::new([7; 16])?, FleetNodeState::Active, 150)
        .await?;
    let admin =
        Server::configured_runtime(&f, &namespace, Some(nodes[0].host.clone()), true).await?;
    let result = async {
        let target = NodeId::new([7; 16])?;
        let root = f.catalog.read(TIMEOUT).await?;
        let exclude = FleetRequestIdentity {
            id: [241; 16],
            operation: [242; 16],
            principal: "proof-wait".into(),
            expected_revision: root.revision(),
        };
        f.catalog
            .begin_evacuation(
                &root,
                &exclude,
                target,
                std::slice::from_ref(&namespace),
                TIMEOUT,
            )
            .await?;
        discover(
            &f,
            &[&nodes[0].host, &nodes[1].host, &nodes[2].host, &spare],
        )
        .await?;
        published(&f, &cells).await?;
        ensure!(proxy.dropped.load(Ordering::Acquire) == 0);
        let revision = f.catalog.read(TIMEOUT).await?.revision();
        let mut request = RaftFleetNodeOperation {
            catalog_id: admin.control.catalog_id().into(),
            operation_id: hex::encode([242; 16]),
            request_id: hex::encode([243; 16]),
            expected_revision: revision,
        };
        proxy.blocked.store(true, Ordering::Release);
        let value = admin
            .control
            .prepare_node_removal(
                "remover",
                &hex::encode(target.bytes()),
                request.clone(),
                TIMEOUT,
            )
            .await?;
        value.validate()?;
        ensure!(
            value.phase == RaftFleetEvacuationPhase::VerifyingBeforeStop
                && value.checked_histories == 0
                && value.applied_revision.is_none()
        );
        let blocker = value.blocker.context("visible waiting proof")?;
        ensure!(
            blocker.stage == RaftFleetVerificationStage::HistoryQuorum
                && blocker.reason == RaftFleetVerificationReason::Unavailable
                && blocker.history.as_deref()
                    == Some(f.catalog.group_binding(&proposal.scope)?.key()?.as_str())
        );
        ensure!(proxy.dropped.load(Ordering::Acquire) > 0 && !spare_node.is_closed());
        let root = f.catalog.read(TIMEOUT).await?;
        ensure!(
            root.revision() == revision
                && f.catalog
                    .finalization_receipt(&root, ProposalId::new([242; 16])?, TIMEOUT)
                    .await?
                    .is_none()
        );
        let status = admin
            .control
            .evacuation_status(&request.operation_id, TIMEOUT)
            .await?;
        status.validate()?;
        ensure!(
            status
                .verification
                .as_ref()
                .and_then(|v| v.blocker.as_ref())
                == Some(&blocker)
                && status.finalization.is_none()
        );
        write_read(&f, &cells, 2, 4, 11).await?;
        published(&f, &cells).await?;
        if change_root {
            f.catalog
                .commit(
                    &root,
                    &FleetMutation {
                        id: [245; 16],
                        operation: [246; 16],
                        principal: "proof-wait-drift".into(),
                        expected_revision: root.revision(),
                        changes: vec![FleetChange {
                            table: FleetTable::Policy,
                            key: "proof-wait-drift".into(),
                            value: Some(vec![1]),
                        }],
                    },
                    TIMEOUT,
                )
                .await?;
            ensure!(admin
                .control
                .evacuation_status(&request.operation_id, TIMEOUT)
                .await?
                .verification
                .is_none());
            ensure!(matches!(
                admin
                    .control
                    .prepare_node_removal(
                        "remover",
                        &hex::encode(target.bytes()),
                        request.clone(),
                        TIMEOUT
                    )
                    .await,
                Err(RaftFleetFailure {
                    code: RaftFleetFailureCode::Conflict,
                    ..
                })
            ));
            request.request_id = hex::encode([244; 16]);
            request.expected_revision = f.catalog.read(TIMEOUT).await?.revision();
        }
        proxy.blocked.store(false, Ordering::Release);
        discover(
            &f,
            &[&nodes[0].host, &nodes[1].host, &nodes[2].host, &spare],
        )
        .await?;
        let step = admin
            .control
            .prepare_node_removal(
                "remover",
                &hex::encode(target.bytes()),
                request.clone(),
                TIMEOUT,
            )
            .await?;
        ensure!(
            step.checked_histories == 1
                && step.blocker.is_none()
                && step.phase == RaftFleetEvacuationPhase::VerifyingBeforeStop
        );
        let mut args =
            admin.departure_args(&hex::encode(target.bytes()), "prepare-removal", &request);
        args.push("--wait".into());
        let (exit, value) = admin.cli(&args, "remover").await?;
        ensure!(exit == 0, "resumed proof {exit}: {value}");
        let done = accepted(value)?;
        ensure!(
            done.phase == RaftFleetEvacuationPhase::Finalizing
                && done.checked_histories == 1
                && done.blocker.is_none()
                && !spare_node.is_closed()
        );
        ensure!(admin
            .control
            .evacuation_status(&request.operation_id, TIMEOUT)
            .await?
            .verification
            .is_none());
        let stop = RaftFleetNodeStop {
            operation: RaftFleetNodeOperation {
                request_id: hex::encode([247; 16]),
                expected_revision: done.observed_revision,
                ..request.clone()
            },
            expected_process: done.process.context("exact fenced process")?,
        };
        f.remote(&nodes[0].host, target, stop.operation.expected_revision)
            .await?;
        let mut stop_args =
            admin.departure_args(&hex::encode(target.bytes()), "stop", &stop.operation);
        stop_args.extend(["--expected-process".into(), stop.expected_process.clone()]);
        let (exit, value) = admin.cli(&stop_args, "stopper").await?;
        ensure!(
            (exit == 0
                && accepted(value.clone())?.phase == RaftFleetEvacuationPhase::StopRequested)
                || (exit == 5 && value["error"]["outcome"] == "unknown"),
            "exact stop {exit}: {value}"
        );
        let outcome = f
            .domain
            .run(cellar::asyncrt::timeout(TIMEOUT, spare_node.finished()))
            .await???;
        spare_joined = true;
        let cellar::sdk::NodeOutcome::Stopped(report) = outcome else {
            anyhow::bail!("departing process did not stop")
        };
        let joined = report.raft.context("joined Raft report")?;
        ensure!(!joined.startup_incomplete && joined.result?.unfinished.is_empty());
        let (exit, value) = admin.cli(&stop_args, "stopper").await?;
        ensure!(exit == 0 && accepted(value)?.phase == RaftFleetEvacuationPhase::Joined);
        let removal = RaftFleetNodeRemove {
            operation: RaftFleetNodeOperation {
                request_id: hex::encode([248; 16]),
                expected_revision: f.catalog.read(TIMEOUT).await?.revision(),
                ..request.clone()
            },
            stop_id: stop.operation.request_id.clone(),
        };
        discover(&f, &[&nodes[0].host, &nodes[1].host, &nodes[2].host]).await?;
        let verified = admin
            .control
            .remove_node(
                "remover",
                &hex::encode(target.bytes()),
                removal.clone(),
                TIMEOUT,
            )
            .await?;
        ensure!(
            verified.phase == RaftFleetEvacuationPhase::VerifyingRemoval
                && verified.checked_histories == 1
                && verified.blocker.is_none()
        );
        proxy.blocked.store(true, Ordering::Release);
        let renewed = admin
            .control
            .remove_node(
                "remover",
                &hex::encode(target.bytes()),
                removal.clone(),
                TIMEOUT,
            )
            .await?;
        renewed.validate()?;
        let waiting = renewed.blocker.context("terminal quorum waiting")?;
        ensure!(
            renewed.phase == RaftFleetEvacuationPhase::VerifyingRemoval
                && renewed.checked_histories == 1
                && renewed.applied_revision.is_none()
                && renewed.stop_id == Some(removal.stop_id.clone())
                && waiting.stage == RaftFleetVerificationStage::TerminalQuorum
                && waiting.history.is_none()
        );
        let root = f.catalog.read(TIMEOUT).await?;
        ensure!(
            root.revision() == removal.operation.expected_revision
                && f.catalog
                    .removal_receipt(&root, ProposalId::new([242; 16])?, TIMEOUT)
                    .await?
                    .is_none()
                && f.catalog
                    .get(&root, FleetTable::Policy, "active-disruption", TIMEOUT)
                    .await?
                    .is_some()
        );
        let status = admin
            .control
            .evacuation_status(&request.operation_id, TIMEOUT)
            .await?;
        status.validate()?;
        ensure!(
            status
                .verification
                .as_ref()
                .and_then(|v| v.blocker.as_ref())
                == Some(&waiting)
                && status.removal.is_none()
                && status.state == RaftFleetNodeState::Finalizing
        );
        proxy.blocked.store(false, Ordering::Release);
        let removed = admin
            .control
            .remove_node(
                "remover",
                &hex::encode(target.bytes()),
                removal.clone(),
                TIMEOUT,
            )
            .await?;
        removed.validate()?;
        ensure!(removed.phase == RaftFleetEvacuationPhase::Removed && removed.blocker.is_none());
        let retried = admin
            .control
            .remove_node("remover", &hex::encode(target.bytes()), removal, TIMEOUT)
            .await?;
        ensure!(retried.applied_revision == removed.applied_revision);
        let current = f.catalog.read(TIMEOUT).await?;
        ensure!(
            f.catalog
                .get(&current, FleetTable::Policy, "active-disruption", TIMEOUT)
                .await?
                .is_none()
                && spare_node.info().data_dir.join("raft").exists()
                && admin
                    .control
                    .evacuation_status(&request.operation_id, TIMEOUT)
                    .await?
                    .verification
                    .is_none()
        );
        write_read(&f, &cells, 3, 5, 16).await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    proxy.blocked.store(false, Ordering::Release);
    let closed = async {
        admin.close().await?;
        for node in nodes {
            stopped(node.node).await?;
        }
        if !spare_joined {
            stopped(spare_node).await?;
        }
        proxy.close().await?;
        f.domain.shutdown().await;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    result?;
    closed
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_operator_proof_wait_resumes_exact_history_without_stopping() -> Result<()> {
    Box::pin(wait_case(false)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_operator_proof_wait_refuses_changed_root_and_reprepares() -> Result<()> {
    Box::pin(wait_case(true)).await
}
