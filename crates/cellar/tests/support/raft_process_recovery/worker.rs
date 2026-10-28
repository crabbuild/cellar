// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

pub(super) async fn run(config: Config) -> Result<()> {
    ensure!(
        config.index < 4 && config.peers.len() == 4,
        "invalid worker identity"
    );
    ensure!(
        !config.directory.exists(),
        "worker must start with an absent data directory"
    );
    let _ = tracing_subscriber::fmt()
        .with_env_filter("cellar::raft::archive=info,cellar::raft::host=debug")
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .try_init();
    let calls = Arc::new(AtomicUsize::new(0));
    let (definition, verify) = fixture::recovery_definition(calls.clone())?;
    let peer = config.peers[config.index].native(config.index)?;
    let node = public_node::node_named(
        &config.directory,
        definition.application.clone(),
        &peer.label,
    )
    .await?;
    let listeners = NodeRaftListeners {
        control: tokio::net::TcpListener::bind(peer.control_address).await?,
        data: tokio::net::TcpListener::bind(peer.data_address).await?,
        query: tokio::net::TcpListener::bind(peer.query_address).await?,
    };
    ensure!(
        !node.info().data_dir.join("raft").exists(),
        "fresh process has Raft state"
    );
    atomic_json(
        &config.control.join("ready.json"),
        &json!({
            "pid": std::process::id(), "data_dir": node.info().data_dir,
            "initially_empty": true, "member": config.index + 1,
        }),
    )?;
    let deadline = Instant::now() + START_TIMEOUT;
    while !config.control.join("start.json").exists() {
        ensure!(
            Instant::now() < deadline,
            "controller did not admit worker startup"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let mut registration = registration(definition.schema)?;
    if let Some(source) = config.source {
        ensure!(config.index == 3 && source < 3, "invalid cold source");
        ensure!(
            !node.info().data_dir.join("raft").exists(),
            "replacement received a preloaded image"
        );
        registration.placement = RaftCellPlacement::JoiningFromS3 {
            member: MemberId::new(4)?,
            source: MemberId::new(source as u64 + 1)?,
            source_node: NodeId::new([source as u8 + 1; 16])?,
        };
    } else {
        ensure!(config.index < 3, "replacement requires S3 placement");
    }
    let mut host = RaftHostOptions::default();
    host.execution.max_image_bytes = config.quota;
    host.execution.receipt_reserve_bytes = (config.quota / 16).min(32 * 1024 * 1024) / 512 * 512;
    host.execution.max_retry_window_ms = 86_400_000;
    host.extents = Some(RaftExtentOptions::default());
    let mut native = RaftCellsConfig {
        query_origins: vec![],
        infrastructure: NodeRaftConfig {
            log: ReplicaLogOptions::new(
                node.info().data_dir.join("raft"),
                peer.node,
                peer.disk,
                peer.process,
                if config.source.is_some() {
                    LogOpenMode::Create
                } else {
                    LogOpenMode::Resume
                },
            ),
            consensus: RaftRuntimeOptions {
                tick_interval_ms: Some(25),
                ..Default::default()
            },
            transport: TransportOptions::default(),
        },
        catalog: CatalogOptions {
            prefix: "partition/test".into(),
            ..Default::default()
        },
        cells: vec![registration],
        peers: config
            .peers
            .iter()
            .enumerate()
            .map(|(i, peer)| peer.native(i))
            .collect::<Result<_>>()?,
        host,
    };
    native = native.with_extent_image_limit(config.quota, RaftExtentOptions::default())?;
    let service = node
        .start_raft_cells(
            native,
            RaftCellsServices {
                listeners,
                tls: public_node::tls()?,
                store: store(&config.prefix)?,
                application: definition.application.clone(),
                versions: public_node::version_keys()?,
            },
        )
        .await?;
    atomic_json(
        &config.control.join("started.json"),
        &json!({
            "pid": std::process::id(), "from_s3": config.source.is_some(), "performance": service.performance(),
        }),
    )?;
    let cell = service.cell(NamespaceId::new([82; 16])?, CELL)?;
    let mut last = 0;
    let stop_sequence = loop {
        let path = config.control.join("request.json");
        if !path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        }
        let request: Request = serde_json::from_slice(&std::fs::read(path)?)?;
        if request.sequence <= last {
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        }
        ensure!(
            request.sequence == last + 1,
            "controller request sequence gap"
        );
        last = request.sequence;
        if matches!(request.action, Action::Stop) {
            break last;
        }
        let result = execute(&config, &definition, &verify, &cell, &calls, request.action).await;
        let response = match result {
            Ok(value) => json!({"sequence": last, "ok": true, "value": value}),
            Err(error) => json!({"sequence": last, "ok": false, "error": format!("{error:#}"),
                "overloaded": error.downcast_ref::<CommandFailure>() == Some(&CommandFailure::Overloaded)}),
        };
        atomic_json(&config.control.join("response.json"), &response)?;
    };
    ensure!(
        service.close().await?.unfinished.is_empty(),
        "worker retained unfinished producers"
    );
    let outcome = node.shutdown(ShutdownMode::Preserve).await?;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("worker shutdown failed: {outcome:?}");
    };
    ensure!(
        !report.drain_timed_out
            && !report.deadline_elapsed
            && report.application_work_drained
            && report.cells_drained
            && report.local_durability_closed,
        "worker shutdown incomplete: {report:?}"
    );
    atomic_json(
        &config.control.join("response.json"),
        &json!({
            "sequence": stop_sequence, "ok": true, "value": {"owners_closed": true, "callbacks": calls.load(Ordering::SeqCst)},
        }),
    )?;
    Ok(())
}

async fn execute(
    config: &Config,
    definition: &public_node::Counter,
    verify: &OperationContract,
    cell: &RaftCellHandle,
    calls: &AtomicUsize,
    action: Action,
) -> Result<Value> {
    match action {
        Action::Status => {
            let status = cell.status().await?;
            Ok(
                json!({"ready": status.command_ready, "leader": status.leader.map(MemberId::get),
                "term": status.term.get(), "revision": status.membership_revision.get(),
                "callbacks": calls.load(Ordering::SeqCst)}),
            )
        }
        Action::Command {
            client,
            retry_until_ms,
        } => {
            let mut options = fixture::command_options(client, 1)?;
            options.retry_until_ms = retry_until_ms;
            let receipt = cell
                .command(
                    definition.increment.clone(),
                    0_u64.to_be_bytes().to_vec(),
                    options.clone(),
                )
                .await?;
            ensure!(
                receipt.id == options.id && receipt.durability == Durability::Quorum,
                "wrong command identity/durability"
            );
            Ok(
                json!({"value": u64::from_be_bytes(receipt.result.as_slice().try_into()?),
                "token": receipt.version_token.as_str(), "index": receipt.version.position.index.get(),
                "term": receipt.version.position.term.get(), "group": hex::encode(receipt.version.scope.group.bytes()),
                "incarnation": hex::encode(receipt.version.scope.incarnation.bytes()), "durability": "quorum"}),
            )
        }
        Action::Read {
            expected,
            token,
            full,
        } => {
            let consistency = match token {
                Some(token) => QueryConsistency::AtLeast(VersionToken::from_encoded(&token)?),
                None => QueryConsistency::Stale,
            };
            let member = MemberId::new(config.index as u64 + 1)?;
            if full {
                fixture::verify_payloads(
                    cell,
                    verify.clone(),
                    expected,
                    config.payload_bytes,
                    &fixture::PayloadSamples {
                        ids: config.sample_ids,
                        hashes: config.sample_hashes,
                    },
                    consistency,
                    member,
                )
                .await?;
            } else {
                let result = cell
                    .query(
                        definition.get.clone(),
                        config.sample_ids[0].to_be_bytes().to_vec(),
                        QueryOptions {
                            stale_policy: if consistency == QueryConsistency::Stale {
                                StaleReadPolicy::DisconnectedHistorical
                            } else {
                                StaleReadPolicy::RecentSource
                            },
                            consistency,
                            timeout: Duration::from_secs(60),
                            ..Default::default()
                        },
                    )
                    .await?;
                ensure!(
                    result.served_by == member
                        && result.result
                            == [expected.to_be_bytes().as_slice(), &config.sample_hashes[0]]
                                .concat(),
                    "read content/serving worker mismatch"
                );
            }
            Ok(json!({"verified": true, "served_by": member.get(), "value": expected}))
        }
        Action::BeginRepair { obsolete } => {
            let status = cell.status().await?;
            let result = cell
                .begin_repair(
                    RepairPlan {
                        id: repair_id()?,
                        policy_revision: 1,
                        expected_revision: status.membership_revision,
                        obsolete: MemberId::new(obsolete)?,
                        replacement: JoiningMember {
                            member: MemberId::new(4)?,
                            node: NodeId::new([4; 16])?,
                            disk: DiskLineage::new([13; 16])?,
                        },
                    },
                    START_TIMEOUT,
                )
                .await?;
            Ok(json!({"started_at": result.started_at.get()}))
        }
        Action::AdvanceRepair { finish } => {
            let result = fixture::repair_step(
                cell,
                repair_id()?,
                if finish {
                    RepairStep::FinishJoint
                } else {
                    RepairStep::PromoteAndRemove
                },
            )
            .await?;
            ensure!(
                result.phase
                    == if finish {
                        RepairPhase::Complete
                    } else {
                        RepairPhase::Joint
                    },
                "unexpected repair phase"
            );
            Ok(json!({"finished": finish}))
        }
        Action::Transfer => {
            let status = cell.status().await?;
            cell.transfer_leadership(
                LeadershipTransfer {
                    target: MemberId::new(4)?,
                    expected_term: status.term,
                    expected_revision: status.membership_revision,
                },
                Duration::from_secs(60),
            )
            .await?;
            Ok(json!({"accepted": true}))
        }
        Action::Stop => anyhow::bail!("stop belongs to the process owner"),
    }
}
