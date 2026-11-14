// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public SDK/HTTP callers share real TLS discovery in the original banks.
use super::*;
use cellar::sdk::fleet_operations::{
    RaftFleetControl, RaftFleetParkPhase, RaftFleetParkRequest, RaftFleetTransitionGroup,
};

fn probes(host: &RaftCells) -> Result<[u64; 4]> {
    let performance = host.performance();
    ensure!(performance.available && !performance.saturated);
    let mut values = [0; 4];
    for (value, name) in values.iter_mut().zip([
        "cell_leader_probe_owners",
        "cell_leader_probe_joins",
        "cell_command_leader_probes",
        "cell_leader_probe_finishes",
    ]) {
        *value = performance
            .counters
            .iter()
            .find(|counter| counter.name == name)
            .with_context(|| format!("missing probe counter {name}"))?
            .value;
    }
    Ok(values)
}

pub(super) async fn run() -> Result<()> {
    for rooted in [false, true] {
        for axum in [false, true] {
            for closing in [false, true] {
                let callbacks = Arc::new(AtomicUsize::new(0));
                let observed = callbacks.clone();
                let filesystem = storage_fault::Gate::new(None);
                let mut cluster = Box::pin(nodes::Cluster::start_with_read_control(
                    rooted,
                    Arc::new(fault::Store::default()),
                    Arc::new(storage_fault::FaultFileSystem {
                        gate: filesystem.clone(),
                    }),
                    Arc::new(move || {
                        observed.fetch_add(1, Ordering::AcqRel);
                    }),
                    Arc::new(|_| {}),
                ))
                .await?;
                let result = Box::pin(exercise(
                    &mut cluster,
                    &callbacks,
                    &filesystem,
                    axum,
                    closing,
                ))
                .await;
                let joined = cluster.close().await;
                if result.is_err() || joined.is_err() {
                    for root in std::mem::take(&mut cluster.managed.roots) {
                        eprintln!("retained probe storm disk: {}", root.keep().display());
                    }
                }
                cluster.managed.fixtures[0].domain.shutdown().await;
                result.with_context(|| {
                    format!("leader probe storm extents={rooted}, axum={axum}, close={closing}")
                })?;
                joined?;
            }
        }
    }
    Ok(())
}

async fn exercise(
    cluster: &mut nodes::Cluster,
    callbacks: &AtomicUsize,
    filesystem: &Arc<storage_fault::Gate>,
    axum: bool,
    closing: bool,
) -> Result<()> {
    cluster.authorize_commands()?;
    cluster.authorize_ingress_reads()?;
    cluster.bind()?;
    let history = counter::history(
        cluster.managed.history.cluster,
        cluster.managed.history.namespace,
        "one",
    )?;
    prepare_park(cluster, &history).await?;
    let ingress = cluster.managed.hosts[INGRESS].clone();
    ensure!(ingress.observe_node(TIMEOUT).await?.node.groups == 0);
    let usage = ingress.resource_usage();
    let ordinary = usage.host.controller.ordinary;
    ensure!(ordinary.max_items == 2 && ordinary.items == 0);
    let bank = usage
        .transport
        .context("original transport observer")?
        .outgoing
        .control;
    let before = probes(&ingress)?;
    let cell = ingress.cell(history.clone())?;
    let increment = cell.contract("Increment")?;
    let retry = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )? + 60_000;
    // The two independent callers can reach proposal admission in either
    // order. Each client starts at sequence one; replay retains that exact ID.
    let id = |ordinal: u8| CommandId::new(ClientId::new([173 + ordinal; 16]).unwrap(), 1).unwrap();
    let options = move |ordinal| CommandOptions {
        durability: Durability::Quorum,
        timeout: TIMEOUT,
        ..CommandOptions::new(id(ordinal), retry)
    };
    // A parked voter must obtain a fresh wake barrier before replying to the
    // authenticated probe. Hold its actual engine sync after probe transmission.
    let origin = ingress.clone();
    let initial_probes = before[2];
    *filesystem.sync_when.lock().unwrap() = Some(Arc::new(move || {
        origin.performance().counters.iter().any(|counter| {
            counter.name == "cell_command_leader_probes" && counter.value > initial_probes
        })
    }));
    let held = filesystem.arm(storage_fault::HOLD_SYNC);
    let sdk = tokio::spawn(cell.command(&increment, 1i64.to_be_bytes().to_vec(), options(1)));
    tokio::time::timeout(TIMEOUT, filesystem.entered.notified())
        .await
        .context("authenticated probe reaches a physical wake sync")?;
    ensure!(probes(&ingress)? == [before[0] + 1, before[1], before[2] + 1, before[3]]);
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT + Duration::from_secs(1))
        .build()?;
    let request = client
        .post(format!(
            "{}?delta=1",
            cluster.web()?.url(INGRESS, axum, "one")
        ))
        .header("x-command-id", format!("{}:1", "af".repeat(16)))
        .header("x-retry-until-ms", retry)
        .header("x-write-durability", "quorum");
    let http = tokio::spawn(http::Web::send(
        request,
        format!("shared leader probe axum={axum}"),
    ));
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let now = probes(&ingress)?;
            ensure!(
                now[0] - before[0] == 1 && now[2] - before[2] == 1 && now[3] == before[3],
                "duplicate probe owner or RPC while sync held: {before:?} -> {now:?}"
            );
            if now[1] - before[1] == 1 {
                break Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .context("SDK and HTTP share one selected probe key")??;
    let held_usage = ingress.resource_usage();
    let transport = held_usage.transport.context("held transport owner")?;
    ensure!(
        transport.outgoing.control.max_bytes == bank.max_bytes
            && transport.outgoing.control.max_items == bank.max_items
    );
    ensure!(
        transport.outgoing.control.bytes >= 32 * 1024 + 4096
            && transport.outgoing.control.bytes <= bank.max_bytes
    );
    ensure!(
        held_usage.host.controller.ordinary.max_items == ordinary.max_items
            && held_usage.host.controller.ordinary.max_bytes == ordinary.max_bytes
            && held_usage.host.controller.ordinary.items == 2
    );
    ensure!(callbacks.load(Ordering::Acquire) == 0);
    let refused = cell
        .command(&increment, 1i64.to_be_bytes().to_vec(), options(3))
        .await
        .err()
        .context("third invocation exceeds the original bank")?;
    ensure!(
        refused.command == id(3)
            && !refused.ambiguous
            && matches!(
                refused.cause.as_ref(),
                CellDispatchError::Runtime(RuntimeError::Overloaded)
            )
    );
    sdk.abort();
    ensure!(sdk.await.unwrap_err().is_cancelled());
    ensure!(
        ingress
            .resource_usage()
            .transport
            .context("cancelled waiter retains transport")?
            .outgoing
            .control
            .bytes
            >= 32 * 1024
    );
    ensure!(callbacks.load(Ordering::Acquire) == 0);
    let mut close = closing.then(|| cluster.stop_owned(INGRESS));
    if let Some(close) = close.as_mut() {
        ensure!(
            tokio::time::timeout(Duration::from_millis(30), close)
                .await
                .is_err(),
            "close abandoned the physical probe"
        );
        ensure!(
            ingress
                .resource_usage()
                .transport
                .context("closing probe owner")?
                .outgoing
                .control
                .bytes
                >= 32 * 1024
        );
    }
    drop(held);
    let receipt = http.await??.success()?;
    ensure!(receipt["durability"] == "quorum");
    let original = receipt["value"].as_i64().context("exact HTTP result")?;
    if let Some(close) = close {
        let report = tokio::time::timeout(TIMEOUT, close).await???;
        ensure!(report == ingress.close().await?);
        let transport = ingress
            .resource_usage()
            .transport
            .context("joined transport observer")?;
        ensure!(
            transport.tasks == 0
                && transport.outgoing.control.bytes == 0
                && transport.outgoing.query.bytes == 0
                && transport.outgoing_connections.query == 0
        );
        ensure!(ingress.resource_usage().control.bytes == 0);
    }
    // A completed HTTP waiter can leave another entered SQL owner unwinding.
    // Synchronize on physical quiescence before replaying either original ID;
    // each replay still establishes its own receipt and current authority.
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let mut idle = true;
            for voter in &cluster.managed.hosts[..INGRESS] {
                let usage = voter.resource_usage();
                let ordinary = usage.host.controller.ordinary;
                let sql = usage.sql.context("live voter SQL observations")?;
                ensure!(ordinary.max_items == 2 && ordinary.max_bytes == 65536);
                idle &= ordinary.items == 0
                    && usage.host.controller.retained_commands == 0
                    && sql.active_cells == 0
                    && sql.retained_operations == 0
                    && sql.queued_or_running == 0;
            }
            if idle {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .context("cancelled public command physical owners drained")??;
    // Reconcile both original IDs at a live voter, including the cancelled SDK
    // caller. Eventual replies must still verify the real TLS exporter.
    let survivor = cluster.managed.hosts[cluster.leader().await?].cell(history)?;
    let mut values = Vec::new();
    for ordinal in 1..=2 {
        let replay = survivor
            .command(&increment, 1i64.to_be_bytes().to_vec(), options(ordinal))
            .await
            .inspect_err(|_| {
                let owners: Vec<_> = cluster.managed.hosts[..INGRESS]
                    .iter()
                    .map(|voter| {
                        let usage = voter.resource_usage();
                        (
                            usage.host.controller.ordinary.items,
                            usage.host.controller.retained_commands,
                            usage.host.controller.active_apply,
                            usage.host.controller.active_checkpoints,
                            usage.sql.map(|sql| {
                                (
                                    sql.active_cells,
                                    sql.retained_operations,
                                    sql.queued_or_running,
                                )
                            }),
                        )
                    })
                    .collect();
                eprintln!("same-ID replay failed ordinal={ordinal}; voter owners={owners:?}");
            })?;
        ensure!(replay.id == id(ordinal) && replay.durability == Durability::Quorum);
        values.push(i64::from_be_bytes(replay.result.as_slice().try_into()?));
        if ordinal == 2 {
            ensure!(replay.result == original.to_be_bytes());
        }
    }
    values.sort_unstable();
    ensure!(values == [1, 2] && callbacks.load(Ordering::Acquire) == 2);
    let after = tokio::time::timeout(TIMEOUT, async {
        loop {
            let now = probes(&ingress)?;
            if now[0] - before[0] == now[3] - before[3] {
                return Ok::<_, anyhow::Error>(now);
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .context("all entered probe plans reached a terminal result")??;
    // Two callers each retain the SDK's original four routing attempts. A
    // completed wake observation can require a subsequent finite plan; the
    // held overlap above must still have exactly one live owner and one join.
    let owners = after[0] - before[0];
    let joins = after[1] - before[1];
    ensure!(
        owners >= 1
            && joins >= 1
            && owners + joins <= 2 * 4
            && (1..=owners * 3).contains(&(after[2] - before[2]))
            && after[3] - before[3] == owners,
        "finite plans and joined terminal owners: {before:?} -> {after:?}"
    );
    eprintln!("public leader discovery axum={axum}, close={closing}: one overlapping authenticated probe owner; {owners} terminal plans, {joins} joins; two original commands; unchanged transport/controller caps");
    Ok(())
}

async fn prepare_park(cluster: &nodes::Cluster, history: &CellHistory) -> Result<()> {
    let current = cluster.leader().await?;
    let group = cluster.managed.history;
    if current != 0 {
        let owner = cluster.managed.hosts[current].group(group)?;
        let progress = owner.replication_status(TIMEOUT).await?;
        owner
            .transfer_leadership(
                LeadershipTransfer {
                    target: MemberId::new(1)?,
                    expected_term: progress.term,
                    expected_revision: progress.membership_revision,
                },
                TIMEOUT,
            )
            .await?;
    }
    tokio::time::timeout(TIMEOUT, async {
        loop {
            if cluster.managed.hosts[0].group(group)?.status().await?.ready
                && cluster.leader().await? == 0
            {
                break Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .context("member one leads before the park cut")??;
    // This public enrollment warms verified membership and advice without an
    // application callback. S3 completion drains its required archive proof.
    cluster.managed.hosts[INGRESS]
        .cell(history.clone())?
        .enroll(
            CellEnrollmentRequest {
                creation: ProposalId::new([181; 16])?,
                durability: Durability::S3,
            },
            TIMEOUT,
        )
        .await?;
    let fixture = &cluster.managed.fixtures[0];
    let control = RaftFleetControl::new(
        &fixture.domain,
        fixture.catalog.clone(),
        vec![cluster.managed.directory.clone()],
    )?
    .with_node_observer(cluster.managed.hosts[0].clone());
    let revision = fixture.catalog.read(TIMEOUT).await?.revision();
    let selection = RaftFleetParkRequest {
        catalog_id: control.catalog_id().into(),
        namespace_id: hex::encode(group.namespace.bytes()),
        group: RaftFleetTransitionGroup {
            group_id: hex::encode(group.group.bytes()),
            incarnation_id: hex::encode(group.incarnation.bytes()),
        },
        operation_id: hex::encode([184; 16]),
        request_id: hex::encode([185; 16]),
        settlement_request_id: hex::encode([186; 16]),
        expected_revision: revision,
    };
    let parked = control
        .park_group(&request(184, revision).principal, &selection, TIMEOUT)
        .await?;
    ensure!(parked.phase == RaftFleetParkPhase::Delivered);
    // Age advice using its existing transport TTL before creating either
    // command. Every subsequent request retains the original five-second bound.
    tokio::time::sleep(Duration::from_millis(
        TransportOptions::default().io_timeout_ms,
    ))
    .await;
    for host in &cluster.managed.hosts[..INGRESS] {
        let status = host.group(group)?.status().await?;
        ensure!(
            !status.ready
                && status.applied.index.get() == parked.generation
                && status.committed == status.applied.index
        );
    }
    Ok(())
}
