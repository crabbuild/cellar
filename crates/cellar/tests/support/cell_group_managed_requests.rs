// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public HTTP requests over cold managed birth, failover and retained restart.
use super::*;
use nodes::INGRESS;

#[path = "../../../../examples/raft-counter/counter.rs"]
mod counter;
#[path = "cell_group_managed_request_http.rs"]
mod http;
#[path = "cell_group_managed_request_nodes.rs"]
mod nodes;
#[path = "cell_group_routing_optimization.rs"]
mod optimization;
#[path = "cell_group_managed_request_parity.rs"]
mod parity;

pub async fn cached_leader_direct_write() -> Result<()> {
    optimization::cached_leader_direct_write().await
}

pub async fn busy_leader_keeps_placement() -> Result<()> {
    optimization::busy_leader_keeps_placement().await
}

pub async fn election_repairs_leader_only() -> Result<()> {
    optimization::election_repairs_leader_only().await
}

pub async fn metadata_outage_on_cached_path() -> Result<()> {
    optimization::metadata_outage_on_cached_path().await
}

pub async fn refresh_storm_is_bounded() -> Result<()> {
    optimization::refresh_storm_is_bounded().await
}

pub async fn run_native() -> Result<()> {
    for rooted in [false, true] {
        Box::pin(request_path(rooted, Arc::new(fault::Store::default()))).await?;
    }
    Ok(())
}

pub async fn request_path(rooted: bool, store: Arc<fault::Store>) -> Result<()> {
    let mut cluster = Box::pin(nodes::Cluster::start(rooted, store)).await?;
    let work = Box::pin(exercise(&mut cluster)).await;
    let joined = cluster.close().await;
    if work.is_err() || joined.is_err() {
        for root in std::mem::take(&mut cluster.managed.roots) {
            eprintln!("retained managed HTTP disk: {}", root.keep().display());
        }
    }
    cluster.managed.fixtures[0].domain.shutdown().await;
    work?;
    joined
}

async fn exercise(cluster: &mut nodes::Cluster) -> Result<()> {
    let retry = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )?
    .checked_add(60_000)
    .context("HTTP retry horizon")?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT + Duration::from_secs(1))
        .build()?;
    let first = cluster.leader().await?;
    let one_history = counter::history(
        cluster.managed.history.cluster,
        cluster.managed.history.namespace,
        "one",
    )?;
    cluster.managed.hosts[first]
        .cell(one_history.clone())?
        .enroll(
            CellEnrollmentRequest {
                creation: ProposalId::new([181; 16])?,
                durability: Durability::Quorum,
            },
            TIMEOUT,
        )
        .await?;
    cluster.bind()?;
    let two_history = counter::history(
        cluster.managed.history.cluster,
        cluster.managed.history.namespace,
        "two",
    )?;
    let two_handle = cluster.managed.hosts[INGRESS].cell(two_history.clone())?;
    let creation = CellEnrollmentRequest {
        creation: ProposalId::new([182; 16])?,
        durability: Durability::Quorum,
    };
    let denied = two_handle
        .enroll(creation, TIMEOUT)
        .await
        .err()
        .context("enrollment without command grant")?;
    ensure!(denied.creation == creation.creation && !denied.ambiguous);
    let origin = (first + 1) % 3;
    ensure!(
        cluster
            .web()?
            .command(&client, (origin, false), "one", 7, 1, retry)
            .await?
            .status
            == 503,
        "membership alone granted a command origin"
    );
    for axum in [false, true] {
        ensure!(
            cluster
                .web()?
                .query(
                    &client,
                    (INGRESS, axum),
                    "one",
                    "linearizable",
                    &serde_json::Value::Null,
                    None,
                )
                .await?
                .status
                == 503,
            "an ingress process without a read grant served a query"
        );
        ensure!(
            cluster
                .web()?
                .command(&client, (INGRESS, axum), "two", 1, 2, retry)
                .await?
                .status
                == 503,
            "an ingress process without a command grant submitted a write"
        );
    }
    let empty = cluster
        .web()?
        .query(
            &client,
            (first, true),
            "one",
            "linearizable",
            &serde_json::Value::Null,
            None,
        )
        .await?
        .success()?;
    http::value(&empty, "one", 0)?;
    let mut foreign = NamespaceCommandOrigin {
        cluster: cluster.managed.history.cluster,
        namespace: NamespaceId::new([211; 16])?,
        revision: 1,
        peer: cluster.managed.peers[origin].clone(),
    };
    ensure!(cluster.managed.hosts[first]
        .grant_command_origin(foreign.clone())
        .is_err());
    foreign.namespace = cluster.managed.history.namespace;
    foreign.peer.process = ProcessGeneration::new([250; 16])?;
    ensure!(cluster.managed.hosts[first]
        .grant_command_origin(foreign)
        .is_err());
    cluster.authorize_commands()?;
    ensure!(
        cluster
            .web()?
            .query(
                &client,
                (INGRESS, true),
                "one",
                "linearizable",
                &serde_json::Value::Null,
                None,
            )
            .await?
            .status
            == 503,
        "a command grant authorized application reads"
    );
    cluster.authorize_ingress_reads()?;
    let enrolled = two_handle.enroll(creation, TIMEOUT).await?;
    ensure!(enrolled.local_enrollment().is_none());
    ensure!(enrolled.record().cell() == &two_history);
    ensure!(enrolled.record().creation() == creation.creation);
    ensure!(enrolled.durability() == Durability::Quorum);
    let original = enrolled.record().clone();
    drop(enrolled);
    let archived = two_handle
        .enroll(
            CellEnrollmentRequest {
                durability: Durability::S3,
                ..creation
            },
            TIMEOUT,
        )
        .await?;
    ensure!(archived.local_enrollment().is_none() && archived.durability() == Durability::S3);
    ensure!(archived.record() == &original);
    drop(archived);
    cluster.inventory().await?;
    Box::pin(parity::check(
        cluster.web()?,
        &client,
        first,
        &cluster.managed.hosts[first].cell(one_history)?,
        retry,
    ))
    .await?;
    let group = cluster.managed.hosts[first].group(cluster.managed.history)?;
    let before_writes = group.observe_partition(TIMEOUT).await?;
    let one = cluster
        .web()?
        .command(&client, ((first + 1) % 3, false), "one", 7, 1, retry)
        .await?
        .success()?;
    let two = cluster
        .web()?
        .command(&client, ((first + 2) % 3, true), "two", 11, 1, retry)
        .await?
        .success()?;
    http::value(&one, "one", 7)?;
    http::value(&two, "two", 11)?;
    ensure!(one["group"] == two["group"]);
    ensure!(one["cell"] != two["cell"]);
    let after_writes = group.observe_partition(TIMEOUT).await?;
    ensure!(after_writes.workload.admitted_writes == before_writes.workload.admitted_writes + 2);
    ensure!(after_writes.workload.applied_writes == before_writes.workload.applied_writes + 2);
    let context = CellGroupPartitionContext {
        directory_digest: [1; 32],
        topology_changed_at_ms: before_writes.workload.sampled_at_ms,
        durability: Durability::Quorum,
        placement_class: [2; 32],
        leader_cpu_bps: 0,
        failure_domain_load_bps: 0,
        active_and_stable: true,
    };
    let rates = after_writes
        .sample_since(&before_writes, context)
        .context("same-owner workload sample")?;
    let elapsed = after_writes.workload.sampled_at_ms - before_writes.workload.sampled_at_ms;
    let upper_rate = 2_000u64.div_ceil(elapsed);
    ensure!(rates.load.cells == 2 && rates.load.writes_per_second == upper_rate);
    ensure!(rates.applies_per_second == rates.load.writes_per_second);
    ensure!(after_writes.sample_since(&after_writes, context).is_none());
    for index in 0..3 {
        for axum in [false, true] {
            let duplicate = cluster
                .web()?
                .command(&client, (index, axum), "one", 7, 1, retry)
                .await?
                .success()?;
            http::same_receipt(&one, &duplicate)?;
            for (name, receipt) in [("one", &one), ("two", &two)] {
                for mode in ["at-least", "linearizable"] {
                    let remote = (index + 1) % 3;
                    let read = cluster
                        .web()?
                        .query(&client, (index, axum), name, mode, receipt, Some(remote))
                        .await?
                        .success()?;
                    ensure!(read["value"] == receipt["value"]);
                    ensure!(read["served_by"].as_u64() == Some(remote as u64 + 1));
                }
            }
            ensure!(
                cluster
                    .web()?
                    .command(&client, (index, axum), "one", 9, 1, retry)
                    .await?
                    .status
                    == 409,
                "conflicting retry changed its identity or input"
            );
            ensure!(
                cluster
                    .web()?
                    .query(&client, (index, axum), "two", "at-least", &one, None)
                    .await?
                    .status
                    == 409,
                "another cell's token granted a minimum version"
            );
        }
    }
    let after_replays = group.observe_partition(TIMEOUT).await?;
    drop(group);
    ensure!(after_replays.workload.admitted_writes == after_writes.workload.admitted_writes);
    ensure!(after_replays.workload.applied_writes == after_writes.workload.applied_writes);
    Box::pin(check_routed_authority(cluster.web()?, &client, retry, &one)).await?;
    let routed = cluster
        .web()?
        .command(&client, (INGRESS, false), "two", 1, 2, retry)
        .await?
        .success()?;
    http::value(&routed, "two", 12)?;
    ensure!(routed["group"] == one["group"] && routed["cell"] != one["cell"]);
    for axum in [false, true] {
        for mode in ["at-least", "linearizable"] {
            let read = cluster
                .web()?
                .query(&client, (INGRESS, axum), "two", mode, &routed, None)
                .await?
                .success()?;
            http::value(&read, "two", 12)?;
            ensure!(matches!(read["served_by"].as_u64(), Some(1..=3)));
        }
        ensure!(
            cluster
                .web()?
                .query(&client, (INGRESS, axum), "one", "at-least", &routed, None)
                .await?
                .status
                == 409,
            "the routed request used another cell's version token"
        );
    }

    cluster.managed.hosts[first].revoke_command_origin(
        cluster.managed.history.cluster,
        cluster.managed.history.namespace,
        cluster.managed.peers[origin].node,
        2,
    )?;
    ensure!(
        cluster
            .web()?
            .command(&client, (origin, true), "one", 3, 2, retry)
            .await?
            .status
            == 503,
        "revoked process still originated a command"
    );
    let unchanged = cluster
        .web()?
        .query(&client, (first, false), "one", "linearizable", &one, None)
        .await?
        .success()?;
    http::value(&unchanged, "one", 7)?;
    let mut restored = NamespaceCommandOrigin {
        cluster: cluster.managed.history.cluster,
        namespace: cluster.managed.history.namespace,
        revision: 1,
        peer: cluster.managed.peers[origin].clone(),
    };
    ensure!(cluster.managed.hosts[first]
        .grant_command_origin(restored.clone())
        .is_err());
    restored.revision = 3;
    cluster.managed.hosts[first].grant_command_origin(restored)?;
    let updated = cluster
        .web()?
        .command(&client, (origin, true), "one", 3, 2, retry)
        .await?
        .success()?;
    http::value(&updated, "one", 10)?;

    let history = counter::history(
        cluster.managed.history.cluster,
        cluster.managed.history.namespace,
        "one",
    )?;
    let ingress = cluster.managed.hosts[INGRESS].cell(history.clone())?;
    let archive_store = &cluster.managed.fixtures[0].store;
    let denied = archive_store
        .denied_group_archive_writes
        .load(Ordering::Acquire);
    let mut options = CommandOptions::new(CommandId::new(ClientId::new([204; 16])?, 1)?, retry);
    options.durability = Durability::S3;

    // Leave catalog and Raft traffic live while publication of new archive
    // objects is refused. The same nonmember path must retain both minima.
    archive_store
        .deny_group_archive_writes
        .store(true, Ordering::Release);
    let during_outage = async {
        let contract = ingress.contract("Increment")?;
        let quorum = ingress
            .command(
                &contract,
                0i64.to_be_bytes().to_vec(),
                CommandOptions::new(CommandId::new(ClientId::new([203; 16])?, 1)?, retry),
            )
            .await?;
        ensure!(quorum.durability == Durability::Quorum && quorum.result == 10i64.to_be_bytes());
        let error = ingress
            .command(&contract, 0i64.to_be_bytes().to_vec(), options.clone())
            .await
            .err()
            .context("S3 command acknowledged without coverage")?;
        ensure!(error.command == options.id && error.ambiguous);
        ensure!(
            archive_store
                .denied_group_archive_writes
                .load(Ordering::Acquire)
                > denied
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    archive_store
        .deny_group_archive_writes
        .store(false, Ordering::Release);
    during_outage?;

    let remote = cluster.managed.hosts[(first + 1) % 3].cell(history)?;
    let archived = remote
        .command(
            &remote.contract("Increment")?,
            0i64.to_be_bytes().to_vec(),
            options,
        )
        .await?;
    ensure!(archived.durability == Durability::S3 && archived.result == 10i64.to_be_bytes());
    ensure!(archived.witness.group == cluster.managed.history);

    // A joined leader stop is a local failover gate, not a process-crash drill.
    cluster.stop(first).await?;
    let successor = cluster.leader().await?;
    ensure!(successor != first);
    let duplicate = cluster
        .web()?
        .command(&client, (successor, false), "one", 7, 1, retry)
        .await?
        .success()?;
    http::same_receipt(&one, &duplicate)?;
    let read = cluster
        .web()?
        .query(
            &client,
            (successor, true),
            "one",
            "linearizable",
            &updated,
            None,
        )
        .await?
        .success()?;
    http::value(&read, "one", 10)?;
    let routed_after_stop = cluster
        .web()?
        .command(&client, (INGRESS, true), "two", 1, 2, retry)
        .await?
        .success()?;
    http::same_receipt(&routed, &routed_after_stop)?;

    Box::pin(cluster.restart()).await?;
    let resumed = cluster.leader().await?;
    cluster.inventory().await?;
    let restarted = cluster.managed.hosts[resumed]
        .group(cluster.managed.history)?
        .observe_partition(TIMEOUT)
        .await?;
    ensure!(restarted.sample_since(&after_replays, context).is_none());
    let duplicate = cluster
        .web()?
        .command(&client, (resumed, true), "one", 7, 1, retry)
        .await?
        .success()?;
    http::same_receipt(&one, &duplicate)?;
    let old_floor = cluster
        .web()?
        .query(
            &client,
            ((resumed + 1) % 3, false),
            "one",
            "at-least",
            &one,
            None,
        )
        .await?
        .success()?;
    http::value(&old_floor, "one", 10)?;
    let routed_after_restart = cluster
        .web()?
        .command(&client, (INGRESS, false), "two", 1, 2, retry)
        .await?
        .success()?;
    http::same_receipt(&routed, &routed_after_restart)?;
    let next = cluster
        .web()?
        .command(&client, ((resumed + 2) % 3, false), "one", 2, 3, retry)
        .await?
        .success()?;
    http::value(&next, "one", 12)?;

    for index in [2, 1] {
        cluster.stop(index).await?;
    }
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let status = cluster.managed.hosts[0]
                .group(cluster.managed.history)?
                .status()
                .await?;
            if !status.ready && status.leader.is_none() {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("observe loss of the actual majority")??;
    let web = cluster.web()?;
    for (address, axum) in [(0, false), (0, true), (INGRESS, false), (INGRESS, true)] {
        let default_read = http::Web::send(
            client.get(web.url(address, axum, "one")),
            format!("default read without quorum node={address} axum={axum}"),
        )
        .await?;
        ensure!(
            default_read.status == 503,
            "default read without quorum returned HTTP {}",
            default_read.status
        );
        ensure!(
            cluster
                .web()?
                .command(&client, (address, axum), "one", 5, 4, retry)
                .await?
                .status
                == 503,
            "write succeeded without quorum"
        );
    }
    Ok(())
}

async fn check_routed_authority(
    web: &http::Web,
    client: &reqwest::Client,
    retry: u64,
    one: &serde_json::Value,
) -> Result<()> {
    for axum in [false, true] {
        let ingress = (INGRESS, axum);
        for name in ["unlisted", "foreign-cluster", "foreign-namespace"] {
            web.command(client, ingress, name, 9, 2, retry)
                .await?
                .refused(400, "invalid request or unavailable cell")?;
            web.query(client, ingress, name, "linearizable", one, None)
                .await?
                .refused(400, "invalid request or unavailable cell")?;
        }
        web.query(client, ingress, "two", "at-least", one, None)
            .await?
            .refused(409, "WrongIncarnation")?;
        let replay = web
            .command(client, ingress, "one", 7, 1, retry)
            .await?
            .success()?;
        http::same_receipt(one, &replay)?;
        ensure!(
            replay["group"] == one["group"]
                && replay["group_incarnation"] == one["group_incarnation"],
            "retry selected a different group history"
        );
        let read = web
            .query(client, ingress, "one", "linearizable", one, None)
            .await?
            .success()?;
        http::value(&read, "one", 7)?;
        ensure!(read["sequence"] == one["sequence"]);
    }
    Ok(())
}
