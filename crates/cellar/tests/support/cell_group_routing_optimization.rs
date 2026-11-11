// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One actual ingress RPC per warm write, with exact SDK and HTTP receipts.
use super::*;
use std::sync::atomic::AtomicUsize;

#[path = "cell_group_routing_busy.rs"]
mod busy;

#[path = "cell_group_routing_refresh.rs"]
mod refresh;

#[path = "cell_group_routing_read_gate.rs"]
mod read_gate;

#[path = "cell_group_routing_read_preferences.rs"]
mod read_preferences;

#[path = "cell_group_routing_reads.rs"]
mod reads;

#[path = "raft_storage_fault.rs"]
#[allow(dead_code)]
mod storage_fault;

const COUNTERS: [&str; 6] = [
    "cell_command_rpc_attempts",
    "cell_command_leader_probes",
    "cell_command_directory_lookups",
    "cell_membership_demand_refreshes",
    "cell_leader_advice_hits",
    "cell_membership_background_observations",
];

fn sample(cluster: &nodes::Cluster) -> Result<[u64; 6]> {
    let mut result = [0; 6];
    for host in &cluster.managed.hosts {
        let performance = host.performance();
        ensure!(performance.available && !performance.saturated);
        for (index, name) in COUNTERS.iter().enumerate() {
            result[index] += performance
                .counters
                .iter()
                .find(|counter| counter.name == *name)
                .with_context(|| format!("missing routing observation {name}"))?
                .value;
        }
    }
    Ok(result)
}

fn direct(cluster: &nodes::Cluster, before: [u64; 6], label: &str) -> Result<()> {
    let after = sample(cluster)?;
    let delta: Vec<_> = after
        .iter()
        .zip(before)
        .map(|(after, before)| after - before)
        .collect();
    ensure!(
        delta[..5] == [1, 0, 0, 0, 1],
        "{label}: routing counters {delta:?}"
    );
    eprintln!("{label}: rpc/probe/directory/member/advice-hit={:?}; background membership observations={}", &delta[..5], delta[5]);
    Ok(())
}

pub(super) async fn cached_leader_direct_write() -> Result<()> {
    run(Scenario::Direct).await
}

pub(super) async fn busy_leader_keeps_placement() -> Result<()> {
    run(Scenario::Busy).await
}

pub(super) async fn election_repairs_leader_only() -> Result<()> {
    run(Scenario::Election).await
}

pub(super) async fn metadata_outage_on_cached_path() -> Result<()> {
    run(Scenario::MetadataOutage).await
}

pub(super) async fn refresh_storm_is_bounded() -> Result<()> {
    for axum in [false, true] {
        run(Scenario::Refresh { axum }).await?;
        run(Scenario::RefreshClose { axum }).await?;
    }
    Ok(())
}

pub(super) async fn read_cohort_has_fresh_barrier() -> Result<()> {
    Box::pin(reads::run()).await
}

pub(super) async fn read_preferences_preserve_proof() -> Result<()> {
    Box::pin(read_preferences::run()).await
}

#[derive(Clone, Copy, Debug)]
enum Scenario {
    Direct,
    Busy,
    Election,
    MetadataOutage,
    Refresh { axum: bool },
    RefreshClose { axum: bool },
}

async fn run(scenario: Scenario) -> Result<()> {
    for rooted in [false, true] {
        let callbacks = Arc::new(AtomicUsize::new(0));
        let count = callbacks.clone();
        let store = Arc::new(fault::Store::default());
        let observer: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            count.fetch_add(1, Ordering::AcqRel);
        });
        let mut cluster = if matches!(
            scenario,
            Scenario::Refresh { .. } | Scenario::RefreshClose { .. }
        ) {
            Box::pin(nodes::Cluster::start_with_cold_ingress(
                rooted,
                store.clone(),
                observer,
            ))
            .await?
        } else {
            Box::pin(nodes::Cluster::start_with_callback(
                rooted,
                store.clone(),
                observer,
            ))
            .await?
        };
        let result = match scenario {
            Scenario::Refresh { axum } => {
                Box::pin(refresh::exercise(&mut cluster, &callbacks, false, axum)).await
            }
            Scenario::RefreshClose { axum } => {
                Box::pin(refresh::exercise(&mut cluster, &callbacks, true, axum)).await
            }
            Scenario::Busy => Box::pin(busy::exercise(&cluster, &callbacks)).await,
            Scenario::Election => Box::pin(exercise_election(&mut cluster, &callbacks)).await,
            Scenario::Direct => Box::pin(exercise(&cluster, &callbacks, None)).await,
            Scenario::MetadataOutage => {
                Box::pin(exercise(&cluster, &callbacks, Some(&store))).await
            }
        };
        store
            .deny_routing_metadata_reads
            .store(false, Ordering::Release);
        for fixture in &cluster.managed.fixtures {
            fixture.store.router_birth_read.resume.notify_one();
        }
        let joined = cluster.close().await;
        if result.is_err() || joined.is_err() {
            for root in std::mem::take(&mut cluster.managed.roots) {
                eprintln!(
                    "retained routing optimization disk: {}",
                    root.keep().display()
                );
            }
        }
        cluster.managed.fixtures[0].domain.shutdown().await;
        result.with_context(|| format!("routing scenario={scenario:?}, extent images={rooted}"))?;
        joined?;
    }
    Ok(())
}

async fn exercise_election(cluster: &mut nodes::Cluster, callbacks: &AtomicUsize) -> Result<()> {
    cluster.authorize_commands()?;
    let history = counter::history(
        cluster.managed.history.cluster,
        cluster.managed.history.namespace,
        "one",
    )?;
    let initial = cluster.leader().await?;
    cluster.managed.hosts[initial]
        .cell(history.clone())?
        .enroll(
            CellEnrollmentRequest {
                creation: ProposalId::new([181; 16])?,
                durability: Durability::Quorum,
            },
            TIMEOUT,
        )
        .await?;
    let ingress = &cluster.managed.hosts[INGRESS];
    let cell = ingress.cell(history.clone())?;
    let increment = cell.contract("Increment")?;
    let retry = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )? + 60_000;
    let client = ClientId::new([198; 16])?;
    let options = |sequence| CommandOptions {
        durability: Durability::Quorum,
        timeout: TIMEOUT,
        ..CommandOptions::new(CommandId::new(client, sequence).unwrap(), retry)
    };
    let original = cell
        .command(&increment, 1i64.to_be_bytes().to_vec(), options(1))
        .await?;
    ensure!(original.result == 1i64.to_be_bytes() && callbacks.load(Ordering::Acquire) == 1);

    let previous = cluster.managed.hosts[initial].group(cluster.managed.history)?;
    let progress = previous.replication_status(TIMEOUT).await?;
    let target = (initial + 1) % INGRESS;
    previous
        .transfer_leadership(
            LeadershipTransfer {
                target: MemberId::new(target as u64 + 1)?,
                expected_term: progress.term,
                expected_revision: progress.membership_revision,
            },
            TIMEOUT,
        )
        .await?;
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let old = previous.status().await?;
            let new = cluster.managed.hosts[target]
                .group(cluster.managed.history)?
                .status()
                .await?;
            if old.leader == Some(target as u64 + 1)
                && old.term > progress.term
                && new.ready
                && new.leader == old.leader
            {
                break Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("new leader and authenticated former-leader observation")??;
    let before = sample(cluster)?;
    let refused = ingress
        .performance()
        .counters
        .iter()
        .find(|counter| counter.name == "cell_command_pre_admission_refusals")
        .context("pre-admission counter")?
        .value;
    let handed_off = cell
        .command(&increment, 1i64.to_be_bytes().to_vec(), options(2))
        .await?;
    let after = sample(cluster)?;
    let delta: Vec<_> = after.iter().zip(before).map(|(a, b)| a - b).collect();
    ensure!(delta[..5] == [2, 0, 0, 0, 2], "handoff routing {delta:?}");
    let refusals = ingress
        .performance()
        .counters
        .iter()
        .find(|counter| counter.name == "cell_command_pre_admission_refusals")
        .context("pre-admission counter")?
        .value;
    ensure!(refusals == refused + 1);
    ensure!(handed_off.result == 2i64.to_be_bytes() && callbacks.load(Ordering::Acquire) == 2);
    ensure!(handed_off.witness.position.term > original.witness.position.term);
    eprintln!("live handoff: routing={delta:?}; one pre-admission refusal, one callback");

    cluster.stop(target).await?;
    let elected = cluster.leader().await?;
    ensure!(elected != target);
    let before = sample(cluster)?;
    let lost = cell
        .command(&increment, 1i64.to_be_bytes().to_vec(), options(3))
        .await
        .err()
        .context("cached stopped leader must fail without a non-admission proof")?;
    ensure!(lost.command == CommandId::new(client, 3)? && lost.ambiguous);
    ensure!(callbacks.load(Ordering::Acquire) == 2);
    let after_lost = sample(cluster)?;
    // A stopped socket may fail during connection establishment, before the
    // exchange counter. In either case an unknown outcome gets no second RPC.
    ensure!(after_lost[0] - before[0] <= 1);
    // Explicitly reconcile the same logical command. The unknown invocation
    // never forwards again automatically; the application retains its ID.
    let recovered = cell
        .command(&increment, 1i64.to_be_bytes().to_vec(), options(3))
        .await?;
    let after = sample(cluster)?;
    ensure!(after[0] - after_lost[0] == 1);
    ensure!(after[2] == before[2] && after[3] == before[3]);
    ensure!(recovered.id == lost.command && recovered.result == 3i64.to_be_bytes());
    ensure!(callbacks.load(Ordering::Acquire) == 3);
    let before = sample(cluster)?;
    let replay = cell
        .command(&increment, 1i64.to_be_bytes().to_vec(), options(1))
        .await?;
    direct(cluster, before, "original receipt after stopped leader")?;
    ensure!(
        replay.id == original.id
            && replay.result == original.result
            && replay.version == recovered.version
            && replay.version.covers(&original.version) == Ok(true)
            && replay.witness == recovered.witness
            && replay.durability == original.durability
    );
    ensure!(callbacks.load(Ordering::Acquire) == 3);
    ensure!(
        cluster.managed.hosts[INGRESS]
            .observe_node(TIMEOUT)
            .await?
            .node
            .groups
            == 0
    );
    Ok(())
}

async fn exercise(
    cluster: &nodes::Cluster,
    callbacks: &AtomicUsize,
    outage: Option<&fault::Store>,
) -> Result<()> {
    cluster.authorize_commands()?;
    cluster.authorize_ingress_reads()?;
    cluster.bind()?;
    let group = cluster.managed.history;
    let history = counter::history(group.cluster, group.namespace, "one")?;
    let leader = cluster.leader().await?;
    cluster.managed.hosts[leader]
        .cell(history.clone())?
        .enroll(
            CellEnrollmentRequest {
                creation: ProposalId::new([181; 16])?,
                durability: Durability::Quorum,
            },
            TIMEOUT,
        )
        .await?;
    let ingress = &cluster.managed.hosts[INGRESS];
    ensure!(ingress.observe_node(TIMEOUT).await?.node.groups == 0);
    ensure!(ingress.resource_usage().host.controller.groups == 0);
    ensure!(matches!(ingress.group(group), Err(RuntimeError::NotFound)));
    let cell = ingress.cell(history.clone())?;
    let increment = cell.contract("Increment")?;
    let retry = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )? + 60_000;
    let client = ClientId::new([197; 16])?;
    let options = |sequence, durability| CommandOptions {
        durability,
        timeout: TIMEOUT,
        ..CommandOptions::new(CommandId::new(client, sequence).unwrap(), retry)
    };
    let warm = cell
        .command(
            &increment,
            1i64.to_be_bytes().to_vec(),
            options(1, Durability::Quorum),
        )
        .await?;
    ensure!(warm.result == 1i64.to_be_bytes() && warm.durability == Durability::Quorum);
    ensure!(callbacks.load(Ordering::Acquire) == 1);

    if let Some(store) = outage {
        store
            .deny_routing_metadata_reads
            .store(true, Ordering::Release);
        // Prove the outage affects actual foreground metadata operations while
        // the warmed data path retains its independently checked authority.
        ensure!(cluster.managed.fixtures[0]
            .domain
            .run(cluster.managed.directory.observe())
            .await?
            .is_err());
        ensure!(cluster.managed.fixtures[0]
            .catalog
            .read(TIMEOUT)
            .await
            .is_err());
        ensure!(store.denied_routing_metadata_reads.load(Ordering::Acquire) >= 2);
    }

    let mut expected = 1;
    for (sequence, durability) in [(2, Durability::Quorum), (3, Durability::S3)] {
        // The archive publisher independently requires current catalog proof.
        // Exercise the available Quorum data path during the metadata cut; the
        // unavailable S3 proof and exact reconciliation are checked below.
        let durability = if outage.is_some() {
            Durability::Quorum
        } else {
            durability
        };
        let before = sample(cluster)?;
        let command = cell
            .command(
                &increment,
                1i64.to_be_bytes().to_vec(),
                options(sequence, durability),
            )
            .await?;
        direct(cluster, before, "SDK cached write")?;
        expected += 1;
        ensure!(
            command.id == CommandId::new(client, sequence)?
                && command.result == (expected as i64).to_be_bytes()
                && command.durability == durability
        );
        ensure!(
            command.version.history == history && callbacks.load(Ordering::Acquire) == expected
        );
        eprintln!(
            "SDK receipt id={:?} durability={durability:?} version={:?} witness={:?}",
            command.id, command.version, command.witness
        );
        let before = sample(cluster)?;
        let replay = cell
            .command(
                &increment,
                1i64.to_be_bytes().to_vec(),
                options(sequence, durability),
            )
            .await?;
        direct(cluster, before, "SDK cached exact replay")?;
        ensure!(
            replay.id == command.id
                && replay.result == command.result
                && replay.version == command.version
                && replay.durability == durability
        );
        ensure!(callbacks.load(Ordering::Acquire) == expected);
    }

    let http_client = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT + Duration::from_secs(1))
        .build()?;
    for (axum, sequence, durability) in [(false, 1, "quorum"), (true, 2, "s3")] {
        let durability = if outage.is_some() {
            "quorum"
        } else {
            durability
        };
        let url = cluster.web()?.url(INGRESS, axum, "one");
        let send = || {
            http_client
                .post(format!("{url}?delta=1"))
                .header("x-command-id", format!("{}:{sequence}", "c7".repeat(16)))
                .header("x-retry-until-ms", retry)
                .header("x-write-durability", durability)
        };
        let before = sample(cluster)?;
        let receipt = http::Web::send(send(), format!("cached write axum={axum}"))
            .await?
            .success()?;
        direct(cluster, before, "HTTP cached write")?;
        expected += 1;
        http::value(&receipt, "one", expected as i64)?;
        ensure!(
            receipt["durability"] == durability && callbacks.load(Ordering::Acquire) == expected
        );
        let before = sample(cluster)?;
        let replay = http::Web::send(send(), format!("cached replay axum={axum}"))
            .await?
            .success()?;
        direct(cluster, before, "HTTP cached exact replay")?;
        http::value(&replay, "one", expected as i64)?;
        ensure!(replay["version"] == receipt["version"] && replay["durability"] == durability);
        ensure!(callbacks.load(Ordering::Acquire) == expected);
        eprintln!("HTTP cached receipt axum={axum}: {receipt}");
    }
    if let Some(store) = outage {
        let before = sample(cluster)?;
        let unavailable = cell
            .command(
                &increment,
                1i64.to_be_bytes().to_vec(),
                options(4, Durability::S3),
            )
            .await
            .err()
            .context("S3 cannot acknowledge without the publisher's current catalog proof")?;
        direct(
            cluster,
            before,
            "S3 proof unavailable during metadata outage",
        )?;
        ensure!(unavailable.command == CommandId::new(client, 4)? && unavailable.ambiguous);
        expected += 1;
        ensure!(callbacks.load(Ordering::Acquire) == expected);
        store
            .deny_routing_metadata_reads
            .store(false, Ordering::Release);
        let settled = cell
            .command(
                &increment,
                1i64.to_be_bytes().to_vec(),
                options(4, Durability::S3),
            )
            .await?;
        ensure!(
            settled.id == unavailable.command
                && settled.result == (expected as i64).to_be_bytes()
                && settled.durability == Durability::S3
        );
        ensure!(callbacks.load(Ordering::Acquire) == expected);
        eprintln!(
            "cached path completed during routing metadata outage; denied reads={}",
            store.denied_routing_metadata_reads.load(Ordering::Acquire)
        );
    }
    for (index, voter) in cluster.managed.hosts[..INGRESS].iter().enumerate() {
        let local = voter.cell(history.clone())?;
        let response = local
            .query(
                ProposalId::new([210 + index as u8; 16])?,
                &local.contract("Get")?,
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::Linearizable,
                    routing: QueryRouting::Local,
                    timeout: TIMEOUT,
                    ..Default::default()
                },
            )
            .await?;
        ensure!(response.result == (expected as i64).to_be_bytes());
    }
    ensure!(callbacks.load(Ordering::Acquire) == expected);
    ensure!(ingress.resource_usage().host.controller.groups == 0);
    Ok(())
}
