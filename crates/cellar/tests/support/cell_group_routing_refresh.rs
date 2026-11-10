// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cold public ingress shares actual directory work through cancellation/close.
use super::*;

fn command_id(ordinal: u8) -> CommandId {
    CommandId::new(ClientId::new([166 + ordinal; 16]).unwrap(), 1).unwrap()
}

fn discovery(host: &RaftCells) -> Result<[u64; 3]> {
    let sample = host.performance();
    ensure!(sample.available && !sample.saturated);
    let mut result = [0; 3];
    for (index, name) in [
        "cell_membership_demand_refreshes",
        "cell_membership_discovery_owners",
        "cell_membership_refresh_joins",
    ]
    .iter()
    .enumerate()
    {
        result[index] = sample
            .counters
            .iter()
            .find(|counter| counter.name == *name)
            .with_context(|| format!("missing discovery counter {name}"))?
            .value;
    }
    Ok(result)
}

pub(super) async fn exercise(
    cluster: &mut nodes::Cluster,
    callbacks: &AtomicUsize,
    close_cut: bool,
    axum: bool,
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
    let ingress = cluster.managed.hosts[INGRESS].clone();
    ensure!(ingress.observe_node(TIMEOUT).await?.node.groups == 0);
    let usage = ingress.resource_usage();
    let control = usage.control;
    let ordinary = usage.host.controller.ordinary;
    let services = usage.control_services;
    // The original controller envelope is tighter than the dispatcher's eight
    // invocations. Exercise its actual two slots without increasing either bank.
    ensure!(ordinary.max_items == 2 && ordinary.items == 0);
    let before = discovery(&ingress)?;
    let cell = ingress.cell(history.clone())?;
    let increment = cell.contract("Increment")?;
    let retry = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )? + 60_000;
    let options = move |sequence| CommandOptions {
        durability: Durability::Quorum,
        timeout: TIMEOUT,
        ..CommandOptions::new(command_id(sequence), retry)
    };
    let store = cluster.managed.fixtures[INGRESS].store.clone();
    let gate = &store.router_birth_read;
    gate.armed.store(true, Ordering::Release);
    let entered = gate.entered.notified();
    tokio::pin!(entered);
    entered.as_mut().enable();
    let sdk = tokio::spawn(cell.command(&increment, 1i64.to_be_bytes().to_vec(), options(1)));
    tokio::time::timeout(TIMEOUT, &mut entered)
        .await
        .context("physical demand discovery entered birth read")?;
    let http_client = reqwest::Client::builder()
        .no_proxy()
        .timeout(TIMEOUT + Duration::from_secs(1))
        .build()?;
    let url = cluster.web()?.url(INGRESS, axum, "one");
    let send = || {
        http_client
            .post(format!("{url}?delta=1"))
            .header("x-command-id", format!("{}:1", "a8".repeat(16)))
            .header("x-retry-until-ms", retry)
            .header("x-write-durability", "quorum")
    };
    let http = tokio::spawn(http::Web::send(
        send(),
        format!("cold refresh pair axum={axum}"),
    ));
    tokio::time::timeout(TIMEOUT, async {
        loop {
            let now = discovery(&ingress)?;
            if now[2] - before[2] == 1 {
                break Ok::<_, anyhow::Error>(());
            }
            ensure!(
                now[1] - before[1] == 1,
                "duplicate physical discovery: {before:?} -> {now:?}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("both public invocations share discovery")??;
    let held = ingress.resource_usage();
    ensure!(
        held.control.max_bytes == control.max_bytes && held.control.max_items == control.max_items
    );
    ensure!(
        held.control.bytes >= services.bytes + 32 * 1024 + 4096
            && held.control.items <= held.control.max_items
    );
    ensure!(
        held.host.controller.ordinary.max_bytes == ordinary.max_bytes
            && held.host.controller.ordinary.max_items == ordinary.max_items
            && held.host.controller.ordinary.items == 2
    );
    ensure!(callbacks.load(Ordering::Acquire) == 0);
    let refused = cell
        .command(&increment, 1i64.to_be_bytes().to_vec(), options(3))
        .await
        .err()
        .context("original controller bank must refuse a third command")?;
    ensure!(refused.command == command_id(3) && !refused.ambiguous);
    ensure!(matches!(
        refused.cause.as_ref(),
        CellDispatchError::Runtime(RuntimeError::Overloaded)
    ));
    // Both HTTP adapters must preserve that same typed admission refusal.
    for adapter in [false, true] {
        let request = http_client
            .post(format!(
                "{}?delta=1",
                cluster.web()?.url(INGRESS, adapter, "one")
            ))
            .header("x-command-id", format!("{}:1", "a9".repeat(16)))
            .header("x-retry-until-ms", retry)
            .header("x-write-durability", "quorum");
        http::Web::send(request, format!("saturated ingress axum={adapter}"))
            .await?
            .refused(503, "Overloaded")?;
    }
    let after = discovery(&ingress)?;
    ensure!(
        [
            after[0] - before[0],
            after[1] - before[1],
            after[2] - before[2]
        ] == [2, 1, 1]
    );
    // Cancelling the first caller cannot release its entered producer/discovery.
    sdk.abort();
    ensure!(sdk.await.unwrap_err().is_cancelled());
    let retained = ingress.resource_usage().control;
    ensure!(retained.bytes >= services.bytes + 32 * 1024);
    ensure!(callbacks.load(Ordering::Acquire) == 0);
    if close_cut {
        let mut closing = cluster.stop_owned(INGRESS);
        let mut observing = Box::pin(async { (&mut closing).await });
        ensure!(
            tokio::time::timeout(Duration::from_millis(30), &mut observing)
                .await
                .is_err()
        );
        drop(observing);
        ensure!(ingress.resource_usage().control_services.workers > 0);
        ensure!(ingress.resource_usage().control.bytes >= 32 * 1024);
        gate.resume.notify_one();
        let first_report = tokio::time::timeout(TIMEOUT, closing).await???;
        let receipt = http.await??.success()?;
        let repeated_report = ingress.close().await?;
        let after_close = ingress.resource_usage();
        ensure!(first_report == repeated_report);
        ensure!(first_report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers);
        ensure!(callbacks.load(Ordering::Acquire) == 2);
        ensure!(after_close.control.bytes == 0 && after_close.control.items == 0);
        ensure!(after_close.control_services == Default::default());
        // Drain permits already-admitted work to finish. Resolve the cancelled
        // SDK request and the HTTP request at a live voter, preserving both IDs.
        let current = cluster.leader().await?;
        let survivor = cluster.managed.hosts[current].cell(history)?;
        for sequence in 1..=2 {
            let replay = survivor
                .command(&increment, 1i64.to_be_bytes().to_vec(), options(sequence))
                .await?;
            ensure!(replay.id == command_id(sequence) && replay.durability == Durability::Quorum);
            if sequence == 2 {
                let original = receipt["value"]
                    .as_i64()
                    .context("drained HTTP command result")?;
                ensure!(replay.result == original.to_be_bytes());
            }
        }
        ensure!(callbacks.load(Ordering::Acquire) == 2);
    } else {
        gate.resume.notify_one();
        let receipt = http.await??.success()?;
        ensure!(receipt["durability"] == "quorum");
        let original = receipt["value"]
            .as_i64()
            .context("HTTP command's exact original result")?;
        ensure!((1..=2).contains(&original));
        // A cancelled invocation retains its original identity even if its
        // producer reached admission; same-ID reconciliation must run only once.
        for sequence in 1..=2 {
            let receipt = cell
                .command(&increment, 1i64.to_be_bytes().to_vec(), options(sequence))
                .await?;
            ensure!(receipt.id == command_id(sequence) && receipt.durability == Durability::Quorum);
            if sequence == 2 {
                ensure!(receipt.result == original.to_be_bytes());
            }
        }
        ensure!(callbacks.load(Ordering::Acquire) == 2);
        for adapter in [false, true] {
            let value = cluster
                .web()?
                .query(
                    &http_client,
                    (INGRESS, adapter),
                    "one",
                    "linearizable",
                    &serde_json::Value::Null,
                    None,
                )
                .await?
                .success()?;
            http::value(&value, "one", 2)?;
        }
        let final_sample = discovery(&ingress)?;
        ensure!(final_sample[1] - before[1] == 1);
    }
    eprintln!("cold SDK/HTTP discovery axum={axum}: 2 demands, 1 owner, 1 join; close cut={close_cut}; unchanged control caps={}/{}; controller caps={}/{}", held.control.max_bytes, held.control.max_items, ordinary.max_bytes, ordinary.max_items);
    Ok(())
}
