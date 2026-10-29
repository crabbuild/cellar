// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Matched first-collection workload: real RF3 groups and quorum SQL writes.
//! This isolates collection pacing; it does not qualify deployed resource limits.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fleet_topology_collection_under_quorum_writes() -> Result<()> {
    Box::pin(collection()).await
}

#[allow(clippy::disallowed_methods)] // The external fixture measures wall time.
async fn collection() -> Result<()> {
    const HISTORIES: u8 = 8;
    let f = Box::pin(Scenario::with_histories(true, HISTORIES)).await?;
    let intent = desired(&f, 189, None).await?;
    let cut = f.s.f.catalog.read(TIMEOUT).await?;
    f.s.f
        .catalog
        .begin_topology(&cut, &intent, std::slice::from_ref(&f.s.namespace), TIMEOUT)
        .await?;
    let evidence = Arc::new(Inputs::new(&f).await?);
    // Hold the evidence refresh after verified collection and before
    // planning. Thus every measured write sees the same stable membership.
    evidence
        .pause_before_planning
        .store(true, Ordering::Release);
    let cells = f.live[0].iter().map(|(_, c)| c.clone()).collect::<Vec<_>>();
    let leader = primary(&f.s.f, &cells).await?;
    let mut options = options();
    options.interval = Duration::from_millis(500);
    options.retry_max_interval = Duration::from_secs(2);
    let start = std::time::Instant::now();
    let service = Arc::new(
        f.s.f
            .catalog
            .start_topology_service_with_collection(
                f.s.nodes[0].host.clone(),
                evidence.clone(),
                evidence.clone(),
                options,
                FleetTopologyCollectionOptions::default(),
            )
            .await?,
    );
    *evidence.planning_service.lock().unwrap() = Some(Arc::downgrade(&service));
    let mut samples = Vec::new();
    let mut sequence = 2;
    let mut last = None;
    f.s.f
        .domain
        .run(cellar::asyncrt::timeout(TIMEOUT * 8, async {
            while !evidence.paused_planning.load(Ordering::Acquire) {
                let before = std::time::Instant::now();
                let receipt = cells[leader]
                    .command(
                        f.s.f.definition.increment.clone(),
                        1_i64.to_be_bytes().to_vec(),
                        public_node::options(sequence)?,
                    )
                    .await?;
                ensure!(receipt.durability == Durability::Quorum);
                samples.push(before.elapsed().as_micros() as u64);
                last = Some(receipt);
                sequence += 1;
            }
            Ok::<_, anyhow::Error>(())
        }))
        .await???;
    let elapsed = start.elapsed();
    let status = service.status();
    ensure!(
        status
            .topology
            .as_ref()
            .is_some_and(|s| s.histories_checked == u64::from(HISTORIES)),
        "{status:?}"
    );
    service.stop();
    f.s.f
        .domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            while service.status().running {
                cellar::asyncrt::sleep(Duration::from_millis(5)).await;
            }
        }))
        .await??;
    Arc::try_unwrap(service)
        .map_err(|_| anyhow::anyhow!("service still retained"))?
        .shutdown()
        .await;
    ensure!(evidence.active.load(Ordering::Acquire) == 0);
    let root = f.s.f.catalog.read(TIMEOUT).await?;
    ensure!(root.revision() == cut.revision() + 1);
    ensure!(f
        .s
        .f
        .catalog
        .pending_movements(&root, TIMEOUT)
        .await?
        .is_empty());
    let receipt = last.context("foreground quorum write")?;
    let read = cells[leader]
        .query(
            f.s.f.definition.get.clone(),
            vec![],
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                ..Default::default()
            },
        )
        .await?;
    ensure!(read.version.covers(&receipt.version)?);
    ensure!(i64::from_be_bytes(read.result.as_slice().try_into()?) == 7 + samples.len() as i64);
    samples.sort_unstable();
    let p99 = samples[(samples.len() * 99).div_ceil(100).saturating_sub(1)];
    cellar::note!(
        "FLEET_COLLECTION_MEASUREMENT {}",
        serde_json::json!({
            "histories":HISTORIES, "hot_histories":1, "ack":"quorum", "nodes":5, "replication_factor":3,
            "collection_ms":elapsed.as_millis(), "poll_rounds":status.poll_rounds,
            "writes":samples.len(), "write_p99_us":p99,
            "write_tps":samples.len() as f64 / elapsed.as_secs_f64(),
            "control_bytes_after_stop":f.s.nodes[0].host.resource_usage().control.bytes,
            "scope":"native first collection under quorum writes; shared host; no container limits"
        })
    );
    f.close().await
}
