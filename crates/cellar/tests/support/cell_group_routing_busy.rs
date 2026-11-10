// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real destination admission refusal retries inside the existing invocation.
use super::*;

struct ArchivePause(Arc<fault::Store>);

impl ArchivePause {
    fn release(&self) {
        self.0.archive_hang.store(false, Ordering::Release);
        self.0.archive_resume.notify_waiters();
    }
}

impl Drop for ArchivePause {
    fn drop(&mut self) {
        self.release();
    }
}

fn refusals(ingress: &RaftCells) -> Result<u64> {
    ingress
        .performance()
        .counters
        .iter()
        .find(|counter| counter.name == "cell_command_pre_admission_refusals")
        .map(|counter| counter.value)
        .context("pre-admission refusal counter")
}

pub(super) async fn exercise(cluster: &nodes::Cluster, callbacks: &AtomicUsize) -> Result<()> {
    cluster.authorize_commands()?;
    let group = cluster.managed.history;
    let history = counter::history(group.cluster, group.namespace, "one")?;
    let leader = cluster.leader().await?;
    let destination = &cluster.managed.hosts[leader];
    let other = counter::history(group.cluster, group.namespace, "two")?;
    for (cell, identity) in [(history.clone(), 181), (other.clone(), 182)] {
        destination
            .cell(cell)?
            .enroll(
                CellEnrollmentRequest {
                    creation: ProposalId::new([identity; 16])?,
                    durability: Durability::Quorum,
                },
                TIMEOUT,
            )
            .await?;
    }
    let ingress = &cluster.managed.hosts[INGRESS];
    let cell = ingress.cell(history.clone())?;
    let increment = cell.contract("Increment")?;
    let retry = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )? + 60_000;
    // Each concurrently submitted command has its own client. A single client
    // must submit sequences in order; this drill is about destination capacity.
    let options = |ordinal: u8, durability| CommandOptions {
        durability,
        timeout: TIMEOUT,
        ..CommandOptions::new(
            CommandId::new(ClientId::new([204 + ordinal; 16]).unwrap(), 1).unwrap(),
            retry,
        )
    };
    let warm = cell
        .command(
            &increment,
            1i64.to_be_bytes().to_vec(),
            options(1, Durability::S3),
        )
        .await?;
    ensure!(warm.result == 1i64.to_be_bytes() && warm.durability == Durability::S3);
    let other_cell = ingress.cell(other.clone())?;
    let warm_other = other_cell
        .command(
            &increment,
            1i64.to_be_bytes().to_vec(),
            options(5, Durability::S3),
        )
        .await?;
    ensure!(warm_other.result == 1i64.to_be_bytes() && warm_other.durability == Durability::S3);
    // Refresh the leader observation after both initial archive proofs exist.
    // This exact replay performs no callback and leaves the original ID intact.
    let replayed_warm = cell
        .command(
            &increment,
            1i64.to_be_bytes().to_vec(),
            options(1, Durability::S3),
        )
        .await?;
    ensure!(replayed_warm.result == warm.result && callbacks.load(Ordering::Acquire) == 2);
    let ordinary = destination.resource_usage().host.controller.ordinary;
    ensure!(ordinary.max_items == 2 && ordinary.items == 0);
    let pause = ArchivePause(cluster.managed.fixtures[0].store.clone());
    pause.0.archive_hang.store(true, Ordering::Release);
    let mut held = tokio::task::JoinSet::new();
    // Independent cells avoid the earlier per-cell SQL Busy boundary. This
    // holds the actual node command bank while both commands await S3 proof.
    for (history, ordinal) in [(history, 2), (other, 3)] {
        held.spawn(destination.cell(history)?.command(
            &increment,
            1i64.to_be_bytes().to_vec(),
            options(ordinal, Durability::S3),
        ));
    }
    tokio::time::timeout(TIMEOUT, async {
        loop {
            if destination.resource_usage().host.controller.ordinary.items == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .context("two original destination slots retained for S3 proof")?;
    let before = sample(cluster)?;
    let refused = refusals(ingress)?;
    let mut incoming = tokio::task::JoinSet::new();
    incoming.spawn(cell.command(
        &increment,
        1i64.to_be_bytes().to_vec(),
        options(4, Durability::Quorum),
    ));
    tokio::time::timeout(TIMEOUT, async {
        while refusals(ingress)? == refused {
            if let Some(result) = held.try_join_next() {
                match result? {
                    Ok(response) => anyhow::bail!("S3 call completed before archive release: {:?}", response.result),
                    Err(error) => anyhow::bail!("S3 call failed before archive release: {error:?}"),
                }
            }
            if let Some(result) = incoming.try_join_next() {
                match result? {
                    Ok(response) => anyhow::bail!("origin completed without the expected overload rejection: {:?}", response.result),
                    Err(error) => anyhow::bail!("origin terminated before overload rejection: {error:?}; counters-before={before:?}; after={:?}", sample(cluster)?),
                }
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("authenticated destination overload reached originating owner")??;
    pause.release();
    while let Some(response) = held.join_next().await {
        let response = response??;
        ensure!(response.durability == Durability::S3 && response.result == 2i64.to_be_bytes());
    }
    let response = incoming
        .join_next()
        .await
        .context("owned routed command")???;
    ensure!(
        response.result == 3i64.to_be_bytes()
            && response.durability == Durability::Quorum
            && callbacks.load(Ordering::Acquire) == 5
    );
    let after = sample(cluster)?;
    let delta: Vec<_> = after.iter().zip(before).map(|(a, b)| a - b).collect();
    let rejected = refusals(ingress)? - refused;
    ensure!(
        rejected > 0 && delta[0] == rejected + 1,
        "busy command routing {delta:?}"
    );
    ensure!(
        delta[1..4] == [0, 0, 0],
        "overload refreshed placement: {delta:?}"
    );
    ensure!(
        destination
            .resource_usage()
            .host
            .controller
            .ordinary
            .max_items
            == ordinary.max_items
    );
    let replay = cell
        .command(
            &increment,
            1i64.to_be_bytes().to_vec(),
            options(4, Durability::Quorum),
        )
        .await?;
    ensure!(replay.result == response.result && callbacks.load(Ordering::Acquire) == 5);
    eprintln!("destination overload: {rejected} proven refusals; same command, zero probe/range/member repair");
    Ok(())
}
