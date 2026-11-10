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
    destination
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
    let client = ClientId::new([204; 16])?;
    let retry = u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_millis(),
    )? + 60_000;
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
    ensure!(warm.result == 1i64.to_be_bytes() && callbacks.load(Ordering::Acquire) == 1);
    let ordinary = destination.resource_usage().host.controller.ordinary;
    ensure!(ordinary.max_items == 2 && ordinary.items == 0);
    let pause = ArchivePause(cluster.managed.fixtures[0].store.clone());
    pause.0.archive_hang.store(true, Ordering::Release);
    let local = destination.cell(history)?;
    let mut held = tokio::task::JoinSet::new();
    for sequence in [2, 3] {
        held.spawn(local.command(
            &increment,
            1i64.to_be_bytes().to_vec(),
            options(sequence, Durability::S3),
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
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("authenticated destination overload reached originating owner")??;
    pause.release();
    while let Some(response) = held.join_next().await {
        ensure!(response??.durability == Durability::S3);
    }
    let response = incoming
        .join_next()
        .await
        .context("owned routed command")???;
    ensure!(response.result == 4i64.to_be_bytes() && callbacks.load(Ordering::Acquire) == 4);
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
    ensure!(replay.result == response.result && callbacks.load(Ordering::Acquire) == 4);
    eprintln!("destination overload: {rejected} proven refusals; same command, zero probe/range/member repair");
    Ok(())
}
