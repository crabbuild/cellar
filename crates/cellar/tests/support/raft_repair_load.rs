//! Competing native snapshot repairs while an independent cell serves traffic.
#![allow(clippy::disallowed_methods)]
use super::{native_cluster as cluster, public_node};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::consensus::*;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

async fn leader(cells: &[RaftCellHandle], excluded: Option<usize>) -> Result<usize> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for (node, cell) in cells.iter().enumerate() {
                if Some(node) != excluded && cell.status().await?.command_ready {
                    return Ok(node);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?
}

async fn write(
    cells: &[RaftCellHandle],
    definitions: &[public_node::Counter],
    excluded: Option<usize>,
    sequence: u64,
    increment: i64,
) -> Result<CommandReceipt> {
    // Retry only the same identity, digest and deadline, including after a lost
    // response. A retry may discover the original committed receipt.
    let options = public_node::options(sequence)?;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let primary = leader(cells, excluded).await?;
            match cells[primary]
                .command(
                    definitions[primary].increment.clone(),
                    increment.to_be_bytes().to_vec(),
                    options.clone(),
                )
                .await
            {
                Ok(receipt) => {
                    ensure!(receipt.result == (sequence as i64 * increment).to_be_bytes());
                    return Ok(receipt);
                }
                Err(
                    CommandFailure::Unavailable
                    | CommandFailure::Overloaded
                    | CommandFailure::NotLeader { .. }
                    | CommandFailure::Ambiguous { .. },
                ) => tokio::time::sleep(Duration::from_millis(10)).await,
                Err(error) => return Err(error.into()),
            }
        }
    })
    .await?
}

fn bounded(service: &RaftCells) -> Result<(usize, usize)> {
    let usage = service.resource_usage();
    let transport = usage.transport.context("live transport resource view")?;
    for budget in [
        transport.snapshot_source,
        transport.snapshot_receive,
        transport.seed_metadata,
        usage.business,
        usage.control,
        usage.archive_capture,
    ] {
        ensure!(budget.items <= budget.max_items && budget.bytes <= budget.max_bytes);
    }
    ensure!(transport.snapshot_source.max_items == 1);
    ensure!(transport.snapshot_receive.max_items == 1);
    ensure!(transport.snapshot_source.max_bytes <= 384 * 1024 * 1024);
    ensure!(transport.snapshot_receive.max_bytes <= 640 * 1024 * 1024);
    let sql = usage.sql.context("live SQL resource view")?;
    ensure!(sql.work_reservations <= sql.work_capacity);
    Ok((
        transport.snapshot_source.items,
        transport.snapshot_receive.items,
    ))
}

pub async fn run() -> Result<()> {
    let (gate, _entered) = public_node::Gate::new();
    gate.release();
    let calls: Vec<_> = (0..3).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    let definitions = calls
        .iter()
        .map(|calls| public_node::counter(calls.clone(), gate.clone()))
        .collect::<Result<Vec<_>>>()?;
    let namespace = NamespaceConfig::new(
        ClusterId::new([1; 16])?,
        NamespaceId::new([2; 16])?,
        NamespaceMode::Raft,
        definitions[0].schema,
    );
    let registrations = (0..3)
        .map(|group| {
            Ok(RaftCellRegistration {
                placement: RaftCellPlacement::Replica,
                namespace: namespace.clone(),
                class: "Counter".into(),
                birth: BirthProposal {
                    scope: GroupScope {
                        cluster: namespace.cluster,
                        namespace: namespace.namespace,
                        cell: format!("Counter:repair-load-{group}"),
                        group: GroupId::new([80 + group; 16])?,
                        incarnation: IncarnationId::new([3; 16])?,
                    },
                    voters: (1..=3)
                        .map(|node| {
                            Ok(InitialMember {
                                member: MemberId::new(node)?,
                                node: NodeId::new([node as u8; 16])?,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                    learners: vec![],
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let cluster = cluster::Cluster::start_with_transport(
        &definitions,
        registrations.clone(),
        20,
        RaftHostOptions {
            checkpoint_entries: 4,
            ..Default::default()
        },
        TransportOptions {
            snapshot_chunk_bytes: 257,
            snapshot_timeout_ms: 30_000,
            io_timeout_ms: 10_000,
            ..Default::default()
        },
    )
    .await?;
    let cells = registrations
        .iter()
        .map(|registration| {
            cluster
                .services
                .iter()
                .map(|service| service.cell(namespace.namespace, &registration.birth.scope.cell))
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let sources = [
        leader(&cells[0], None).await?,
        leader(&cells[1], None).await?,
    ];
    let follower = (0..3)
        .find(|node| !sources.contains(node))
        .context("common follower")?;
    cluster.isolate(follower, false).await?;
    let mut receipts = Vec::new();
    let mut cuts = Vec::new();
    for (group, group_cells) in cells.iter().take(2).enumerate() {
        let before = group_cells[follower].status().await?.checkpoint;
        let mut last = None;
        for sequence in 1..=12 {
            last = Some(
                write(
                    group_cells,
                    &definitions,
                    Some(follower),
                    sequence,
                    group as i64 + 1,
                )
                .await?,
            );
        }
        receipts.push(last.context("repair workload receipt")?);
        cuts.push(
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let primary = leader(group_cells, Some(follower)).await?;
                    let status = group_cells[primary].status().await?;
                    if status.checkpoint.index > before.index {
                        return Ok::<_, anyhow::Error>(status.checkpoint);
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await??,
        );
    }
    // Retain actual TLS response chunks on every possible snapshot source. The
    // receiver must reserve its one slot before trying to read these bytes.
    let holds = (0..3)
        .filter(|node| *node != follower)
        .map(|source| cluster.hold_data_responses_after(follower, source, 4096))
        .collect::<Result<Vec<_>>>()?;
    cluster.heal();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let held: usize = (0..3)
                .filter(|node| *node != follower)
                .map(|source| cluster.held_data_response_bytes(follower, source))
                .sum();
            if held > 0 && bounded(&cluster.services[follower])?.1 == 1 {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .context("real snapshot response hold")??;

    let traffic = async {
        for sequence in 1..=24 {
            let receipt = write(&cells[2], &definitions, Some(follower), sequence, 7).await?;
            let read = cells[2][follower]
                .query(
                    definitions[follower].get.clone(),
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::AtLeast(receipt.version_token),
                        timeout: Duration::from_secs(5),
                        ..Default::default()
                    },
                )
                .await?;
            ensure!(read.result == (sequence as i64 * 7).to_be_bytes());
        }
        Ok::<_, anyhow::Error>(())
    };
    tokio::pin!(traffic);
    let mut samples = 0;
    loop {
        let mut source_items = 0;
        for service in &cluster.services {
            source_items += bounded(service)?.0;
        }
        ensure!(
            source_items > 0,
            "held snapshot lost its source reservation"
        );
        ensure!(
            bounded(&cluster.services[follower])?.1 == 1,
            "held snapshot receive reservation ended before traffic completed"
        );
        samples += 1;
        cellar::asyncrt::select! {
            done = &mut traffic => { done.context("healthy commands/queries during repair")?; break; }
            _ = tokio::time::sleep(Duration::from_millis(2)) => (),
        }
    }
    for group in 0..2 {
        ensure!(
            cells[group][follower].status().await?.checkpoint.index < cuts[group].index,
            "unfinished snapshot exposed a checkpoint"
        );
    }
    drop(holds);
    for group in 0..2 {
        let read = cells[group][follower]
            .query(
                definitions[follower].get.clone(),
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(receipts[group].version_token.clone()),
                    timeout: Duration::from_secs(15),
                    ..Default::default()
                },
            )
            .await?;
        ensure!(read.result == (12 * (group as i64 + 1)).to_be_bytes());
        ensure!(
            cells[group][follower].status().await?.checkpoint.index >= cuts[group].index,
            "repair must install the compacted history"
        );
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let idle = cluster
                .services
                .iter()
                .map(bounded)
                .collect::<Result<Vec<_>>>()?;
            if idle.iter().all(|usage| *usage == (0, 0)) {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .context("snapshot reservations physically released")??;
    ensure!(
        calls[follower].load(Ordering::SeqCst) == 0,
        "follower replayed business logic"
    );
    ensure!(
        calls
            .iter()
            .map(|calls| calls.load(Ordering::SeqCst))
            .sum::<usize>()
            == 48,
        "each of 24 repair and 24 traffic commands must execute once"
    );
    cellar::note!("repair budget samples={samples}; repaired_cells=2; concurrent_commands=24; concurrent_queries=24; snapshot_receive_peak=1");
    cluster.close().await
}
