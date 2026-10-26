// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Slow immutable snapshot encoding must not hold the cell's SQL connection.

use super::*;
use cellar::sdk::{consensus::*, ShutdownMode};
use std::sync::atomic::AtomicUsize;

#[path = "raft_storage_fault.rs"]
#[allow(dead_code)]
mod faults;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queries_and_commands_progress_while_checkpoint_encoding_is_held() -> Result<()> {
    checkpoint_drill(Mode::Progress).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn close_joins_held_snapshot_encoder_before_releasing_its_reservations() -> Result<()> {
    checkpoint_drill(Mode::Close).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn encoding_completion_does_not_release_a_newer_sql_operation() -> Result<()> {
    checkpoint_drill(Mode::CompleteDuringCommand).await
}

#[derive(PartialEq, Eq)]
enum Mode {
    Progress,
    Close,
    CompleteDuringCommand,
}

async fn checkpoint_drill(mode: Mode) -> Result<()> {
    let (callback_gate, mut entered) = public_node::Gate::new();
    if mode != Mode::CompleteDuringCommand {
        callback_gate.release();
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let definition = public_node::counter(calls.clone(), callback_gate.clone())?;
    let directory = tempfile::tempdir()?;
    let gate = faults::Gate::new(None);
    let node = public_node::node_declared_with_filesystem(
        directory.path(),
        definition.application.clone(),
        "raft-host",
        vec![],
        Arc::new(faults::FaultFileSystem { gate: gate.clone() }),
    )
    .await?;
    let mut namespace = config(2, NamespaceMode::Raft, true);
    namespace.schema_digest = definition.schema;
    let host = RaftHostOptions {
        blocking_workers: 1,
        checkpoint_entries: 1,
        ..Default::default()
    };
    let service = public_node::cells_with_host(
        &node,
        definition.application.clone(),
        Arc::new(object_store::memory::InMemory::new()),
        RaftCellRegistration {
            placement: RaftCellPlacement::Replica,
            namespace,
            class: "Counter".into(),
            birth: proposal(2, "Counter:snapshot-progress", 5, 1),
        },
        false,
        host,
    )
    .await?;
    let cell = service.cell(NamespaceId::new([2; 16])?, "Counter:snapshot-progress")?;
    public_node::ready(&cell).await?;
    let mut hold = Some(gate.arm(faults::HOLD_SNAPSHOT_TEMP));
    let outcome = async {
        let first = cell
            .command(
                definition.increment.clone(),
                7_i64.to_be_bytes().to_vec(),
                public_node::options(1)?,
            )
            .await?;
        tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
        // The independent source reservation proves checkpoint work has been
        // admitted. Waiting only for the scratch gate could observe the archive
        // encoder before the checkpoint has left its queue.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if service
                    .resource_usage()
                    .transport
                    .is_some_and(|transport| transport.snapshot_source.items != 0)
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        if mode == Mode::Close {
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if service.performance().operations.iter().any(|operation| {
                        operation.name == "snapshot_encode"
                            && operation.started
                                > operation.succeeded + operation.failed + operation.abandoned
                    }) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?;
            let close = service.close();
            tokio::pin!(close);
            ensure!(
                tokio::time::timeout(Duration::from_millis(50), &mut close)
                    .await
                    .is_err(),
                "close completed before the held encoder exited"
            );
            let usage = service.resource_usage();
            ensure!(
                usage
                    .transport
                    .context("transport reservation counters unavailable")?
                    .snapshot_source
                    .items
                    != 0
            );
            ensure!(
                usage
                    .images
                    .context("image reservation counters unavailable")?
                    .stores
                    != 0
            );
            drop(hold.take());
            ensure!(close.await?.unfinished.is_empty());
            return Ok(());
        }
        let before = cell
            .query(
                definition.get.clone(),
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::AtLeast(first.version_token),
                    timeout: Duration::from_secs(5),
                    ..Default::default()
                },
            )
            .await
            .context("query waited behind held checkpoint encoding")?;
        ensure!(before.result == 7_i64.to_be_bytes());
        let mut options = public_node::options(2)?;
        options.timeout = Duration::from_secs(5);
        let second = cell.command(
            definition.increment.clone(),
            1_i64.to_be_bytes().to_vec(),
            options,
        );
        if mode == Mode::CompleteDuringCommand {
            tokio::time::timeout(Duration::from_secs(5), entered.recv())
                .await?
                .context("new command did not enter its SQL callback")?;
            drop(hold.take());
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if cell.status().await?.checkpoint.index >= first.version.position.index {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await??;
            let blocked = cell
                .query(
                    definition.get.clone(),
                    vec![],
                    QueryOptions {
                        consistency: QueryConsistency::Linearizable,
                        timeout: Duration::from_millis(100),
                        ..Default::default()
                    },
                )
                .await;
            ensure!(matches!(blocked, Err(CommandFailure::VersionUnavailable)));
            let usage = service.resource_usage();
            ensure!(
                usage
                    .sql
                    .context("live SQL counters unavailable")?
                    .work_reservations
                    == 1,
                "encoding completion cleared the new callback's SQL busy flag"
            );
            // Group inspection is sampled, whereas the SQL reservation above
            // is live. Require a cycle that began after checkpoint completion
            // instead of mistaking an earlier idle sample for a cleared flag.
            let observation_started = std::time::Instant::now();
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let groups = service
                        .resource_usage()
                        .groups
                        .context("live group counters unavailable")?;
                    if groups.sample_age_ms < observation_started.elapsed().as_millis() as u64 {
                        ensure!(
                            groups.sql_busy == 1 && groups.pending_commands == 1,
                            "encoding completion changed the newer SQL operation's state"
                        );
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await??;
            callback_gate.release();
        }
        let second = second
            .await
            .context("command waited behind held checkpoint encoding")?;
        ensure!(second.result == 8_i64.to_be_bytes());
        let after = cell
            .query(
                definition.get.clone(),
                vec![],
                QueryOptions {
                    consistency: QueryConsistency::Linearizable,
                    timeout: Duration::from_secs(5),
                    ..Default::default()
                },
            )
            .await?;
        ensure!(after.result == 8_i64.to_be_bytes());
        ensure!(calls.load(Ordering::SeqCst) == 2);
        if mode == Mode::Progress {
            ensure!(service
                .resource_usage()
                .transport
                .is_some_and(|transport| transport.snapshot_source.items != 0));
        }
        Ok::<_, anyhow::Error>(())
    }
    .await;
    drop(hold);
    callback_gate.release();
    ensure!(service.close().await?.unfinished.is_empty());
    node.shutdown(ShutdownMode::Preserve).await?;
    outcome?;
    // The node join also releases its infrastructure handles. The weak
    // transport observer must then report an absent owner, not a fabricated zero.
    let usage = service.resource_usage();
    ensure!(
        usage.transport.is_none(),
        "closed transport owner remains alive"
    );
    let images = usage.images.context("closed image counters unavailable")?;
    ensure!(images.stores == 0 && images.generations == 0 && images.bytes == 0);
    let sql = usage.sql.context("closed SQL counters unavailable")?;
    ensure!(sql.queue_closed && sql.running_workers == 0 && sql.work_reservations == 0);
    Ok(())
}
