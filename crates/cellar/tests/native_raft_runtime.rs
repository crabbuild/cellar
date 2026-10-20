//! Multi-group Ready/LightReady/application ordering using the production worker.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_storage_fault.rs"]
mod fault;
#[path = "support/raft_runtime_fixture.rs"]
mod fixture;

use anyhow::{bail, ensure, Result};
use cellar::application::consensus::verification::*;
use cellar::sdk::ExecutionDomain;
use cellar_logic::consensus::{LogIndex, LogPosition, OperationId, Term};
use cellar_ltx::DirectFileSystem;
use fixture::*;
use futures_util::{stream::FuturesUnordered, FutureExt, StreamExt};
use raft::prelude::{ConfChangeSingle, ConfChangeType, ConfChangeV2};
use raft::Storage;
use std::sync::Arc;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ready_persistence_order() -> Result<()> {
    let mut cluster = Cluster::new(3, &[10, 11, 12]).await?;
    let release = cluster.hosts[0].gate.arm(fault::HOLD_SYNC);
    cluster.hosts[0].runtime.campaign(&scope(10)).await?;
    tokio::time::timeout(
        Duration::from_secs(5),
        cluster.hosts[0].gate.entered.notified(),
    )
    .await?;
    assert!(
        cluster.hosts[0].events.try_recv().is_err(),
        "messages must wait for the held sync"
    );
    assert_eq!(
        cluster.hosts[0]
            .log
            .group(&scope(10))?
            .initial_state()?
            .hard_state
            .term,
        1
    );
    drop(release);
    cluster.ready(10, 0).await?;
    cluster.elect(11, 1).await?;
    cluster.elect(12, 2).await?;
    cluster.settled(&[10, 11, 12]).await?;
    cluster.persisted.clear();
    let release = cluster.hosts[0].gate.arm(fault::HOLD_SYNC);
    let accepted = futures_util::future::try_join_all([(0, 10), (1, 11), (2, 12)].into_iter().map(
        |(node, id)| {
            let runtime = cluster.hosts[node].runtime.clone();
            async move { runtime.propose(&scope(id), vec![id], vec![id; 32]).await }
        },
    ))
    .await?;
    // Every group has an outstanding proposal before any peer delivery.
    for (node, id) in [(0, 10), (1, 11), (2, 12)] {
        assert!(
            cluster.hosts[node]
                .runtime
                .status(&scope(id))
                .await?
                .committed
                < accepted[node].position.index
        );
    }
    tokio::time::timeout(
        Duration::from_secs(5),
        cluster.hosts[0].gate.entered.notified(),
    )
    .await?;
    for _ in 0..3000 {
        cluster.pump().await?;
        if cluster.hosts[0]
            .runtime
            .status(&scope(11))
            .await?
            .awaiting_ready
            && cluster.hosts[0]
                .runtime
                .status(&scope(12))
                .await?
                .awaiting_ready
        {
            break;
        }
    }
    ensure!(
        cluster.hosts[0]
            .runtime
            .status(&scope(11))
            .await?
            .awaiting_ready
            && cluster.hosts[0]
                .runtime
                .status(&scope(12))
                .await?
                .awaiting_ready
    );
    drop(release);
    for (node, id) in [(0, 10), (1, 11), (2, 12)] {
        for replica in 0..3 {
            cluster
                .applied_to(id, replica, accepted[node].position.index.get())
                .await?;
            let group = cluster.hosts[replica].log.group(&scope(id))?;
            let index = accepted[node].position.index.get();
            assert_eq!(
                &group.entries(
                    index,
                    index + 1,
                    None,
                    raft::GetEntriesContext::empty(false)
                )?[0]
                    .data[..],
                &vec![id; 32]
            );
            assert_eq!(
                cluster.hosts[replica]
                    .runtime
                    .status(&scope(id))
                    .await?
                    .is_leader,
                replica == node
            );
        }
    }
    let mut batches = std::collections::BTreeMap::<u64, std::collections::BTreeSet<_>>::new();
    for &(node, group, sequence) in &cluster.persisted {
        if node == 0 {
            batches.entry(sequence).or_default().insert(group);
        }
    }
    ensure!(
        batches
            .values()
            .any(|groups| groups.contains(&scope(11).group) && groups.contains(&scope(12).group)),
        "independent groups must share an actual synced engine batch"
    );
    cluster.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn light_ready_commit_saved() -> Result<()> {
    let mut cluster = Cluster::new(1, &[10]).await?;
    cluster.elect(10, 0).await?;
    for _ in 0..10 {
        cluster.pump().await?;
    }
    ensure!(
        cluster
            .light_commits
            .iter()
            .any(|&(n, g, index)| n == 0 && g == scope(10).group && index >= 2),
        "the test must observe actual LightReady commit advancement"
    );
    let directory = tempfile::tempdir()?;
    let signal = directory.path().join("light.signal");
    let output = directory.path().join("child.log");
    let file = std::fs::File::create(&output)?;
    let mut child = Child(
        std::process::Command::new(std::env::current_exe()?)
            .args([
                "--ignored",
                "--exact",
                "runtime_process_fixture",
                "--test-threads=1",
                "--nocapture",
            ])
            .env("CELLAR_RAFT_RUNTIME_ROOT", directory.path())
            .env("CELLAR_RAFT_RUNTIME_SIGNAL", &signal)
            .stdout(file.try_clone()?)
            .stderr(file)
            .spawn()?,
    );
    tokio::time::timeout(Duration::from_secs(20), async {
        while !signal.exists() {
            if let Some(status) = child.0.try_wait()? {
                bail!(
                    "child exited {status}: {}",
                    std::fs::read_to_string(&output)?
                );
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        Ok::<(), anyhow::Error>(())
    })
    .await??;
    child.0.kill()?;
    ensure!(!child.0.wait()?.success());
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = domain
        .run(ReplicaLog::open(
            storage_options(&directory.path().join("node-1"), 1, 50, LogOpenMode::Resume),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    let group = log.group(&scope(10))?;
    assert!(group.initial_state()?.hard_state.commit >= 2);
    assert_eq!(group.term(2)?, 2);
    log.close().await?;
    domain.shutdown().await;
    cluster.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn apply_progress_order() -> Result<()> {
    let mut cluster = Cluster::new(1, &[10]).await?;
    cluster.elect(10, 0).await?;
    cluster.hold_apply.insert((0, scope(10).group));
    let first = cluster.hosts[0]
        .runtime
        .propose(&scope(10), vec![], b"first".to_vec())
        .await?;
    for _ in 0..3000 {
        cluster.pump().await?;
        if !cluster.held_apply.is_empty() {
            break;
        }
    }
    let old = cluster
        .held_apply
        .first()
        .ok_or_else(|| anyhow::anyhow!("no apply request"))?
        .1
        .clone();
    assert_eq!(old.position(), first.position);
    let second = cluster.hosts[0]
        .runtime
        .propose(&scope(10), vec![], b"second".to_vec())
        .await?;
    for _ in 0..30 {
        cluster.pump().await?;
    }
    assert_eq!(
        cluster.held_apply.len(),
        1,
        "only the ordered apply head is issued"
    );
    assert!(cluster.hosts[0].runtime.status(&scope(10)).await?.committed >= second.position.index);
    assert!(
        cluster.hosts[0]
            .runtime
            .status(&scope(10))
            .await?
            .applied
            .index
            < first.position.index
    );
    cluster.finish_apply(0, old.clone()).await?;
    cluster
        .applied_to(10, 0, first.position.index.get())
        .await?;
    for _ in 0..100 {
        cluster.pump().await?;
        if cluster.held_apply.len() == 2 {
            break;
        }
    }
    let later = cluster
        .held_apply
        .get(1)
        .ok_or_else(|| anyhow::anyhow!("no second apply request"))?
        .1
        .clone();
    assert_eq!(later.position(), second.position);
    assert_eq!(
        cluster.hosts[0]
            .runtime
            .complete_apply(ApplyCompletion {
                image: write_image(&cluster.hosts[0].path, &old)?,
                ticket: old.clone()
            })
            .await,
        Err(RuntimeError::StaleCompletion)
    );
    assert_eq!(
        cluster.hosts[0].runtime.status(&scope(10)).await?.applied,
        first.position
    );
    cluster.finish_apply(0, later.clone()).await?;
    cluster
        .applied_to(10, 0, second.position.index.get())
        .await?;
    // Retire a worker with committed but unapplied work, then reissue it in the
    // same process/disk identity. The old worker's callback cannot finish it.
    let third = cluster.hosts[0]
        .runtime
        .propose(&scope(10), vec![], b"third".to_vec())
        .await?;
    for _ in 0..3000 {
        cluster.pump().await?;
        if cluster.held_apply.len() == 3 {
            break;
        }
    }
    let retired = cluster
        .held_apply
        .get(2)
        .ok_or_else(|| anyhow::anyhow!("no third apply"))?
        .1
        .clone();
    cluster.hosts[0].runtime.close().await?;
    let (runtime, events) =
        RaftRuntime::start(cluster.hosts[0].log.clone(), runtime_options(1)).await?;
    cluster.hosts[0].runtime = runtime;
    cluster.hosts[0].events = events;
    for _ in 0..3000 {
        cluster.pump().await?;
        if cluster.held_apply.len() == 4 {
            break;
        }
    }
    let current = cluster
        .held_apply
        .get(3)
        .ok_or_else(|| anyhow::anyhow!("no reissued apply"))?
        .1
        .clone();
    assert_eq!(current.replica(), retired.replica());
    assert_eq!(current.position(), retired.position());
    assert_eq!(current.entry_digest(), retired.entry_digest());
    assert_eq!(
        cluster.hosts[0]
            .runtime
            .complete_apply(ApplyCompletion {
                image: write_image(&cluster.hosts[0].path, &retired)?,
                ticket: retired
            })
            .await,
        Err(RuntimeError::StaleCompletion)
    );
    assert_eq!(
        cluster.hosts[0].runtime.status(&scope(10)).await?.applied,
        second.position
    );
    cluster.finish_apply(0, current).await?;
    cluster
        .applied_to(10, 0, third.position.index.get())
        .await?;
    cluster.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_group_isolation() -> Result<()> {
    let mut cluster = Cluster::new(3, &[10, 11, 12]).await?;
    for (id, leader) in [(10, 0), (11, 1), (12, 2)] {
        cluster.elect(id, leader).await?;
    }
    cluster.hold_apply.insert((0, scope(10).group));
    let blocked = cluster.hosts[0]
        .runtime
        .propose(&scope(10), vec![], b"held apply".to_vec())
        .await?;
    for _ in 0..3000 {
        cluster.pump().await?;
        if !cluster.held_apply.is_empty() {
            break;
        }
    }
    ensure!(!cluster.held_apply.is_empty());
    let release = cluster.hosts[0].gate.arm(fault::HOLD_SYNC);
    let first = cluster.hosts[1]
        .runtime
        .propose(&scope(11), vec![], b"other leader".to_vec())
        .await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            cluster.pump().await?;
            if cluster.hosts[0]
                .runtime
                .status(&scope(11))
                .await?
                .awaiting_ready
            {
                return Ok::<(), anyhow::Error>(());
            }
        }
    })
    .await??;
    tokio::time::timeout(
        Duration::from_secs(5),
        cluster.hosts[0].gate.entered.notified(),
    )
    .await?;
    let second = cluster.hosts[2]
        .runtime
        .propose(&scope(12), vec![], b"third leader".to_vec())
        .await?;
    cluster
        .applied_to(11, 1, first.position.index.get())
        .await?;
    cluster
        .applied_to(12, 2, second.position.index.get())
        .await?;
    assert!(
        cluster.hosts[0]
            .runtime
            .status(&scope(10))
            .await?
            .applied
            .index
            < blocked.position.index
    );
    drop(release);
    let ticket = cluster.held_apply[0].1.clone();
    cluster.finish_apply(0, ticket).await?;
    cluster
        .applied_to(10, 0, blocked.position.index.get())
        .await?;
    cluster.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn noop_configuration_progress() -> Result<()> {
    let mut cluster = Cluster::new(1, &[10]).await?;
    cluster.elect(10, 0).await?;
    assert_eq!(
        cluster.hosts[0].log.group(&scope(10))?.applied()?.image,
        None
    );
    let command = cluster.hosts[0]
        .runtime
        .propose(&scope(10), vec![], b"state image".to_vec())
        .await?;
    cluster
        .applied_to(10, 0, command.position.index.get())
        .await?;
    let before = cluster.hosts[0].log.group(&scope(10))?.applied()?.image;
    let single = ConfChangeSingle {
        node_id: 2,
        change_type: ConfChangeType::AddLearnerNode,
        ..Default::default()
    };
    let mut change = ConfChangeV2::default();
    change.set_changes(vec![single].into());
    let accepted = cluster.hosts[0]
        .runtime
        .configure(&scope(10), vec![], change)
        .await?;
    cluster
        .applied_to(10, 0, accepted.position.index.get())
        .await?;
    let group = cluster.hosts[0].log.group(&scope(10))?;
    assert_eq!(group.initial_state()?.conf_state.learners, vec![2]);
    assert_eq!(
        group.applied()?.image,
        before,
        "protocol entries cannot invent data changes"
    );
    cluster.hosts[0].runtime.close().await?;
    cluster.hosts[0].log.close().await?;
    let log = cluster.hosts[0]
        .domain
        .run(ReplicaLog::open(
            storage_options(&cluster.hosts[0].path, 1, 60, LogOpenMode::Resume),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    assert_eq!(
        log.group(&scope(10))?.initial_state()?.conf_state.learners,
        vec![2]
    );
    log.close().await?;
    cluster.hosts[0].domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn current_term_admission_barrier() -> Result<()> {
    let mut cluster = Cluster::new(3, &[10]).await?;
    cluster.hold_append_responses.insert(scope(10).group);
    cluster.hosts[0].runtime.campaign(&scope(10)).await?;
    for _ in 0..3000 {
        cluster.pump().await?;
        if cluster.hosts[0].runtime.status(&scope(10)).await?.is_leader {
            break;
        }
    }
    let status = cluster.hosts[0].runtime.status(&scope(10)).await?;
    ensure!(status.is_leader);
    assert!(!status.protocol_ready);
    assert!(matches!(
        cluster.hosts[0]
            .runtime
            .propose(&scope(10), vec![], b"too soon".to_vec())
            .await,
        Err(RuntimeError::NotReady)
    ));
    cluster.release_messages();
    cluster.ready(10, 0).await?;
    let command = cluster.hosts[0]
        .runtime
        .propose(&scope(10), vec![], b"after barrier".to_vec())
        .await?;
    cluster
        .applied_to(10, 0, command.position.index.get())
        .await?;
    cluster.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_requires_verified_install() -> Result<()> {
    let mut cluster = Cluster::new(3, &[10]).await?;
    cluster.allow_snapshots = true;
    // Seed a leader's durable checkpoint before constructing its RawNode.
    cluster.hosts[0].runtime.close().await?;
    let group = cluster.hosts[0].log.group(&scope(10))?;
    let mut snapshot = raft::prelude::Snapshot::default();
    snapshot.mut_metadata().index = 5;
    snapshot.mut_metadata().term = 3;
    snapshot
        .mut_metadata()
        .set_conf_state(group.initial_state()?.conf_state);
    let bytes = b"application fixture at 3:5";
    let image_path = cluster.hosts[0].path.join("seed.fixture");
    std::fs::write(&image_path, bytes)?;
    std::fs::File::open(&image_path)?.sync_all()?;
    std::fs::File::open(&cluster.hosts[0].path)?.sync_all()?;
    let image = CommittedImage {
        name: "seed.fixture".into(),
        digest: <sha2::Sha256 as sha2::Digest>::digest(bytes).into(),
        length: bytes.len() as u64,
        ltx_epoch: 1,
        txid: 5,
        page_size: 512,
        checksum: 5,
    };
    let mut write = GroupWrite::new(&group);
    write.snapshot = Some(SnapshotChange::Install {
        snapshot,
        applied: DurableApply {
            position: LogPosition {
                index: LogIndex::new(5),
                term: Term::new(3),
            },
            image: Some(image),
        },
    });
    let mut hard = group.initial_state()?.hard_state;
    hard.term = 3;
    hard.commit = 5;
    write.hard_state = Some(hard);
    cluster.hosts[0]
        .log
        .persist(OperationId::new(500), vec![write])
        .await?;
    let (runtime, events) =
        RaftRuntime::start(cluster.hosts[0].log.clone(), runtime_options(1)).await?;
    cluster.hosts[0].runtime = runtime;
    cluster.hosts[0].events = events;
    cluster.hosts[0].runtime.campaign(&scope(10)).await?;
    for _ in 0..3000 {
        cluster.pump().await?;
        if !cluster.held_snapshots.is_empty() {
            break;
        }
    }
    ensure!(
        !cluster.held_snapshots.is_empty(),
        "a lagging follower must request an actual Raft snapshot"
    );
    for &(node, ref ticket, _) in &cluster.held_snapshots {
        assert_eq!(
            cluster.hosts[node]
                .log
                .group(&scope(10))?
                .applied()?
                .position
                .index
                .get(),
            1
        );
        assert_eq!(ticket.position().index.get(), 5);
    }
    let held = cluster.held_snapshots.clone();
    for (node, ticket, _) in held {
        let mut applied = DurableApply {
            position: ticket.position(),
            image: Some(write_image(&cluster.hosts[node].path, &ticket)?),
        };
        applied.position.index = LogIndex::new(6);
        assert_eq!(
            cluster.hosts[node]
                .runtime
                .complete_snapshot(SnapshotCompletion {
                    ticket: ticket.clone(),
                    applied: applied.clone()
                })
                .await,
            Err(RuntimeError::StaleCompletion)
        );
        applied.position = ticket.position();
        cluster.hosts[node]
            .runtime
            .complete_snapshot(SnapshotCompletion { ticket, applied })
            .await?;
    }
    cluster.ready(10, 0).await?;
    for _ in 0..30 {
        cluster.pump().await?;
    }
    ensure!(!cluster.installed_snapshots.is_empty());
    cluster.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_peer_message_is_refused() -> Result<()> {
    let mut cluster = Cluster::new(3, &[10]).await?;
    cluster.elect(10, 0).await?;
    let mut message = raft::prelude::Message {
        from: 1,
        to: 2,
        term: u64::MAX,
        index: u64::MAX,
        ..Default::default()
    };
    message.set_msg_type(raft::prelude::MessageType::MsgAppend);
    message.term = u64::MAX;
    message.index = u64::MAX;
    assert!(matches!(
        cluster.hosts[1]
            .runtime
            .step(&scope(10), message.clone())
            .await,
        Err(RuntimeError::Invalid(_))
    ));
    message.term = 2;
    message.index = 1;
    message.log_term = 1;
    let entry = raft::prelude::Entry {
        index: 4,
        term: 2,
        ..Default::default()
    };
    message.set_entries(vec![entry].into());
    assert!(matches!(
        cluster.hosts[1].runtime.step(&scope(10), message).await,
        Err(RuntimeError::Invalid(_))
    ));
    let mut read = raft::prelude::Message {
        from: 2,
        to: 1,
        ..Default::default()
    };
    read.set_msg_type(raft::prelude::MessageType::MsgReadIndex);
    assert!(
        matches!(
            cluster.hosts[0].runtime.step(&scope(10), read).await,
            Err(RuntimeError::Invalid(_))
        ),
        "an empty read request must not reach raft-rs' indexed entry access"
    );
    let mut invalid_snapshot = raft::prelude::Message {
        from: 1,
        to: 2,
        term: 2,
        ..Default::default()
    };
    invalid_snapshot.set_msg_type(raft::prelude::MessageType::MsgSnapshot);
    assert!(matches!(
        cluster.hosts[1]
            .runtime
            .step(&scope(10), invalid_snapshot.clone())
            .await,
        Err(RuntimeError::Invalid(_))
    ));
    invalid_snapshot.mut_snapshot().mut_metadata().index = 5;
    invalid_snapshot.mut_snapshot().mut_metadata().term = 2;
    invalid_snapshot
        .mut_snapshot()
        .mut_metadata()
        .mut_conf_state()
        .set_voters((1..=8).collect());
    assert!(matches!(
        cluster.hosts[1]
            .runtime
            .step(&scope(10), invalid_snapshot)
            .await,
        Err(RuntimeError::Invalid(_))
    ));
    let command = cluster.hosts[0]
        .runtime
        .propose(&scope(10), vec![], b"still healthy".to_vec())
        .await?;
    cluster
        .applied_to(10, 1, command.position.index.get())
        .await?;
    cluster.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn runtime_close_fences_full_queue() -> Result<()> {
    let mut cluster = Cluster::new(1, &[10]).await?;
    cluster.hosts[0].runtime.close().await?;
    let mut options = runtime_options(1);
    options.max_requests = 4;
    let (runtime, events) = RaftRuntime::start(cluster.hosts[0].log.clone(), options).await?;
    cluster.hosts[0].runtime = runtime;
    cluster.hosts[0].events = events;
    assert!(matches!(
        RaftRuntime::start(cluster.hosts[0].log.clone(), runtime_options(1)).await,
        Err(RuntimeError::Storage(LogError::AlreadyExists))
    ));
    let release = cluster.hosts[0].gate.arm(fault::HOLD_SYNC);
    cluster.hosts[0].runtime.campaign(&scope(10)).await?;
    tokio::time::timeout(
        Duration::from_secs(5),
        cluster.hosts[0].gate.entered.notified(),
    )
    .await?;
    let mut pending = FuturesUnordered::new();
    for _ in 0..4 {
        let runtime = cluster.hosts[0].runtime.clone();
        pending.push(
            async move {
                runtime
                    .propose(&scope(10), vec![], b"queued".to_vec())
                    .await
            }
            .boxed(),
        );
    }
    assert!(pending.next().now_or_never().is_none());
    assert_eq!(
        cluster.hosts[0].runtime.status(&scope(10)).await,
        Err(RuntimeError::Overloaded)
    );
    let runtime = cluster.hosts[0].runtime.clone();
    let mut closing = cluster.hosts[0]
        .domain
        .spawn(async move { runtime.close().await })?;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut closing)
            .await
            .is_err(),
        "close must wait for submitted I/O"
    );
    drop(release);
    closing.await??;
    let (runtime, _events) =
        RaftRuntime::start(cluster.hosts[0].log.clone(), runtime_options(1)).await?;
    runtime.close().await?;
    cluster.hosts[0].log.close().await?;
    cluster.hosts[0].domain.shutdown().await;
    assert_eq!(runtime.status(&scope(10)).await, Err(RuntimeError::Closed));
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn election_randomness_is_explicit() -> Result<()> {
    let first = Cluster::new(3, &[10, 11, 12]).await?;
    let second = Cluster::new(3, &[10, 11, 12]).await?;
    for node in 0..3 {
        for id in [10, 11, 12] {
            assert_eq!(
                first.hosts[node]
                    .runtime
                    .status(&scope(id))
                    .await?
                    .election_timeout,
                second.hosts[node]
                    .runtime
                    .status(&scope(id))
                    .await?
                    .election_timeout
            );
        }
    }
    first.close().await?;
    second.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn owned_clock_elects_without_campaign() -> Result<()> {
    let mut cluster = Cluster::new(1, &[10]).await?;
    cluster.hosts[0].runtime.close().await?;
    let mut options = runtime_options(1);
    options.tick_interval_ms = Some(1);
    let (runtime, events) = RaftRuntime::start(cluster.hosts[0].log.clone(), options).await?;
    cluster.hosts[0].runtime = runtime;
    cluster.hosts[0].events = events;
    cluster.ready(10, 0).await?;
    assert!(
        cluster.hosts[0]
            .runtime
            .status(&scope(10))
            .await?
            .term
            .get()
            > 1
    );
    cluster.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_readindex_is_bounded() -> Result<()> {
    let mut cluster = Cluster::new(3, &[10, 11]).await?;
    cluster.elect(10, 0).await?;
    cluster.elect(11, 1).await?;
    cluster.hold_read_responses = true;
    for index in 0_u64..256 {
        cluster.hosts[0]
            .runtime
            .read_index(&scope(10), index.to_be_bytes().to_vec())
            .await?;
        cluster.pump().await?;
    }
    assert_eq!(
        cluster.hosts[0]
            .runtime
            .read_index(&scope(10), b"over budget".to_vec())
            .await,
        Err(RuntimeError::Overloaded)
    );
    assert_eq!(
        cluster.hosts[0]
            .runtime
            .read_index(&scope(11), b"node budget".to_vec())
            .await,
        Err(RuntimeError::Overloaded)
    );
    cluster.release_messages();
    for _ in 0..3000 {
        cluster.pump().await?;
        match cluster.hosts[0]
            .runtime
            .read_index(&scope(10), b"after recovery".to_vec())
            .await
        {
            Ok(()) => return cluster.close().await,
            Err(RuntimeError::Overloaded) => (),
            Err(error) => return Err(error.into()),
        }
    }
    bail!("read index admission never recovered after peer acknowledgements")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn domain_shutdown_joins_consensus_and_log() -> Result<()> {
    let mut cluster = Cluster::new(1, &[10]).await?;
    cluster.elect(10, 0).await?;
    let release = cluster.hosts[0].gate.arm(fault::HOLD_SYNC);
    cluster.hosts[0]
        .runtime
        .propose(&scope(10), vec![], b"in-flight at closure".to_vec())
        .await?;
    tokio::time::timeout(
        Duration::from_secs(5),
        cluster.hosts[0].gate.entered.notified(),
    )
    .await?;
    let domain = &cluster.hosts[0].domain;
    let mut shutdown = Box::pin(domain.shutdown());
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut shutdown)
            .await
            .is_err()
    );
    assert_eq!(
        cluster.hosts[0].runtime.status(&scope(10)).await,
        Err(RuntimeError::Closed)
    );
    drop(release);
    tokio::time::timeout(Duration::from_secs(5), shutdown).await?;
    assert!(cluster.hosts[0].log.group(&scope(10)).is_err());
    // Retained consensus/storage handles cannot retain the old directory locks.
    let next = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = next
        .run(ReplicaLog::open(
            storage_options(&cluster.hosts[0].path, 1, 70, LogOpenMode::Resume),
            Arc::new(DirectFileSystem),
        ))
        .await??;
    assert_eq!(log.group(&scope(10))?.applied()?.position.index.get(), 2);
    assert_eq!(log.group(&scope(10))?.initial_state()?.hard_state.commit, 2);
    log.close().await?;
    next.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_engine_closes_with_typed_error() -> Result<()> {
    let mut cluster = Cluster::new(1, &[10, 11, 12]).await?;
    for id in [10, 11, 12] {
        cluster.elect(id, 0).await?;
    }
    cluster.settled(&[10, 11, 12]).await?;
    let _fault = cluster.hosts[0].gate.arm(fault::FAIL_SYNC);
    cluster.hosts[0]
        .runtime
        .propose(&scope(10), vec![], b"fail sync".to_vec())
        .await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match cluster.hosts[0].events.try_recv() {
                Ok(_) => (),
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => return,
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {
                    tokio::time::sleep(Duration::from_millis(1)).await
                }
            }
        }
    })
    .await?;
    cluster.hosts[0].domain.shutdown().await;
    assert!(
        matches!(
            cluster.hosts[0].runtime.close().await,
            Err(RuntimeError::Storage(LogError::Io(_)))
        ),
        "engine failure must retain its typed cause rather than a consensus-task panic"
    );
    assert!(cluster.hosts[0].log.group(&scope(11)).is_err());
    Ok(())
}

struct Child(std::process::Child);

impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "process-kill helper, invoked only by its parent test"]
async fn runtime_process_fixture() -> Result<()> {
    let root = std::path::PathBuf::from(std::env::var("CELLAR_RAFT_RUNTIME_ROOT")?);
    let signal = std::path::PathBuf::from(std::env::var("CELLAR_RAFT_RUNTIME_SIGNAL")?);
    let mut cluster = Cluster::at(&root, 1, &[10]).await?;
    cluster.elect(10, 0).await?;
    for _ in 0..100 {
        cluster.pump().await?;
        if !cluster.light_commits.is_empty() {
            break;
        }
    }
    ensure!(!cluster.light_commits.is_empty());
    std::fs::write(&signal, b"LightReady commit sync observed")?;
    std::fs::File::open(signal)?.sync_all()?;
    loop {
        std::thread::park();
    }
}
