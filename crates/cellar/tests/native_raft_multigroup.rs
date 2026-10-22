//! Multi-group consensus scheduling and real-engine process-crash boundaries.
//! The scheduler fixture's application markers are stubs. The repair-load
//! fixture and container runner exercise actual native SQLite/LTX publication.
#![allow(clippy::disallowed_methods)]
#[allow(dead_code)]
#[path = "support/raft_catalog_fault.rs"]
mod bundle_faults;
#[allow(dead_code)]
#[path = "support/raft_ltx_fixture.rs"]
mod bundle_images;
#[path = "support/raft_bundle_native.rs"]
mod bundles;
#[path = "support/raft_storage_fault.rs"]
mod fault;
#[allow(dead_code)] // Shared fixture also serves snapshot, peer-fault and lifecycle targets.
#[path = "support/raft_runtime_fixture.rs"]
mod fixture;
#[allow(dead_code)] // Shared native fixture also serves membership and snapshot drills.
#[path = "support/raft_cluster.rs"]
mod native_cluster;
#[allow(dead_code)]
#[path = "support/raft_node.rs"]
mod public_node;
#[path = "support/raft_repair_load.rs"]
mod repair_load;

use anyhow::{ensure, Result};
use cellar::application::consensus::verification::*;
use cellar::sdk::ExecutionDomain;
use cellar_logic::consensus::OperationId;
use cellar_ltx::DirectFileSystem;
use fixture::*;
use raft::{prelude::Entry, GetEntriesContext, Storage};
use std::{path::Path, sync::Arc, time::Duration};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repair_budget_under_load() -> Result<()> {
    repair_load::run().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn archive_bundle_group_index() -> Result<()> {
    bundles::run().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn many_groups_mixed_roles() -> Result<()> {
    let ids: Vec<u8> = (10..22).collect();
    let mut cluster = Cluster::new(3, &ids).await?;
    for (index, &id) in ids.iter().enumerate() {
        cluster.elect(id, index % 3).await?;
    }
    cluster.settled(&ids).await?;
    for (index, &id) in ids.iter().enumerate() {
        let leader = index % 3;
        let receipt = cluster.hosts[leader]
            .runtime
            .propose(&scope(id), vec![id], vec![id; 73])
            .await?;
        for member in 0..3 {
            cluster
                .applied_to(id, member, receipt.position.index.get())
                .await?;
            let state = cluster.hosts[member].runtime.status(&scope(id)).await?;
            assert_eq!(state.is_leader, member == leader);
            let log = cluster.hosts[member].log.group(&scope(id))?;
            let entries = log.entries(
                receipt.position.index.get(),
                receipt.position.index.get() + 1,
                None,
                GetEntriesContext::empty(false),
            )?;
            assert_eq!(&entries[0].data[..], &[id; 73]);
        }
    }
    cluster.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hot_group_fairness() -> Result<()> {
    let mut cluster = Cluster::new(3, &[10, 11, 12]).await?;
    cluster.elect(10, 0).await?;
    cluster.elect(11, 1).await?;
    cluster.settled(&[10, 11]).await?;
    // Fill one leader's per-group queue behind a retained apply. Other groups
    // must still complete writes and a previously unelected group must elect.
    cluster.hold_apply.insert((0, scope(10).group));
    cluster.hosts[0]
        .runtime
        .propose(&scope(10), vec![1], vec![10; 4096])
        .await?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while cluster.held_apply.is_empty() {
            cluster.pump().await?;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let mut flood = tokio::task::JoinSet::new();
    for index in 0..64u8 {
        let runtime = cluster.hosts[0].runtime.clone();
        flood.spawn(async move {
            runtime
                .propose(&scope(10), vec![index], vec![index; 4096])
                .await
        });
    }
    let receipt = cluster.hosts[1]
        .runtime
        .propose(&scope(11), vec![2], vec![11; 1024])
        .await?;
    for member in 0..3 {
        cluster
            .applied_to(11, member, receipt.position.index.get())
            .await?;
    }
    cluster.elect(12, 2).await?;
    let elected = cluster.hosts[2].runtime.status(&scope(12)).await?;
    assert!(elected.is_leader && elected.protocol_ready);
    assert!(
        !cluster.held_apply.is_empty(),
        "hot apply remained held throughout independent progress"
    );
    // Cancellation drops request waiters; runtime closure physically joins the
    // retained work. No accepted proposal is claimed to be a committed result.
    flood.abort_all();
    while let Some(result) = flood.join_next().await {
        match result {
            Ok(result) => {
                assert!(result.is_ok() || matches!(result, Err(RuntimeError::Overloaded)))
            }
            Err(error) => ensure!(error.is_cancelled(), "hot caller panicked: {error}"),
        }
    }
    cluster.close().await
}

async fn open_batch(
    path: &Path,
    mode: LogOpenMode,
    process: u8,
    fs: Arc<dyn cellar_ltx::FileSystem>,
) -> Result<(ExecutionDomain, ReplicaLog)> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = domain
        .run(ReplicaLog::open(
            storage_options(path, 1, process, mode),
            fs,
        ))
        .await??;
    Ok((domain, log))
}

async fn persist_batch(log: &ReplicaLog, index: u64) -> Result<()> {
    let mut writes = Vec::new();
    for id in [10, 11, 12] {
        let group = log.group(&scope(id))?;
        let mut write = GroupWrite::new(&group);
        let mut hard = group.initial_state()?.hard_state;
        hard.commit = index;
        write.hard_state = Some(hard);
        write.entries.push(Entry {
            index,
            term: 1,
            data: vec![id; index as usize * 31].into(),
            ..Default::default()
        });
        writes.push(write);
    }
    let receipt = log.persist(OperationId::new(index), writes).await?;
    assert_eq!(receipt.groups.len(), 3);
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
async fn cross_group_batch_crash() -> Result<()> {
    for cut in [fault::CRASH_BEFORE_SYNC, fault::CRASH_AFTER_SYNC, 0] {
        let root = tempfile::tempdir()?;
        let path = root.path().join("node");
        let signal = root.path().join("cut.signal");
        let (domain, log) =
            open_batch(&path, LogOpenMode::Create, 1, Arc::new(DirectFileSystem)).await?;
        for id in [10, 11, 12] {
            seed_group(&log, id, 1, 3).await?;
        }
        persist_batch(&log, 2).await?;
        log.close().await?;
        domain.shutdown().await;
        let output = root.path().join("child.log");
        let file = std::fs::File::create(&output)?;
        let mut child = Child(
            std::process::Command::new(std::env::current_exe()?)
                .args(["--ignored", "--exact", "batch_crash_child", "--nocapture"])
                .env("CELLAR_BATCH_ROOT", &path)
                .env("CELLAR_BATCH_SIGNAL", &signal)
                .env("CELLAR_BATCH_CUT", cut.to_string())
                .stdout(file.try_clone()?)
                .stderr(file)
                .spawn()?,
        );
        tokio::time::timeout(Duration::from_secs(20), async {
            while !signal.exists() {
                ensure!(
                    child.0.try_wait()?.is_none(),
                    "child exited before cut: {}",
                    std::fs::read_to_string(&output)?
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        child.0.kill()?;
        ensure!(!child.0.wait()?.success());
        let (domain, log) =
            open_batch(&path, LogOpenMode::Resume, 3, Arc::new(DirectFileSystem)).await?;
        for id in [10, 11, 12] {
            let group = log.group(&scope(id))?;
            let committed = group.initial_state()?.hard_state.commit;
            if cut == fault::CRASH_BEFORE_SYNC {
                assert!([2, 3].contains(&committed));
            } else {
                assert_eq!(committed, 3);
            }
            assert_eq!(group.last_index()?, committed);
            for index in 2..=committed {
                let entries =
                    group.entries(index, index + 1, None, GetEntriesContext::empty(false))?;
                assert_eq!(&entries[0].data[..], &vec![id; index as usize * 31]);
            }
        }
        log.close().await?;
        domain.shutdown().await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "process-kill helper launched by cross_group_batch_crash"]
async fn batch_crash_child() -> Result<()> {
    let path = std::env::var("CELLAR_BATCH_ROOT")?;
    let signal = std::path::PathBuf::from(std::env::var("CELLAR_BATCH_SIGNAL")?);
    let cut = std::env::var("CELLAR_BATCH_CUT")?.parse()?;
    let gate = fault::Gate::new(Some(signal.clone()));
    let (_domain, log) = open_batch(
        Path::new(&path),
        LogOpenMode::Resume,
        2,
        Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
    )
    .await?;
    let _release = gate.arm(cut);
    persist_batch(&log, 3).await?;
    if cut == 0 {
        std::fs::write(&signal, b"acknowledged")?;
        std::fs::File::open(signal)?.sync_all()?;
    }
    std::future::pending::<()>().await;
    Ok(())
}

#[test]
fn harness_rejects_stale_binary() -> Result<()> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let output = std::process::Command::new("python3")
        .args(["-m", "unittest", "test_raft_run.Freshness"])
        .current_dir(root.join("tools/perf"))
        .output()?;
    ensure!(
        output.status.success(),
        "image refusal cases failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
