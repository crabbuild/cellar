// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Lifecycle guards use the real serialized engine, fsync and retained reopen.
use super::*;
use crate::asyncrt::ExecutionDomain;
use crate::raft::fleet_stop::{BootBinding, StopSelector};
use cellar_logic::consensus::{DiskLineage, NodeId, ProcessGeneration};

use crate::raft::storage_fault as fault;

fn options(root: &std::path::Path) -> ReplicaLogOptions {
    ReplicaLogOptions::new(
        root.join("log"),
        NodeId::new([1; 16]).unwrap(),
        DiskLineage::new([2; 16]).unwrap(),
        ProcessGeneration::new([3; 16]).unwrap(),
        LogOpenMode::Create,
    )
}

fn boot(log: &ReplicaLog, epoch: u64, nonce: u8) -> anyhow::Result<BootBinding> {
    Ok(BootBinding {
        catalog: [4; 32],
        fleet: [5; 16],
        node: log.limits().node.bytes(),
        disk: log.limits().disk.bytes(),
        engine: log.storage_instance()?.bytes(),
        boot: [nonce; 16],
        process: log.limits().process.bytes(),
        epoch,
        join_key: crate::peer_probe::PeerProbeSigner::from_secret([6; 32]).public_key_bytes(),
    })
}

fn deadline(log: &ReplicaLog) -> anyhow::Result<u64> {
    Ok(log.domain().mono_ms()? + 5000)
}

fn stop(boot: &BootBinding, epoch: u64) -> StopSelector {
    StopSelector {
        purpose: crate::raft::fleet_stop::LifecyclePurpose::Evacuation,
        boot: boot.clone(),
        epoch,
        operation: [10; 16],
        finalization: [11; 16],
        request: [12; 16],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fleet_lifecycle_quiesce_and_same_epoch_fence_survive_reopen() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut options = options(root.path());
    let log = domain
        .run(ReplicaLog::open(
            options.clone(),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let binding = boot(&log, 1, 7)?;
    log.update_lifecycle(LifecycleUpdate::Boot {
        boot: binding.clone(),
        deadline: deadline(&log)?,
    })
    .await?;
    let mut selector = stop(&binding, 2);
    assert!(matches!(
        log.update_lifecycle(LifecycleUpdate::Quiesce {
            selector: selector.clone(),
            deadline: deadline(&log)?
        })
        .await,
        Err(LogError::WrongIdentity)
    ));
    selector.purpose = crate::raft::fleet_stop::LifecyclePurpose::Maintenance;
    for _ in 0..2 {
        log.update_lifecycle(LifecycleUpdate::Quiesce {
            selector: selector.clone(),
            deadline: deadline(&log)?,
        })
        .await?;
    }
    assert_eq!(log.lifecycle_guard()?.unwrap().quiescing(), Some(&selector));
    assert!(log.lifecycle_guard()?.unwrap().accepted().is_none());
    log.close().await?;
    options.mode = LogOpenMode::Resume;
    let log = domain
        .run(ReplicaLog::open(
            options.clone(),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    assert_eq!(log.lifecycle_guard()?.unwrap().quiescing(), Some(&selector));
    for _ in 0..2 {
        log.update_lifecycle(LifecycleUpdate::Fence {
            boot: binding.clone(),
            epoch: selector.epoch,
            operation: selector.operation,
            request: [13; 16],
            deadline: deadline(&log)?,
        })
        .await?;
    }
    log.close().await?;
    let log = domain
        .run(ReplicaLog::open(
            options,
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    assert!(log.lifecycle_guard()?.unwrap().quiescing().is_none());
    assert!(matches!(
        log.update_lifecycle(LifecycleUpdate::Quiesce {
            selector: selector.clone(),
            deadline: deadline(&log)?
        })
        .await,
        Err(LogError::WrongIdentity)
    ));
    assert!(matches!(
        log.update_lifecycle(LifecycleUpdate::Stop {
            selector: stop(&binding, 2),
            deadline: deadline(&log)?
        })
        .await,
        Err(LogError::WrongIdentity)
    ));
    assert!(matches!(
        log.update_lifecycle(LifecycleUpdate::Fence {
            boot: binding,
            epoch: selector.epoch,
            operation: selector.operation,
            request: [14; 16],
            deadline: deadline(&log)?
        })
        .await,
        Err(LogError::WrongIdentity)
    ));
    selector.epoch += 1;
    log.update_lifecycle(LifecycleUpdate::Quiesce {
        selector: selector.clone(),
        deadline: deadline(&log)?,
    })
    .await?;
    assert_eq!(log.lifecycle_guard()?.unwrap().quiescing(), Some(&selector));
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fleet_lifecycle_fences_and_boot_nonce_survive_reopen() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut options = options(root.path());
    let log = domain
        .run(ReplicaLog::open(
            options.clone(),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let first = boot(&log, 1, 7)?;
    log.update_lifecycle(LifecycleUpdate::Boot {
        boot: first.clone(),
        deadline: deadline(&log)?,
    })
    .await?;
    log.update_lifecycle(LifecycleUpdate::Fence {
        boot: first.clone(),
        epoch: 2,
        operation: [10; 16],
        request: [13; 16],
        deadline: deadline(&log)?,
    })
    .await?;
    assert!(matches!(
        log.update_lifecycle(LifecycleUpdate::Stop {
            selector: stop(&first, 2),
            deadline: deadline(&log)?
        })
        .await,
        Err(LogError::WrongIdentity)
    ));
    log.close().await?;
    options.mode = LogOpenMode::Resume;
    // Deliberately reuse the process hint: the boot nonce must still fence old
    // requests independently of a caller's process-generation convention.
    let log = domain
        .run(ReplicaLog::open(
            options.clone(),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let rebound = boot(&log, 3, 7)?;
    assert!(matches!(
        log.update_lifecycle(LifecycleUpdate::Boot {
            boot: rebound,
            deadline: deadline(&log)?
        })
        .await,
        Err(LogError::WrongIdentity)
    ));
    let second = boot(&log, 3, 8)?;
    log.update_lifecycle(LifecycleUpdate::Boot {
        boot: second.clone(),
        deadline: deadline(&log)?,
    })
    .await?;
    assert!(matches!(
        log.update_lifecycle(LifecycleUpdate::Stop {
            selector: stop(&first, 2),
            deadline: deadline(&log)?
        })
        .await,
        Err(LogError::WrongIdentity)
    ));
    let accepted = stop(&second, 4);
    log.update_lifecycle(LifecycleUpdate::Stop {
        selector: accepted.clone(),
        deadline: deadline(&log)?,
    })
    .await?;
    log.update_lifecycle(LifecycleUpdate::Stop {
        selector: accepted.clone(),
        deadline: deadline(&log)?,
    })
    .await?;
    assert!(matches!(
        log.update_lifecycle(LifecycleUpdate::Fence {
            boot: second,
            epoch: 5,
            operation: [10; 16],
            request: [14; 16],
            deadline: deadline(&log)?
        })
        .await,
        Err(LogError::AlreadyExists)
    ));
    log.close().await?;
    assert_eq!(log.lifecycle_guard()?.unwrap().accepted(), Some(&accepted));
    let log = domain
        .run(ReplicaLog::open(
            options,
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    assert_eq!(log.lifecycle_guard()?.unwrap().accepted(), Some(&accepted));
    let mut foreign = boot(&log, 6, 9)?;
    log.update_lifecycle(LifecycleUpdate::Boot {
        boot: foreign.clone(),
        deadline: deadline(&log)?,
    })
    .await?;
    assert!(matches!(
        log.update_lifecycle(LifecycleUpdate::Stop {
            selector: accepted,
            deadline: deadline(&log)?
        })
        .await,
        Err(LogError::WrongIdentity)
    ));
    foreign.catalog = [90; 32];
    assert!(matches!(
        log.update_lifecycle(LifecycleUpdate::Boot {
            boot: foreign,
            deadline: deadline(&log)?
        })
        .await,
        Err(LogError::WrongIdentity)
    ));
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fleet_lifecycle_expired_queued_stop_has_no_effect() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = fault::Gate::new(None);
    let log = domain
        .run(ReplicaLog::open(
            options(root.path()),
            Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
        ))
        .await??;
    let binding = boot(&log, 1, 7)?;
    log.update_lifecycle(LifecycleUpdate::Boot {
        boot: binding.clone(),
        deadline: deadline(&log)?,
    })
    .await?;
    let release = gate.arm(fault::HOLD_SYNC);
    let writer = log.clone();
    let prior = domain.spawn(async move { writer.reserve_node_revision().await })?;
    domain
        .run(crate::asyncrt::timeout(
            Duration::from_secs(5),
            gate.entered.notified(),
        ))
        .await??;
    let mut pending = Box::pin(log.update_lifecycle(LifecycleUpdate::Stop {
        selector: stop(&binding, 2),
        deadline: log.domain().mono_ms()? + 25,
    }));
    assert!(futures_util::poll!(pending.as_mut()).is_pending());
    domain
        .run(async { crate::asyncrt::sleep(Duration::from_millis(75)).await })
        .await?;
    drop(release);
    prior.await??;
    assert!(
        matches!(pending.await, Err(LogError::Invalid(reason)) if reason == "expired node lifecycle command")
    );
    assert!(log.lifecycle_guard()?.unwrap().accepted().is_none());
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fleet_lifecycle_failed_sync_grants_no_cached_acceptance() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = fault::Gate::new(None);
    let log = domain
        .run(ReplicaLog::open(
            options(root.path()),
            Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
        ))
        .await??;
    let binding = boot(&log, 1, 7)?;
    log.update_lifecycle(LifecycleUpdate::Boot {
        boot: binding.clone(),
        deadline: deadline(&log)?,
    })
    .await?;
    let release = gate.arm(fault::FAIL_SYNC);
    assert!(matches!(
        log.update_lifecycle(LifecycleUpdate::Stop {
            selector: stop(&binding, 2),
            deadline: deadline(&log)?
        })
        .await,
        Err(LogError::Io(_))
    ));
    drop(release);
    assert!(log.close().await.is_err());
    assert!(log.lifecycle_guard()?.unwrap().accepted().is_none());
    // A failed sync is ambiguous on disk; this test does not assert that the
    // mutation failed to commit. It asserts only the absence of cached proof.
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fleet_lifecycle_missing_record_refuses_resume() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut options = options(root.path());
    let log = domain
        .run(ReplicaLog::open(
            options.clone(),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let performance = log.performance();
    log.close().await?;
    let dir = options.directory.join("log").to_string_lossy().into_owned();
    domain
        .blocking(move || -> anyhow::Result<()> {
            let engine = Engine::open_with_file_system(
                raft_engine::Config {
                    dir,
                    ..Default::default()
                },
                Arc::new(EngineFileSystem(
                    Arc::new(cellar_ltx::DirectFileSystem),
                    performance,
                )),
            )?;
            let mut batch = LogBatch::default();
            batch.delete(0, LIFECYCLE_KEY.to_vec());
            engine.write(&mut batch, true)?;
            Ok(())
        })?
        .await??;
    options.mode = LogOpenMode::Resume;
    assert!(matches!(
        domain
            .run(ReplicaLog::open(
                options,
                Arc::new(cellar_ltx::DirectFileSystem)
            ))
            .await?,
        Err(LogError::LostStableState)
    ));
    domain.shutdown().await;
    Ok(())
}
