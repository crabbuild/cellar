// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Durable engine identity and node-root revision fences, independently of
//! group votes/configurations. These observations grant no discovery authority.
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_close_waiter_does_not_abandon_storage_shutdown() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("node");
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = Gate::new(None);
    let log = start(
        &domain,
        options(&path, LogOpenMode::Create, 1),
        Arc::new(FaultFileSystem { gate: gate.clone() }),
    )
    .await?;
    let instance = log.storage_instance()?;
    let release = gate.arm(HOLD_SYNC);
    let owner = log.clone();
    let pending = tokio::spawn(async move { owner.reserve_node_revision().await });
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
    let owner = log.clone();
    let mut close = tokio::spawn(async move { owner.close().await });
    ensure!(tokio::time::timeout(Duration::from_millis(100), &mut close)
        .await
        .is_err());
    ensure!(
        log.storage_instance().is_err(),
        "close did not close admission"
    );
    close.abort();
    ensure!(close.await.is_err());
    drop(release);
    pending.await??;
    log.close().await?;
    let reopened = start(
        &domain,
        options(&path, LogOpenMode::Resume, 2),
        Arc::new(DirectFileSystem),
    )
    .await?;
    assert_eq!(reopened.storage_instance()?, instance);
    reopened.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_instance_survives_restart_and_marker_substitution_is_refused() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("node");
    let other = root.path().join("other");
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = start(
        &domain,
        options(&path, LogOpenMode::Create, 1),
        Arc::new(DirectFileSystem),
    )
    .await?;
    let instance = log.storage_instance()?;
    assert_ne!(instance.bytes(), [0; 16]);
    let group = log.create_group(bootstrap(10)).await?;
    let mut write = GroupWrite::new(&group);
    write.hard_state = Some(hard(&group, 7, 2, 1)?);
    log.persist(OperationId::new(1), vec![write]).await?;
    log.close().await?;
    let log = start(
        &domain,
        options(&path, LogOpenMode::Resume, 2),
        Arc::new(DirectFileSystem),
    )
    .await?;
    assert_eq!(log.storage_instance()?, instance);
    assert_eq!(
        log.group(&bootstrap(10).scope)?
            .initial_state()?
            .hard_state
            .vote,
        2
    );
    log.close().await?;
    let fresh = start(
        &domain,
        options(&other, LogOpenMode::Create, 3),
        Arc::new(DirectFileSystem),
    )
    .await?;
    assert_ne!(
        fresh.storage_instance()?,
        instance,
        "repeated configured node/disk values reused an engine identity"
    );
    fresh.close().await?;
    let original = std::fs::read(path.join("node.json"))?;
    std::fs::copy(other.join("node.json"), path.join("node.json"))?;
    ensure!(matches!(
        start(
            &domain,
            options(&path, LogOpenMode::Resume, 4),
            Arc::new(DirectFileSystem)
        )
        .await,
        Err(ref error) if matches!(error.downcast_ref::<LogError>(), Some(LogError::WrongIdentity))
    ));
    std::fs::write(path.join("node.json"), original)?;
    let log = start(
        &domain,
        options(&path, LogOpenMode::Resume, 5),
        Arc::new(DirectFileSystem),
    )
    .await?;
    assert_eq!(log.storage_instance()?, instance);
    assert_eq!(
        log.group(&bootstrap(10).scope)?
            .initial_state()?
            .hard_state
            .vote,
        2
    );
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_instance_marker_requires_a_nonzero_current_field() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("node");
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = start(
        &domain,
        options(&path, LogOpenMode::Create, 1),
        Arc::new(DirectFileSystem),
    )
    .await?;
    log.close().await?;
    let original = std::fs::read(path.join("node.json"))?;
    let value: serde_json::Value = serde_json::from_slice(&original)?;
    assert_eq!(value["format"], 1);
    let mut missing = value.clone();
    missing.as_object_mut().unwrap().remove("storage_instance");
    let mut zero = value;
    zero["storage_instance"] = serde_json::to_value([0_u8; 16])?;
    for (i, marker) in [missing, zero].into_iter().enumerate() {
        std::fs::write(path.join("node.json"), serde_json::to_vec(&marker)?)?;
        let reopened = start(
            &domain,
            options(&path, LogOpenMode::Resume, i as u8 + 2),
            Arc::new(DirectFileSystem),
        )
        .await;
        ensure!(matches!(
            reopened,
            Err(ref error) if matches!(error.downcast_ref::<LogError>(), Some(LogError::Corrupt(_) | LogError::WrongIdentity))
        ));
    }
    std::fs::write(path.join("node.json"), original)?;
    let log = start(
        &domain,
        options(&path, LogOpenMode::Resume, 4),
        Arc::new(DirectFileSystem),
    )
    .await?;
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_revision_preserves_group_state_and_restart_order() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("node");
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let log = start(
        &domain,
        options(&path, LogOpenMode::Create, 1),
        Arc::new(DirectFileSystem),
    )
    .await?;
    let instance = log.storage_instance()?;
    let group = log.create_group(bootstrap(10)).await?;
    let mut write = GroupWrite::new(&group);
    write.hard_state = Some(hard(&group, 7, 2, 1)?);
    let previous = log
        .persist(OperationId::new(1), vec![write])
        .await?
        .sequence;
    let applied = group.applied()?;
    let first = log.reserve_node_revision().await?;
    let second = log.reserve_node_revision().await?;
    ensure!(first > previous && second > first);
    assert_eq!(log.groups()?.len(), 1);
    assert_eq!(group.initial_state()?.hard_state, hard(&group, 7, 2, 1)?);
    assert_eq!(group.applied()?, applied);
    log.close().await?;
    ensure!(matches!(log.storage_instance(), Err(LogError::Closed)));
    ensure!(matches!(
        log.reserve_node_revision().await,
        Err(LogError::Closed)
    ));
    let log = start(
        &domain,
        options(&path, LogOpenMode::Resume, 2),
        Arc::new(DirectFileSystem),
    )
    .await?;
    assert_eq!(log.storage_instance()?, instance);
    ensure!(log.reserve_node_revision().await? > second);
    let group = log.group(&bootstrap(10).scope)?;
    assert_eq!(group.initial_state()?.hard_state.vote, 2);
    assert_eq!(group.applied()?, applied);
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_revision_waits_for_its_actual_sync() -> Result<()> {
    let root = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = Gate::new(None);
    let log = start(
        &domain,
        options(&root.path().join("node"), LogOpenMode::Create, 1),
        Arc::new(FaultFileSystem { gate: gate.clone() }),
    )
    .await?;
    let before = log.reserve_node_revision().await?;
    let release = gate.arm(HOLD_SYNC);
    let owner = log.clone();
    let future = tokio::spawn(async move { owner.reserve_node_revision().await });
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
    ensure!(
        !future.is_finished(),
        "directory revision escaped before its root sync"
    );
    drop(release);
    ensure!(future.await?? > before);
    assert!(log.groups()?.is_empty());
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_node_revision_sync_closes_proof_admission() -> Result<()> {
    let root = tempfile::tempdir()?;
    let path = root.path().join("node");
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = Gate::new(None);
    let log = start(
        &domain,
        options(&path, LogOpenMode::Create, 1),
        Arc::new(FaultFileSystem { gate: gate.clone() }),
    )
    .await?;
    let instance = log.storage_instance()?;
    let previous = log.reserve_node_revision().await?;
    let release = gate.arm(FAIL_SYNC_HOLD_LOCK_DROP);
    ensure!(log.reserve_node_revision().await.is_err());
    ensure!(log.storage_instance().is_err());
    ensure!(log.reserve_node_revision().await.is_err());
    tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
    let owner = log.clone();
    let mut close = tokio::spawn(async move { owner.close().await });
    ensure!(
        tokio::time::timeout(Duration::from_millis(100), &mut close)
            .await
            .is_err(),
        "failed close returned before its actual storage lock was released"
    );
    drop(release);
    ensure!(close.await?.is_err());
    let log = start(
        &domain,
        options(&path, LogOpenMode::Resume, 2),
        Arc::new(DirectFileSystem),
    )
    .await?;
    assert_eq!(log.storage_instance()?, instance);
    ensure!(log.reserve_node_revision().await? > previous);
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Saved {
    instance: [u8; 16],
    before: u64,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_revision_process_kill_matrix() -> Result<()> {
    for cut in [
        "node-fence-before-sync",
        "node-fence-after-sync",
        "node-fence-after-reply",
    ] {
        let root = tempfile::tempdir()?;
        let path = root.path().join("node");
        seed(&path).await?;
        kill_at_cut(&path, cut).await?;
        let saved: Saved =
            serde_json::from_slice(&std::fs::read(path.join("node-fence-baseline.json"))?)?;
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let log = start(
            &domain,
            options(&path, LogOpenMode::Resume, 3),
            Arc::new(DirectFileSystem),
        )
        .await?;
        assert_eq!(log.storage_instance()?.bytes(), saved.instance);
        let next = log.reserve_node_revision().await?;
        ensure!(next > saved.before);
        if cut != "node-fence-before-sync" {
            ensure!(next > saved.before + 1);
        }
        let group = log.group(&bootstrap(10).scope)?;
        assert_eq!(group.initial_state()?.hard_state.term, 7);
        assert_eq!(group.initial_state()?.hard_state.vote, 2);
        assert_eq!(group.initial_state()?.hard_state.commit, 1);
        log.close().await?;
        domain.shutdown().await;
    }
    Ok(())
}

pub(super) async fn child_node_fence(
    log: &ReplicaLog,
    gate: &Arc<Gate>,
    path: &Path,
    signal: &Path,
    cut: &str,
) -> Result<()> {
    let saved = Saved {
        instance: log.storage_instance()?.bytes(),
        before: log.reserve_node_revision().await?,
    };
    let baseline = path.join("node-fence-baseline.json");
    let mut file = std::fs::File::create(baseline)?;
    std::io::Write::write_all(&mut file, &serde_json::to_vec(&saved)?)?;
    file.sync_all()?;
    let mode = match cut {
        "node-fence-before-sync" => CRASH_BEFORE_SYNC,
        "node-fence-after-sync" => CRASH_AFTER_SYNC,
        "node-fence-after-reply" => OFF,
        _ => bail!("unknown node fence cut"),
    };
    let _release = gate.arm(mode);
    let allocated = log.reserve_node_revision().await?;
    assert_eq!(allocated, saved.before + 1);
    let mut file = std::fs::File::create(signal)?;
    std::io::Write::write_all(&mut file, b"node fence reply")?;
    file.sync_all()?;
    std::future::pending::<()>().await;
    Ok(())
}
