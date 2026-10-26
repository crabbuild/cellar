use super::*;
use crate::asyncrt::ExecutionDomain;
use crate::raft::member_state::FleetGroupBinding;
use cellar_logic::consensus::{
    ClusterId, DiskLineage, GroupId, IncarnationId, NamespaceId, NodeId, ProcessGeneration,
};

use crate::raft::storage_fault as fault;

fn bootstrap(id: u8) -> GroupBootstrap {
    GroupBootstrap {
        scope: GroupScope {
            cluster: ClusterId::new([1; 16]).unwrap(),
            namespace: NamespaceId::new([2; 16]).unwrap(),
            cell: format!("Counter:{id}"),
            group: GroupId::new([id; 16]).unwrap(),
            incarnation: IncarnationId::new([4; 16]).unwrap(),
        },
        member: MemberId::new(1).unwrap(),
        birth_digest: [9; 32],
        voters: (1..=3).map(|n| MemberId::new(n).unwrap()).collect(),
        learners: vec![],
    }
}

fn options(root: &std::path::Path) -> ReplicaLogOptions {
    ReplicaLogOptions::new(
        root.join("log"),
        NodeId::new([1; 16]).unwrap(),
        DiskLineage::new([2; 16]).unwrap(),
        ProcessGeneration::new([3; 16]).unwrap(),
        LogOpenMode::Create,
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_genesis_reopen_never_retrofits_bare_or_rebound_slots() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut options = options(root.path());
    let log = domain
        .run(ReplicaLog::open(
            options.clone(),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let initial = bootstrap(5);
    let binding = FleetGroupBinding::new(IncarnationId::new([6; 16])?, &initial.scope, [90; 32])?;
    let guarded = log
        .create_managed_group(initial.clone(), binding.clone())
        .await?;
    let before = guarded.membership()?;
    assert_eq!(before.fleet_status().unwrap().changed_at.get(), 1);
    let bare = bootstrap(7);
    log.create_group(bare.clone()).await?;
    let bare_binding = FleetGroupBinding::new(IncarnationId::new([6; 16])?, &bare.scope, [90; 32])?;
    assert!(matches!(
        log.create_managed_group(bare.clone(), bare_binding).await,
        Err(LogError::WrongIdentity)
    ));
    assert!(log
        .group(&bare.scope)?
        .membership()?
        .fleet_status()
        .is_none());
    let wrong = FleetGroupBinding::new(IncarnationId::new([8; 16])?, &initial.scope, [90; 32])?;
    assert!(matches!(
        log.create_managed_group(initial.clone(), wrong).await,
        Err(LogError::WrongIdentity)
    ));
    let other_catalog =
        FleetGroupBinding::new(IncarnationId::new([6; 16])?, &initial.scope, [91; 32])?;
    assert!(matches!(
        log.create_managed_group(initial.clone(), other_catalog)
            .await,
        Err(LogError::WrongIdentity)
    ));
    let instance = log.storage_instance()?;
    log.close().await?;
    options.mode = LogOpenMode::Resume;
    options.process = ProcessGeneration::new([9; 16])?;
    let log = domain
        .run(ReplicaLog::open(
            options,
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    assert_eq!(log.storage_instance()?, instance);
    let reopened = log.create_managed_group(initial.clone(), binding).await?;
    assert_eq!(reopened.membership()?, before);
    assert_eq!(log.create_group(initial).await?.membership()?, before);
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_genesis_publication_waits_for_actual_engine_sync() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = fault::Gate::new(None);
    let log = domain
        .run(ReplicaLog::open(
            options(root.path()),
            Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
        ))
        .await??;
    let initial = bootstrap(5);
    let binding = FleetGroupBinding::new(IncarnationId::new([6; 16])?, &initial.scope, [90; 32])?;
    let release = gate.arm(fault::HOLD_SYNC);
    let owned = log.clone();
    let proposal = initial.clone();
    let create =
        domain.spawn(async move { owned.create_managed_group(proposal, binding).await })?;
    let held = domain
        .run(crate::asyncrt::timeout(
            Duration::from_secs(5),
            gate.entered.notified(),
        ))
        .await?;
    let parked = matches!(log.group(&initial.scope), Err(LogError::NotFound));
    let reserved = log.group_count_with_pending(Some(&initial.scope));
    drop(release);
    let group = create.await??;
    held?;
    assert!(parked, "unsynced genesis entered the visible group cache");
    assert_eq!(reserved?, 1);
    assert_eq!(log.group_count_with_pending(Some(&initial.scope))?, 1);
    let status = group.membership()?.fleet_status().unwrap();
    assert_eq!(status.changed_at.get(), 1);
    assert_eq!(status.consumed_sequence, 0);
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}
