// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn physical_catalog_rejects_cell_aliases_across_restart() -> anyhow::Result<()> {
    let root = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let mut options = options(root.path());
    let initial = bootstrap(5);
    let mut alias = initial.clone();
    alias.scope.cell = "Counter:alias".into();
    let log = domain
        .run(ReplicaLog::open(
            options.clone(),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let key = log.create_group(initial.clone()).await?.numeric_key();
    for pass in 0..2 {
        let log = if pass == 0 {
            log.clone()
        } else {
            options.mode = LogOpenMode::Resume;
            options.process = ProcessGeneration::new([9; 16])?;
            domain
                .run(ReplicaLog::open(
                    options.clone(),
                    Arc::new(cellar_ltx::DirectFileSystem),
                ))
                .await??
        };
        assert_eq!(log.group(&initial.scope)?.numeric_key(), key);
        assert_eq!(log.create_group(initial.clone()).await?.numeric_key(), key);
        assert!(matches!(
            log.group(&alias.scope),
            Err(LogError::WrongIdentity)
        ));
        assert!(matches!(
            log.create_group(alias.clone()).await,
            Err(LogError::WrongIdentity)
        ));
        assert_eq!(log.group_count_with_pending(Some(&initial.scope))?, 1);
        assert!(matches!(
            log.group_count_with_pending(Some(&alias.scope)),
            Err(LogError::WrongIdentity)
        ));
        assert!(matches!(
            log.physical_inventory(Some(&alias.scope)),
            Err(LogError::WrongIdentity)
        ));
        assert!(matches!(
            log.cell_group_inventory(None),
            Err(LogError::WrongIdentity)
        ));
        let inventory = log.physical_inventory(Some(&initial.scope))?;
        assert_eq!(inventory.histories().len(), 1);
        assert_eq!(
            inventory.history(&initial.scope)?.unwrap().birth_digest(),
            initial.birth_digest
        );
        assert_eq!(log.group_count_with_pending(Some(&bootstrap(7).scope))?, 2);
        assert_eq!(log.groups()?.len(), 1);
        log.close().await?;
    }
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn physical_mapping_domain_refuses_old_and_corrupt_engine_records() -> anyhow::Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    for corruption in [
        "old-scope",
        "old-adapter",
        "unknown-domain",
        "zero-history",
        "empty-cell",
    ] {
        let root = tempfile::tempdir()?;
        let mut options = options(root.path());
        let initial = bootstrap(5);
        let log = domain
            .run(ReplicaLog::open(
                options.clone(),
                Arc::new(cellar_ltx::DirectFileSystem),
            ))
            .await??;
        let key = log.create_group(initial.clone()).await?.numeric_key();
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
                let mut record: serde_json::Value =
                    serde_json::from_slice(&engine.get(key, GROUP_KEY).unwrap())?;
                let mapping = &mut record["mapping"];
                assert_eq!(mapping["format"], 1);
                assert_eq!(mapping["domain"], "cellar-group-mapping-1");
                assert_eq!(mapping["application"]["domain"], "single-cell-1");
                assert_eq!(mapping["application"]["state"], initial.scope.cell);
                assert_eq!(mapping["history"].as_object().unwrap().len(), 4);
                assert!(mapping["history"].get("cell").is_none());
                let mut catalog_key = b"g:".to_vec();
                catalog_key.extend_from_slice(&initial.scope.group.bytes());
                catalog_key.extend_from_slice(&initial.scope.incarnation.bytes());
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(
                        &engine.get(0, &catalog_key).unwrap()
                    )?,
                    *mapping
                );
                match corruption {
                    "old-scope" => {
                        let fields = mapping.as_object_mut().unwrap();
                        fields.remove("domain");
                        fields.remove("history");
                        fields.remove("application");
                        fields.insert(
                            "scope".into(),
                            serde_json::to_value(
                                crate::application::consensus::protocol::WireScope::from(
                                    &initial.scope,
                                ),
                            )?,
                        );
                    }
                    "old-adapter" => {
                        mapping.as_object_mut().unwrap().remove("application");
                        mapping["application_cell"] = serde_json::json!(initial.scope.cell);
                    }
                    "unknown-domain" => {
                        mapping["domain"] = serde_json::json!("cellar-image-mapping-1")
                    }
                    "zero-history" => {
                        mapping["history"]["group"] = serde_json::json!(vec![0u8; 16])
                    }
                    "empty-cell" => mapping["application"]["state"] = serde_json::json!(""),
                    _ => unreachable!(),
                }
                let mut batch = LogBatch::default();
                batch.put(0, catalog_key, serde_json::to_vec(mapping)?)?;
                batch.put(key, GROUP_KEY.to_vec(), serde_json::to_vec(&record)?)?;
                engine.write(&mut batch, true)?;
                Ok(())
            })?
            .await??;
        options.mode = LogOpenMode::Resume;
        assert!(
            matches!(
                domain
                    .run(ReplicaLog::open(
                        options,
                        Arc::new(cellar_ltx::DirectFileSystem)
                    ))
                    .await?,
                Err(LogError::Corrupt(_))
            ),
            "accepted {corruption}"
        );
    }
    domain.shutdown().await;
    Ok(())
}
