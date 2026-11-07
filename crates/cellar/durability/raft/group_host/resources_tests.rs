// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real public closure must join collection past its physical deletion barrier.
#![allow(clippy::disallowed_methods)]
use super::*;
use crate::raft::{
    catalog::Catalog,
    group_host::{collection::Collection, startup_tests::StartupNode},
    storage_fault as fault,
    version::{VersionKeys, VersionSigningKey},
};
use cellar_logic::{
    consensus::{ClusterId, NamespaceId},
    NamespaceMode,
};
use cellar_ltx::internal::ExtentId;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn collection_close_joins_held_deletion_and_retains_admission() -> anyhow::Result<()> {
    let mut node = StartupNode::open(1).await?;
    let class = Arc::new(RaftClass::new("CollectionOnly", 1, [65; 32])?);
    let catalog = Catalog::new(
        Arc::new(object_store::memory::InMemory::new()),
        Default::default(),
    )?;
    let namespace = node
        .domain
        .run(catalog.open_namespace(NamespaceConfig::new(
            ClusterId::new([1; 16])?,
            NamespaceId::new([2; 16])?,
            NamespaceMode::Raft,
            class.schema_digest(),
        )))
        .await??;
    let prepared = Prepared {
        namespaces: vec![Namespace {
            directory: RangeDirectory::new(namespace, Default::default())?,
            class,
        }],
        peers: vec![node.peer()],
        groups: vec![],
        query_origins: vec![],
        command_origins: vec![],
    };
    let input = node.service();
    let public = node
        .domain
        .run(start(
            input,
            prepared,
            node.root.path().join("cells"),
            test_support::host_options(),
            test_support::limits(),
            VersionKeys::new(
                VersionSigningKey::new([31; 32], u64::MAX)?,
                vec![],
                Duration::from_secs(86400),
            )?,
            None,
        ))
        .await??;

    let gate = fault::Gate::new(None);
    let directory = node.root.path().join("collection-inventory");
    let files = Arc::new(fault::FaultFileSystem { gate: gate.clone() });
    let path = directory.clone();
    let store = node
        .domain
        .blocking(move || -> anyhow::Result<_> {
            let store = NodeExtentStore::open_serialized(path, files, Default::default())?;
            let bytes = vec![7; 4096];
            store.put_extent_bytes(ExtentId::of(&bytes)?, &bytes)?;
            Ok(store)
        })?
        .await??;
    let images = Images {
        directory: directory.clone(),
        materialized: None,
        extents: Some(store.clone()),
        cache: None,
        accounting: None,
    };
    // The engine has no groups: its complete recovered reference set is empty.
    let inventories = images
        .adopt_recovered(&node.log, &[], &[])
        .await?
        .into_iter()
        .collect();
    let collection = Collection::new(inventories)?.ok_or_else(|| anyhow::anyhow!("collection"))?;
    let observer = collection.observer();
    *gate.sync_path.lock().unwrap() = Some(directory);
    let entered = gate.entered.notified();
    tokio::pin!(entered);
    entered.as_mut().enable();
    let release = gate.arm(fault::HOLD_SELECTED_DIRECTORY_SYNC);
    collection.start(&public, 20)?;
    tokio::time::timeout(Duration::from_secs(5), entered).await?;
    let charged = store.usage()?;
    assert_eq!(
        charged.objects, 1,
        "deletion refunded before its physical call returned"
    );
    let budget = public.control_budget().clone();
    let pending = budget.usage();
    assert!(pending.0 > 4096 && pending.1 == 2);
    let mut closing = Box::pin(public.close());
    assert!(
        tokio::time::timeout(Duration::from_millis(30), &mut closing)
            .await
            .is_err()
    );
    drop(closing);
    assert_eq!(
        budget.usage(),
        pending,
        "cancelled close released admitted physical collection"
    );
    assert!(
        node.log.storage_instance().is_ok(),
        "storage closed before collector joined"
    );
    drop(release);
    tokio::time::timeout(Duration::from_secs(5), public.close()).await??;
    assert_eq!(budget.usage(), (0, 0));
    assert_eq!(store.usage()?.objects, 0);
    let finished = observer
        .capture()
        .ok_or_else(|| anyhow::anyhow!("collection observation"))?;
    assert!(!finished.running && finished.files_reclaimed == 1 && finished.failures == 0);
    assert!(matches!(
        node.log.storage_instance(),
        Err(crate::raft::storage::LogError::Closed)
    ));
    drop(images);
    drop(store);
    drop(public);
    node.close().await
}
