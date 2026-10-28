// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use object_store::list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore};

#[path = "raft_fleet_capacity_tests.rs"]
mod capacity;

struct NoListing;

#[async_trait::async_trait]
impl PaginatedListStore for NoListing {
    async fn list_paginated(
        &self,
        _: Option<&str>,
        _: PaginatedListOptions,
    ) -> object_store::Result<PaginatedListResult> {
        Err(object_store::Error::NotSupported {
            source: "managed bootstrap never lists".into(),
        })
    }
}

fn config() -> Config {
    Config {
        incarnation: [80; 16],
        physical_host: [90; 16],
        owner: "managed-lab-test".into(),
        nodes: 5,
        group_capacity: 8,
        image_bytes: 4 * 1024 * 1024,
        extent_bytes: 1024 * 1024 * 1024,
        replay_extent_bytes: 256 * 1024 * 1024,
        durable_bytes: 4 * 1024 * 1024 * 1024,
        resources: FleetDockerResourcePolicy {
            durable_overhead_bytes: 2 * 1024 * 1024 * 1024,
            resident: FleetResourceBounds {
                engine_slots: 1,
                durable_bytes: 32 * 1024 * 1024,
                memory_bytes: 8 * 1024 * 1024,
            },
            peak: FleetResourceBounds {
                engine_slots: 1,
                durable_bytes: 64 * 1024 * 1024,
                memory_bytes: 16 * 1024 * 1024,
            },
            reserve: FleetResourceBounds::default(),
        },
    }
}

fn bucket() -> Result<Bucket> {
    let store = Arc::new(object_store::memory::InMemory::new());
    Bucket::from_clients(
        store.clone(),
        store.clone(),
        Arc::new(NoListing),
        StorageBackend::S3,
        "managed-lab".into(),
        String::new(),
    )
}

fn namespace() -> Result<NamespaceConfig> {
    Ok(NamespaceConfig::new(
        ClusterId::new([1; 16])?,
        NamespaceId::new([2; 16])?,
        NamespaceMode::Raft,
        [3; 32],
    ))
}

#[tokio::test]
async fn managed_bootstrap_reopens_exact_requests_without_activating_capacity() -> Result<()> {
    let bucket = bucket()?;
    let first = Managed::open(bucket.clone(), config(), namespace()?, "Counter", true).await?;
    let before = first.bootstrap().await?;
    ensure!(before["revision"] == 7);
    first.domain.shutdown().await;
    let second = Managed::open(bucket, config(), namespace()?, "Counter", true).await?;
    ensure!(second.bootstrap().await? == before);
    let cut = second.catalog.read(TIMEOUT).await?;
    for index in 1..=5 {
        let auth = second
            .catalog
            .authorize_host(&cut, NodeId::new([index; 16])?, TIMEOUT)
            .await?;
        ensure!(
            auth.state() == FleetNodeState::Registered
                && !auth.eligibility().receive_replicas
                && auth.physical_host().bytes() == config().physical_host
        );
    }
    ensure!(second.namespace.config().image_storage == RaftImageStorage::Extents);
    ensure!(
        second
            .catalog
            .check_inventory(&cut, std::slice::from_ref(&second.namespace), TIMEOUT)
            .await?
            .histories()
            == 0
    );
    second.domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn managed_bootstrap_refuses_missing_fleet_and_changed_physical_identity() -> Result<()> {
    let bucket = bucket()?;
    ensure!(
        Managed::open(bucket.clone(), config(), namespace()?, "Counter", false)
            .await
            .is_err()
    );
    let first = Managed::open(bucket.clone(), config(), namespace()?, "Counter", true).await?;
    first.bootstrap().await?;
    first.domain.shutdown().await;
    let mut changed = config();
    changed.physical_host = [91; 16];
    let second = Managed::open(bucket, changed, namespace()?, "Counter", true).await?;
    ensure!(second.bootstrap().await.is_err());
    ensure!(second.catalog.read(TIMEOUT).await?.revision() == 7);
    second.domain.shutdown().await;
    Ok(())
}

#[test]
fn managed_config_refuses_unbacked_image_reservations_and_unknown_commands() -> Result<()> {
    let mut input = config();
    input.validate()?;
    input.durable_bytes = input.image_bytes * 8;
    ensure!(input.validate().is_err());
    input = config();
    input.resources.resident.durable_bytes = 1;
    ensure!(input.validate().is_err());
    input = config();
    input.resources.durable_overhead_bytes = input.replay_extent_bytes;
    ensure!(input.validate().is_err());
    ensure!(serde_json::from_value::<Config>(json!({"unexpected":true})).is_err());
    ensure!(serde_json::from_value::<Command>(json!({"action":"RestartContainer"})).is_err());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn managed_configuration_opens_the_native_runtime_with_its_installed_bounds() -> Result<()> {
    use cellar::sdk::consensus::verification::{RaftRuntime, ReplicaLog};
    let m = Managed::open(bucket()?, config(), namespace()?, "Counter", true).await?;
    m.bootstrap().await?;
    let root = tempfile::tempdir()?;
    let node = NodeId::new([1; 16])?;
    let disk = DiskLineage::new([11; 16])?;
    let process = ProcessGeneration::new([21; 16])?;
    let mut options = RaftCellsConfig {
        query_origins: vec![],
        cells: vec![],
        host: RaftHostOptions::default(),
        catalog: Managed::catalog_options(),
        peers: vec![RaftPeerNode {
            node,
            disk,
            process,
            label: "raft-node1".into(),
            server_name: "localhost".into(),
            control_address: "127.0.0.1:9100".parse()?,
            data_address: "127.0.0.1:9101".parse()?,
            query_address: "127.0.0.1:9102".parse()?,
        }],
        infrastructure: NodeRaftConfig {
            log: ReplicaLogOptions::new(
                root.path().join("log"),
                node,
                disk,
                process,
                LogOpenMode::Create,
            ),
            consensus: RaftRuntimeOptions::default(),
            transport: TransportOptions::default(),
        },
    };
    m.configure(&mut options, node).await?;
    let log = m
        .domain
        .run(ReplicaLog::open(
            options.infrastructure.log,
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    let opened = m
        .domain
        .run(RaftRuntime::start_parked(
            log.clone(),
            options.infrastructure.consensus,
        ))
        .await?;
    if let Ok((runtime, _events)) = &opened {
        runtime.close().await?;
    }
    log.close().await?;
    m.domain.shutdown().await;
    opened?;
    Ok(())
}
