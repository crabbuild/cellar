// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Managed boot, cold capacity and retained-group qualification over embedded Nodes.
//! Deployment bounds use fixture attestations; the full lifecycle gate is separate.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{consensus::*, *};
use managed_fixture::{fault, request, start_node, Fixture, TIMEOUT};
use std::{
    path::Path,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

#[path = "support/raft_catalog_fault.rs"]
pub mod catalog_fault;

#[path = "support/raft_storage_fault.rs"]
pub mod storage_fault;

#[path = "support/cell_group_managed_fixture.rs"]
pub mod managed_fixture;

#[path = "support/cell_group_managed_bootstrap.rs"]
mod bootstrap;
#[path = "support/cell_group_managed_inspection_catchup.rs"]
mod inspection_catchup;
#[path = "support/cell_group_managed_registry.rs"]
mod registry;
#[path = "support/cell_group_rustfs.rs"]
#[allow(dead_code)]
mod rustfs;
#[path = "support/cell_group_managed_shutdown.rs"]
mod shutdown;

async fn stopped(node: Node, service: &RaftCells) -> Result<()> {
    ensure!(matches!(
        node.shutdown(ShutdownMode::Preserve).await?,
        NodeOutcome::Stopped(_)
    ));
    let report = service.close().await?;
    ensure!(
        report.groups_total == 0
            && report.mutation_outcomes == MutationOutcomes::NoUnresolvedLocalProducers
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_boot_retained_restart_and_exact_stop_owner() -> Result<()> {
    for rooted in [false, true] {
        let f = Fixture::new(rooted).await?;
        let root = tempfile::tempdir()?;
        let mut previous: Option<FleetBootBinding> = None;
        for generation in [24, 25] {
            let node = start_node(root.path(), f.application.clone(), &f.spec.label).await?;
            let (config, services) = f
                .config(
                    &node,
                    if previous.is_none() {
                        LogOpenMode::Create
                    } else {
                        LogOpenMode::Resume
                    },
                    generation,
                )
                .await?;
            let host = node.start_raft_cells(config, services).await?;
            let view = host.observe_node(TIMEOUT).await?;
            let boot = view.fleet_boot.context("synced boot")?;
            ensure!(view.node.groups == 0 && view.declared_namespaces == 1);
            ensure!(boot.process == [generation; 16] && boot.node == f.spec.node.bytes());
            if let Some(old) = &previous {
                ensure!(
                    boot.engine == old.engine
                        && boot.epoch > old.epoch
                        && boot.boot != old.boot
                        && boot.join_key != old.join_key
                );
            }
            let mut foreign = boot.clone();
            foreign.process = [99; 16];
            let request: FleetStopRequest = serde_json::from_value(
                serde_json::json!({"selector":{"purpose":"maintenance","boot":foreign,"epoch":boot.epoch+1,"operation":vec![41;16],"finalization":vec![42;16],"request":vec![43;16]},"action":"stop"}),
            )?;
            ensure!(
                matches!(
                    node.request_fleet_stop(request, TIMEOUT).await,
                    Err(FleetCatalogError::Conflict)
                ),
                "Node must install the exact managed stop reporter"
            );
            stopped(node, &host).await?;
            if previous.is_none() {
                let blank = tempfile::tempdir()?;
                let candidate =
                    start_node(blank.path(), f.application.clone(), &f.spec.label).await?;
                let (config, services) = f.config(&candidate, LogOpenMode::Create, 26).await?;
                let before = f.catalog.read(TIMEOUT).await?.revision();
                ensure!(
                    candidate.start_raft_cells(config, services).await.is_err(),
                    "recreated managed engine accepted the pinned disk identity"
                );
                ensure!(
                    f.catalog.read(TIMEOUT).await?.revision() == before,
                    "invalid engine consumed a rooted boot"
                );
                candidate.shutdown(ShutdownMode::Preserve).await?;
            }
            previous = Some(boot);
        }
        f.domain.shutdown().await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_declaration_mismatch_refuses_before_boot_claim() -> Result<()> {
    let f = Fixture::new(false).await?;
    let root = tempfile::tempdir()?;
    let node = start_node(root.path(), f.application.clone(), &f.spec.label).await?;
    let (mut config, services) = f.config(&node, LogOpenMode::Create, 24).await?;
    config.namespaces[0].namespace.schema_digest = [82; 32];
    let before = f.catalog.read(TIMEOUT).await?.revision();
    ensure!(node.start_raft_cells(config, services).await.is_err());
    ensure!(f.catalog.read(TIMEOUT).await?.revision() == before);
    node.shutdown(ShutdownMode::Preserve).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_directory_shutdown_joins_entered_root_refresh() -> Result<()> {
    let f = Fixture::new(false).await?;
    let root = tempfile::tempdir()?;
    let node = start_node(root.path(), f.application.clone(), &f.spec.label).await?;
    let (config, services) = f.config(&node, LogOpenMode::Create, 24).await?;
    let host = node.start_raft_cells(config, services).await?;
    // No application traffic drives discovery. A rooted node-table change must
    // be installed by the independently timed directory reader.
    let mut peer = f.spec.clone();
    peer.node = NodeId::new([5; 16])?;
    peer.disk = DiskLineage::new([15; 16])?;
    peer.physical_host = IncarnationId::new([91; 16])?;
    peer.label = "managed-other".into();
    let cut = f.catalog.read(TIMEOUT).await?;
    let cut = f
        .catalog
        .register_node(&cut, &request(3, cut.revision()), &peer, TIMEOUT)
        .await?
        .snapshot;
    tokio::time::timeout(TIMEOUT, async {
        while host
            .observe_node(TIMEOUT)
            .await?
            .fleet_trust_revision
            .is_none_or(|revision| revision < cut.revision())
        {
            tokio::task::yield_now().await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    f.store.fleet_read_hang.store(true, Ordering::Release);
    f.store.hold_fleet_reads.store(true, Ordering::Release);
    tokio::time::timeout(TIMEOUT, async {
        while f.store.active_fleet_reads.load(Ordering::Acquire) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let mut closing = Box::pin(node.shutdown(ShutdownMode::Preserve));
    ensure!(
        tokio::time::timeout(Duration::from_millis(30), &mut closing)
            .await
            .is_err(),
        "shutdown dropped entered directory work"
    );
    ensure!(f.store.active_fleet_reads.load(Ordering::Acquire) > 0);
    f.store.hold_fleet_reads.store(false, Ordering::Release);
    f.store.fleet_read_hang.store(false, Ordering::Release);
    f.store.fleet_read_resume.notify_waiters();
    tokio::time::timeout(TIMEOUT, closing).await??;
    ensure!(f.store.active_fleet_reads.load(Ordering::Acquire) == 0);
    ensure!(host.close().await?.groups_total == 0);
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_directory_preserves_explicit_advertised_endpoints() -> Result<()> {
    let f = Fixture::new(false).await?;
    let root = tempfile::tempdir()?;
    let node = start_node(root.path(), f.application.clone(), &f.spec.label).await?;
    let (mut config, mut services) = f.config(&node, LogOpenMode::Create, 24).await?;
    let listeners = NodeRaftListeners {
        control: tokio::net::TcpListener::bind("0.0.0.0:0").await?,
        data: tokio::net::TcpListener::bind("0.0.0.0:0").await?,
        query: tokio::net::TcpListener::bind("0.0.0.0:0").await?,
    };
    let advertised = |bound: std::net::SocketAddr| {
        std::net::SocketAddr::new(std::net::Ipv4Addr::LOCALHOST.into(), bound.port())
    };
    let peer = &mut config.peers[0];
    peer.control_address = advertised(listeners.control.local_addr()?);
    peer.data_address = advertised(listeners.data.local_addr()?);
    peer.query_address = advertised(listeners.query.local_addr()?);
    ensure!(peer.control_address != listeners.control.local_addr()?);
    services.listeners = listeners;
    let host = node.start_raft_cells(config, services).await?;
    ensure!(host.observe_node(TIMEOUT).await?.fleet_boot.is_some());
    stopped(node, &host).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_owner_mismatch_refuses_before_boot_claim() -> Result<()> {
    for wrong_node in [true, false] {
        let f = Fixture::new(false).await?;
        let root = tempfile::tempdir()?;
        let node = start_node(root.path(), f.application.clone(), &f.spec.label).await?;
        let (mut config, services) = f.config(&node, LogOpenMode::Create, 24).await?;
        if wrong_node {
            let other = NodeId::new([5; 16])?;
            let disk = DiskLineage::new([15; 16])?;
            config.infrastructure.log.node = other;
            config.infrastructure.log.disk = disk;
            config.peers[0].node = other;
            config.peers[0].disk = disk;
        } else {
            config.peers[0].server_name = "other.example".into();
        }
        let before = f.catalog.read(TIMEOUT).await?.revision();
        ensure!(node.start_raft_cells(config, services).await.is_err());
        ensure!(
            f.catalog.read(TIMEOUT).await?.revision() == before,
            "invalid managed owner consumed a rooted boot (wrong_node={wrong_node})"
        );
        node.shutdown(ShutdownMode::Preserve).await?;
        f.domain.shutdown().await;
    }
    Ok(())
}
