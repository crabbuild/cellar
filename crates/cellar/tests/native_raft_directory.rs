// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real engine fences and conditional object-store registration. These adapter
//! cases do not qualify controller RPCs or automatic membership repair.
#![allow(clippy::disallowed_methods)]
#[path = "support/raft_storage_fault.rs"]
mod fault;
#[path = "support/raft_directory_native.rs"]
mod native;
#[path = "support/raft_node.rs"]
#[allow(dead_code)]
mod public_node;
#[path = "support/raft_catalog_fault.rs"]
mod store;

use anyhow::{ensure, Result};
use cellar::application::consensus::verification::*;
use cellar::sdk::consensus::RaftPeerNode;
use cellar::sdk::ExecutionDomain;
use cellar_logic::{consensus::*, NamespaceMode};
use cellar_ltx::{DirectFileSystem, FileSystem};
use futures_util::TryStreamExt;
use object_store::{path::Path as ObjectPath, ObjectStore};
use std::{
    path::Path,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

const TIMEOUT: Duration = Duration::from_secs(5);

fn config(namespace: u8) -> NamespaceConfig {
    NamespaceConfig::new(
        ClusterId::new([1; 16]).unwrap(),
        NamespaceId::new([namespace; 16]).unwrap(),
        NamespaceMode::Raft,
        [21; 32],
    )
}

fn options(path: &Path, mode: LogOpenMode, process: u8) -> ReplicaLogOptions {
    ReplicaLogOptions::new(
        path.into(),
        NodeId::new([1; 16]).unwrap(),
        DiskLineage::new([101; 16]).unwrap(),
        ProcessGeneration::new([process; 16]).unwrap(),
        mode,
    )
}

fn peer(process: u8) -> RaftPeerNode {
    RaftPeerNode {
        node: NodeId::new([1; 16]).unwrap(),
        disk: DiskLineage::new([101; 16]).unwrap(),
        process: ProcessGeneration::new([process; 16]).unwrap(),
        label: "directory-1".into(),
        server_name: "localhost".into(),
        control_address: format!("127.0.0.1:{}", 20000 + u16::from(process) * 3)
            .parse()
            .unwrap(),
        data_address: format!("127.0.0.1:{}", 20001 + u16::from(process) * 3)
            .parse()
            .unwrap(),
        query_address: format!("127.0.0.1:{}", 20002 + u16::from(process) * 3)
            .parse()
            .unwrap(),
    }
}

fn key(leaf: &str) -> ObjectPath {
    ObjectPath::from(format!(
        "node-directory/test/raft/{}/nodes/{}/{}",
        hex::encode([1; 16]),
        hex::encode([1; 16]),
        leaf
    ))
}

async fn open(
    domain: &ExecutionDomain,
    options: ReplicaLogOptions,
    fs: Arc<dyn FileSystem>,
) -> Result<ReplicaLog> {
    Ok(domain.run(ReplicaLog::open(options, fs)).await??)
}

struct Fixture {
    root: tempfile::TempDir,
    domain: ExecutionDomain,
    store: Arc<store::Store>,
    catalog: Catalog,
    namespace: NamespaceCatalog,
    log: ReplicaLog,
}

impl Fixture {
    async fn new() -> Result<Self> {
        let root = tempfile::tempdir()?;
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let store = Arc::new(store::Store::default());
        let catalog = Catalog::new(
            store.clone(),
            CatalogOptions {
                prefix: "node-directory/test".into(),
                ..Default::default()
            },
        )?;
        let namespace = catalog.open_namespace(config(2)).await?;
        let log = open(
            &domain,
            options(&root.path().join("node"), LogOpenMode::Create, 1),
            Arc::new(DirectFileSystem),
        )
        .await?;
        Ok(Self {
            root,
            domain,
            store,
            catalog,
            namespace,
            log,
        })
    }

    async fn registration(&self) -> Result<NodeRegistration> {
        Ok(self
            .namespace
            .register_node_storage(&self.log, TIMEOUT)
            .await?)
    }

    async fn restart(&mut self, process: u8) -> Result<()> {
        self.log.close().await?;
        self.log = open(
            &self.domain,
            options(&self.root.path().join("node"), LogOpenMode::Resume, process),
            Arc::new(DirectFileSystem),
        )
        .await?;
        Ok(())
    }

    async fn close(self) -> Result<()> {
        self.log.close().await?;
        self.domain.shutdown().await;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn directory_pin_shared_by_namespaces_has_no_group_authority() -> Result<()> {
    let f = Fixture::new().await?;
    f.registration().await?;
    let other = f.catalog.open_namespace(config(3)).await?;
    other.register_node_storage(&f.log, TIMEOUT).await?;
    let mut fleet = config(4);
    fleet.mode = NamespaceMode::Fleet;
    let fleet = f.catalog.open_namespace(fleet).await?;
    ensure!(matches!(
        fleet.register_node_storage(&f.log, TIMEOUT).await,
        Err(CatalogError::WrongMode)
    ));
    let objects = f.store.list(None).try_collect::<Vec<_>>().await?;
    assert_eq!(
        objects
            .iter()
            .filter(|o| o.location.filename() == Some("pin")
                && o.location.as_ref().starts_with(&format!(
                    "node-directory/test/raft/{}/nodes/",
                    hex::encode([1; 16])
                )))
            .count(),
        1
    );
    ensure!(objects.iter().any(|object| object.location == key("pin")));
    assert_eq!(
        objects
            .iter()
            .filter(|object| object.location.as_ref().ends_with("/births/pin"))
            .count(),
        2
    );
    ensure!(!objects
        .iter()
        .any(|o| o.location.as_ref().contains("/claims/")));
    ensure!(f.log.groups()?.is_empty());
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn directory_pin_reconciles_lost_create_and_confirmation() -> Result<()> {
    for confirmation in [false, true] {
        let f = Fixture::new().await?;
        if confirmation {
            f.store.fail_confirmation.store(true, Ordering::Release);
        } else {
            f.store.fail_after.store(true, Ordering::Release);
        }
        let first = f.namespace.register_node_storage(&f.log, TIMEOUT).await;
        if confirmation {
            ensure!(first.is_err());
        } else {
            ensure!(first.is_ok());
        }
        f.registration().await?;
        ensure!(f.store.head(&key("pin")).await.is_ok());
        ensure!(f.log.groups()?.is_empty());
        f.close().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn directory_pin_refuses_new_engine_and_changed_disk() -> Result<()> {
    let f = Fixture::new().await?;
    f.registration().await?;
    let fresh = open(
        &f.domain,
        options(&f.root.path().join("fresh"), LogOpenMode::Create, 2),
        Arc::new(DirectFileSystem),
    )
    .await?;
    assert_ne!(fresh.storage_instance()?, f.log.storage_instance()?);
    ensure!(matches!(
        f.namespace.register_node_storage(&fresh, TIMEOUT).await,
        Err(CatalogError::LostStableState)
    ));
    fresh.close().await?;
    let mut changed = options(&f.root.path().join("changed"), LogOpenMode::Create, 3);
    changed.disk = DiskLineage::new([99; 16])?;
    let changed = open(&f.domain, changed, Arc::new(DirectFileSystem)).await?;
    ensure!(matches!(
        f.namespace.register_node_storage(&changed, TIMEOUT).await,
        Err(CatalogError::Conflict(_))
    ));
    changed.close().await?;
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn directory_publication_retry_and_restart_order() -> Result<()> {
    for confirmation in [false, true] {
        let mut f = Fixture::new().await?;
        let reg = f.registration().await?;
        let publication = reg.publication(peer(1), TIMEOUT).await?;
        if confirmation {
            f.store.fail_confirmation.store(true, Ordering::Release);
        } else {
            f.store.fail_after.store(true, Ordering::Release);
        }
        let first = publication.publish(TIMEOUT).await;
        if confirmation {
            ensure!(first.is_err());
        } else {
            ensure!(first.is_ok());
        }
        let first = publication.publish(TIMEOUT).await?;
        assert_eq!(publication.publish(TIMEOUT).await?, first);
        assert_eq!(
            reg.observe_node(&peer(1), TIMEOUT).await?,
            Some(first.clone())
        );
        let instance = f.log.storage_instance()?;
        f.restart(2).await?;
        let reg2 = f.registration().await?;
        let second = reg2
            .publication(peer(2), TIMEOUT)
            .await?
            .publish(TIMEOUT)
            .await?;
        ensure!(second.revision() > first.revision());
        assert_eq!(second.storage_instance(), instance);
        assert_eq!(reg2.observe_node(&peer(1), TIMEOUT).await?, Some(second));
        ensure!(publication.publish(TIMEOUT).await.is_err());
        f.close().await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn directory_late_closed_owner_cannot_replace_new_process() -> Result<()> {
    let mut f = Fixture::new().await?;
    let reg = f.registration().await?;
    let old = reg.publication(peer(1), TIMEOUT).await?;
    f.store.hold_next_view.store(true, Ordering::Release);
    f.store.view_hang.store(true, Ordering::Release);
    let pending = tokio::spawn(async move { old.publish(TIMEOUT).await });
    tokio::time::timeout(TIMEOUT, async {
        while !f.store.view_waiting.load(Ordering::Acquire) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await?;
    f.restart(2).await?;
    let new = f.registration().await?;
    let current = new
        .publication(peer(2), TIMEOUT)
        .await?
        .publish(TIMEOUT)
        .await?;
    f.store.view_hang.store(false, Ordering::Release);
    f.store.view_resume.notify_waiters();
    ensure!(pending.await?.is_err());
    assert_eq!(new.observe_node(&peer(1), TIMEOUT).await?, Some(current));
    ensure!(reg.publication(peer(1), TIMEOUT).await.is_err());
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn directory_scope_trust_corruption_and_bounds() -> Result<()> {
    let f = Fixture::new().await?;
    let reg = f.registration().await?;
    reg.publication(peer(1), TIMEOUT)
        .await?
        .publish(TIMEOUT)
        .await?;
    let valid = f.store.inner.get(&key("view")).await?.bytes().await?;
    let mut wrong = peer(1);
    wrong.disk = DiskLineage::new([99; 16])?;
    ensure!(reg.observe_node(&wrong, TIMEOUT).await.is_err());
    let mut wrong = peer(1);
    wrong.label = "wrong-node".into();
    ensure!(reg.observe_node(&wrong, TIMEOUT).await.is_err());
    let mut wrong = peer(1);
    wrong.server_name = "other.invalid".into();
    ensure!(reg.observe_node(&wrong, TIMEOUT).await.is_err());
    f.store
        .inner
        .put(&key("view"), b"not a current record".to_vec().into())
        .await?;
    ensure!(matches!(
        reg.observe_node(&peer(1), TIMEOUT).await,
        Err(CatalogError::Corrupt(_))
    ));
    f.store
        .inner
        .put(&key("view"), vec![0; 65 * 1024].into())
        .await?;
    ensure!(matches!(
        reg.observe_node(&peer(1), TIMEOUT).await,
        Err(CatalogError::Limit)
    ));
    f.store.inner.put(&key("view"), valid.into()).await?;
    f.store.inner.delete(&key("pin")).await?;
    ensure!(
        matches!(
            f.namespace.register_node_storage(&f.log, TIMEOUT).await,
            Err(CatalogError::Corrupt(_))
        ),
        "missing immutable pin was recreated over a surviving view"
    );
    ensure!(matches!(
        f.store.head(&key("pin")).await,
        Err(object_store::Error::NotFound { .. })
    ));
    ensure!(f.log.groups()?.is_empty());
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn directory_missing_view_restarts_above_old_revision() -> Result<()> {
    let mut f = Fixture::new().await?;
    let reg = f.registration().await?;
    let old = reg
        .publication(peer(1), TIMEOUT)
        .await?
        .publish(TIMEOUT)
        .await?;
    f.store.inner.delete(&key("view")).await?;
    f.restart(2).await?;
    let reg = f.registration().await?;
    let new = reg
        .publication(peer(2), TIMEOUT)
        .await?
        .publish(TIMEOUT)
        .await?;
    ensure!(new.revision() > old.revision());
    assert_eq!(reg.observe_node(&peer(1), TIMEOUT).await?, Some(new));
    f.close().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn directory_deadline_does_not_publish_without_fence() -> Result<()> {
    let root = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = fault::Gate::new(None);
    let log = open(
        &domain,
        options(&root.path().join("node"), LogOpenMode::Create, 1),
        Arc::new(fault::FaultFileSystem { gate: gate.clone() }),
    )
    .await?;
    let store = Arc::new(store::Store::default());
    let catalog = Catalog::new(
        store.clone(),
        CatalogOptions {
            prefix: "node-directory/test".into(),
            ..Default::default()
        },
    )?;
    let namespace = catalog.open_namespace(config(2)).await?;
    let reg = namespace.register_node_storage(&log, TIMEOUT).await?;
    let release = gate.arm(fault::HOLD_SYNC);
    let owner = reg.clone();
    let attempt =
        tokio::spawn(async move { owner.publication(peer(1), Duration::from_millis(150)).await });
    tokio::time::timeout(TIMEOUT, gate.entered.notified()).await?;
    ensure!(matches!(attempt.await?, Err(CatalogError::Unconfirmed)));
    ensure!(matches!(
        store.head(&key("view")).await,
        Err(object_store::Error::NotFound { .. })
    ));
    drop(release);
    let ready = reg
        .publication(peer(1), TIMEOUT)
        .await?
        .publish(TIMEOUT)
        .await?;
    ensure!(ready.revision() > 1);
    log.close().await?;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn directory_reused_process_and_conflicting_attempt_refused() -> Result<()> {
    let mut f = Fixture::new().await?;
    let reg = f.registration().await?;
    let first = reg
        .publication(peer(1), TIMEOUT)
        .await?
        .publish(TIMEOUT)
        .await?;
    let conflicting = reg.publication(peer(1), TIMEOUT).await?;
    ensure!(matches!(
        conflicting.publish(TIMEOUT).await,
        Err(CatalogError::Conflict(_))
    ));
    ensure!(matches!(
        reg.publication(peer(2), TIMEOUT).await,
        Err(CatalogError::Conflict(_))
    ));
    assert_eq!(reg.observe_node(&peer(1), TIMEOUT).await?, Some(first));
    f.restart(1).await?;
    let reused = f
        .registration()
        .await?
        .publication(peer(1), TIMEOUT)
        .await?;
    ensure!(matches!(
        reused.publish(TIMEOUT).await,
        Err(CatalogError::Conflict(_))
    ));
    f.close().await
}
