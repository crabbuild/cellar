//! The admission boundary uses real conditional writes. Lifecycle state edits
//! stand in for the later typed drain controller; they grant no removal receipt.
use super::fault;
use anyhow::{ensure, Result};
use cellar::application::consensus::verification::*;
use cellar::bucket::{Bucket, StorageBackend};
use cellar::sdk::{consensus::*, ExecutionDomain};
use object_store::list::*;
use object_store::{path::Path, ObjectStore};
use sha2::{Digest, Sha256};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct NoListing;

#[async_trait::async_trait]
impl PaginatedListStore for NoListing {
    async fn list_paginated(
        &self,
        _: Option<&str>,
        _: PaginatedListOptions,
    ) -> object_store::Result<PaginatedListResult> {
        Err(object_store::Error::NotSupported {
            source: "birth admission never lists".into(),
        })
    }
}

struct Fixture {
    domain: ExecutionDomain,
    store: Arc<fault::Store>,
    bucket: Bucket,
    fleet: FleetCatalog,
    namespace: NamespaceCatalog,
    proposal: BirthProposal,
}

impl Fixture {
    async fn new() -> Result<Self> {
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let store = Arc::new(fault::Store::default());
        let bucket = Bucket::from_clients(
            store.clone(),
            store.clone(),
            Arc::new(NoListing),
            StorageBackend::S3,
            "births".into(),
            "tenant/".into(),
        )?;
        let fleet = Self::catalog(&domain, bucket.clone(), "coordination/")?;
        fleet.initialize(TIMEOUT).await?;
        for id in 1..=3u8 {
            let root = fleet.read(TIMEOUT).await?;
            fleet
                .register_node(
                    &root,
                    &Self::request(id, root.revision()),
                    &FleetNodeSpec {
                        node: NodeId::new([id; 16])?,
                        disk: DiskLineage::new([id + 10; 16])?,
                        physical_host: IncarnationId::new([id + 20; 16])?,
                        label: format!("birth-node-{id}"),
                        server_name: "localhost".into(),
                        capability_digest: [3; 32],
                        storage_instance: Some(EngineInstanceId::new([id + 30; 16])?),
                    },
                    TIMEOUT,
                )
                .await?;
        }
        let config = fleet.bind_namespace(NamespaceConfig::new(
            ClusterId::new([1; 16])?,
            NamespaceId::new([2; 16])?,
            NamespaceMode::Raft,
            [3; 32],
        ))?;
        let namespace = Catalog::new(
            store.clone(),
            CatalogOptions {
                prefix: "birth-data/".into(),
                ..Default::default()
            },
        )?
        .open_namespace(config)
        .await?;
        let proposal = BirthProposal {
            scope: GroupScope {
                cluster: ClusterId::new([1; 16])?,
                namespace: NamespaceId::new([2; 16])?,
                cell: "Counter:managed".into(),
                group: GroupId::new([4; 16])?,
                incarnation: IncarnationId::new([5; 16])?,
            },
            voters: (1..=3)
                .map(|n| {
                    Ok(InitialMember {
                        member: MemberId::new(n)?,
                        node: NodeId::new([n as u8; 16])?,
                    })
                })
                .collect::<Result<_>>()?,
            learners: vec![],
        };
        let f = Self {
            domain,
            store,
            bucket,
            fleet,
            namespace,
            proposal,
        };
        for id in 1..=3 {
            f.edit_node(id, "state", serde_json::json!("Active"), 10 + id)
                .await?;
        }
        Ok(f)
    }

    fn catalog(domain: &ExecutionDomain, bucket: Bucket, prefix: &str) -> Result<FleetCatalog> {
        Ok(FleetCatalog::new(
            domain,
            bucket,
            ClusterId::new([1; 16])?,
            IncarnationId::new([80; 16])?,
            prefix,
        )?)
    }

    fn request(id: u8, revision: u64) -> FleetRequestIdentity {
        FleetRequestIdentity {
            id: [id; 16],
            operation: [100; 16],
            principal: "birth-test".into(),
            expected_revision: revision,
        }
    }

    async fn edit_node(
        &self,
        id: u8,
        field: &str,
        replacement: serde_json::Value,
        request_id: u8,
    ) -> Result<()> {
        self.edit_node_using(&self.fleet, id, field, replacement, request_id)
            .await
    }

    async fn edit_node_using(
        &self,
        fleet: &FleetCatalog,
        id: u8,
        field: &str,
        replacement: serde_json::Value,
        request_id: u8,
    ) -> Result<()> {
        let root = fleet.read(TIMEOUT).await?;
        let key = hex::encode([id; 16]);
        let bytes = fleet
            .get(&root, FleetTable::Nodes, &key, TIMEOUT)
            .await?
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes[13..bytes.len() - 32])?;
        let mut payload = std::str::from_utf8(&bytes[13..bytes.len() - 32])?.to_owned();
        let mut changes = vec![
            (field, replacement),
            (
                "epoch",
                serde_json::json!(value["epoch"].as_u64().unwrap() + 1),
            ),
        ];
        if field == "state" && changes[0].1 == "Draining" {
            changes.push(("operation", serde_json::json!(vec![100; 16])));
        }
        for (field, replacement) in changes {
            payload = payload.replace(
                &format!("\"{field}\":{}", value[field]),
                &format!("\"{field}\":{replacement}"),
            );
        }
        let mut updated = bytes[..9].to_vec();
        updated.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        updated.extend_from_slice(payload.as_bytes());
        updated.extend_from_slice(&Sha256::digest(&updated));
        fleet
            .commit(
                &root,
                &FleetMutation {
                    id: [request_id; 16],
                    operation: [100; 16],
                    principal: "birth-test".into(),
                    expected_revision: root.revision(),
                    changes: vec![FleetChange {
                        table: FleetTable::Nodes,
                        key,
                        value: Some(updated),
                    }],
                },
                TIMEOUT,
            )
            .await?;
        Ok(())
    }

    async fn reserved(&self) -> Result<FleetBirthReservation> {
        let root = self.fleet.read(TIMEOUT).await?;
        Ok(self
            .fleet
            .reserve_birth(
                &root,
                &Self::request(40, root.revision()),
                &self.namespace,
                self.proposal.clone(),
                TIMEOUT,
            )
            .await?)
    }

    async fn confirm(&self, auth: &FleetBirthAuthorization) -> Result<FleetSnapshot> {
        let root = self.fleet.read(TIMEOUT).await?;
        Ok(self
            .fleet
            .confirm_birth(
                &root,
                &Self::request(41, root.revision()),
                &self.namespace,
                auth,
                TIMEOUT,
            )
            .await?
            .snapshot)
    }

    async fn waiting(&self) -> Result<()> {
        self.domain
            .run(cellar::asyncrt::timeout(TIMEOUT, async {
                while !self.store.birth_waiting.load(Ordering::Acquire) {
                    tokio::task::yield_now().await;
                }
            }))
            .await??;
        Ok(())
    }

    fn hold(&self) {
        self.store.birth_hang.store(true, Ordering::Release);
        self.store.hold_next_birth.store(true, Ordering::Release);
    }

    fn release(&self) {
        self.store.birth_hang.store(false, Ordering::Release);
        self.store.birth_resume.notify_waiters();
    }
}

#[tokio::test]
async fn fleet_birth_reservation_managed_gate() -> Result<()> {
    let f = Fixture::new().await?;
    ensure!(matches!(
        f.namespace.resolve_birth(f.proposal.clone()).await,
        Err(CatalogError::ManagedAdmissionRequired)
    ));
    ensure!(matches!(
        f.namespace
            .cancel_birth(f.proposal.clone(), ProposalId::new([9; 16])?, TIMEOUT)
            .await,
        Err(CatalogError::ManagedAdmissionRequired)
    ));
    let mut unmanaged = f.namespace.config().clone();
    unmanaged.managed_fleet = None;
    let catalog = Catalog::new(
        f.store.clone(),
        CatalogOptions {
            prefix: "birth-data/".into(),
            ..Default::default()
        },
    )?;
    ensure!(matches!(
        catalog.existing_namespace(unmanaged).await,
        Err(CatalogError::Conflict("immutable namespace root"))
    ));
    let other = Fixture::catalog(&f.domain, f.bucket.clone(), "other-root/")?;
    ensure!(matches!(
        other.bind_namespace(f.namespace.config().clone()),
        Err(FleetCatalogError::Conflict)
    ));
    let reservation = f.reserved().await?;
    ensure!(matches!(
        f.fleet
            .authorize_group(&reservation.commit.snapshot, &f.proposal.scope, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    let birth = f
        .domain
        .run(
            f.namespace
                .resolve_managed_birth(&reservation.authorization, TIMEOUT),
        )
        .await??;
    let root = f.confirm(&reservation.authorization).await?;
    let history = f.fleet.history(&root, &f.proposal.scope, TIMEOUT).await?;
    ensure!(
        *history.state() == FleetHistoryState::Live
            && history.birth_digest() == birth.digest()
            && history.schema_digest() == [3; 32]
    );
    f.fleet
        .authorize_group(&root, &f.proposal.scope, TIMEOUT)
        .await?;
    let retry = f
        .domain
        .run(
            f.namespace
                .resolve_managed_birth(&reservation.authorization, TIMEOUT),
        )
        .await??;
    ensure!(retry.digest() == birth.digest());
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn fleet_birth_reservation_drain_race() -> Result<()> {
    let f = Fixture::new().await?;
    let reservation = f.reserved().await?;
    f.hold();
    let namespace = f.namespace.clone();
    let auth = reservation.authorization.clone();
    let delayed = f
        .domain
        .spawn(async move { namespace.resolve_managed_birth(&auth, TIMEOUT).await })?;
    f.waiting().await?;
    f.edit_node(1, "state", serde_json::json!("Draining"), 50)
        .await?;
    let root = f.fleet.read(TIMEOUT).await?;
    let pending = f.fleet.history(&root, &f.proposal.scope, TIMEOUT).await?;
    ensure!(*pending.state() == FleetHistoryState::Reserved);
    ensure!(pending
        .initial_placement()?
        .voters
        .iter()
        .any(|m| m.node == NodeId::new([1; 16]).unwrap()));
    f.release();
    delayed.await??;
    let root = f.confirm(&reservation.authorization).await?;
    ensure!(
        *f.fleet
            .history(&root, &f.proposal.scope, TIMEOUT)
            .await?
            .state()
            == FleetHistoryState::Live
    );
    f.domain.shutdown().await;

    let f = Fixture::new().await?;
    let old = f.fleet.read(TIMEOUT).await?;
    f.store.fleet_cas_hang.store(true, Ordering::Release);
    f.store.hold_next_fleet_cas.store(true, Ordering::Release);
    let fleet = f.fleet.clone();
    let namespace = f.namespace.clone();
    let proposal = f.proposal.clone();
    let delayed = f.domain.spawn(async move {
        fleet
            .reserve_birth(
                &old,
                &Fixture::request(40, old.revision()),
                &namespace,
                proposal,
                TIMEOUT,
            )
            .await
    })?;
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            while !f.store.fleet_cas_waiting.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        }))
        .await??;
    let concurrent = Fixture::catalog(&f.domain, f.bucket.clone(), "coordination/")?;
    f.edit_node_using(&concurrent, 1, "state", serde_json::json!("Draining"), 50)
        .await?;
    f.store.fleet_cas_hang.store(false, Ordering::Release);
    f.store.fleet_cas_resume.notify_waiters();
    ensure!(matches!(delayed.await?, Err(FleetCatalogError::Conflict)));
    ensure!(
        matches!(f.reserved().await, Err(error) if error.downcast_ref::<FleetCatalogError>().is_some_and(|e| matches!(e, FleetCatalogError::Conflict)))
    );
    ensure!(f
        .namespace
        .birth_disposition(&f.proposal.scope.cell)
        .await?
        .is_none());
    let root = f.fleet.read(TIMEOUT).await?;
    ensure!(f
        .fleet
        .page(&root, FleetTable::Histories, None, 128, TIMEOUT)
        .await?
        .entries
        .is_empty());
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn fleet_birth_reservation_lost_reply() -> Result<()> {
    let f = Fixture::new().await?;
    let root = f.fleet.read(TIMEOUT).await?;
    let request = Fixture::request(40, root.revision());
    let mut invalid = f.proposal.clone();
    invalid.scope.cell.clear();
    ensure!(matches!(
        f.fleet
            .reserve_birth(&root, &request, &f.namespace, invalid, TIMEOUT)
            .await,
        Err(FleetCatalogError::Invalid(_))
    ));
    f.store.fail_head_after.store(true, Ordering::Release);
    f.store
        .fail_head_confirmation
        .store(true, Ordering::Release);
    ensure!(f
        .fleet
        .reserve_birth(&root, &request, &f.namespace, f.proposal.clone(), TIMEOUT)
        .await
        .is_err());
    f.edit_node(1, "state", serde_json::json!("Draining"), 50)
        .await?;
    let fresh = Fixture::catalog(&f.domain, f.bucket.clone(), "coordination/")?;
    let current = fresh.read(TIMEOUT).await?;
    let retry = fresh
        .reserve_birth(
            &current,
            &request,
            &f.namespace,
            f.proposal.clone(),
            TIMEOUT,
        )
        .await?;
    ensure!(
        retry.commit.applied_revision == root.revision() + 1
            && retry.commit.snapshot.revision() == current.revision()
    );
    let mut changed = f.proposal.clone();
    changed.voters[0].member = MemberId::new(10)?;
    ensure!(matches!(
        fresh
            .reserve_birth(&current, &request, &f.namespace, changed, TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    f.domain
        .run(
            f.namespace
                .resolve_managed_birth(&retry.authorization, TIMEOUT),
        )
        .await??;
    f.confirm(&retry.authorization).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn fleet_birth_reservation_cancelled_writer() -> Result<()> {
    let f = Fixture::new().await?;
    let reservation = f.reserved().await?;
    f.hold();
    let auth = reservation.authorization.clone();
    let namespace = f.namespace.clone();
    let delayed = f
        .domain
        .spawn(async move { namespace.resolve_managed_birth(&auth, TIMEOUT).await })?;
    f.waiting().await?;
    let observed = f
        .domain
        .run(f.namespace.cancel_managed_birth(
            &reservation.authorization,
            ProposalId::new([60; 16])?,
            TIMEOUT,
        ))
        .await??;
    ensure!(matches!(observed, BirthDisposition::Cancelled { .. }));
    f.release();
    ensure!(matches!(delayed.await?, Err(CatalogError::BirthCancelled)));
    let root = f.confirm(&reservation.authorization).await?;
    ensure!(
        matches!(f.fleet.history(&root, &f.proposal.scope, TIMEOUT).await?.state(), FleetHistoryState::Cancelled { operation } if *operation == [60;16])
    );
    ensure!(matches!(
        f.fleet
            .authorize_group(&root, &f.proposal.scope, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    let mut changed = f.proposal.clone();
    changed.scope.group = GroupId::new([61; 16])?;
    ensure!(matches!(
        f.fleet
            .reserve_birth(
                &root,
                &Fixture::request(61, root.revision()),
                &f.namespace,
                changed,
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn fleet_birth_reservation_domains() -> Result<()> {
    let f = Fixture::new().await?;
    f.edit_node(2, "physical_host", serde_json::json!(vec![21; 16]), 50)
        .await?;
    let root = f.fleet.read(TIMEOUT).await?;
    ensure!(matches!(
        f.fleet
            .reserve_birth(
                &root,
                &Fixture::request(40, root.revision()),
                &f.namespace,
                f.proposal.clone(),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(f
        .namespace
        .birth_disposition(&f.proposal.scope.cell)
        .await?
        .is_none());
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn fleet_birth_reservation_admission_bounds() -> Result<()> {
    let f = Fixture::new().await?;
    let root = f.fleet.read(TIMEOUT).await?;
    let mut request = Fixture::request(40, root.revision());
    request.principal = "x".repeat(257);
    ensure!(matches!(
        f.fleet
            .reserve_birth(&root, &request, &f.namespace, f.proposal.clone(), TIMEOUT)
            .await,
        Err(FleetCatalogError::Invalid(_))
    ));
    let request = Fixture::request(40, root.revision());
    ensure!(matches!(
        f.fleet
            .reserve_birth(
                &root,
                &request,
                &f.namespace,
                f.proposal.clone(),
                Duration::ZERO
            )
            .await,
        Err(FleetCatalogError::Invalid(_))
    ));
    let mut unknown = f.proposal.clone();
    unknown.voters[0].node = NodeId::new([99; 16])?;
    ensure!(matches!(
        f.fleet
            .reserve_birth(&root, &request, &f.namespace, unknown, TIMEOUT)
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    f.edit_node(1, "state", serde_json::json!("Registered"), 50)
        .await?;
    let root = f.fleet.read(TIMEOUT).await?;
    ensure!(matches!(
        f.fleet
            .reserve_birth(
                &root,
                &Fixture::request(40, root.revision()),
                &f.namespace,
                f.proposal.clone(),
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Conflict)
    ));
    ensure!(f
        .fleet
        .page(&root, FleetTable::Histories, None, 128, TIMEOUT)
        .await?
        .entries
        .is_empty());
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn fleet_birth_reservation_timeout_retains_intent() -> Result<()> {
    let f = Fixture::new().await?;
    let reservation = f.reserved().await?;
    f.hold();
    ensure!(matches!(
        f.domain
            .run(
                f.namespace
                    .resolve_managed_birth(&reservation.authorization, Duration::from_millis(20))
            )
            .await?,
        Err(CatalogError::Unconfirmed)
    ));
    ensure!(!f.store.birth_waiting.load(Ordering::Acquire));
    let root = f.fleet.read(TIMEOUT).await?;
    ensure!(
        *f.fleet
            .history(&root, &f.proposal.scope, TIMEOUT)
            .await?
            .state()
            == FleetHistoryState::Reserved
    );
    ensure!(f
        .namespace
        .birth_disposition(&f.proposal.scope.cell)
        .await?
        .is_none());
    f.release();
    f.domain
        .run(
            f.namespace
                .resolve_managed_birth(&reservation.authorization, TIMEOUT),
        )
        .await??;
    f.confirm(&reservation.authorization).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn fleet_birth_reservation_corrupt_confirmation() -> Result<()> {
    let f = Fixture::new().await?;
    let reservation = f.reserved().await?;
    f.domain
        .run(
            f.namespace
                .resolve_managed_birth(&reservation.authorization, TIMEOUT),
        )
        .await??;
    let key = Path::from(format!(
        "birth-data/namespaces/{}/cells/{}/birth",
        hex::encode(f.proposal.scope.namespace.bytes()),
        hex::encode(Sha256::digest(f.proposal.scope.cell.as_bytes()))
    ));
    let original = f.store.inner.get(&key).await?.bytes().await?;
    let mut corrupt = original.to_vec();
    corrupt[0] ^= 1;
    f.store.inner.put(&key, corrupt.into()).await?;
    let root = f.fleet.read(TIMEOUT).await?;
    ensure!(matches!(
        f.fleet
            .confirm_birth(
                &root,
                &Fixture::request(41, root.revision()),
                &f.namespace,
                &reservation.authorization,
                TIMEOUT
            )
            .await,
        Err(FleetCatalogError::Corrupt(_))
    ));
    ensure!(
        *f.fleet
            .history(&root, &f.proposal.scope, TIMEOUT)
            .await?
            .state()
            == FleetHistoryState::Reserved
    );
    f.store.inner.put(&key, original.into()).await?;
    f.confirm(&reservation.authorization).await?;
    f.domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn fleet_birth_reservation_unguarded_genesis() -> Result<()> {
    let f = Fixture::new().await?;
    let reservation = f.reserved().await?;
    let birth = f
        .domain
        .run(
            f.namespace
                .resolve_managed_birth(&reservation.authorization, TIMEOUT),
        )
        .await??;
    f.confirm(&reservation.authorization).await?;
    let root = tempfile::tempdir()?;
    let log = f
        .domain
        .run(ReplicaLog::open(
            ReplicaLogOptions::new(
                root.path().join("raft"),
                NodeId::new([1; 16])?,
                DiskLineage::new([11; 16])?,
                ProcessGeneration::new([51; 16])?,
                LogOpenMode::Create,
            ),
            Arc::new(cellar_ltx::DirectFileSystem),
        ))
        .await??;
    ensure!(matches!(
        f.domain
            .run(f.namespace.bootstrap_member(&birth, &log))
            .await?,
        Err(CatalogError::ManagedAdmissionRequired)
    ));
    ensure!(matches!(
        log.group(&f.proposal.scope),
        Err(LogError::NotFound)
    ));
    f.domain
        .run(log.create_group(GroupBootstrap {
            scope: f.proposal.scope.clone(),
            member: MemberId::new(1)?,
            birth_digest: birth.digest(),
            voters: birth.voters().iter().map(|m| m.member).collect(),
            learners: vec![],
        }))
        .await??;
    ensure!(matches!(
        f.domain
            .run(f.namespace.bootstrap_member(&birth, &log))
            .await?,
        Err(CatalogError::ManagedAdmissionRequired)
    ));
    ensure!(log
        .group(&f.proposal.scope)?
        .membership()?
        .fleet_status()
        .is_none());
    log.close().await?;
    f.domain.shutdown().await;
    Ok(())
}
