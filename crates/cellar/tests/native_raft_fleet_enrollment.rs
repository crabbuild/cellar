//! Low-level immutable birth selection gates for the managed enrollment work.
//! The complete dynamic host/trust inventory remains part of FO-03 delivery.
#[path = "support/raft_catalog_fault.rs"]
mod fault;

use anyhow::{ensure, Result};
use cellar::application::consensus::verification::{
    Catalog, CatalogError, CatalogOptions, NamespaceCatalog,
};
use cellar::sdk::{consensus::*, ExecutionDomain};
use object_store::{path::Path, ObjectStore};
use sha2::{Digest, Sha256};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

const TIMEOUT: Duration = Duration::from_secs(5);

struct Fixture {
    domain: ExecutionDomain,
    store: Arc<fault::Store>,
    namespace: NamespaceCatalog,
    proposal: BirthProposal,
}

impl Fixture {
    async fn new() -> Result<Self> {
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let store = Arc::new(fault::Store::default());
        let catalog = Self::catalog(store.clone())?;
        let config = NamespaceConfig::new(
            ClusterId::new([1; 16])?,
            NamespaceId::new([2; 16])?,
            NamespaceMode::Raft,
            [3; 32],
        );
        let namespace = catalog.open_namespace(config).await?;
        let proposal = BirthProposal {
            scope: GroupScope {
                cluster: ClusterId::new([1; 16])?,
                namespace: NamespaceId::new([2; 16])?,
                cell: "Counter:cancelled".into(),
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
                .collect::<Result<Vec<_>>>()?,
            learners: vec![],
        };
        Ok(Self {
            domain,
            store,
            namespace,
            proposal,
        })
    }

    fn catalog(store: Arc<fault::Store>) -> Result<Catalog> {
        Ok(Catalog::new(
            store,
            CatalogOptions {
                prefix: "enrollment/".into(),
                ..Default::default()
            },
        )?)
    }

    fn key(&self) -> Path {
        Path::from(format!(
            "enrollment/namespaces/{}/cells/{}/birth",
            hex::encode([2; 16]),
            hex::encode(Sha256::digest(self.proposal.scope.cell.as_bytes()))
        ))
    }

    fn hold_birth(&self) {
        self.store.birth_hang.store(true, Ordering::Release);
        self.store.hold_next_birth.store(true, Ordering::Release);
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

    fn release(&self) {
        self.store.birth_hang.store(false, Ordering::Release);
        self.store.birth_resume.notify_waiters();
    }

    async fn cancel(&self, operation: u8) -> Result<BirthDisposition, CatalogError> {
        self.domain
            .run(self.namespace.cancel_birth(
                self.proposal.clone(),
                ProposalId::new([operation; 16]).unwrap(),
                TIMEOUT,
            ))
            .await
            .map_err(|_| CatalogError::Unconfirmed)?
    }

    async fn close(self) {
        self.domain.shutdown().await;
    }
}

#[tokio::test]
async fn fleet_enrollment_cancelled_birth() -> Result<()> {
    let f = Fixture::new().await?;
    f.hold_birth();
    let (namespace, proposal) = (f.namespace.clone(), f.proposal.clone());
    let delayed = f
        .domain
        .spawn(async move { namespace.resolve_birth(proposal).await })?;
    f.waiting().await?;
    let cancelled = f.cancel(6).await?;
    ensure!(
        matches!(cancelled, BirthDisposition::Cancelled { scope, operation }
        if scope == f.proposal.scope && operation == ProposalId::new([6; 16])?)
    );
    let tombstone = f.store.inner.get(&f.key()).await?.bytes().await?;
    f.release();
    ensure!(matches!(delayed.await?, Err(CatalogError::BirthCancelled)));
    ensure!(matches!(
        f.namespace.birth(&f.proposal.scope.cell).await,
        Err(CatalogError::BirthCancelled)
    ));
    // Another incarnation at the same name cannot treat cancellation as absence.
    let mut recreated = f.proposal.clone();
    recreated.scope.group = GroupId::new([7; 16])?;
    recreated.scope.incarnation = IncarnationId::new([8; 16])?;
    ensure!(matches!(
        f.namespace.resolve_birth(recreated).await,
        Err(CatalogError::BirthCancelled)
    ));
    ensure!(
        matches!(f.cancel(9).await?, BirthDisposition::Cancelled { operation, .. }
        if operation == ProposalId::new([6; 16])?)
    );
    assert_eq!(f.store.inner.get(&f.key()).await?.bytes().await?, tombstone);
    assert_eq!(f.store.non_create.load(Ordering::Acquire), 0);
    f.close().await;
    Ok(())
}

#[tokio::test]
async fn fleet_enrollment_birth_wins_delayed_cancellation() -> Result<()> {
    let f = Fixture::new().await?;
    f.hold_birth();
    let (namespace, proposal) = (f.namespace.clone(), f.proposal.clone());
    let delayed = f.domain.spawn(async move {
        namespace
            .cancel_birth(proposal, ProposalId::new([6; 16]).unwrap(), TIMEOUT)
            .await
    })?;
    f.waiting().await?;
    let live = f.namespace.resolve_birth(f.proposal.clone()).await?;
    f.release();
    let observed = delayed.await??;
    ensure!(
        matches!(observed, BirthDisposition::Live(birth) if birth.digest() == live.digest() && birth.scope() == live.scope())
    );
    let retry = f.cancel(6).await?;
    ensure!(matches!(retry, BirthDisposition::Live(birth) if birth.digest() == live.digest()));
    assert_eq!(
        f.namespace
            .birth(&f.proposal.scope.cell)
            .await?
            .unwrap()
            .digest(),
        live.digest()
    );
    assert_eq!(f.store.non_create.load(Ordering::Acquire), 0);
    f.close().await;
    Ok(())
}

#[tokio::test]
async fn fleet_enrollment_cancelled_birth_lost_reply() -> Result<()> {
    let f = Fixture::new().await?;
    f.store.fail_birth_after.store(true, Ordering::Release);
    ensure!(matches!(
        f.cancel(6).await?,
        BirthDisposition::Cancelled { .. }
    ));
    let f2 = Fixture::new().await?;
    f2.store
        .fail_birth_confirmation
        .store(true, Ordering::Release);
    ensure!(
        f2.cancel(6).await.is_err(),
        "lost confirmation must remain ambiguous"
    );
    let fresh = Fixture::catalog(f2.store.clone())?
        .existing_namespace(f2.namespace.config().clone())
        .await?;
    ensure!(
        matches!(fresh.birth_disposition(&f2.proposal.scope.cell).await?, Some(BirthDisposition::Cancelled { operation, .. })
        if operation == ProposalId::new([6; 16])?)
    );
    ensure!(matches!(
        f2.domain
            .run(fresh.cancel_birth(f2.proposal.clone(), ProposalId::new([6; 16])?, TIMEOUT))
            .await??,
        BirthDisposition::Cancelled { .. }
    ));
    ensure!(matches!(
        fresh.resolve_birth(f2.proposal.clone()).await,
        Err(CatalogError::BirthCancelled)
    ));
    f.close().await;
    f2.close().await;
    Ok(())
}

#[tokio::test]
async fn fleet_enrollment_birth_cancellation_deadline() -> Result<()> {
    for after_commit in [false, true] {
        let f = Fixture::new().await?;
        if after_commit {
            f.store.birth_hang.store(true, Ordering::Release);
            f.store
                .hold_birth_confirmation
                .store(true, Ordering::Release);
        } else {
            f.hold_birth();
        }
        let (namespace, proposal) = (f.namespace.clone(), f.proposal.clone());
        let attempt = f.domain.spawn(async move {
            namespace
                .cancel_birth(
                    proposal,
                    ProposalId::new([6; 16]).unwrap(),
                    Duration::from_millis(250),
                )
                .await
        })?;
        f.waiting().await?;
        ensure!(matches!(attempt.await?, Err(CatalogError::Unconfirmed)));
        assert!(
            !f.store.birth_waiting.load(Ordering::Acquire),
            "cancelled I/O remained physically owned"
        );
        f.release();
        let observed = f
            .namespace
            .birth_disposition(&f.proposal.scope.cell)
            .await?;
        if after_commit {
            ensure!(matches!(observed, Some(BirthDisposition::Cancelled { .. })));
        } else {
            ensure!(observed.is_none());
        }
        // Either timeout outcome still requires the exact original decision;
        // no missing reply or absence observation releases an inventory claim.
        ensure!(matches!(
            f.cancel(6).await?,
            BirthDisposition::Cancelled { .. }
        ));
        ensure!(matches!(
            f.namespace.resolve_birth(f.proposal.clone()).await,
            Err(CatalogError::BirthCancelled)
        ));
        ensure!(matches!(
            f.domain
                .run(f.namespace.cancel_birth(
                    f.proposal.clone(),
                    ProposalId::new([6; 16])?,
                    Duration::ZERO
                ))
                .await?,
            Err(CatalogError::Invalid(_))
        ));
        f.close().await;
    }
    Ok(())
}

#[tokio::test]
async fn fleet_enrollment_invalid_birth_tombstone() -> Result<()> {
    let f = Fixture::new().await?;
    f.cancel(6).await?;
    let mut oversized = f.proposal.clone();
    oversized.voters = vec![oversized.voters[0]; 33];
    f.store.fail_birth_read.store(true, Ordering::Release);
    ensure!(matches!(
        f.domain
            .run(
                f.namespace
                    .cancel_birth(oversized, ProposalId::new([6; 16])?, TIMEOUT)
            )
            .await?,
        Err(CatalogError::Invalid("initial member count"))
    ));
    assert!(
        f.store.fail_birth_read.load(Ordering::Acquire),
        "oversized input reached storage before admission"
    );
    f.store.fail_birth_read.store(false, Ordering::Release);
    let mut bytes = f.store.inner.get(&f.key()).await?.bytes().await?.to_vec();
    let end = bytes.len() - 32;
    let payload = std::str::from_utf8(&bytes[13..end])?;
    let needle = format!("\"cancelled_by\":[{}]", vec!["6"; 16].join(","));
    let replacement = format!("\"cancelled_by\":[{}]", vec!["0"; 16].join(","));
    ensure!(payload.contains(&needle));
    let invalid = payload.replace(&needle, &replacement);
    bytes[13..end].copy_from_slice(invalid.as_bytes());
    let checksum = Sha256::digest(&bytes[..end]);
    bytes[end..].copy_from_slice(&checksum);
    f.store.inner.put(&f.key(), bytes.into()).await?;
    ensure!(matches!(
        f.namespace.birth_disposition(&f.proposal.scope.cell).await,
        Err(CatalogError::Corrupt("birth cancellation identity"))
    ));
    ensure!(matches!(
        f.namespace.resolve_birth(f.proposal.clone()).await,
        Err(CatalogError::Corrupt(_))
    ));
    f.close().await;
    Ok(())
}
