//! CAS behavior uses real conditional in-memory writes with lost-response faults.
//! This qualifies the catalog adapter, not the future fleet operating envelope.
#[path = "support/raft_catalog_fault.rs"]
mod fault;

use anyhow::{ensure, Result};
use cellar::application::consensus::verification::*;
use cellar::bucket::{Bucket, StorageBackend};
use cellar::sdk::ExecutionDomain;
use cellar_logic::consensus::{ClusterId, IncarnationId};
use futures_util::TryStreamExt;
use object_store::{list::*, path::Path, ObjectStore};
use std::{
    collections::BTreeMap,
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
            source: "fleet catalog must never list".into(),
        })
    }
}

struct Fixture {
    domain: ExecutionDomain,
    bucket: Bucket,
    store: Arc<fault::Store>,
    catalog: FleetCatalog,
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
            "fleet-test".into(),
            "tenant/".into(),
        )?;
        let catalog = Self::catalog(&domain, bucket.clone())?;
        catalog.initialize(TIMEOUT).await?;
        Ok(Self {
            domain,
            bucket,
            store,
            catalog,
        })
    }

    fn catalog(domain: &ExecutionDomain, bucket: Bucket) -> Result<FleetCatalog> {
        Ok(FleetCatalog::new(
            domain,
            bucket,
            ClusterId::new([1; 16])?,
            IncarnationId::new([2; 16])?,
            "coordination/",
        )?)
    }

    fn key(tail: &str) -> String {
        format!(
            "tenant/coordination/raft/{}/fleet/{tail}",
            hex::encode([1; 16])
        )
    }

    async fn close(self) {
        self.domain.shutdown().await;
    }
}

fn mutation(id: u8, revision: u64, key: &str, value: &[u8]) -> FleetMutation {
    FleetMutation {
        id: [id; 16],
        operation: [100; 16],
        expected_revision: revision,
        principal: "operator-test".into(),
        changes: vec![FleetChange {
            table: FleetTable::Operations,
            key: key.into(),
            value: Some(value.into()),
        }],
    }
}

#[tokio::test]
async fn fleet_root_concurrent_cas() -> Result<()> {
    let f = Fixture::new().await?;
    let second = Fixture::catalog(&f.domain, f.bucket.clone())?;
    let a = f.catalog.read(TIMEOUT).await?;
    let b = second.read(TIMEOUT).await?;
    let ma = mutation(3, 1, "winner", b"a");
    let mb = mutation(4, 1, "winner", b"b");
    let (a, b) = tokio::join!(
        f.catalog.commit(&a, &ma, TIMEOUT),
        second.commit(&b, &mb, TIMEOUT)
    );
    ensure!(a.is_ok() != b.is_ok());
    ensure!(matches!(
        a.as_ref().err().or(b.as_ref().err()),
        Some(FleetCatalogError::Conflict)
    ));
    let root = f.catalog.read(TIMEOUT).await?;
    ensure!(root.revision() == 2);
    let winner = f
        .catalog
        .get(&root, FleetTable::Operations, "winner", TIMEOUT)
        .await?;
    ensure!(
        winner
            == Some(if a.is_ok() {
                b"a".to_vec()
            } else {
                b"b".to_vec()
            })
    );
    f.close().await;
    Ok(())
}

#[tokio::test]
async fn fleet_root_lost_response() -> Result<()> {
    let f = Fixture::new().await?;
    let before = f.catalog.read(TIMEOUT).await?;
    let first = mutation(3, 1, "lost-reply", b"retained");
    f.store.fail_head_after.store(true, Ordering::Release);
    ensure!(
        f.catalog
            .commit(&before, &first, TIMEOUT)
            .await?
            .applied_revision
            == 2
    );
    let before = f.catalog.read(TIMEOUT).await?;
    let ambiguous = mutation(4, 2, "lost-confirmation", b"committed");
    f.store
        .fail_head_confirmation
        .store(true, Ordering::Release);
    ensure!(f
        .catalog
        .commit(&before, &ambiguous, TIMEOUT)
        .await
        .is_err());
    let later = f.catalog.read(TIMEOUT).await?;
    ensure!(later.revision() == 3);
    f.catalog
        .commit(&later, &mutation(5, 3, "later-writer", b"yes"), TIMEOUT)
        .await?;
    // A fresh adapter, current snapshot and original request must settle the
    // historical result, including after another committed root update.
    let recovered = Fixture::catalog(&f.domain, f.bucket.clone())?;
    let now = recovered.read(TIMEOUT).await?;
    let retry = recovered.commit(&now, &ambiguous, TIMEOUT).await?;
    ensure!(retry.applied_revision == 3 && retry.snapshot.revision() == 4);
    f.close().await;
    Ok(())
}

#[tokio::test]
async fn fleet_root_orphan_pages() -> Result<()> {
    let f = Fixture::new().await?;
    let root = f.catalog.read(TIMEOUT).await?;
    let request = mutation(3, 1, "uncommitted", b"orphan");
    f.store.fail_head_before.store(true, Ordering::Release);
    ensure!(f.catalog.commit(&root, &request, TIMEOUT).await.is_err());
    let observed = f.catalog.read(TIMEOUT).await?;
    ensure!(observed.revision() == 1);
    ensure!(f
        .catalog
        .get(&observed, FleetTable::Operations, "uncommitted", TIMEOUT)
        .await?
        .is_none());
    let objects = f.store.inner.list(None).try_collect::<Vec<_>>().await?;
    ensure!(objects
        .iter()
        .any(|object| object.location.as_ref().contains("/objects/")));
    ensure!(
        f.catalog
            .commit(&observed, &request, TIMEOUT)
            .await?
            .applied_revision
            == 2
    );
    f.close().await;
    Ok(())
}

#[tokio::test]
async fn fleet_root_rejects_invalid_records() -> Result<()> {
    use sha2::{Digest, Sha256};
    for invalid in ["duplicate", "format", "oversized", "missing-page"] {
        let f = Fixture::new().await?;
        let key = Fixture::key("head");
        let original = f
            .store
            .inner
            .get(&Path::from(key.clone()))
            .await?
            .bytes()
            .await?;
        let payload = &original[13..original.len() - 32];
        let payload = match invalid {
            "duplicate" => String::from_utf8(payload.to_vec())?
                .replacen("\"format\":1", "\"format\":1,\"format\":1", 1)
                .into_bytes(),
            "format" => String::from_utf8(payload.to_vec())?
                .replacen("\"format\":1", "\"format\":2", 1)
                .into_bytes(),
            "missing-page" => String::from_utf8(payload.to_vec())?
                .replacen(
                    "\"tables\":[null",
                    &format!("\"tables\":[{}", serde_json::to_string(&[9u8; 32])?),
                    1,
                )
                .into_bytes(),
            _ => vec![0; 1024 * 1024 + 1],
        };
        let mut bytes = original[..9].to_vec();
        bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&payload);
        bytes.extend_from_slice(&Sha256::digest(&bytes));
        f.store.inner.put(&Path::from(key), bytes.into()).await?;
        if invalid == "missing-page" {
            let root = f.catalog.read(TIMEOUT).await?;
            ensure!(f
                .catalog
                .get(&root, FleetTable::Policy, "absent", TIMEOUT)
                .await
                .is_err());
        } else {
            ensure!(f.catalog.read(TIMEOUT).await.is_err());
        }
        f.close().await;
    }
    Ok(())
}

#[tokio::test]
async fn fleet_root_terminal_retry() -> Result<()> {
    let f = Fixture::new().await?;
    let root = f.catalog.read(TIMEOUT).await?;
    let request = mutation(3, 1, "operation-state", b"terminal-receipt-digest");
    let committed = f.catalog.commit(&root, &request, TIMEOUT).await?;
    let retry = f
        .catalog
        .commit(&committed.snapshot, &request, TIMEOUT)
        .await?;
    ensure!(retry.applied_revision == 2 && retry.snapshot.revision() == 2);
    let mut conflicting = request;
    conflicting.expected_revision = 2;
    conflicting.changes[0].value = Some(b"different".to_vec());
    ensure!(matches!(
        f.catalog
            .commit(&retry.snapshot, &conflicting, TIMEOUT)
            .await,
        Err(FleetCatalogError::PayloadConflict)
    ));
    f.close().await;
    Ok(())
}

#[tokio::test]
async fn fleet_root_unreferenced_receipt() -> Result<()> {
    let f = Fixture::new().await?;
    let tail = "receipts/original-id/candidate-digest";
    f.store
        .inner
        .put(
            &Path::from(Fixture::key(tail)),
            b"unreferenced".to_vec().into(),
        )
        .await?;
    let root = f.catalog.read(TIMEOUT).await?;
    ensure!(f
        .catalog
        .get(&root, FleetTable::Operations, "original-id", TIMEOUT)
        .await?
        .is_none());
    ensure!(root.revision() == 1);
    f.close().await;
    Ok(())
}

#[tokio::test]
async fn fleet_root_finalization_replan() -> Result<()> {
    let f = Fixture::new().await?;
    let root = f.catalog.read(TIMEOUT).await?;
    let a = mutation(3, 1, "operation", b"finalizing:candidate-a");
    let one = f.catalog.commit(&root, &a, TIMEOUT).await?;
    let b = mutation(4, 2, "operation", b"finalizing:candidate-b");
    f.store.fail_head_before.store(true, Ordering::Release);
    ensure!(f.catalog.commit(&one.snapshot, &b, TIMEOUT).await.is_err());
    let current = f.catalog.read(TIMEOUT).await?;
    ensure!(
        f.catalog
            .get(&current, FleetTable::Operations, "operation", TIMEOUT)
            .await?
            == Some(b"finalizing:candidate-a".to_vec())
    );
    let two = f.catalog.commit(&current, &b, TIMEOUT).await?;
    let final_request = mutation(5, 3, "operation", b"completed:candidate-b");
    let final_root = f
        .catalog
        .commit(&two.snapshot, &final_request, TIMEOUT)
        .await?;
    ensure!(
        f.catalog
            .get(
                &final_root.snapshot,
                FleetTable::Operations,
                "operation",
                TIMEOUT
            )
            .await?
            == Some(b"completed:candidate-b".to_vec())
    );
    ensure!(
        f.catalog
            .commit(&final_root.snapshot, &a, TIMEOUT)
            .await?
            .applied_revision
            == 2
    );
    let now = f.catalog.read(TIMEOUT).await?;
    ensure!(now.revision() == 4);
    f.close().await;
    Ok(())
}

#[tokio::test]
async fn fleet_index_pages_preserve_snapshot_and_cursor() -> Result<()> {
    let f = Fixture::new().await?;
    let initial = f.catalog.read(TIMEOUT).await?;
    let mut current = initial.clone();
    let mut expected = BTreeMap::new();
    for batch in 0..24u8 {
        let changes = (0..8u8)
            .map(|at| {
                let key = format!("history-{}", u16::from(batch) * 8 + u16::from(at));
                let value = vec![batch; 1024];
                expected.insert(key.clone(), value.clone());
                FleetChange {
                    table: FleetTable::Histories,
                    key,
                    value: Some(value),
                }
            })
            .collect();
        let mut request = mutation(batch + 1, current.revision(), "unused", b"");
        request.changes = changes;
        current = f
            .catalog
            .commit(&current, &request, TIMEOUT)
            .await?
            .snapshot;
    }
    ensure!(f
        .catalog
        .page(&initial, FleetTable::Histories, None, 7, TIMEOUT)
        .await?
        .entries
        .is_empty());
    let mut actual = BTreeMap::new();
    let mut cursor = None;
    loop {
        let page = f
            .catalog
            .page(&current, FleetTable::Histories, cursor.as_ref(), 7, TIMEOUT)
            .await?;
        for entry in page.entries {
            ensure!(actual.insert(entry.key, entry.value).is_none());
        }
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    ensure!(actual == expected);
    let first_page = f
        .catalog
        .page(&current, FleetTable::Histories, None, 1, TIMEOUT)
        .await?;
    let mut removal = mutation(99, current.revision(), "unused", b"");
    removal.changes = vec![FleetChange {
        table: FleetTable::Histories,
        key: "history-0".into(),
        value: None,
    }];
    let updated = f
        .catalog
        .commit(&current, &removal, TIMEOUT)
        .await?
        .snapshot;
    ensure!(f
        .catalog
        .page(
            &updated,
            FleetTable::Histories,
            first_page.next.as_ref(),
            1,
            TIMEOUT
        )
        .await
        .is_err());
    ensure!(f
        .catalog
        .get(&current, FleetTable::Histories, "history-0", TIMEOUT)
        .await?
        .is_some());
    ensure!(f
        .catalog
        .get(&updated, FleetTable::Histories, "history-0", TIMEOUT)
        .await?
        .is_none());
    f.close().await;
    Ok(())
}

#[tokio::test]
async fn fleet_missing_root_never_resets_a_pinned_fleet() -> Result<()> {
    let f = Fixture::new().await?;
    f.store
        .inner
        .delete(&Path::from(Fixture::key("head")))
        .await?;
    ensure!(matches!(
        f.catalog.initialize(TIMEOUT).await,
        Err(FleetCatalogError::MissingRoot)
    ));
    ensure!(matches!(
        f.catalog.read(TIMEOUT).await,
        Err(FleetCatalogError::MissingRoot)
    ));
    f.close().await;
    Ok(())
}

#[tokio::test]
async fn fleet_catalog_abandoned_reply_keeps_journal_and_backpressure() -> Result<()> {
    let f = Fixture::new().await?;
    let catalog = FleetCatalog::new(
        &f.domain,
        f.bucket.clone(),
        ClusterId::new([1; 16])?,
        IncarnationId::new([2; 16])?,
        "archives/",
    )?;
    let root = catalog.initialize(TIMEOUT).await?;
    let ordinal = f.store.archive_heads_waiting.load(Ordering::Acquire);
    f.store.archive_heads_hang.store(true, Ordering::Release);
    let request = mutation(3, 1, "cancelled-waiter", b"retained");
    let (owner, submitted) = (catalog.clone(), request.clone());
    let job = f
        .domain
        .spawn(async move { owner.commit(&root, &submitted, TIMEOUT).await })?;
    f.domain
        .run(cellar::asyncrt::timeout(TIMEOUT, async {
            while f.store.archive_heads_waiting.load(Ordering::Acquire) == ordinal {
                tokio::task::yield_now().await;
            }
        }))
        .await??;
    ensure!(matches!(
        catalog.read(TIMEOUT).await,
        Err(FleetCatalogError::Overloaded)
    ));
    job.abort();
    ensure!(job.await.is_err());
    f.store.archive_heads_hang.store(false, Ordering::Release);
    f.store.archive_heads_resume.notify_waiters();
    let observed = catalog.read(TIMEOUT).await?;
    ensure!(observed.revision() == 2);
    ensure!(
        catalog
            .commit(&observed, &request, TIMEOUT)
            .await?
            .applied_revision
            == 2
    );
    f.close().await;
    Ok(())
}

#[tokio::test]
async fn fleet_catalog_closed_domain_refuses_new_io() -> Result<()> {
    let f = Fixture::new().await?;
    f.domain.close();
    ensure!(matches!(
        f.catalog.read(TIMEOUT).await,
        Err(FleetCatalogError::Closed)
    ));
    f.close().await;
    Ok(())
}
