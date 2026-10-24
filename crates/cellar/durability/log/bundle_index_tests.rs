// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Historical row-cache hints must not outgrow the retained bundle inventory.
use super::*;

fn row(epoch: u64) -> cellar_ltx::bundle::BundleRow {
    cellar_ltx::bundle::BundleRow {
        cell: "Counter:fixed".into(),
        cell_epoch: epoch,
        txid: 1,
        offset: 0,
        len: 1,
    }
}

#[test]
fn epoch_churn_keeps_eviction_markers_bounded() {
    let mut index = BundleIndex::default();
    for epoch in 1..=20_000 {
        index.insert(format!("bundle-{epoch}"), vec![row(epoch)]);
        if epoch > BUNDLE_ROW_INDEX_CAPACITY as u64 {
            index.remove(&[format!(
                "bundle-{}",
                epoch - BUNDLE_ROW_INDEX_CAPACITY as u64
            )]);
        }
        assert!(index.bundles.len() <= BUNDLE_ROW_INDEX_CAPACITY);
        assert!(index.by_cell.len() <= 1);
        assert!(index.recent.len() <= BUNDLE_ROW_INDEX_CAPACITY);
    }
    assert!(
        index.evicted.len() <= BUNDLE_ROW_INDEX_CAPACITY,
        "fixed live inventory retained {} historical epoch markers",
        index.evicted.len()
    );
}

#[test]
fn complete_bundle_gc_releases_eviction_markers() {
    let mut index = BundleIndex::default();
    for epoch in 1..=BUNDLE_ROW_INDEX_CAPACITY as u64 + 1 {
        index.insert(format!("bundle-{epoch}"), vec![row(epoch)]);
    }
    assert_eq!(index.evicted.len(), 1);
    let keys = index.bundles.keys().cloned().collect::<Vec<_>>();
    index.remove(&keys);
    assert!(index.bundles.is_empty());
    assert!(index.by_cell.is_empty());
    assert!(index.recent.is_empty());
    assert_eq!(index.evicted.len(), 0);
}

#[test]
fn later_lower_row_cannot_understate_a_retained_eviction_cut() {
    let mut index = BundleIndex::default();
    let mut high = row(1);
    high.cell = "Counter:slow".into();
    high.txid = 100;
    index.insert("slow-high".into(), vec![high.clone()]);
    for epoch in 1..=8192 {
        index.insert(format!("busy-{epoch}"), vec![row(epoch)]);
    }
    let mut low = high;
    low.txid = 50;
    index.insert("slow-low".into(), vec![low]);
    for number in 0..BUNDLE_ROW_INDEX_CAPACITY {
        index.insert(format!("flush-{number}"), vec![row(u64::MAX)]);
    }
    let view = index.cell_rows("Counter:slow", 1);
    assert!(view.located.is_empty());
    assert!(view.evicted.contains(&"slow-high".into()));
    assert!(view.evicted.contains(&"slow-low".into()));
    assert!(
        view.evicted_through.is_some_and(|through| through >= 100),
        "retained txid 100 was hidden by an understated cut {:?}",
        view.evicted_through
    );
}

#[test]
fn gc_releases_distinct_epoch_membership_once_per_evicted_bundle() {
    let mut index = BundleIndex::default();
    let mut second = row(1);
    second.txid = 2;
    index.insert("mixed".into(), vec![row(1), second, row(2)]);
    index.insert("shared".into(), vec![row(1)]);
    for number in 0..BUNDLE_ROW_INDEX_CAPACITY {
        index.insert(format!("flush-{number}"), vec![row(u64::MAX)]);
    }
    let first_epoch = ("Counter:fixed".into(), 1);
    let second_epoch = ("Counter:fixed".into(), 2);
    assert_eq!(index.evicted[&first_epoch].bundles, 2);
    assert_eq!(index.evicted[&second_epoch].bundles, 1);

    // A bundle whose rows are still cached owns no eviction-cut reference.
    index.insert("recent".into(), vec![row(1)]);
    index.remove(&["recent".into()]);
    assert_eq!(index.evicted[&first_epoch].bundles, 2);
    index.remove(&["mixed".into()]);
    assert_eq!(index.evicted[&first_epoch].bundles, 1);
    assert!(!index.evicted.contains_key(&second_epoch));
    assert!(index.evicted[&first_epoch].through >= 1);
    index.remove(&["shared".into()]);
    assert!(!index.evicted.contains_key(&first_epoch));
    assert!(index.by_cell.contains_key("Counter:fixed"));
}

struct NoPeers;

impl LogTransport for NoPeers {
    fn post<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
        _: &'a str,
        _: Vec<u8>,
        _: Option<std::time::Duration>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<bytes::Bytes>> + Send + 'a>,
    > {
        Box::pin(async { anyhow::bail!("row-cache fallback must not require a peer witness") })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn evicted_row_metadata_reloads_persisted_rows_and_rejects_corruption() -> anyhow::Result<()>
{
    use crate::sdk::{
        ExecutionDomain, ReplicaStorage, ReplicaStorageOptions, Replication, ReplicationOptions,
    };
    use cellar_ltx::{bundle, ltx, TXID};

    let directory = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let checked = domain
        .run(async {
            let bucket = Arc::new(crate::bucket::Bucket::open_dev(
                &directory.path().join("objects.sqlite"),
            )?);
            let ownership = Arc::new(crate::ownership_store::BucketOwnership::new(
                (*bucket).clone(),
                (*bucket).clone(),
                "bundle-index".into(),
                "bundle-index-generation".into(),
            ));
            ownership.prepare_lineage().await?;
            let replica = Replication::start(
                &directory.path().join("replicas"),
                ReplicaStorage::new(
                    bucket.store.clone(),
                    ReplicaStorageOptions {
                        prefix: bucket.prefix.clone(),
                        ..Default::default()
                    },
                )?,
                ReplicationOptions {
                    compaction: None,
                    ..Default::default()
                },
            )?;
            let manager = Arc::new(NodeLogManager::new(
                &ownership.log_session()?,
                bucket.clone(),
                Arc::new(OwnLog {
                    ownership,
                    nudge: Box::new(|| {}),
                    write_lock: tokio::sync::Mutex::new(()),
                }),
                replica.ltx(),
                Arc::new(NoPeers),
                LogOptions::default(),
            )?);
            let mut owner = DurabilityOwner::new(manager.clone(), false);
            let result = async {
                let header = ltx::Header {
                    version: ltx::VERSION,
                    flags: ltx::HEADER_FLAG_NO_CHECKSUM,
                    page_size: 4096,
                    commit: 1,
                    min_txid: TXID(1),
                    max_txid: TXID(1),
                    ..Default::default()
                };
                let body = bundle::encode(&[bundle::BundleEntry {
                    cell: "Counter:slow".into(),
                    cell_epoch: 1,
                    txid: 1,
                    bytes: ltx::encode_file(&header, &[(1, vec![0; 4096])], 0)?,
                }])?;
                let key = "bundle-index/slow";
                bucket.put(key, body.clone()).await?;
                {
                    let mut index = manager.bundle_index.lock().unwrap();
                    index.insert(key.into(), bundle::decode_rows(&body)?);
                    for epoch in 2..=BUNDLE_ROW_INDEX_CAPACITY as u64 + 1 {
                        index.insert(format!("filler-{epoch}"), vec![row(epoch)]);
                    }
                    // Row metadata left the cache, but its retained bundle
                    // still owns the cutoff needed to find the persisted row.
                    let view = index.cell_rows("Counter:slow", 1);
                    anyhow::ensure!(view.located.is_empty());
                    anyhow::ensure!(view.evicted == vec![key.to_string()]);
                    anyhow::ensure!(view.evicted_through == Some(1));
                }
                let rows = manager.rows_for("Counter:slow", 1).await?;
                anyhow::ensure!(rows.len() == 1, "row-cache eviction hid a retained row");
                anyhow::ensure!(rows[0].source == key);
                anyhow::ensure!(rows[0].row.cell == "Counter:slow");
                anyhow::ensure!(rows[0].row.cell_epoch == 1 && rows[0].row.txid == 1);
                bucket.put(key, b"corrupt bundle".to_vec()).await?;
                anyhow::ensure!(
                    manager.rows_for("Counter:slow", 1).await.is_err(),
                    "a failed authoritative read must not become an empty overlay"
                );
                bucket.put(key, body).await?;
                anyhow::ensure!(manager.rows_for("Counter:slow", 1).await?.len() == 1);
                anyhow::Ok(())
            }
            .await;
            owner.shutdown_local().await;
            result
        })
        .await;
    domain.shutdown().await;
    checked??;
    Ok(())
}
