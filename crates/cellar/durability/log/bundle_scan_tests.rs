//! The production close/fold gather must not retain every bundle's body.
use super::*;
use crate::sdk::{
    ExecutionDomain, ReplicaStorage, ReplicaStorageOptions, Replication, ReplicationOptions,
};
use cellar_ltx::{
    bundle::{self, BundleEntry},
    ltx, DirectFileSystem, FileSystem,
};
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicBool, AtomicU64};

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
        Box::pin(async { anyhow::bail!("bundle inventory must not manufacture a peer witness") })
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_bundle_scan_spills_original_rows_under_its_memory_budget() -> anyhow::Result<()>
{
    profile(
        1,
        "production_bundle_scan_spills_original_rows_under_its_memory_budget",
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_production_bundle_scans_keep_one_node_memory_bound() -> anyhow::Result<()> {
    profile(
        2,
        "concurrent_production_bundle_scans_keep_one_node_memory_bound",
    )
    .await
}

async fn profile(scans: usize, test: &str) -> anyhow::Result<()> {
    const CHILD: &str = "CELLAR_TEST_BUNDLE_SCAN_PATH";
    let Some(database) = std::env::var_os(CHILD) else {
        let directory = tempfile::tempdir()?;
        let database = directory.path().join("objects.sqlite");
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let prepared = domain
            .run(async {
                let bucket = crate::bucket::Bucket::open_dev(&database)?;
                let ownership = crate::ownership_store::BucketOwnership::new(
                    bucket.clone(),
                    bucket.clone(),
                    "bundle-memory".into(),
                    "bundle-generation".into(),
                );
                let lineage = ownership.prepare_lineage().await?.to_owned();
                // prepare_lineage alone captures but does not publish a node
                // root. Persist it through the real CAS so the measured child
                // resolves the same session without copying any object bodies.
                let record = cellar_logic::NodeLeaseRecord {
                    node: "bundle-memory".into(),
                    lineage: Some(lineage),
                    addr: "127.0.0.1:1".into(),
                    expires_ms: crate::ownership_store::now_ms() + 60_000,
                    peer_protocol: crate::peer_auth::PROTOCOL_VERSION,
                    generation: "bundle-generation".into(),
                    log_state: None,
                    etag: String::new(),
                };
                let mut stamped = None;
                anyhow::ensure!(
                    matches!(
                        ownership
                            .cas_node_lease(cellar_logic::CasGuard::Absent, &record, &mut stamped)
                            .await
                            .map_err(|error| error.into_error())?,
                        cellar_logic::LeaseCasOutcome::Applied { .. }
                    ),
                    "fixture node root was not persisted"
                );
                let session = ownership.log_session()?;
                let mut seed = 17_u64;
                let mut pages = Vec::new();
                for pgno in 1..=1024 {
                    let mut bytes = vec![0; 4096];
                    for byte in &mut bytes {
                        seed ^= seed << 13;
                        seed ^= seed >> 7;
                        seed ^= seed << 17;
                        *byte = seed as u8;
                    }
                    pages.push((pgno, bytes));
                }
                let mut manifest = BTreeMap::new();
                for txid in 2..=33_u64 {
                    let header = ltx::Header {
                        version: ltx::VERSION,
                        flags: ltx::HEADER_FLAG_NO_CHECKSUM,
                        page_size: 4096,
                        commit: 1024,
                        min_txid: cellar_ltx::TXID(txid),
                        max_txid: cellar_ltx::TXID(txid),
                        ..Default::default()
                    };
                    let payload = ltx::encode_file(&header, &pages, 0)?;
                    anyhow::ensure!(
                        payload.len() >= 4 << 20,
                        "fixture compressed below its memory target"
                    );
                    manifest.insert(txid, (payload.len(), hex::encode(Sha256::digest(&payload))));
                    let entries = if scans == 1 {
                        vec![BundleEntry {
                            cell: "cell".into(),
                            cell_epoch: 1,
                            txid,
                            bytes: payload,
                        }]
                    } else {
                        vec![
                            BundleEntry {
                                cell: "cell-a".into(),
                                cell_epoch: 1,
                                txid,
                                bytes: payload.clone(),
                            },
                            BundleEntry {
                                cell: "cell-b".into(),
                                cell_epoch: 1,
                                txid,
                                bytes: payload,
                            },
                        ]
                    };
                    let body = bundle::encode(&entries)?;
                    bucket
                        .put(&format!("log/{session}/bundle/{txid:020}"), body)
                        .await?;
                }
                DirectFileSystem.write(
                    &database.with_extension("manifest"),
                    &serde_json::to_vec(&manifest)?,
                )?;
                anyhow::Ok(())
            })
            .await;
        domain.shutdown().await;
        prepared??;
        let name = format!(
            "{}::{test}",
            module_path!().split_once("::").expect("test module").1
        );
        let output = std::process::Command::new(std::env::current_exe()?)
            .args(["--exact", &name, "--nocapture", "--test-threads=1"])
            .env(CHILD, database)
            .output()?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        eprintln!("{stderr}");
        anyhow::ensure!(
            output.status.success() && stdout.contains("1 passed; 0 failed"),
            "production bundle profile failed: {stdout}\n{stderr}"
        );
        return Ok(());
    };
    let database = std::path::PathBuf::from(database);
    let manifest: BTreeMap<u64, (usize, String)> =
        serde_json::from_slice(&DirectFileSystem.read(&database.with_extension("manifest"))?)?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let watch = tempfile::tempdir()?;
    let prepared = domain
        .run(async {
            let bucket = Arc::new(crate::bucket::Bucket::open_dev(&database)?);
            let ownership = Arc::new(crate::ownership_store::BucketOwnership::new(
                (*bucket).clone(),
                (*bucket).clone(),
                "bundle-memory".into(),
                "bundle-generation".into(),
            ));
            ownership.prepare_lineage().await?;
            let replica = Replication::start(
                watch.path(),
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
                bucket,
                Arc::new(OwnLog {
                    ownership,
                    nudge: Box::new(|| {}),
                    write_lock: tokio::sync::Mutex::new(()),
                }),
                replica.ltx(),
                Arc::new(NoPeers),
                LogOptions::default(),
            )?);
            if scans > 1 {
                // Match the live writer's complete cell-to-bundle membership.
                // Scalar descriptors come from the exact parent-built layout;
                // no body is read into this child's RSS baseline. Each actual
                // fold must still verify the whole mixed-cell envelope.
                for (txid, (length, _)) in &manifest {
                    let rows = ["cell-a", "cell-b"]
                        .into_iter()
                        .enumerate()
                        .map(|(ordinal, cell)| bundle::BundleRow {
                            cell: cell.into(),
                            cell_epoch: 1,
                            txid: *txid,
                            offset: ordinal as u64 * *length as u64,
                            len: *length as u64,
                        })
                        .collect();
                    manager
                        .bundle_index
                        .lock()
                        .unwrap()
                        .insert(format!("log/{}/bundle/{txid:020}", manager.session), rows);
                }
            }
            let owner = DurabilityOwner::new(manager.clone(), false);
            anyhow::Ok((manager, owner, replica))
        })
        .await;
    let prepared = prepared
        .map_err(anyhow::Error::from)
        .and_then(|result| result);
    if prepared.is_err() {
        domain.shutdown().await;
    }
    let (manager, mut owner, replica) = prepared?;
    let baseline = crate::memory::sample().rss_bytes;
    let peak = Arc::new(AtomicU64::new(baseline));
    let stop = Arc::new(AtomicBool::new(false));
    let (sampled_peak, stopped) = (peak.clone(), stop.clone());
    // This caller-owned sampler runs outside the measured execution domain and
    // is always stopped/joined, including when the scan or proof checks fail.
    #[allow(clippy::disallowed_methods)]
    let sampler = tokio::spawn(async move {
        while !stopped.load(Ordering::Relaxed) {
            sampled_peak.fetch_max(crate::memory::sample().rss_bytes, Ordering::Relaxed);
            #[allow(clippy::disallowed_methods)]
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    });
    let result = domain
        .run(async {
            // Poll both real scans together and finish every original row
            // check. A failure cannot cancel the other scan's owned I/O.
            let results = futures_util::future::join_all((0..scans).map(|ordinal| {
                let expected_cell = if scans == 1 {
                    "cell"
                } else if ordinal == 0 {
                    "cell-a"
                } else {
                    "cell-b"
                };
                let only_cell = (scans > 1).then_some(expected_cell);
                let manager = &manager;
                let manifest = &manifest;
                async move {
                    let rows = manager
                        .gather_uncovered_bundles(only_cell, manager.recovery_rows())
                        .await?;
                    anyhow::ensure!(!rows.is_empty());
                    let mut rows = manager
                        .recovery_io(rows.cancellation(), None, move || rows.finish())
                        .await?;
                    let mut expected = 2_u64;
                    loop {
                        let (returned, batch) = manager
                            .recovery_io(rows.cancellation(), None, move || {
                                let batch = rows.next_batch()?;
                                anyhow::Ok((rows, batch))
                            })
                            .await?;
                        rows = returned;
                        if batch.is_empty() {
                            break;
                        }
                        for ((cell, epoch, txid), bytes) in batch {
                            anyhow::ensure!(
                                cell == expected_cell && epoch == 1 && txid == expected
                            );
                            let (length, digest) =
                                manifest.get(&txid).context("unexpected original row")?;
                            anyhow::ensure!(
                                bytes.len() == *length
                                    && hex::encode(Sha256::digest(&bytes)) == *digest
                            );
                            expected += 1;
                        }
                    }
                    anyhow::ensure!(
                        expected == 34 && manifest.len() == 32,
                        "original bundle row missing"
                    );
                    anyhow::Ok(())
                }
            }))
            .await;
            for result in results {
                result?;
            }
            anyhow::Ok(())
        })
        .await;
    stop.store(true, Ordering::Relaxed);
    let sampled = sampler.await;
    let closed = domain.run(owner.shutdown_local()).await;
    domain.shutdown().await;
    drop(replica);
    sampled?;
    closed?;
    result??;
    let growth = peak.load(Ordering::Relaxed).saturating_sub(baseline);
    eprintln!("production bundle scan RSS: scans={scans} baseline={baseline} growth={growth}");
    anyhow::ensure!(
        baseline > 0 && growth <= 64 << 20,
        "bundle scan retained history: {growth} > {}",
        64 << 20
    );
    Ok(())
}
