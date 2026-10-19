use super::super::{log_to_wire, read_record, DurabilityOwner, LogState, OwnLog, RecoverMode};
use super::*;
use crate::{
    bucket::Bucket,
    ownership_store::BucketOwnership,
    sdk::{
        ExecutionDomain, ReplicaStorage, ReplicaStorageOptions, Replication, ReplicationOptions,
    },
};
use cellar_logic::{log_tier, CasGuard, LeaseCasOutcome, NodeLeaseRecord};
use cellar_ltx::{
    bundle::{self, BundleEntry},
    ltx, ReplicaClient, TXID,
};
use std::{path::Path, sync::Arc, time::Duration};

struct NoPeers;

impl super::super::LogTransport for NoPeers {
    fn post<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
        _: &'a str,
        _: Vec<u8>,
        _: Option<Duration>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<bytes::Bytes>> + Send + 'a>>
    {
        Box::pin(async { anyhow::bail!("retained-bundle fixture has no follower proof") })
    }
}

struct Fixture {
    manager: Arc<NodeLogManager>,
    owner: DurabilityOwner,
    _replica: Replication,
    dead: String,
    keys: Vec<String>,
    entries: Vec<BundleEntry>,
}

async fn fixture(directory: &Path) -> Result<Fixture> {
    let bucket = Arc::new(Bucket::open_dev(&directory.join("objects.sqlite"))?);
    fixture_in(directory, bucket).await
}

#[derive(Clone, Copy)]
enum UploadSize {
    Single,
    Multipart,
}

async fn fixture_in(directory: &Path, bucket: Arc<Bucket>) -> Result<Fixture> {
    fixture_with_size(directory, bucket, UploadSize::Single).await
}

async fn fixture_with_size(
    directory: &Path,
    bucket: Arc<Bucket>,
    size: UploadSize,
) -> Result<Fixture> {
    let original = BucketOwnership::new(
        (*bucket).clone(),
        (*bucket).clone(),
        "bundle-recovery".into(),
        "original".into(),
    );
    let lineage = original.prepare_lineage().await?.to_owned();
    original.set_own_log(Some(log_to_wire(
        &log_tier::LogRecord {
            epoch: 1,
            ensemble: Default::default(),
            tiered: 0,
            state: LogState::Open,
            claimant: None,
            claimed_ms: None,
        },
        false,
    )));
    let record = NodeLeaseRecord {
        node: "bundle-recovery".into(),
        lineage: Some(lineage),
        addr: "127.0.0.1:1".into(),
        expires_ms: 0,
        peer_protocol: crate::peer_auth::PROTOCOL_VERSION,
        generation: "original".into(),
        log_state: None,
        etag: String::new(),
    };
    let mut stamped = None;
    anyhow::ensure!(matches!(
        original
            .cas_node_lease(CasGuard::Absent, &record, &mut stamped)
            .await
            .map_err(|error| error.into_error())?,
        LeaseCasOutcome::Applied { .. }
    ));
    let dead = original.log_session()?;
    let mut keys = Vec::new();
    let mut entries = Vec::new();
    for index in 0..2 {
        let (page_size, pages) = match size {
            UploadSize::Single => (512, vec![(1, vec![index as u8; 512])]),
            UploadSize::Multipart => {
                let mut seed = 17_u64 + index as u64;
                let mut pages = Vec::new();
                for pgno in 1..=1536 {
                    let mut bytes = vec![0; 4096];
                    for byte in &mut bytes {
                        seed ^= seed << 13;
                        seed ^= seed >> 7;
                        seed ^= seed << 17;
                        *byte = seed as u8;
                    }
                    pages.push((pgno, bytes));
                }
                (4096, pages)
            }
        };
        let header = ltx::Header {
            version: ltx::VERSION,
            flags: ltx::HEADER_FLAG_NO_CHECKSUM,
            page_size,
            commit: u32::try_from(pages.len())?,
            min_txid: TXID(1),
            max_txid: TXID(1),
            ..Default::default()
        };
        let entry = BundleEntry {
            cell: format!("cell-{index}"),
            cell_epoch: 1,
            txid: 1,
            bytes: ltx::encode_file(&header, &pages, 0)?,
        };
        let multipart = entry.bytes.len() >= cellar_ltx::client::object_store::MULTIPART_THRESHOLD;
        anyhow::ensure!(
            multipart == matches!(size, UploadSize::Multipart),
            "fixture did not enter the selected upload path"
        );
        let key = format!("log/{dead}/bundle/e1-{index}.ltxb");
        bucket
            .put(&key, bundle::encode(std::slice::from_ref(&entry))?)
            .await?;
        keys.push(key);
        entries.push(entry);
    }
    let ownership = Arc::new(BucketOwnership::new(
        (*bucket).clone(),
        (*bucket).clone(),
        "bundle-recovery".into(),
        "recoverer".into(),
    ));
    ownership.prepare_lineage().await?;
    let replica = Replication::start(
        &directory.join("recoverer"),
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
        crate::log_options::LogOptions::default(),
    )?);
    let owner = DurabilityOwner::new(manager.clone(), false);
    Ok(Fixture {
        manager,
        owner,
        _replica: replica,
        dead,
        keys,
        entries,
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn corrupt_bundle_refuses_the_whole_recovery_window_until_original_bytes_return() -> Result<()>
{
    let directory = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let outcome = domain
        .run(async {
            let mut fixture = fixture(directory.path()).await?;
            let manager = &fixture.manager;
            let outcome = async {
                let original = manager
                    .bucket
                    .get(&fixture.keys[1])
                    .await?
                    .context("original bundle")?
                    .0;
                let mut damaged = original.to_vec();
                damaged[ltx::HEADER_SIZE + 1] ^= 1;
                manager
                    .bucket
                    .put(&fixture.keys[1], damaged.clone())
                    .await?;
                let storage = ReplicaStorage::new(
                    manager.bucket.store.clone(),
                    ReplicaStorageOptions {
                        prefix: manager.bucket.prefix.clone(),
                        ..Default::default()
                    },
                )?;
                for _ in 0..2 {
                    let error = manager
                        .recover_as(&fixture.dead, RecoverMode::Boot)
                        .await
                        .unwrap_err();
                    anyhow::ensure!(
                        format!("{error:#}").contains("bundle checksum mismatch"),
                        "wrong refusal: {error:#}"
                    );
                    let retained = read_record(&manager.bucket, &fixture.dead)
                        .await?
                        .context("retained root")?;
                    anyhow::ensure!(
                        retained.record.state == LogState::Recovering
                            && retained.record.tiered == 0
                    );
                    anyhow::ensure!(!retained.may_have_fleet_acks && retained.wire.expires_ms == 0);
                    for entry in &fixture.entries {
                        anyhow::ensure!(
                            storage
                                .client_for(&entry.cell, 1)
                                .ltx_files(0, TXID(0))
                                .await?
                                .is_empty(),
                            "a partial window advanced cell coverage before every bundle verified"
                        );
                    }
                    anyhow::ensure!(manager
                        .bucket
                        .list(&format!("log/{}/recovered/", fixture.dead))
                        .await?
                        .is_empty());
                    anyhow::ensure!(
                        manager
                            .bucket
                            .get(&fixture.keys[1])
                            .await?
                            .context("retained damaged bundle")?
                            .0
                            .as_ref()
                            == damaged
                    );
                }
                manager
                    .bucket
                    .put(&fixture.keys[1], original.to_vec())
                    .await?;
                manager.recover_as(&fixture.dead, RecoverMode::Boot).await?;
                let sealed = read_record(&manager.bucket, &fixture.dead)
                    .await?
                    .context("sealed root")?;
                anyhow::ensure!(
                    sealed.record.state == LogState::Sealed && sealed.wire.expires_ms == 0
                );
                for entry in &fixture.entries {
                    let bytes = storage
                        .client_for(&entry.cell, 1)
                        .open_ltx_file(0, TXID(1), TXID(1))
                        .await?;
                    anyhow::ensure!(
                        bytes.as_slice() == entry.bytes.as_slice(),
                        "recovered original row changed"
                    );
                }
                Ok(())
            }
            .await;
            fixture.owner.shutdown_local().await;
            outcome
        })
        .await;
    domain.shutdown().await;
    outcome?
}

#[cfg(not(cellar_internal_tests))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_waiting_for_node_scan_admission_keeps_its_claim_live() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let prepared = domain.run(fixture(directory.path())).await;
    let prepared = prepared
        .map_err(anyhow::Error::from)
        .and_then(|result| result);
    if prepared.is_err() {
        domain.shutdown().await;
    }
    let mut fixture = prepared?;
    let reservation = fixture.manager.bundle_scan_budget.acquire().await?;
    let manager = fixture.manager.clone();
    let dead = fixture.dead.clone();
    let mut task =
        match domain.spawn(async move { manager.recover_as(&dead, RecoverMode::Boot).await }) {
            Ok(task) => task,
            Err(error) => {
                drop(reservation);
                let stopped = domain.run(fixture.owner.shutdown_local()).await;
                domain.shutdown().await;
                stopped?;
                return Err(error.into());
            }
        };
    // This observer runs outside the recovery domain. The production heartbeat
    // remains five seconds; readiness is the persisted claim, not a sleep.
    #[allow(clippy::disallowed_methods)]
    let observed = tokio::time::timeout(Duration::from_secs(12), async {
        let mut first = None;
        loop {
            let read = domain
                .run(read_record(&fixture.manager.bucket, &fixture.dead))
                .await??
                .context("claimed root disappeared")?;
            if read.record.state == LogState::Recovering {
                let claimed = read.record.claimed_ms.context("claim heartbeat missing")?;
                if first.is_some_and(|first| claimed > first) {
                    return anyhow::Ok(());
                }
                first.get_or_insert(claimed);
            }
            anyhow::ensure!(
                read.record.state != LogState::Sealed,
                "recovery bypassed node scan admission"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    drop(reservation);
    #[allow(clippy::disallowed_methods)]
    let joined = tokio::time::timeout(Duration::from_secs(10), &mut task).await;
    if joined.is_err() {
        task.abort();
        let _ = task.await;
    }
    let stopped = domain.run(fixture.owner.shutdown_local()).await;
    domain.shutdown().await;
    stopped?;
    observed??;
    joined???;
    Ok(())
}

#[cfg(not(cellar_internal_tests))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_recovery_retains_admission_until_entered_sqlite_upload_joins() -> Result<()> {
    cancelled_upload(UploadSize::Single).await
}

#[cfg(not(cellar_internal_tests))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_multipart_recovery_retains_admission_until_entered_sqlite_upload_joins(
) -> Result<()> {
    cancelled_upload(UploadSize::Multipart).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multipart_recovery_preserves_original_rows_metadata_and_releases_admission() -> Result<()>
{
    use futures_util::FutureExt as _;
    let directory = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let outcome = domain
        .run(async {
            let bucket = Arc::new(Bucket::open_dev(&directory.path().join("objects.sqlite"))?);
            let mut fixture =
                fixture_with_size(directory.path(), bucket, UploadSize::Multipart).await?;
            let outcome = async {
                let manager = &fixture.manager;
                manager.recover_as(&fixture.dead, RecoverMode::Boot).await?;
                let sealed = read_record(&manager.bucket, &fixture.dead)
                    .await?
                    .context("multipart recovery root")?;
                anyhow::ensure!(
                    sealed.record.state == LogState::Sealed
                        && sealed.wire.expires_ms == 0
                        && sealed.wire.generation == "original"
                        && Some(sealed.wire.lineage.as_str()) == fixture.dead.split('/').nth(1)
                        && !sealed.may_have_fleet_acks
                );
                let storage = ReplicaStorage::new(
                    manager.bucket.store.clone(),
                    ReplicaStorageOptions {
                        prefix: manager.bucket.prefix.clone(),
                        ..Default::default()
                    },
                )?;
                for entry in &fixture.entries {
                    let client = storage.client_for(&entry.cell, entry.cell_epoch);
                    let files = client.ltx_files(0, TXID(0)).await?;
                    anyhow::ensure!(
                        files.len() == 1
                            && files[0].min_txid == TXID(1)
                            && files[0].max_txid == TXID(1)
                            && files[0].size == entry.bytes.len() as i64,
                        "multipart recovery changed file identity or size"
                    );
                    let actual = client.open_ltx_file(0, TXID(1), TXID(1)).await?;
                    anyhow::ensure!(
                        actual == entry.bytes,
                        "multipart recovery changed original bytes"
                    );
                }
                let objects = manager.bucket.list("cells/").await?;
                anyhow::ensure!(objects.len() == fixture.entries.len());
                for object in objects {
                    let response = manager.bucket.store.get(&object.location).await?;
                    anyhow::ensure!(
                        response
                            .attributes
                            .get(&object_store::Attribute::Metadata(
                                "litestream-timestamp".into()
                            ))
                            .map(AsRef::<str>::as_ref)
                            == Some("1970-01-01T00:00:00Z"),
                        "multipart timestamp metadata changed"
                    );
                }
                let admitted = manager
                    .bundle_scan_budget
                    .acquire()
                    .now_or_never()
                    .context("completed multipart recovery leaked scan admission")??;
                drop(admitted);
                Ok(())
            }
            .await;
            fixture.owner.shutdown_local().await;
            outcome
        })
        .await;
    domain.shutdown().await;
    outcome?
}

#[cfg(not(cellar_internal_tests))]
async fn cancelled_upload(size: UploadSize) -> Result<()> {
    use futures_util::FutureExt as _;
    let directory = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let prepared = domain
        .run(async {
            let database = directory.path().join("objects.sqlite");
            let (store, gate) =
                crate::local_store::held_upload_store(&database, &domain, "cells/")?;
            let mut bucket = Bucket::open_dev(&database)?;
            bucket.store = store.clone();
            bucket.cas_store = store;
            let fixture = fixture_with_size(directory.path(), Arc::new(bucket), size).await?;
            Ok::<_, anyhow::Error>((fixture, gate))
        })
        .await;
    let prepared = prepared
        .map_err(anyhow::Error::from)
        .and_then(|result| result);
    if prepared.is_err() {
        domain.shutdown().await;
    }
    let (mut fixture, gate): (Fixture, crate::local_store::UploadGate) = prepared?;
    let manager = fixture.manager.clone();
    let dead = fixture.dead.clone();
    let task = match domain.spawn(async move { manager.recover_as(&dead, RecoverMode::Boot).await })
    {
        Ok(task) => task,
        Err(error) => {
            gate.release();
            let stopped = domain.run(fixture.owner.shutdown_local()).await;
            domain.shutdown().await;
            stopped?;
            return Err(error.into());
        }
    };
    #[allow(clippy::disallowed_methods)]
    let entered = tokio::time::timeout(Duration::from_secs(5), gate.entered()).await;
    task.abort();
    let cancelled = task.await;
    let mut waiting = Box::pin(fixture.manager.bundle_scan_budget.acquire());
    let observed = waiting.as_mut().now_or_never();
    let reserved = observed.is_none();
    drop(observed);
    fixture.owner.stop_local_now();
    #[allow(clippy::disallowed_methods)]
    let stopped = tokio::time::timeout(
        Duration::from_secs(2),
        domain.run(fixture.owner.shutdown_local()),
    )
    .await;
    domain.close();
    let mut shutdown = Box::pin(domain.shutdown());
    let pending = shutdown.as_mut().now_or_never().is_none();
    gate.release();
    shutdown.await;
    let admitted_after_join = if reserved {
        #[allow(clippy::disallowed_methods)]
        let granted = tokio::time::timeout(Duration::from_secs(2), waiting).await;
        granted.map(|result| result.map(drop))
    } else {
        Ok(Ok(()))
    };
    let audit = ExecutionDomain::new(tokio::runtime::Handle::current());
    let retained = audit
        .run(read_record(&fixture.manager.bucket, &fixture.dead))
        .await;
    audit.shutdown().await;
    entered?;
    stopped??;
    anyhow::ensure!(
        cancelled.is_err(),
        "recovery caller did not cancel and join"
    );
    anyhow::ensure!(gate.calls() > 0, "no real SQLite upload entered");
    anyhow::ensure!(
        pending,
        "domain shutdown completed before its entered SQLite worker joined"
    );
    let retained = retained??.context("cancelled recovery lost its root")?;
    anyhow::ensure!(
        retained.record.state == LogState::Recovering
            && retained.wire.expires_ms == 0
            && retained.wire.generation == "original"
            && Some(retained.wire.lineage.as_str()) == fixture.dead.split('/').nth(1)
            && !retained.may_have_fleet_acks,
        "cancelled upload authorized a seal or changed the original root identity"
    );
    admitted_after_join??;
    anyhow::ensure!(reserved, "cancelled recovery returned scan admission while an entered SQLite upload still held its body");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovery_upload_keeps_small_cell_histories_together_and_restores_every_commit(
) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let outcome = domain
        .run(async {
            let mut fixture = fixture(directory.path()).await?;
            let manager = &fixture.manager;
            let outcome = async {
                let rows = manager.recovery_rows();
                let root = directory.path().to_owned();
                let (rows, positions) = crate::asyncrt::blocking(move || {
                    let mut rows = rows;
                    let mut positions = Vec::new();
                    for index in 0..3 {
                        let cell = format!("upload-history-{index}");
                        let path = root.join(format!("source-{index}.sqlite"));
                        let mut db = cellar_ltx::Db::open(&path)?;
                        db.min_checkpoint_page_n = u32::MAX;
                        db.truncate_page_n = 0;
                        db.checkpoint_interval = Duration::ZERO;
                        let writer = rusqlite::Connection::open(&path)?;
                        writer.execute_batch(
                            "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (0)",
                        )?;
                        for value in 1..=256 {
                            writer.execute("UPDATE counter SET value = ?1", [value])?;
                            db.capture()?;
                        }
                        let position = db.pos()?;
                        for txid in 1..=position.txid.0 {
                            rows.insert(
                                (cell.clone(), 1, txid),
                                db.read_ltx_file(0, TXID(txid), TXID(txid))?,
                                recovery_rows::Priority::Preferred,
                                recovery_rows::Duplicates::First,
                            )?;
                        }
                        positions.push((cell, position));
                        db.close()?;
                    }
                    anyhow::Ok((rows, positions))
                })
                .await??;
                let count = manager.upload_spilled(rows, None).await?;
                assert_eq!(
                    count as u64,
                    positions.iter().map(|(_, pos)| pos.txid.0).sum::<u64>()
                );
                let storage = ReplicaStorage::new(
                    manager.bucket.store.clone(),
                    ReplicaStorageOptions {
                        prefix: manager.bucket.prefix.clone(),
                        ..Default::default()
                    },
                )?;
                for (index, (cell, position)) in positions.iter().enumerate() {
                    let client = storage.client_for(cell, 1);
                    let files = client.ltx_files(0, TXID(0)).await?;
                    assert_eq!(
                        files.len(),
                        1,
                        "small contiguous history was split into avoidable uploads"
                    );
                    let restored = directory.path().join(format!("restored-{index}.sqlite"));
                    cellar_ltx::restore(&client, &restored, position.txid).await?;
                    let value = crate::asyncrt::blocking(move || {
                        let connection = rusqlite::Connection::open(restored)?;
                        anyhow::Ok(connection.query_row(
                            "SELECT value FROM counter",
                            [],
                            |row| row.get::<_, i64>(0),
                        )?)
                    })
                    .await??;
                    assert_eq!(value, 256, "recovery omitted committed state");
                }
                Ok(())
            }
            .await;
            fixture.owner.shutdown_local().await;
            outcome
        })
        .await;
    domain.shutdown().await;
    outcome?
}
