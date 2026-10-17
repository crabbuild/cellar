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
        let header = ltx::Header {
            version: ltx::VERSION,
            flags: ltx::HEADER_FLAG_NO_CHECKSUM,
            page_size: 512,
            commit: 1,
            min_txid: TXID(1),
            max_txid: TXID(1),
            ..Default::default()
        };
        let entry = BundleEntry {
            cell: format!("cell-{index}"),
            cell_epoch: 1,
            txid: 1,
            bytes: ltx::encode_file(&header, &[(1, vec![index as u8; 512])], 0)?,
        };
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
