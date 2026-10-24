// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Missing proof blocks recovery; a cold, retained witness can later repair it.

use super::*;
use std::sync::atomic::AtomicBool;

struct ReturningWitness {
    missing: Transport,
    retained: Transport,
    use_retained: AtomicBool,
}

impl ReturningWitness {
    fn selected(&self) -> &Transport {
        if self.use_retained.load(Ordering::Acquire) {
            &self.retained
        } else {
            &self.missing
        }
    }
}

impl LogTransport for ReturningWitness {
    fn post<'a>(
        &'a self,
        node: &'a str,
        addr: &'a str,
        path: &'a str,
        body: Vec<u8>,
        timeout: Option<Duration>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes>> + Send + 'a>> {
        self.selected().post(node, addr, path, body, timeout)
    }

    fn open_stream<'a>(
        &'a self,
        node: &'a str,
        addr: &'a str,
        path: &'a str,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<LogStreamIo>> + Send + 'a>> {
        self.selected().open_stream(node, addr, path)
    }
}

fn rows(tail: cellar::node_log::TailResp) -> Vec<(u64, String, u64, u64, Vec<u8>)> {
    tail.entries
        .into_iter()
        .map(|entry| {
            (
                entry.seq,
                entry.cell,
                entry.cell_epoch,
                entry.txid,
                entry.bytes,
            )
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn returned_cold_witness_recovers_original_state_after_recorded_refusal() -> Result<()> {
    run(false, 1).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_node_lease_blocks_takeover_until_original_metadata_returns() -> Result<()> {
    run(true, 1).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spilled_recovery_merges_older_bundle_rows_before_newer_follower_rows() -> Result<()> {
    run(false, 180).await
}

async fn run(missing_lease: bool, transactions: u64) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let original = pair(
        &directory.path().join("fleet.sqlite"),
        directory.path(),
        "witness",
        LogOptions::default(),
    )
    .await?;
    for transaction in 0..transactions {
        write(&original, 11 + transaction as i64, transaction == 0).await?;
    }
    let request = TailReq {
        leader: original.session.clone(),
    };
    let mut proven = original
        .transport
        .follower_domain
        .run(original.transport.follower.tail(&request))
        .await??;
    ensure!(proven.range.complete && !proven.entries.is_empty());
    let mut retained_bundle = None;
    if transactions > 1 {
        ensure!(
            proven.entries.len() > 128,
            "fixture did not cross gather run/batch limits"
        );
        // Publish only the original lower prefix, then legitimately truncate
        // those exact sequences. Recovery must merge older bucket rows with
        // newer followers before the first upload can raise cell coverage.
        let prefix_count = proven.entries.len() / 4;
        let entries = proven.entries[..prefix_count]
            .iter()
            .map(|entry| cellar_ltx::bundle::BundleEntry {
                cell: entry.cell.clone(),
                cell_epoch: entry.cell_epoch,
                txid: entry.txid,
                bytes: entry.bytes.clone(),
            })
            .collect::<Vec<_>>();
        let key = format!(
            "log/{}/bundle/e{}-00000000.ltxb",
            original.session, proven.range.fragment_epoch
        );
        let body = cellar_ltx::bundle::encode(&entries)?;
        original
            .domain
            .run(original.bucket.put(&key, body.clone()))
            .await??;
        let acknowledged = original
            .transport
            .follower_domain
            .run(
                original
                    .transport
                    .follower
                    .append(cellar::node_log::AppendReq {
                        leader: original.session.clone(),
                        epoch: proven.range.fragment_epoch,
                        truncate_to: proven.entries[prefix_count - 1].seq,
                        entries: vec![],
                    }),
            )
            .await?;
        ensure!(
            acknowledged.ok,
            "published prefix did not permit truncation"
        );
        proven = original
            .transport
            .follower_domain
            .run(original.transport.follower.tail(&request))
            .await??;
        ensure!(proven.range.complete && proven.range.base > 0 && proven.entries.len() > 128);
        retained_bundle = Some((key, body));
    }
    let original_range = proven.range;
    let original_rows = rows(proven);
    let bucket = original.bucket.clone();
    let session = original.session.clone();
    let follower_node = original.transport.follower_node.clone();
    let follower_lease = original.transport._follower_lease.clone();
    original.domain.shutdown().await;
    original.transport.follower_domain.shutdown().await;
    drop(original);
    std::fs::remove_dir_all(directory.path().join("leader-witness"))?;

    let recovery = ExecutionDomain::new(tokio::runtime::Handle::current());
    let peer = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
    let outcome = recovery
        .run(async {
            let objects = bucket.store.list(None).try_collect::<Vec<_>>().await?;
            ensure!(
                !objects.iter().any(|object| {
                    let key = object.location.as_ref();
                    key.starts_with(&format!("{}cells/", bucket.prefix))
                        || (key.starts_with(&format!("{}log/{session}/", bucket.prefix))
                            && !retained_bundle.as_ref().is_some_and(|(saved, _)| key == format!("{}{saved}", bucket.prefix)))
                }),
                "bucket data could mask missing follower proof"
            );
            if let Some((key, bytes)) = &retained_bundle {
                ensure!(bucket.get(key).await?.is_some_and(|(body, _)| body.as_ref() == bytes), "original prefix bundle changed");
            }
            let empty = peer
                .run(async {
                    Arc::new(FollowerStore::new(
                        &directory.path().join("empty-witness"),
                        Some(bucket.clone()),
                        &follower_node,
                    ))
                })
                .await?;
            let retained = peer
                .run(async {
                    Arc::new(FollowerStore::new(
                        &directory.path().join("follower-witness"),
                        Some(bucket.clone()),
                        &follower_node,
                    ))
                })
                .await?;
            let transport = |follower| Transport {
                owner: recovery.services(),
                follower_domain: peer.clone(),
                follower,
                follower_node: follower_node.clone(),
                _follower_lease: follower_lease.clone(),
                appends: AtomicUsize::new(0),
                streams: AtomicUsize::new(0),
            };
            let returned = Arc::new(ReturningWitness {
                missing: transport(empty),
                retained: transport(retained.clone()),
                use_retained: AtomicBool::new(missing_lease),
            });
            let replica = Replication::start(
                &directory.path().join("recovery"),
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
            let lease = Arc::new(
                BucketOwnership::new(
                    (*bucket).clone(),
                    (*bucket).clone(),
                    "recovery-witness".into(),
                    "recovery-generation-witness".into(),
                )
                .with_lease_ttl_ms(TTL),
            );
            lease.prepare_lineage().await?;
            let log = Arc::new(NodeLogManager::new(
                &lease.log_session()?,
                bucket.clone(),
                Arc::new(OwnLog {
                    ownership: lease.clone(),
                    nudge: Box::new(|| {}),
                    write_lock: tokio::sync::Mutex::new(()),
                }),
                replica.ltx(),
                returned.clone(),
                LogOptions::default(),
            )?);
            let mut owner = DurabilityOwner::new(log.clone(), false);
            let leader = session.split('/').next().unwrap();
            let previous = lease.read_node_lease(leader).await?.unwrap();
            let wait = previous
                .expires_ms
                .saturating_sub(cellar::ownership_store::now_ms());
            tokio::time::sleep(Duration::from_millis(wait + 1)).await;

            let key = format!("nodes/{leader}.json");
            let diagnostic = if missing_lease {
                let (saved, _) = bucket.get(&key).await?.context("old lease missing")?;
                let original: serde_json::Value = serde_json::from_slice(&saved)?;
                ensure!(original["log"]["may_have_fleet_acks"] == true);
                bucket.delete(&key).await?;
                ensure!(bucket.get(&key).await?.is_none());
                let gate = log.ensure_recovered(Some(leader), previous.lineage.as_deref()).await;
                let direct = log.recover(&session).await;
                ensure!(
                    gate.is_err() && direct.is_err(),
                    "missing node lease permitted takeover gate ({gate:?}) or explicit recovery ({direct:?})"
                );
                for result in [gate, direct] {
                    let error = result.unwrap_err();
                    ensure!(format!("{error:#}").contains("missing node lease"));
                }
                ensure!(bucket.get(&key).await?.is_none(), "refusal fabricated metadata");
                ensure!(
                    replica.restore_snapshot(CELL).await?.is_none(),
                    "missing metadata exposed bucket state as a complete restore"
                );
                let cold = peer.run(retained.tail(&request)).await??;
                ensure!(
                    cold.range == original_range && rows(cold) == original_rows,
                    "metadata refusal changed retained follower proof"
                );
                ensure!(
                    bucket.put_cas(&key, saved.to_vec(), None).await?.is_some(),
                    "a concurrent node replaced the absent record"
                );
                None
            } else {
                let refusal = log
                    .recover(&session)
                    .await
                    .expect_err("empty disk certified missing proof");
                ensure!(format!("{refusal:#}").contains("no complete true witness"));
                let (body, _) = bucket.get(&key).await?.unwrap();
                let blocked: serde_json::Value = serde_json::from_slice(&body)?;
                ensure!(
                    blocked["generation"] == previous.generation
                        && blocked["log"]["state"] == "recovering"
                );
                let epoch = blocked["log"]["epoch"].as_u64().unwrap();
                let diagnostic = format!("log/{session}.e{epoch}.loss.json");
                let (recorded, _) = bucket
                    .get(&diagnostic)
                    .await?
                    .expect("missing blocked diagnostic");
                let recorded_value: serde_json::Value = serde_json::from_slice(&recorded)?;
                ensure!(recorded_value["recovery_blocked"] == true);
                Some((diagnostic, recorded))
            };
            ensure!(
                replica.restore_snapshot(CELL).await?.is_none(),
                "refusal exposed a restored cell"
            );

            returned.use_retained.store(true, Ordering::Release);
            let cold = peer.run(retained.tail(&request)).await??;
            ensure!(
                cold.range == original_range && rows(cold) == original_rows,
                "returned disk changed proven bytes"
            );
            tokio::time::timeout(Duration::from_secs(5), log.ensure_recovered(Some(leader), previous.lineage.as_deref())).await??;
            tokio::time::timeout(Duration::from_secs(5), log.recover(&session)).await??;
            let snapshot = replica
                .restore_snapshot(CELL)
                .await?
                .expect("returned witness did not restore state");
            let connection = rusqlite::Connection::open(snapshot.path())?;
            let recovered: (i64, i64) =
                connection.query_row("SELECT COUNT(*), SUM(value) FROM accounts", [], |row| {
                    Ok((row.get(0)?, row.get(1)?))
                })?;
            ensure!(
                recovered == (transactions as i64, (transactions * (transactions + 21) / 2) as i64),
                "recovery lost or repeated the original command"
            );
            let mut query = connection.prepare("SELECT value FROM accounts ORDER BY rowid")?;
            let actual = query
                .query_map([], |row| row.get::<_, i64>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let expected = (0..transactions)
                .map(|transaction| 11 + transaction as i64)
                .collect::<Vec<_>>();
            ensure!(actual == expected, "recovery changed original row contents");
            drop(query);
            let (body, _) = bucket.get(&format!("nodes/{leader}.json")).await?.unwrap();
            let sealed: serde_json::Value = serde_json::from_slice(&body)?;
            ensure!(
                sealed["generation"] == previous.generation && sealed["log"]["state"] == "sealed"
            );
            if let Some((diagnostic, recorded)) = diagnostic {
                let (history, _) = bucket.get(&diagnostic).await?.unwrap();
                ensure!(
                    history == recorded,
                    "recovery rewrote a prior diagnostic as permission"
                );
            }
            drop(connection);
            owner.shutdown_local().await;
            Ok::<_, anyhow::Error>(())
        })
        .await?;
    recovery.shutdown().await;
    peer.shutdown().await;
    outcome
}
