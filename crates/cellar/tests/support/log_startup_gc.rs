//! A boot sweep must preserve another generation's acknowledged bundle.
//! Hold the real no-log root read; neither the store nor transport supplies proof.

use super::*;
use anyhow::Result;
use futures_util::{stream::BoxStream, StreamExt};
use object_store::{path::Path as ObjectPath, *};
use sha2::Digest;
use std::sync::atomic::AtomicBool;
use tokio::sync::Semaphore;

pub(super) struct AppendHold {
    entered: Semaphore,
    release: Semaphore,
}

pub(super) struct HeldTransport {
    pub base: Arc<Transport>,
    pub hold: Arc<AppendHold>,
}

impl LogTransport for HeldTransport {
    fn post<'a>(
        &'a self,
        node: &'a str,
        addr: &'a str,
        path: &'a str,
        body: Vec<u8>,
        deadline: Option<Duration>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes>> + Send + 'a>> {
        Box::pin(async move {
            if path == "/peer/log/append" {
                self.hold.entered.add_permits(1);
                self.hold.release.acquire().await?.forget();
            }
            self.base.post(node, addr, path, body, deadline).await
        })
    }
}

#[derive(Debug)]
struct RootCut {
    store: Arc<dyn ObjectStore>,
    key: String,
    bytes: std::sync::Mutex<Option<Bytes>>,
    armed: AtomicBool,
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
}

impl std::fmt::Display for RootCut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("startup root read cut")
    }
}

#[async_trait::async_trait]
impl ObjectStore for RootCut {
    async fn put_opts(
        &self,
        path: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.store.put_opts(path, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        path: &ObjectPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.store.put_multipart_opts(path, options).await
    }

    async fn get_opts(
        &self,
        path: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let result = self.store.get_opts(path, options).await?;
        if path.as_ref() != self.key || !self.armed.swap(false, Ordering::AcqRel) {
            return Ok(result);
        }
        let meta = result.meta.clone();
        let range = result.range.clone();
        let attributes = result.attributes.clone();
        let bytes = result.bytes().await?;
        *self.bytes.lock().unwrap() = Some(bytes.clone());
        self.entered.add_permits(1);
        self.release
            .acquire()
            .await
            .map_err(|error| object_store::Error::Generic {
                store: "startup-root-cut",
                source: Box::new(error),
            })?
            .forget();
        Ok(GetResult {
            payload: GetResultPayload::Stream(
                futures_util::stream::once(async { Ok(bytes) }).boxed(),
            ),
            meta,
            range,
            attributes,
        })
    }

    async fn delete(&self, path: &ObjectPath) -> object_store::Result<()> {
        self.store.delete(path).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.store.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.store.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        self.store.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        self.store.copy_if_not_exists(from, to).await
    }
}

struct Release(Arc<Semaphore>);

impl Drop for Release {
    fn drop(&mut self) {
        self.0.add_permits(1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_gc_preserves_a_competing_generations_real_bucket_proof() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let hold = Arc::new(AppendHold {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let live = pair_with_proof(
        &directory.path().join("fleet.sqlite"),
        directory.path(),
        "startup-gc",
        LogOptions::default(),
        Some(hold.clone()),
        false,
    )
    .await?;
    let cold = ExecutionDomain::new(tokio::runtime::Handle::current());
    let node = live.session.split('/').next().unwrap();
    let cut = Arc::new(RootCut {
        store: live.bucket.store.clone(),
        key: format!("{}nodes/{node}.json", live.bucket.prefix),
        bytes: std::sync::Mutex::new(None),
        armed: AtomicBool::new(true),
        entered: Arc::new(Semaphore::new(0)),
        release: Arc::new(Semaphore::new(0)),
    });
    let (contender, mut owner, contender_session) = cold
        .run(async {
            let replica = Replication::start(
                &directory.path().join("contender"),
                ReplicaStorage::new(
                    live.bucket.store.clone(),
                    ReplicaStorageOptions {
                        prefix: live.bucket.prefix.clone(),
                        ..Default::default()
                    },
                )?,
                ReplicationOptions {
                    compaction: None,
                    ..Default::default()
                },
            )?;
            let ownership = Arc::new(
                BucketOwnership::new(
                    (*live.bucket).clone(),
                    (*live.bucket).clone(),
                    node.into(),
                    cellar::sdk::PeerProbeSigner::generate().public_key_hex(),
                )
                .with_lease_ttl_ms(TTL),
            );
            ownership.prepare_lineage().await?;
            let mut bucket = (*live.bucket).clone();
            bucket.store = cut.clone();
            let session = ownership.log_session()?;
            let log = Arc::new(NodeLogManager::new(
                &session,
                Arc::new(bucket),
                Arc::new(OwnLog {
                    ownership,
                    nudge: Box::new(|| {}),
                    write_lock: tokio::sync::Mutex::new(()),
                }),
                replica.ltx(),
                live.transport.clone(),
                LogOptions::default(),
            )?);
            let owner = DurabilityOwner::new(log.clone(), false);
            Ok::<_, anyhow::Error>((log, owner, session))
        })
        .await??;
    let boot = Box::pin(cold.run(contender.recover_self()));
    let scenario = async {
        let _release = Release(cut.release.clone());
        tokio::time::timeout(Duration::from_secs(5), cut.entered.acquire())
            .await??
            .forget();
        live.domain
            .run(async {
                let key = format!("nodes/{node}.json");
                let (bytes, _) = live.bucket.get(&key).await?.context("live root missing")?;
                let before: serde_json::Value = serde_json::from_slice(&bytes)?;
                let captured: serde_json::Value = serde_json::from_slice(
                    cut.bytes
                        .lock()
                        .unwrap()
                        .as_ref()
                        .context("missing root snapshot")?,
                )?;
                ensure!(
                    captured.get("log").is_none()
                        && captured["generation"] == live.session.split('/').nth(2).unwrap()
                        && live.session.split('/').nth(2).unwrap().len() == 64,
                    "boot did not capture the real no-log generation"
                );
                ensure!(
                    before.get("log").is_none(),
                    "cut did not follow a no-log observation"
                );
                live.log.maintain().await?;
                ensure!(live.log.healthy());
                let activated = live
                    .replica
                    .ltx()
                    .activate(ActivationOptions {
                        cell: CELL,
                        epoch: 1,
                        fresh: true,
                        took_over: false,
                        resume_local: false,
                        prior: None,
                    })
                    .await?;
                let connection = rusqlite::Connection::open(&activated.path)?;
                connection.execute_batch(
                    "CREATE TABLE accounts(value INTEGER); INSERT INTO accounts VALUES(11)",
                )?;
                drop(connection);
                let proof = tokio::time::timeout(
                    Duration::from_secs(5),
                    live.replica.ltx().handoff_wait(CELL, 1),
                )
                .await??;
                tokio::time::timeout(Duration::from_secs(5), hold.entered.acquire())
                    .await??
                    .forget();
                ensure!(
                    proof == cellar_logic::ProofSource::Bucket
                        && live.transport.appends.load(Ordering::SeqCst) == 0,
                    "the actual bundle must prove the write before any follower ack: {proof:?}"
                );
                let (bytes, _) = live
                    .bucket
                    .get(&key)
                    .await?
                    .context("active root missing")?;
                let root: serde_json::Value = serde_json::from_slice(&bytes)?;
                ensure!(
                    root["generation"] == live.session.split('/').nth(2).unwrap()
                        && root["log"]["state"] == "open"
                        && root["expires_ms"].as_u64().unwrap() > cellar::ownership_store::now_ms()
                );
                ensure!(live
                    .bucket
                    .list(&format!("cells/{CELL}/ltx/"))
                    .await?
                    .is_empty());
                let objects = live
                    .bucket
                    .list(&format!("log/{}/bundle/", live.session))
                    .await?;
                ensure!(!objects.is_empty() && objects.len() <= 100);
                let mut retained = Vec::new();
                for meta in objects {
                    let key = meta.location.to_string();
                    let (bytes, _) = live
                        .bucket
                        .get(&key)
                        .await?
                        .context("credited bundle missing")?;
                    ensure!(bytes.len() <= 16 << 20);
                    let rows = cellar_ltx::bundle::decode_rows(&bytes)?;
                    ensure!(rows
                        .iter()
                        .any(|row| row.cell == CELL && row.cell_epoch == 1 && row.txid > 0));
                    retained.push((key, bytes));
                }
                Ok::<_, anyhow::Error>(retained)
            })
            .await?
    };
    let (recovered, retained) = tokio::join!(boot, scenario);
    let outcome = async {
        recovered??;
        let retained = retained?;
        cold.run(contender.gc_other_generations()).await??;
        live.domain
            .run(async {
                let mut missing = Vec::new();
                for (key, expected) in &retained {
                    if live.bucket.get(key).await?.map(|(bytes, _)| bytes) != Some(expected.clone())
                    {
                        missing.push(key.clone());
                    }
                }
                let percell = live.bucket.list(&format!("cells/{CELL}/ltx/")).await?;
                let (root, _) = live
                    .bucket
                    .get(&format!("nodes/{node}.json"))
                    .await?
                    .context("root vanished during GC")?;
                let root: serde_json::Value = serde_json::from_slice(&root)?;
                ensure!(
                    root["log"]["state"] == "open"
                        && root["generation"] == live.session.split('/').nth(2).unwrap()
                        && root["expires_ms"].as_u64().unwrap() > cellar::ownership_store::now_ms(),
                    "live generation moved during the retirement audit"
                );
                ensure!(percell.is_empty(), "per-cell data masked the bundle proof");
                Ok::<_, anyhow::Error>((missing, retained.clone()))
            })
            .await?
    }
    .await;
    // Force-join the original writer before releasing its held append. This
    // preserves the original Bucket proof and leaves the real follower empty.
    live.domain.shutdown().await;
    drop(live.owner);
    hold.release.add_permits(100);
    ensure!(
        live.transport.appends.load(Ordering::SeqCst) == 0,
        "a follower ack repaired the measured Bucket prefix"
    );
    std::fs::remove_dir_all(directory.path().join("leader-startup-gc"))?;
    let (missing, retained) = match outcome {
        Ok(audit) => audit,
        Err(error) => {
            cold.run(owner.shutdown_local()).await?;
            cold.shutdown().await;
            live.transport.follower_domain.shutdown().await;
            return Err(error);
        }
    };
    let recovery = ExecutionDomain::new(tokio::runtime::Handle::current());
    let restored = recovery
        .run(async {
            let (bytes, _) = live
                .bucket
                .get(&format!("nodes/{node}.json"))
                .await?
                .context("dead root missing")?;
            let root: serde_json::Value = serde_json::from_slice(&bytes)?;
            let wait = root["expires_ms"]
                .as_u64()
                .unwrap()
                .saturating_sub(cellar::ownership_store::now_ms());
            tokio::time::sleep(Duration::from_millis(wait + 1)).await;
            let replica = Replication::start(
                &directory.path().join("recovery"),
                ReplicaStorage::new(
                    live.bucket.store.clone(),
                    ReplicaStorageOptions {
                        prefix: live.bucket.prefix.clone(),
                        ..Default::default()
                    },
                )?,
                ReplicationOptions {
                    compaction: None,
                    ..Default::default()
                },
            )?;
            let ownership = Arc::new(
                BucketOwnership::new(
                    (*live.bucket).clone(),
                    (*live.bucket).clone(),
                    "startup-gc-recovery".into(),
                    cellar::sdk::PeerProbeSigner::generate().public_key_hex(),
                )
                .with_lease_ttl_ms(TTL),
            );
            ownership.prepare_lineage().await?;
            let transport = Arc::new(Transport {
                owner: recovery.services(),
                follower_domain: live.transport.follower_domain.clone(),
                follower: live.transport.follower.clone(),
                follower_node: live.transport.follower_node.clone(),
                _follower_lease: live.transport._follower_lease.clone(),
                appends: AtomicUsize::new(0),
                streams: AtomicUsize::new(0),
            });
            let manager = Arc::new(NodeLogManager::new(
                &ownership.log_session()?,
                live.bucket.clone(),
                Arc::new(OwnLog {
                    ownership,
                    nudge: Box::new(|| {}),
                    write_lock: tokio::sync::Mutex::new(()),
                }),
                replica.ltx(),
                transport,
                LogOptions::default(),
            )?);
            let mut owner = DurabilityOwner::new(manager.clone(), false);
            let result = async {
                tokio::time::timeout(Duration::from_secs(5), manager.recover(&live.session))
                    .await??;
                let snapshot = replica.restore_snapshot(CELL).await?;
                let rows = match snapshot {
                    Some(snapshot) => {
                        let db = rusqlite::Connection::open(snapshot.path())?;
                        Some(db.query_row(
                            "SELECT count(*),sum(value) FROM accounts",
                            [],
                            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
                        )?)
                    }
                    None => None,
                };
                Ok::<_, anyhow::Error>(rows)
            }
            .await;
            owner.shutdown_local().await;
            result
        })
        .await?;
    recovery.shutdown().await;
    let collection = cold.run(async {
        // More ignored entries than one page: they must neither be deleted
        // nor starve covered objects behind them. This is a bounded-page
        // regression, not a provider memory/capacity qualification.
        let (node, lineage_generation) = live.session.split_once('/').unwrap();
        let lineage = lineage_generation.split('/').next().unwrap();
        let prefix = format!("log/{node}/{lineage}/");
        for n in 0..600 {
            live.bucket.put(&format!("{prefix}0/loss/{n:04}"), b"retained diagnostic".to_vec()).await?;
        }
        let bytes = retained.first().context("no original proof")?.1.clone();
        let current = format!("log/{contender_session}/bundle/current.ltxb");
        let foreign = format!("log/{node}/00000000000000000000000000000000/foreign/bundle/proof.ltxb");
        let corrupt = format!("log/{}/bundle/corrupt.ltxb", live.session);
        live.bucket.put(&current, bytes.to_vec()).await?;
        live.bucket.put(&foreign, bytes.to_vec()).await?;
        live.bucket.put(&corrupt, b"not a bundle".to_vec()).await?;
        let mut keep = vec![current, foreign, corrupt];
        let mut eligible = retained.iter().map(|(key,_)| key.clone()).collect::<Vec<_>>();
        for (cell, version, deletable) in [(CELL, 1, true), ("never-covered", 1, false), (CELL, 2, false)] {
            let bytes = serde_json::to_vec(&serde_json::json!({"version":version,"cells":[{"cell":cell,"epoch":1,"through":1}]}))?;
            let key = format!("log/{}/recovered/{:x}.json", live.session, sha2::Sha256::digest(&bytes));
            live.bucket.put(&key, bytes).await?;
            if deletable { eligible.push(key); } else { keep.push(key); }
        }
        let corrupt_hint = format!("log/{}/recovered/wrong-digest.json", live.session);
        live.bucket.put(&corrupt_hint, b"{}".to_vec()).await?;
        keep.push(corrupt_hint);
        contender.gc_other_generations().await?;
        for (key, bytes) in &retained {
            ensure!(live.bucket.get(key).await?.map(|(value,_)|value) == Some(bytes.clone()), "one pass skipped its page bound");
        }
        for _ in 0..4 { contender.gc_other_generations().await?; }
        for key in eligible {
            ensure!(live.bucket.get(&key).await?.is_none(), "fresh per-cell coverage did not retire {key}");
        }
        for key in keep {
            ensure!(live.bucket.get(&key).await?.is_some(), "GC deleted an excluded or unproven object: {key}");
        }
        ensure!(live.bucket.get(&format!("{prefix}0/loss/0000")).await?.is_some());
        Ok::<_, anyhow::Error>(())
    }).await;
    cold.run(owner.shutdown_local()).await?;
    cold.shutdown().await;
    live.transport.follower_domain.shutdown().await;
    let restored = restored?;
    collection??;
    ensure!(missing.is_empty() && restored == Some((1,11)),
        "startup GC lost original Bucket proof before cold recovery: missing={missing:?}; restored={restored:?}; follower_data_appends={}",
        live.transport.appends.load(Ordering::SeqCst));
    Ok(())
}
