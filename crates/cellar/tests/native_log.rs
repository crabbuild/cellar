//! Caller-owned log transports, real follower proofs, and crash recovery.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Result};
use bytes::Bytes;
use cellar::bucket::Bucket;
use cellar::ltx_repl::Shipper;
use cellar::node_log::{
    decode_append, encode_tail_resp, serve_log_stream, SealReq, TailReq, TailWireReq,
};
use cellar::ownership_store::BucketOwnership;
use cellar::replication::ActivationOptions;
use cellar::sdk::{
    DurabilityOwner, ExecutionDomain, FollowerStore, HedgeMode, LogOptions, LogStreamIo,
    LogTransport, LogWindowOptions, NodeLogManager, OwnLog, ReplicaStorage, ReplicaStorageOptions,
    Replication, ReplicationOptions,
};
use cellar_logic::{CasGuard, LeaseCasOutcome, NodeLeaseRecord};
use futures_util::TryStreamExt;
use object_store::{
    throttle::{ThrottleConfig, ThrottledStore},
    ObjectStore,
};
use std::{
    path::Path,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::mpsc;

const CELL: &str = "Account:shared-business-id";
const TTL: u64 = 1_000;

async fn lease_writer(
    domain: &ExecutionDomain,
    ownership: Arc<BucketOwnership>,
    addr: &str,
) -> Result<mpsc::UnboundedSender<()>> {
    let generation = ownership.process_generation().unwrap().to_owned();
    let (send, mut receive) = mpsc::unbounded_channel();
    let addr = addr.to_owned();
    domain
        .run(async move {
            let node = addr.split(':').next().unwrap().to_owned();
            let mut record = NodeLeaseRecord {
                node,
                addr,
                expires_ms: cellar::ownership_store::now_ms() + TTL,
                peer_protocol: cellar::peer_auth::PROTOCOL_VERSION,
                generation,
                log_state: None,
                etag: String::new(),
            };
            let mut stamped = None;
            let LeaseCasOutcome::Applied { mut etag } = ownership
                .cas_node_lease(CasGuard::Absent, &record, &mut stamped)
                .await
                .map_err(|e| e.into_error())?
            else {
                anyhow::bail!("initial lease was rejected")
            };
            cellar::asyncrt::spawn(async move {
                loop {
                    let wake = futures_util::future::select(
                        Box::pin(receive.recv()),
                        Box::pin(tokio::time::sleep(Duration::from_millis(TTL / 3))),
                    )
                    .await;
                    if matches!(wake, futures_util::future::Either::Left((None, _))) {
                        return Ok::<_, anyhow::Error>(());
                    }
                    record.expires_ms = cellar::ownership_store::now_ms() + TTL;
                    let LeaseCasOutcome::Applied { etag: applied } = ownership
                        .cas_node_lease(CasGuard::Match(etag), &record, &mut stamped)
                        .await
                        .map_err(|e| e.into_error())?
                    else {
                        anyhow::bail!("lease renewal was rejected")
                    };
                    etag = applied;
                }
            })
            .detach();
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    Ok(send)
}

struct Transport {
    owner: Arc<cellar::host_services::HostServices>,
    follower_domain: Arc<ExecutionDomain>,
    follower: Arc<FollowerStore>,
    follower_node: String,
    _follower_lease: mpsc::UnboundedSender<()>,
    appends: AtomicUsize,
    streams: AtomicUsize,
}
impl LogTransport for Transport {
    fn post<'a>(
        &'a self,
        node: &'a str,
        _: &'a str,
        path: &'a str,
        body: Vec<u8>,
        _: Option<Duration>,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<Bytes>> + Send + 'a>> {
        Box::pin(async move {
            assert!(Arc::ptr_eq(&self.owner, &cellar::asyncrt::services()));
            ensure!(node == self.follower_node, "unexpected peer");
            self.follower_domain
                .run(async {
                    match path {
                        "/peer/log/append" => {
                            self.appends.fetch_add(1, Ordering::SeqCst);
                            Ok(serde_json::to_vec(
                                &self.follower.append(decode_append(&body)?).await,
                            )?
                            .into())
                        }
                        "/peer/log/seal" => Ok(serde_json::to_vec(
                            &self
                                .follower
                                .seal(&serde_json::from_slice::<SealReq>(&body)?)
                                .await?,
                        )?
                        .into()),
                        "/peer/log/tail" => {
                            let request: TailWireReq = serde_json::from_slice(&body)?;
                            Ok(
                                encode_tail_resp(&self.follower.tail(&request.request).await?)
                                    .into(),
                            )
                        }
                        _ => anyhow::bail!("unexpected log route {path}"),
                    }
                })
                .await?
        })
    }
    fn open_stream<'a>(
        &'a self,
        node: &'a str,
        _: &'a str,
        path: &'a str,
    ) -> Pin<Box<dyn std::future::Future<Output = Result<LogStreamIo>> + Send + 'a>> {
        Box::pin(async move {
            assert!(Arc::ptr_eq(&self.owner, &cellar::asyncrt::services()));
            ensure!(node == self.follower_node && path == "/peer/log/stream");
            self.streams.fetch_add(1, Ordering::SeqCst);
            let (client, server) = tokio::io::duplex(64 * 1024);
            let follower = self.follower.clone();
            self.follower_domain
                .spawn(async move { serve_log_stream(server, follower).await })?
                .detach();
            Ok(Box::new(client) as LogStreamIo)
        })
    }
}
struct Pair {
    domain: ExecutionDomain,
    replica: Replication,
    log: Arc<NodeLogManager>,
    owner: DurabilityOwner,
    transport: Arc<Transport>,
    bucket: Arc<Bucket>,
    session: String,
}
async fn pair(shared: &Path, directory: &Path, id: &str, policy: LogOptions) -> Result<Pair> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let follower_domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
    let leader = format!("leader-{id}");
    let generation = format!("generation-{id}");
    let follower_node = format!("follower-{id}");
    let mut bucket = Bucket::open_dev(shared)?;
    bucket.prefix = format!("fleets/{id}/");
    let bucket = Arc::new(bucket);
    let ownership = Arc::new(
        BucketOwnership::new(
            (*bucket).clone(),
            (*bucket).clone(),
            leader.clone(),
            generation.clone(),
        )
        .with_lease_ttl_ms(TTL),
    );
    let follower_ownership = Arc::new(
        BucketOwnership::new(
            (*bucket).clone(),
            (*bucket).clone(),
            follower_node.clone(),
            format!("follower-generation-{id}"),
        )
        .with_lease_ttl_ms(TTL),
    );
    let publish = lease_writer(&domain, ownership.clone(), &format!("{leader}:1234")).await?;
    let follower_lease = lease_writer(
        &follower_domain,
        follower_ownership,
        &format!("{follower_node}:1234"),
    )
    .await?;
    let follower = follower_domain
        .run(async {
            Arc::new(FollowerStore::new(
                &directory.join(format!("follower-{id}")),
                Some(bucket.clone()),
                &follower_node,
            ))
        })
        .await?;
    let transport = Arc::new(Transport {
        owner: domain.services(),
        follower_domain,
        follower,
        follower_node,
        _follower_lease: follower_lease,
        appends: AtomicUsize::new(0),
        streams: AtomicUsize::new(0),
    });
    let session = format!("{leader}/{generation}");
    let (replica, log, owner) = domain
        .run(async {
            let store: Arc<dyn ObjectStore> = Arc::new(ThrottledStore::new(
                bucket.store.clone(),
                ThrottleConfig {
                    wait_put_per_call: Duration::from_secs(60),
                    ..Default::default()
                },
            ));
            let replica = Replication::start(
                &directory.join(format!("leader-{id}")),
                ReplicaStorage::new(
                    store,
                    ReplicaStorageOptions {
                        prefix: bucket.prefix.clone(),
                        ..Default::default()
                    },
                )?,
                ReplicationOptions {
                    flush_interval: Duration::from_secs(3600),
                    compaction: None,
                    hydrate_bytes_per_second: 0,
                    ..Default::default()
                },
            )?;
            let log = Arc::new(NodeLogManager::new(
                &session,
                bucket.clone(),
                Arc::new(OwnLog {
                    ownership,
                    nudge: Box::new(move || {
                        let _ = publish.send(());
                    }),
                    write_lock: tokio::sync::Mutex::new(()),
                }),
                replica.ltx(),
                transport.clone(),
                policy,
            )?);
            log.recover_self().await?;
            let owner = DurabilityOwner::new(log.clone(), true);
            log.maintain().await?;
            ensure!(log.healthy() && log.ensemble_width() == 1);
            Ok::<_, anyhow::Error>((replica, log, owner))
        })
        .await??;
    Ok(Pair {
        domain,
        replica,
        log,
        owner,
        transport,
        bucket,
        session,
    })
}
async fn write(pair: &Pair, value: i64, fresh: bool) -> Result<()> {
    pair.domain
        .run(async {
            let activated = pair
                .replica
                .ltx()
                .activate(ActivationOptions {
                    cell: CELL,
                    epoch: 1,
                    fresh,
                    took_over: false,
                    resume_local: !fresh,
                    prior: None,
                })
                .await?;
            let connection = rusqlite::Connection::open(activated.path)?;
            connection.execute_batch("CREATE TABLE IF NOT EXISTS accounts(value INTEGER)")?;
            connection.execute("INSERT INTO accounts VALUES (?1)", [value])?;
            drop(connection);
            ensure!(
                tokio::time::timeout(
                    Duration::from_secs(5),
                    pair.replica.ltx().handoff_wait(CELL, 1),
                )
                .await??
                    == cellar_logic::ProofSource::Fleet,
                "real follower fsyncs must prove the write while bucket uploads are delayed"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_log_hosts_prove_follower_writes_and_recover_after_leader_loss() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let shared = directory.path().join("shared-objects.sqlite");
    let a = pair(
        &shared,
        directory.path(),
        "a",
        LogOptions {
            hedge: HedgeMode::Fixed(0),
            pipeline_depth: 2,
            record_publish_timeout: Duration::from_secs(2),
            ..Default::default()
        },
    )
    .await?;
    let b = pair(
        &shared,
        directory.path(),
        "b",
        LogOptions {
            hedge: HedgeMode::Fixed(0),
            window: Some(LogWindowOptions {
                max_appends: 3,
                max_bytes: 8 << 20,
            }),
            ordered_streams: true,
            record_publish_timeout: Duration::from_secs(3),
            ..Default::default()
        },
    )
    .await?;
    assert_eq!(a.log.pipeline(), 2);
    assert_eq!(b.log.pipeline(), 3);
    write(&a, 11, true).await?;
    write(&b, 22, true).await?;
    for pair in [&a, &b] {
        pair.domain
            .run(async {
                let objects = pair.bucket.store.list(None).try_collect::<Vec<_>>().await?;
                ensure!(
                    !objects.iter().any(|o| o
                        .location
                        .as_ref()
                        .starts_with(&format!("{}cells/", pair.bucket.prefix))),
                    "bucket upload must not be credited as the follower proof"
                );
                let tail = pair
                    .transport
                    .follower_domain
                    .run(pair.transport.follower.tail(&TailReq {
                        leader: pair.session.clone(),
                    }))
                    .await??;
                ensure!(
                    tail.range.complete && !tail.entries.is_empty(),
                    "actual follower disk must retain the LTX rows"
                );
                let (lease, _) = pair
                    .bucket
                    .get(&format!(
                        "nodes/{}.json",
                        pair.session.split('/').next().unwrap()
                    ))
                    .await?
                    .unwrap();
                let lease: serde_json::Value = serde_json::from_slice(&lease)?;
                assert!(lease.get("ownership_index_generation").is_none());
                assert_eq!(lease["generation"], pair.session.split('/').nth(1).unwrap());
                assert_eq!(lease["log"]["may_have_fleet_acks"], true);
                Ok::<_, anyhow::Error>(())
            })
            .await??;
    }
    assert!(a.transport.appends.load(Ordering::SeqCst) > 0);
    assert_eq!(a.transport.streams.load(Ordering::SeqCst), 0);
    assert!(b.transport.streams.load(Ordering::SeqCst) > 0);
    assert_eq!(b.transport.appends.load(Ordering::SeqCst), 0);
    // Simulate a crashed embedded process: cancel its work without a seal or
    // a durability drain. Its follower remains alive in a separate domain.
    a.domain.shutdown().await;
    drop(a.owner);
    write(&b, 23, false).await?;
    let recovery = ExecutionDomain::new(tokio::runtime::Handle::current());
    recovery
        .run(async {
            let replica = Replication::start(
                &directory.path().join("recovery-a"),
                ReplicaStorage::new(
                    a.bucket.store.clone(),
                    ReplicaStorageOptions {
                        prefix: a.bucket.prefix.clone(),
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
                    (*a.bucket).clone(),
                    (*a.bucket).clone(),
                    "recovery-a".into(),
                    "recovery-generation-a".into(),
                )
                .with_lease_ttl_ms(TTL),
            );
            let transport = Arc::new(Transport {
                owner: recovery.services(),
                follower_domain: a.transport.follower_domain.clone(),
                follower: a.transport.follower.clone(),
                follower_node: a.transport.follower_node.clone(),
                _follower_lease: a.transport._follower_lease.clone(),
                appends: AtomicUsize::new(0),
                streams: AtomicUsize::new(0),
            });
            let log = Arc::new(NodeLogManager::new(
                "recovery-a/recovery-generation-a",
                a.bucket.clone(),
                Arc::new(OwnLog {
                    ownership: lease.clone(),
                    nudge: Box::new(|| {}),
                    write_lock: tokio::sync::Mutex::new(()),
                }),
                replica.ltx(),
                transport,
                LogOptions::default(),
            )?);
            let mut owner = DurabilityOwner::new(log.clone(), false);
            let leader = a.session.split('/').next().unwrap();
            let previous = lease.read_node_lease(leader).await?.unwrap();
            let wait = previous
                .expires_ms
                .saturating_sub(cellar::ownership_store::now_ms());
            tokio::time::sleep(Duration::from_millis(wait + 1)).await;
            tokio::time::timeout(Duration::from_secs(5), log.recover(&a.session)).await??;
            let snapshot = replica
                .restore_snapshot(CELL)
                .await?
                .expect("the follower's committed state must restore");
            let connection = rusqlite::Connection::open(snapshot.path())?;
            assert_eq!(
                connection
                    .query_row::<i64, _, _>("SELECT value FROM accounts", [], |r| r.get(0))?,
                11
            );
            drop(connection);
            let (wire, _) = a
                .bucket
                .get(&format!("nodes/{leader}.json"))
                .await?
                .unwrap();
            let wire: serde_json::Value = serde_json::from_slice(&wire)?;
            assert_eq!(wire["log"]["state"], "sealed");
            owner.shutdown_local().await;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    recovery.shutdown().await;
    b.domain.shutdown().await;
    drop(b.owner);
    a.transport.follower_domain.shutdown().await;
    b.transport.follower_domain.shutdown().await;
    Ok(())
}

#[test]
fn explicit_log_policy_survives_unusable_process_environment() -> Result<()> {
    let mut child = std::process::Command::new(std::env::current_exe()?);
    child.args([
        "--exact",
        "independent_log_hosts_prove_follower_writes_and_recover_after_leader_loss",
        "--nocapture",
    ]);
    for setting in [
        "CELLAR_LOG_PIPELINE",
        "CELLAR_LOG_HEDGE_MS",
        "CELLAR_LOG_WINDOW",
        "CELLAR_LOG_WINDOW_BYTES",
        "CELLAR_LOG_TRANSPORT",
        "CELLAR_LOG_EVICT_BUDGET_MS",
        "CELLAR_LOG_EVICT_SUSTAIN_MS",
        "CELLAR_LOG_EVICT_BACKSTOP_MS",
        "CELLAR_LOG_EVICT_QUARANTINE_MS",
    ] {
        child.env(setting, "unusable");
    }
    let output = child.output()?;
    ensure!(
        output.status.success(),
        "explicit log policy consulted process environment:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn log_construction_rejects_wrong_identity_namespace_domain_and_policy() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut pair = pair(
        &directory.path().join("objects.sqlite"),
        directory.path(),
        "validated",
        LogOptions::default(),
    )
    .await?;
    let own = || {
        Arc::new(OwnLog {
            ownership: Arc::new(BucketOwnership::new(
                (*pair.bucket).clone(),
                (*pair.bucket).clone(),
                "leader-validated".into(),
                "generation-validated".into(),
            )),
            nudge: Box::new(|| {}),
            write_lock: tokio::sync::Mutex::new(()),
        })
    };
    pair.domain
        .run(async {
            for session in [
                "leader-validated",
                "leader-validated/wrong-generation",
                "wrong-node/generation-validated",
                "../generation-validated",
            ] {
                ensure!(NodeLogManager::new(
                    session,
                    pair.bucket.clone(),
                    own(),
                    pair.replica.ltx(),
                    pair.transport.clone(),
                    LogOptions::default()
                )
                .is_err());
            }
            for options in [
                LogOptions {
                    pipeline_depth: 0,
                    ..Default::default()
                },
                LogOptions {
                    record_publish_timeout: Duration::ZERO,
                    ..Default::default()
                },
                LogOptions {
                    window: Some(LogWindowOptions {
                        max_appends: 0,
                        max_bytes: 10,
                    }),
                    ..Default::default()
                },
                LogOptions {
                    window: Some(LogWindowOptions {
                        max_appends: 2,
                        max_bytes: 0,
                    }),
                    ..Default::default()
                },
            ] {
                ensure!(NodeLogManager::new(
                    &pair.session,
                    pair.bucket.clone(),
                    own(),
                    pair.replica.ltx(),
                    pair.transport.clone(),
                    options
                )
                .is_err());
            }
            let mut wrong_bucket = (*pair.bucket).clone();
            wrong_bucket.prefix = "other-fleet/".into();
            ensure!(NodeLogManager::new(
                &pair.session,
                Arc::new(wrong_bucket),
                own(),
                pair.replica.ltx(),
                pair.transport.clone(),
                LogOptions::default()
            )
            .is_err());
            ensure!(pair.log.recover("leader-validated").await.is_err());
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    pair.transport
        .follower_domain
        .run(async {
            ensure!(NodeLogManager::new(
                &pair.session,
                pair.bucket.clone(),
                own(),
                pair.replica.ltx(),
                pair.transport.clone(),
                LogOptions::default()
            )
            .is_err());
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    pair.domain.run(pair.owner.shutdown_local()).await?;
    pair.domain.shutdown().await;
    pair.transport.follower_domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn command_line_log_policy_validates_and_is_captured_by_the_manager() -> Result<()> {
    const CHILD: &str = "CELLAR_LOG_ADAPTER_CHILD";
    if std::env::var(CHILD).as_deref() == Ok("1") {
        let options = LogOptions::from_env()?;
        assert_eq!(options.pipeline_depth, 5);
        assert_eq!(options.hedge, HedgeMode::Fixed(0));
        assert_eq!(
            options.window,
            Some(LogWindowOptions {
                max_appends: 3,
                max_bytes: 1 << 20
            })
        );
        assert!(options.ordered_streams);
        assert_eq!(options.eviction.backstop_ms, 1_000);
        let directory = tempfile::tempdir()?;
        let mut pair = pair(
            &directory.path().join("objects.sqlite"),
            directory.path(),
            "cli",
            options.clone(),
        )
        .await?;
        for (name, invalid) in [
            ("CELLAR_LOG_PIPELINE", "0"),
            ("CELLAR_LOG_HEDGE_MS", "unusable"),
            ("CELLAR_LOG_WINDOW", "unusable"),
            ("CELLAR_LOG_WINDOW_BYTES", "0"),
            ("CELLAR_LOG_TRANSPORT", "unusable"),
            ("CELLAR_LOG_EVICT_BACKSTOP_MS", "0"),
        ] {
            let previous = std::env::var(name)?;
            std::env::set_var(name, invalid);
            ensure!(LogOptions::from_env().is_err(), "accepted {name}={invalid}");
            assert_eq!(pair.log.options(), &options);
            pair.domain.run(pair.log.maintain()).await??;
            std::env::set_var(name, previous);
        }
        pair.domain.run(pair.owner.shutdown_local()).await?;
        pair.domain.shutdown().await;
        pair.transport.follower_domain.shutdown().await;
        return Ok(());
    }
    let output = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "command_line_log_policy_validates_and_is_captured_by_the_manager",
            "--nocapture",
        ])
        .env_clear()
        .env(CHILD, "1")
        .env("CELLAR_LOG_PIPELINE", "5")
        .env("CELLAR_LOG_HEDGE_MS", "0")
        .env("CELLAR_LOG_WINDOW", "3")
        .env("CELLAR_LOG_WINDOW_BYTES", "1048576")
        .env("CELLAR_LOG_TRANSPORT", "stream")
        .env("CELLAR_LOG_EVICT_BACKSTOP_MS", "1000")
        .output()?;
    ensure!(
        output.status.success(),
        "CLI log adapter failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
