// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Caller-configured production storage, paging, and credential providers.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use cellar::replication::{ActivationOptions, SyncWait};
use cellar::sdk::{
    CompactionConfig, ExecutionDomain, ReplicaRangeReaderFactory, ReplicaStorage,
    ReplicaStorageOptions, ReplicaTimestampMetadataKey, Replication, ReplicationOptions,
};
use cellar_ltx::client::object_store::StoreRangeReader;
use cellar_ltx::paged::RangeReader;
use cellar_ltx::{ObjectStoreClient, ObjectStoreConfig};
use futures_util::TryStreamExt;
use object_store::{
    aws::{AmazonS3Builder, AwsCredential},
    CredentialProvider, ObjectStore,
};
use std::{
    io::{BufRead, BufReader, Write},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

const CELL: &str = "Account:same-business-id";

fn options() -> ReplicationOptions {
    ReplicationOptions {
        flush_interval: Duration::ZERO,
        compaction: None,
        hydrate_bytes_per_second: 0,
        paged_min_bytes: 0,
        ..Default::default()
    }
}

async fn start(
    domain: &ExecutionDomain,
    watch: &std::path::Path,
    storage: ReplicaStorage,
) -> Result<Replication> {
    domain
        .run(async { Replication::start(watch, storage, options()) })
        .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_replica_maintains_aged_wal_and_releases_its_database_on_close() -> Result<()> {
    use std::sync::atomic::AtomicI64;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as i64;
    let clock = Arc::new(AtomicI64::new(now));
    let selected = clock.clone();
    let domain = ExecutionDomain::with_wall_clock(
        tokio::runtime::Handle::current(),
        Arc::new(move || selected.load(Ordering::SeqCst)),
    );
    let directory = tempfile::tempdir()?;
    let storage = ReplicaStorage::new(
        Arc::new(object_store::memory::InMemory::new()),
        ReplicaStorageOptions::default(),
    )?;
    let replica = start(&domain, directory.path(), storage).await?;
    domain
        .run(async {
            let active = replica
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
            let writer = rusqlite::Connection::open(&active.path)?;
            writer.execute_batch(
                "CREATE TABLE counter(value INTEGER); INSERT INTO counter VALUES (7)",
            )?;
            ensure!(matches!(
                replica
                    .ltx()
                    .sync_wait(CELL, 1, Duration::from_secs(5))
                    .await,
                SyncWait::Durable
            ));
            // Immutable SQLite reads only the main file, so the table becomes
            // visible here only after maintenance backfills its WAL frames.
            let uri = format!("file:{}?immutable=1", active.path.display());
            let main_value = || -> Result<i64> {
                let main = rusqlite::Connection::open_with_flags(
                    &uri,
                    rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY
                        | rusqlite::OpenFlags::SQLITE_OPEN_URI,
                )?;
                Ok(main.query_row("SELECT value FROM counter", [], |row| row.get(0))?)
            };
            assert!(
                main_value().is_err(),
                "capture must leave age maintenance to its owner"
            );
            clock.fetch_add(120_000, Ordering::SeqCst);
            tokio::time::timeout(Duration::from_secs(5), async {
                while main_value().ok() != Some(7) {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await?;
            writer.execute("UPDATE counter SET value = 8", [])?;
            ensure!(matches!(
                replica
                    .ltx()
                    .sync_wait(CELL, 1, Duration::from_secs(5))
                    .await,
                SyncWait::Durable
            ));
            let snapshot = replica
                .restore_snapshot(CELL)
                .await?
                .context("durable snapshot")?;
            let restored = rusqlite::Connection::open(snapshot.path())?;
            assert_eq!(
                restored.query_row("SELECT value FROM counter", [], |row| row.get::<_, i64>(0))?,
                8
            );
            drop(restored);
            drop(writer);
            replica.ltx().close_in_place(CELL, 1).await?;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    domain.shutdown().await;
    Ok(())
}

async fn write(replica: &Replication, value: i64) -> Result<()> {
    let active = replica
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
    let connection = rusqlite::Connection::open(active.path)?;
    connection.execute_batch("CREATE TABLE accounts(value INTEGER, payload BLOB)")?;
    connection.execute(
        "INSERT INTO accounts VALUES (?1, zeroblob(262144))",
        [value],
    )?;
    ensure!(
        matches!(
            replica
                .ltx()
                .sync_wait(CELL, 1, Duration::from_secs(5))
                .await,
            SyncWait::Durable
        ),
        "a real bucket upload must prove the committed SQLite state"
    );
    Ok(())
}

struct Readers {
    store: Arc<dyn ObjectStore>,
    owner: Arc<cellar::host_services::HostServices>,
    paths: Mutex<Vec<String>>,
    reads: Arc<AtomicUsize>,
}

struct Reader {
    inner: StoreRangeReader,
    owner: Arc<cellar::host_services::HostServices>,
    reads: Arc<AtomicUsize>,
}

impl RangeReader for Reader {
    fn read_range(
        &self,
        info: &cellar_ltx::ltx::FileInfo,
        offset: u64,
        len: u64,
    ) -> cellar_ltx::Result<Vec<u8>> {
        assert!(
            Arc::ptr_eq(&self.owner, &cellar::asyncrt::services()),
            "page faults must retain their owning domain"
        );
        self.reads.fetch_add(1, Ordering::SeqCst);
        self.inner.read_range(info, offset, len)
    }
}

#[async_trait::async_trait]
impl ReplicaRangeReaderFactory for Readers {
    async fn create(&self, prefix: &str) -> Result<Box<dyn RangeReader>> {
        assert!(Arc::ptr_eq(&self.owner, &cellar::asyncrt::services()));
        self.paths.lock().unwrap().push(prefix.into());
        Ok(Box::new(Reader {
            inner: StoreRangeReader::new(self.store.clone(), prefix.into()),
            owner: self.owner.clone(),
            reads: self.reads.clone(),
        }))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_store_namespaces_retain_real_durability_and_caller_page_readers() -> Result<()> {
    let a = ExecutionDomain::new(tokio::runtime::Handle::current());
    let b = ExecutionDomain::new(tokio::runtime::Handle::current());
    let takeover = ExecutionDomain::new(tokio::runtime::Handle::current());
    let directory = tempfile::tempdir()?;
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let alpha = ReplicaStorage::new(
        store.clone(),
        ReplicaStorageOptions {
            prefix: "fleets/alpha".into(),
            ..Default::default()
        },
    )?;
    let beta = ReplicaStorage::new(
        store.clone(),
        ReplicaStorageOptions {
            prefix: "fleets/beta/".into(),
            timestamp_metadata_key: ReplicaTimestampMetadataKey::Underscore,
            ..Default::default()
        },
    )?;
    assert!(Arc::ptr_eq(&store, &alpha.object_store()));
    assert_eq!(alpha.prefix(), "fleets/alpha/");
    let replica_a = start(&a, &directory.path().join("a"), alpha.clone()).await?;
    let replica_b = start(&b, &directory.path().join("b"), beta).await?;
    a.run(write(&replica_a, 3)).await??;
    b.run(write(&replica_b, 7)).await??;
    for (domain, replica, expected) in [(&a, &replica_a, 3), (&b, &replica_b, 7)] {
        domain
            .run(async {
                let snapshot = replica
                    .restore_snapshot(CELL)
                    .await?
                    .expect("the actual LTX chain must restore");
                let connection = rusqlite::Connection::open(snapshot.path())?;
                assert_eq!(
                    connection
                        .query_row::<i64, _, _>("SELECT value FROM accounts", [], |r| r.get(0))?,
                    expected
                );
                Ok::<_, anyhow::Error>(())
            })
            .await??;
    }
    let objects = store.list(None).try_collect::<Vec<_>>().await?;
    for (prefix, metadata_key) in [
        ("fleets/alpha/", "litestream-timestamp"),
        ("fleets/beta/", "litestream_timestamp"),
    ] {
        let object = objects
            .iter()
            .find(|o| o.location.as_ref().starts_with(prefix))
            .expect("namespace must hold a real LTX object");
        let result = store.get(&object.location).await?;
        assert!(result
            .attributes
            .get(&object_store::Attribute::Metadata(metadata_key.into()))
            .is_some());
    }
    let readers = Arc::new(Readers {
        store: store.clone(),
        owner: takeover.services(),
        paths: Mutex::new(Vec::new()),
        reads: Arc::new(AtomicUsize::new(0)),
    });
    let replica_c = start(
        &takeover,
        &directory.path().join("c"),
        alpha.with_range_readers(readers.clone()),
    )
    .await?;
    assert_eq!(replica_c.options(), &options());
    replica_c.set_paged_fleet(true);
    takeover
        .run(async {
            let activated = replica_c
                .ltx()
                .activate(ActivationOptions {
                    cell: CELL,
                    epoch: 2,
                    fresh: false,
                    took_over: true,
                    resume_local: false,
                    prior: None,
                })
                .await?;
            let vfs = activated
                .vfs
                .expect("a zero-threshold takeover must use the real paged VFS");
            let connection = rusqlite::Connection::open_with_flags_and_vfs(
                &activated.path,
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
                &vfs,
            )?;
            assert_eq!(
                connection.query_row::<(i64, i64), _, _>(
                    "SELECT value, length(payload) FROM accounts",
                    [],
                    |r| Ok((r.get(0)?, r.get(1)?))
                )?,
                (3, 262144)
            );
            drop(connection);
            replica_c.ltx().close_in_place(CELL, 2).await
        })
        .await??;
    assert_eq!(
        readers.paths.lock().unwrap().as_slice(),
        &[format!("fleets/alpha/cells/{CELL}/ltx/e1")]
    );
    assert!(readers.reads.load(Ordering::SeqCst) > 0);
    a.run(replica_a.ltx().close_in_place(CELL, 1)).await??;
    a.shutdown().await;
    b.run(async {
        let snapshot = replica_b.restore_snapshot(CELL).await?.unwrap();
        let connection = rusqlite::Connection::open(snapshot.path())?;
        assert_eq!(
            connection.query_row::<i64, _, _>("SELECT value FROM accounts", [], |r| r.get(0))?,
            7
        );
        replica_b.ltx().close_in_place(CELL, 1).await
    })
    .await??;
    b.shutdown().await;
    takeover.shutdown().await;
    Ok(())
}

#[derive(Debug)]
struct Credentials(Arc<AtomicUsize>);

#[async_trait::async_trait]
impl CredentialProvider for Credentials {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
        let call = self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Arc::new(AwsCredential {
            key_id: if call == 0 {
                "application-provider"
            } else {
                "application-provider-refreshed"
            }
            .into(),
            secret_key: "fixture-secret".into(),
            token: None,
        }))
    }
}

async fn supplied_s3(native: bool) -> Result<()> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    listener.set_nonblocking(true)?;
    let server = std::thread::spawn(move || -> Result<()> {
        for attempt in 0..if native { 2 } else { 1 } {
            let deadline = std::time::Instant::now() + Duration::from_secs(3);
            let (socket, _) = loop {
                match listener.accept() {
                    Ok(accepted) => break accepted,
                    Err(e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            && std::time::Instant::now() < deadline =>
                    {
                        std::thread::sleep(Duration::from_millis(5))
                    }
                    Err(e) => {
                        return Err(e).with_context(|| {
                            format!("S3 fixture native={native} attempt={attempt}: accept request")
                        })
                    }
                }
            };
            // Accepted sockets may inherit the listener's nonblocking mode.
            // Header reads use bounded blocking I/O on this dedicated thread.
            socket.set_nonblocking(false)?;
            socket.set_read_timeout(Some(Duration::from_secs(3)))?;
            let mut socket = BufReader::new(socket);
            let mut request = String::new();
            loop {
                let mut line = String::new();
                ensure!(
                    socket.read_line(&mut line).with_context(|| {
                        format!("S3 fixture native={native} attempt={attempt}: read headers")
                    })? > 0,
                    "request headers stopped"
                );
                let done = line == "\r\n";
                request.push_str(&line);
                ensure!(request.len() <= 16384, "request header bound");
                if done {
                    break;
                }
            }
            ensure!(
            request.starts_with(
                "GET /caller-bucket/application/data/0000/0000000000000001-0000000000000001.ltx "
            ),
            "unexpected page path: {request}"
        );
            ensure!(
                request.contains(if attempt == 0 {
                    "Credential=application-provider/"
                } else {
                    "Credential=application-provider-refreshed/"
                }),
                "page reads must use the original provider: {request}"
            );
            ensure!(
                request.to_ascii_lowercase().contains("range: bytes=1-3"),
                "expected range: {request}"
            );
            if native && attempt == 0 {
                socket.get_mut().write_all(
                    b"HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                )?;
            } else {
                socket.get_mut().write_all(b"HTTP/1.1 206 Partial Content\r\nContent-Length: 3\r\nContent-Range: bytes 1-3/5\r\nConnection: close\r\n\r\nbcd")?;
            }
        }
        Ok(())
    });
    let calls = Arc::new(AtomicUsize::new(0));
    let store = Arc::new(
        AmazonS3Builder::new()
            .with_bucket_name("caller-bucket")
            .with_region("test-1")
            .with_endpoint(format!("http://{address}"))
            .with_allow_http(true)
            .with_credentials(Arc::new(Credentials(calls.clone())))
            .with_retry(object_store::RetryConfig {
                max_retries: 0,
                ..Default::default()
            })
            .build()?,
    );
    let config = ObjectStoreConfig {
        bucket: "caller-bucket".into(),
        path: "application/data".into(),
        region: "test-1".into(),
        endpoint: format!("http://{address}"),
        access_key_id: "unused-config-key".into(),
        secret_access_key: "unused-config-secret".into(),
        session_token: String::new(),
        force_path_style: true,
        skip_verify: false,
        part_size: 0,
        timestamp_metadata_key: Default::default(),
    };
    let client = if native {
        ObjectStoreClient::with_s3_store(config, store)
    } else {
        ObjectStoreClient::with_store(config, store)
    };
    let reader = client.blocking_range_reader().await?;
    let info = cellar_ltx::ltx::FileInfo {
        level: 0,
        min_txid: cellar_ltx::TXID(1),
        max_txid: cellar_ltx::TXID(1),
        pre_apply_checksum: 0,
        post_apply_checksum: 0,
        size: 5,
        created_at: None,
    };
    let bytes = cellar::asyncrt::blocking(move || reader.read_range(&info, 1, 3)).await?;
    server.join().expect("S3 peer must complete")?;
    let bytes = bytes?;
    assert_eq!(bytes, b"bcd");
    ensure!(
        calls.load(Ordering::SeqCst) >= if native { 2 } else { 1 },
        "the original provider must supply and refresh the credential"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn generic_and_native_s3_page_reads_keep_the_supplied_credential_provider() -> Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    domain
        .run(async {
            supplied_s3(false).await?;
            supplied_s3(true).await
        })
        .await??;
    domain.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn replication_configuration_rejects_invalid_limits_and_namespaces() -> Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let directory = tempfile::tempdir()?;
    let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    for prefix in ["/absolute", "/", "a//b", "../fleet", "a/../b", "a//"] {
        ensure!(
            ReplicaStorage::new(
                store.clone(),
                ReplicaStorageOptions {
                    prefix: prefix.into(),
                    ..Default::default()
                }
            )
            .is_err(),
            "invalid prefix: {prefix}"
        );
    }
    let storage = ReplicaStorage::new(store.clone(), Default::default())?;
    for invalid in [
        ReplicationOptions {
            durability_timeout: Duration::ZERO,
            ..Default::default()
        },
        ReplicationOptions {
            sync_concurrency: 0,
            ..Default::default()
        },
        ReplicationOptions {
            restore_concurrency: 0,
            ..Default::default()
        },
        ReplicationOptions {
            hydration_concurrency: 0,
            ..Default::default()
        },
        ReplicationOptions {
            coverage_cache_entries: 1_048_577,
            ..Default::default()
        },
        ReplicationOptions {
            sync_concurrency: usize::MAX,
            ..Default::default()
        },
        ReplicationOptions {
            compaction: Some(CompactionConfig {
                min_txids: 1,
                min_bytes: 1,
                concurrency: 1,
            }),
            ..Default::default()
        },
        ReplicationOptions {
            compaction: Some(CompactionConfig {
                min_txids: 2,
                min_bytes: u64::MAX,
                concurrency: 1,
            }),
            ..Default::default()
        },
        ReplicationOptions {
            compaction: Some(CompactionConfig {
                min_txids: 2,
                min_bytes: 1,
                concurrency: 0,
            }),
            ..Default::default()
        },
    ] {
        assert!(domain
            .run(async { Replication::start(directory.path(), storage.clone(), invalid) })
            .await?
            .is_err());
    }
    assert!(store.list(None).try_collect::<Vec<_>>().await?.is_empty());
    domain.shutdown().await;
    Ok(())
}

// Resolve and mutate process environment only in an isolated exact-test child.
#[tokio::test]
async fn command_line_replication_adapter_validates_and_captures_policy() -> Result<()> {
    const CHILD: &str = "CELLAR_REPLICATION_ADAPTER_CHILD";
    if std::env::var(CHILD).as_deref() == Ok("1") {
        let policy = ReplicationOptions::from_env()?;
        assert_eq!(policy.flush_interval, Duration::from_millis(17));
        assert_eq!(policy.durability_timeout, Duration::from_secs(3));
        assert_eq!(policy.epoch_gc_grace, Some(Duration::from_secs(7)));
        assert_eq!(policy.hydrate_bytes_per_second, 2 << 20);
        assert_eq!(policy.paged_min_bytes, 5 << 20);
        assert!(!policy.paged_restore);
        assert_eq!(policy.truncate_pages, 0);
        assert_eq!(policy.coverage_cache_entries, 2);
        assert_eq!(
            policy.compaction,
            Some(CompactionConfig {
                min_txids: 2,
                min_bytes: 1 << 20,
                concurrency: 1,
            })
        );
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let directory = tempfile::tempdir()?;
        let storage = ReplicaStorage::new(
            Arc::new(object_store::memory::InMemory::new()),
            Default::default(),
        )?;
        let replica = domain
            .run(async { Replication::start(directory.path(), storage, policy.clone()) })
            .await??;
        for (name, invalid) in [
            ("CELLAR_LOG_FLUSH_MS", "unusable"),
            ("CELLAR_LTX_DURABILITY_TIMEOUT_SECS", "0"),
            ("CELLAR_LTX_HYDRATE_MBPS", "18446744073709551615"),
            ("CELLAR_LTX_PAGED_MIN_MB", "18446744073709551615"),
            ("CELLAR_LTX_PAGED", "unusable"),
            ("CELLAR_LTX_COMPACTION_MIN_MB", "18446744073709551615"),
            ("CELLAR_LTX_TRUNCATE_PAGES", "4294967296"),
            ("CELLAR_LTX_COVERAGE_CACHE_ENTRIES", "1048577"),
            ("CELLAR_LTX_COMPACTIONS", "0"),
        ] {
            let previous = std::env::var(name)?;
            std::env::set_var(name, invalid);
            ensure!(
                ReplicationOptions::from_env().is_err(),
                "accepted {name}={invalid}"
            );
            assert_eq!(replica.options(), &policy);
            std::env::set_var(name, previous);
        }
        std::env::set_var("CELLAR_LTX_COVERAGE_CACHE_ENTRIES", "0");
        assert_eq!(ReplicationOptions::from_env()?.coverage_cache_entries, 0);
        assert_eq!(replica.options(), &policy);
        std::env::set_var("CELLAR_LTX_RETENTION_SECS", "0");
        assert_eq!(ReplicationOptions::from_env()?.epoch_gc_grace, None);
        domain.shutdown().await;
        return Ok(());
    }
    let output = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "command_line_replication_adapter_validates_and_captures_policy",
            "--nocapture",
        ])
        .env_clear()
        .env(CHILD, "1")
        .env("CELLAR_LOG_FLUSH_MS", "17")
        .env("CELLAR_LTX_DURABILITY_TIMEOUT_SECS", "3")
        .env("CELLAR_LTX_RETENTION_SECS", "7")
        .env("CELLAR_LTX_HYDRATE_MBPS", "2")
        .env("CELLAR_LTX_PAGED_MIN_MB", "5")
        .env("CELLAR_LTX_PAGED", "0")
        .env("CELLAR_LTX_TRUNCATE_PAGES", "0")
        .env("CELLAR_LTX_COVERAGE_CACHE_ENTRIES", "2")
        .env("CELLAR_LTX_COMPACTION", "1")
        .env("CELLAR_LTX_COMPACTION_MIN_TXIDS", "2")
        .env("CELLAR_LTX_COMPACTION_MIN_MB", "1")
        .env("CELLAR_LTX_COMPACTIONS", "1")
        .output()?;
    ensure!(
        output.status.success(),
        "CLI replication adapter failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clean_reload_inventory_preserves_evicted_cache() -> Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let directory = tempfile::tempdir()?;
    let storage = ReplicaStorage::new(
        Arc::new(object_store::memory::InMemory::new()),
        Default::default(),
    )?;
    let replica = start(&domain, directory.path(), storage).await?;
    domain
        .run(async {
            let mut paths = std::collections::BTreeMap::new();
            for cell in [CELL, "Account:resident", "Account:stale"] {
                let active = replica
                    .ltx()
                    .activate(ActivationOptions {
                        cell,
                        epoch: 1,
                        fresh: true,
                        took_over: false,
                        resume_local: false,
                        prior: None,
                    })
                    .await?;
                let connection = rusqlite::Connection::open(&active.path)?;
                connection.execute_batch(
                    "CREATE TABLE state(value INTEGER); INSERT INTO state VALUES(7)",
                )?;
                drop(connection);
                paths.insert(cell, active.path);
                if cell == CELL {
                    replica.ltx().release(cell, 1).await?;
                } else {
                    replica.ltx().close_for_reload(cell, 1).await?;
                }
            }
            let cached = paths[CELL].with_extension("evicted");
            assert!(
                cached.is_file(),
                "ordinary eviction must create a real cache entry"
            );
            let keep = std::collections::BTreeSet::from([("Account:resident".to_string(), 1)]);
            assert_eq!(replica.ltx().prune_stale_live(&keep)?, 1);
            assert!(
                cached.is_file(),
                "a clean reload must retain the ordinary eviction cache"
            );
            assert!(paths["Account:resident"].is_file());
            assert!(!paths["Account:stale"].exists());
            let missing_live = std::collections::BTreeSet::from([(CELL.to_string(), 1)]);
            assert!(
                replica.ltx().prune_stale_live(&missing_live).is_err(),
                "a cache entry cannot stand in for a missing certified live database"
            );
            anyhow::Ok(())
        })
        .await??;
    domain.shutdown().await;
    Ok(())
}
