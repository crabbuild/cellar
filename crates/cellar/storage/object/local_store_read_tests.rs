// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::sdk::ExecutionDomain;
use anyhow::{ensure, Result};
use object_store::GetRange;

#[tokio::test]
async fn stored_ltx_bytes_do_not_retain_upload_request_capacity() -> Result<()> {
    use cellar_ltx::{
        client::object_store::{ObjectStoreClient, ObjectStoreConfig},
        ltx, ReplicaClient, TXID,
    };
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let guard = crate::bucket::IoGuard::new(Arc::new(slots.clone().acquire_owned().await?));
    let mut extensions = object_store::Extensions::new();
    extensions.insert(guard);
    let store = Arc::new(object_store::memory::InMemory::new());
    let client = ObjectStoreClient::with_store(ObjectStoreConfig::default(), store.clone())
        .with_upload_extensions(extensions);
    let header = ltx::Header {
        version: ltx::VERSION,
        flags: ltx::HEADER_FLAG_NO_CHECKSUM,
        page_size: 512,
        commit: 1,
        min_txid: TXID(1),
        max_txid: TXID(1),
        ..Default::default()
    };
    let bytes = ltx::encode_file(&header, &[(1, vec![0x5a; 512])], 0)?;
    client.write_ltx_file(0, TXID(1), TXID(1), &bytes).await?;
    drop(client);
    let permit = slots.try_acquire().map_err(|error| {
        anyhow::anyhow!("stored payload retained completed request capacity: {error}")
    })?;
    let reader = ObjectStoreClient::with_store(ObjectStoreConfig::default(), store);
    ensure!(reader.open_ltx_file(0, TXID(1), TXID(1)).await? == bytes);
    drop(permit);
    Ok(())
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn head_does_not_materialize_the_object() -> Result<()> {
    profile("head", "head_does_not_materialize_the_object").await
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn range_does_not_materialize_the_object() -> Result<()> {
    profile("range", "range_does_not_materialize_the_object").await
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn full_reads_stream_without_materializing_the_object() -> Result<()> {
    profile("full", "full_reads_stream_without_materializing_the_object").await
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn profile(mode: &str, test: &str) -> Result<()> {
    use crate::bucket::Bucket;
    let head = mode == "head";

    const CHILD: &str = "CELLAR_TEST_LOCAL_RANGE_RSS_PATH";
    const OBJECT_BYTES: usize = 80 << 20;
    const READ_BYTES: usize = 4096;
    let Some(database) = std::env::var_os(CHILD) else {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("objects.sqlite");
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let prepared = domain
            .run(async {
                let bucket = Bucket::open_dev(&path)?;
                bucket.put("large", vec![0x5a; OBJECT_BYTES]).await
            })
            .await;
        domain.shutdown().await;
        prepared??;
        // Populate outside the measured process so upload allocations cannot
        // conceal a full-object read in its RSS baseline.
        let output = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                &format!("{}::{test}", module_path!().split_once("::").unwrap().1),
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD, &path)
            .output()?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        eprintln!("{stderr}");
        ensure!(
            output.status.success() && stdout.contains("1 passed; 0 failed"),
            "isolated local-store profile failed: {stdout}\n{stderr}"
        );
        return Ok(());
    };
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let bucket = Bucket::open_dev(std::path::Path::new(&database))?;
            let baseline = crate::memory::sample().rss_bytes;
            ensure!(baseline > 0, "RSS unavailable");
            let response = bucket
                .store
                .get_opts(
                    &Path::from("large"),
                    GetOptions {
                        head,
                        range: (mode == "range")
                            .then_some(GetRange::Bounded(123..123 + READ_BYTES as u64)),
                        ..Default::default()
                    },
                )
                .await?;
            ensure!(response.meta.size == OBJECT_BYTES as u64);
            let mut peak = crate::memory::sample().rss_bytes;
            std::hint::black_box(&response);
            if mode == "full" {
                let mut stream = response.into_stream();
                let mut total = 0;
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk?;
                    ensure!(!chunk.is_empty() && chunk.len() <= READ_CHUNK_BYTES);
                    ensure!(chunk.iter().all(|byte| *byte == 0x5a));
                    total += chunk.len();
                    peak = peak.max(crate::memory::sample().rss_bytes);
                }
                ensure!(total == OBJECT_BYTES);
            } else if !head {
                ensure!(response.range == (123..123 + READ_BYTES as u64));
                let bytes = response.bytes().await?;
                ensure!(bytes.len() == READ_BYTES && bytes.iter().all(|byte| *byte == 0x5a));
                peak = peak.max(crate::memory::sample().rss_bytes);
                std::hint::black_box(&bytes);
            }
            Ok::<_, anyhow::Error>((baseline, peak))
        })
        .await;
    domain.shutdown().await;
    let (baseline, peak) = result??;
    let growth = peak.saturating_sub(baseline);
    eprintln!("local-store mode={mode}: baseline={baseline} peak={peak} growth={growth}");
    ensure!(
        growth <= 16 << 20,
        "local metadata/range read retained {growth} bytes, budget {}",
        16 << 20
    );
    Ok(())
}

#[tokio::test]
async fn head_and_ranges_keep_metadata_conditions_and_exact_bytes() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let store = LocalStore::open(directory.path().join("objects.sqlite"))?;
            let path = Path::from("key");
            let accepted = store
                .put_opts(
                    &path,
                    Bytes::from_static(b"0123456789").into(),
                    PutOptions::default(),
                )
                .await?;
            let head = store
                .get_opts(
                    &path,
                    GetOptions {
                        head: true,
                        if_match: accepted.e_tag.clone(),
                        ..Default::default()
                    },
                )
                .await?;
            ensure!(head.meta.e_tag == accepted.e_tag && head.meta.size == 10);
            ensure!(head.bytes().await?.is_empty());
            for (range, expected) in [
                (None, b"0123456789".as_slice()),
                (Some(GetRange::Bounded(2..7)), b"23456".as_slice()),
                (Some(GetRange::Offset(8)), b"89".as_slice()),
                (Some(GetRange::Suffix(3)), b"789".as_slice()),
                (Some(GetRange::Bounded(8..100)), b"89".as_slice()),
            ] {
                let reply = store
                    .get_opts(
                        &path,
                        GetOptions {
                            range,
                            if_match: accepted.e_tag.clone(),
                            ..Default::default()
                        },
                    )
                    .await?;
                ensure!(reply.meta.size == 10 && reply.meta.e_tag == accepted.e_tag);
                ensure!(reply.bytes().await?.as_ref() == expected);
            }
            let refused = store
                .get_opts(
                    &path,
                    GetOptions {
                        if_match: Some("wrong".into()),
                        ..Default::default()
                    },
                )
                .await;
            ensure!(matches!(refused, Err(Error::Precondition { .. })));
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result??;
    Ok(())
}

#[tokio::test]
async fn replacing_a_body_mid_read_refuses_instead_of_mixing_versions() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let store = LocalStore::open(directory.path().join("objects.sqlite"))?;
            let path = Path::from("key");
            store
                .put_opts(
                    &path,
                    vec![7_u8; READ_CHUNK_BYTES * 3].into(),
                    PutOptions::default(),
                )
                .await?;
            let response = store.get_opts(&path, GetOptions::default()).await?;
            let mut body = response.into_stream();
            let first = body.next().await.unwrap()?;
            ensure!(first.len() == READ_CHUNK_BYTES && first.iter().all(|byte| *byte == 7));
            store
                .put_opts(
                    &path,
                    vec![8_u8; READ_CHUNK_BYTES * 3].into(),
                    PutOptions::default(),
                )
                .await?;
            ensure!(matches!(
                body.next().await,
                Some(Err(Error::Precondition { .. }))
            ));
            ensure!(body.next().await.is_none());
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result??;
    Ok(())
}

#[tokio::test]
async fn small_get_keeps_metadata_and_original_bytes_across_replacement() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain
        .run(async {
            let store = LocalStore::open(directory.path().join("lease.sqlite"))?;
            let key = Path::from("nodes/leader.json");
            for length in [0, 8, READ_CHUNK_BYTES] {
                let original = vec![0x51; length];
                let accepted = store
                    .put_opts(&key, original.clone().into(), PutOptions::default())
                    .await?;
                // No conditional GET: an ordinary lease renewal must not
                // invalidate the body of an already returned small response.
                let response = store.get_opts(&key, GetOptions::default()).await?;
                ensure!(response.meta.e_tag == accepted.e_tag);
                ensure!(response.meta.size == length as u64);
                let ranged = if length > 2 {
                    Some(
                        store
                            .get_opts(
                                &key,
                                GetOptions {
                                    range: Some(GetRange::Bounded(1..length as u64 - 1)),
                                    ..Default::default()
                                },
                            )
                            .await?,
                    )
                } else {
                    None
                };
                let replacement = store
                    .put_opts(&key, vec![0xa3; length + 1].into(), PutOptions::default())
                    .await?;
                ensure!(replacement.e_tag != accepted.e_tag);
                let head = store.head(&key).await?;
                ensure!(head.e_tag == replacement.e_tag && head.size == length as u64 + 1);
                ensure!(response.bytes().await?.as_ref() == original.as_slice());
                if let Some(response) = ranged {
                    ensure!(response.meta.e_tag == accepted.e_tag);
                    ensure!(response.bytes().await?.as_ref() == &original[1..length - 1]);
                }
            }
            Ok::<_, anyhow::Error>(())
        })
        .await;
    domain.shutdown().await;
    result??;
    Ok(())
}

pub(super) struct ReadGate {
    expected: Arc<crate::host_services::HostServices>,
    entered: tokio::sync::Notify,
    released: Mutex<bool>,
    wake: std::sync::Condvar,
    calls: std::sync::atomic::AtomicUsize,
}

impl std::fmt::Debug for ReadGate {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ReadGate")
    }
}

impl ReadGate {
    fn new(domain: &ExecutionDomain) -> Arc<Self> {
        Arc::new(Self {
            expected: domain.services(),
            entered: tokio::sync::Notify::new(),
            released: Mutex::new(false),
            wake: std::sync::Condvar::new(),
            calls: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    pub(super) fn wait(&self) {
        assert!(Arc::ptr_eq(&self.expected, &crate::asyncrt::services()));
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.entered.notify_one();
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.wake.wait(released).unwrap();
        }
    }

    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.wake.notify_all();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn large_body_keeps_lease_cas_progress_and_atomic_visibility() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = ReadGate::new(&domain);
    let result = domain
        .run(async {
            let mut store = LocalStore::open(directory.path().join("objects.sqlite"))?;
            let key = Path::from("large");
            let lease = Path::from("nodes/owner");
            store
                .put_opts(
                    &key,
                    Bytes::from_static(b"old").into(),
                    PutOptions::default(),
                )
                .await?;
            let token = store
                .put_opts(
                    &lease,
                    Bytes::from_static(b"old lease").into(),
                    PutOptions::default(),
                )
                .await?;
            store.body_gate = Some((key.to_string(), gate.clone()));
            let writer_store = store.clone();
            let writer_key = key.clone();
            let writer = crate::asyncrt::try_spawn(async move {
                writer_store
                    .put_opts(
                        &writer_key,
                        Bytes::from(vec![0x5a; READ_CHUNK_BYTES * 4]).into(),
                        PutOptions::default(),
                    )
                    .await
            })?;
            crate::asyncrt::timeout(Duration::from_secs(5), gate.entered.notified()).await?;
            let before = store.get(&key).await?.bytes().await?;
            let lease_store = store.clone();
            let mut renewal = crate::asyncrt::try_spawn(async move {
                lease_store
                    .put_opts(
                        &lease,
                        Bytes::from_static(b"renewed").into(),
                        PutMode::Update(object_store::UpdateVersion {
                            e_tag: token.e_tag,
                            version: None,
                        })
                        .into(),
                    )
                    .await
            })?;
            let observed = crate::asyncrt::timeout(Duration::from_millis(333), &mut renewal).await;
            let timely = observed.is_ok();
            gate.release();
            writer.await??;
            match observed {
                Ok(result) => result??,
                Err(_) => renewal.await??,
            };
            ensure!(before.as_ref() == b"old", "partial object became visible");
            ensure!(
                store.get(&key).await?.bytes().await?.as_ref() == vec![0x5a; READ_CHUNK_BYTES * 4]
            );
            ensure!(
                timely,
                "lease CAS waited for the remainder of an unrelated large body"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await;
    gate.release();
    domain.shutdown().await;
    result?
}

#[cfg(not(cellar_internal_tests))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn body_reads_retain_their_original_domain_and_join_entered_io() -> Result<()> {
    use futures_util::FutureExt as _;
    let directory = tempfile::tempdir()?;
    let owner = ExecutionDomain::new(tokio::runtime::Handle::current());
    let consumer = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = ReadGate::new(&owner);
    let prepared = owner
        .run(async {
            let mut store = LocalStore::open(directory.path().join("objects.sqlite"))?;
            let key = Path::from("key");
            store
                .put_opts(
                    &key,
                    vec![0x51; READ_CHUNK_BYTES + 1].into(),
                    PutOptions::default(),
                )
                .await?;
            store.read_gate = Some(gate.clone());
            store.get_opts(&key, GetOptions::default()).await
        })
        .await;
    let response = prepared
        .map_err(anyhow::Error::from)
        .and_then(|response| response.map_err(anyhow::Error::from));
    if response.is_err() {
        gate.release();
        owner.shutdown().await;
        consumer.shutdown().await;
    }
    let mut body = response?.into_stream();
    let task = match consumer.spawn(async move { body.next().await }) {
        Ok(task) => task,
        Err(error) => {
            gate.release();
            owner.shutdown().await;
            consumer.shutdown().await;
            return Err(error.into());
        }
    };
    // The caller's test driver, outside both domains, owns this guard.
    #[allow(clippy::disallowed_methods)]
    let entered = tokio::time::timeout(Duration::from_secs(2), gate.entered.notified()).await;
    owner.close();
    let mut joined = Box::pin(owner.shutdown());
    let remained_joined = joined.as_mut().now_or_never().is_none();
    gate.release();
    joined.await;
    let completed = task.await;
    let consumer_alive = consumer.run(async { true }).await;
    consumer.shutdown().await;
    entered?;
    ensure!(
        remained_joined,
        "shutdown returned before entered SQLite I/O was released"
    );
    let error = completed?
        .ok_or_else(|| anyhow::anyhow!("body returned no result"))?
        .err()
        .ok_or_else(|| anyhow::anyhow!("closed owner delivered body bytes"))?;
    ensure!(
        std::error::Error::source(&error)
            .is_some_and(|source| source.is::<crate::asyncrt::DomainClosed>()),
        "{error}"
    );
    ensure!(consumer_alive?);
    ensure!(gate.calls.load(std::sync::atomic::Ordering::SeqCst) == 1);
    Ok(())
}

#[cfg(not(cellar_internal_tests))]
#[tokio::test]
async fn closed_response_owner_refuses_a_new_read_on_another_domain() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let owner = ExecutionDomain::new(tokio::runtime::Handle::current());
    let consumer = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = ReadGate::new(&owner);
    let response = owner
        .run(async {
            let mut store = LocalStore::open(directory.path().join("objects.sqlite"))?;
            let key = Path::from("key");
            store
                .put_opts(
                    &key,
                    Bytes::from_static(b"original").into(),
                    PutOptions::default(),
                )
                .await?;
            store.read_gate = Some(gate.clone());
            store.get_opts(&key, GetOptions::default()).await
        })
        .await;
    owner.shutdown().await;
    gate.release();
    let response = response
        .map_err(anyhow::Error::from)
        .and_then(|response| response.map_err(anyhow::Error::from));
    if response.is_err() {
        consumer.shutdown().await;
    }
    let mut body = response?.into_stream();
    let result = consumer.run(async { body.next().await }).await;
    let consumer_alive = consumer.run(async { true }).await;
    consumer.shutdown().await;
    let error = result?
        .ok_or_else(|| anyhow::anyhow!("body returned no result"))?
        .err()
        .ok_or_else(|| anyhow::anyhow!("closed owner started body I/O"))?;
    ensure!(
        std::error::Error::source(&error)
            .is_some_and(|source| source.is::<crate::asyncrt::DomainClosed>()),
        "{error}"
    );
    ensure!(consumer_alive?);
    ensure!(gate.calls.load(std::sync::atomic::Ordering::SeqCst) == 0);
    Ok(())
}

#[cfg(not(cellar_internal_tests))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_body_reader_retains_its_native_io_reservation() -> Result<()> {
    cancelled_reserved_read(false).await
}

#[cfg(not(cellar_internal_tests))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_metadata_reader_retains_its_native_io_reservation() -> Result<()> {
    cancelled_reserved_read(true).await
}

#[cfg(not(cellar_internal_tests))]
async fn cancelled_reserved_read(metadata: bool) -> Result<()> {
    use futures_util::FutureExt as _;
    let directory = tempfile::tempdir()?;
    let owner = ExecutionDomain::new(tokio::runtime::Handle::current());
    let gate = ReadGate::new(&owner);
    let prepared = owner
        .run(async {
            let mut store = LocalStore::open(directory.path().join("reserved.sqlite"))?;
            store
                .put_opts(
                    &Path::from("key"),
                    if metadata {
                        b"original".to_vec()
                    } else {
                        vec![0x51; READ_CHUNK_BYTES + 1]
                    }
                    .into(),
                    PutOptions::default(),
                )
                .await?;
            if metadata {
                store.metadata_gate = Some(gate.clone());
            } else {
                store.read_gate = Some(gate.clone());
            }
            Ok::<_, anyhow::Error>(store)
        })
        .await;
    let prepared = prepared
        .map_err(anyhow::Error::from)
        .and_then(|result| result);
    if prepared.is_err() {
        gate.release();
        owner.shutdown().await;
    }
    let store = prepared?;
    let slots = Arc::new(tokio::sync::Semaphore::new(1));
    let permit = Arc::new(slots.clone().try_acquire_owned()?);
    let mut options = GetOptions {
        head: metadata,
        ..Default::default()
    };
    options
        .extensions
        .insert(crate::bucket::IoGuard::new(permit));
    let task = match owner.spawn(async move {
        let response = store.get_opts(&Path::from("key"), options).await?;
        response.bytes().await?;
        Ok::<_, anyhow::Error>(())
    }) {
        Ok(task) => task,
        Err(error) => {
            gate.release();
            owner.shutdown().await;
            return Err(error.into());
        }
    };
    // The test driver owns the guard and always releases/joins native I/O.
    #[allow(clippy::disallowed_methods)]
    let entered = tokio::time::timeout(Duration::from_secs(2), gate.entered.notified()).await;
    task.abort();
    let cancelled = task.await;
    let reserved = slots.available_permits() == 0;
    owner.close();
    let mut shutdown = Box::pin(owner.shutdown());
    let joined = shutdown.as_mut().now_or_never().is_none();
    gate.release();
    shutdown.await;
    entered?;
    ensure!(cancelled.is_err(), "caller did not cancel");
    ensure!(
        reserved,
        "cancelled read returned admission while SQLite I/O still ran"
    );
    ensure!(joined, "shutdown returned before SQLite I/O joined");
    ensure!(slots.available_permits() == 1);
    ensure!(gate.calls.load(std::sync::atomic::Ordering::SeqCst) == 1);
    Ok(())
}

/// A caller-owned fault control. Dropping it always unblocks entered I/O.
#[cfg(not(cellar_internal_tests))]
pub(crate) struct UploadGate(Arc<ReadGate>);

#[cfg(not(cellar_internal_tests))]
impl UploadGate {
    pub(crate) async fn entered(&self) {
        self.0.entered.notified().await;
    }

    pub(crate) fn release(&self) {
        self.0.release();
    }

    pub(crate) fn calls(&self) -> usize {
        self.0.calls.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(not(cellar_internal_tests))]
impl Drop for UploadGate {
    fn drop(&mut self) {
        self.release();
    }
}

#[cfg(not(cellar_internal_tests))]
pub(crate) fn held_upload_store(
    database: &std::path::Path,
    domain: &ExecutionDomain,
    prefix: &str,
) -> Result<(Arc<dyn ObjectStore>, UploadGate)> {
    let gate = ReadGate::new(domain);
    let mut store = LocalStore::open(database)?;
    store.put_gate = Some((prefix.to_owned(), gate.clone()));
    Ok((Arc::new(store), UploadGate(gate)))
}

// The single runtime thread must remain available while a real listing owns
// its SQLite connection. The external watchdog only prevents a broken adapter
// from deadlocking the regression; its release is explicitly a test failure.
#[cfg(not(cellar_internal_tests))]
#[tokio::test]
async fn listings_yield_and_join_entered_sqlite_work() -> Result<()> {
    use futures_util::{FutureExt as _, TryStreamExt as _};
    for kind in ["stream", "delimiter", "page"] {
        let directory = tempfile::tempdir()?;
        let owner = ExecutionDomain::new(tokio::runtime::Handle::current());
        let gate = ReadGate::new(&owner);
        let mut store = LocalStore::open(directory.path().join("list.sqlite"))?;
        store.put_sync("selected/key".into(), Bytes::new(), PutOptions::default())?;
        store.list_gate = Some(gate.clone());
        let (release, released) = std::sync::mpsc::channel();
        let watchdog_gate = gate.clone();
        let watchdog = std::thread::spawn(move || {
            let requested = released.recv_timeout(Duration::from_secs(3)).is_ok();
            watchdog_gate.release();
            requested
        });
        let task = owner.spawn(async move {
            let prefix = Path::from("selected");
            let objects = match kind {
                "stream" => store.list(Some(&prefix)).try_collect::<Vec<_>>().await?,
                "delimiter" => store.list_with_delimiter(Some(&prefix)).await?.objects,
                _ => {
                    store
                        .list_paginated(Some("selected/"), PaginatedListOptions::default())
                        .await?
                        .result
                        .objects
                }
            };
            Ok::<_, anyhow::Error>(objects)
        })?;
        #[allow(clippy::disallowed_methods)]
        let entered = tokio::time::timeout(Duration::from_secs(2), gate.entered.notified()).await;
        let yielded = !*gate.released.lock().unwrap();
        task.abort();
        let cancelled = task.await;
        owner.close();
        let mut shutdown = Box::pin(owner.shutdown());
        let joining = shutdown.as_mut().now_or_never().is_none();
        let _ = release.send(());
        if joining {
            shutdown.await;
        }
        let timely = watchdog.join().expect("listing watchdog panicked");
        entered?;
        ensure!(
            yielded && timely,
            "{kind} listing blocked the async runtime"
        );
        ensure!(cancelled.is_err(), "{kind} caller did not cancel");
        ensure!(joining, "{kind} shutdown did not join entered SQLite work");
        ensure!(gate.calls.load(std::sync::atomic::Ordering::SeqCst) == 1);
    }
    Ok(())
}

#[cfg(not(cellar_internal_tests))]
#[tokio::test]
async fn listing_stream_keeps_its_original_domain_and_refuses_a_closed_owner() -> Result<()> {
    use futures_util::TryStreamExt as _;
    for closed in [false, true] {
        let directory = tempfile::tempdir()?;
        let owner = ExecutionDomain::new(tokio::runtime::Handle::current());
        let consumer = ExecutionDomain::new(tokio::runtime::Handle::current());
        let gate = ReadGate::new(&owner);
        gate.release();
        let mut store = LocalStore::open(directory.path().join("list.sqlite"))?;
        store.put_sync("selected/key".into(), Bytes::new(), PutOptions::default())?;
        store.list_gate = Some(gate.clone());
        let listing = owner.run(async { store.list(None) }).await?;
        if closed {
            owner.shutdown().await;
        }
        let result = consumer
            .run(async { listing.try_collect::<Vec<_>>().await })
            .await;
        consumer.shutdown().await;
        owner.shutdown().await;
        let result = result?;
        if closed {
            ensure!(
                result.is_err(),
                "closed owner admitted listing I/O on another domain"
            );
            ensure!(gate.calls.load(std::sync::atomic::Ordering::SeqCst) == 0);
        } else {
            let objects = result?;
            ensure!(objects.len() == 1 && objects[0].location == Path::from("selected/key"));
            ensure!(gate.calls.load(std::sync::atomic::Ordering::SeqCst) == 1);
        }
    }
    Ok(())
}
