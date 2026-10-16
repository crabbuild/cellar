use super::*;
use crate::sdk::ExecutionDomain;
use anyhow::{ensure, Result};
use object_store::GetRange;

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
                    Bytes::from_static(b"original").into(),
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
