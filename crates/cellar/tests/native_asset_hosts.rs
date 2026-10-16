//! Asset resources retain explicit configuration and their serving host.
#![allow(clippy::disallowed_methods)] // Real filesystem and runtime probes.

#[allow(dead_code)]
#[path = "support/assets.rs"]
mod fixture;

use anyhow::{ensure, Result};
use cellar::sdk::{
    AssetCache, AssetCacheOptions, DirectFileSystem, ExecutionDomain, FileSystem, HostDirEntry,
    HostFile, HostFileIo, HostMetadata, HostServices,
};
use http_body_util::BodyExt as _;
use std::{
    io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};

struct Probe {
    paths: Mutex<Vec<PathBuf>>,
    reads: AtomicUsize,
    opens: AtomicUsize,
    creates: AtomicUsize,
    wrong_owner: AtomicBool,
    block: Mutex<bool>,
    release: Condvar,
    entered: tokio::sync::Notify,
}

impl Probe {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            paths: Mutex::new(Vec::new()),
            reads: AtomicUsize::new(0),
            opens: AtomicUsize::new(0),
            creates: AtomicUsize::new(0),
            wrong_owner: AtomicBool::new(false),
            block: Mutex::new(false),
            release: Condvar::new(),
            entered: tokio::sync::Notify::new(),
        })
    }

    fn record(&self, path: &Path) {
        self.paths.lock().unwrap().push(path.to_owned());
    }

    fn unblock(&self) {
        *self.block.lock().unwrap() = false;
        self.release.notify_all();
    }
}

struct UnblockOnDrop(Arc<Probe>);

impl Drop for UnblockOnDrop {
    fn drop(&mut self) {
        self.0.unblock();
    }
}

struct Filesystem(Arc<Probe>);

struct File {
    file: HostFile,
    probe: Arc<Probe>,
    owner: Arc<HostServices>,
}

impl HostFileIo for File {
    fn read_exact_at(&mut self, offset: u64, len: usize) -> io::Result<Vec<u8>> {
        self.probe.reads.fetch_add(1, Ordering::SeqCst);
        if !Arc::ptr_eq(&self.owner, &cellar::asyncrt::services()) {
            self.probe.wrong_owner.store(true, Ordering::SeqCst);
        }
        let mut block = self.probe.block.lock().unwrap();
        if *block {
            self.probe.entered.notify_one();
        }
        while *block {
            block = self.probe.release.wait(block).unwrap();
        }
        drop(block);
        self.file.read_exact_at(offset, len)
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.file.write_all(bytes)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    fn file_len(&mut self) -> io::Result<u64> {
        self.file.file_len()
    }
}

impl FileSystem for Filesystem {
    fn read_dir_iter(&self, p: &Path) -> io::Result<cellar::sdk::HostDirIterator> {
        self.0.record(p);
        DirectFileSystem.read_dir_iter(p)
    }

    fn temporary_file(&self, directory: Option<&Path>) -> io::Result<HostFile> {
        if let Some(path) = directory {
            self.0.record(path);
        }
        Ok(HostFile::from_io(File {
            file: DirectFileSystem.temporary_file(directory)?,
            probe: self.0.clone(),
            owner: cellar::asyncrt::services(),
        }))
    }

    fn read(&self, p: &Path) -> io::Result<Vec<u8>> {
        self.0.record(p);
        DirectFileSystem.read(p)
    }

    fn read_dir(&self, p: &Path) -> io::Result<Vec<HostDirEntry>> {
        self.0.record(p);
        DirectFileSystem.read_dir(p)
    }

    fn metadata(&self, p: &Path) -> io::Result<HostMetadata> {
        self.0.record(p);
        DirectFileSystem.metadata(p)
    }

    fn create(&self, p: &Path) -> io::Result<HostFile> {
        self.0.record(p);
        DirectFileSystem.create(p)
    }

    fn create_new(&self, p: &Path) -> io::Result<HostFile> {
        self.0.record(p);
        self.0.creates.fetch_add(1, Ordering::SeqCst);
        let mut block = self.0.block.lock().unwrap();
        if *block {
            self.0.entered.notify_one();
        }
        while *block {
            block = self.0.release.wait(block).unwrap();
        }
        drop(block);
        DirectFileSystem.create_new(p)
    }

    fn open(&self, p: &Path) -> io::Result<HostFile> {
        self.0.record(p);
        self.0.opens.fetch_add(1, Ordering::SeqCst);
        Ok(HostFile::from_io(File {
            file: DirectFileSystem.open(p)?,
            probe: self.0.clone(),
            owner: cellar::asyncrt::services(),
        }))
    }

    fn write(&self, p: &Path, bytes: &[u8]) -> io::Result<()> {
        self.0.record(p);
        DirectFileSystem.write(p, bytes)
    }

    fn sync_all(&self, p: &Path) -> io::Result<()> {
        self.0.record(p);
        DirectFileSystem.sync_all(p)
    }

    fn rename(&self, a: &Path, b: &Path) -> io::Result<()> {
        self.0.record(a);
        self.0.record(b);
        DirectFileSystem.rename(a, b)
    }

    fn remove_file(&self, p: &Path) -> io::Result<()> {
        self.0.record(p);
        DirectFileSystem.remove_file(p)
    }

    fn remove_dir_all(&self, p: &Path) -> io::Result<()> {
        self.0.record(p);
        DirectFileSystem.remove_dir_all(p)
    }

    fn create_dir_all(&self, p: &Path) -> io::Result<()> {
        self.0.record(p);
        DirectFileSystem.create_dir_all(p)
    }

    fn set_modified_unix_millis(&self, p: &Path, ms: i64) -> io::Result<()> {
        self.0.record(p);
        DirectFileSystem.set_modified_unix_millis(p, ms)
    }
}

#[test]
fn asset_loading_ignores_process_cache_configuration() -> Result<()> {
    const CHILD: &str = "CELLAR_ASSET_ENV_PROBE";
    if std::env::var_os(CHILD).is_none() {
        let status = std::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "asset_loading_ignores_process_cache_configuration",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("CELLAR_ASSET_CACHE_DIR", "/dev/null/unusable-asset-cache")
            .env("CELLAR_ASSET_CACHE_BYTES", "invalid-byte-count")
            .env("CELLAR_MAX_ASSET_FILE_BYTES", "invalid-file-limit")
            .status()?;
        ensure!(
            status.success(),
            "explicit asset cache inspected process configuration"
        );
        return Ok(());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let directory = tempfile::tempdir()?;
        let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
        let cache = AssetCache::new(AssetCacheOptions::new(directory.path().join("cache")))?;
        domain
            .run(async {
                let bucket =
                    cellar::bucket::Bucket::open_dev(&directory.path().join("bucket.sqlite"))?;
                let resolver =
                    fixture::resolver(&bucket, "explicit", b"host assets", "404-page", &cache)
                        .await?;
                let response = resolver
                    .ingress_response("/plain.txt", None, false, &Default::default())
                    .await?
                    .unwrap();
                ensure!(response.into_body().collect().await?.to_bytes() == "host assets");
                Ok::<_, anyhow::Error>(())
            })
            .await??;
        domain.shutdown().await;
        Ok(())
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn asset_streams_retain_their_owner_and_join_started_file_reads() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let owner = ExecutionDomain::new(tokio::runtime::Handle::current());
    let consumer = ExecutionDomain::new(tokio::runtime::Handle::current());
    let probe = Probe::new();
    let root = directory.path().join("caller-files");
    let cache = AssetCache::with_filesystem(
        AssetCacheOptions::new(&root),
        Arc::new(Filesystem(probe.clone())),
    )?;
    let bytes = vec![b'a'; 130_000];
    let resolver = owner
        .run(async {
            let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("bucket.sqlite"))?;
            fixture::resolver(&bucket, "owner", &bytes, "404-page", &cache).await
        })
        .await??;
    // The consumer deliberately polls outside the serving host's domain.
    let response = owner
        .run(resolver.ingress_response("/plain.txt", None, false, &Default::default()))
        .await??
        .unwrap();
    let body = consumer
        .run(async { response.into_body().collect().await })
        .await??;
    ensure!(body.to_bytes().as_ref() == bytes);
    ensure!(
        probe.reads.load(Ordering::SeqCst) == 2,
        "asset reads were not bounded to 64 KiB chunks"
    );
    ensure!(
        !probe.wrong_owner.load(Ordering::SeqCst),
        "body polling changed the filesystem's host"
    );

    let response = owner
        .run(resolver.ingress_response("/plain.txt", None, false, &Default::default()))
        .await??
        .unwrap();
    *probe.block.lock().unwrap() = true;
    let _unblock = UnblockOnDrop(probe.clone());
    let reading = consumer.spawn(async { response.into_body().collect().await })?;
    tokio::time::timeout(Duration::from_secs(5), probe.entered.notified()).await?;
    owner.close();
    let mut joining = Box::pin(owner.shutdown());
    let waiting = tokio::time::timeout(Duration::from_millis(50), &mut joining).await;
    // Release even on a failed assertion, so the runtime never hangs in Drop.
    probe.unblock();
    ensure!(
        waiting.is_err(),
        "shutdown returned with a caller filesystem read still running"
    );
    tokio::time::timeout(Duration::from_secs(5), joining).await?;
    ensure!(
        reading.await?.is_err(),
        "a closed host admitted another asset chunk"
    );
    ensure!(!probe.wrong_owner.load(Ordering::SeqCst));
    ensure!(probe
        .paths
        .lock()
        .unwrap()
        .iter()
        .all(|path| path.starts_with(&root)));
    ensure!(
        consumer.run(async { 42 }).await? == 42,
        "asset owner closed its consumer"
    );
    consumer.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_fallback_refresh_can_resume_on_a_surviving_host() -> Result<()> {
    use cellar::assets::{AssetResolver, FallbackSource};
    use cellar::protocol::{AssetIndex, AssetManifestRef};
    use sha2::{Digest as _, Sha256};
    let directory = tempfile::tempdir()?;
    let loading = ExecutionDomain::new(tokio::runtime::Handle::current());
    let owner = ExecutionDomain::new(tokio::runtime::Handle::current());
    let survivor = ExecutionDomain::new(tokio::runtime::Handle::current());
    let cache = AssetCache::new(AssetCacheOptions::new(
        directory.path().join("shared-cache"),
    ))?;
    let resolver = loading.run(async {
        let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("bucket.sqlite"))?;
        fixture::resolver(&bucket, "boot", b"boot", "404-page", &cache).await?;
        let original = bucket.get("boot/assets.json").await?.unwrap().0;
        let boot: AssetIndex = serde_json::from_slice(&original)?;
        let boot_ref = AssetManifestRef {
            index: "assets.json".into(), sha256: format!("{:x}", Sha256::digest(&original)),
            file_count: boot.entries.len() as u32, total_bytes: boot.entries.values().map(|entry| entry.bytes).sum(),
        };
        let mut newer = boot;
        let mut entry = newer.entries["/plain.txt"].clone();
        entry.bytes = 32;
        entry.sha256 = format!("{:x}", Sha256::digest([b'n'; 32]));
        bucket.put(&cellar::protocol::asset_blob_key(&entry.sha256).unwrap(), vec![b'n'; 32]).await?;
        newer.entries.clear();
        newer.entries.insert("/fresh.txt".into(), entry);
        let body = serde_json::to_vec(&newer)?;
        let reference = AssetManifestRef {
            index: "assets.json".into(), sha256: format!("{:x}", Sha256::digest(&body)), file_count: 1, total_bytes: 32,
        };
        bucket.put("later/assets.json", body).await?;
        let manifest = serde_json::json!({
            "schema_version":1,"version":"later","script_name":"assets","main_module":null,
            "do_classes":[],"sqlite_classes":[],"modules":[],"raw_metadata":{},"assets":reference,
        });
        bucket.put("later/manifest.json", serde_json::to_vec(&manifest)?).await?;
        bucket.put("deploy/current.json", serde_json::to_vec(&serde_json::json!({
            "script_name":"assets","version":"later","prefix":"later","rollout":{"percent":100},
        }))?).await?;
        // A distinct serving policy rejects the same newer index without
        // creating its cache directory or borrowing another host's ceiling.
        let strict_root = directory.path().join("strict-cache");
        let mut strict = AssetCacheOptions::new(&strict_root);
        strict.max_file_bytes = 10;
        let strict = AssetCache::new(strict)?;
        ensure!(AssetResolver::load(&bucket, "later", &reference, false, None, &strict).await.is_err());
        ensure!(!strict_root.exists());
        AssetResolver::load(&bucket, "boot", &boot_ref, false, Some(FallbackSource {
            pointer_key: "deploy/current.json".into(), boot_prefix: "boot".into(),
        }), &cache).await
    }).await??;
    loading.shutdown().await;
    // On this current-thread runtime the refresh cannot poll before we close
    // its owner in the same turn. Closure must cancel it and release admission.
    owner
        .run(async {
            ensure!(resolver
                .ingress_response("/fresh.txt", None, false, &Default::default())
                .await?
                .is_none());
            owner.close();
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    owner.shutdown().await;
    survivor
        .run(async {
            ensure!(
                resolver
                    .ingress_response("/fresh.txt", None, false, &Default::default())
                    .await?
                    .is_none(),
                "a refresh outlived its closed host"
            );
            tokio::time::sleep(Duration::from_millis(5100)).await;
            let response = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    if let Some(response) = resolver
                        .ingress_response("/fresh.txt", None, false, &Default::default())
                        .await?
                    {
                        return Ok::<_, anyhow::Error>(response);
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await??;
            ensure!(response.into_body().collect().await?.to_bytes().as_ref() == [b'n'; 32]);
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    survivor.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn cancelled_publication_keeps_shared_cache_downloads_serialized() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let loading = ExecutionDomain::new(tokio::runtime::Handle::current());
    let first = ExecutionDomain::new(tokio::runtime::Handle::current());
    let second = ExecutionDomain::new(tokio::runtime::Handle::current());
    let probe = Probe::new();
    let cache = AssetCache::with_filesystem(
        AssetCacheOptions::new(directory.path().join("cache")),
        Arc::new(Filesystem(probe.clone())),
    )?;
    let resolver = loading
        .run(async {
            let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("bucket.sqlite"))?;
            fixture::resolver(&bucket, "shared", b"one publication", "404-page", &cache).await
        })
        .await??;
    loading.shutdown().await;
    *probe.block.lock().unwrap() = true;
    let _unblock = UnblockOnDrop(probe.clone());
    let original = resolver.clone();
    let writing = first.spawn(async move {
        original
            .ingress_response("/plain.txt", None, false, &Default::default())
            .await
    })?;
    tokio::time::timeout(Duration::from_secs(5), probe.entered.notified()).await?;
    let reading = second.spawn(async move {
        let response = resolver
            .ingress_response("/plain.txt", None, false, &Default::default())
            .await?
            .unwrap();
        Ok::<_, anyhow::Error>(response.into_body().collect().await?.to_bytes())
    })?;
    tokio::time::timeout(Duration::from_secs(5), async {
        // Both initial reads and the publisher's locked recheck missed before
        // bytes became visible; the second host must wait for this publication.
        while probe.opens.load(Ordering::SeqCst) < 3 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    first.close();
    let mut joining = Box::pin(first.shutdown());
    let waiting = tokio::time::timeout(Duration::from_millis(50), &mut joining).await;
    probe.unblock();
    ensure!(
        waiting.is_err(),
        "shutdown left a filesystem publication running"
    );
    tokio::time::timeout(Duration::from_secs(5), joining).await?;
    ensure!(
        writing.await.is_err(),
        "cancelled host finished its response"
    );
    ensure!(tokio::time::timeout(Duration::from_secs(5), reading).await??? == "one publication");
    ensure!(
        probe.creates.load(Ordering::SeqCst) == 1,
        "cancelled publication released download coordination before its file work completed"
    );
    ensure!(
        !probe.wrong_owner.load(Ordering::SeqCst),
        "shared cache file reads borrowed the publisher's closed host"
    );
    second.shutdown().await;
    Ok(())
}
