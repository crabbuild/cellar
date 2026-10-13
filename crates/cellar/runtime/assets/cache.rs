//! Caller-selected asset storage. Work belongs to the serving execution domain.

use crate::bucket::Bucket;
use crate::protocol::{asset_blob_key, AssetEntry};
use anyhow::Context as _;
use cellar_ltx::{FileSystem, HostFile};
use futures_util::Stream;
use rand::RngCore as _;
use sha2::{Digest as _, Sha256};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

/// No default directory is inferred from the process or environment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AssetCacheOptions {
    pub directory: PathBuf,
    /// Cleanup target. Open files and failed deletions can retain extra bytes.
    /// Zero discards stored assets after opening their response handles.
    pub max_bytes: u64,
    /// Per-file serving and fallback-index limit, captured before loading.
    pub max_file_bytes: u64,
}

impl AssetCacheOptions {
    pub fn new(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
            max_bytes: 512 * 1024 * 1024,
            max_file_bytes: 25 * 1024 * 1024,
        }
    }

    /// Explicit CLI adapter; capture once before loading deployments.
    pub fn from_env(directory: impl Into<PathBuf>) -> anyhow::Result<Self> {
        let directory = std::env::var_os("CELLAR_ASSET_CACHE_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| directory.into());
        let max_bytes =
            crate::env_vars::with_default("CELLAR_ASSET_CACHE_BYTES", 512 * 1024 * 1024)?;
        Ok(Self {
            directory,
            max_bytes,
            max_file_bytes: crate::env_vars::max_asset_file_bytes()?,
        })
    }
}

/// Clone this capability to deliberately share one cache and cleanup target
/// across scripts or nodes. Each read and maintenance task retains its serving
/// execution domain; the cache does not own the caller's executor or filesystem.
#[derive(Clone)]
pub struct AssetCache(Arc<CacheInner>);

struct CacheInner {
    options: AssetCacheOptions,
    filesystem: Arc<dyn FileSystem>,
    downloads: DownloadLocks,
    cleanup: Arc<tokio::sync::Mutex<()>>,
}

type DownloadLocks = Arc<Mutex<HashMap<String, DownloadSlot>>>;
struct DownloadSlot {
    lock: Arc<tokio::sync::Mutex<()>>,
    users: usize,
}

struct Download {
    digest: String,
    lock: Arc<tokio::sync::Mutex<()>>,
    locks: DownloadLocks,
}
impl Drop for Download {
    fn drop(&mut self) {
        let mut locks = self.locks.lock().unwrap();
        // Count admitted callers explicitly: an owned lock future/guard
        // also retains an Arc, including while cancellation drops its fields.
        // A begun publication retains this admission inside its file closure.
        let slot = locks.get_mut(&self.digest).unwrap();
        slot.users -= 1;
        if slot.users == 0 {
            locks.remove(&self.digest);
        }
    }
}

impl AssetCache {
    pub fn new(options: AssetCacheOptions) -> anyhow::Result<Self> {
        Self::with_filesystem(options, Arc::new(cellar_ltx::DirectFileSystem))
    }

    pub fn with_filesystem(
        options: AssetCacheOptions,
        filesystem: Arc<dyn FileSystem>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !options.directory.as_os_str().is_empty(),
            "asset cache directory must not be empty"
        );
        anyhow::ensure!(
            options.max_file_bytes > 0,
            "asset file limit must be positive"
        );
        Ok(Self(Arc::new(CacheInner {
            options,
            filesystem,
            downloads: Arc::new(Mutex::new(HashMap::new())),
            cleanup: Arc::new(tokio::sync::Mutex::new(())),
        })))
    }

    pub fn options(&self) -> &AssetCacheOptions {
        &self.0.options
    }

    fn download(&self, digest: &str) -> Download {
        let lock = {
            let mut locks = self.0.downloads.lock().unwrap();
            let slot = locks
                .entry(digest.to_owned())
                .or_insert_with(|| DownloadSlot {
                    lock: Arc::new(tokio::sync::Mutex::new(())),
                    users: 0,
                });
            slot.users += 1;
            slot.lock.clone()
        };
        Download {
            digest: digest.to_owned(),
            lock,
            locks: self.0.downloads.clone(),
        }
    }

    async fn cached(&self, path: PathBuf, bytes: u64) -> anyhow::Result<Option<HostFile>> {
        let filesystem = self.0.filesystem.clone();
        crate::asyncrt::try_blocking(move || match filesystem.open(&path) {
            Ok(mut file) => {
                if file.file_len()? == bytes {
                    Ok(Some(file))
                } else {
                    drop(file);
                    let _ = filesystem.remove_file(&path);
                    Ok(None)
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => {
                Err(error).with_context(|| format!("open asset cache file {}", path.display()))
            }
        })?
        .await
        .context("join asset cache read")?
    }

    fn touch(&self, path: PathBuf) {
        let filesystem = self.0.filesystem.clone();
        let millis = crate::asyncrt::wall_ms();
        if let Ok(task) = crate::asyncrt::try_blocking(move || {
            let _ = filesystem.set_modified_unix_millis(&path, millis);
        }) {
            task.detach();
        }
    }

    pub(super) async fn open(
        &self,
        bucket: &Bucket,
        asset_path: &str,
        entry: &AssetEntry,
    ) -> anyhow::Result<HostFile> {
        let key = asset_blob_key(&entry.sha256).context("invalid asset cache digest")?;
        let path = self
            .0
            .options
            .directory
            .join(&entry.sha256[..2])
            .join(&entry.sha256);
        if let Some(file) = self.cached(path.clone(), entry.bytes).await? {
            self.touch(path);
            return Ok(file);
        }
        let download = self.download(&entry.sha256);
        let guard = download.lock.clone().lock_owned().await;
        if let Some(file) = self.cached(path.clone(), entry.bytes).await? {
            self.touch(path);
            return Ok(file);
        }
        let (body, _) = bucket
            .get(&key)
            .await
            .with_context(|| format!("read asset {asset_path}"))?
            .with_context(|| {
                format!(
                    "asset {asset_path} missing from {}://{}/{key}",
                    bucket.scheme(),
                    bucket.name
                )
            })?;
        anyhow::ensure!(
            body.len() as u64 == entry.bytes,
            "asset {asset_path} has the wrong bucket size"
        );
        let digest = format!("{:x}", Sha256::digest(&body));
        anyhow::ensure!(
            digest == entry.sha256,
            "asset {asset_path} checksum mismatch: expected {}, got {digest}",
            entry.sha256
        );
        let parent = path
            .parent()
            .context("asset cache has no parent")?
            .to_path_buf();
        let temporary = parent.join(format!(
            ".{}.{}.tmp",
            crate::asyncrt::process_tag(),
            crate::asyncrt::rng("asset-cache-file").next_u64()
        ));
        let filesystem = self.0.filesystem.clone();
        let publish_path = path.clone();
        let file = crate::asyncrt::try_blocking(move || {
            // A begun publication keeps its download admission until file
            // work completes, even if its serving async task is cancelled.
            let _download = download;
            let _guard = guard;
            filesystem.create_dir_all(&parent)?;
            let mut file = filesystem.create_new(&temporary)?;
            let write = file.write_all(&body).and_then(|()| file.sync_all());
            drop(file);
            if let Err(error) = write {
                let _ = filesystem.remove_file(&temporary);
                return Err(error.into());
            }
            if let Err(error) = filesystem.rename(&temporary, &publish_path) {
                let _ = filesystem.remove_file(&temporary);
                return Err(error.into());
            }
            filesystem.open(&publish_path).map_err(anyhow::Error::from)
        })?
        .await
        .context("join asset cache publication")??;
        self.touch(path);
        let cache = self.clone();
        if let Ok(task) = crate::asyncrt::try_spawn(async move {
            if let Err(error) = cache.cleanup().await {
                tracing::warn!(event = "asset_cache_cleanup_failed", %error, "could not enforce the local asset cache target");
            }
        }) {
            task.detach();
        }
        Ok(file)
    }

    async fn cleanup(&self) -> anyhow::Result<()> {
        let guard = self.0.cleanup.clone().lock_owned().await;
        let filesystem = self.0.filesystem.clone();
        let options = self.0.options.clone();
        crate::asyncrt::try_blocking(move || {
            let _guard = guard;
            cleanup_files(filesystem.as_ref(), &options)
        })?
        .await
        .context("join asset cache cleanup")?
    }
}

pub(super) fn file_stream(
    file: HostFile,
    offset: u64,
    length: u64,
) -> anyhow::Result<impl Stream<Item = anyhow::Result<Vec<u8>>> + Send> {
    let owner = crate::asyncrt::services()
        .domain_token()
        .context("asset stream requires an execution domain")?;
    Ok(futures_util::stream::try_unfold(
        (file, offset, length, owner),
        |(mut file, offset, remaining, owner)| async move {
            if remaining == 0 {
                return Ok(None);
            }
            let bytes = remaining.min(64 * 1024) as usize;
            let (file, body) = owner
                .blocking(move || {
                    let body = file.read_exact_at(offset, bytes);
                    (file, body)
                })?
                .await
                .context("join asset stream read")?;
            let body = body.context("read asset cache body")?;
            Ok(Some((
                body,
                (file, offset + bytes as u64, remaining - bytes as u64, owner),
            )))
        },
    ))
}

fn cleanup_files(filesystem: &dyn FileSystem, options: &AssetCacheOptions) -> anyhow::Result<()> {
    let mut files = Vec::new();
    let mut total = 0u64;
    for shard in filesystem.read_dir(&options.directory)? {
        if !shard.is_dir {
            continue;
        }
        for entry in filesystem.read_dir(&shard.path)? {
            let name = entry.file_name.to_string_lossy();
            if name.len() != 64
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                continue;
            }
            let metadata = filesystem.metadata(&entry.path)?;
            if !metadata.is_file {
                continue;
            }
            total = total.saturating_add(metadata.len);
            files.push((metadata.modified_unix_millis, metadata.len, entry.path));
        }
    }
    files.sort_by_key(|(modified, _, _)| *modified);
    for (_, bytes, path) in files {
        if total <= options.max_bytes {
            break;
        }
        if filesystem.remove_file(&path).is_ok() {
            total = total.saturating_sub(bytes);
        }
    }
    Ok(())
}
