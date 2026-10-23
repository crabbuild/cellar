//! Public-node crash cuts on actual image and engine filesystem operations.
#![allow(clippy::disallowed_methods)]

use super::{image_fault, log_fault};
use cellar_ltx::{
    DirectFileSystem, FileSystem, HostDirEntry, HostDirIterator, HostFile, HostFileLock,
    HostMetadata,
};
use std::{
    io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        OnceLock,
    },
};

#[derive(Default)]
pub struct Filesystem {
    armed: OnceLock<Armed>,
}

struct Armed {
    root: PathBuf,
    images: image_fault::FaultFileSystem,
    log: log_fault::FaultFileSystem,
    cut: String,
    image_created: AtomicBool,
    engine_armed: AtomicBool,
}

impl Filesystem {
    pub fn arm(&self, root: &Path, image_directory: PathBuf, cut: &str, signal: PathBuf) {
        let images = image_fault::Faults::new(image_directory, Some(signal.clone()));
        let mode = match cut {
            "image-before-sync" => image_fault::BEFORE_IMAGE_SYNC,
            "image-after-write" => image_fault::AFTER_IMAGE_WRITE,
            "image-after-file-sync" => image_fault::AFTER_IMAGE_SYNC,
            // Arm after image creation, so initial directory creation cannot trigger it.
            _ => 0,
        };
        images.mode.store(mode, Ordering::Release);
        let armed = Armed {
            root: root.into(),
            images: image_fault::FaultFileSystem { faults: images },
            log: log_fault::FaultFileSystem {
                gate: log_fault::Gate::new(Some(signal)),
            },
            cut: cut.into(),
            image_created: AtomicBool::new(false),
            engine_armed: AtomicBool::new(false),
        };
        assert!(self.armed.set(armed).is_ok(), "fixture armed twice");
    }

    fn select(&self, path: &Path) -> &dyn FileSystem {
        if let Some(armed) = self.armed.get() {
            if path.starts_with(armed.root.join("raft-cells")) {
                return &armed.images;
            }
            if path.starts_with(armed.root.join("raft")) {
                return &armed.log;
            }
        }
        &DirectFileSystem
    }
}

impl FileSystem for Filesystem {
    fn create_read_write(&self, path: &Path) -> io::Result<HostFile> {
        let file = self.select(path).create_read_write(path)?;
        if let Some(armed) = self.armed.get() {
            if path.parent() == Some(armed.images.faults.directory.as_path())
                && path.extension().is_some_and(|e| e == "sqlite")
            {
                if armed.cut == "image-after-directory-sync" {
                    armed
                        .images
                        .faults
                        .mode
                        .store(image_fault::AFTER_DIRECTORY_SYNC, Ordering::Release);
                }
                armed.image_created.store(true, Ordering::Release);
            }
        }
        Ok(file)
    }

    fn open_read_write(&self, path: &Path) -> io::Result<HostFile> {
        self.select(path).open_read_write(path)
    }

    fn lock_file(&self, path: &Path) -> io::Result<HostFileLock> {
        self.select(path).lock_file(path)
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        self.select(path).read(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<HostDirEntry>> {
        self.select(path).read_dir(path)
    }

    fn read_dir_iter(&self, path: &Path) -> io::Result<HostDirIterator> {
        self.select(path).read_dir_iter(path)
    }

    fn metadata(&self, path: &Path) -> io::Result<HostMetadata> {
        self.select(path).metadata(path)
    }

    fn create(&self, path: &Path) -> io::Result<HostFile> {
        self.select(path).create(path)
    }

    fn create_new(&self, path: &Path) -> io::Result<HostFile> {
        self.select(path).create_new(path)
    }

    fn open(&self, path: &Path) -> io::Result<HostFile> {
        self.select(path).open(path)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.select(path).remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        self.select(path).remove_dir_all(path)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.select(path).create_dir_all(path)
    }

    fn temporary_file(&self, directory: Option<&Path>) -> io::Result<HostFile> {
        DirectFileSystem.temporary_file(directory)
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        self.select(path).write(path, bytes)
    }

    fn remove_dir_batch(&self, path: &Path, limit: usize) -> io::Result<bool> {
        DirectFileSystem.remove_dir_batch(path, limit)
    }

    fn set_modified_unix_millis(&self, path: &Path, millis: i64) -> io::Result<()> {
        DirectFileSystem.set_modified_unix_millis(path, millis)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.select(from).rename(from, to)
    }

    fn sync_all(&self, path: &Path) -> io::Result<()> {
        self.select(path).sync_all(path)?;
        if let Some(armed) = self.armed.get() {
            if path == armed.images.faults.directory
                && armed.image_created.load(Ordering::Acquire)
                && !armed.engine_armed.swap(true, Ordering::AcqRel)
            {
                let mode = match armed.cut.as_str() {
                    "engine-before-sync" => log_fault::CRASH_BEFORE_SYNC,
                    "engine-after-sync" => log_fault::CRASH_AFTER_SYNC,
                    _ => 0,
                };
                // Engine startup has finished before image sealing. The next
                // log fsync is the atomic fresh-group mapping/checkpoint batch.
                armed.log.gate.mode.store(mode, Ordering::Release);
            }
        }
        Ok(())
    }
}

pub fn signal(path: &Path) -> io::Result<()> {
    let mut file = std::fs::File::create(path)?;
    std::io::Write::write_all(&mut file, b"reached")?;
    file.sync_all()
}

#[derive(Debug)]
pub struct Store {
    pub inner: object_store::memory::InMemory,
    directory: PathBuf,
    mutation: tokio::sync::Mutex<()>,
    pub cut: String,
    pub signal: PathBuf,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Object {
    path: String,
    bytes: Vec<u8>,
}

impl Store {
    pub async fn open(directory: PathBuf, cut: String, signal: PathBuf) -> anyhow::Result<Self> {
        std::fs::create_dir_all(&directory)?;
        let store = Self {
            inner: Default::default(),
            directory,
            mutation: Default::default(),
            cut,
            signal,
        };
        for (index, entry) in std::fs::read_dir(&store.directory)?.enumerate() {
            anyhow::ensure!(index < 4096, "persistent fixture object limit");
            let entry = entry?;
            if entry.path().extension().is_some_and(|e| e == "stage") {
                continue;
            }
            anyhow::ensure!(
                entry.metadata()?.len() <= 64 * 1024 * 1024,
                "persistent fixture object size"
            );
            let object: Object = serde_json::from_slice(&std::fs::read(entry.path())?)?;
            store
                .inner
                .put(
                    &object_store::path::Path::parse(object.path)?,
                    object.bytes.into(),
                )
                .await?;
        }
        Ok(store)
    }

    fn path(&self, path: &ObjectPath) -> PathBuf {
        use sha2::{Digest as _, Sha256};
        self.directory
            .join(hex::encode(Sha256::digest(path.as_ref().as_bytes())))
    }

    async fn persist(&self, path: &ObjectPath) -> object_store::Result<()> {
        let bytes = self.inner.get(path).await?.bytes().await?;
        let object = Object {
            path: path.to_string(),
            bytes: bytes.to_vec(),
        };
        let target = self.path(path);
        let stage = target.with_extension("stage");
        let result = (|| -> anyhow::Result<()> {
            let mut file = std::fs::File::create(&stage)?;
            std::io::Write::write_all(&mut file, &serde_json::to_vec(&object)?)?;
            file.sync_all()?;
            std::fs::rename(&stage, target)?;
            DirectFileSystem.sync_all(&self.directory)?;
            Ok(())
        })();
        result.map_err(|source| object_store::Error::Generic {
            store: "persistent-seed-fixture",
            source: source.into(),
        })
    }
}

impl std::fmt::Display for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "persistent seed fixture")
    }
}
use futures_util::stream::BoxStream;
use object_store::{
    path::Path as ObjectPath, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

#[async_trait::async_trait]
impl ObjectStore for Store {
    async fn put_opts(
        &self,
        path: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let _mutation = self.mutation.lock().await;
        let claim = path.as_ref().ends_with("/claims/0000000000000004");
        if claim && self.cut == "engine-after-reply" {
            signal(&self.signal).unwrap();
            std::future::pending::<()>().await;
        }
        let result = self.inner.put_opts(path, payload, options).await?;
        self.persist(path).await?;
        if claim && self.cut == "after-claim" {
            signal(&self.signal).unwrap();
            std::future::pending::<()>().await;
        }
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        path: &ObjectPath,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let _ = (path, options);
        Err(object_store::Error::NotSupported {
            source: Box::new(io::Error::other(
                "fixture requires conditional single-object PUT",
            )),
        })
    }

    async fn get_opts(
        &self,
        path: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(path, options).await
    }

    async fn delete(&self, path: &ObjectPath) -> object_store::Result<()> {
        let _mutation = self.mutation.lock().await;
        self.inner.delete(path).await?;
        std::fs::remove_file(self.path(path)).map_err(|source| object_store::Error::Generic {
            store: "persistent-seed-fixture",
            source: source.into(),
        })
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        let _mutation = self.mutation.lock().await;
        self.inner.copy(from, to).await?;
        self.persist(to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        let _mutation = self.mutation.lock().await;
        self.inner.copy_if_not_exists(from, to).await?;
        self.persist(to).await
    }
}
