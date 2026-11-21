// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Observe the actual archive journal between release and the next remote intent.
use super::*;
use cellar_ltx::{FileSystem, HostDirEntry, HostDirIterator, HostFile, HostFileLock, HostMetadata};
use futures_util::FutureExt;
use std::{
    io,
    path::{Path, PathBuf},
    sync::{atomic::AtomicBool, Mutex},
};

#[derive(Clone)]
struct Saved {
    pending: bool,
    retiring: bool,
    synced: bool,
}

struct Files {
    journals: PathBuf,
    armed: AtomicBool,
    saved: Mutex<Vec<Saved>>,
    before_intent_sync: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}

impl Files {
    fn journal(&self, path: &Path) -> bool {
        self.armed.load(Ordering::Acquire)
            && path.parent().is_some_and(|parent| parent == self.journals)
    }
}

impl FileSystem for Files {
    fn create_read_write(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.create_read_write(path)
    }

    fn open_read_write(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.open_read_write(path)
    }

    fn clone_read_write(&self, from: &Path, to: &Path) -> io::Result<Option<HostFile>> {
        DirectFileSystem.clone_read_write(from, to)
    }

    fn temporary_file(&self, directory: Option<&Path>) -> io::Result<HostFile> {
        DirectFileSystem.temporary_file(directory)
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        DirectFileSystem.read(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<HostDirEntry>> {
        DirectFileSystem.read_dir(path)
    }

    fn read_dir_iter(&self, path: &Path) -> io::Result<HostDirIterator> {
        DirectFileSystem.read_dir_iter(path)
    }

    fn metadata(&self, path: &Path) -> io::Result<HostMetadata> {
        DirectFileSystem.metadata(path)
    }

    fn symlink_metadata(&self, path: &Path) -> io::Result<HostMetadata> {
        DirectFileSystem.symlink_metadata(path)
    }

    fn create(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.create(path)
    }

    fn create_new(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.create_new(path)
    }

    fn open(&self, path: &Path) -> io::Result<HostFile> {
        DirectFileSystem.open(path)
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        DirectFileSystem.write(path, bytes)
    }

    fn sync_all(&self, path: &Path) -> io::Result<()> {
        if self.journal(path) && self.saved.lock().unwrap().last().is_some_and(|r| r.pending) {
            let held = self.before_intent_sync.lock().unwrap().take();
            if let Some(held) = held {
                held();
            }
        }
        DirectFileSystem.sync_all(path)?;
        if self.journal(path) {
            if let Some(saved) = self.saved.lock().unwrap().last_mut() {
                saved.synced = true;
            }
        }
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        DirectFileSystem.rename(from, to)?;
        if from.file_name().is_some_and(|name| name == "next")
            && to.file_name().is_some_and(|name| name == "state")
            && to.parent().is_some_and(|parent| self.journal(parent))
        {
            let bytes = DirectFileSystem.read(to)?;
            assert!(bytes.len() <= 32 * 1024);
            let value: serde_json::Value = serde_json::from_slice(&bytes)?;
            let record = &value["record"];
            let mut saved = self.saved.lock().unwrap();
            assert!(saved.len() < 32, "bounded journal trace exhausted");
            saved.push(Saved {
                pending: record["pending"] != "None",
                retiring: !record["retire"].is_null(),
                synced: false,
            });
        }
        Ok(())
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.remove_file(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.remove_dir_all(path)
    }

    fn remove_dir_batch(&self, path: &Path, limit: usize) -> io::Result<bool> {
        DirectFileSystem.remove_dir_batch(path, limit)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        DirectFileSystem.create_dir_all(path)
    }

    fn lock_file(&self, path: &Path) -> io::Result<HostFileLock> {
        DirectFileSystem.lock_file(path)
    }
}

pub(crate) async fn retirement_coalesces_next_archive_intent(
    rooted: bool,
    store: Arc<dyn ObjectStore>,
) -> Result<()> {
    let mut root = tempfile::tempdir()?;
    root.disable_cleanup(true);
    eprintln!(
        "retained archive retirement disk: {}",
        root.path().display()
    );
    let files = Arc::new(Files {
        journals: root.path().join("node-1/log/cell-group-archive-journals"),
        armed: AtomicBool::new(false),
        saved: Mutex::new(Vec::new()),
        before_intent_sync: Mutex::new(None),
    });
    let h = Harness::configured_resources(
        rooted,
        store,
        1,
        Durability::Quorum,
        None,
        None,
        Some(Resources::JournalStorage {
            root,
            filesystem: files.clone(),
        }),
    )
    .await?;
    let previous = h.controllers[0]
        .archive_status(h.route.history)?
        .context("archive status")?
        .published
        .context("initial publication")?
        .position()
        .index;
    let gate = h.remote.hold_retirement_after(previous.get());

    struct Release(Arc<archive_controller::archive_store::Gate>);

    impl Drop for Release {
        fn drop(&mut self) {
            self.0.release.notify_one();
        }
    }
    let release = Release(gate.clone());
    let checked = std::panic::AssertUnwindSafe(async {
        let first = async {
            h.command(1, b"first", Durability::S3, Duration::from_secs(5))
                .await?
                .response()
                .await
                .map_err(anyhow::Error::from)
        };
        let (entered, response) = futures_util::future::join(
            tokio::time::timeout(Duration::from_secs(5), gate.entered.notified()),
            first,
        )
        .await;
        entered.context("entered predecessor release")?;
        let response = response?;
        assert_eq!(response.durability(), Durability::S3);
        assert_eq!(response.result(), b"first");
        drop(response);
        let newer = h
            .command(2, b"next", Durability::Quorum, Duration::from_secs(5))
            .await?
            .response()
            .await?;
        assert_eq!(newer.durability(), Durability::Quorum);
        assert_eq!(newer.result(), b"next");
        let version = newer.observation().version;
        drop(newer);
        let pending = h
            .command(2, b"next", Durability::S3, Duration::from_secs(5))
            .await?;
        h.waiting().await.context("wait for S3 output owners")?;
        assert_eq!(h.calls.load(Ordering::SeqCst), 2);
        let (entered, started) = tokio::sync::oneshot::channel();
        let (unblock, blocked) = std::sync::mpsc::sync_channel(1);

        struct IntentRelease(std::sync::mpsc::SyncSender<()>);

        impl Drop for IntentRelease {
            fn drop(&mut self) {
                let _ = self.0.try_send(());
            }
        }
        let intent_release = IntentRelease(unblock);
        *files.before_intent_sync.lock().unwrap() = Some(Box::new(move || {
            let _ = entered.send(());
            blocked
                .recv_timeout(Duration::from_secs(5))
                .expect("release original intent barrier");
        }));
        files.armed.store(true, Ordering::Release);
        drop(release);
        tokio::time::timeout(Duration::from_secs(5), started)
            .await
            .context("next journal intent directory barrier")??;
        // The old release completed, but no successor remote mutation can
        // precede the next intent's actual directory barrier.
        let objects = h.remote.inner.list(None).try_collect::<Vec<_>>().await?;
        let registry = objects
            .iter()
            .find(|object| object.location.as_ref().ends_with("/ownership/registry"))
            .context("registry")?;
        let bytes = h
            .remote
            .inner
            .get(&registry.location)
            .await?
            .bytes()
            .await?;
        let registry: serde_json::Value = serde_json::from_slice(&bytes)?;
        assert_eq!(
            registry["holds"].as_array().context("retained head")?.len(),
            1
        );
        assert!(registry["pending"]
            .as_array()
            .context("new allocations")?
            .is_empty());
        assert_eq!(h.calls.load(Ordering::SeqCst), 2);
        drop(intent_release);
        let result = pending.response().await?;
        assert_eq!(result.durability(), Durability::S3);
        assert_eq!(result.result(), b"next");
        assert_eq!(result.observation().version, version);
        assert_eq!(h.calls.load(Ordering::SeqCst), 2);
        drop(result);
        let saved = files.saved.lock().unwrap().clone();
        files.armed.store(false, Ordering::Release);
        Ok::<_, anyhow::Error>(saved)
    })
    .catch_unwind()
    .await;
    // Join the original owners even when a correctness or avoided-work oracle fails.
    h.close().await?;
    let saved = match checked {
        Ok(result) => result?,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    let intent = saved
        .iter()
        .position(|record| record.pending)
        .context("next durable intent")?;
    assert!(
        saved[intent].synced,
        "remote work escaped an unsynced intent"
    );
    assert!(
        !saved[intent].retiring,
        "released predecessor debt remained in the new intent"
    );
    assert_eq!(
        intent, 0,
        "retirement wrote a standalone cleared record before its next intent"
    );
    Ok(())
}
