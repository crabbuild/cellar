// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Filesystem conformance through production invocation, transaction and proof paths.
use anyhow::{anyhow, Result};
use cellar::engine_api::{Compat, HttpResponse, RuntimeFetch, RuntimeOptions, WorkerConfigOptions};
use cellar::fleet::LoadedDeployment;
use cellar::generation::{DeploymentGraph, Generation, GenerationOptions};
use cellar::runtime::RuntimeManager;
use cellar::sdk::filesystem::{
    Directory, Errno, File, Filesystem, OpenOptions, Outcome, Policy, Seek, StatVfs,
};
use cellar::sdk::{Cell, Context, HandlerFuture, Invocation, Output, Registry, Storage};
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicBool, AtomicI64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[path = "support/filesystem_admission.rs"]
mod admission;
#[path = "../../cellar-javascript/tests/support/native.rs"]
mod native;
#[path = "support/filesystem_readonly.rs"]
mod readonly;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn readonly_filesystem_recovers_and_preserves_business_transactions() -> Result<()> {
    readonly::exercise(readonly::application(true)?, |_| {}).await
}
#[path = "support/filesystem_contracts.rs"]
mod contracts;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_filesystem_handle_admission_is_domain_owned() -> Result<()> {
    admission::run().await
}

fn response(value: &str) -> Output {
    Output::Http(HttpResponse {
        status: 200,
        body: value.as_bytes().to_vec(),
        stream: None,
        headers: Vec::new(),
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}

#[derive(Default)]
struct Signals {
    retained_context: Mutex<Option<Context>>,
    retained_view: Mutex<Option<Filesystem>>,
    retained_storage: Mutex<Option<Storage>>,
    retained: Mutex<Option<File>>,
    retained_dir: Mutex<Option<Directory>>,
    clock: Arc<AtomicI64>,
    background_started: tokio::sync::Notify,
    background_release: tokio::sync::Notify,
    background_done: tokio::sync::Notify,
    background_baseline: Mutex<Option<StatVfs>>,
    transaction_started: tokio::sync::Notify,
    transaction_dropped: Arc<AtomicBool>,
    transaction_baseline: Mutex<Option<StatVfs>>,
}

struct DropFlag(Arc<AtomicBool>);

impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

struct Workspace {
    signals: Arc<Signals>,
}

impl Cell for Workspace {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Ok(Output::Done);
            };
            let path = url::Url::parse(&request.url)?.path().to_string();
            if path == "/denied" {
                let error = context.fs().err().unwrap();
                assert_eq!(error.code, "CapabilityMissing");
                assert_eq!(error.operation, "filesystem.acquire");
                return Ok(response("denied"));
            }
            let fs = context.fs()?;
            match path.as_str() {
                "/transactions" => {
                    contracts::transactions(&context).await?;
                    Ok(response("transactions"))
                }
                "/metadata" => {
                    contracts::metadata(&fs, &self.signals.clock)?;
                    contracts::directory_mutation(&fs)?;
                    contracts::directory_relative(&fs)?;
                    Ok(response("metadata"))
                }
                "/namespace" => {
                    contracts::namespace(&context)?;
                    Ok(response("namespace"))
                }
                "/reserved-namespace" => {
                    contracts::reserved_namespace(&fs)?;
                    Ok(response("reserved namespace"))
                }
                "/background" => {
                    while fs.collect_garbage()? > 0 {}
                    *self.signals.background_baseline.lock().unwrap() = Some(fs.statvfs()?);
                    fs.write_file("/background", b"before")?;
                    let file = fs.open("/background", OpenOptions::read_write())?;
                    fs.unlink("/background")?;
                    *self.signals.retained.lock().unwrap() = Some(file.dup()?);
                    *self.signals.retained_dir.lock().unwrap() = Some(fs.opendir("/")?);
                    *self.signals.retained_context.lock().unwrap() = Some(context.clone());
                    let before_foreign_calls = (fs.stat("/")?, fs.statvfs()?);
                    let signals = self.signals.clone();
                    context.wait_until(async move {
                        signals.background_started.notify_one();
                        signals.background_release.notified().await;
                        assert_eq!((fs.stat("/")?, fs.statvfs()?), before_foreign_calls);
                        assert_eq!(file.lseek(Seek::Current(0))?, 0);
                        file.pwrite(b"after!", 0)?;
                        fs.collect_garbage()?;
                        assert_eq!(file.fstat()?.nlink, 0);
                        let mut bytes = [0; 6];
                        file.read_exact(&mut bytes)?;
                        assert_eq!(&bytes, b"after!");
                        file.close()?;
                        fs.sync().await?;
                        signals.background_done.notify_one();
                        Ok(())
                    })?;
                    Ok(response("background accepted"))
                }
                "/foreign-handles" => {
                    let file = self.signals.retained.lock().unwrap();
                    let mut bytes = [0];
                    let error = file.as_ref().unwrap().read(&mut bytes).unwrap_err();
                    assert_eq!(
                        (error.errno, error.outcome),
                        (Errno::ESTALE, Outcome::NotApplied)
                    );
                    let directory = self.signals.retained_dir.lock().unwrap();
                    contracts::foreign_directory(&context, directory.as_ref().unwrap())?;
                    Ok(response("foreign handles refused"))
                }
                "/background-expired" => {
                    let error = self
                        .signals
                        .retained
                        .lock()
                        .unwrap()
                        .as_ref()
                        .unwrap()
                        .fstat()
                        .unwrap_err();
                    if error.errno == Errno::ESTALE {
                        return Ok(response("retiring"));
                    }
                    assert_eq!(error.errno, Errno::EBADF);
                    while fs.collect_garbage()? > 0 {}
                    let current = fs.statvfs()?;
                    let prior = self.signals.background_baseline.lock().unwrap();
                    let prior = prior.as_ref().unwrap();
                    assert_eq!(
                        (
                            current.logical_bytes,
                            current.allocated_bytes,
                            current.inodes
                        ),
                        (prior.logical_bytes, prior.allocated_bytes, prior.inodes)
                    );
                    self.signals
                        .retained
                        .lock()
                        .unwrap()
                        .take()
                        .unwrap()
                        .close()?;
                    self.signals
                        .retained_dir
                        .lock()
                        .unwrap()
                        .take()
                        .unwrap()
                        .closedir()?;
                    self.signals.retained_context.lock().unwrap().take();
                    Ok(response("background expired"))
                }
                "/retire-transaction" => {
                    *self.signals.transaction_baseline.lock().unwrap() = Some(fs.statvfs()?);
                    let nested_context = context.clone();
                    let signals = self.signals.clone();
                    context
                        .storage()?
                        .transaction(&context, move |outer| async move {
                            outer.fs()?.write_file("/retired-root", b"root")?;
                            outer.put_json("retired-root", &json!(true))?;
                            drop(outer.transaction(&nested_context, move |child| async move {
                                let _dropped = DropFlag(signals.transaction_dropped.clone());
                                let fs = child.fs()?;
                                fs.write_file("/retired-child", b"child")?;
                                child.put_json("retired-child", &json!(true))?;
                                let file = fs.open("/retired-child", OpenOptions::read_write())?;
                                *signals.retained.lock().unwrap() = Some(file.dup()?);
                                signals.transaction_started.notify_one();
                                std::future::pending::<()>().await;
                                file.close()?;
                                Ok(())
                            }));
                            std::future::pending::<()>().await;
                            Ok(())
                        })
                        .await?;
                    Err(anyhow!("retired transaction unexpectedly completed"))
                }
                "/retired-transaction-check" => {
                    for path in ["/retired-root", "/retired-child"] {
                        assert_eq!(fs.stat(path).unwrap_err().errno, Errno::ENOENT);
                    }
                    let storage = context.storage()?;
                    assert!(storage.get_json("retired-root")?.is_none());
                    assert!(storage.get_json("retired-child")?.is_none());
                    let current = fs.statvfs()?;
                    let prior = self.signals.transaction_baseline.lock().unwrap();
                    let prior = prior.as_ref().unwrap();
                    assert_eq!(
                        (
                            current.logical_bytes,
                            current.allocated_bytes,
                            current.inodes
                        ),
                        (prior.logical_bytes, prior.allocated_bytes, prior.inodes)
                    );
                    fs.write_file("/retired-child", b"new")?;
                    let old = self.signals.retained.lock().unwrap().take().unwrap();
                    assert_eq!(old.fstat().unwrap_err().errno, Errno::EBADF);
                    old.close()?;
                    assert_eq!(fs.read_file("/retired-child")?, b"new");
                    Ok(response("retired transaction rolled back"))
                }
                "/contracts" => {
                    for (error, operation) in [
                        (fs.stat("/missing").unwrap_err(), "filesystem.stat"),
                        (fs.lstat("/missing").unwrap_err(), "filesystem.lstat"),
                        (
                            fs.read_file("/missing").unwrap_err(),
                            "filesystem.read_file",
                        ),
                        (fs.opendir("/missing").err().unwrap(), "filesystem.opendir"),
                        (
                            fs.open(vec![b'x'; 4097], OpenOptions::read_only())
                                .err()
                                .unwrap(),
                            "filesystem.open",
                        ),
                    ] {
                        assert_eq!(error.operation, operation);
                        assert_eq!(error.outcome, Outcome::NotApplied);
                    }
                    fs.mkdir("/permissions", 0o700)?;
                    let file = fs.open(
                        "/permissions/file",
                        OpenOptions::read_write().create_new(true),
                    )?;
                    file.write(b"rights")?;
                    file.fchmod(0)?;
                    assert_eq!(
                        fs.open("/permissions/file", OpenOptions::read_only())
                            .err()
                            .unwrap()
                            .errno,
                        Errno::EACCES
                    );
                    // Open-description rights survive later mode changes.
                    file.pwrite(b"R", 0)?;
                    let mut bytes = [0; 1];
                    file.pread(&mut bytes, 0)?;
                    assert_eq!(&bytes, b"R");
                    file.fchmod(0o600)?;
                    let before = file.fstat()?;
                    file.write(&[])?;
                    file.pwrite(&[], 0)?;
                    file.read(&mut [])?;
                    file.pread(&mut [], 0)?;
                    let after = file.fstat()?;
                    assert_eq!(
                        (
                            before.version,
                            before.mtime_ns,
                            before.ctime_ns,
                            before.size
                        ),
                        (after.version, after.mtime_ns, after.ctime_ns, after.size)
                    );
                    assert_eq!(file.lseek(Seek::Current(0))?, 6);
                    file.lseek(Seek::Start(0))?;
                    for (seek, errno) in [
                        (Seek::Current(-1), Errno::EINVAL),
                        (Seek::Start(u64::MAX), Errno::EOVERFLOW),
                    ] {
                        let error = file.lseek(seek).unwrap_err();
                        assert_eq!((error.operation, error.errno), ("filesystem.lseek", errno));
                        assert_eq!(file.lseek(Seek::Current(0))?, 0);
                    }
                    let error = file.fchmod(0o4600).unwrap_err();
                    assert_eq!(
                        (error.operation, error.errno),
                        ("filesystem.fchmod", Errno::EOPNOTSUPP)
                    );
                    assert_eq!(file.fstat()?.mode, 0o600);
                    fs.chmod("/permissions", 0o600)?;
                    assert_eq!(
                        fs.stat("/permissions/file").unwrap_err().errno,
                        Errno::EACCES
                    );
                    fs.chmod("/permissions", 0o500)?;
                    let before = fs.statvfs()?;
                    assert_eq!(
                        fs.creat("/permissions/refused", 0o600).err().unwrap().errno,
                        Errno::EACCES
                    );
                    assert_eq!(fs.statvfs()?.inodes, before.inodes);
                    fs.chmod("/permissions", 0o700)?;
                    file.close()?;
                    let a = fs.opendir("/permissions")?;
                    let b = fs.opendir("/permissions")?;
                    let error = b.seekdir(a.telldir()?).unwrap_err();
                    assert_eq!(
                        (error.operation, error.errno),
                        ("filesystem.seekdir", Errno::EINVAL)
                    );
                    a.closedir()?;
                    b.closedir()?;
                    let mut handles = Vec::new();
                    let before = fs.stat("/permissions/file")?;
                    for _ in 0..fs.capabilities()?.limits.max_handles {
                        handles.push(fs.open("/permissions/file", OpenOptions::read_only())?);
                    }
                    let error = fs.creat("/permissions/file", 0o600).err().unwrap();
                    assert_eq!(
                        (error.operation, error.errno),
                        ("filesystem.creat", Errno::EMFILE)
                    );
                    assert_eq!(fs.stat("/permissions/file")?.version, before.version);
                    assert_eq!(
                        fs.creat("/permissions/not-created", 0o600)
                            .err()
                            .unwrap()
                            .errno,
                        Errno::EMFILE
                    );
                    assert_eq!(
                        fs.stat("/permissions/not-created").unwrap_err().errno,
                        Errno::ENOENT
                    );
                    assert_eq!(fs.opendir("/").err().unwrap().errno, Errno::EMFILE);
                    handles.pop().unwrap().close()?;
                    fs.creat("/permissions/admitted", 0o600)?.close()?;
                    drop(handles);
                    let storage = context.storage()?;
                    storage
                        .transaction(&context, |tx| async move {
                            let fs = tx.fs()?;
                            let file = fs.open("/permissions/file", OpenOptions::read_write())?;
                            let dir = fs.opendir("/permissions")?;
                            for (error, operation) in [
                                (fs.sync().await.unwrap_err(), "filesystem.sync"),
                                (file.fsync().await.unwrap_err(), "filesystem.fsync"),
                                (file.fdatasync().await.unwrap_err(), "filesystem.fdatasync"),
                                (dir.fsync().await.unwrap_err(), "filesystem.fsync"),
                            ] {
                                assert_eq!(
                                    (error.operation, error.code, error.outcome),
                                    (operation, "TransactionAffinity", Outcome::NotApplied)
                                );
                            }
                            file.write(b"TX")?;
                            Ok(())
                        })
                        .await?;
                    assert_eq!(fs.read_file("/permissions/file")?, b"TXghts");
                    Ok(response("contracts"))
                }
                "/exercise" => {
                    assert!(!fs.capabilities()?.symlinks);
                    fs.mkdir("/data", 0o777)?;
                    assert_eq!(fs.stat("/data")?.mode, 0o755);
                    let file = fs.open(
                        "/data/work",
                        OpenOptions::read_write().create_new(true).mode(0o600),
                    )?;
                    assert_eq!(file.write(b"abcdef")?, 6);
                    assert_eq!(file.lseek(Seek::Start(1))?, 1);
                    let duplicate = file.dup()?;
                    let mut bytes = [0; 2];
                    assert_eq!(duplicate.read(&mut bytes)?, 2);
                    assert_eq!(&bytes, b"bc");
                    assert_eq!(file.lseek(Seek::Current(0))?, 3);
                    assert_eq!(file.pread(&mut bytes, 0)?, 2);
                    assert_eq!(&bytes, b"ab");
                    assert_eq!(file.lseek(Seek::Current(0))?, 3);
                    let separate = fs.open("/data/work", OpenOptions::read_only())?;
                    assert_eq!(separate.lseek(Seek::Current(0))?, 0);
                    duplicate.close()?;
                    let append = fs.open("/data/work", OpenOptions::read_write().append(true))?;
                    append.pwrite(b"A", 0)?;
                    append.write(b"!")?;
                    assert_eq!(fs.read_file("/data/work")?, b"Abcdef!");
                    file.ftruncate(3)?;
                    assert_eq!(file.lseek(Seek::Current(0))?, 3);
                    file.ftruncate(10)?;
                    assert_eq!(fs.read_file("/data/work")?, b"Abc\0\0\0\0\0\0\0");
                    let binary = b"/data/\xff";
                    fs.write_file(binary, b"binary")?;
                    let directory = fs.opendir("/data")?;
                    assert_eq!(directory.readdir()?.unwrap().name, b".");
                    assert_eq!(directory.readdir()?.unwrap().name, b"..");
                    let cookie = directory.telldir()?;
                    assert!(directory.readdir()?.is_some());
                    directory.seekdir(cookie)?;
                    fs.rename("/data", "/moved")?;
                    let relative = fs.openat(&directory, "work", OpenOptions::read_only())?;
                    assert_eq!(relative.fstat()?.inode, file.fstat()?.inode);
                    fs.unlink("/moved/work")?;
                    fs.collect_garbage()?;
                    assert_eq!(file.fstat()?.nlink, 0);
                    assert_eq!(relative.pread(&mut bytes, 0)?, 2);
                    directory.closedir()?;
                    relative.close()?;
                    separate.close()?;
                    append.close()?;
                    file.close()?;
                    fs.mkdir("/parent", 0o700)?;
                    fs.mkdir("/parent/child", 0o700)?;
                    let removed = fs.opendir("/parent/child")?;
                    let parent = fs.stat("/parent")?.inode;
                    fs.rmdir("/parent/child")?;
                    fs.rmdir("/parent")?;
                    while fs.collect_garbage()? > 0 {}
                    assert_eq!(
                        fs.fstatat(&removed, "..", cellar::sdk::filesystem::StatFlags::Follow)?
                            .inode,
                        parent
                    );
                    removed.closedir()?;
                    while fs.collect_garbage()? > 0 {}
                    fs.write_file("/published", b"old")?;
                    let old = fs.open("/published", OpenOptions::read_only())?;
                    fs.replace_file("/published", b"new")?;
                    let mut original = [0; 3];
                    old.read_exact(&mut original)?;
                    assert_eq!(&original, b"old");
                    let before = fs.stat("/published")?;
                    assert_ne!(old.fstat()?.inode, before.inode);
                    let patch = cellar::sdk::filesystem::Edit {
                        offset: 1,
                        delete_bytes: 1,
                        insert: b"OO".to_vec(),
                    };
                    let version = cellar::sdk::filesystem::FileVersion::from(&before);
                    let after = fs.edit_file("/published", version, &patch)?;
                    assert_eq!(after.inode, before.inode);
                    assert!(after.version > before.version);
                    assert_eq!(fs.read_file("/published")?, b"nOOw");
                    assert_eq!(
                        fs.edit_file("/published", version, &patch)
                            .unwrap_err()
                            .code,
                        "Conflict"
                    );
                    assert_eq!(
                        fs.read_file_bounded("/published", 2).unwrap_err().errno,
                        Errno::EFBIG
                    );
                    assert_eq!(fs.read_file("/").unwrap_err().errno, Errno::EISDIR);
                    old.close()?;
                    let first = fs.read_dir("/", 1, None)?;
                    assert_eq!(first.entries.len(), 1);
                    let second = fs.read_dir("/", 1, first.next.as_ref())?;
                    assert_ne!(first.entries[0].name, second.entries[0].name);
                    let storage = context.storage()?;
                    let rolled: Result<()> = storage.tx(|tx| {
                        let transactional = tx.fs()?;
                        transactional.write_file("/rollback", b"must disappear")?;
                        tx.put_json("rollback", &json!(true))?;
                        assert_eq!(fs.stat("/").unwrap_err().code, "TransactionAffinity");
                        Err(anyhow!("rollback"))
                    });
                    assert!(rolled.is_err());
                    assert_eq!(fs.stat("/rollback").unwrap_err().errno, Errno::ENOENT);
                    assert!(storage.get_json("rollback")?.is_none());
                    let escaped = storage.tx(|tx| {
                        let txfs = tx.fs()?;
                        txfs.write_file("/receipt.txt", b"committed together")?;
                        tx.put_json("receipt", &json!({"saved":true}))?;
                        Ok(txfs.open("/receipt.txt", OpenOptions::read_only())?)
                    })?;
                    assert_eq!(escaped.fstat().unwrap_err().errno, Errno::EBADF);
                    escaped.close()?;
                    let budget_result: Result<()> = storage.tx(|tx| {
                        tx.put_json("over-budget", &json!(true))?;
                        let txfs = tx.fs()?;
                        let limit = txfs.capabilities()?.limits.max_transaction_operations;
                        for _ in 0..=limit {
                            txfs.stat("/")?;
                        }
                        Ok(())
                    });
                    assert_eq!(
                        budget_result
                            .unwrap_err()
                            .downcast_ref::<cellar::sdk::filesystem::Error>()
                            .unwrap()
                            .code,
                        "TransactionWorkLimit"
                    );
                    assert!(storage.get_json("over-budget")?.is_none());
                    let partial =
                        fs.open("/partial", OpenOptions::read_write().create_new(true))?;
                    let file_limit = fs.capabilities()?.limits.max_file_bytes;
                    partial.lseek(Seek::Start(file_limit - 8))?;
                    let error = partial.write_all(b"1234567890abcdef").unwrap_err();
                    assert_eq!(error.operation, "filesystem.write_all");
                    assert_eq!(
                        (error.errno, error.outcome, error.completed_bytes),
                        (Errno::EFBIG, Outcome::LocallyCommitted, 8)
                    );
                    assert_eq!(partial.lseek(Seek::Current(0))?, file_limit);
                    let mut prefix = [0; 8];
                    partial.pread(&mut prefix, file_limit - 8)?;
                    assert_eq!(&prefix, b"12345678");
                    partial.close()?;
                    fs.unlink("/partial")?;
                    let partial_tx: Result<()> = storage.tx(|tx| {
                        let file = tx
                            .fs()?
                            .open("/partial-tx", OpenOptions::write_only().create_new(true))?;
                        file.lseek(Seek::Start(file_limit - 8))?;
                        file.write_all(b"1234567890abcdef")?;
                        Ok(())
                    });
                    let error = partial_tx.unwrap_err();
                    let error = error
                        .downcast_ref::<cellar::sdk::filesystem::Error>()
                        .unwrap();
                    assert_eq!(
                        (error.outcome, error.completed_bytes),
                        (Outcome::Unknown, 8)
                    );
                    assert_eq!(fs.stat("/partial-tx").unwrap_err().errno, Errno::ENOENT);
                    let short = fs.open("/short", OpenOptions::read_write().create_new(true))?;
                    short.write_all(b"abc")?;
                    short.lseek(Seek::Start(0))?;
                    let mut output = [0; 4];
                    let error = short.read_exact(&mut output).unwrap_err();
                    assert_eq!(error.operation, "filesystem.read_exact");
                    assert_eq!(
                        (error.code, error.outcome, error.completed_bytes),
                        ("UnexpectedEof", Outcome::Unknown, 3)
                    );
                    assert_eq!(short.lseek(Seek::Current(0))?, 3);
                    assert_eq!(&output[..3], b"abc");
                    short.close()?;
                    let read_only = fs.open("/short", OpenOptions::read_only())?;
                    assert_eq!(read_only.write_all(&[]).unwrap_err().errno, Errno::EBADF);
                    read_only.close()?;
                    fs.unlink("/short")?;
                    let gc_budget: Result<()> = storage.tx(|tx| {
                        tx.put_json("gc-over-budget", &json!(true))?;
                        let txfs = tx.fs()?;
                        txfs.collect_garbage()?;
                        txfs.collect_garbage()?;
                        Ok(())
                    });
                    assert_eq!(
                        gc_budget
                            .unwrap_err()
                            .downcast_ref::<cellar::sdk::filesystem::Error>()
                            .unwrap()
                            .code,
                        "TransactionWorkLimit"
                    );
                    assert!(storage.get_json("gc-over-budget")?.is_none());
                    let large = fs.open("/large", OpenOptions::read_write().create_new(true))?;
                    let original_size = 100 * 1024 * 1024;
                    large.ftruncate(original_size)?;
                    let old = large.fstat()?;
                    fs.edit_file(
                        "/large",
                        (&old).into(),
                        &cellar::sdk::filesystem::Edit {
                            offset: 65534,
                            delete_bytes: 4,
                            insert: b"EDIT".to_vec(),
                        },
                    )?;
                    let mut patch = [0; 4];
                    large.pread(&mut patch, 65534)?;
                    assert_eq!(&patch, b"EDIT");
                    assert_eq!(large.fstat()?.size, original_size);
                    let current = large.fstat()?;
                    fs.edit_file(
                        "/large",
                        (&current).into(),
                        &cellar::sdk::filesystem::Edit {
                            offset: original_size,
                            delete_bytes: 0,
                            insert: b"tail".to_vec(),
                        },
                    )?;
                    large.pread(&mut patch, original_size)?;
                    assert_eq!(&patch, b"tail");
                    let current = large.fstat()?;
                    assert_eq!(current.size, original_size + 4);
                    fs.edit_file(
                        "/large",
                        (&current).into(),
                        &cellar::sdk::filesystem::Edit {
                            offset: original_size,
                            delete_bytes: 4,
                            insert: Vec::new(),
                        },
                    )?;
                    assert_eq!(large.fstat()?.size, original_size);
                    let current = large.fstat()?;
                    assert_eq!(
                        fs.edit_file(
                            "/large",
                            (&current).into(),
                            &cellar::sdk::filesystem::Edit {
                                offset: 0,
                                delete_bytes: 0,
                                insert: b"shift".to_vec()
                            }
                        )
                        .unwrap_err()
                        .errno,
                        Errno::EFBIG
                    );
                    large.close()?;
                    fs.unlink("/large")?;
                    while fs.collect_garbage()? > 0 {}
                    assert!(storage.delete_all().is_err());
                    fs.sync().await?;
                    Ok(response("exercised"))
                }
                "/capture" => {
                    *self.signals.retained_context.lock().unwrap() = Some(context.clone());
                    *self.signals.retained_view.lock().unwrap() = Some(fs.clone());
                    *self.signals.retained_storage.lock().unwrap() = Some(context.storage()?);
                    *self.signals.retained.lock().unwrap() =
                        Some(fs.open("/receipt.txt", OpenOptions::read_only())?);
                    Ok(response("captured"))
                }
                "/foreign-views" | "/retired-views" => {
                    let previous = self.signals.retained_view.lock().unwrap().clone().unwrap();
                    let storage = self
                        .signals
                        .retained_storage
                        .lock()
                        .unwrap()
                        .clone()
                        .unwrap();
                    contracts::preflight_authority(&context, &previous, &storage).await?;
                    if path == "/retired-views" {
                        self.signals.retained_view.lock().unwrap().take();
                        self.signals.retained_storage.lock().unwrap().take();
                    }
                    Ok(response("authority preflight refused"))
                }
                "/expired" => {
                    let retained = self.signals.retained.lock().unwrap().take().unwrap();
                    assert_eq!(retained.fstat().unwrap_err().errno, Errno::EBADF);
                    let previous = self
                        .signals
                        .retained_context
                        .lock()
                        .unwrap()
                        .take()
                        .unwrap();
                    let error = previous.fs().err().unwrap();
                    assert_eq!(
                        (error.operation, error.outcome),
                        ("filesystem.acquire", Outcome::NotApplied)
                    );
                    Ok(response("expired"))
                }
                "/recovered" => {
                    assert_eq!(fs.read_file("/receipt.txt")?, b"committed together");
                    assert_eq!(
                        context.storage()?.get_json("receipt")?,
                        Some(json!({"saved":true}))
                    );
                    fs.write_file("/after-recovery", b"new write")?;
                    fs.sync().await?;
                    Ok(response("recovered"))
                }
                "/isolated" => {
                    assert_eq!(fs.stat("/receipt.txt").unwrap_err().errno, Errno::ENOENT);
                    Ok(response("isolated"))
                }
                _ => Err(anyhow!("unknown filesystem test path")),
            }
        })
    }
}

fn deployment(script: &str, classes: Vec<String>, delete_alarm: bool) -> LoadedDeployment {
    LoadedDeployment {
        event_streams: Vec::new(),
        native: None,
        options: WorkerConfigOptions {
            stream_bindings: Vec::new(),
            src: String::new(),
            script_name: script.into(),
            do_classes: classes,
            bindings: Vec::new(),
            blob_bindings: Vec::new(),
            sql_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: Vec::new(),
            node: "test".into(),
            modules: Vec::new(),
            compat: Compat {
                delete_all_deletes_alarm: delete_alarm,
                ..Default::default()
            },
        },
        script_name: script.into(),
        version: "test".into(),
        prefix: "test/".into(),
        asset_binding: None,
        loader_bindings: Vec::new(),
        assets: None,
        services: Vec::new(),
        crons: Vec::new(),
        cron_revision: None,
        containers: Vec::new(),
        fence_image: None,
        container_images: Vec::new(),
    }
}

async fn fetch(runtime: &RuntimeManager, scope: &str, path: &str) -> Result<HttpResponse> {
    runtime
        .fetch_cell(
            scope.into(),
            None,
            RuntimeFetch {
                url: format!("http://cell{path}"),
                method: "GET".into(),
                body: Vec::new().into(),
                headers: Vec::new(),
                request_id: Some(cellar::engine_api::next_request_id()),
                order: None,
                parent: None,
            },
            None,
        )
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn context_filesystem_transactions_descriptors_and_recovery() -> Result<()> {
    let clock = Arc::new(AtomicI64::new(1_700_000_000_000));
    let sample = clock.clone();
    let domain = cellar::sdk::ExecutionDomain::with_wall_clock(
        tokio::runtime::Handle::current(),
        Arc::new(move || sample.load(Ordering::SeqCst)),
    );
    let result = domain
        .run(async move { cellar::asyncrt::timeout(Duration::from_secs(45), run(clock)).await? })
        .await;
    domain.shutdown().await;
    result??;
    Ok(())
}

async fn run(clock: Arc<AtomicI64>) -> Result<()> {
    let limits = cellar::sdk::CellLimits {
        filesystem: cellar::sdk::filesystem::FilesystemLimits {
            max_transfer_bytes: 8,
            ..Default::default()
        },
        ..Default::default()
    };
    cellar::asyncrt::services().configure_cell_limits(limits)?;
    let signals = Arc::new(Signals {
        clock,
        ..Default::default()
    });
    let mut registry = Registry::new(|context, _| {
        Box::pin(async move {
            let error = context.fs().err().unwrap();
            assert_eq!(
                (error.operation, error.outcome),
                ("filesystem.acquire", Outcome::NotApplied)
            );
            Ok(response("stateless"))
        })
    });
    for class in ["Workspace", "Denied"] {
        let signals = signals.clone();
        registry = registry.cell(class, move |_, _| {
            Ok(Arc::new(Workspace {
                signals: signals.clone(),
            }))
        })?;
    }
    registry = registry.filesystem("Workspace", Policy::default())?;
    let application = Arc::new(registry) as Arc<dyn cellar::sdk::Application>;
    let graph = DeploymentGraph::single(deployment(
        "native-filesystem",
        vec!["Workspace".into(), "Denied".into()],
        false,
    ));
    let generation = Generation::build_native(
        1,
        graph,
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        std::collections::HashMap::from([("native-filesystem".into(), application)]),
    )?;
    let directory = tempfile::tempdir()?;
    let runtime = RuntimeManager::start(
        generation,
        RuntimeOptions {
            data_dir: directory.path().into(),
            replication: None,
            wake: None,
            alarm_observer: Arc::new(|_, _| {}),
            node: "test".into(),
            region: "test".into(),
            bucket: None,
        },
    )?;
    for scope in ["Workspace:one", "Workspace:two", "Denied:one"] {
        runtime
            .restore_cell(
                scope,
                &cellar_logic::RestoreSpec {
                    epoch: 1,
                    fresh: true,
                    took_over: false,
                    resume_local: false,
                    prior: None,
                },
            )
            .await?;
        runtime.start_cell(scope.into(), 1, true).await?;
        runtime.publish_cell(scope, 1)?;
    }
    let (gates, mut requests) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_gate_tx(gates);
    let (shutdown, mut shutdown_rx) = tokio::sync::watch::channel(false);
    let tickets = Arc::new(Mutex::new(Vec::new()));
    let collected = tickets.clone();
    let adapter = cellar::asyncrt::spawn(async move {
        while let Some(request) = cellar::asyncrt::select_biased! { "filesystem test proof adapter shutdown";
            _=shutdown_rx.changed()=>None,
            request=requests.recv()=>request,
        } {
            collected.lock().unwrap().push(request.ticket);
            let _ = request.reply.send(Ok(()));
        }
    });
    assert_eq!(
        runtime
            .fetch_worker("http://worker/".into(), "GET".into(), vec![], vec![])
            .await?
            .body,
        b"stateless"
    );
    assert_eq!(
        fetch(&runtime, "Denied:one", "/denied").await?.body,
        b"denied"
    );
    assert_eq!(
        fetch(&runtime, "Workspace:one", "/exercise").await?.body,
        b"exercised"
    );
    assert_eq!(
        fetch(&runtime, "Workspace:one", "/contracts").await?.body,
        b"contracts"
    );
    assert_eq!(
        fetch(&runtime, "Workspace:one", "/transactions")
            .await?
            .body,
        b"transactions"
    );
    assert_eq!(
        fetch(&runtime, "Workspace:one", "/metadata").await?.body,
        b"metadata"
    );
    assert_eq!(
        fetch(&runtime, "Workspace:one", "/namespace").await?.body,
        b"namespace"
    );
    assert_eq!(
        fetch(&runtime, "Workspace:one", "/reserved-namespace")
            .await?
            .body,
        b"reserved namespace"
    );
    let host = runtime.clone();
    let background =
        cellar::asyncrt::spawn(async move { fetch(&host, "Workspace:one", "/background").await });
    signals.background_started.notified().await;
    // The response completes while accepted background work still owns its
    // descriptors. A different cell cannot borrow them during that interval.
    assert_eq!(
        cellar::asyncrt::timeout(Duration::from_secs(2), background)
            .await???
            .body,
        b"background accepted"
    );
    assert_eq!(
        fetch(&runtime, "Workspace:two", "/foreign-handles")
            .await?
            .body,
        b"foreign handles refused"
    );
    signals.background_release.notify_one();
    signals.background_done.notified().await;
    let mut expired = false;
    for _ in 0..64 {
        if fetch(&runtime, "Workspace:one", "/background-expired")
            .await?
            .body
            == b"background expired"
        {
            expired = true;
            break;
        }
    }
    assert!(
        expired,
        "completed background event did not retire its file capabilities"
    );
    let host = runtime.clone();
    let transaction = cellar::asyncrt::spawn(async move {
        fetch(&host, "Workspace:one", "/retire-transaction").await
    });
    signals.transaction_started.notified().await;
    runtime.swap_out_cell("Workspace:one", 1).await?;
    assert!(transaction.await?.is_err());
    assert!(signals.transaction_dropped.load(Ordering::SeqCst));
    runtime.start_cell("Workspace:one".into(), 1, false).await?;
    runtime.publish_cell("Workspace:one", 1)?;
    assert_eq!(
        fetch(&runtime, "Workspace:one", "/retired-transaction-check")
            .await?
            .body,
        b"retired transaction rolled back"
    );
    assert_eq!(
        fetch(&runtime, "Workspace:one", "/capture").await?.body,
        b"captured"
    );
    assert_eq!(
        fetch(&runtime, "Workspace:two", "/foreign-views")
            .await?
            .body,
        b"authority preflight refused"
    );
    assert_eq!(
        fetch(&runtime, "Workspace:one", "/expired").await?.body,
        b"expired"
    );
    assert_eq!(
        fetch(&runtime, "Workspace:two", "/isolated").await?.body,
        b"isolated"
    );
    runtime.swap_out_cell("Workspace:one", 1).await?;
    runtime.start_cell("Workspace:one".into(), 1, false).await?;
    runtime.publish_cell("Workspace:one", 1)?;
    assert_eq!(
        fetch(&runtime, "Workspace:one", "/retired-views")
            .await?
            .body,
        b"authority preflight refused"
    );
    assert_eq!(
        fetch(&runtime, "Workspace:one", "/recovered").await?.body,
        b"recovered"
    );
    assert!(tickets
        .lock()
        .unwrap()
        .iter()
        .any(|ticket| ticket.channel == cellar_logic::Channel::Sync
            && ticket.epoch == Some(1)
            && ticket.position.is_some()));
    for scope in ["Workspace:one", "Workspace:two", "Denied:one"] {
        runtime.swap_out_cell(scope, 1).await?;
    }
    shutdown.send(true)?;
    adapter.await?;
    Ok(())
}
