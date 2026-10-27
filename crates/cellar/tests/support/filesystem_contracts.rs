// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Filesystem semantic cases executed inside production native cell turns.

use anyhow::{anyhow, Result};
use cellar::sdk::filesystem::{
    Directory, Edit, Errno, Error, FileVersion, Filesystem, OpenOptions, Outcome, Seek, StatFlags,
    Time, Times, UnlinkFlags,
};
use cellar::sdk::Context;
use serde_json::json;
use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};

fn affinity(error: Error) {
    assert_eq!(
        (error.code, error.errno, error.outcome),
        ("TransactionAffinity", Errno::EBADF, Outcome::NotApplied)
    );
}

pub(crate) async fn preflight_authority(
    context: &Context,
    previous: &Filesystem,
    storage: &cellar::sdk::Storage,
) -> Result<()> {
    fn refused(error: Error, operation: &str) {
        assert_eq!(
            (error.operation, error.outcome, error.completed_bytes),
            (operation, Outcome::NotApplied, 0),
            "authority refusal was classified as an uncertain storage effect: {error:?}"
        );
        assert!(
            std::error::Error::source(&error).is_some(),
            "authority cause was lost"
        );
    }
    let current = context.fs()?;
    let before = (current.statvfs()?, current.stat("/")?);
    refused(previous.stat("/").unwrap_err(), "filesystem.stat");
    refused(
        previous.write_file("/scope-refused", b"bad").unwrap_err(),
        "filesystem.write_file",
    );
    refused(
        previous.mkdir("/scope-refused", 0o700).unwrap_err(),
        "filesystem.mkdir",
    );
    refused(storage.fs().err().unwrap(), "filesystem.acquire");
    refused(previous.sync().await.unwrap_err(), "filesystem.sync");
    assert_eq!(
        current.stat("/scope-refused").unwrap_err().errno,
        Errno::ENOENT
    );
    assert_eq!((current.statvfs()?, current.stat("/")?), before);
    Ok(())
}

pub(crate) async fn transactions(context: &Context) -> Result<()> {
    let storage = context.storage()?;
    let fs = context.fs()?;
    fs.write_file("/tx-base", b"original")?;
    let ordinary = fs.open("/tx-base", OpenOptions::read_write())?;
    ordinary.lseek(Seek::Start(2))?;
    storage.tx(|outer| {
        let parent = outer.fs()?;
        let file = parent.open("/tx-base", OpenOptions::read_write())?;
        file.lseek(Seek::Start(2))?;
        affinity(ordinary.write(b"bad").unwrap_err());
        let mut escaped = None;
        let child: Result<()> = outer.tx(|inner| {
            let child = inner.fs()?;
            affinity(parent.stat("/tx-base").unwrap_err());
            affinity(file.pwrite(b"bad", 0).unwrap_err());
            let speculative = child.open("/tx-base", OpenOptions::read_write())?;
            speculative.write_all(b"changed!")?;
            escaped = Some(speculative.dup()?);
            child.write_file("/tx-rolled-child", b"rollback")?;
            inner.put_json("tx-child-rolled", &json!(true))?;
            Err(anyhow!("roll back child only"))
        });
        assert_eq!(child.unwrap_err().to_string(), "roll back child only");
        let escaped = escaped.take().unwrap();
        assert_eq!(escaped.fstat().unwrap_err().errno, Errno::EBADF);
        escaped.close()?;
        assert_eq!(parent.read_file("/tx-base")?, b"original");
        assert_eq!(file.lseek(Seek::Current(0))?, 2);
        assert_eq!(
            parent.stat("/tx-rolled-child").unwrap_err().errno,
            Errno::ENOENT
        );
        assert!(outer.get_json("tx-child-rolled")?.is_none());
        let committed = outer.tx(|inner| {
            inner.fs()?.write_file("/tx-child-commit", b"child")?;
            inner.put_json("tx-child-commit", &json!(true))?;
            Ok(inner
                .fs()?
                .open("/tx-child-commit", OpenOptions::read_only())?)
        })?;
        // Releasing the child's savepoint ends its capabilities even though
        // its data remains visible through the still-active parent view.
        assert_eq!(committed.fstat().unwrap_err().errno, Errno::EBADF);
        committed.close()?;
        assert_eq!(parent.read_file("/tx-child-commit")?, b"child");
        file.write(b"OK")?;
        outer.put_json("tx-parent-commit", &json!(true))?;
        Ok(())
    })?;
    assert_eq!(ordinary.lseek(Seek::Current(0))?, 2);
    assert_eq!(fs.read_file("/tx-base")?, b"orOKinal");
    ordinary.close()?;
    assert_eq!(storage.get_json("tx-parent-commit")?, Some(json!(true)));

    let rolled: Result<()> = storage.tx(|outer| {
        outer.tx(|child| {
            child.fs()?.write_file("/tx-outer-rollback", b"child")?;
            child.put_json("tx-outer-rollback", &json!(true))
        })?;
        Err(anyhow!("roll back released child with its parent"))
    });
    assert!(rolled.is_err());
    assert_eq!(
        fs.stat("/tx-outer-rollback").unwrap_err().errno,
        Errno::ENOENT
    );
    assert!(storage.get_json("tx-outer-rollback")?.is_none());
    storage.tx(|outer| {
        let terminal = outer.tx(|child| {
            let fs = child.fs()?;
            fs.write_file("/tx-explicit-rollback", b"discard")?;
            let file = fs.open("/tx-explicit-rollback", OpenOptions::read_write())?;
            child.rollback()?;
            assert_eq!(file.fstat().unwrap_err().errno, Errno::EBADF);
            assert!(fs.stat("/tx-explicit-rollback").is_err());
            Ok(file)
        })?;
        terminal.close()?;
        assert_eq!(
            outer.fs()?.stat("/tx-explicit-rollback").unwrap_err().errno,
            Errno::ENOENT
        );
        Ok(())
    })?;

    let asynchronous_context = context.clone();
    let escaped = storage
        .transaction(context, move |outer| async move {
            let parent = Arc::new(outer.fs()?.open("/tx-base", OpenOptions::read_only())?);
            let retained = parent.clone();
            let escaped = outer
                .transaction(&asynchronous_context, move |child| async move {
                    tokio::task::yield_now().await;
                    affinity(retained.lseek(Seek::Start(7)).unwrap_err());
                    let file = child.fs()?.open("/tx-base", OpenOptions::read_write())?;
                    file.pwrite(b"ASYNC", 0)?;
                    tokio::task::yield_now().await;
                    assert_eq!(file.lseek(Seek::Current(0))?, 0);
                    Ok(file)
                })
                .await?;
            assert_eq!(escaped.fstat().unwrap_err().errno, Errno::EBADF);
            escaped.close()?;
            assert_eq!(parent.lseek(Seek::Current(0))?, 0);
            assert_eq!(outer.fs()?.read_file("/tx-base")?, b"ASYNCnal");
            Ok(parent)
        })
        .await?;
    assert_eq!(escaped.fstat().unwrap_err().errno, Errno::EBADF);
    drop(escaped);

    // Executed work is not refunded by rolling back successive savepoints.
    // Otherwise an application could evade a root transaction's work bound.
    let exhausted: Result<()> = storage.tx(|outer| {
        let parent = outer.fs()?;
        let maximum = parent.capabilities()?.limits.max_transaction_operations;
        outer.put_json("tx-work-rollback", &json!(true))?;
        for count in [maximum / 2, maximum - maximum / 2] {
            let child: Result<()> = outer.tx(|child| {
                let child = child.fs()?;
                for _ in 0..count {
                    child.stat("/tx-base")?;
                }
                Err(anyhow!("work performed before child rollback"))
            });
            assert_eq!(
                child.unwrap_err().to_string(),
                "work performed before child rollback"
            );
        }
        let error = parent.stat("/tx-base").unwrap_err();
        assert_eq!(
            (error.code, error.outcome),
            ("TransactionWorkLimit", Outcome::NotApplied)
        );
        Err(error.into())
    });
    assert!(exhausted.is_err());
    assert!(storage.get_json("tx-work-rollback")?.is_none());
    let bytes_exhausted: Result<()> = storage.tx(|outer| {
        outer.put_json("tx-byte-work-rollback", &json!(true))?;
        let child: Result<()> = outer.tx(|child| {
            child.fs()?.collect_garbage()?;
            Err(anyhow!("roll back admitted maintenance"))
        });
        assert_eq!(
            child.unwrap_err().to_string(),
            "roll back admitted maintenance"
        );
        // The first maintenance call reserved its bounded chunk work against
        // the root, even though its child savepoint subsequently rolled back.
        outer.tx(|child| {
            child.fs()?.collect_garbage()?;
            Ok(())
        })
    });
    let error = bytes_exhausted.unwrap_err();
    let error = error.downcast_ref::<Error>().unwrap();
    assert_eq!(
        (error.code, error.outcome),
        ("TransactionWorkLimit", Outcome::NotApplied)
    );
    assert!(storage.get_json("tx-byte-work-rollback")?.is_none());
    storage.tx(|fresh| {
        assert_eq!(fresh.fs()?.read_file("/tx-base")?, b"ASYNCnal");
        Ok(())
    })?;
    Ok(())
}

pub(crate) fn metadata(fs: &Filesystem, clock: &AtomicI64) -> Result<()> {
    fs.write_file("/metadata", b"original")?;
    let initial = fs.stat("/metadata")?;
    let started = clock.load(Ordering::SeqCst);
    assert_eq!(
        (initial.uid, initial.gid, initial.atime_ns),
        (1000, 1000, 0)
    );
    assert_eq!(initial.mtime_ns, started * 1_000_000);
    assert_eq!(initial.ctime_ns, initial.mtime_ns);
    let file = fs.open("/metadata", OpenOptions::read_only())?;
    clock.store(started + 1, Ordering::SeqCst);
    file.fchmod(0o400)?;
    let chmod = fs.stat("/metadata")?;
    assert_eq!(chmod.mode, 0o400);
    assert_eq!(chmod.ctime_ns, (started + 1) * 1_000_000);
    assert_eq!(
        (chmod.mtime_ns, chmod.version),
        (initial.mtime_ns, initial.version)
    );
    for time in [-123_456_789, i64::MIN, i64::MAX] {
        fs.utimens(
            "/metadata",
            Times {
                atime: Time::Omit,
                mtime: Time::Value(time),
            },
        )?;
        let changed = file.fstat()?;
        assert_eq!((changed.mtime_ns, changed.atime_ns), (time, 0));
        assert_eq!(changed.version, initial.version);
    }
    let before = file.fstat()?;
    fs.utimens(
        "/metadata",
        Times {
            atime: Time::Omit,
            mtime: Time::Omit,
        },
    )?;
    assert_eq!(file.fstat()?, before);
    let error = fs
        .utimens(
            "/metadata",
            Times {
                atime: Time::Now,
                mtime: Time::Value(0),
            },
        )
        .unwrap_err();
    assert_eq!(
        (error.operation, error.errno, error.outcome),
        ("filesystem.utimens", Errno::EOPNOTSUPP, Outcome::NotApplied)
    );
    assert_eq!(file.fstat()?, before);
    clock.store(started + 2, Ordering::SeqCst);
    fs.utimens(
        "/metadata",
        Times {
            atime: Time::Omit,
            mtime: Time::Now,
        },
    )?;
    let now = file.fstat()?;
    assert_eq!(
        (now.mtime_ns, now.ctime_ns),
        ((started + 2) * 1_000_000, (started + 2) * 1_000_000)
    );
    assert_eq!(now.version, initial.version);
    assert_eq!(
        fs.utimens(
            "/missing",
            Times {
                atime: Time::Omit,
                mtime: Time::Omit
            }
        )
        .unwrap_err()
        .errno,
        Errno::ENOENT
    );
    file.close()?;
    Ok(())
}

pub(crate) fn directory_mutation(fs: &Filesystem) -> Result<()> {
    fs.mkdir("/enumerate", 0o700)?;
    for name in ["a", "b", "c"] {
        fs.write_file(format!("/enumerate/{name}"), name.as_bytes())?;
    }
    let first = fs.read_dir("/enumerate", 1, None)?;
    assert_eq!(first.entries[0].name, b"a");
    let directory = fs.opendir("/enumerate")?;
    assert_eq!(directory.readdir()?.unwrap().name, b".");
    assert_eq!(directory.readdir()?.unwrap().name, b"..");
    assert_eq!(directory.readdir()?.unwrap().name, b"a");
    let cookie = directory.telldir()?;
    let inode = directory.fstat()?.inode;
    fs.unlink("/enumerate/b")?;
    fs.write_file("/enumerate/d", b"d")?;
    fs.rename("/enumerate/c", "/enumerate/renamed")?;
    fs.rename("/enumerate", "/enumerated")?;
    assert_eq!(directory.fstat()?.inode, inode);
    let remaining = directory.read_page(10)?;
    assert_eq!(
        remaining
            .iter()
            .map(|entry| entry.name.as_slice())
            .collect::<Vec<_>>(),
        vec![b"d".as_slice(), b"renamed".as_slice()]
    );
    directory.seekdir(cookie)?;
    assert_eq!(directory.read_page(10)?, remaining);
    let page = fs.read_dir("/enumerated", 10, first.next.as_ref())?;
    assert_eq!(page.entries, remaining);
    assert!(page.next.is_none());
    fs.mkdir("/replacement", 0o700)?;
    assert_eq!(
        fs.read_dir("/replacement", 10, first.next.as_ref())
            .unwrap_err()
            .errno,
        Errno::EINVAL
    );
    directory.closedir()?;
    Ok(())
}

pub(crate) fn foreign_directory(context: &Context, foreign: &Directory) -> Result<()> {
    let fs = context.fs()?;
    fs.mkdir("/at-authority", 0o700)?;
    fs.mkdir("/at-authority/empty", 0o700)?;
    fs.write_file("/at-authority/source", b"source")?;
    fs.write_file("/at-authority/target", b"target")?;
    let local = fs.opendir("/at-authority")?;
    let snapshot = || -> Result<_> {
        Ok((
            [
                "/",
                "/at-authority",
                "/at-authority/empty",
                "/at-authority/source",
                "/at-authority/target",
            ]
            .into_iter()
            .map(|path| fs.stat(path))
            .collect::<cellar::sdk::filesystem::Result<Vec<_>>>()?,
            fs.statvfs()?,
            local.telldir()?,
        ))
    };
    let before = snapshot()?;
    for prefix in ["at-authority/", "/at-authority/"] {
        let source = format!("{prefix}source");
        let target = format!("{prefix}target");
        let empty = format!("{prefix}empty");
        let created = format!("{prefix}created");
        let refuse = |result: cellar::sdk::filesystem::Result<()>, operation: &'static str| {
            namespace_refusal(result, operation, Errno::ESTALE);
            assert_eq!(snapshot().unwrap(), before);
        };
        refuse(
            fs.openat(
                foreign,
                &created,
                OpenOptions::write_only().create_new(true),
            )
            .map(|_| ()),
            "filesystem.openat",
        );
        refuse(
            fs.openat(foreign, &source, OpenOptions::write_only().truncate(true))
                .map(|_| ()),
            "filesystem.openat",
        );
        for flags in [StatFlags::Follow, StatFlags::NoFollow] {
            refuse(
                fs.fstatat(foreign, &source, flags).map(|_| ()),
                "filesystem.fstatat",
            );
        }
        refuse(fs.mkdirat(foreign, &created, 0o700), "filesystem.mkdirat");
        refuse(
            fs.unlinkat(foreign, &source, UnlinkFlags::File),
            "filesystem.unlinkat",
        );
        refuse(
            fs.unlinkat(foreign, &empty, UnlinkFlags::RemoveDirectory),
            "filesystem.unlinkat",
        );
        refuse(
            fs.renameat(foreign, &source, &local, "target"),
            "filesystem.renameat",
        );
        refuse(
            fs.renameat(&local, "source", foreign, &target),
            "filesystem.renameat",
        );
    }
    let storage = context.storage()?;
    for foreign_source in [true, false] {
        let result: Result<()> = storage.tx(|tx| {
            tx.put_json("foreign-at-must-rollback", &json!(true))?;
            let fs = tx.fs()?;
            fs.write_file("/at-authority/source", b"changed")?;
            let local = fs.opendir("/at-authority")?;
            if foreign_source {
                fs.renameat(foreign, "/at-authority/source", &local, "target")?;
            } else {
                fs.renameat(&local, "source", foreign, "/at-authority/target")?;
            }
            Ok(())
        });
        namespace_refusal(
            result.map_err(|error| error.downcast::<Error>().unwrap()),
            "filesystem.renameat",
            Errno::ESTALE,
        );
        assert!(storage.get_json("foreign-at-must-rollback")?.is_none());
        assert_eq!(snapshot()?, before);
    }
    assert_eq!(fs.read_file("/at-authority/source")?, b"source");
    assert_eq!(fs.read_file("/at-authority/target")?, b"target");
    namespace_refusal(
        fs.stat("/at-authority/created"),
        "filesystem.stat",
        Errno::ENOENT,
    );
    local.closedir()?;
    fs.unlink("/at-authority/source")?;
    fs.unlink("/at-authority/target")?;
    fs.rmdir("/at-authority/empty")?;
    fs.rmdir("/at-authority")?;
    while fs.collect_garbage()? > 0 {}
    Ok(())
}

pub(crate) fn directory_relative(fs: &Filesystem) -> Result<()> {
    let file = fs.creat("/at-created", 0o600)?;
    file.write_all(b"created")?;
    assert_eq!(file.fstat()?.mode, 0o600);
    file.close()?;
    fs.truncate("/at-created", 3)?;
    assert_eq!(fs.read_file("/at-created")?, b"cre");
    assert_eq!(fs.lstat("/at-created")?, fs.stat("/at-created")?);
    fs.unlink("/at-created")?;

    fs.mkdir("/at-source", 0o700)?;
    fs.mkdir("/at-destination", 0o700)?;
    let source = fs.opendir("/at-source")?;
    let destination = fs.opendir("/at-destination")?;
    fs.mkdirat(&source, "sub", 0o777)?;
    assert_eq!(fs.fstatat(&source, "sub", StatFlags::NoFollow)?.mode, 0o755);
    let held = fs.openat(&source, b"\xff", OpenOptions::read_write().create_new(true))?;
    held.write_all(b"relative")?;
    let identity = held.fstat()?;
    fs.rename("/at-source", "/at-moved")?;
    assert_eq!(fs.fstatat(&source, b"\xff", StatFlags::NoFollow)?, identity);
    let absolute = fs.openat(&destination, b"/at-moved/\xff", OpenOptions::read_only())?;
    assert_eq!(absolute.fstat()?.inode, identity.inode);
    absolute.close()?;
    fs.renameat(&source, b"\xff", &destination, b"\xfe")?;
    namespace_refusal(
        fs.fstatat(&source, b"\xff", StatFlags::Follow),
        "filesystem.fstatat",
        Errno::ENOENT,
    );
    assert_eq!(
        fs.fstatat(&destination, b"\xfe", StatFlags::Follow)?.inode,
        identity.inode
    );
    fs.unlinkat(&destination, b"\xfe", UnlinkFlags::File)?;
    assert_eq!(held.fstat()?.nlink, 0);
    let mut bytes = [0; 8];
    assert_eq!(held.pread(&mut bytes, 0)?, 8);
    assert_eq!(&bytes, b"relative");
    fs.unlinkat(&source, "sub", UnlinkFlags::RemoveDirectory)?;
    source.rewinddir()?;
    assert_eq!(
        source
            .read_page(3)?
            .into_iter()
            .map(|entry| entry.name)
            .collect::<Vec<_>>(),
        [b".".to_vec(), b"..".to_vec()]
    );
    held.close()?;
    source.closedir()?;
    destination.closedir()?;
    fs.rmdir("/at-moved")?;
    fs.rmdir("/at-destination")?;
    let usage = fs.statvfs()?;
    let limits = fs.capabilities()?.limits;
    assert_eq!(
        usage.logical_bytes + usage.available_logical_bytes,
        limits.max_logical_bytes
    );
    assert_eq!(
        usage.allocated_bytes + usage.available_allocated_bytes,
        limits.max_allocated_bytes
    );
    assert_eq!(usage.inodes + usage.available_inodes, limits.max_inodes);
    Ok(())
}

fn namespace_refusal<T>(
    result: cellar::sdk::filesystem::Result<T>,
    operation: &'static str,
    errno: Errno,
) {
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("{operation} unexpectedly succeeded"),
    };
    assert_eq!(
        (
            error.code,
            error.operation,
            error.errno,
            error.outcome,
            error.completed_bytes
        ),
        ("Filesystem", operation, errno, Outcome::NotApplied, 0)
    );
}

pub(crate) fn namespace(context: &Context) -> Result<()> {
    let fs = context.fs()?;
    // Earlier scenarios must not leave asynchronous reclamation changing the
    // accounting baseline while this scenario checks failed namespace calls.
    while fs.collect_garbage()? > 0 {}
    fs.mkdir("/namespace", 0o700)?;
    for name in ["source", "empty", "nonempty"] {
        fs.mkdir(format!("/namespace/{name}"), 0o700)?;
    }
    fs.mkdir("/namespace/nonempty/child", 0o700)?;
    fs.write_file("/namespace/file", b"source")?;
    fs.write_file("/namespace/other", b"victim")?;
    let paths = [
        "/",
        "/namespace",
        "/namespace/file",
        "/namespace/other",
        "/namespace/source",
        "/namespace/empty",
        "/namespace/nonempty",
        "/namespace/nonempty/child",
    ];
    let snapshot = || -> Result<_> {
        Ok((
            paths
                .iter()
                .map(|path| fs.stat(path))
                .collect::<cellar::sdk::filesystem::Result<Vec<_>>>()?,
            fs.statvfs()?,
        ))
    };
    let before = snapshot()?;
    assert_eq!(
        fs.stat("/../../namespace//./file")?,
        fs.stat("namespace/file")?
    );
    for (path, errno) in [
        (String::new(), Errno::ENOENT),
        ("/namespace/fi\0le".into(), Errno::EINVAL),
        (
            format!("/namespace/{}", "x".repeat(256)),
            Errno::ENAMETOOLONG,
        ),
        ("/x".repeat(65), Errno::ENAMETOOLONG),
        ("/x".repeat(2050), Errno::ENAMETOOLONG),
        ("/namespace/file/child".into(), Errno::ENOTDIR),
        ("/namespace/file/".into(), Errno::ENOTDIR),
        ("/namespace/missing".into(), Errno::ENOENT),
    ] {
        namespace_refusal(fs.stat(path), "filesystem.stat", errno);
        assert_eq!(snapshot()?, before);
    }
    namespace_refusal(
        fs.open(
            "/namespace/file",
            OpenOptions::write_only().create_new(true),
        ),
        "filesystem.open",
        Errno::EEXIST,
    );
    namespace_refusal(
        fs.open("/namespace", OpenOptions::read_only()),
        "filesystem.open",
        Errno::EISDIR,
    );
    let version = FileVersion::from(&fs.stat("/namespace/file")?);
    for offset in [0, 3, 6] {
        assert_eq!(
            fs.edit_file(
                "/namespace/file",
                version,
                &Edit {
                    offset,
                    delete_bytes: 0,
                    insert: Vec::new(),
                },
            )?,
            version
        );
        assert_eq!(snapshot()?, before);
        assert_eq!(fs.read_file("/namespace/file")?, b"source");
    }
    let storage = context.storage()?;
    for path in ["/", "/namespace/source", "/namespace/nonempty"] {
        let version = FileVersion::from(&fs.stat(path)?);
        for insert in [b"".as_slice(), b"x"] {
            let edit = Edit {
                offset: 0,
                delete_bytes: 0,
                insert: insert.to_vec(),
            };
            namespace_refusal(
                fs.edit_file(path, version, &edit),
                "filesystem.edit_file",
                Errno::EISDIR,
            );
            assert_eq!(snapshot()?, before);
            // A file-type refusal must also roll back preceding file and
            // business-state changes in the same local transaction.
            let result: Result<()> = storage.tx(|tx| {
                tx.put_json("directory-edit-must-rollback", &json!(true))?;
                let fs = tx.fs()?;
                fs.write_file("/namespace/file", b"changed")?;
                fs.edit_file(path, version, &edit)?;
                Ok(())
            });
            namespace_refusal(
                result.map_err(|error| error.downcast::<Error>().unwrap()),
                "filesystem.edit_file",
                Errno::EISDIR,
            );
            assert!(storage.get_json("directory-edit-must-rollback")?.is_none());
            assert_eq!(snapshot()?, before);
            assert_eq!(fs.read_file("/namespace/file")?, b"source");
        }
    }
    namespace_refusal(
        fs.mkdir("/namespace/file", 0o700),
        "filesystem.mkdir",
        Errno::EEXIST,
    );
    namespace_refusal(
        fs.unlink("/namespace/source"),
        "filesystem.unlink",
        Errno::EPERM,
    );
    namespace_refusal(
        fs.rmdir("/namespace/file"),
        "filesystem.rmdir",
        Errno::ENOTDIR,
    );
    namespace_refusal(
        fs.rmdir("/namespace/nonempty"),
        "filesystem.rmdir",
        Errno::ENOTEMPTY,
    );
    namespace_refusal(fs.rmdir("/"), "filesystem.rmdir", Errno::EBUSY);
    for (old, new, errno) in [
        ("/namespace/file", "/namespace/file/", Errno::ENOTDIR),
        ("/namespace/file/", "/namespace/file", Errno::ENOTDIR),
        ("/namespace/file", "/namespace/missing/", Errno::ENOTDIR),
        ("/namespace/file", "/namespace/empty", Errno::EISDIR),
        ("/namespace/source", "/namespace/file", Errno::ENOTDIR),
        ("/namespace/source", "/namespace/nonempty", Errno::ENOTEMPTY),
        (
            "/namespace/nonempty",
            "/namespace/nonempty/child/moved",
            Errno::EINVAL,
        ),
        ("/namespace/source/.", "/namespace/empty", Errno::EINVAL),
    ] {
        namespace_refusal(fs.rename(old, new), "filesystem.rename", errno);
        assert_eq!(snapshot()?, before);
    }
    fs.rename("/namespace/file", "/namespace/file")?;
    fs.rename("/namespace/source", "/namespace/source/")?;
    assert_eq!(snapshot()?, before);
    assert_eq!(fs.read_file("/namespace/file")?, b"source");
    assert_eq!(fs.read_file("/namespace/other")?, b"victim");

    let source = fs.opendir("/namespace/source")?;
    let displaced = fs.opendir("/namespace/empty")?;
    let parent_links = fs.stat("/namespace")?.nlink;
    fs.rename("/namespace/source", "/namespace/empty/")?;
    assert_eq!(fs.stat("/namespace/empty")?.inode, source.fstat()?.inode);
    assert_eq!(displaced.fstat()?.nlink, 0);
    assert_eq!(fs.stat("/namespace")?.nlink, parent_links - 1);
    assert!(displaced.readdir()?.is_none());
    namespace_refusal(
        fs.stat("/namespace/source"),
        "filesystem.stat",
        Errno::ENOENT,
    );
    namespace_refusal(
        fs.mkdirat(&displaced, "refused", 0o700),
        "filesystem.mkdirat",
        Errno::ENOENT,
    );
    source.closedir()?;
    displaced.closedir()?;

    let source = fs.open("/namespace/file", OpenOptions::read_only())?;
    let displaced = fs.open("/namespace/other", OpenOptions::read_only())?;
    fs.rename("/namespace/file", "/namespace/other")?;
    assert_eq!(fs.stat("/namespace/other")?.inode, source.fstat()?.inode);
    assert_eq!(displaced.fstat()?.nlink, 0);
    fs.unlink("/namespace/other")?;
    fs.collect_garbage()?;
    for (file, expected) in [(&source, b"source"), (&displaced, b"victim")] {
        let mut bytes = [0; 6];
        assert_eq!(file.pread(&mut bytes, 0)?, 6);
        assert_eq!(&bytes, expected);
        assert_eq!(file.fstat()?.nlink, 0);
    }
    source.close()?;
    displaced.close()?;
    for path in [
        "/namespace/nonempty/child",
        "/namespace/nonempty",
        "/namespace/empty",
        "/namespace",
    ] {
        fs.rmdir(path)?;
    }
    while fs.collect_garbage()? > 0 {}
    Ok(())
}

#[derive(serde::Deserialize)]
struct ReservedPaths {
    cases: Vec<ReservedPath>,
}

#[derive(serde::Deserialize)]
struct ReservedPath {
    id: String,
    group: String,
    operation: String,
    path: String,
    errno: String,
    base: Option<String>,
    #[serde(default)]
    remove_directory: bool,
}

pub(crate) fn reserved_namespace(fs: &Filesystem) -> Result<()> {
    use cellar::sdk::filesystem::UnlinkFlags;
    while fs.collect_garbage()? > 0 {}
    fs.mkdir("/reserved", 0o700)?;
    fs.mkdir("/reserved/child", 0o700)?;
    fs.write_file("/reserved/file", b"kept")?;
    let parent = fs.opendir("/reserved")?;
    let child = fs.opendir("/reserved/child")?;
    let snapshot = || -> Result<_> {
        Ok((
            ["/", "/reserved", "/reserved/child", "/reserved/file"]
                .iter()
                .map(|path| fs.stat(path))
                .collect::<cellar::sdk::filesystem::Result<Vec<_>>>()?,
            fs.statvfs()?,
        ))
    };
    let before = snapshot()?;
    let fixture: ReservedPaths =
        serde_json::from_str(include_str!("filesystem_reserved_paths.json"))?;
    assert_eq!(fixture.cases.len(), 42);
    for case in fixture.cases {
        let base = match case.base.as_deref() {
            Some("parent") => &parent,
            Some("child") => &child,
            None => &parent,
            Some(value) => panic!("unknown reserved-path base {value}"),
        };
        let (result, operation) = match case.operation.as_str() {
            "mkdir" => (fs.mkdir(&case.path, 0o700), "filesystem.mkdir"),
            "rmdir" => (fs.rmdir(&case.path), "filesystem.rmdir"),
            "unlink" => (fs.unlink(&case.path), "filesystem.unlink"),
            "mkdirat" => (fs.mkdirat(base, &case.path, 0o700), "filesystem.mkdirat"),
            "unlinkat" => (
                fs.unlinkat(
                    base,
                    &case.path,
                    if case.remove_directory {
                        UnlinkFlags::RemoveDirectory
                    } else {
                        UnlinkFlags::File
                    },
                ),
                "filesystem.unlinkat",
            ),
            value => panic!("unknown reserved-path operation {value}"),
        };
        let error = result.expect_err(&case.id);
        assert_eq!(
            format!("{:?}", error.errno),
            case.errno,
            "{} ({})",
            case.id,
            case.group
        );
        assert_eq!(
            (
                error.code,
                error.operation,
                error.outcome,
                error.completed_bytes
            ),
            ("Filesystem", operation, Outcome::NotApplied, 0),
            "{}",
            case.id
        );
        assert_eq!(snapshot()?, before, "{}", case.id);
    }
    assert_eq!(fs.read_file("/reserved/file")?, b"kept");
    assert_eq!(child.readdir()?.unwrap().name, b".");
    let cookie = child.telldir()?;
    fs.rmdir("/reserved/child")?;
    assert_eq!(child.fstat()?.nlink, 0);
    assert!(child.readdir()?.is_none());
    assert_eq!(child.telldir()?, cookie);
    child.rewinddir()?;
    assert!(child.read_page(3)?.is_empty());
    child.seekdir(cookie)?;
    assert!(child.readdir()?.is_none());
    namespace_refusal(
        fs.mkdirat(&child, "refused", 0o700),
        "filesystem.mkdirat",
        Errno::ENOENT,
    );
    child.closedir()?;
    parent.closedir()?;
    fs.unlink("/reserved/file")?;
    fs.rmdir("/reserved")?;
    while fs.collect_garbage()? > 0 {}
    Ok(())
}
