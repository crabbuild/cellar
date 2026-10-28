// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::application::filesystem::{types::Path, FsPath, OpenOptions};

fn path(value: impl Into<FsPath>) -> Path {
    let value: FsPath = value.into();
    Path::parse(value, FilesystemLimits::default()).unwrap()
}

fn setup() -> Result<(super::super::Cells, tempfile::TempDir)> {
    let directory = tempfile::tempdir()?;
    Ok((super::super::Cells::default(), directory))
}

fn open_connection(directory: &tempfile::TempDir) -> Result<()> {
    super::super::open_at_epoch(
        "Fs:test",
        directory.path().join("cell.sqlite").to_str().unwrap(),
        1,
        true,
        None,
        false,
    )
}

fn open(directory: &tempfile::TempDir) -> Result<()> {
    open_connection(directory)?;
    initialize("Fs:test", FilesystemLimits::default())
}

async fn in_domain(test: impl FnOnce() -> Result<()>) -> Result<()> {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let result = domain.run(async { test() }).await;
    domain.shutdown().await;
    result?
}

// Include identity/cookie counters, hidden data and durable wake discovery,
// which public metadata alone cannot prove unchanged after a refusal.
fn persistent_filesystem_state(limits: FilesystemLimits) -> Result<Vec<rusqlite::types::Value>> {
    read("Fs:test", limits, |db| {
        let mut values = Vec::new();
        for table in [
            "__sys_fs_meta",
            "__sys_fs_inodes",
            "__sys_fs_entries",
            "__sys_fs_chunks",
            "__sys_fs_discarded_ranges",
            "__sys_work",
        ] {
            values.push(rusqlite::types::Value::from(table.to_owned()));
            let mut statement = db
                .connection
                .prepare(&format!("SELECT * FROM {table} ORDER BY 1,2"))?;
            let columns = statement.column_count();
            let mut rows = statement.query([])?;
            while let Some(row) = rows.next()? {
                for column in 0..columns {
                    values.push(row.get(column)?);
                }
            }
        }
        Ok(values)
    })
}

#[tokio::test]
async fn readonly_open_preserves_existing_files_and_refuses_creation_without_effects() -> Result<()>
{
    in_domain(|| {
        let (cells, directory) = setup()?;
        let _installed = cells.install();
        open(&directory)?;
        let limits = FilesystemLimits::default();
        let inode = mutate("Fs:test", limits, |db| {
            let inode = db.open(
                1,
                &path("/kept"),
                OpenOptions::read_write().create_new(true),
                0o022,
            )?;
            db.pwrite(inode, 0, b"original", false)?;
            Ok(inode)
        })?;
        let before = persistent_filesystem_state(limits)?;
        assert_eq!(
            read("Fs:test", limits, |db| db.open_read_only(
                1,
                &path("/kept"),
                OpenOptions::read_only().create(true)
            ))?,
            inode
        );
        assert_eq!(persistent_filesystem_state(limits)?, before);
        for (name, options, expected) in [
            ("/kept", OpenOptions::read_write(), Errno::EROFS),
            (
                "/kept",
                OpenOptions::write_only().truncate(true),
                Errno::EROFS,
            ),
            (
                "/missing",
                OpenOptions::read_only().create(true),
                Errno::EROFS,
            ),
            (
                "/missing",
                OpenOptions::read_only().create_new(true),
                Errno::EROFS,
            ),
            (
                "/kept",
                OpenOptions::read_only().create_new(true),
                Errno::EEXIST,
            ),
            ("/missing", OpenOptions::read_only(), Errno::ENOENT),
            (
                "/absent/child",
                OpenOptions::read_only().create(true),
                Errno::ENOENT,
            ),
            (
                "/kept/child",
                OpenOptions::read_only().create(true),
                Errno::ENOTDIR,
            ),
            (
                "/kept",
                OpenOptions::read_only().truncate(true),
                Errno::EINVAL,
            ),
        ] {
            let error = Error::from(
                read("Fs:test", limits, |db| {
                    db.open_read_only(1, &path(name), options)
                })
                .unwrap_err(),
            );
            assert_eq!(
                (error.errno, error.outcome),
                (
                    expected,
                    crate::application::filesystem::Outcome::NotApplied
                ),
                "{name}"
            );
            assert_eq!(persistent_filesystem_state(limits)?, before);
        }
        super::super::close("Fs:test");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn reserved_namespace_creation_and_removal_follow_operation_specific_errors() -> Result<()> {
    in_domain(|| {
        let (cells, directory) = setup()?;
        let _installed = cells.install();
        open(&directory)?;
        let limits = FilesystemLimits::default();
        let (parent, child) = mutate("Fs:test", limits, |db| {
            db.mkdir(1, &path("/parent"), 0o700, 0o022)?;
            db.mkdir(1, &path("/parent/child"), 0o700, 0o022)?;
            db.open(
                1,
                &path("/parent/file"),
                OpenOptions::read_write().create_new(true),
                0o022,
            )?;
            Ok((
                db.resolve(1, &path("/parent"))?,
                db.resolve(1, &path("/parent/child"))?,
            ))
        })?;
        let before = persistent_filesystem_state(limits)?;
        for (start, name, create, expected) in [
            (1, "/", true, Errno::EEXIST),
            (1, "//", true, Errno::EEXIST),
            (1, "/./", true, Errno::EEXIST),
            (1, ".", true, Errno::EEXIST),
            (1, "..", true, Errno::EEXIST),
            (1, "/parent/.", true, Errno::EEXIST),
            (1, "/parent/child/..", true, Errno::EEXIST),
            (1, "/parent/../..", true, Errno::EEXIST),
            (1, "/missing/.", true, Errno::ENOENT),
            (1, "/missing/..", true, Errno::ENOENT),
            (1, "/parent/file/.", true, Errno::ENOTDIR),
            (1, "/", false, Errno::EBUSY),
            (1, "//", false, Errno::EBUSY),
            (1, "/parent/..", false, Errno::EBUSY),
            (1, "/.", false, Errno::EBUSY),
            (1, "/parent/.", false, Errno::EINVAL),
            (1, "/parent/./", false, Errno::EINVAL),
            (1, "/parent/child/..", false, Errno::ENOTEMPTY),
            (1, "/parent/file/..", false, Errno::ENOTDIR),
            (1, "/missing/..", false, Errno::ENOENT),
            (parent, ".", true, Errno::EEXIST),
            (parent, "..", true, Errno::EEXIST),
            (parent, ".", false, Errno::EINVAL),
            (parent, "..", false, Errno::EBUSY),
            (child, "..", false, Errno::ENOTEMPTY),
            (child, "../..", true, Errno::EEXIST),
        ] {
            let error = Error::from(
                mutate("Fs:test", limits, |db| {
                    if create {
                        db.mkdir(start, &path(name), 0o700, 0o022)
                    } else {
                        db.unlink(start, &path(name), true)
                    }
                })
                .unwrap_err(),
            );
            assert_eq!(
                (error.errno, error.outcome),
                (
                    expected,
                    crate::application::filesystem::Outcome::NotApplied
                ),
                "start={start} path={name} mkdir={create}"
            );
            assert_eq!(persistent_filesystem_state(limits)?, before);
        }
        super::super::close("Fs:test");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn reserved_namespace_unlink_refuses_directory_identities_with_eperm() -> Result<()> {
    in_domain(|| {
        let (cells, directory) = setup()?;
        let _installed = cells.install();
        open(&directory)?;
        let limits = FilesystemLimits::default();
        let parent = mutate("Fs:test", limits, |db| {
            db.mkdir(1, &path("/parent"), 0o700, 0o022)?;
            db.mkdir(1, &path("/parent/child"), 0o700, 0o022)?;
            db.open(
                1,
                &path("/parent/file"),
                OpenOptions::read_write().create_new(true),
                0o022,
            )?;
            db.resolve(1, &path("/parent"))
        })?;
        let before = persistent_filesystem_state(limits)?;
        for (start, name, expected) in [
            (1, "/parent", Errno::EPERM),
            (1, "/parent/.", Errno::EPERM),
            (1, "/parent/child/..", Errno::EPERM),
            (1, "/parent/child", Errno::EPERM),
            (1, "/", Errno::EBUSY),
            (1, "/parent/..", Errno::EBUSY),
            (1, "/parent/file/.", Errno::ENOTDIR),
            (1, "/missing/.", Errno::ENOENT),
            (parent, ".", Errno::EPERM),
            (parent, "child/..", Errno::EPERM),
            (parent, "..", Errno::EBUSY),
        ] {
            let error = Error::from(
                mutate("Fs:test", limits, |db| db.unlink(start, &path(name), false)).unwrap_err(),
            );
            assert_eq!(
                (error.errno, error.outcome),
                (
                    expected,
                    crate::application::filesystem::Outcome::NotApplied
                ),
                "start={start} path={name}"
            );
            assert_eq!(persistent_filesystem_state(limits)?, before);
        }
        super::super::close("Fs:test");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn rename_trailing_slash_requires_a_directory_even_for_the_same_inode() -> Result<()> {
    in_domain(|| {
        let (cells, directory) = setup()?;
        let _installed = cells.install();
        open(&directory)?;
        let limits = FilesystemLimits::default();
        let inode = mutate("Fs:test", limits, |db| {
            db.mkdir(1, &path("/directory"), 0o700, 0o022)?;
            let inode = db.open(
                1,
                &path("/file"),
                OpenOptions::read_write().create_new(true),
                0o022,
            )?;
            db.pwrite(inode, 0, b"retained", false)?;
            Ok(inode)
        })?;
        let snapshot = || persistent_filesystem_state(limits);
        let before = snapshot()?;
        for (old, new) in [
            ("/file", "/file/"),
            ("/file/", "/file"),
            ("/file", "/missing/"),
            ("/file", "/directory/file/"),
        ] {
            let error = Error::from(
                mutate("Fs:test", limits, |db| {
                    db.rename(1, &path(old), 1, &path(new))
                })
                .unwrap_err(),
            );
            assert_eq!(
                (error.errno, error.outcome),
                (
                    Errno::ENOTDIR,
                    crate::application::filesystem::Outcome::NotApplied
                )
            );
            assert_eq!(snapshot()?, before);
        }
        for (old, new) in [("/file", "/file"), ("/directory", "/directory/")] {
            mutate("Fs:test", limits, |db| {
                db.rename(1, &path(old), 1, &path(new))
            })?;
            assert_eq!(snapshot()?, before);
        }
        let mut contents = [0; 8];
        assert_eq!(
            read("Fs:test", limits, |db| db.pread(inode, 0, &mut contents))?,
            8
        );
        assert_eq!(&contents, b"retained");
        super::super::close("Fs:test");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn inspection_preserves_absence_and_accounts_for_overlapping_discarded_ranges() -> Result<()>
{
    in_domain(|| {
        let (cells, directory) = setup()?;
        let _installed = cells.install();
        open_connection(&directory)?;
        let before = with("Fs:test", |connection| connection.total_changes()).unwrap();
        assert!(inspect("Fs:test")?.is_none());
        assert_eq!(
            with("Fs:test", |connection| connection.total_changes()).unwrap(),
            before
        );
        assert!(!with("Fs:test", initialized).unwrap()?);
        let limits = FilesystemLimits::default();
        initialize("Fs:test", limits)?;
        let orphan = mutate("Fs:test", limits, |db| {
            let live = db.open(
                1,
                &path("/private-name"),
                OpenOptions::read_write().create_new(true),
                0o022,
            )?;
            db.pwrite(live, 0, &vec![7; 3 * CHUNK_BYTES], false)?;
            db.truncate(live, CHUNK_BYTES as u64)?;
            db.truncate(live, (3 * CHUNK_BYTES) as u64)?;
            db.truncate(live, CHUNK_BYTES as u64)?;
            db.pwrite(live, (2 * CHUNK_BYTES) as u64, &[9], false)?;
            let orphan = db.open(
                1,
                &path("/orphan"),
                OpenOptions::read_write().create_new(true),
                0o022,
            )?;
            db.pwrite(orphan, 0, &vec![3; 2 * CHUNK_BYTES], false)?;
            db.unlink(1, &path("/orphan"), false)?;
            Ok(orphan)
        })?;
        let before = with("Fs:test", |connection| connection.total_changes()).unwrap();
        let sample = inspect("Fs:test")?.unwrap();
        assert_eq!(
            with("Fs:test", |connection| connection.total_changes()).unwrap(),
            before
        );
        assert_eq!(sample.inodes, 3);
        assert_eq!(sample.allocated_bytes, (5 * CHUNK_BYTES) as u64);
        assert_eq!(
            (
                sample.unlinked_inodes,
                sample.unlinked_logical_bytes,
                sample.unlinked_allocated_bytes
            ),
            (1, (2 * CHUNK_BYTES) as u64, (2 * CHUNK_BYTES) as u64)
        );
        assert_eq!(sample.discarded_ranges, 2);
        // Both ranges cover the old chunk, but the rewritten chunk is live.
        assert_eq!(sample.discarded_allocated_bytes, CHUNK_BYTES as u64);
        while mutate("Fs:test", limits, |db| {
            db.collect(&std::collections::HashSet::from([orphan]))
        })? > 0
        {}
        let pinned = inspect("Fs:test")?.unwrap();
        assert_eq!(pinned.unlinked_allocated_bytes, (2 * CHUNK_BYTES) as u64);
        assert_eq!(pinned.discarded_allocated_bytes, 0);
        while mutate("Fs:test", limits, |db| db.collect(&Default::default()))? > 0 {}
        let reclaimed = inspect("Fs:test")?.unwrap();
        assert_eq!(
            (
                reclaimed.inodes,
                reclaimed.unlinked_inodes,
                reclaimed.discarded_ranges
            ),
            (2, 0, 0)
        );
        assert_eq!(reclaimed.allocated_bytes, (2 * CHUNK_BYTES) as u64);
        super::super::close("Fs:test");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn inspection_interrupts_excess_metadata_work_and_restores_the_connection() -> Result<()> {
    in_domain(|| {
    let (cells, directory) = setup()?;
    let _installed = cells.install();
    open(&directory)?;
    let limits = FilesystemLimits {
        max_inodes: 250_000,
        ..Default::default()
    };
    mutate("Fs:test", limits, |db| {
        // Valid, empty orphan identities stress metadata aggregation without
        // creating file contents or a large allocation in the test driver.
        db.connection.execute_batch("WITH RECURSIVE ids(id) AS (SELECT 2 UNION ALL SELECT id+1 FROM ids WHERE id<200001)
            INSERT INTO __sys_fs_inodes(id,kind,parent,mode,size,nlink,mtime_ns,ctime_ns,version,next_cookie)
            SELECT id,0,1,384,0,0,0,0,1,1 FROM ids;
            UPDATE __sys_fs_meta SET next_inode=200002 WHERE singleton=1;")?;
        Ok(())
    })?;
    let before = with("Fs:test", |connection| connection.total_changes()).unwrap();
    let error = inspect("Fs:test").unwrap_err();
    assert_eq!(error.downcast_ref::<rusqlite::Error>().and_then(rusqlite::Error::sqlite_error_code),
        Some(rusqlite::ErrorCode::OperationInterrupted));
    assert_eq!(
        with("Fs:test", |connection| connection.total_changes()).unwrap(),
        before
    );
    assert_eq!(read("Fs:test", limits, |db| db.statvfs())?.inodes, 200001);
    // A subsequent ordinary mutation proves the temporary progress callback
    // was removed, rather than leaving an exhausted connection-wide hook.
    mutate("Fs:test", limits, |db| {
        db.open(
            1,
            &path("/after-observation"),
            OpenOptions::read_write().create_new(true),
            0o022,
        )?;
        Ok(())
    })?;
    super::super::close("Fs:test");
    Ok(())
    }).await
}

#[test]
fn range_edits_sparse_growth_and_truncate_gc_preserve_new_content() -> Result<()> {
    let (cells, directory) = setup()?;
    let _installed = cells.install();
    open(&directory)?;
    let limits = FilesystemLimits {
        gc_batch_chunks: 1,
        max_allocated_bytes: (3 * CHUNK_BYTES) as u64,
        ..FilesystemLimits::default()
    };
    let inode = mutate("Fs:test", limits, |db| {
        db.open(
            1,
            &path("/data"),
            OpenOptions::read_write().create_new(true),
            0o022,
        )
    })?;
    mutate("Fs:test", limits, |db| {
        db.pwrite(inode, 0, &vec![7; 3 * CHUNK_BYTES], false)?;
        db.truncate(inode, 5)?;
        db.truncate(inode, (3 * CHUNK_BYTES) as u64)?;
        db.pwrite(inode, (CHUNK_BYTES + 9) as u64, b"new", false)?;
        Ok(())
    })?;
    let mut bytes = vec![1; 3 * CHUNK_BYTES];
    read("Fs:test", limits, |db| {
        assert_eq!(db.pread(inode, 0, &mut bytes)?, bytes.len());
        Ok(())
    })?;
    assert_eq!(&bytes[..5], &[7; 5]);
    assert!(bytes[5..CHUNK_BYTES + 9].iter().all(|byte| *byte == 0));
    assert_eq!(&bytes[CHUNK_BYTES + 9..CHUNK_BYTES + 12], b"new");
    assert!(bytes[CHUNK_BYTES + 12..].iter().all(|byte| *byte == 0));
    while mutate("Fs:test", limits, |db| db.collect(&Default::default()))? > 0 {}
    let mut after = vec![1; bytes.len()];
    read("Fs:test", limits, |db| {
        db.pread(inode, 0, &mut after)?;
        Ok(())
    })?;
    assert_eq!(after, bytes);
    mutate("Fs:test", limits, |db| {
        db.pwrite(inode, 100_000_000, b"x", false)?;
        Ok(())
    })?;
    let stats = read("Fs:test", limits, |db| db.statvfs())?;
    assert_eq!(stats.logical_bytes, 100_000_001);
    assert_eq!(stats.allocated_bytes, 3 * CHUNK_BYTES as u64);
    super::super::close("Fs:test");
    Ok(())
}

#[test]
fn quota_rollback_namespace_replacement_and_protected_tables() -> Result<()> {
    let (cells, directory) = setup()?;
    let _installed = cells.install();
    open(&directory)?;
    let limits = FilesystemLimits {
        max_allocated_bytes: CHUNK_BYTES as u64,
        ..FilesystemLimits::default()
    };
    let inode = mutate("Fs:test", limits, |db| {
        db.open(
            1,
            &path("/one"),
            OpenOptions::read_write().create(true),
            0o022,
        )
    })?;
    let error = mutate("Fs:test", limits, |db| {
        db.pwrite(inode, 0, &vec![8; CHUNK_BYTES + 1], false)
    })
    .unwrap_err();
    assert_eq!(Error::from(error).errno, Errno::EDQUOT);
    assert_eq!(read("Fs:test", limits, |db| db.stat(inode))?.size, 0);
    assert_eq!(
        read("Fs:test", limits, |db| db.statvfs())?.allocated_bytes,
        0
    );
    mutate("Fs:test", limits, |db| {
        db.mkdir(1, &path("/dir"), 0o777, 0o022)?;
        db.rename(1, &path("/one"), 1, &path("/dir/two"))?;
        assert_eq!(db.resolve(1, &path("/dir/two"))?, inode);
        let invalid = db.rename(1, &path("/dir"), 1, &path("/dir/sub"));
        assert_eq!(Error::from(invalid.unwrap_err()).errno, Errno::EINVAL);
        Ok(())
    })?;
    assert!(super::super::sql_exec("Fs:test", "DELETE FROM __sys_fs_inodes", &[]).is_err());
    assert!(super::super::delete_all_with_alarm("Fs:test", false).is_err());
    mutate("Fs:test", limits, |db| {
        db.unlink(1, &path("/dir/two"), false)?;
        db.unlink(1, &path("/dir"), true)
    })?;
    let pinned = std::collections::HashSet::from([inode]);
    mutate("Fs:test", limits, |db| {
        db.collect(&pinned)?;
        Ok(())
    })?;
    assert_eq!(read("Fs:test", limits, |db| db.stat(inode))?.nlink, 0);
    while mutate("Fs:test", limits, |db| db.collect(&Default::default()))? > 0 {}
    super::super::delete_all_with_alarm("Fs:test", false)?;
    assert_eq!(
        read("Fs:test", limits, |db| db.stat(1))?.kind,
        Kind::Directory
    );
    super::super::close("Fs:test");
    Ok(())
}

#[test]
fn directory_pins_retain_removed_ancestors_and_metadata_gc_batches_progress() -> Result<()> {
    let (cells, directory) = setup()?;
    let _installed = cells.install();
    open(&directory)?;
    let limits = FilesystemLimits::default();
    let (parent, child) = mutate("Fs:test", limits, |db| {
        db.mkdir(1, &path("/parent"), 0o700, 0)?;
        db.mkdir(1, &path("/parent/child"), 0o700, 0)?;
        let parent = db.resolve(1, &path("/parent"))?;
        let child = db.resolve(1, &path("/parent/child"))?;
        db.unlink(1, &path("/parent/child"), true)?;
        db.unlink(1, &path("/parent"), true)?;
        Ok((parent, child))
    })?;
    assert_eq!(
        mutate("Fs:test", limits, |db| db
            .collect(&std::collections::HashSet::from([child])))?,
        0
    );
    assert_eq!(
        read("Fs:test", limits, |db| db.resolve(child, &path("..")))?,
        parent
    );
    assert_eq!(
        mutate("Fs:test", limits, |db| db.collect(&Default::default()))?,
        2
    );
    assert_eq!(read("Fs:test", limits, |db| db.statvfs())?.inodes, 1);
    super::super::close("Fs:test");
    Ok(())
}

#[test]
fn subtree_renames_obey_depth_and_namespace_work_bounds() -> Result<()> {
    let (cells, directory) = setup()?;
    let _installed = cells.install();
    open(&directory)?;
    let mut limits = FilesystemLimits {
        max_path_depth: 3,
        ..FilesystemLimits::default()
    };
    mutate("Fs:test", limits, |db| {
        for name in ["/a", "/a/child", "/a/child/leaf", "/target"] {
            db.mkdir(1, &path(name), 0o700, 0)?;
        }
        Ok(())
    })?;
    let error = mutate("Fs:test", limits, |db| {
        db.rename(1, &path("/a"), 1, &path("/target/a"))
    })
    .unwrap_err();
    assert_eq!(Error::from(error).errno, Errno::ENAMETOOLONG);
    limits.max_namespace_directories = 2;
    let error = mutate("Fs:test", limits, |db| {
        db.rename(1, &path("/a"), 1, &path("/renamed"))
    })
    .unwrap_err();
    assert_eq!(Error::from(error).errno, Errno::EDQUOT);
    assert!(read("Fs:test", limits, |db| db
        .resolve(1, &path("/a/child/leaf")))
    .is_ok());
    super::super::close("Fs:test");
    Ok(())
}

#[tokio::test]
async fn exhausted_identity_cookie_and_revision_leave_no_partial_mutation() -> Result<()> {
    in_domain(|| {
        let (cells, directory) = setup()?;
        let _installed = cells.install();
        open(&directory)?;
        let limits = FilesystemLimits::default();
        let source = mutate("Fs:test", limits, |db| {
            let source = db.open(
                1,
                &path("/source"),
                OpenOptions::read_write().create_new(true),
                0o022,
            )?;
            let target = db.open(
                1,
                &path("/target"),
                OpenOptions::read_write().create_new(true),
                0o022,
            )?;
            db.pwrite(source, 0, b"source", false)?;
            db.pwrite(target, 0, b"target", false)?;
            Ok(source)
        })?;
        // Capture all persistent effects, including counters, chunk bytes and
        // maintenance discovery. A failed replacement must restore its victim.
        let snapshot = || persistent_filesystem_state(limits);
        mutate("Fs:test", limits, |db| {
            db.connection
                .execute("UPDATE __sys_fs_meta SET next_inode=?1", [i64::MAX - 1])?;
            Ok(())
        })?;
        let last = mutate("Fs:test", limits, |db| {
            db.open(
                1,
                &path("/last-inode"),
                OpenOptions::read_write().create_new(true),
                0o022,
            )
        })?;
        assert_eq!(last, (i64::MAX - 1) as u64);
        let before = snapshot()?;
        let error = Error::from(
            mutate("Fs:test", limits, |db| {
                db.open(
                    1,
                    &path("/overflow"),
                    OpenOptions::read_write().create_new(true),
                    0o022,
                )
            })
            .unwrap_err(),
        );
        assert_eq!(
            (error.errno, error.outcome),
            (
                Errno::EOVERFLOW,
                crate::application::filesystem::Outcome::NotApplied
            )
        );
        assert_eq!(snapshot()?, before);

        // Reset only the test fixture's allocation cursor to isolate cookie
        // exhaustion. Real callers cannot write any of these reserved tables.
        mutate("Fs:test", limits, |db| {
            db.connection
                .execute("UPDATE __sys_fs_meta SET next_inode=10", [])?;
            db.connection.execute(
                "UPDATE __sys_fs_inodes SET next_cookie=?1 WHERE id=1",
                [i64::MAX - 1],
            )?;
            db.mkdir(1, &path("/last-cookie"), 0o700, 0o022)
        })?;
        let before = snapshot()?;
        for replace in [false, true] {
            let error = Error::from(
                mutate("Fs:test", limits, |db| {
                    if replace {
                        db.rename(1, &path("/source"), 1, &path("/target"))
                    } else {
                        db.mkdir(1, &path("/cookie-overflow"), 0o700, 0o022)
                    }
                })
                .unwrap_err(),
            );
            assert_eq!(
                (error.errno, error.outcome),
                (
                    Errno::EOVERFLOW,
                    crate::application::filesystem::Outcome::NotApplied
                )
            );
            assert_eq!(snapshot()?, before);
        }
        mutate("Fs:test", limits, |db| {
            db.connection
                .execute("UPDATE __sys_fs_meta SET revision=?1", [i64::MAX - 1])?;
            db.pwrite(source, 0, b"final!", false)?;
            Ok(())
        })?;
        assert_eq!(
            read("Fs:test", limits, |db| db.stat(source))?.version,
            i64::MAX as u64
        );
        let before = snapshot()?;
        let error = Error::from(
            mutate("Fs:test", limits, |db| db.pwrite(source, 0, b"lost", false)).unwrap_err(),
        );
        assert_eq!(
            (error.errno, error.outcome),
            (
                Errno::EOVERFLOW,
                crate::application::filesystem::Outcome::NotApplied
            )
        );
        assert_eq!(snapshot()?, before);
        super::super::close("Fs:test");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn pinned_orphans_do_not_starve_reclamation_at_the_allocation_quota() -> Result<()> {
    in_domain(|| {
        let (cells, directory) = setup()?;
        let _installed = cells.install();
        open(&directory)?;
        let limits = FilesystemLimits {
            max_allocated_bytes: (3 * CHUNK_BYTES) as u64,
            gc_batch_chunks: 1,
            ..Default::default()
        };
        let (pinned, pressure) = mutate("Fs:test", limits, |db| {
            let pinned = db.open(
                1,
                &path("/pinned"),
                OpenOptions::read_write().create_new(true),
                0o022,
            )?;
            let victim = db.open(
                1,
                &path("/victim"),
                OpenOptions::read_write().create_new(true),
                0o022,
            )?;
            let pressure = db.open(
                1,
                &path("/pressure"),
                OpenOptions::read_write().create_new(true),
                0o022,
            )?;
            db.pwrite(pinned, 0, &vec![1; CHUNK_BYTES], false)?;
            db.pwrite(victim, 0, &vec![2; 2 * CHUNK_BYTES], false)?;
            db.unlink(1, &path("/pinned"), false)?;
            db.unlink(1, &path("/victim"), false)?;
            Ok((pinned, pressure))
        })?;
        // Overwriting an already allocated chunk is allowed at the ceiling.
        mutate("Fs:test", limits, |db| db.pwrite(pinned, 0, &[1], false))?;
        let before = read("Fs:test", limits, |db| db.statvfs())?;
        let error = Error::from(
            mutate("Fs:test", limits, |db| {
                db.pwrite(pressure, 0, b"new", false)
            })
            .unwrap_err(),
        );
        assert_eq!(error.errno, Errno::EDQUOT);
        assert_eq!(read("Fs:test", limits, |db| db.statvfs())?, before);
        assert_eq!(read("Fs:test", limits, |db| db.stat(pressure))?.size, 0);

        let pins = std::collections::HashSet::from([pinned]);
        assert_eq!(mutate("Fs:test", limits, |db| db.collect(&pins))?, 1);
        assert_eq!(
            read("Fs:test", limits, |db| db.statvfs())?.allocated_bytes,
            (2 * CHUNK_BYTES) as u64
        );
        // A later orphan is reclaimed despite the first orphan staying open,
        // and the released physical quota becomes available to another file.
        mutate("Fs:test", limits, |db| {
            db.pwrite(pressure, 0, b"new", false)
        })?;
        for _ in 0..4 {
            if mutate("Fs:test", limits, |db| db.collect(&pins))? == 0 {
                break;
            }
        }
        assert_eq!(read("Fs:test", limits, |db| db.statvfs())?.inodes, 3);
        assert_eq!(read("Fs:test", limits, |db| db.stat(pinned))?.nlink, 0);
        let mut bytes = vec![0; CHUNK_BYTES];
        read("Fs:test", limits, |db| db.pread(pinned, 0, &mut bytes))?;
        assert!(bytes.iter().all(|byte| *byte == 1));
        assert_eq!(mutate("Fs:test", limits, |db| db.collect(&pins))?, 0);
        assert!(read("Fs:test", limits, |db| db.pending_gc())?);
        for _ in 0..3 {
            if mutate("Fs:test", limits, |db| db.collect(&Default::default()))? == 0 {
                break;
            }
        }
        assert!(!read("Fs:test", limits, |db| db.pending_gc())?);
        assert_eq!(
            read("Fs:test", limits, |db| db.statvfs())?.allocated_bytes,
            CHUNK_BYTES as u64
        );
        super::super::close("Fs:test");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn sqlite_full_during_chunk_growth_rolls_back_and_allows_later_writes() -> Result<()> {
    in_domain(|| {
        let (cells, directory) = setup()?;
        let _installed = cells.install();
        open(&directory)?;
        let limits = FilesystemLimits::default();
        let inode = mutate("Fs:test", limits, |db| {
            let inode = db.open(
                1,
                &path("/data"),
                OpenOptions::read_write().create_new(true),
                0o022,
            )?;
            db.pwrite(inode, 0, &vec![7; CHUNK_BYTES], false)?;
            Ok(inode)
        })?;
        let before = read("Fs:test", limits, |db| Ok((db.stat(inode)?, db.statvfs()?)))?;
        let maximum: i64 = with("Fs:test", |connection| {
            // Fixture-only fault setup uses the same scoped mechanism as the
            // native harness; application SQL keeps its pragma restrictions.
            super::super::without_sql_authorizer(connection, || {
                let maximum =
                    connection.pragma_query_value(None, "max_page_count", |row| row.get(0))?;
                let pages: i64 =
                    connection.pragma_query_value(None, "page_count", |row| row.get(0))?;
                connection.pragma_update(None, "max_page_count", pages)?;
                Ok::<_, anyhow::Error>(maximum)
            })
        })
        .unwrap()?;
        let error = Error::from(
            mutate("Fs:test", limits, |db| {
                db.pwrite(inode, CHUNK_BYTES as u64, &vec![9; 2 * CHUNK_BYTES], false)
            })
            .unwrap_err(),
        );
        assert_eq!(error.errno, Errno::ENOSPC);
        // Storage failure is conservatively unknown to the caller. This test
        // independently observes the rollback; it must not turn lost replies
        // into a general promise that retrying a mutation is safe.
        assert_eq!(
            error.outcome,
            crate::application::filesystem::Outcome::Unknown
        );
        assert_eq!(
            read("Fs:test", limits, |db| Ok((db.stat(inode)?, db.statvfs()?)))?,
            before
        );
        let mut bytes = vec![0; CHUNK_BYTES];
        read("Fs:test", limits, |db| db.pread(inode, 0, &mut bytes))?;
        assert!(bytes.iter().all(|byte| *byte == 7));
        with("Fs:test", |connection| {
            super::super::without_sql_authorizer(connection, || {
                connection.pragma_update(None, "max_page_count", maximum)
            })
        })
        .unwrap()?;
        mutate("Fs:test", limits, |db| {
            db.pwrite(inode, CHUNK_BYTES as u64, b"recovered", false)
        })?;
        let mut appended = [0; 9];
        assert_eq!(
            read("Fs:test", limits, |db| db.pread(
                inode,
                CHUNK_BYTES as u64,
                &mut appended
            ))?,
            9
        );
        assert_eq!(&appended, b"recovered");
        super::super::close("Fs:test");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn old_maintenance_constraint_refuses_initialization_and_recovery_without_reset() -> Result<()>
{
    in_domain(|| {
        for initialized_fs in [false, true] {
            let (cells, directory) = setup()?;
            let _installed = cells.install();
            open_connection(&directory)?;
            let limits = FilesystemLimits::default();
            super::super::put_json(
                "Fs:test",
                "business",
                &serde_json::json!({"preserve": true}),
            )?;
            if initialized_fs {
                initialize("Fs:test", limits)?;
                mutate("Fs:test", limits, |db| {
                    let inode = db.open(
                        1,
                        &path("/preserved"),
                        OpenOptions::read_write().create_new(true),
                        0o022,
                    )?;
                    db.pwrite(inode, 0, b"keep", false)?;
                    Ok(())
                })?;
            }
            let old_schema = super::super::work::SCHEMA.replace(", 'filesystem-maintenance'", "");
            assert!(!old_schema.contains("filesystem-maintenance"));
            with("Fs:test", |connection| {
                connection.execute_batch(&format!("DROP TABLE __sys_work; {old_schema}"))
            })
            .unwrap()?;
            if !initialized_fs {
                let error = Error::from(initialize("Fs:test", limits).unwrap_err());
                assert_eq!(error.errno, Errno::EOPNOTSUPP);
                assert_eq!(error.code, "UnsupportedFormat");
                assert!(!with("Fs:test", initialized).unwrap()?);
                assert_eq!(
                    super::super::get_json_value("Fs:test", "business")?,
                    Some(serde_json::json!({"preserve": true}))
                );
            }
            super::super::close("Fs:test");
            if initialized_fs {
                let error = open_connection(&directory).unwrap_err();
                assert!(error
                    .to_string()
                    .contains("unsupported filesystem maintenance format"));
                assert!(
                    with("Fs:test", |_| ()).is_none(),
                    "failed recovery installed a connection"
                );
                let connection = Connection::open(directory.path().join("cell.sqlite"))?;
                let entries: u64 =
                    connection.query_row("SELECT count(*) FROM __sys_fs_entries", [], |row| {
                        row.get(0)
                    })?;
                assert_eq!(entries, 1);
                let bytes: Vec<u8> =
                    connection
                        .query_row("SELECT data FROM __sys_fs_chunks", [], |row| row.get(0))?;
                assert_eq!(&bytes[..4], b"keep");
                let work_schema: String = connection.query_row(
                    "SELECT sql FROM sqlite_schema WHERE name='__sys_work'",
                    [],
                    |row| row.get(0),
                )?;
                assert!(!work_schema.contains("filesystem-maintenance"));
            }
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn aggregate_logical_quota_at_integer_limit_is_a_known_refusal() -> Result<()> {
    in_domain(|| {
        let (cells, directory) = setup()?;
        let _installed = cells.install();
        open(&directory)?;
        let limits = FilesystemLimits {
            max_file_bytes: i64::MAX as u64,
            max_logical_bytes: i64::MAX as u64,
            ..Default::default()
        };
        let (huge, small) = mutate("Fs:test", limits, |db| {
            let huge = db.open(
                1,
                &path("/huge"),
                OpenOptions::read_write().create_new(true),
                0o022,
            )?;
            let small = db.open(
                1,
                &path("/small"),
                OpenOptions::read_write().create_new(true),
                0o022,
            )?;
            db.truncate(huge, i64::MAX as u64)?;
            Ok((huge, small))
        })?;
        mutate("Fs:test", limits, |db| db.pwrite(huge, 0, b"edge", false))?;
        assert_eq!(
            read("Fs:test", limits, |db| db.statvfs())?.logical_bytes,
            i64::MAX as u64
        );
        let before = read("Fs:test", limits, |db| Ok((db.stat(small)?, db.statvfs()?)))?;
        for positional_write in [false, true] {
            let error = Error::from(
                mutate("Fs:test", limits, |db| {
                    if positional_write {
                        db.pwrite(small, 0, b"x", false).map(|_| ())
                    } else {
                        db.truncate(small, 1)
                    }
                })
                .unwrap_err(),
            );
            assert_eq!(
                (error.errno, error.outcome),
                (
                    Errno::EDQUOT,
                    crate::application::filesystem::Outcome::NotApplied
                )
            );
            assert_eq!(
                read("Fs:test", limits, |db| Ok((db.stat(small)?, db.statvfs()?)))?,
                before
            );
        }
        // Shrinking remains permitted after a quota reduction, even while
        // another byte of growth would exceed the new ceiling.
        let lowered = FilesystemLimits {
            max_logical_bytes: 1,
            ..limits
        };
        mutate("Fs:test", lowered, |db| db.truncate(huge, 1))?;
        assert_eq!(
            read("Fs:test", lowered, |db| db.statvfs())?.logical_bytes,
            1
        );
        super::super::close("Fs:test");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn old_resize_accounting_refuses_recovery_without_changing_file_state() -> Result<()> {
    in_domain(|| {
        let (cells, directory) = setup()?;
        let _installed = cells.install();
        open(&directory)?;
        let limits = FilesystemLimits::default();
        mutate("Fs:test", limits, |db| {
            let inode = db.open(
                1,
                &path("/preserved"),
                OpenOptions::read_write().create_new(true),
                0o022,
            )?;
            db.pwrite(inode, 0, b"keep", false)?;
            db.connection.execute_batch(
                "DROP TRIGGER __sys_fs_inode_size;
                CREATE TRIGGER __sys_fs_inode_size AFTER UPDATE OF size ON __sys_fs_inodes BEGIN
                    UPDATE __sys_fs_meta SET logical_bytes=logical_bytes+NEW.size-OLD.size;
                END;",
            )?;
            Ok(())
        })?;
        super::super::close("Fs:test");
        let error = open_connection(&directory).unwrap_err();
        assert!(error
            .to_string()
            .contains("unsupported filesystem quota accounting format"));
        assert!(
            with("Fs:test", |_| ()).is_none(),
            "failed recovery installed a connection"
        );
        let connection = Connection::open(directory.path().join("cell.sqlite"))?;
        let logical: u64 =
            connection.query_row("SELECT logical_bytes FROM __sys_fs_meta", [], |row| {
                row.get(0)
            })?;
        assert_eq!(logical, 4);
        let bytes: Vec<u8> =
            connection.query_row("SELECT data FROM __sys_fs_chunks", [], |row| row.get(0))?;
        assert_eq!(&bytes[..4], b"keep");
        Ok(())
    })
    .await
}
