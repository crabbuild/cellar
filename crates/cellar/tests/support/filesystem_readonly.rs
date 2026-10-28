// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Seed with a writer, then reopen the same durable cell under a read-only class.
use anyhow::{bail, ensure, Context as _, Result};
use cellar::sdk::filesystem::{
    Edit, Errno, FileVersion, Filesystem, OpenOptions, Outcome, Policy, Seek, Stat, Time, Times,
    UnlinkFlags,
};
use cellar::sdk::{
    Application, Cell, Context, HandlerFuture, Invocation, NodeOutcome, Output, Registry,
    ShutdownMode,
};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

fn metadata(value: Stat) -> Value {
    json!({"inode":value.inode.to_string(),"kind":value.kind,"mode":value.mode,"uid":value.uid,"gid":value.gid,
        "size":value.size.to_string(),"nlink":value.nlink.to_string(),"atime_ns":value.atime_ns.to_string(),
        "mtime_ns":value.mtime_ns.to_string(),"ctime_ns":value.ctime_ns.to_string(),"version":value.version.to_string(),"allocated_bytes":value.allocated_bytes.to_string()})
}

fn snapshot(fs: &Filesystem) -> Result<Value> {
    let usage = fs.statvfs()?;
    Ok(
        json!({"file":metadata(fs.stat("/kept")?),"directory":metadata(fs.stat("/directory")?),"root":metadata(fs.stat("/")?),
        "usage":{"logical_bytes":usage.logical_bytes.to_string(),"allocated_bytes":usage.allocated_bytes.to_string(),
        "inodes":usage.inodes.to_string(),"available_logical_bytes":usage.available_logical_bytes.to_string(),
        "available_allocated_bytes":usage.available_allocated_bytes.to_string(),"available_inodes":usage.available_inodes.to_string(),"chunk_bytes":usage.chunk_bytes}}),
    )
}

fn refuses<T>(
    result: cellar::sdk::filesystem::Result<T>,
    operation: &str,
    errno: Errno,
) -> Result<()> {
    let error = match result {
        Err(error) => error,
        Ok(_) => bail!("{operation} unexpectedly succeeded under read-only policy"),
    };
    ensure!(
        error.code == "Filesystem"
            && error.errno == errno
            && error.operation == operation
            && error.outcome == Outcome::NotApplied
            && error.completed_bytes == 0,
        "unexpected read-only failure: {error:?}"
    );
    Ok(())
}

async fn check(context: &Context) -> Result<()> {
    let fs = context.fs()?;
    let storage = context.storage()?;
    ensure!(fs.capabilities()?.read_only && storage.fs()?.capabilities()?.read_only);
    let expected = storage
        .get_json("readonly_expected")?
        .context("missing writer checkpoint")?;
    ensure!(
        snapshot(&fs)? == expected,
        "read-only recovery changed file state"
    );
    let file = fs.open("/kept", OpenOptions::read_only().create(true))?;
    let root = fs.opendir("/")?;
    let dir = fs.opendir("/directory")?;
    let copied = file.dup()?;
    let mut bytes = [0; 2];
    copied.read_exact(&mut bytes)?;
    ensure!(&bytes == b"or" && file.lseek(Seek::Current(0))? == 2);
    copied.close()?;
    let mut all = [0; 8];
    ensure!(file.pread(&mut all, 0)? == 8 && &all == b"original");
    fs.openat(&root, "kept", OpenOptions::read_only().create(true))?
        .close()?;
    refuses(
        fs.open("/kept", OpenOptions::read_only().create_new(true)),
        "filesystem.open",
        Errno::EEXIST,
    )?;
    refuses(
        fs.open("/missing", OpenOptions::read_only()),
        "filesystem.open",
        Errno::ENOENT,
    )?;
    refuses(
        fs.open("/kept", OpenOptions::read_only().truncate(true)),
        "filesystem.open",
        Errno::EINVAL,
    )?;
    let cookie = dir.telldir()?;
    // Unsupported mode bits are an option refusal even under read-only policy.
    // They must not masquerade as a writable-operation permission refusal.
    for mode in [0o1600, 0o2600, 0o4600, 0o100600, u32::MAX] {
        refuses(
            fs.chmod("/kept", mode),
            "filesystem.chmod",
            Errno::EOPNOTSUPP,
        )?;
        refuses(file.fchmod(mode), "filesystem.fchmod", Errno::EOPNOTSUPP)?;
        ensure!(snapshot(&fs)? == expected && fs.read_file("/kept")? == b"original");
        ensure!(file.lseek(Seek::Current(0))? == 2 && dir.telldir()? == cookie);
    }
    let version = FileVersion::from(&file.fstat()?);
    let edit = Edit {
        offset: 0,
        delete_bytes: 1,
        insert: b"X".to_vec(),
    };
    macro_rules! rejected {
        ($result:expr,$operation:literal) => {{
            refuses($result, concat!("filesystem.", $operation), Errno::EROFS)?;
            ensure!(
                snapshot(&fs)? == expected,
                "{} changed read-only file state",
                $operation
            );
        }};
    }
    rejected!(fs.mkdir("/forbidden", 0o700), "mkdir");
    rejected!(fs.mkdirat(&dir, "forbidden", 0o700), "mkdirat");
    rejected!(fs.rmdir("/directory"), "rmdir");
    rejected!(fs.unlink("/kept"), "unlink");
    rejected!(fs.unlinkat(&root, "kept", UnlinkFlags::File), "unlinkat");
    rejected!(fs.rename("/kept", "/renamed"), "rename");
    rejected!(fs.renameat(&root, "kept", &dir, "renamed"), "renameat");
    rejected!(fs.truncate("/kept", 0), "truncate");
    rejected!(fs.chmod("/kept", 0o400), "chmod");
    rejected!(
        fs.utimens(
            "/kept",
            Times {
                atime: Time::Omit,
                mtime: Time::Value(0)
            }
        ),
        "utimens"
    );
    rejected!(fs.write_file("/kept", b"bad"), "write_file");
    rejected!(fs.replace_file("/kept", b"bad"), "replace_file");
    rejected!(fs.edit_file("/kept", version, &edit), "edit_file");
    rejected!(fs.open("/kept", OpenOptions::read_write()), "open");
    rejected!(
        fs.open("/kept", OpenOptions::write_only().truncate(true)),
        "open"
    );
    rejected!(
        fs.open("/missing", OpenOptions::read_only().create(true)),
        "open"
    );
    rejected!(
        fs.openat(&root, "missing", OpenOptions::read_only().create_new(true)),
        "openat"
    );
    rejected!(fs.creat("/missing", 0o600), "creat");
    rejected!(file.fchmod(0o400), "fchmod");
    rejected!(fs.collect_garbage(), "collect_garbage");
    refuses(file.write(b"bad"), "filesystem.write", Errno::EBADF)?;
    refuses(file.pwrite(b"bad", 0), "filesystem.pwrite", Errno::EBADF)?;
    refuses(file.ftruncate(0), "filesystem.ftruncate", Errno::EBADF)?;
    fs.utimens(
        "/kept",
        Times {
            atime: Time::Omit,
            mtime: Time::Omit,
        },
    )?;
    file.fsync().await?;
    file.fdatasync().await?;
    dir.fsync().await?;
    file.close()?;
    root.closedir()?;
    dir.closedir()?;
    ensure!(snapshot(&fs)? == expected && fs.read_file("/kept")? == b"original");
    ensure!(fs.stat("/missing").unwrap_err().errno == Errno::ENOENT);
    let rollback: Result<()> = storage.tx(|tx| {
        tx.put_json("readonly_must_rollback", &json!(true))?;
        tx.fs()?.write_file("/kept", b"bad")?;
        Ok(())
    });
    let error = rollback.expect_err("read-only transaction unexpectedly committed");
    let error = error
        .downcast_ref::<cellar::sdk::filesystem::Error>()
        .context("lost filesystem error")?;
    ensure!(error.errno == Errno::EROFS && error.outcome == Outcome::NotApplied);
    ensure!(storage.get_json("readonly_must_rollback")?.is_none());
    for descriptor in [false, true] {
        let rollback: Result<()> = storage.tx(|tx| {
            tx.put_json("readonly_mode_must_rollback", &json!(true))?;
            let fs = tx.fs()?;
            if descriptor {
                fs.open("/kept", OpenOptions::read_only())?.fchmod(0o4600)?;
            } else {
                fs.chmod("/kept", 0o4600)?;
            }
            Ok(())
        });
        let error = rollback.expect_err("unsupported mode transaction unexpectedly committed");
        let error = error
            .downcast_ref::<cellar::sdk::filesystem::Error>()
            .context("lost unsupported mode error")?;
        ensure!(
            error.errno == Errno::EOPNOTSUPP
                && error.outcome == Outcome::NotApplied
                && error.completed_bytes == 0
                && error.operation
                    == if descriptor {
                        "filesystem.fchmod"
                    } else {
                        "filesystem.chmod"
                    }
        );
        ensure!(storage.get_json("readonly_mode_must_rollback")?.is_none());
        ensure!(snapshot(&fs)? == expected);
    }
    storage.tx(|tx| {
        let fs = tx.fs()?;
        ensure!(fs.capabilities()?.read_only);
        let file = fs.open("/kept", OpenOptions::read_only())?;
        let mut bytes = [0; 8];
        file.read_exact(&mut bytes)?;
        ensure!(&bytes == b"original");
        file.close()?;
        tx.put_json("readonly_business_state", &json!(true))
    })?;
    ensure!(storage.get_json("readonly_business_state")? == Some(json!(true)));
    ensure!(snapshot(&fs)? == expected);
    fs.sync().await?;
    Ok(())
}

struct Counter;

impl Cell for Counter {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(_) = invocation else {
                return Ok(Output::Done);
            };
            let fs = context.fs()?;
            if fs.capabilities()?.read_only {
                check(&context).await?;
                Ok(response("ok"))
            } else {
                let storage = context.storage()?;
                storage.tx(|tx| {
                    let fs = tx.fs()?;
                    fs.mkdir("/directory", 0o700)?;
                    fs.write_file("/kept", b"original")?;
                    tx.put_json("readonly_expected", &snapshot(&fs)?)
                })?;
                storage.sync().await?;
                Ok(response("seeded"))
            }
        })
    }
}

fn response(body: &str) -> Output {
    Output::Http(cellar::engine_api::HttpResponse {
        status: 200,
        body: body.as_bytes().to_vec(),
        stream: None,
        headers: Vec::new(),
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}

pub fn application(read_only: bool) -> Result<Arc<dyn Application>> {
    Ok(Arc::new(
        Registry::new(|context, invocation| {
            Box::pin(async move {
                let Invocation::Fetch(request) = invocation else {
                    return Ok(Output::Done);
                };
                Ok(Output::Http(
                    context
                        .namespace("COUNTERS")?
                        .get_by_name("readonly")
                        .fetch(&context, request)
                        .await?,
                ))
            })
        })
        .cell("Counter", |_, _| Ok(Arc::new(Counter)))?
        .filesystem(
            "Counter",
            Policy {
                read_only,
                ..Policy::default()
            },
        )?,
    ))
}

pub async fn exercise(
    reader: Arc<dyn Application>,
    mut observe: impl FnMut(NodeOutcome),
) -> Result<()> {
    let directory = tempfile::tempdir()?;
    for (application, path, wanted) in [
        (application(false)?, "seed", "seeded"),
        (reader, "readonly", "ok"),
    ] {
        let (node, address) =
            super::native::start_configured(directory.path(), application, |config, _, _| {
                config.runtime.handler_budget = Duration::from_secs(2)
            })
            .await?;
        let result = async {
            for _ in 0..if path == "readonly" { 2 } else { 1 } {
                let reply = reqwest::get(format!("{address}/readonly/{path}")).await?;
                let status = reply.status();
                let body = reply.text().await?;
                ensure!(
                    status.is_success() && body == wanted,
                    "read-only {path}: {status} {body}"
                );
            }
            Ok(())
        }
        .await;
        let outcome = node.shutdown(ShutdownMode::Preserve).await?;
        observe(outcome.clone());
        result?;
        let NodeOutcome::Stopped(report) = outcome else {
            bail!("read-only node failed")
        };
        ensure!(
            report.application_work_drained && !report.drain_timed_out,
            "read-only work leaked: {report:?}"
        );
    }
    Ok(())
}
