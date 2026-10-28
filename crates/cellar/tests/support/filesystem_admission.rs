// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Host descriptor admission through independent production execution domains.
use anyhow::{bail, ensure, Result};
use cellar::asyncrt::ExecutionDomain;
use cellar::engine_api::RuntimeOptions;
use cellar::generation::{DeploymentGraph, Generation, GenerationOptions};
use cellar::runtime::RuntimeManager;
use cellar::sdk::filesystem::{Directory, Errno, File, OpenOptions, Outcome, Policy, Seek};
use cellar::sdk::{Cell, Context, HandlerFuture, Invocation, Output, Registry};
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::oneshot;

#[derive(Default)]
struct Holdings {
    files: Mutex<Vec<File>>,
    directories: Mutex<Vec<Directory>>,
    release: Mutex<Vec<oneshot::Sender<()>>>,
}

impl Holdings {
    fn hold(
        self: &Arc<Self>,
        context: &Context,
        files: Vec<File>,
        dirs: Vec<Directory>,
    ) -> Result<()> {
        self.files.lock().unwrap().extend(files);
        self.directories.lock().unwrap().extend(dirs);
        let (send, recv) = oneshot::channel();
        self.release.lock().unwrap().push(send);
        context.wait_until(async move {
            let _ = recv.await;
            Ok(())
        })
    }

    fn release_one(&self) -> Result<()> {
        let sender = self.release.lock().unwrap().remove(0);
        sender
            .send(())
            .map_err(|_| anyhow::anyhow!("filesystem holder already ended"))
    }

    fn release_all(&self) {
        for sender in self.release.lock().unwrap().drain(..) {
            let _ = sender.send(());
        }
    }

    fn clear(&self) {
        self.files.lock().unwrap().clear();
        self.directories.lock().unwrap().clear();
    }
}

fn refused<T>(
    result: cellar::sdk::filesystem::Result<T>,
    operation: &str,
    errno: Errno,
) -> Result<()> {
    let error = match result {
        Err(error) => error,
        Ok(_) => bail!("{operation} unexpectedly succeeded"),
    };
    ensure!(
        error.code == "Filesystem"
            && error.operation == operation
            && error.errno == errno
            && error.outcome == Outcome::NotApplied
            && error.completed_bytes == 0,
        "unexpected filesystem admission failure: {error:?}"
    );
    Ok(())
}

struct Admission(Arc<Holdings>);

impl Cell for Admission {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Ok(Output::Done);
            };
            let path = url::Url::parse(&request.url)?.path().to_owned();
            let fs = context.fs()?;
            ensure!(
                fs.capabilities()?.limits.max_handles == 3
                    && fs.capabilities()?.limits.max_host_handles == 4
            );
            match path.as_str() {
                "/hold" => {
                    fs.write_file("/kept", b"original")?;
                    let file = fs.open("/kept", OpenOptions::read_write())?;
                    let duplicate = file.dup()?;
                    let dir = fs.opendir("/")?;
                    refused(
                        fs.open("/local-refused", OpenOptions::write_only().create(true)),
                        "filesystem.open",
                        Errno::EMFILE,
                    )?;
                    self.0.hold(&context, vec![file, duplicate], vec![dir])?;
                    Ok(super::response("holding three"))
                }
                "/host-full" => {
                    fs.write_file("/kept", b"original")?;
                    let file = fs.open("/kept", OpenOptions::read_write())?;
                    let before = (file.fstat()?, fs.statvfs()?);
                    refused(file.dup(), "filesystem.dup", Errno::ENFILE)?;
                    refused(fs.opendir("/"), "filesystem.opendir", Errno::ENFILE)?;
                    refused(
                        fs.open("/host-refused", OpenOptions::write_only().create_new(true)),
                        "filesystem.open",
                        Errno::ENFILE,
                    )?;
                    refused(
                        fs.open("/kept", OpenOptions::write_only().truncate(true)),
                        "filesystem.open",
                        Errno::ENFILE,
                    )?;
                    ensure!(
                        (file.fstat()?, fs.statvfs()?) == before
                            && file.lseek(Seek::Current(0))? == 0
                    );
                    ensure!(fs.stat("/host-refused").unwrap_err().errno == Errno::ENOENT);
                    ensure!(fs.read_file("/kept")? == b"original");
                    file.close()?;
                    // A closed descriptor frees host admission before its weak
                    // registry slot is compacted or another event ends.
                    fs.open("/freed-slot", OpenOptions::write_only().create_new(true))?
                        .close()?;
                    fs.unlink("/freed-slot")?;
                    let file = fs.open("/kept", OpenOptions::read_only())?;
                    self.0.hold(&context, vec![file], vec![])?;
                    Ok(super::response("host full"))
                }
                "/refused" => {
                    let before = fs.statvfs()?;
                    refused(
                        fs.open("/candidate", OpenOptions::write_only().create_new(true)),
                        "filesystem.open",
                        Errno::ENFILE,
                    )?;
                    ensure!(
                        fs.statvfs()? == before
                            && fs.stat("/candidate").unwrap_err().errno == Errno::ENOENT
                    );
                    Ok(super::response("refused"))
                }
                "/after-retirement" => {
                    let error = self.0.files.lock().unwrap()[0].fstat().unwrap_err();
                    if error.errno == Errno::ESTALE {
                        return Ok(super::response("pending"));
                    }
                    ensure!(
                        error.errno == Errno::EBADF,
                        "holder did not retire: {error:?}"
                    );
                    // Strong references to the old file and directory handles
                    // still exist. Only their owning event has ended.
                    ensure!(
                        self.0.files.lock().unwrap().len() == 3
                            && self.0.directories.lock().unwrap().len() == 1
                    );
                    let file = fs.open("/candidate", OpenOptions::read_write().create_new(true))?;
                    let duplicate = file.dup()?;
                    let dir = fs.opendir("/")?;
                    refused(
                        fs.open("/local-refused", OpenOptions::write_only().create(true)),
                        "filesystem.open",
                        Errno::EMFILE,
                    )?;
                    file.write_all(b"new")?;
                    ensure!(fs.read_file("/candidate")? == b"new");
                    file.close()?;
                    duplicate.close()?;
                    dir.closedir()?;
                    Ok(super::response("admitted"))
                }
                "/independent" => {
                    fs.write_file("/kept", b"independent")?;
                    let escaped = context.storage()?.tx(|tx| {
                        let fs = tx.fs()?;
                        (0..3)
                            .map(|_| fs.open("/kept", OpenOptions::read_only()))
                            .collect::<cellar::sdk::filesystem::Result<Vec<_>>>()
                            .map_err(anyhow::Error::from)
                    })?;
                    for file in &escaped {
                        refused(file.fstat(), "filesystem.fstat", Errno::EBADF)?;
                    }
                    // Transaction expiry must free capacity while these
                    // escaped handles still have strong owners as well.
                    let file = fs.open("/kept", OpenOptions::read_only())?;
                    let duplicate = file.dup()?;
                    let dir = fs.opendir("/")?;
                    ensure!(fs.read_file("/kept")? == b"independent");
                    file.close()?;
                    duplicate.close()?;
                    dir.closedir()?;
                    let mut rolled_back = Vec::new();
                    let rollback: Result<()> = context.storage()?.tx(|tx| {
                        let fs = tx.fs()?;
                        for _ in 0..3 {
                            rolled_back.push(fs.open("/kept", OpenOptions::read_only())?);
                        }
                        bail!("intentional admission rollback")
                    });
                    ensure!(rollback.unwrap_err().to_string() == "intentional admission rollback");
                    for file in &rolled_back {
                        refused(file.fstat(), "filesystem.fstat", Errno::EBADF)?;
                    }
                    let file = fs.open("/kept", OpenOptions::read_only())?;
                    let duplicate = file.dup()?;
                    let dir = fs.opendir("/")?;
                    file.close()?;
                    duplicate.close()?;
                    dir.closedir()?;
                    Ok(super::response("independent"))
                }
                _ => bail!("unknown filesystem admission case {path}"),
            }
        })
    }
}

async fn host<F, Fut>(holdings: Arc<Holdings>, scopes: &[&str], exercise: F) -> Result<()>
where
    F: FnOnce(RuntimeManager) -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let limits = cellar::sdk::CellLimits {
        filesystem: cellar::sdk::filesystem::FilesystemLimits {
            max_handles: 3,
            max_host_handles: 4,
            ..Default::default()
        },
        ..Default::default()
    };
    cellar::asyncrt::services().configure_cell_limits(limits)?;
    let shared = holdings.clone();
    let app = Registry::new(|_, _| Box::pin(async { Ok(super::response("unused")) }))
        .cell("Admission", move |_, _| {
            Ok(Arc::new(Admission(shared.clone())))
        })?
        .filesystem("Admission", Policy::default())?;
    let generation = Generation::build_native(
        1,
        DeploymentGraph::single(super::deployment(
            "filesystem-admission",
            vec!["Admission".into()],
            false,
        )),
        GenerationOptions {
            node: "test".into(),
            region: "test".into(),
        },
        std::collections::HashMap::from([(
            "filesystem-admission".into(),
            Arc::new(app) as Arc<dyn cellar::sdk::Application>,
        )]),
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
    for scope in scopes {
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
        runtime.start_cell((*scope).into(), 1, true).await?;
        runtime.publish_cell(scope, 1)?;
    }
    let (gates, mut requests) = tokio::sync::mpsc::unbounded_channel();
    cellar::host_channels::set_gate_tx(gates);
    let (stop, mut stopping) = tokio::sync::watch::channel(false);
    let adapter = cellar::asyncrt::spawn(async move {
        loop {
            let request = cellar::asyncrt::select_biased! { "filesystem admission proof adapter shutdown";
                _=stopping.changed()=>None,
                request=requests.recv()=>request,
            };
            let Some(request) = request else { break };
            let _ = request.reply.send(Ok(()));
        }
    });
    let result = exercise(runtime.clone()).await;
    holdings.release_all();
    holdings.clear();
    let mut cleanup = Ok(());
    for scope in scopes {
        if let Err(error) = runtime.swap_out_cell(scope, 1).await {
            cleanup = Err(error);
        }
    }
    let _ = stop.send(true);
    adapter.await?;
    result?;
    cleanup
}

async fn reply(runtime: &RuntimeManager, scope: &str, path: &str, wanted: &str) -> Result<()> {
    let reply = super::fetch(runtime, scope, path).await?;
    ensure!(
        reply.status == 200 && reply.body == wanted.as_bytes(),
        "admission {scope}{path}: {} {}",
        reply.status,
        String::from_utf8_lossy(&reply.body)
    );
    Ok(())
}

pub(crate) async fn run() -> Result<()> {
    let first = ExecutionDomain::new(tokio::runtime::Handle::current());
    let second = ExecutionDomain::new(tokio::runtime::Handle::current());
    let independent_domain = &second;
    let holdings = Arc::new(Holdings::default());
    let shared = holdings.clone();
    let result = first
        .run(async {
            host(
                holdings,
                &["Admission:held", "Admission:full", "Admission:candidate"],
                |runtime| async move {
                    reply(&runtime, "Admission:held", "/hold", "holding three").await?;
                    reply(&runtime, "Admission:full", "/host-full", "host full").await?;
                    reply(&runtime, "Admission:candidate", "/refused", "refused").await?;
                    independent_domain
                        .run(host(
                            Arc::new(Holdings::default()),
                            &["Admission:independent"],
                            |independent| async move {
                                reply(
                                    &independent,
                                    "Admission:independent",
                                    "/independent",
                                    "independent",
                                )
                                .await
                            },
                        ))
                        .await??;
                    shared.release_one()?;
                    for _ in 0..100 {
                        let result =
                            super::fetch(&runtime, "Admission:candidate", "/after-retirement")
                                .await?;
                        if result.status == 200 && result.body == b"admitted" {
                            return Ok(());
                        }
                        ensure!(
                            result.status == 200 && result.body == b"pending",
                            "unexpected admission poll: {} {}",
                            result.status,
                            String::from_utf8_lossy(&result.body)
                        );
                        cellar::asyncrt::sleep(Duration::from_millis(10)).await;
                    }
                    bail!("retired filesystem handles did not release host admission")
                },
            )
            .await
        })
        .await;
    second.shutdown().await;
    first.shutdown().await;
    result??;
    Ok(())
}
