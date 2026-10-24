//! Explicit all-runtime qualification; never a dependency of native applications.
#![allow(clippy::disallowed_methods, clippy::disallowed_macros)]
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{rpc, Application, Output, Registry};
use cellar_javascript::{EventKind, JavaScriptRuntime, NodeOptions, NodeRuntime, Options, Source};
use cellar_python::{
    EventKind as PythonEvent, Options as PythonOptions, PythonRuntime, Source as PythonSource,
};
use std::{path::PathBuf, sync::Arc, time::Duration};

mod admission;
mod application;
#[path = "../native_counter.rs"]
mod counter;
mod crash;
mod deadline;
mod fleet;
#[allow(dead_code)]
#[path = "../fixtures/generated/rust/example.counter.v1.rs"]
mod generated;
#[path = "../../../crates/cellar-javascript/tests/support/native.rs"]
mod native;
mod proof;
mod qualify;
mod remote;

#[derive(Clone, Copy)]
enum Suite {
    Local,
    Remote,
    Crash,
    Proof,
    Fence,
    Recruitment,
    Recovery,
    RecoveryProbe,
    Deadline,
    Cancellation,
    All,
}

fn main() -> Result<()> {
    let mut arguments = std::env::args().skip(1);
    let mut report_path = None;
    let mut suite = Suite::Local;
    let mut server_name = None;
    let mut server_data = None;
    let mut server_bucket = None;
    let mut server_ready = None;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--report" => {
                report_path = Some(PathBuf::from(
                    arguments.next().context("--report needs a path")?,
                ))
            }
            "--suite" => {
                suite = match arguments.next().as_deref() {
                    Some("local") => Suite::Local,
                    Some("remote") => Suite::Remote,
                    Some("crash") => Suite::Crash,
                    Some("proof") => Suite::Proof,
                    Some("fence") => Suite::Fence,
                    Some("proof-recruitment") => Suite::Recruitment,
                    Some("recovery") => Suite::Recovery,
                    Some("recovery-probe") => Suite::RecoveryProbe,
                    Some("deadline") => Suite::Deadline,
                    Some("cancellation") => Suite::Cancellation,
                    Some("all") => Suite::All,
                    _ => anyhow::bail!("--suite must be local, remote, deadline, cancellation, crash, proof, fence, proof-recruitment, recovery, recovery-probe or all"),
                }
            }
            "--serve-name" => server_name = Some(arguments.next().context("missing server node name")?),
            "--serve-data" => server_data = Some(PathBuf::from(arguments.next().context("missing server data path")?)),
            "--serve-bucket" => server_bucket = Some(PathBuf::from(arguments.next().context("missing server bucket path")?)),
            "--serve-ready" => server_ready = Some(PathBuf::from(arguments.next().context("missing server readiness path")?)),
            _ => anyhow::bail!(
                "usage: cellar-rpc-qualification [--suite local|remote|deadline|cancellation|crash|proof|fence|proof-recruitment|recovery|recovery-probe|all] [--report path.json]"
            ),
        }
    }
    let server = if let Some(name) = server_name {
        ensure!(
            report_path.is_none(),
            "crash owner does not publish a success report"
        );
        Some(crash::Server {
            name,
            data: server_data.context("server data path is required")?,
            bucket: server_bucket.context("server bucket path is required")?,
            ready: server_ready.context("server readiness path is required")?,
        })
    } else {
        ensure!(
            server_data.is_none() && server_bucket.is_none() && server_ready.is_none(),
            "server options require --serve-name"
        );
        None
    };
    if let Some(path) = &report_path {
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    let (proof_sender, proof_receiver) = std::sync::mpsc::sync_channel(1);
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()?;
    let contract = Arc::new(rpc::Contract::from_descriptor(
        include_bytes!("../fixtures/counter.binpb"),
        rpc::Limits::default(),
    )?);
    let mut python_options = PythonOptions::default();
    python_options.module_paths.push(
        root.join("tools/rpc/fixtures/generated/python")
            .canonicalize()?,
    );
    python_options
        .module_paths
        .push(root.join("tools/rpc").canonicalize()?);
    let site = std::env::var_os("CELLAR_RPC_PYTHON_SITE")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join(".cellar/rpc/python/lib/python3.14/site-packages"));
    python_options.module_paths.push(site.canonicalize()?);
    let python_source = PythonSource::new(include_str!("../python.py"), "rpc/python.py")
        .events([PythonEvent::Fetch])
        .cell_rpc(
            "PythonCounter",
            "example.counter.v1.Counter",
            contract.clone(),
        )?
        .require_distribution("protobuf", "7.34.2");
    let code = std::fs::read_to_string(root.join("tools/rpc/dist/app.v8.js"))
        .context("build fixtures with npm run build --prefix tools/rpc")?;
    let source = Source::new(code, "rpc/app.ts")
        .events([EventKind::Fetch])
        .cell_rpc("V8Counter", "example.counter.v1.Counter", contract.clone())?;
    let node_source = Source::module().events([EventKind::Fetch]).cell_rpc(
        "NodeCounter",
        "example.counter.v1.Counter",
        contract.clone(),
    )?;
    let node_options = NodeOptions::new(root.join("tools/rpc/dist/node/node.mjs").canonicalize()?);
    let options = Options {
        turn_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    // SAFETY: this executable owns the process main thread and CPython. All
    // Rust workers and JS runtimes start inside its supervised host callback.
    let report = unsafe {
        PythonRuntime::run(python_options, python_source, move |python, _stop| {
            JavaScriptRuntime::run(options.clone(), source, |v8| {
                NodeRuntime::run(options, node_options, node_source, |node| {
                    let crash = Arc::new(crash::Gate::default());
                    let transaction_checkpoint = crash.clone();
                    let live = Arc::new(proof::Live::default());
                    let live_checkpoint = live.clone();
                    let rust = Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
                        .cell("RustCounter", move |_, _| {
                            Ok(Arc::new(generated::CounterRegistration::register(
                                rpc::Router::new(),
                                Arc::new(crash::Counter {
                                    crash: transaction_checkpoint.clone(),
                                    live: live_checkpoint.clone(),
                                }),
                            )?))
                        })?
                        .rpc_service(
                            "RustCounter",
                            "example.counter.v1.Counter",
                            contract.clone(),
                        )?;
                    let applications: Vec<(&str, &str, Arc<dyn Application>)> = vec![
                        ("rust", "RustCounter", Arc::new(rust)),
                        ("python", "PythonCounter", python),
                        ("v8", "V8Counter", v8),
                        ("node", "NodeCounter", node),
                    ];
                    let app = Arc::new(application::Composite::new(
                        applications,
                        contract,
                        crash,
                        live,
                    )?);
                    let proof = tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(4)
                        .enable_all()
                        .build()?
                        .block_on(async move {
                            if let Some(server) = server { return crash::serve(app, server).await; }
                            match suite {
                                Suite::Local => qualify::local(app).await,
                                Suite::Remote => remote::run(app).await,
                                Suite::Crash => crash::run(app).await,
                                Suite::Proof => proof::run(app).await,
                                Suite::Fence => proof::fences(app).await,
                                Suite::Recruitment => proof::recruitment(app).await,
                                Suite::Recovery => admission::run(app).await,
                                Suite::RecoveryProbe => admission::probe(app).await,
                                Suite::Deadline => deadline::run(app).await,
                                Suite::Cancellation => deadline::cancellation::run(app).await,
                                Suite::All => {
                                    let local = qualify::local(app.clone()).await?;
                                    let remote = remote::run(app.clone()).await?;
                                    let deadline = deadline::run(app.clone()).await?;
                                    let cancellation = deadline::cancellation::run(app.clone()).await?;
                                    let recovery = admission::run(app.clone()).await?;
                                    let crash = crash::run(app.clone()).await?;
                                    let proof = proof::run(app).await?;
                                    Ok(serde_json::json!({"format":1,"suite":"all","local":local,"remote":remote,"deadline":deadline,"cancellation":cancellation,"recovery":recovery,"crash":crash,"proof":proof}))
                                }
                            }
                        })?;
                    proof_sender
                        .send(proof)
                        .context("qualification result receiver closed")?;
                    Ok(())
                })
            })
        })?
    };
    ensure!(
        report.complete(),
        "Python runtime did not fully drain after RPC qualification"
    );
    // Publish evidence only after every runtime has joined and drained. A failed
    // rerun cannot leave an older success report at the requested path.
    let proof = proof_receiver
        .try_recv()
        .context("qualification result is missing")?;
    let text = serde_json::to_string_pretty(&proof)?;
    if let Some(path) = report_path {
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, format!("{text}\n"))?;
    }
    println!("{text}");
    Ok(())
}
