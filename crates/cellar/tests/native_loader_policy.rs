//! Caller-owned loader budgets on real compiled applications and worker pools.
#![allow(clippy::disallowed_methods)]
#[path = "support/html.rs"]
mod fixture;
use anyhow::{ensure, Result};
use cellar::application::{
    facets::Startup,
    loader::{Worker, WorkerCode},
    service::Fetcher,
};
use cellar::engine_api::{HttpResponse, RuntimeFetch};
use cellar::generation::Generation;
use cellar::native::clone::{decode, Object, StructuredValue, Value};
use cellar::runtime::RuntimeManager;
use cellar::sdk::{
    Application, Cell, Context, ExecutionDomain, HandlerFuture, Invocation, LoaderPolicy, Output,
    Registry, Request, RuntimePolicy,
};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

fn output(body: impl ToString) -> Output {
    Output::Http(HttpResponse {
        status: 200,
        body: body.to_string().into_bytes(),
        headers: Vec::new(),
        stream: None,
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}

fn request() -> Request {
    Request {
        url: "http://loaded/read".into(),
        method: "GET".into(),
        body: Vec::new().into(),
        headers: Vec::new(),
        entrypoint: None,
    }
}

fn bytes() -> StructuredValue {
    StructuredValue {
        root: Value::Reference(0),
        objects: vec![Object::ArrayBuffer {
            bytes: vec![7; 128],
            max_length: None,
            immutable: false,
        }],
    }
}

fn props_object() -> StructuredValue {
    StructuredValue {
        root: Value::Reference(0),
        objects: vec![
            Object::Object(vec![(Value::from("bytes"), Value::Reference(1))]),
            Object::ArrayBuffer {
                bytes: vec![7; 128],
                max_length: None,
                immutable: false,
            },
        ],
    }
}

fn payload_len(graph: &StructuredValue) -> usize {
    graph
        .objects
        .iter()
        .find_map(|o| match o {
            Object::ArrayBuffer { bytes, .. } => Some(bytes.len()),
            _ => None,
        })
        .unwrap_or(0)
}

struct Leaf(usize);

impl Cell for Leaf {
    fn invoke(self: Arc<Self>, _: Context, _: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move { Ok(output(self.0)) })
    }
}

fn child() -> Result<Arc<dyn Application>> {
    Ok(Arc::new(
        Registry::new(|ctx, event| {
            Box::pin(async move {
                let Invocation::Fetch(req) = event else {
                    return Ok(Output::Done);
                };
                let size = if let Some(entry) = req.entrypoint {
                    payload_len(&decode(&entry.props)?)
                } else {
                    ctx.loaded_environment()?.map_or(0, payload_len)
                };
                Ok(output(size))
            })
        })
        .cell("Leaf", |ctx, _| {
            Ok(Arc::new(Leaf(payload_len(&ctx.facet().unwrap().props))))
        })?,
    ))
}

#[derive(Default)]
struct Mailbox {
    own: Mutex<Option<Fetcher>>,
    foreign: Mutex<Option<Fetcher>>,
}

struct Parent {
    workers: Mutex<Vec<Worker>>,
    mailbox: Arc<Mailbox>,
}

impl Cell for Parent {
    fn invoke(self: Arc<Self>, ctx: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(req) = event else {
                return Ok(Output::Done);
            };
            let operation = req.url.rsplit('/').next().unwrap();
            match operation {
                "load" => {
                    let worker = ctx
                        .loader("WORKERS")?
                        .load(&ctx, WorkerCode::new(child()?))?;
                    match worker.wait_ready().await {
                        Ok(()) => {
                            self.workers.lock().unwrap().push(worker);
                            Ok(output("loaded"))
                        }
                        Err(e) => Ok(output(format!("{e}"))),
                    }
                }
                "dispose" => {
                    let worker = self.workers.lock().unwrap().pop().unwrap();
                    worker.dispose(&ctx)?;
                    Ok(output("disposed"))
                }
                "environment" => {
                    let mut code = WorkerCode::new(child()?);
                    code.environment = Some(bytes());
                    let worker = ctx.loader("WORKERS")?.load(&ctx, code)?;
                    match worker.wait_ready().await {
                        Ok(()) => {
                            let response =
                                worker.entrypoint("default").fetch(&ctx, request()).await?;
                            worker.dispose(&ctx)?;
                            Ok(Output::Http(response))
                        }
                        Err(e) => Ok(output(format!("{e}"))),
                    }
                }
                "route-budget" => {
                    let before = cellar::host_loader::loader_registry().lock().unwrap().len();
                    let mut code = WorkerCode::new(child()?);
                    code.environment_fetcher(ctx.exported_entrypoint("x".repeat(400))?)?;
                    let worker = ctx.loader("WORKERS")?.load(&ctx, code)?;
                    let error = worker.wait_ready().await.err().unwrap();
                    ensure!(error.to_string().contains("environment exceeds size limit"));
                    assert_eq!(
                        cellar::host_loader::loader_registry().lock().unwrap().len(),
                        before
                    );
                    Ok(output("routes bounded"))
                }
                "properties" => {
                    *self.mailbox.own.lock().unwrap() = Some(ctx.exported_entrypoint("default")?);
                    let worker = ctx
                        .loader("WORKERS")?
                        .load(&ctx, WorkerCode::new(child()?))?;
                    worker.wait_ready().await?;
                    let entry = worker.entrypoint("default").with_props(&bytes());
                    let loaded_class = worker.cell_class("Leaf").with_props(&bytes());
                    let local_class = ctx.exported_cell_class("Leaf")?.with_props(&props_object());
                    let service = ctx.exported_entrypoint("default")?.with_props(&bytes());
                    if let Err(error) = entry {
                        ensure!(error.to_string().contains("size limit"));
                        ensure!(loaded_class.is_err() && local_class.is_err() && service.is_err());
                        worker.dispose(&ctx)?;
                        return Ok(output("properties bounded"));
                    }
                    ensure!(service.is_ok());
                    let response = entry?.fetch(&ctx, request()).await?;
                    assert_eq!(response.body, b"128");
                    let loaded_class = loaded_class?;
                    let loaded = ctx.facets()?.get(&ctx, "loaded", move |_| {
                        Box::pin(async move { Ok(Startup::new(loaded_class)) })
                    })?;
                    assert_eq!(loaded.fetch(&ctx, request()).await?.body, b"128");
                    let local_class = local_class?;
                    let local = ctx.facets()?.get(&ctx, "local", move |_| {
                        Box::pin(async move { Ok(Startup::new(local_class)) })
                    })?;
                    assert_eq!(local.fetch(&ctx, request()).await?.body, b"128");
                    worker.dispose(&ctx)?;
                    Ok(output("properties copied"))
                }
                "foreign" => {
                    let foreign = self.mailbox.foreign.lock().unwrap().clone().unwrap();
                    let fetch_error = foreign.fetch(&ctx, request()).await.err().unwrap();
                    ensure!(fetch_error
                        .to_string()
                        .contains("another or closed execution domain"));
                    let rpc_error = foreign
                        .rpc(
                            &ctx,
                            cellar::WorkerRpcOperation::Get {
                                path: vec!["value".into()],
                            },
                        )
                        .await
                        .err()
                        .unwrap();
                    ensure!(rpc_error
                        .to_string()
                        .contains("another or closed execution domain"));
                    let before = cellar::host_loader::loader_registry().lock().unwrap().len();
                    for broker in [false, true] {
                        let mut code = WorkerCode::new(child()?);
                        if broker {
                            code.outbound =
                                cellar::application::loader::Outbound::Broker(foreign.clone());
                        } else {
                            code.environment_fetcher(foreign.clone())?;
                        }
                        let worker = ctx.loader("WORKERS")?.load(&ctx, code)?;
                        let error = worker.wait_ready().await.err().unwrap();
                        ensure!(error
                            .to_string()
                            .contains("another or closed execution domain"));
                        assert_eq!(
                            cellar::host_loader::loader_registry().lock().unwrap().len(),
                            before
                        );
                    }
                    Ok(output("foreign refused"))
                }
                _ => anyhow::bail!("unknown operation"),
            }
        })
    }
}

fn parent(mailbox: Arc<Mailbox>, class: &str) -> Result<Arc<dyn Application>> {
    Ok(Arc::new(
        Registry::new(|_, _| Box::pin(async { Ok(Output::Done) }))
            .cell(class, move |_, _| {
                Ok(Arc::new(Parent {
                    workers: Mutex::new(Vec::new()),
                    mailbox: mailbox.clone(),
                }))
            })?
            .cell("Leaf", |ctx, _| {
                Ok(Arc::new(Leaf(payload_len(&ctx.facet().unwrap().props))))
            })?,
    ))
}

struct Host {
    domain: ExecutionDomain,
    runtime: Arc<RuntimeManager>,
    mailbox: Arc<Mailbox>,
    gates: Arc<AtomicUsize>,
    _directory: tempfile::TempDir,
}

async fn host(policy: LoaderPolicy) -> Result<Host> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    domain.services().configure_loader(policy)?;
    domain.services().configure_runtime(RuntimePolicy {
        max_cells_per_worker: 1,
        ..Default::default()
    })?;
    let directory = tempfile::tempdir()?;
    let mailbox = Arc::new(Mailbox::default());
    let gates = Arc::new(AtomicUsize::new(0));
    let runtime = domain
        .run(async {
            // This runtime-only fixture isolates loader budgets rather than
            // actor/fleet proof. Like native_loader/native_cells, it must serve
            // the host gate contract now that facet registration writes parent
            // storage. Real Node source/facet tests retain production gates.
            let (send, mut requests) = tokio::sync::mpsc::unbounded_channel();
            cellar::host_channels::set_gate_tx(send);
            let observed = gates.clone();
            cellar::asyncrt::spawn(async move {
                while let Some(gate) = requests.recv().await {
                    observed.fetch_add(1, Ordering::SeqCst);
                    let _ = gate.reply.send(Ok(()));
                }
            })
            .detach();
            let mut graph = fixture::graph("main", "", vec!["Root".into()]);
            graph.primary.loader_bindings = vec!["WORKERS".into()];
            let mut other = fixture::graph("other", "", vec!["OtherRoot".into()]).primary;
            other.loader_bindings = vec!["WORKERS".into()];
            graph.cohosted.push(other);
            let generation = Generation::build_native(
                1,
                graph,
                fixture::generation_options(),
                HashMap::from([
                    ("main".into(), parent(mailbox.clone(), "Root")?),
                    ("other".into(), parent(mailbox.clone(), "OtherRoot")?),
                ]),
            )?;
            let runtime = Arc::new(RuntimeManager::start(
                generation,
                fixture::runtime_options(directory.path().into()),
            )?);
            for scope in [
                "Root:shared",
                "Root:second",
                "OtherRoot:first",
                "OtherRoot:second",
            ] {
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
            Ok::<_, anyhow::Error>(runtime)
        })
        .await??;
    domain.services().configure_loader(policy)?;
    ensure!(domain
        .services()
        .configure_loader(LoaderPolicy::default())
        .is_err());
    Ok(Host {
        domain,
        runtime,
        mailbox,
        gates,
        _directory: directory,
    })
}

async fn call(host: &Host, cell: &str, op: &str) -> Result<String> {
    let response = tokio::time::timeout(
        Duration::from_secs(10),
        host.domain.run(host.runtime.fetch_cell(
            cell.into(),
            None,
            RuntimeFetch {
                url: format!("http://policy/{op}"),
                method: "POST".into(),
                headers: Vec::new(),
                body: Vec::new().into(),
                request_id: None,
                order: None,
                parent: None,
            },
            None,
        )),
    )
    .await???;
    ensure!(
        response.status == 200,
        "unexpected host response: {}",
        response.status
    );
    Ok(String::from_utf8(response.body)?)
}

fn policy(workers: usize, principal: usize, bytes: usize) -> LoaderPolicy {
    LoaderPolicy {
        max_workers: workers,
        max_workers_per_principal: principal,
        max_environment_bytes: bytes,
        max_props_bytes: bytes,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hosts_share_principal_budgets_without_sharing_fleet_loader_capacity() -> Result<()> {
    let a = host(policy(3, 2, 256)).await?;
    let b = host(policy(6, 4, 256)).await?;
    for _ in 0..2 {
        assert_eq!(call(&a, "Root:shared", "load").await?, "loaded");
    }
    ensure!(call(&a, "Root:second", "load")
        .await?
        .contains("for script"));
    for _ in 0..4 {
        assert_eq!(call(&b, "Root:shared", "load").await?, "loaded");
    }
    ensure!(call(&b, "Root:second", "load")
        .await?
        .contains("for script"));
    assert_eq!(call(&a, "OtherRoot:first", "load").await?, "loaded");
    ensure!(call(&a, "OtherRoot:second", "load")
        .await?
        .contains("too many loaded workers (limit 3)"));
    for _ in 0..2 {
        assert_eq!(call(&b, "OtherRoot:first", "load").await?, "loaded");
    }
    ensure!(call(&b, "OtherRoot:second", "load")
        .await?
        .contains("too many loaded workers (limit 6)"));
    assert_eq!(call(&a, "Root:shared", "dispose").await?, "disposed");
    assert_eq!(call(&a, "Root:second", "load").await?, "loaded");
    a.domain.shutdown().await;
    assert_eq!(call(&b, "Root:shared", "dispose").await?, "disposed");
    assert_eq!(call(&b, "Root:second", "load").await?, "loaded");
    b.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn originating_hosts_bound_environments_routes_and_derived_properties() -> Result<()> {
    let a = host(policy(4, 4, 64)).await?;
    let b = host(policy(4, 4, 256)).await?;
    ensure!(call(&a, "Root:shared", "environment")
        .await?
        .contains("environment exceeds size limit"));
    assert_eq!(call(&b, "Root:shared", "environment").await?, "128");
    for h in [&a, &b] {
        assert_eq!(
            call(h, "Root:shared", "route-budget").await?,
            "routes bounded"
        );
    }
    assert_eq!(
        call(&a, "Root:shared", "properties").await?,
        "properties bounded"
    );
    assert_eq!(
        call(&b, "Root:shared", "properties").await?,
        "properties copied"
    );
    ensure!(
        b.gates.load(Ordering::SeqCst) > 0,
        "parent facet registration did not reach its host gate"
    );
    let small = a.mailbox.own.lock().unwrap().clone().unwrap();
    let large = b.mailbox.own.lock().unwrap().clone().unwrap();
    // Pure derivation can happen elsewhere, but it cannot borrow that scope's
    // larger budget. Actual work must use the capability's original host.
    assert!(b
        .domain
        .run(async { small.with_props(&bytes()) })
        .await?
        .is_err());
    assert!(a
        .domain
        .run(async { large.with_props(&bytes()) })
        .await?
        .is_ok());
    *a.mailbox.foreign.lock().unwrap() = Some(large.clone());
    *b.mailbox.foreign.lock().unwrap() = Some(small);
    assert_eq!(call(&a, "Root:shared", "foreign").await?, "foreign refused");
    assert_eq!(call(&b, "Root:shared", "foreign").await?, "foreign refused");
    a.domain.shutdown().await;
    assert!(a
        .domain
        .services()
        .configure_loader(policy(4, 4, 64))
        .is_err());
    assert_eq!(call(&b, "Root:shared", "environment").await?, "128");
    b.domain.shutdown().await;
    Ok(())
}

#[test]
fn explicit_loader_policy_survives_invalid_environment() -> Result<()> {
    let mut child = std::process::Command::new(std::env::current_exe()?);
    child.args([
        "--exact",
        "originating_hosts_bound_environments_routes_and_derived_properties",
        "--nocapture",
    ]);
    for name in [
        "CELLAR_LOADER_MAX_WORKERS",
        "CELLAR_LOADER_MAX_WORKERS_PER_PRINCIPAL",
        "CELLAR_LOADER_MAX_ENV_BYTES",
        "CELLAR_LOADER_MAX_PROPS_BYTES",
        "CELLAR_MAX_DYNAMIC_WORKER_CODE_BYTES",
        "CELLAR_MAX_LOADED_WORKERS",
    ] {
        child.env(name, "unusable");
    }
    let output = child.output()?;
    ensure!(
        output.status.success(),
        "explicit loader policy consulted environment:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[tokio::test]
async fn invalid_policies_do_not_pin_defaults_and_live_policies_are_immutable() -> Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let defaults = LoaderPolicy::default();
    let ceiling = cellar::native::clone::Limits::default().bytes;
    for invalid in [
        LoaderPolicy {
            max_workers: 0,
            ..defaults
        },
        LoaderPolicy {
            max_workers_per_principal: 0,
            ..defaults
        },
        LoaderPolicy {
            max_workers_per_principal: defaults.max_workers + 1,
            ..defaults
        },
        LoaderPolicy {
            max_environment_bytes: 0,
            ..defaults
        },
        LoaderPolicy {
            max_environment_bytes: ceiling + 1,
            ..defaults
        },
        LoaderPolicy {
            max_props_bytes: 0,
            ..defaults
        },
        LoaderPolicy {
            max_props_bytes: ceiling + 1,
            ..defaults
        },
    ] {
        ensure!(domain.services().configure_loader(invalid).is_err());
    }
    let custom = policy(1, 1, 64);
    domain.services().configure_loader(custom)?;
    assert_eq!(domain.services().loader_policy(), custom);
    domain.services().configure_loader(custom)?;
    ensure!(domain.services().configure_loader(defaults).is_err());
    domain.shutdown().await;
    ensure!(domain.services().configure_loader(custom).is_err());
    let fresh = ExecutionDomain::new(tokio::runtime::Handle::current());
    assert_eq!(fresh.services().loader_policy(), defaults);
    ensure!(fresh.services().configure_loader(custom).is_err());
    fresh.shutdown().await;
    Ok(())
}

// Process environment is changed only inside this exact-test child.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn command_line_loader_policy_validates_and_is_captured_by_workers() -> Result<()> {
    const CHILD: &str = "CELLAR_LOADER_POLICY_ADAPTER_CHILD";
    if std::env::var(CHILD).as_deref() == Ok("1") {
        let captured = LoaderPolicy::from_env()?;
        assert_eq!(captured, policy(3, 2, 256));
        let h = host(captured).await?;
        for (name, invalid) in [
            ("CELLAR_LOADER_MAX_WORKERS", "0"),
            ("CELLAR_LOADER_MAX_WORKERS_PER_PRINCIPAL", "4"),
            ("CELLAR_LOADER_MAX_ENV_BYTES", "unusable"),
            ("CELLAR_LOADER_MAX_PROPS_BYTES", "67108865"),
        ] {
            let previous = std::env::var(name)?;
            std::env::set_var(name, invalid);
            ensure!(
                LoaderPolicy::from_env().is_err(),
                "accepted {name}={invalid}"
            );
            assert_eq!(h.domain.services().loader_policy(), captured);
            assert_eq!(call(&h, "Root:shared", "environment").await?, "128");
            std::env::set_var(name, previous);
        }
        assert_eq!(call(&h, "Root:shared", "load").await?, "loaded");
        assert_eq!(call(&h, "Root:shared", "load").await?, "loaded");
        ensure!(call(&h, "Root:second", "load")
            .await?
            .contains("for script"));
        h.domain.shutdown().await;
        std::env::set_var("CELLAR_LOADER_MAX_WORKERS", "1");
        std::env::remove_var("CELLAR_LOADER_MAX_WORKERS_PER_PRINCIPAL");
        assert_eq!(LoaderPolicy::from_env()?, policy(1, 1, 256));
        return Ok(());
    }
    let output = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "command_line_loader_policy_validates_and_is_captured_by_workers",
            "--nocapture",
        ])
        .env_clear()
        .env(CHILD, "1")
        .env("CELLAR_LOADER_MAX_WORKERS", "3")
        .env("CELLAR_LOADER_MAX_WORKERS_PER_PRINCIPAL", "2")
        .env("CELLAR_LOADER_MAX_ENV_BYTES", "256")
        .env("CELLAR_LOADER_MAX_PROPS_BYTES", "256")
        .output()?;
    ensure!(
        output.status.success(),
        "CLI loader adapter failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[test]
fn cli_exposes_compiled_loader_budgets_and_rejects_retired_module_byte_setting() -> Result<()> {
    let binary = env!("CARGO_BIN_EXE_cellar");
    let help = std::process::Command::new(binary)
        .args(["diagnose", "--help"])
        .env_clear()
        .output()?;
    ensure!(help.status.success());
    let text = String::from_utf8(help.stdout)?;
    for name in [
        "CELLAR_LOADER_MAX_WORKERS",
        "CELLAR_LOADER_MAX_WORKERS_PER_PRINCIPAL",
        "CELLAR_LOADER_MAX_ENV_BYTES",
        "CELLAR_LOADER_MAX_PROPS_BYTES",
    ] {
        ensure!(text.contains(name), "missing {name} from help");
    }
    ensure!(!text.contains("CELLAR_MAX_DYNAMIC_WORKER_CODE_BYTES"));
    let stale = std::process::Command::new(binary)
        .args(["diagnose", "--version"])
        .env_clear()
        .env("CELLAR_MAX_DYNAMIC_WORKER_CODE_BYTES", "67108864")
        .output()?;
    ensure!(!stale.status.success());
    let error = String::from_utf8(stale.stderr)?;
    ensure!(error.contains("CELLAR_MAX_DYNAMIC_WORKER_CODE_BYTES"));
    ensure!(error.contains("Cellar loads compiled Rust applications"));
    Ok(())
}
