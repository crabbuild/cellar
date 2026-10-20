//! Real-node adapter contract, on the process main thread (one interpreter).
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Result};
use cellar::sdk::*;
use cellar_python::{EventKind, Options, PythonApplication, PythonRuntime, Source};
use serde_json::{json, Value};
use std::{path::Path, sync::Arc, time::Duration};

#[path = "../../cellar/tests/support/html.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "support/watchdog.rs"]
mod watchdog;

async fn start(path: &Path, application: Arc<PythonApplication>) -> Result<(Node, String)> {
    start_with(path, application, "python-test", None).await
}

async fn start_with(
    path: &Path,
    application: Arc<PythonApplication>,
    name: &str,
    bucket: Option<cellar::bucket::Bucket>,
) -> Result<(Node, String)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let internal_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let internal = internal_listener.local_addr()?;
    let signer = Arc::new(PeerProbeSigner::generate());
    let actor = ActorOptions {
        core: ActorPolicy {
            max_resident: 16,
            max_cached_routes: 16,
            max_activations: 8,
            max_evictions: 4,
            max_releases: 4,
            max_outbound_websockets: 8,
            ownership_on_evict: OwnershipOnEvict::Sticky,
            require_node_lease: true,
            peer_protocol: cellar::peer_auth::PROTOCOL_VERSION,
            operation_deadline_ms: Some(5_000),
            owner_log_recovery_backoff_ms: 50,
            owner_log_recovery_attempts: 3,
            alarm_resident_ms: 100,
            idle_evict_ms: None,
            pressure: Default::default(),
        },
        lease_ttl_ms: 1_000,
        placement_weight: 1,
        generation: signer.public_key_hex(),
        resume_generation: None,
    };
    let mut config = NodeConfig::new(
        ActorIdentity {
            node: name.into(),
            region: "test".into(),
            advertise: internal.to_string(),
        },
        actor,
        path.to_path_buf(),
    );
    config.maintenance.rebalance = false;
    config.runtime.handler_budget = Duration::from_millis(250);
    let executor = tokio::runtime::Handle::current();
    let peer_http = reqwest::Client::new();
    let peer_auth = Arc::new(PeerAuth::new([31; 32], name)?);
    let fleet = bucket
        .map(|bucket| -> Result<NodeFleet> {
            Ok(NodeFleet {
                replica_storage: ReplicaStorage::new(
                    bucket.store.clone(),
                    ReplicaStorageOptions {
                        prefix: bucket.prefix.clone(),
                        ..Default::default()
                    },
                )?,
                lease_bucket: bucket.clone(),
                bucket,
                replication: ReplicationOptions {
                    flush_interval: Duration::ZERO,
                    durability_timeout: Duration::from_secs(2),
                    compaction: None,
                    ..Default::default()
                },
                log_transport: Arc::new(SignedPeerTransport::from_clients(
                    peer_http.clone(),
                    peer_http.clone(),
                    peer_auth.clone(),
                )),
                log: LogOptions {
                    record_publish_timeout: Duration::from_secs(3),
                    ..Default::default()
                },
                fleet_durability: false,
            })
        })
        .transpose()?;
    let services = NodeServices {
        wall_clock: None,
        http_client: None,
        host_executor: executor.clone(),
        core_executor: executor,
        listener,
        internal_listener,
        peer_http,
        peer_auth,
        probe_signer: signer,
        fleet,
        containers: None,
        metrics: None,
    };
    let mut graph = fixture::graph("python-test", "", vec!["Counter".into()]);
    graph.primary.options.bindings = vec![("COUNTERS".into(), "Counter".into())];
    graph.primary.options.vars = vec![("DOMAIN_MARKER".into(), name.into())];
    let applications =
        CompiledApplications::default()
            .register("python-test", "test", move || Ok(application.clone()))?;
    Ok((
        Node::start(config, services, graph, applications).await?,
        format!("http://{address}"),
    ))
}

async fn stop(node: Node, monitor: &cellar_python::Stop) -> Result<()> {
    stop_mode(node, ShutdownMode::Preserve, monitor).await
}

async fn stop_mode(node: Node, mode: ShutdownMode, monitor: &cellar_python::Stop) -> Result<()> {
    let outcome = node.shutdown(mode).await?;
    monitor.record_node(outcome.clone());
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("node failed while stopping")
    };
    ensure!(
        report.ingress_drained
            && report.application_work_drained
            && report.cells_drained
            && !report.drain_timed_out,
        "incomplete shutdown: {report:?}"
    );
    Ok(())
}

fn main() -> Result<()> {
    let mode = std::env::args().nth(1).unwrap_or_default();
    if mode.starts_with("--owner-") {
        return watchdog::child(
            &mode,
            Path::new(&std::env::args().nth(2).expect("private owner directory")),
        );
    }
    if mode == "--runtime-fatal" {
        let directory = tempfile::tempdir()?;
        let error = run(
            Options::default(),
            Source::new(include_str!("bridge.py"), "bridge.py")
                .events([EventKind::Fetch])
                .cell_events("Counter", [EventKind::Fetch, EventKind::Alarm]),
            move |application, stop| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()?;
                runtime.block_on(async move {
                    let monitor = stop.clone();
                    let (node, url) = start(directory.path(), application).await?;
                    let client = reqwest::Client::builder()
                        .timeout(Duration::from_secs(5))
                        .build()?;
                    let (outcome, _) = futures_util::future::join(
                        node.run_until(async move {
                            stop.cancelled().await;
                            ShutdownMode::Preserve
                        }),
                        client.get(format!("{url}/fatal")).send(),
                    )
                    .await;
                    let outcome = outcome?;
                    monitor.record_node(outcome.clone());
                    ensure!(
                        matches!(outcome, NodeOutcome::Stopped(_)),
                        "host did not stop after Python failure"
                    );
                    Ok(())
                })
            },
        )
        .expect_err("background interpreter failure must reach the supervisor");
        ensure!(
            error
                .downcast_ref::<cellar_python::Incomplete>()
                .is_some_and(|failure| failure.report.runtime_failed)
                && !error.to_string().contains("controlled background exit"),
            "unexpected fatal error: {error}"
        );
        return Ok(());
    }
    if mode == "--runtime-stubborn" {
        let directory = tempfile::tempdir()?;
        let error = run(
            Options {
                shutdown_timeout: Duration::from_millis(100),
                ..Default::default()
            },
            Source::new(include_str!("bridge.py"), "bridge.py")
                .events([EventKind::Fetch])
                .cell_events("Counter", [EventKind::Fetch, EventKind::Alarm]),
            move |application, monitor| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(2)
                    .enable_all()
                    .build()?;
                runtime.block_on(async move {
                    let (node, url) = start(directory.path(), application).await?;
                    reqwest::get(format!("{url}/stubborn"))
                        .await?
                        .error_for_status()?;
                    stop(node, &monitor).await
                })
            },
        )
        .expect_err("a cancellation-suppressing task must fail shutdown");
        ensure!(
            error
                .to_string()
                .contains("Python shutdown incomplete: 1 tasks"),
            "wrong shutdown error: {error}"
        );
        return Ok(());
    }
    if mode.starts_with("--runtime-") {
        let mut source = Source::new("x=1", "lifecycle.py");
        if mode == "--runtime-dependency" {
            source = source.require_distribution("cellar-missing-test-package", "0");
        }
        let error = run(Options::default(), source, move |_, _| {
            if mode == "--runtime-panic" {
                panic!("controlled supervisor panic");
            }
            anyhow::bail!("controlled supervisor failure")
        })
        .expect_err("lifecycle failure must propagate");
        ensure!(!error.to_string().is_empty());
        return Ok(());
    }
    for mode in [
        "--runtime-dependency",
        "--runtime-failure",
        "--runtime-panic",
        "--runtime-stubborn",
        "--runtime-fatal",
    ] {
        lifecycle_mode(mode)?;
    }
    watchdog::qualify()?;
    let directory = tempfile::tempdir()?;
    let report = run(
        Options {
            max_body_bytes: 4096,
            max_value_bytes: 16384,
            max_operations: 64,
            max_tasks: 129,
            ..Default::default()
        },
        Source::new(include_str!("bridge.py"), "bridge.py")
            .events([EventKind::Fetch])
            .cell_events("Counter", [EventKind::Fetch, EventKind::Alarm]),
        move |application, monitor| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(10))
                    .build()?;
                let (node, url) = start(directory.path(), application.clone()).await?;
                let (other, other_url) = start_with(
                    &directory.path().join("scope-other"),
                    application.clone(),
                    "scope-other",
                    None,
                )
                .await?;
                let scoped = context_isolation(&client, &url, &other_url).await;
                stop(other, &monitor).await?;
                scoped?;
                host_await_progress(&client, &url).await?;
                let call = |path: &str| client.get(format!("{url}{path}"));
                ensure!(
                    call("/threadpool")
                        .send()
                        .await?
                        .json::<String>()
                        .await?
                        .len()
                        == 64
                );
                ensure!(call("/library").send().await?.json::<String>().await?.len() == 64);
                ensure!(call("/codec").send().await?.json::<bool>().await?);
                ensure!(call("/task-budget").send().await?.json::<bool>().await?);
                let repeated = call("/headers")
                    .header("x-repeated", "one")
                    .header("x-repeated", "two")
                    .send()
                    .await?;
                ensure!(repeated.status().is_success());
                let repeated: Vec<_> = repeated
                    .headers()
                    .get_all("x-repeated")
                    .iter()
                    .map(|value| value.to_str())
                    .collect::<std::result::Result<_, _>>()?;
                ensure!(repeated == ["first", "second"]);
                ensure!(call("/header-limit")
                    .send()
                    .await?
                    .status()
                    .is_server_error());
                ensure!(call("/header-count")
                    .send()
                    .await?
                    .status()
                    .is_server_error());
                ensure!(call("/duplicate-alarm").send().await?.json::<u32>().await? == 1);
                ensure!(call("/generator").send().await?.json::<bool>().await?);
                ensure!(call("/atomic").send().await?.json::<bool>().await?);
                ensure!(call("/unawaited").send().await?.json::<bool>().await?);
                ensure!(call("/late").send().await?.json::<Value>().await?.is_null());
                ensure!(call("/budget").send().await?.json::<bool>().await?);
                ensure!(call("/expire").send().await?.json::<bool>().await?);
                ensure!(call("/expire").send().await?.json::<String>().await? == "ContextExpired");
                let bytes = vec![0, 128, 255, 42];
                ensure!(
                    client
                        .post(format!("{url}/echo"))
                        .body(bytes.clone())
                        .send()
                        .await?
                        .bytes()
                        .await?
                        .as_ref()
                        == bytes.as_slice()
                );
                ensure!(call("/failure").send().await?.status().is_server_error());
                let private = client
                    .post(format!("{url}/failure-private"))
                    .body("SENSITIVE-PYTHON-SENTINEL")
                    .send()
                    .await?;
                ensure!(private.status().is_server_error());
                let failure = private.text().await?;
                ensure!(
                    !failure.contains("SENSITIVE-PYTHON-SENTINEL")
                        && !failure.contains("private-internal")
                        && !failure.contains("request-body-secret"),
                    "handler exception exposed private application data"
                );
                ensure!(call("/oversize").send().await?.status().is_server_error());
                ensure!(call("/cancel").send().await?.status().is_server_error());
                // Failure, cancellation and body limits must not poison later turns.
                let result: Value = client
                    .post(format!("{url}/counter"))
                    .send()
                    .await?
                    .json()
                    .await?;
                ensure!(result == json!({"count":1,"alarms":0,"name":"one"}));
                call("/alarm").send().await?.error_for_status()?;
                let mut fired = false;
                for _ in 0..40 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    if call("/counter").send().await?.json::<Value>().await?["alarms"] == 1 {
                        fired = true;
                        break;
                    }
                }
                ensure!(fired, "alarm did not execute in Python");
                stop(node, &monitor).await?;
                let (node, url) = start(directory.path(), application.clone()).await?;
                let after: Value = client
                    .get(format!("{url}/counter"))
                    .send()
                    .await?
                    .json()
                    .await?;
                ensure!(
                    after == json!({"count":1,"alarms":1,"name":"one"}),
                    "restart lost state: {after}"
                );
                stop(node, &monitor).await?;
                let bucket =
                    cellar::bucket::Bucket::open_dev(&directory.path().join("objects.sqlite"))?;
                let (a, a_url) = start_with(
                    &directory.path().join("a"),
                    application.clone(),
                    "python-a",
                    Some(bucket.clone()),
                )
                .await?;
                let first: Value = client
                    .post(format!("{a_url}/counter"))
                    .send()
                    .await?
                    .json()
                    .await?;
                ensure!(first["count"] == 1, "first fleet write: {first}");
                let (b, b_url) = start_with(
                    &directory.path().join("b"),
                    application,
                    "python-b",
                    Some(bucket),
                )
                .await?;
                let remote: Value = client
                    .post(format!("{b_url}/counter"))
                    .send()
                    .await?
                    .json()
                    .await?;
                ensure!(remote["count"] == 2, "remote owner routing: {remote}");
                stop_mode(a, ShutdownMode::Handoff, &monitor).await?;
                let recovered: Value = client
                    .post(format!("{b_url}/counter"))
                    .send()
                    .await?
                    .json()
                    .await?;
                ensure!(
                    recovered["count"] == 3,
                    "owner recovery lost acknowledged write: {recovered}"
                );
                stop(b, &monitor).await
            })
        },
    )?;
    ensure!(report.complete());
    ensure!(
        report.budget_exhaustions > 0
            && report.bridge.peak_queued_bytes > 0
            && report.bridge.transferred_bytes > 0
    );
    ensure!(
        report.native_outcomes.len() == 5,
        "missing native report: {:?}",
        report.native_outcomes
    );
    let roundtrip: cellar_python::Report = serde_json::from_slice(&serde_json::to_vec(&report)?)?;
    ensure!(roundtrip.complete());
    ensure!(report.objects_created > 0 && report.objects_created == report.objects_retired);
    pyo3::Python::attach(|py| -> Result<()> {
        use pyo3::prelude::*;
        ensure!(
            py.import("sys")?
                .getattr("modules")?
                .get_item("_cellar_application")?
                .getattr("generator_closed")?
                .extract::<bool>()?,
            "async generator finally did not run"
        );
        Ok(())
    })?;
    // Reinitialization must fail before touching a second interpreter.
    ensure!(run(
        Options::default(),
        Source::new("x=1", "unused.py"),
        |_, _| Ok(())
    )
    .is_err());
    cellar::cli_output::Output::new(cellar::cli_output::Format::Text).line(format_args!("PASS embedded Python: HTTP, native import, checked storage, SQL, alarms, expiration, cancellation, restart, remote owner routing, handoff recovery, shutdown"))?;
    Ok(())
}

fn lifecycle_mode(mode: &str) -> Result<()> {
    let mut child = std::process::Command::new(std::env::current_exe()?)
        .arg(mode)
        .spawn()?;
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = child.try_wait()? {
            ensure!(status.success(), "lifecycle test failed: {mode}");
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            anyhow::bail!("lifecycle test timed out: {mode}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn run<F>(options: Options, source: Source, host: F) -> Result<cellar_python::Report>
where
    F: FnOnce(Arc<PythonApplication>, cellar_python::Stop) -> Result<()> + Send + 'static,
{
    // SAFETY: this standalone harness owns process initialization on main. Each
    // lifecycle scenario has its own process; no foreign Python API is called.
    unsafe { PythonRuntime::run(options, source, host) }
}

async fn context_isolation(client: &reqwest::Client, holder: &str, other: &str) -> Result<()> {
    let holding = client.get(format!("{holder}/scope-hold")).send();
    let crossing = async {
        let mut ready = false;
        for _ in 0..20 {
            if client
                .get(format!("{other}/scope-ready"))
                .send()
                .await?
                .json::<bool>()
                .await?
            {
                ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        ensure!(ready, "first domain did not expose its live context");
        let stolen: Value = client
            .get(format!("{other}/scope-steal"))
            .send()
            .await?
            .json()
            .await?;
        let direct: Value = client
            .get(format!("{other}/scope-direct"))
            .send()
            .await?
            .json()
            .await?;
        client
            .get(format!("{other}/scope-release"))
            .send()
            .await?
            .error_for_status()?;
        ensure!(
            stolen == json!({"refused":true,"code":"ContextMismatch"}),
            "live context crossed domains: {stolen}"
        );
        ensure!(
            direct == json!({"refused":true,"code":"ContextMismatch"}),
            "native sender crossed domains: {direct}"
        );
        Ok::<_, anyhow::Error>(())
    };
    let (held, crossed) = futures_util::future::join(holding, crossing).await;
    held?.error_for_status()?;
    crossed
}

async fn host_await_progress(client: &reqwest::Client, url: &str) -> Result<()> {
    let waiting = client.get(format!("{url}/host-await")).send();
    let progress = async {
        let mut started = false;
        for _ in 0..20 {
            if client
                .get(format!("{url}/host-waiting"))
                .send()
                .await?
                .json::<bool>()
                .await?
            {
                started = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        ensure!(started, "Python handler did not enter its host SDK await");
        let response = client.get(format!("{url}/host-progress")).send().await?;
        client
            .get(format!("{url}/host-release"))
            .send()
            .await?
            .error_for_status()?;
        ensure!(
            response.json::<bool>().await?,
            "another Python task could not run during a host await"
        );
        Ok::<_, anyhow::Error>(())
    };
    let (response, progress) = futures_util::future::join(waiting, progress).await;
    ensure!(response?.json::<bool>().await?);
    progress
}
