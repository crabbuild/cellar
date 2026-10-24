// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Real-node adapter contract, on the process main thread (one interpreter).
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::*;
use cellar_python::{EventKind, Options, PythonApplication, PythonRuntime, Source};
use pyo3::types::PyAnyMethods;
use serde_json::{json, Value};
use std::{path::Path, sync::Arc, time::Duration};

#[path = "support/authority.rs"]
mod authority;
#[path = "support/binding_faults.rs"]
mod binding_faults;
#[path = "support/bindings.rs"]
mod bindings;
#[path = "support/capacity.rs"]
mod capacity;
#[path = "support/containers.rs"]
mod containers;
#[path = "support/cycles.rs"]
mod cycles;
#[path = "support/delivery.rs"]
mod delivery;
#[path = "support/durability.rs"]
mod durability;
#[path = "support/fault_gates.rs"]
mod fault_gates;
#[path = "support/fencing.rs"]
mod fencing;
#[path = "../../cellar/tests/support/html.rs"]
#[allow(dead_code)]
mod fixture;
#[path = "support/initial_reads.rs"]
mod initial_reads;
#[path = "support/lifetimes.rs"]
mod lifetimes;
#[path = "support/limits.rs"]
mod limits;
#[path = "support/native_writer.rs"]
mod native_writer;
#[path = "support/privacy.rs"]
mod privacy;
#[path = "support/proof_authority.rs"]
mod proof_authority;
#[path = "support/python_control.rs"]
mod python_control;
#[path = "support/queue_lost_ack.rs"]
mod queue_lost_ack;
#[path = "support/queue_observation.rs"]
mod queue_observation;
#[path = "support/routing.rs"]
mod routing;
#[path = "support/schedule_recovery.rs"]
mod schedule_recovery;
#[path = "support/schedules.rs"]
mod schedules;
#[path = "support/wakeups.rs"]
mod wakeups;
#[path = "support/watchdog.rs"]
mod watchdog;

async fn start(path: &Path, application: Arc<PythonApplication>) -> Result<(Node, String)> {
    start_with(path, application, "python-test", None).await
}

async fn start_with(
    path: &Path,
    application: Arc<dyn Application>,
    name: &str,
    bucket: Option<cellar::bucket::Bucket>,
) -> Result<(Node, String)> {
    start_configured(path, application, name, bucket, |_| {}).await
}

async fn start_configured(
    path: &Path,
    application: Arc<dyn Application>,
    name: &str,
    bucket: Option<cellar::bucket::Bucket>,
    configure: impl FnOnce(&mut NodeConfig),
) -> Result<(Node, String)> {
    start_services(path, application, name, bucket, move |config, _| {
        configure(config)
    })
    .await
}

async fn start_services(
    path: &Path,
    application: Arc<dyn Application>,
    name: &str,
    bucket: Option<cellar::bucket::Bucket>,
    configure: impl FnOnce(&mut NodeConfig, &mut NodeServices),
) -> Result<(Node, String)> {
    start_graph(
        path,
        application,
        name,
        bucket,
        move |config, services, _| configure(config, services),
    )
    .await
}

async fn start_graph(
    path: &Path,
    application: Arc<dyn Application>,
    name: &str,
    bucket: Option<cellar::bucket::Bucket>,
    configure: impl FnOnce(&mut NodeConfig, &mut NodeServices, &mut DeploymentGraph),
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
    let mut services = NodeServices {
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
    configure(&mut config, &mut services, &mut graph);
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
    if mode == "--runtime-contract" {
        return contract();
    }
    if mode == "--runtime-initial-reads" {
        return initial_reads::child();
    }
    if mode == "--runtime-wakeups" {
        return wakeups::child();
    }
    if mode == "--runtime-privacy" {
        return privacy::child();
    }
    if mode == "--runtime-proof-authority-failure" {
        return proof_authority::child("failure", false);
    }
    if mode == "--runtime-proof-authority-recovery" {
        return proof_authority::child("recovery", false);
    }
    if mode == "--runtime-routing" {
        return routing::child();
    }
    if let Some(mode) = mode.strip_prefix("--runtime-proof-authority-") {
        let (kind, outcome) = mode.split_once('-').context("proof authority mode")?;
        ensure!(matches!(kind, "sync" | "output") && matches!(outcome, "deadline" | "fence"));
        return proof_authority::child(kind, outcome == "fence");
    }
    if mode == "--runtime-authority" || mode == "--runtime-authority-fence" {
        return authority::child(mode.ends_with("-fence"));
    }
    if matches!(
        mode.as_str(),
        "--runtime-cycles" | "--runtime-cycles-fence" | "--runtime-cycles-inner-cancel"
    ) {
        return cycles::child(match mode.as_str() {
            "--runtime-cycles-fence" => cycles::End::Fence,
            "--runtime-cycles-inner-cancel" => cycles::End::InnerCancellation,
            _ => cycles::End::Deadline,
        });
    }
    if let Some(mode) = mode.strip_prefix("--runtime-capacity-") {
        return capacity::child(mode);
    }
    if mode == "--runtime-lifetimes" {
        return lifetimes::child();
    }
    if mode == "--runtime-lifetimes-close" {
        return lifetimes::closing_child();
    }
    if mode == "--runtime-lifetimes-cancel" {
        return lifetimes::cancelling_child();
    }
    if mode == "--runtime-schedule-recovery" {
        return schedule_recovery::qualify();
    }
    if mode == "--schedule-owner" {
        return schedule_recovery::child(
            &std::env::args().nth(2).context("schedule recovery mode")?,
            Path::new(
                &std::env::args()
                    .nth(3)
                    .context("schedule recovery directory")?,
            ),
        );
    }
    if mode == "--runtime-watchdog" {
        return watchdog::qualify();
    }
    if mode == "--runtime-delivery" {
        return delivery::child();
    }
    if mode == "--runtime-schedules" {
        return schedules::child();
    }
    if mode == "--runtime-binding-faults" {
        return binding_faults::child();
    }
    if mode == "--runtime-queue-lost-ack" {
        return queue_lost_ack::child();
    }
    if mode == "--runtime-containers" {
        return containers::child();
    }
    if mode == "--runtime-limits" {
        return limits::child();
    }
    if mode == "--runtime-bindings" {
        return bindings::child();
    }
    if mode == "--runtime-durability" {
        return durability::child();
    }
    if mode == "--runtime-fenced" {
        return fencing::child();
    }
    if mode.starts_with("--owner-") {
        return watchdog::child(
            &mode,
            Path::new(&std::env::args().nth(2).expect("private owner directory")),
        );
    }
    if matches!(
        mode.as_str(),
        "--runtime-fatal" | "--runtime-unraisable" | "--runtime-thread-error"
    ) {
        let endpoint = match mode.as_str() {
            "--runtime-unraisable" => "/unraisable",
            "--runtime-thread-error" => "/thread-error",
            _ => "/fatal",
        };
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
                        client.get(format!("{url}{endpoint}")).send(),
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
        let failure = error.downcast_ref::<cellar_python::Incomplete>().unwrap();
        if endpoint == "/unraisable" {
            ensure!(failure.report.unraisable_errors == 1);
        } else if endpoint == "/thread-error" {
            ensure!(failure.report.thread_errors == 1);
        }
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
        ensure!(
            matches!(
                mode.as_str(),
                "--runtime-dependency" | "--runtime-failure" | "--runtime-panic"
            ),
            "unknown runtime test mode: {mode}"
        );
        let mut source = Source::new("x=1", "lifecycle.py");
        if mode == "--runtime-dependency" {
            source = source.require_distribution("cellar-missing-test-package", "0");
        }
        let trace_allocations = matches!(mode.as_str(), "--runtime-failure" | "--runtime-panic");
        let error = run(
            Options {
                trace_allocations,
                ..Options::default()
            },
            source,
            move |_, _| {
                if mode == "--runtime-panic" {
                    panic!("controlled supervisor panic");
                }
                anyhow::bail!("controlled supervisor failure")
            },
        )
        .expect_err("lifecycle failure must propagate");
        if trace_allocations {
            let tracing = pyo3::Python::attach(|py| -> pyo3::PyResult<bool> {
                py.import("tracemalloc")?
                    .call_method0("is_tracing")?
                    .extract()
            })?;
            ensure!(
                !tracing,
                "owned allocation profiler survived a supervisor failure"
            );
        }
        ensure!(!error.to_string().is_empty());
        return Ok(());
    }
    for mode in [
        "--runtime-wakeups",
        "--runtime-dependency",
        "--runtime-failure",
        "--runtime-panic",
        "--runtime-stubborn",
        "--runtime-fatal",
        "--runtime-unraisable",
        "--runtime-thread-error",
        "--runtime-fenced",
        "--runtime-durability",
        "--runtime-bindings",
        "--runtime-delivery",
        "--runtime-schedules",
        "--runtime-schedule-recovery",
        "--runtime-binding-faults",
        "--runtime-queue-lost-ack",
        "--runtime-limits",
        "--runtime-containers",
        "--runtime-initial-reads",
        "--runtime-cycles",
        "--runtime-cycles-fence",
        "--runtime-cycles-inner-cancel",
        "--runtime-authority",
        "--runtime-authority-fence",
        "--runtime-proof-authority-sync-deadline",
        "--runtime-proof-authority-sync-fence",
        "--runtime-proof-authority-output-deadline",
        "--runtime-proof-authority-output-fence",
        "--runtime-proof-authority-failure",
        "--runtime-proof-authority-recovery",
        "--runtime-routing",
        "--runtime-privacy",
        "--runtime-lifetimes",
        "--runtime-lifetimes-close",
        "--runtime-lifetimes-cancel",
        "--runtime-capacity-invocations",
        "--runtime-capacity-nested-invocations",
        "--runtime-capacity-nested-remote",
        "--runtime-capacity-tasks",
        "--runtime-capacity-objects",
        "--runtime-capacity-bytes",
        "--runtime-capacity-replies",
        "--runtime-capacity-cleanup-error",
        "--runtime-capacity-cleanup-full",
    ] {
        lifecycle_mode(mode)?;
    }
    watchdog::qualify()?;
    contract()
}

fn contract() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let report = run(
        Options {
            max_body_bytes: 4096,
            max_value_bytes: 16384,
            max_operations: 64,
            max_batch_operations: 4,
            max_tasks: 129,
            telemetry_interval: Duration::from_millis(10),
            trace_allocations: true,
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
                let mut unsupported = fixture::graph("python-test", "", vec!["Counter".into()]);
                unsupported.primary.crons.push("* * * * *".into());
                ensure!(application.validate_deployment(&unsupported.primary).is_err(), "undeclared cron event accepted");
                unsupported.primary.crons.clear();
                unsupported.primary.options.queue_consumers.push(serde_json::from_value(json!({"queue":"test", "max_batch_size":10, "max_batch_timeout":1, "max_retries":2}))?);
                ensure!(application.validate_deployment(&unsupported.primary).is_err(), "undeclared queue event accepted");
                unsupported.primary.options.queue_consumers.clear();
                unsupported.primary.options.do_classes.extend([
                    cellar::deploy::SQL_CLASS.into(), cellar::deploy::KV_CLASS.into(), cellar::deploy::QUEUE_CLASS.into(),
                ]);
                application.validate_deployment(&unsupported.primary)?;
                unsupported.primary.options.do_classes.push("MissingPythonClass".into());
                ensure!(application.validate_deployment(&unsupported.primary).is_err(), "undeclared Python class accepted");
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
                scoped.context("cross-domain context isolation")?;
                host_await_progress(&client, &url, &application).await.context("host-await progress")?;
                let parallel: Value = client.get(format!("{url}/parallel-python")).send().await?
                    .error_for_status()?.json().await?;
                let free_threaded = pyo3::Python::attach(|py| -> pyo3::PyResult<bool> {
                    Ok(py.import("sys")?.getattr("abiflags")?.extract::<String>()?.contains('t'))
                })?;
                ensure!(parallel["free_threaded"] == free_threaded && parallel["gil_enabled"] == !free_threaded,
                    "bridge import unexpectedly changed GIL mode: {parallel}");
                for seed in 1..=2 {
                    let expected: u64 = (0..100_000u64).map(|index|(index ^ seed) % 97).sum();
                    ensure!(parallel["values"][(seed-1) as usize] == expected,"parallel Python result: {parallel}");
                }
                ensure!(client.get(format!("{url}/loop-lag")).send().await?.error_for_status()?.json::<bool>().await?);
                tokio::time::timeout(Duration::from_secs(1), async {
                    loop {
                        if let Some(sample) = application.interpreter_snapshot() {
                            if sample.interpreter.loop_delay.maximum_ns >= 40_000_000 {
                                ensure!(sample.interpreter.thread_cpu_ns > 0 && sample.interpreter.allocated_blocks_current > 0);
                                break;
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    Ok::<_, anyhow::Error>(())
                }).await??;
                let (writer_node, writer_url) = start_with(
                    &directory.path().join("native-writer"),
                    Arc::new(native_writer::ApplicationWithWriter(application.clone())),
                    "native-writer", None,
                ).await?;
                let conflict = native_writer::qualify(&client, &writer_url).await;
                stop(writer_node, &monitor).await?;
                conflict.context("native writer conflict")?;
                let call = |path: &str| client.get(format!("{url}{path}"));
                ensure!(
                    response_json::<String>(call("/threadpool").send().await?).await?.len()
                        == 64
                );
                ensure!(response_json::<String>(call("/library").send().await?).await?.len() == 64);
                ensure!(response_json::<bool>(call("/constructor-failure").send().await?).await?);
                ensure!(response_json::<bool>(call("/codec").send().await?).await?);
                ensure!(response_json::<bool>(call("/task-budget").send().await?).await?);
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
                ensure!(response_json::<u32>(call("/duplicate-alarm").send().await?).await? == 1);
                ensure!(response_json::<bool>(call("/generator").send().await?).await?);
                ensure!(call("/atomic").send().await?.error_for_status().context("atomic storage")?.json::<bool>().await?);
                ensure!(response_json::<bool>(call("/read-batch").send().await?).await?);
                ensure!(call("/batch-validation").send().await?.error_for_status().context("batch validation")?.json::<bool>().await?);
                ensure!(call("/protocol-validation").send().await?.error_for_status().context("protocol validation")?.json::<bool>().await?);
                ensure!(response_json::<bool>(call("/unawaited").send().await?).await?);
                ensure!(response_json::<Value>(call("/late").send().await?).await?.is_null());
                ensure!(response_json::<bool>(call("/budget").send().await?).await?);
                ensure!(response_json::<bool>(call("/expire").send().await?).await?);
                ensure!(response_json::<String>(call("/expire").send().await?).await? == "ContextExpired");
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
                let result: Value = response_json(client.post(format!("{url}/counter")).send().await?)
                    .await.context("counter after cancellation")?;
                ensure!(result == json!({"count":1,"alarms":0,"name":"one"}));
                call("/alarm").send().await?.error_for_status()?;
                let mut fired = false;
                for _ in 0..40 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    if response_json::<Value>(call("/counter").send().await?).await?["alarms"] == 1 {
                        fired = true;
                        break;
                    }
                }
                ensure!(fired, "alarm did not execute in Python");
                stop(node, &monitor).await?;
                let (node, url) = start(directory.path(), application.clone()).await?;
                let after: Value = response_json(client.get(format!("{url}/counter")).send().await?)
                    .await.context("counter after restart")?;
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
                let first: Value = response_json(client.post(format!("{a_url}/counter")).send().await?)
                    .await.context("first fleet write")?;
                ensure!(first["count"] == 1, "first fleet write: {first}");
                let (b, b_url) = start_with(
                    &directory.path().join("b"),
                    application,
                    "python-b",
                    Some(bucket),
                )
                .await?;
                let remote: Value = response_json(client.post(format!("{b_url}/counter")).send().await?)
                    .await.context("remote owner write")?;
                ensure!(remote["count"] == 2, "remote owner routing: {remote}");
                stop_mode(a, ShutdownMode::Handoff, &monitor).await?;
                let recovered: Value = response_json(client.post(format!("{b_url}/counter")).send().await?)
                    .await.context("owner recovery write")?;
                ensure!(
                    recovered["count"] == 3,
                    "owner recovery lost acknowledged write: {recovered}"
                );
                stop(b, &monitor).await
            })
        },
    )?;
    ensure!(report.complete());
    ensure!(report
        .interpreter
        .traced_current_bytes
        .is_some_and(|value| value > 0));
    ensure!(report.interpreter.traced_peak_bytes >= report.interpreter.traced_current_bytes);
    ensure!(report
        .interpreter
        .trace_bookkeeping_bytes
        .is_some_and(|value| value > 0));
    ensure!(
        report.memory.before_interpreter.process_rss_bytes.is_some()
            && report.memory.after_join.process_rss_bytes.is_some()
    );
    ensure!(
        report
            .memory
            .before_interpreter
            .jemalloc_allocated_bytes
            .is_some()
            && report.memory.after_join.jemalloc_allocated_bytes.is_some()
    );
    ensure!(report.interpreter.loop_delay.maximum_ns >= 40_000_000);
    ensure!(
        report.interpreter.loop_delay.samples
            == report
                .interpreter
                .loop_delay
                .bucket_counts
                .iter()
                .sum::<u64>()
    );
    let operations = &report.bridge.operations;
    ensure!(operations.settled() && operations.admitted > 0 && operations.failed > 0);
    ensure!(
        operations.abandoned_running > 0,
        "dropped native futures were not accounted for"
    );
    ensure!(
        operations.queue_wait.samples == operations.started
            && operations.sdk_duration.samples == operations.started
    );
    ensure!(
        operations.reply_wait.samples == operations.delivered
            && operations.round_trip.samples == operations.delivered
    );
    for histogram in [
        &operations.queue_wait,
        &operations.sdk_duration,
        &operations.reply_wait,
        &operations.round_trip,
    ] {
        ensure!(histogram.samples == histogram.bucket_counts.iter().sum::<u64>());
    }
    ensure!(
        report.budget_exhaustions > 0
            && report.bridge.peak_queued_bytes > 0
            && report.bridge.reserved_bytes_total > 0
            && report.bridge.input_copied_bytes > 0
            && report.bridge.output_copied_bytes > 0
    );
    ensure!(
        report.native_outcomes.len() == 6,
        "missing native report: {:?}",
        report.native_outcomes
    );
    let roundtrip: cellar_python::Report = serde_json::from_slice(&serde_json::to_vec(&report)?)?;
    ensure!(roundtrip.complete());
    ensure!(report.objects_created > 0 && report.objects_created == report.objects_retired);
    ensure!(
        report.interpreter.thread_cpu_ns > 0
            && report.interpreter.wall_ns > 0
            && report.interpreter.process_peak_rss_bytes > 0
            && report.interpreter.allocated_blocks_initial > 0
            && report.interpreter.allocated_blocks_final > 0
            && report.interpreter.handlers_started > 0
            && report.interpreter.handlers_started
                == report.interpreter.handlers_completed
                    + report.interpreter.handlers_failed
                    + report.interpreter.handlers_cancelled
            && report.interpreter.handler_wall_ns_sum >= report.interpreter.handler_wall_ns_max
            && report.interpreter.handler_wall_ns_max > 0,
        "incomplete interpreter measurements: {:?}",
        report.interpreter
    );
    pyo3::Python::attach(|py| -> Result<()> {
        use pyo3::prelude::*;
        py.import("gc")?.call_method0("collect")?;
        let app = py
            .import("sys")?
            .getattr("modules")?
            .get_item("_cellar_application")?;
        ensure!(
            app.getattr("instances")?.len()? == 0,
            "retired Python cell objects remain live"
        );
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
    let output = tempfile::NamedTempFile::new()?;
    let mut child = std::process::Command::new(std::env::current_exe()?)
        .arg(mode)
        .stdout(output.as_file().try_clone()?)
        .stderr(output.as_file().try_clone()?)
        .spawn()?;
    let deadline = std::time::Instant::now()
        + Duration::from_secs(if mode == "--runtime-schedule-recovery" {
            240
        } else if mode == "--runtime-binding-faults" {
            90
        } else if matches!(
            mode,
            "--runtime-durability"
                | "--runtime-cycles"
                | "--runtime-cycles-fence"
                | "--runtime-cycles-inner-cancel"
                | "--runtime-authority"
                | "--runtime-authority-fence"
                | "--runtime-proof-authority-sync-deadline"
                | "--runtime-proof-authority-sync-fence"
                | "--runtime-proof-authority-output-deadline"
                | "--runtime-proof-authority-output-fence"
                | "--runtime-proof-authority-failure"
                | "--runtime-proof-authority-recovery"
                | "--runtime-routing"
                | "--runtime-bindings"
                | "--runtime-delivery"
                | "--runtime-queue-lost-ack"
                | "--runtime-schedules"
                | "--runtime-lifetimes"
                | "--runtime-lifetimes-close"
                | "--runtime-lifetimes-cancel"
                | "--runtime-capacity-invocations"
                | "--runtime-capacity-nested-invocations"
                | "--runtime-capacity-nested-remote"
                | "--runtime-capacity-tasks"
                | "--runtime-capacity-objects"
                | "--runtime-capacity-bytes"
                | "--runtime-capacity-replies"
                | "--runtime-capacity-cleanup-error"
                | "--runtime-capacity-cleanup-full"
        ) {
            60
        } else {
            15
        });
    loop {
        if let Some(status) = child.try_wait()? {
            let log = std::fs::read_to_string(output.path())?;
            ensure!(
                !log.contains("PRIVATE-UNMANAGED-EXCEPTION-SENTINEL"),
                "private Python diagnostic reached process logs: {mode}"
            );
            ensure!(status.success(), "lifecycle test failed: {mode}: {log}");
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

// These fixture endpoints promise JSON on success. Preserve the failing URL
// and HTTP status without dumping application payloads or retrying mutations.
async fn response_json<T: serde::de::DeserializeOwned>(response: reqwest::Response) -> Result<T> {
    let url = response.url().clone();
    let status = response.status();
    response
        .error_for_status()?
        .json()
        .await
        .with_context(|| format!("decode JSON from {url} (HTTP {status})"))
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
        let rebound: Value = client
            .get(format!("{other}/scope-rebound"))
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
        ensure!(
            rebound == json!({"refused":true,"code":"ContextMismatch"}),
            "rebound module attribute replaced native caller identity: {rebound}"
        );
        Ok::<_, anyhow::Error>(())
    };
    let (held, crossed) = futures_util::future::join(holding, crossing).await;
    held?.error_for_status()?;
    crossed
}

async fn host_await_progress(
    client: &reqwest::Client,
    url: &str,
    application: &PythonApplication,
) -> Result<()> {
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
        let live = application.bridge_report().operations;
        ensure!(
            live.running > 0 && live.pending > 0 && !live.settled(),
            "live pending SDK work disappeared from telemetry: {live:?}"
        );
        tokio::time::timeout(Duration::from_millis(100), async {
            loop {
                if let Some(sample) = application.interpreter_snapshot() {
                    if sample.work.active_invocations > 0 {
                        ensure!(
                            sample.work.active_tasks > 0
                                && sample.interpreter.allocated_blocks_current > 0
                        );
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
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
