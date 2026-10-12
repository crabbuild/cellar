use super::*;
use crate::application::{Application, Cell, Context, HandlerFuture, Invocation, Output, Registry};
use crate::asyncrt::{ExecutionDomain, TaskHandle};
use crate::node_lifecycle::{FenceReason, FenceReport, NodeOutcome, ShutdownPolicy};
use std::{collections::HashMap, time::Duration};
use tokio::sync::Semaphore;

struct Signals {
    entered: mpsc::UnboundedSender<()>,
    release: Semaphore,
}
struct Root {
    label: &'static str,
    signals: Arc<Signals>,
}
fn output(text: impl ToString) -> Output {
    Output::Http(HttpResponse {
        status: 200,
        body: text.to_string().into_bytes(),
        headers: vec![],
        stream: None,
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}
impl Cell for Root {
    fn invoke(self: Arc<Self>, ctx: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(req) = event else {
                return Ok(Output::Done);
            };
            if req.url.ends_with("/hold") {
                self.signals.entered.send(())?;
                self.signals.release.acquire().await?.forget();
            }
            if req.url.ends_with("/facet") {
                let class = ctx.exported_cell_class("Leaf")?;
                let facet = ctx.facets()?.get(&ctx, "same", move |_| {
                    Box::pin(async move { Ok(crate::application::facets::Startup::new(class)) })
                })?;
                return Ok(Output::Http(facet.fetch(&ctx, req).await?));
            }
            let storage = ctx.storage()?;
            let mut count = storage
                .get_json("count")?
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            if req.method == "POST" {
                count += 1;
                storage.put_json("count", &serde_json::json!(count))?;
            }
            Ok(output(format!("{}:{count}", self.label)))
        })
    }
}
struct Leaf(&'static str);
impl Cell for Leaf {
    fn invoke(self: Arc<Self>, ctx: Context, _: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            // Force a facet SQLite write and its real output proof.
            ctx.storage()?
                .put_json("label", &serde_json::json!(self.0))?;
            Ok(output(self.0))
        })
    }
}
fn graph() -> DeploymentGraph {
    DeploymentGraph::single(fleet::LoadedDeployment {
        native: None,
        options: WorkerConfigOptions {
            src: String::new(),
            script_name: "driver".into(),
            do_classes: vec!["Root".into()],
            bindings: vec![("ROOT".into(), "Root".into())],
            blob_bindings: vec![],
            sql_bindings: vec![],
            kv_bindings: vec![],
            queue_bindings: vec![],
            queue_consumers: vec![],
            workflow_bindings: vec![],
            vars: vec![],
            node: "test".into(),
            modules: vec![],
            compat: Compat::default(),
        },
        script_name: "driver".into(),
        version: "test".into(),
        prefix: "driver/".into(),
        asset_binding: None,
        loader_bindings: vec![],
        assets: None,
        services: vec![],
        crons: vec![],
        containers: vec![],
        fence_image: None,
        container_images: vec![],
    })
}
struct Host {
    domain: ExecutionDomain,
    app: AppHandle,
    origin: String,
    stop: oneshot::Sender<ShutdownMode>,
    task: TaskHandle<anyhow::Result<NodeOutcome>>,
    fence: mpsc::UnboundedSender<FenceReport>,
    core_exit: mpsc::UnboundedSender<Result<(), String>>,
    signals: Arc<Signals>,
    entered: mpsc::UnboundedReceiver<()>,
}
async fn host(label: &'static str, data_dir: &std::path::Path) -> anyhow::Result<Host> {
    host_with_policy(
        label,
        data_dir,
        ShutdownPolicy {
            total_ms: 5_000,
            token_wait_ms: 0,
            no_progress_ms: 3_000,
        },
    )
    .await
}
async fn host_with_policy(
    label: &'static str,
    data_dir: &std::path::Path,
    shutdown: ShutdownPolicy,
) -> anyhow::Result<Host> {
    host_with_fence_input(label, data_dir, shutdown, false).await
}
async fn host_with_fence_input(
    label: &'static str,
    data_dir: &std::path::Path,
    shutdown: ShutdownPolicy,
    fence_reports_closed: bool,
) -> anyhow::Result<Host> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    let (entered, received) = mpsc::unbounded_channel();
    let signals = Arc::new(Signals {
        entered,
        release: Semaphore::new(0),
    });
    let (fence, mut fence_rx) = mpsc::unbounded_channel();
    if fence_reports_closed {
        fence_rx.close();
    }
    let (core_exit, actor_exit_rx) = mpsc::unbounded_channel();
    let options = crate::actor::ActorOptions {
        core: cellar_logic::Config {
            max_resident: 32,
            max_cached_routes: 2_048,
            max_activations: 8,
            max_releases: 4,
            max_evictions: 4,
            operation_deadline_ms: Some(2_000),
            peer_protocol: peer_auth::PROTOCOL_VERSION,
            max_outbound_websockets: 8,
            ownership_on_evict: cellar_logic::OwnershipOnEvict::Sticky,
            require_node_lease: true,
            owner_log_recovery_backoff_ms: 50,
            owner_log_recovery_attempts: 3,
            alarm_resident_ms: 100,
            idle_evict_ms: None,
            pressure: Default::default(),
        },
        lease_ttl_ms: 10_000,
        placement_weight: 1,
        generation: format!("{label}-process"),
        resume_generation: None,
    };
    domain.services().configure_actor(&options)?;
    let (prepared, actor, actor_rx, origin) = domain
        .run(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let internal_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
            let origin = format!("http://{}", listener.local_addr()?);
            let advertise = internal_listener.local_addr()?.to_string();
            let parent_signals = signals.clone();
            let application = Registry::new(|ctx, event| {
                Box::pin(async move {
                    let Invocation::Fetch(req) = event else {
                        return Ok(Output::Done);
                    };
                    let cell = ctx.namespace("ROOT")?.get_by_name("same");
                    Ok(Output::Http(cell.fetch(&ctx, req).await?))
                })
            })
            .cell("Root", move |_, _| {
                Ok(Arc::new(Root {
                    label,
                    signals: parent_signals.clone(),
                }))
            })?
            .cell("Leaf", move |_, _| Ok(Arc::new(Leaf(label))))?;
            let generation = Generation::build_native(
                1,
                graph(),
                GenerationOptions {
                    node: "test".into(),
                    region: "test".into(),
                },
                HashMap::from([(
                    "driver".into(),
                    Arc::new(application) as Arc<dyn Application>,
                )]),
            )?;
            let runtime = RuntimeManager::start(
                generation,
                RuntimeOptions {
                    data_dir: data_dir.into(),
                    replication: None,
                    wake: None,
                    alarm_observer: Arc::new(|_, _| {}),
                    node: "test".into(),
                    region: "test".into(),
                    bucket: None,
                },
            )?;
            let (tx, actor_rx) = mpsc::unbounded_channel();
            let peer_auth = Arc::new(PeerAuth::new([7; 32], "test")?);
            let peer_http = reqwest::Client::new();
            let drain_pins = DrainPinRegistry::default();
            let actor = Actor::new(
                options.clone(),
                fence.clone(),
                ActorServices {
                    runtime: Some(runtime.clone()),
                    ownership: None,
                    drain_pins: drain_pins.clone(),
                    peer_http: peer_http.clone(),
                    peer_auth: peer_auth.clone(),
                },
                ActorIdentity {
                    node: "test".into(),
                    region: "test".into(),
                    advertise: advertise.clone(),
                },
            )
            .await?;
            let (websockets, websocket_rx) = mpsc::unbounded_channel();
            let (reload, _) = crate::generation::reload_channel();
            let app = AppHandle {
                tx,
                runtime: Some(runtime),
                reload,
                peer_http,
                peer_auth,
                advertise,
                websockets,
                draining: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                fleet_ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
                public_in_flight: Arc::new(AtomicUsize::new(0)),
                drain_pins,
                trust_forwarded_headers: false,
                max_outbound_websockets: 8,
                max_request_body_bytes: 1 << 20,
                operation_deadline_ms: 2_000,
                follower: None,
            };
            let prepared = PreparedDriver {
                app,
                listener,
                internal_listener,
                channels: Channels::install(websocket_rx),
                fence_rx,
                actor_exit_rx,
                ready_ownership: None,
                fleet_bucket: None,
                clean_reload_node: "test".into(),
                process_generation: options.generation,
                data_dir: data_dir.into(),
                durability_owner: None,
                policy: RunPolicy {
                    shutdown,
                    local_cache_max_bytes: None,
                    local_cache_prune_period: Duration::from_secs(60),
                },
            };
            Ok::<_, anyhow::Error>((prepared, actor, actor_rx, origin))
        })
        .await??;
    let app = prepared.app.clone();
    let actor_task = actor.spawn(&domain, &tokio::runtime::Handle::current(), actor_rx)?;
    let exit_tx = core_exit.clone();
    domain
        .spawn(async move {
            let outcome = match actor_task.await {
                Ok(result) => result.map_err(|e| e.to_string()),
                Err(error) => Err(error.to_string()),
            };
            let _ = exit_tx.send(outcome);
        })?
        .detach();
    let (stop, stopping) = oneshot::channel();
    let task = domain.spawn(run(prepared, async {
        stopping.await.unwrap_or(ShutdownMode::Handoff)
    }))?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
    loop {
        if tokio::time::timeout(Duration::from_millis(100), app.healthy()).await == Ok(true) {
            break;
        }
        anyhow::ensure!(
            tokio::time::Instant::now() < deadline,
            "driver actor did not become healthy"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    Ok(Host {
        domain,
        app,
        origin,
        stop,
        task,
        fence,
        core_exit,
        signals,
        entered: received,
    })
}
fn client() -> anyhow::Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?)
}
async fn call(client: &reqwest::Client, host: &Host, path: &str) -> anyhow::Result<String> {
    let response = client.post(format!("{}{path}", host.origin)).send().await?;
    anyhow::ensure!(
        response.status() == 200,
        "driver HTTP status: {}",
        response.status()
    );
    Ok(response.text().await?)
}
async fn stopped(
    task: TaskHandle<anyhow::Result<NodeOutcome>>,
    mode: ShutdownMode,
) -> anyhow::Result<ShutdownReport> {
    let outcome = tokio::time::timeout(Duration::from_secs(8), task).await???;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("unexpected driver outcome: {outcome:?}")
    };
    assert_eq!(report.mode, mode);
    assert!(
        report.ingress_drained
            && report.cells_drained
            && report.log_quiesced
            && report.local_durability_closed
    );
    assert_eq!(report.pending_dispatches, 0);
    assert!(!report.drain_timed_out && !report.deadline_elapsed);
    Ok(report)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn caller_stops_drain_admitted_cells_and_facets_without_exiting_or_stopping_a_sibling(
) -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut a = host("a", &directory.path().join("a")).await?;
    let b = host("b", &directory.path().join("b")).await?;
    let client = client()?;
    assert_eq!(call(&client, &a, "/facet").await?, "a");
    assert_eq!(call(&client, &b, "/facet").await?, "b");
    let holding_client = client.clone();
    let holding_origin = a.origin.clone();
    let held = tokio::spawn(async move {
        holding_client
            .post(format!("{holding_origin}/hold"))
            .send()
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), a.entered.recv())
        .await?
        .unwrap();
    a.stop.send(ShutdownMode::Handoff).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !a.app.is_draining() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(call(&client, &b, "/write").await?, "b:1");
    a.signals.release.add_permits(1);
    let response = held.await??;
    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await?, "a:1");
    stopped(a.task, ShutdownMode::Handoff).await?;
    a.domain.shutdown().await;
    assert_eq!(call(&client, &b, "/write").await?, "b:2");
    let reopened = host("a", &directory.path().join("a")).await?;
    assert_eq!(call(&client, &reopened, "/write").await?, "a:2");
    reopened.stop.send(ShutdownMode::Handoff).unwrap();
    stopped(reopened.task, ShutdownMode::Handoff).await?;
    reopened.domain.shutdown().await;
    b.stop.send(ShutdownMode::Preserve).unwrap();
    let report = stopped(b.task, ShutdownMode::Preserve).await?;
    assert_eq!(
        report.clean_reload_prepared,
        directory.path().join("b/.clean-reload.json").exists()
    );
    b.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fencing_core_failure_and_cancellation_return_control_and_close_admission(
) -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let a = host("a", &directory.path().join("a")).await?;
    let b = host("b", &directory.path().join("b")).await?;
    a.fence.send(FenceReport {
        code: 9,
        reason: FenceReason::NodeLeaseMissing,
    })?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), a.task).await???,
        NodeOutcome::Fenced {
            code: 9,
            reason: FenceReason::NodeLeaseMissing,
        }
    );
    assert!(a.app.is_draining());
    a.domain.shutdown().await;
    assert_eq!(call(&client()?, &b, "/write").await?, "b:1");
    b.core_exit.send(Err("owned core failed".into()))?;
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(3), b.task).await???,
        NodeOutcome::CoreFailed {
            error: "owned core failed".into()
        }
    );
    assert!(b.app.is_draining());
    b.domain.shutdown().await;
    let c = host("c", &directory.path().join("c")).await?;
    c.task.abort();
    assert!(c.task.await.is_err());
    assert!(c.app.is_draining());
    c.domain.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fencing_during_application_drain_preserves_the_authority_report() -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut h = host("draining", directory.path()).await?;
    let request_client = client()?;
    let origin = h.origin.clone();
    let held =
        tokio::spawn(async move { request_client.post(format!("{origin}/hold")).send().await });
    tokio::time::timeout(Duration::from_secs(3), h.entered.recv())
        .await?
        .unwrap();
    h.stop.send(ShutdownMode::Handoff).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !h.app.is_draining() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    h.fence.send(FenceReport {
        code: 3,
        reason: FenceReason::NodeLeaseMismatch,
    })?;
    let outcome = tokio::time::timeout(Duration::from_secs(3), h.task).await???;
    assert_eq!(
        outcome,
        NodeOutcome::Fenced {
            code: 3,
            reason: FenceReason::NodeLeaseMismatch
        }
    );
    h.domain.shutdown().await;
    let _ = held.await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn losing_fence_reports_is_a_core_failure_without_fabricating_a_fence() -> anyhow::Result<()>
{
    let directory = tempfile::tempdir()?;
    let a = host_with_fence_input(
        "closed-fence-input",
        directory.path(),
        ShutdownPolicy {
            total_ms: 5_000,
            token_wait_ms: 0,
            no_progress_ms: 3_000,
        },
        true,
    )
    .await?;
    // The actor is healthy and has issued no Halt. Only its reporting path
    // was disconnected; supervision must fail closed without inventing one.
    assert!(a.app.healthy().await);
    let outcome = tokio::time::timeout(Duration::from_secs(3), a.task).await???;
    assert!(
        matches!(outcome, NodeOutcome::CoreFailed { .. }),
        "closed fence input fabricated an authority fence: {outcome:?}"
    );
    assert!(a.app.is_draining());
    a.domain.shutdown().await;
    Ok(())
}

#[test]
fn explicit_driver_policy_does_not_consult_shutdown_environment() -> anyhow::Result<()> {
    let output = std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", "daemon::driver::tests::caller_stops_drain_admitted_cells_and_facets_without_exiting_or_stopping_a_sibling", "--nocapture"])
        .env("CELLAR_SHUTDOWN_TOTAL_MS", "unusable").output()?;
    anyhow::ensure!(
        output.status.success(),
        "driver consulted the environment:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_deadline_cut_reports_unfinished_work_and_retains_the_last_committed_state(
) -> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    let mut h = host_with_policy(
        "cut",
        directory.path(),
        ShutdownPolicy {
            total_ms: 400,
            token_wait_ms: 0,
            no_progress_ms: 100,
        },
    )
    .await?;
    let request_client = client()?;
    assert_eq!(call(&request_client, &h, "/write").await?, "cut:1");
    let origin = h.origin.clone();
    let held =
        tokio::spawn(async move { request_client.post(format!("{origin}/hold")).send().await });
    tokio::time::timeout(Duration::from_secs(3), h.entered.recv())
        .await?
        .unwrap();
    h.stop.send(ShutdownMode::Handoff).unwrap();
    let outcome = tokio::time::timeout(Duration::from_secs(3), h.task).await???;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("unexpected cut outcome: {outcome:?}")
    };
    assert!(!report.ingress_drained && !report.cells_drained && !report.clean_reload_prepared);
    assert!(report.drain_timed_out);
    h.domain.shutdown().await;
    let _ = held.await?;
    let reopened = host("cut", directory.path()).await?;
    let response = client()?
        .get(format!("{}/read", reopened.origin))
        .send()
        .await?;
    assert_eq!(response.status(), 200);
    assert_eq!(response.text().await?, "cut:1");
    reopened.stop.send(ShutdownMode::Handoff).unwrap();
    stopped(reopened.task, ShutdownMode::Handoff).await?;
    reopened.domain.shutdown().await;
    Ok(())
}

#[test]
fn shutdown_policy_rejects_inconsistent_budgets_and_cli_settings_are_captured() -> anyhow::Result<()>
{
    const CHILD: &str = "CELLAR_SHUTDOWN_POLICY_ADAPTER_CHILD";
    if std::env::var(CHILD).as_deref() == Ok("1") {
        assert_eq!(ShutdownPolicy::default().total_ms, 40_000);
        let captured = ShutdownPolicy::from_env()?;
        assert_eq!(
            captured,
            ShutdownPolicy {
                total_ms: 8_000,
                token_wait_ms: 6_000,
                no_progress_ms: 5_000
            }
        );
        for invalid in ["0", "-5", "unusable"] {
            std::env::set_var("CELLAR_SHUTDOWN_TOTAL_MS", invalid);
            assert!(ShutdownPolicy::from_env().is_err());
            captured.validate()?;
        }
        return Ok(());
    }
    for invalid in [
        ShutdownPolicy {
            total_ms: 0,
            ..Default::default()
        },
        ShutdownPolicy {
            token_wait_ms: 40_001,
            ..Default::default()
        },
        ShutdownPolicy {
            no_progress_ms: 0,
            ..Default::default()
        },
        ShutdownPolicy {
            no_progress_ms: 40_001,
            ..Default::default()
        },
    ] {
        assert!(invalid.validate().is_err());
    }
    let output = std::process::Command::new(std::env::current_exe()?)
        .args(["--exact", "daemon::driver::tests::shutdown_policy_rejects_inconsistent_budgets_and_cli_settings_are_captured", "--nocapture"])
        .env_clear().env(CHILD, "1").env("CELLAR_SHUTDOWN_TOTAL_MS", "8000").output()?;
    anyhow::ensure!(
        output.status.success(),
        "shutdown policy adapter failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
