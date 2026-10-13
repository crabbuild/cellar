//! Production worker and cell limits in independently configured library hosts.
#![allow(clippy::disallowed_methods)]
#[path = "support/html.rs"]
mod fixture;
use anyhow::{ensure, Result};
use cellar::engine_api::{HttpResponse, RuntimeFetch};
use cellar::generation::Generation;
use cellar::runtime::{RuntimeManager, StatelessRuntime};
use cellar::sdk::{
    Application, Cell, Context, ExecutionDomain, HandlerFuture, Invocation, Output, Registry,
    RuntimePolicy,
};
use cellar::worker_config::WorkerConfig;
use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};
use tokio::sync::{mpsc, Semaphore};

struct Signals {
    started: mpsc::UnboundedSender<()>,
    release: Semaphore,
}

fn handler(signals: Arc<Signals>, event: Invocation) -> HandlerFuture<Output> {
    Box::pin(async move {
        let Invocation::Fetch(request) = event else {
            return Ok(Output::Done);
        };
        if request.url.ends_with("/hold") {
            signals.started.send(())?;
            signals.release.acquire().await?.forget();
        }
        Ok(Output::Http(HttpResponse {
            status: 200,
            body: b"answered".to_vec(),
            headers: Vec::new(),
            stream: None,
            websocket: None,
            write_position: None,
            observed_position: None,
        }))
    })
}

struct WaitingCell(Arc<Signals>);

impl Cell for WaitingCell {
    fn invoke(self: Arc<Self>, _: Context, event: Invocation) -> HandlerFuture<Output> {
        handler(self.0.clone(), event)
    }
}

fn signals() -> (Arc<Signals>, mpsc::UnboundedReceiver<()>) {
    let (started, receive) = mpsc::unbounded_channel();
    (
        Arc::new(Signals {
            started,
            release: Semaphore::new(0),
        }),
        receive,
    )
}

fn registry(signals: Arc<Signals>) -> Result<Registry> {
    let cells = signals.clone();
    Registry::new(move |_, event| handler(signals.clone(), event)).cell("Waiting", move |_, _| {
        Ok(Arc::new(WaitingCell(cells.clone())))
    })
}

fn policy(requests: usize, cells: usize, budget: Duration) -> RuntimePolicy {
    RuntimePolicy {
        max_stateless_workers: cells,
        max_stateless_requests: Some(requests),
        max_cells_per_worker: cells,
        max_cell_requests: requests,
        admission_wait: Duration::ZERO,
        handler_budget: budget,
    }
}

async fn stateless(domain: &ExecutionDomain, signals: Arc<Signals>) -> Result<StatelessRuntime> {
    domain
        .run(async move {
            let config = WorkerConfig::new(fixture::graph("policy", "", vec![]).primary.options)
                .with_native_application(Arc::new(registry(signals)?));
            StatelessRuntime::start(Arc::new(config), "test".into(), "test".into())
        })
        .await?
}

fn fetch(
    runtime: &StatelessRuntime,
    path: &str,
) -> impl std::future::Future<Output = Result<HttpResponse>> + Send + 'static {
    runtime.fetch(
        format!("http://policy/{path}"),
        "GET".into(),
        Vec::new().into(),
        Vec::new(),
        None,
    )
}

async fn started(receive: &mut mpsc::UnboundedReceiver<()>) -> Result<()> {
    ensure!(tokio::time::timeout(Duration::from_secs(2), receive.recv())
        .await?
        .is_some());
    Ok(())
}

// A running turn plus a queued turn reaches the real pool's growth threshold.
// Keep the running turn on the blocking executor so the application's async
// executor remains free to admit the next event.
async fn saturated_worker_pool(
    domain: &ExecutionDomain,
    runtime: &StatelessRuntime,
    grows: bool,
) -> Result<()> {
    let slot = domain
        .run(async { runtime.isolates.admit(false).map(|a| a.slot().clone()) })
        .await??;
    let working = slot.clone();
    let (entered, receive) = tokio::sync::oneshot::channel();
    let (release, waiting) = std::sync::mpsc::channel();
    let active = domain.blocking(move || {
        tokio::runtime::Handle::current().block_on(working.turn(|_| {
            let _ = entered.send(());
            let _ = waiting.recv();
        }))
    })?;
    tokio::time::timeout(Duration::from_secs(2), receive).await??;
    let mut queued = Box::pin(slot.turn(|_| ()));
    std::future::poll_fn(|cx| {
        assert!(queued.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
    let admitted = domain
        .run(async { runtime.isolates.admit(false) })
        .await??;
    assert_eq!(admitted.slot().heap_id() != slot.heap_id(), grows);
    drop(admitted);
    drop(queued);
    release.send(())?;
    active.await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn separate_hosts_enforce_worker_admission_and_handler_budgets() -> Result<()> {
    let first = ExecutionDomain::new(tokio::runtime::Handle::current());
    let second = ExecutionDomain::new(tokio::runtime::Handle::current());
    first
        .services()
        .configure_runtime(policy(1, 1, Duration::from_millis(250)))?;
    second
        .services()
        .configure_runtime(policy(3, 2, Duration::from_secs(10)))?;
    let (signals_a, mut started_a) = signals();
    let (signals_b, mut started_b) = signals();
    let runtime_a = stateless(&first, signals_a).await?;
    let runtime_b = stateless(&second, signals_b.clone()).await?;

    // Actual pool admission and placement use each host's own bounds.
    let a = first
        .run(async { runtime_a.isolates.admit(false) })
        .await??;
    assert!(first
        .run(async { runtime_a.isolates.admit(false) })
        .await?
        .is_err());
    let b = second
        .run(async {
            Ok::<_, anyhow::Error>((
                runtime_b.isolates.admit(false)?,
                runtime_b.isolates.admit(false)?,
                runtime_b.isolates.admit(false)?,
            ))
        })
        .await??;
    drop((a, b));
    saturated_worker_pool(&first, &runtime_a, false).await?;
    saturated_worker_pool(&second, &runtime_b, true).await?;
    assert_eq!(runtime_a.isolates.len(), 1);
    assert_eq!(runtime_b.isolates.len(), 2);

    // Build the drive in a different domain, then run it in the slot's owner.
    // Choosing the budget from the construction scope would select ten seconds.
    let slot = first
        .run(async { runtime_a.isolates.admit(false).map(|a| a.slot().clone()) })
        .await??;
    let (reply, receive) = tokio::sync::oneshot::channel();
    #[allow(clippy::async_yields_async)]
    // Construction and execution deliberately use different domains.
    let driving = second
        .run(async move {
            cellar::runtime::drive(
                slot,
                cellar::WorkerJob::Fetch {
                    queued_at: std::time::Instant::now(),
                    entrypoint: None,
                    invocation_limits: None,
                    url: "http://policy/hold".into(),
                    method: "GET".into(),
                    body: Vec::new().into(),
                    headers: Vec::new(),
                    request_id: None,
                    tail_report: None,
                    reply,
                },
                None,
            )
        })
        .await?;
    let drive_a = first.spawn(driving)?;
    started(&mut started_a).await?;
    let pending_b1 = second.spawn(fetch(&runtime_b, "hold"))?;
    let pending_b2 = second.spawn(fetch(&runtime_b, "hold"))?;
    started(&mut started_b).await?;
    started(&mut started_b).await?;
    assert!(first.run(fetch(&runtime_a, "instant")).await?.is_err());
    assert_eq!(second.run(fetch(&runtime_b, "instant")).await??.status, 200);
    let error = tokio::time::timeout(Duration::from_secs(2), receive)
        .await??
        .err()
        .expect("the owning host must expire its handler");
    assert!(format!("{error:#}").contains("250ms budget"), "{error:#}");
    drive_a.await?;
    assert_eq!(
        runtime_b.isolates.census().requests,
        2,
        "the other host still owns two pending handlers"
    );
    signals_b.release.add_permits(2);
    assert_eq!(pending_b1.await??.status, 200);
    assert_eq!(pending_b2.await??.status, 200);
    assert_eq!(first.run(fetch(&runtime_a, "instant")).await??.status, 200);
    first.shutdown().await;
    assert_eq!(second.run(fetch(&runtime_b, "instant")).await??.status, 200);
    second.shutdown().await;
    Ok(())
}

async fn cells(
    domain: &ExecutionDomain,
    signals: Arc<Signals>,
    path: std::path::PathBuf,
) -> Result<RuntimeManager> {
    domain
        .run(async move {
            for cell in ["Waiting:one", "Waiting:two"] {
                std::fs::create_dir_all(path.join(cell).join("ltx/e1"))?;
            }
            let generation = Generation::build_native(
                1,
                fixture::graph("policy", "", vec!["Waiting".into()]),
                fixture::generation_options(),
                HashMap::from([(
                    "policy".into(),
                    Arc::new(registry(signals)?) as Arc<dyn Application>,
                )]),
            )?;
            RuntimeManager::start(generation, fixture::runtime_options(path))
        })
        .await?
}

fn request(path: &str) -> RuntimeFetch {
    RuntimeFetch {
        url: format!("http://policy/{path}"),
        method: "GET".into(),
        body: Vec::new().into(),
        headers: Vec::new(),
        request_id: None,
        order: None,
        parent: None,
    }
}

async fn cell_fetch(runtime: RuntimeManager) -> Result<HttpResponse> {
    runtime
        .fetch_cell("Waiting:one".into(), None, request("hold"), None)
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn separate_hosts_enforce_cell_admission_and_cells_per_worker() -> Result<()> {
    let first = ExecutionDomain::new(tokio::runtime::Handle::current());
    let second = ExecutionDomain::new(tokio::runtime::Handle::current());
    first
        .services()
        .configure_runtime(policy(1, 1, Duration::from_secs(10)))?;
    second
        .services()
        .configure_runtime(policy(2, 2, Duration::from_secs(10)))?;
    let (signals_a, mut started_a) = signals();
    let (signals_b, mut started_b) = signals();
    let directory = tempfile::tempdir()?;
    let runtime_a = cells(&first, signals_a.clone(), directory.path().join("a")).await?;
    let runtime_b = cells(&second, signals_b.clone(), directory.path().join("b")).await?;
    for (domain, runtime, share_worker) in
        [(&first, &runtime_a, false), (&second, &runtime_b, true)]
    {
        domain
            .run(async {
                let (one, _) = runtime.start_cell("Waiting:one".into(), 1, false).await?;
                let (two, _) = runtime.start_cell("Waiting:two".into(), 1, false).await?;
                assert_eq!(one == two, share_worker);
                runtime.publish_cell("Waiting:one", 1)?;
                runtime.publish_cell("Waiting:two", 1)
            })
            .await??;
    }
    let a = first.spawn(cell_fetch(runtime_a.clone()))?;
    let b1 = second.spawn(cell_fetch(runtime_b.clone()))?;
    started(&mut started_a).await?;
    started(&mut started_b).await?;
    let refused_a = first.run(cell_fetch(runtime_a.clone())).await??;
    assert_eq!(refused_a.status, 503);
    assert!(refused_a
        .headers
        .contains(&("x-cellar-overload".into(), "cell".into())));
    let b2 = second.spawn(cell_fetch(runtime_b.clone()))?;
    started(&mut started_b).await?;
    assert_eq!(
        second.run(cell_fetch(runtime_b.clone())).await??.status,
        503
    );
    signals_a.release.add_permits(1);
    signals_b.release.add_permits(2);
    for answered in [a.await??, b1.await??, b2.await??] {
        assert_eq!(answered.status, 200);
    }
    for (domain, runtime) in [(&first, &runtime_a), (&second, &runtime_b)] {
        domain
            .run(async {
                runtime.swap_out_cell("Waiting:one", 1).await?;
                runtime.swap_out_cell("Waiting:two", 1).await
            })
            .await??;
    }
    first.shutdown().await;
    second.shutdown().await;
    Ok(())
}

#[tokio::test]
async fn runtime_policy_rejects_invalid_limits_and_reconfiguration() -> Result<()> {
    let domain = ExecutionDomain::new(tokio::runtime::Handle::current());
    for invalid in [
        RuntimePolicy {
            max_stateless_workers: 0,
            ..Default::default()
        },
        RuntimePolicy {
            max_stateless_requests: Some(0),
            ..Default::default()
        },
        RuntimePolicy {
            max_cells_per_worker: 0,
            ..Default::default()
        },
        RuntimePolicy {
            max_cell_requests: 0,
            ..Default::default()
        },
        RuntimePolicy {
            handler_budget: Duration::ZERO,
            ..Default::default()
        },
    ] {
        assert!(domain.services().configure_runtime(invalid).is_err());
    }
    let valid = RuntimePolicy {
        max_stateless_requests: None,
        admission_wait: Duration::ZERO,
        ..Default::default()
    };
    domain.services().configure_runtime(valid)?;
    domain.services().configure_runtime(valid)?;
    assert_eq!(domain.services().runtime_policy(), valid);
    assert!(domain
        .services()
        .configure_runtime(RuntimePolicy::default())
        .is_err());
    let defaults = ExecutionDomain::new(tokio::runtime::Handle::current());
    assert_eq!(
        defaults.services().runtime_policy(),
        RuntimePolicy::default()
    );
    assert!(defaults.services().configure_runtime(valid).is_err());
    defaults.shutdown().await;
    domain.shutdown().await;
    assert!(domain.services().configure_runtime(valid).is_err());
    Ok(())
}

#[test]
fn library_runtime_policy_does_not_depend_on_process_environment() -> Result<()> {
    let output = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "separate_hosts_enforce_worker_admission_and_handler_budgets",
            "--nocapture",
        ])
        .env("CELLAR_MAX_STATELESS_ISOLATES", "unusable")
        .env("CELLAR_MAX_REQUESTS", "unusable")
        .env("CELLAR_MAX_CELL_REQUESTS", "unusable")
        .env("CELLAR_ADMISSION_WAIT_MS", "unusable")
        .env("CELLAR_HANDLER_BUDGET_S", "unusable")
        .output()?;
    ensure!(
        output.status.success(),
        "library runtime read the process environment:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

// Environment changes are confined to an exact-test subprocess.
#[test]
fn command_line_policy_resolves_environment_once() -> Result<()> {
    const CHILD: &str = "CELLAR_RUNTIME_POLICY_ADAPTER_CHILD";
    if std::env::var(CHILD).as_deref() == Ok("1") {
        let policy = RuntimePolicy::from_env()?;
        assert_eq!(
            policy,
            RuntimePolicy {
                max_stateless_workers: 2,
                max_stateless_requests: Some(7),
                max_cell_requests: 11,
                admission_wait: Duration::ZERO,
                handler_budget: Duration::from_secs(3),
                ..Default::default()
            }
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let domain = ExecutionDomain::new(runtime.handle().clone());
        domain.services().configure_runtime(policy)?;
        std::env::set_var("CELLAR_HANDLER_BUDGET_S", "unusable");
        assert!(RuntimePolicy::from_env().is_err());
        runtime.block_on(domain.run(async {
            assert_eq!(cellar::engine_api::handler_budget(), Duration::from_secs(3));
            assert_eq!(cellar::engine_api::admission_wait(), Duration::ZERO);
        }))?;
        runtime.block_on(domain.shutdown());
        return Ok(());
    }
    let output = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "command_line_policy_resolves_environment_once",
            "--nocapture",
        ])
        .env_clear()
        .env(CHILD, "1")
        .env("CELLAR_MAX_STATELESS_ISOLATES", "2")
        .env("CELLAR_MAX_REQUESTS", "7")
        .env("CELLAR_MAX_CELL_REQUESTS", "11")
        .env("CELLAR_ADMISSION_WAIT_MS", "0")
        .env("CELLAR_HANDLER_BUDGET_S", "3")
        .output()?;
    ensure!(
        output.status.success(),
        "CLI policy adapter failed:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}
