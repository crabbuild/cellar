//! Disposable owner-death and noncooperative-interpreter drills.
use super::*;
use std::{
    process::{Child, Command},
    sync::atomic::{AtomicUsize, Ordering},
    time::Instant,
};

const OWNER_LEASE_MS: u64 = 5_000;

struct FactoryProbe {
    application: Arc<PythonApplication>,
    created: AtomicUsize,
}

impl Application for FactoryProbe {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        if matches!(&event, Invocation::Fetch(request) if request.url.ends_with("/native-factory-count"))
        {
            return Box::pin(async move {
                let body = serde_json::to_vec(&self.created.load(Ordering::Acquire))?;
                Ok(Output::Http(
                    http::Response::new(
                        200,
                        body,
                        vec![("content-type".into(), "application/json".into())],
                    )
                    .into_http(&context)?,
                ))
            });
        }
        let native_path = match &event {
            Invocation::Fetch(request) if request.url.ends_with("/native-create") => {
                Some(("blocked-factory", "/"))
            }
            Invocation::Fetch(request) if request.url.ends_with("/native-alarm-state") => {
                Some(("one", "/native-alarm-state"))
            }
            _ => None,
        };
        if let Some((name, path)) = native_path {
            return Box::pin(async move {
                let response = context
                    .namespace("COUNTERS")?
                    .get_by_name(name)
                    .fetch(
                        &context,
                        Request {
                            url: format!("http://cell{path}"),
                            method: "GET".into(),
                            headers: Vec::new(),
                            body: Vec::new().into(),
                            entrypoint: None,
                        },
                    )
                    .await?;
                Ok(Output::Http(response))
            });
        }
        self.application.clone().invoke(context, event)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.application.supports_cell(class)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        let cell = self.application.create_cell(context, identity)?;
        self.created.fetch_add(1, Ordering::Release);
        Ok(Arc::new(AlarmProbe(cell)))
    }

    fn validate_deployment(&self, deployment: &cellar::fleet::LoadedDeployment) -> Result<()> {
        self.application.validate_deployment(deployment)
    }
}

struct AlarmProbe(Arc<dyn Cell>);

impl Cell for AlarmProbe {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        if matches!(&event, Invocation::Fetch(request) if request.url.ends_with("/native-alarm-state"))
        {
            // Observe the real retry row through a native cell turn and its
            // output proof, even while Python's loop/GIL cannot make progress.
            return Box::pin(async move {
                let body = serde_json::to_vec(&context.storage()?.get_alarm()?)?;
                Ok(Output::Http(
                    http::Response::new(200, body, Vec::new()).into_http(&context)?,
                ))
            });
        }
        self.0.clone().invoke(context, event)
    }
}

pub(super) fn child(mode: &str, root: &Path) -> Result<()> {
    // A blocked Python loop can prevent run() from returning after the native
    // host closes its listener. Preserve the fence reason in the failure log.
    tracing_subscriber::fmt()
        .with_env_filter("cellar::actor=warn,cellar::daemon::embedded=warn")
        .with_ansi(false)
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|error| anyhow::anyhow!("watchdog tracing: {error}"))?;
    let directory = root.to_path_buf();
    let recovered = mode == "--owner-recovered";
    let marker = directory.join("clean-shutdown.json");
    // Execute the shipped example's actual alarm method without requiring its
    // unrelated numerical/validation dependencies in this stdlib-only fixture.
    // The existing fixture owns routing and arms a durable business identity.
    let source = format!(
        r#"{}
import ast
_example_tree = ast.parse({})
_example_class = next(node for node in _example_tree.body if isinstance(node, ast.ClassDef) and node.name == "Counter")
_example_globals = {{"__name__": "qualified_example_alarm"}}
exec(compile(ast.Module(body=[_example_class], type_ignores=[]), "example_app.py", "exec"), _example_globals)
_example_alarm = _example_globals["Counter"].alarm
async def _observed_example_alarm(self, scheduled_ms, ctx):
    alarm_deliveries.append(scheduled_ms)
    return await _example_alarm(self, scheduled_ms, ctx)
Counter.alarm = _observed_example_alarm
"#,
        include_str!("../bridge.py"),
        serde_json::to_string(include_str!("../../../../examples/python/app.py"))?
    );
    let report = run(
        Options::default(),
        Source::new(source, "bridge.py")
            .events([EventKind::Fetch])
            .cell_events("Counter", [EventKind::Fetch, EventKind::Alarm]),
        move |application, stop| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let bucket = cellar::bucket::Bucket::open_dev(&directory.join("objects.sqlite"))?;
                let application = Arc::new(FactoryProbe {
                    application,
                    created: AtomicUsize::new(0),
                });
                let (node, url) = start_configured(
                    &directory.join("disk"),
                    application,
                    if recovered {
                        "recovered-python"
                    } else {
                        "killed-python"
                    },
                    Some(bucket),
                    move |config| {
                        // This fixture tests interpreter noncooperation, not
                        // subsecond lease renewal on a shared runner.
                        config.actor.lease_ttl_ms = OWNER_LEASE_MS;
                        config.startup.recovery_retry_backoff =
                            Duration::from_millis(OWNER_LEASE_MS);
                        // Both initial seeding and recovery include cold cell
                        // activation and Python mailbox round trips. Keep a
                        // finite native deadline without timing healthy work
                        // against this harness's unrelated 250 ms fixtures.
                        config.runtime.handler_budget = Duration::from_secs(2);
                    },
                )
                .await?;
                std::fs::write(directory.join("ready"), url)?;
                let monitor = stop.clone();
                let outcome = node
                    .run_until(async move {
                        stop.clone().cancelled().await;
                        ShutdownMode::Preserve
                    })
                    .await?;
                monitor.record_node(outcome.clone());
                // Complete shutdown evidence may only be written after the
                // joined node outcome and Python loop cleanup have both passed.
                ensure!(matches!(outcome, NodeOutcome::Stopped(_)));
                Ok(())
            })
        },
    )?;
    std::fs::write(marker, serde_json::to_vec(&report)?)?;
    Ok(())
}

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn(mode: &str, root: &Path) -> Result<Process> {
    let ready = root.join("ready");
    if ready.exists() {
        std::fs::remove_file(ready)?;
    }
    Ok(Process(
        Command::new(std::env::current_exe()?)
            .arg(mode)
            .arg(root)
            .spawn()?,
    ))
}

async fn fetch_json<T: serde::de::DeserializeOwned>(
    request: reqwest::RequestBuilder,
    stage: &str,
) -> Result<T> {
    let response = request.send().await.with_context(|| stage.to_string())?;
    let status = response.status();
    let body = response.bytes().await.with_context(|| stage.to_string())?;
    ensure!(
        status.is_success(),
        "{stage}: HTTP {status}: {}",
        String::from_utf8_lossy(&body[..body.len().min(512)])
    );
    serde_json::from_slice(&body).with_context(|| format!("{stage}: invalid JSON response"))
}

async fn ready(process: &mut Process, root: &Path, client: &reqwest::Client) -> Result<String> {
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut last_error = String::from("readiness file not written");
    loop {
        ensure!(
            process.0.try_wait()?.is_none(),
            "owner exited before readiness"
        );
        if let Ok(url) = std::fs::read_to_string(root.join("ready")) {
            match fetch_json::<String>(client.get(format!("{url}/library")), "owner readiness")
                .await
            {
                Ok(_) => return Ok(url),
                Err(error) => last_error = format!("{error:#}"),
            }
        }
        ensure!(
            Instant::now() < deadline,
            "owner readiness deadline elapsed: {last_error}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

pub(super) fn qualify() -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        for route in ["cpu-loop", "blocked-native"] {
            let directory = tempfile::tempdir()?;
            let root = directory.path();
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()?;
            let mut owner = spawn("--owner-running", root)?;
            let url = ready(&mut owner, root, &client).await
                .with_context(|| format!("{route}: initial owner startup"))?;
            let before: Value = fetch_json(
                client.post(format!("{url}/counter")),
                &format!("{route}: acknowledge initial counter"),
            )
            .await?;
            ensure!(
                before["count"] == 1,
                "write was not acknowledged before kill"
            );
            let alarm: i64 = fetch_json(
                client.post(format!("{url}/durable-alarm-arm")),
                &format!("{route}: acknowledge alarm intent"),
            )
            .await?;
            // Trigger noncooperation, verify it cannot fabricate a successful
            // heartbeat or shutdown, then exercise the external hard-stop path.
            let trigger = client.get(format!("{url}/{route}")).send().await;
            ensure!(
                trigger.is_err() || trigger?.status().is_server_error(),
                "blocking handler unexpectedly completed"
            );
            let heartbeat = client.get(format!("{url}/library")).send().await;
            ensure!(
                heartbeat.is_err() || heartbeat?.status().is_server_error(),
                "blocked interpreter answered heartbeat"
            );
            // Root routing and the real native cell factory must progress
            // without borrowing Python, even while its loop/GIL is blocked.
            let cold = client.get(format!("{url}/native-create")).send().await;
            ensure!(
                cold.is_err() || cold?.status().is_server_error(),
                "blocked Python cell handler unexpectedly completed"
            );
            let factories: usize = fetch_json(
                client.get(format!("{url}/native-factory-count")),
                &format!("{route}: native factory count while Python is blocked"),
            )
            .await?;
            ensure!(
                factories == 2,
                "native factory waited for blocked Python ({route}): {factories}"
            );
            ensure!(
                owner.0.try_wait()?.is_none(),
                "noncooperative runtime exited without watchdog"
            );
            // A wall-clock sleep does not prove the failed alarm's retry row
            // has committed. Observe that row through an independent native
            // turn before killing the owner and deleting all its local state.
            tokio::time::timeout(Duration::from_secs(15), async {
                loop {
                    let retry: Option<i64> = fetch_json(
                        client.get(format!("{url}/native-alarm-state")),
                        &format!("{route}: durable retry while Python is blocked"),
                    )
                    .await?;
                    if retry.is_some_and(|retry| retry > alarm) {
                        return Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .with_context(|| format!("{route}: alarm retry was not durably observed"))??;
            owner.0.kill()?;
            owner.0.wait()?;
            ensure!(
                !root.join("clean-shutdown.json").exists(),
                "hard kill fabricated clean drain"
            );
            // Remove every owner-local SQLite file. Recovery must use the fleet
            // bucket, rather than accidentally reading the killed owner's disk.
            std::fs::remove_dir_all(root.join("disk"))?;
            tokio::time::sleep(Duration::from_millis(OWNER_LEASE_MS + 100)).await;
            let mut recovery = spawn("--owner-recovered", root)?;
            let url = ready(&mut recovery, root, &client).await
                .with_context(|| format!("{route}: recovered owner startup"))?;
            let recovered: Value = fetch_json(
                client.post(format!("{url}/counter")),
                &format!("{route}: acknowledge recovered counter"),
            )
            .await?;
            ensure!(
                recovered["count"] == 2,
                "{route}: recovery lost acknowledged state: {recovered}"
            );
            let expected = json!({"intent":alarm,"receipt":true,"effects":1});
            let mut observed = Value::Null;
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    let mut state: Value = fetch_json(
                        client.get(format!("{url}/durable-alarm-state")),
                        &format!("{route}: recovered alarm state"),
                    )
                    .await?;
                    observed = state.clone();
                    let deliveries = state.as_object_mut().context("alarm state object")?
                        .remove("deliveries").context("alarm delivery timestamps")?;
                    if state == expected {
                        ensure!(deliveries.as_array().and_then(|rows|rows.first())
                            .and_then(Value::as_i64).is_some_and(|delivered|delivered > alarm),
                            "{route}: native retry did not change the delivery deadline: {deliveries}");
                        return Ok::<_, anyhow::Error>(());
                    }
                    ensure!(
                        state["intent"] == alarm
                            && state["effects"].as_u64().is_some_and(|count| count <= 1),
                        "{route}: recovered alarm lost atomic state or duplicated effects: {state}"
                    );
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .with_context(|| format!("acknowledged alarm did not recover: {observed}"))??;
            let mut replay: Value = fetch_json(
                client.get(format!("{url}/durable-alarm-repeat")),
                &format!("{route}: replay original alarm occurrence"),
            )
            .await?;
            replay.as_object_mut().context("alarm replay object")?.remove("deliveries");
            ensure!(
                replay == expected,
                "{route}: original alarm occurrence replay duplicated effects: {replay}"
            );
            cellar::cli_output::Output::new(cellar::cli_output::Format::Text).line(format_args!(
                "PASS Python watchdog ({route}): acknowledged state recovered after owner kill and disk deletion; durable alarm applied once and replay deduplicated"
            ))?;
        }
        Ok(())
    })
}
