// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Native timeout and process/disk loss must retain the business occurrence.
use super::*;
use std::{
    process::{Child, Command},
    time::Instant,
};

struct Controlled {
    application: Arc<dyn Application>,
    stop: Arc<tokio::sync::Notify>,
}

impl Application for Controlled {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        if matches!(&event, Invocation::Fetch(request) if request.url.ends_with("/native-stop")) {
            return Box::pin(async move {
                self.stop.notify_one();
                Ok(Output::Http(
                    http::Response::new(200, Vec::new(), Vec::new()).into_http(&context)?,
                ))
            });
        }
        self.application.clone().invoke(context, event)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.application.supports_cell(class)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        self.application.create_cell(context, identity)
    }

    fn validate_deployment(&self, deployment: &cellar::fleet::LoadedDeployment) -> Result<()> {
        self.application.validate_deployment(deployment)
    }
}

pub(super) fn child(mode: &str, root: &Path) -> Result<()> {
    let directory = root.to_path_buf();
    let name = format!("schedule-{mode}");
    let budget = if mode == "timeout" {
        Duration::from_millis(1500)
    } else {
        Duration::from_secs(30)
    };
    let source = format!(
        "FAULT={}\n{}",
        serde_json::to_string(mode)?,
        include_str!("schedule_recovery.py")
    );
    let report = run(
        Options::default(),
        Source::new(source, "schedule_recovery.py")
            .events([EventKind::Fetch, EventKind::Scheduled]),
        move |application, monitor| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let shutdown = Arc::new(tokio::sync::Notify::new());
                let application: Arc<dyn Application> = Arc::new(Controlled {
                    application: Arc::new(schedules::Observed(application)),
                    stop: shutdown.clone(),
                });
                let bucket = cellar::bucket::Bucket::open_dev(&directory.join("fleet.sqlite"))?;
                let (node, url) = start_graph(
                    &directory.join("disk"),
                    application,
                    &name,
                    Some(bucket),
                    move |config, _services, graph| {
                        config.runtime.handler_budget = budget;
                        config.actor.lease_ttl_ms = 3000;
                        config.startup.recovery_retry_backoff = Duration::from_secs(3);
                        graph.primary.options.bindings.clear();
                        graph.primary.options.do_classes = vec![cellar::deploy::SQL_CLASS.into()];
                        graph
                            .primary
                            .options
                            .sql_bindings
                            .push(("DB".into(), "recovery-db".into()));
                        graph.primary.crons = vec!["* * * * *".into()];
                    },
                )
                .await?;
                std::fs::write(directory.join("ready"), url)?;
                let outcome = node
                    .run_until(async move {
                        shutdown.notified().await;
                        ShutdownMode::Preserve
                    })
                    .await?;
                monitor.record_node(outcome.clone());
                ensure!(matches!(outcome, NodeOutcome::Stopped(_)));
                Ok(())
            })
        },
    )?;
    ensure!(
        report.complete() && report.bridge.operations.settled(),
        "cron owner did not drain: {report:?}"
    );
    std::fs::write(root.join("clean.json"), serde_json::to_vec(&report)?)?;
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
            .arg("--schedule-owner")
            .arg(mode)
            .arg(root)
            .spawn()?,
    ))
}

async fn value(client: &reqwest::Client, url: &str, route: &str) -> Result<Value> {
    Ok(client
        .get(format!("{url}{route}"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

async fn ready(process: &mut Process, root: &Path, client: &reqwest::Client) -> Result<String> {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        ensure!(
            process.0.try_wait()?.is_none(),
            "cron owner exited before readiness"
        );
        if let Ok(url) = std::fs::read_to_string(root.join("ready")) {
            if value(client, &url, "/ready").await.is_ok() {
                return Ok(url);
            }
        }
        ensure!(Instant::now() < deadline, "cron owner readiness elapsed");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn observed(
    client: &reqwest::Client,
    url: &str,
    timeout: Duration,
    check: impl Fn(&Value) -> bool,
) -> Result<Value> {
    let deadline = Instant::now() + timeout;
    let mut last = Value::Null;
    loop {
        // Only read-only state probes repeat through ownership/recovery refusal.
        if let Ok(state) = value(client, url, "/state").await {
            if check(&state) {
                return Ok(state);
            }
            last = state;
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "cron did not reach expected state: {last}; history={:?}",
                value(client, url, "/history").await
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn finish(
    process: &mut Process,
    root: &Path,
    client: &reqwest::Client,
    url: &str,
) -> Result<()> {
    // Submit shutdown once; the listener may close before its response arrives.
    let _ = client.post(format!("{url}/native-stop")).send().await;
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = process.0.try_wait()? {
            ensure!(status.success(), "cron owner failed shutdown: {status}");
            let report: cellar_python::Report =
                serde_json::from_slice(&std::fs::read(root.join("clean.json"))?)?;
            ensure!(
                report.complete() && report.bridge.operations.settled(),
                "cron did not join: {report:?}"
            );
            return Ok(());
        }
        ensure!(Instant::now() < deadline, "cron owner did not stop");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

pub(super) fn qualify() -> Result<()> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(async {
            // Each case uses real UTC and may wait a full calendar minute.
            // Keep independent node fixtures serial so runner contention cannot
            // expire their leases; do not jump the native retry/ownership clock.
            case("timeout").await?;
            case("crash").await?;
            Ok(())
        })
}

async fn case(mode: &str) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;
    let mut owner = spawn(mode, root)?;
    let mut url = ready(&mut owner, root, &client).await?;
    ensure!(value(&client, &url, "/setup").await? == true);
    let entered = observed(&client, &url, Duration::from_secs(75), |s| {
        s["entered"] == true
    })
    .await?;
    let original = entered["effects"][0]["occurrence"]
        .as_i64()
        .context("original occurrence")?;
    ensure!(
        entered["attempts"].as_array().is_some_and(|a| a.len() == 1),
        "first occurrence: {entered}"
    );
    if mode == "crash" {
        owner.0.kill()?;
        owner.0.wait()?;
        ensure!(
            !root.join("clean.json").exists(),
            "killed owner claimed drain"
        );
        std::fs::remove_dir_all(root.join("disk"))?;
        tokio::time::sleep(Duration::from_millis(3100)).await;
        owner = spawn("recovered", root)?;
        url = ready(&mut owner, root, &client).await?;
    }
    let settled = observed(&client, &url, Duration::from_secs(20), |s| {
        s["attempts"].as_array().is_some_and(|a| a.len() == 2)
    })
    .await?;
    ensure!(
        settled["effects"] == json!([{"cron":"* * * * *", "occurrence":original}])
            && settled["attempts"]
                == json!([
                    {"cron":"* * * * *", "occurrence":original, "attempt":1},
                    {"cron":"* * * * *", "occurrence":original, "attempt":2}])
            && settled["expired"] == json!([]),
        "cron replay changed identity/effects: {settled}"
    );
    if mode == "timeout" {
        ensure!(
            settled["cancelled"] == 1 && settled["refused"] == 2,
            "cron cancellation authority: {settled}"
        );
    }
    let history = value(&client, &url, "/history").await?;
    let items = history["items"]
        .as_array()
        .context("cron execution history")?;
    ensure!(
        items.len() == 2
            && items.iter().all(|r| r["scheduled_at"] == original)
            && items
                .iter()
                .any(|r| r["attempt"] == 1 && r["status"] == "started" && r["ended_at"].is_null())
            && items.iter().any(|r| r["attempt"] == 2
                && r["status"] == "completed"
                && r["ended_at"].is_number()),
        "cron lost interrupted history or invented completion ({mode}): {history}"
    );
    ensure!(
        history["active_alarm"]
            .as_i64()
            .is_some_and(|at| at > original),
        "cron did not resume calendar: {history}"
    );
    finish(&mut owner, root, &client, &url).await?;
    Ok(())
}
