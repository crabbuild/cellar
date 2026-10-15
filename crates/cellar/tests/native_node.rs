//! Full public node bootstrap on caller-owned executors and infrastructure.
#![allow(clippy::disallowed_methods)]
#[allow(dead_code)]
#[path = "support/html.rs"]
mod fixture;
#[path = "support/node_assets.rs"]
mod node_assets;
#[path = "support/node_events.rs"]
mod node_events;
#[path = "support/node_http.rs"]
mod node_http;
use anyhow::{ensure, Context as _, Result};
use cellar::bucket::Bucket;
use cellar::engine_api::HttpResponse;
use cellar::sdk::*;
use std::{
    path::Path,
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};
use tokio::sync::{mpsc, Semaphore};

struct Executor(Option<tokio::runtime::Runtime>);

impl Executor {
    fn new(label: &str) -> Result<Self> {
        Ok(Self(Some(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .thread_name(label)
                .enable_all()
                .build()?,
        )))
    }

    fn handle(&self) -> tokio::runtime::Handle {
        self.0.as_ref().unwrap().handle().clone()
    }
}

impl Drop for Executor {
    fn drop(&mut self) {
        self.0.take().unwrap().shutdown_background();
    }
}

struct Signals {
    entered: mpsc::UnboundedSender<()>,
    release: Semaphore,
}

struct ReleaseFactoryOnDrop(Arc<(Mutex<bool>, Condvar)>);

impl Drop for ReleaseFactoryOnDrop {
    fn drop(&mut self) {
        *self.0 .0.lock().unwrap() = true;
        self.0 .1.notify_all();
    }
}

struct Account {
    label: &'static str,
    signals: Option<Arc<Signals>>,
}

fn response(text: impl ToString) -> Output {
    Output::Http(HttpResponse {
        status: 200,
        body: text.to_string().into_bytes(),
        headers: Vec::new(),
        stream: None,
        websocket: None,
        write_position: None,
        observed_position: None,
    })
}

impl Cell for Account {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            if request.url.ends_with("/hold") {
                let signals = self.signals.as_ref().unwrap();
                signals.entered.send(())?;
                signals.release.acquire().await?.forget();
            }
            let storage = context.storage()?;
            let mut count = storage
                .get_json("count")?
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            if request.method == "POST" {
                count += 1;
                storage.put_json("count", &serde_json::json!(count))?;
            }
            Ok(response(format!("{}:{count}", self.label)))
        })
    }
}

fn applications(
    label: &'static str,
    signals: Option<Arc<Signals>>,
) -> Result<CompiledApplications> {
    CompiledApplications::default().register("embedded-node", "test", move || {
        let signals = signals.clone();
        Ok(Arc::new(
            Registry::new(|context, event| {
                Box::pin(async move {
                    let Invocation::Fetch(request) = event else {
                        return Ok(Output::Done);
                    };
                    let cell = context
                        .namespace("ACCOUNTS")?
                        .get_by_name("same-business-id");
                    Ok(Output::Http(cell.fetch(&context, request).await?))
                })
            })
            .cell("Account", move |_, _| {
                Ok(Arc::new(Account {
                    label,
                    signals: signals.clone(),
                }))
            })?,
        ))
    })
}

struct BackgroundSignals {
    events: mpsc::UnboundedSender<&'static str>,
    first: Semaphore,
    nested: Semaphore,
}

fn background_applications(signals: Arc<BackgroundSignals>) -> Result<CompiledApplications> {
    CompiledApplications::default().register("embedded-node", "test", move || {
        let signals = signals.clone();
        Ok(Arc::new(
            Registry::new(move |context, event| {
                let signals = signals.clone();
                Box::pin(async move {
                    let Invocation::Fetch(request) = event else {
                        return Ok(Output::Done);
                    };
                    let background = context.clone();
                    context.wait_until(async move {
                        signals.events.send("entered")?;
                        signals.first.acquire().await?.forget();
                        let nested = background.clone();
                        background.wait_until(async move {
                            signals.events.send("nested")?;
                            signals.nested.acquire().await?.forget();
                            let cell = nested
                                .namespace("ACCOUNTS")?
                                .get_by_name("same-business-id");
                            let answer = cell.fetch(&nested, request).await?;
                            ensure!(answer.status == 200, "background cell write failed");
                            signals.events.send("written")?;
                            Ok(())
                        })?;
                        Ok(())
                    })?;
                    Ok(response("accepted"))
                })
            })
            .cell("Account", |_, _| {
                Ok(Arc::new(Account {
                    label: "background",
                    signals: None,
                }))
            })?,
        ))
    })
}

async fn background_event(
    events: &mut mpsc::UnboundedReceiver<&'static str>,
    expected: &'static str,
) -> Result<()> {
    let observed = tokio::time::timeout(Duration::from_secs(5), events.recv()).await?;
    ensure!(
        observed == Some(expected),
        "background event {observed:?}, expected {expected}"
    );
    Ok(())
}

struct AlarmBackground(Arc<BackgroundSignals>);

impl Cell for AlarmBackground {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let storage = context.storage()?;
            match event {
                Invocation::Fetch(request) => {
                    if request.url.ends_with("/arm") {
                        storage.set_alarm(0)?;
                        Ok(response("armed"))
                    } else {
                        Ok(response(storage.get_json("count")?.unwrap_or_default()))
                    }
                }
                Invocation::Alarm { .. } => {
                    let signals = self.0.clone();
                    context.wait_until(async move {
                        signals.events.send("entered")?;
                        signals.first.acquire().await?.forget();
                        storage.put_json("count", &serde_json::json!(1))?;
                        signals.events.send("written")?;
                        Ok(())
                    })?;
                    Ok(Output::Done)
                }
                _ => Ok(Output::Done),
            }
        })
    }
}

fn alarm_background_applications(signals: Arc<BackgroundSignals>) -> Result<CompiledApplications> {
    CompiledApplications::default().register("embedded-node", "test", move || {
        let signals = signals.clone();
        Ok(Arc::new(
            Registry::new(|context, event| {
                Box::pin(async move {
                    let Invocation::Fetch(request) = event else {
                        return Ok(Output::Done);
                    };
                    let cell = context
                        .namespace("ACCOUNTS")?
                        .get_by_name("same-business-id");
                    Ok(Output::Http(cell.fetch(&context, request).await?))
                })
            })
            .cell("Account", move |_, _| {
                Ok(Arc::new(AlarmBackground(signals.clone())))
            })?,
        ))
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_node_shutdown_drains_alarm_background_before_the_durability_cut() -> Result<()> {
    for mode in [ShutdownMode::Handoff, ShutdownMode::Preserve] {
        let directory = tempfile::tempdir()?;
        let core = Executor::new("application-core")?;
        let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
        let path = directory.path().join("alarm");
        let (events, mut received) = mpsc::unbounded_channel();
        let signals = Arc::new(BackgroundSignals {
            events,
            first: Semaphore::new(0),
            nested: Semaphore::new(0),
        });
        let (config, services) = inputs("alarm", &path, &core, Some(bucket.clone()), 0).await?;
        let node = Node::start(
            config,
            services,
            graph(),
            alarm_background_applications(signals.clone())?,
        )
        .await?;
        ready(&node).await?;
        let client = client()?;
        ensure!(
            call(&client, &node, reqwest::Method::POST, "/arm").await? == "armed",
            "alarm was not armed"
        );
        background_event(&mut received, "entered").await?;
        let owner = node.services();
        ensure!(
            owner.application_work().pending_events > 0,
            "alarm background lost its event lifetime"
        );
        let address = node.info().public_address;
        let mut stopping = tokio::spawn(node.shutdown(mode));
        let admission_closed = async {
            loop {
                let response = client.get(format!("http://{address}/read")).send().await?;
                if response.status() == 503 {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        cellar::asyncrt::select! {
            outcome = &mut stopping => anyhow::bail!("shutdown retired alarm background work: {outcome:?}"),
            closed = tokio::time::timeout(Duration::from_secs(5), admission_closed) => closed??,
        }
        signals.first.add_permits(1);
        background_event(&mut received, "written").await?;
        let outcome = tokio::time::timeout(Duration::from_secs(12), stopping).await???;
        let NodeOutcome::Stopped(report) = outcome else {
            anyhow::bail!("unexpected stop {outcome:?}");
        };
        ensure!(
            report.application_work_drained
                && report.pending_application_events == 0
                && report.cells_drained
                && !report.drain_timed_out,
            "alarm background drain incomplete: {report:?}"
        );
        ensure!(
            owner.application_work().pending_events == 0,
            "alarm event outlived its node"
        );
        let reopened = start("alarm", "alarm", &path, &core, Some(bucket), None, 0).await?;
        ready(&reopened).await?;
        ensure!(
            call(&client, &reopened, reqwest::Method::GET, "/read").await? == "alarm:1",
            "alarm background write was not retained across restart"
        );
        stopped(reopened).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_node_shutdown_drains_nested_background_across_generations_and_isolates_hosts(
) -> Result<()> {
    for mode in [ShutdownMode::Handoff, ShutdownMode::Preserve] {
        let directory = tempfile::tempdir()?;
        let core = Executor::new("application-core")?;
        let (events, mut received) = mpsc::unbounded_channel();
        let signals = Arc::new(BackgroundSignals {
            events,
            first: Semaphore::new(0),
            nested: Semaphore::new(0),
        });
        let path = directory.path().join("background");
        let mut bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
        bucket.prefix = "fleets/background/".into();
        let mut sibling_bucket = bucket.clone();
        sibling_bucket.prefix = "fleets/sibling/".into();
        let (config, services) =
            inputs("background", &path, &core, Some(bucket.clone()), 0).await?;
        let mut node = Node::start(
            config,
            services,
            graph(),
            background_applications(signals.clone())?,
        )
        .await?;
        let sibling = start(
            "sibling",
            "sibling",
            &directory.path().join("sibling"),
            &core,
            Some(sibling_bucket),
            None,
            0,
        )
        .await?;
        ready(&node).await?;
        ready(&sibling).await?;
        let client = client()?;
        ensure!(
            call(&client, &node, reqwest::Method::POST, "/background").await? == "accepted",
            "response did not precede background work"
        );
        background_event(&mut received, "entered").await?;
        let owner = node.services();
        ensure!(
            owner.application_work().pending_events > 0,
            "completed response lost its event lifetime"
        );
        node.adopt(graph(), applications("new", None)?).await?;
        let address = node.info().public_address;
        let mut stopping = tokio::spawn(node.shutdown(mode));
        let admission_closed = async {
            loop {
                let response = client.get(format!("http://{address}/read")).send().await?;
                if response.status() == 503 {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        };
        cellar::asyncrt::select! {
            outcome = &mut stopping => anyhow::bail!("shutdown retired accepted background work before its first stage: {outcome:?}"),
            closed = tokio::time::timeout(Duration::from_secs(5), admission_closed) => closed??,
        }
        signals.first.add_permits(1);
        cellar::asyncrt::select! {
            outcome = &mut stopping => anyhow::bail!("shutdown retired nested background work: {outcome:?}"),
            event = background_event(&mut received, "nested") => event?,
        }
        ensure!(
            owner.application_work().pending_events > 0,
            "nested work lost its event lifetime"
        );
        ensure!(
            call(&client, &sibling, reqwest::Method::POST, "/write").await? == "sibling:1",
            "another host stopped during background drain"
        );
        signals.nested.add_permits(1);
        background_event(&mut received, "written").await?;
        let outcome = tokio::time::timeout(Duration::from_secs(12), stopping).await???;
        let NodeOutcome::Stopped(report) = outcome else {
            anyhow::bail!("unexpected stop {outcome:?}");
        };
        ensure!(
            report.ingress_drained
                && report.application_work_drained
                && report.pending_application_events == 0
                && report.cells_drained
                && report.pending_dispatches == 0
                && !report.drain_timed_out,
            "background drain was incomplete: {report:?}"
        );
        ensure!(
            owner.application_work().pending_events == 0,
            "node completion preceded event retirement"
        );
        let reopened = start("background", "new", &path, &core, Some(bucket), None, 0).await?;
        ready(&reopened).await?;
        ensure!(
            call(&client, &reopened, reqwest::Method::GET, "/read").await? == "new:1",
            "background write was not retained across restart"
        );
        stopped(reopened).await?;
        stopped(sibling).await?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_node_deadline_reports_background_work_before_forced_retirement() -> Result<()> {
    for mode in [ShutdownMode::Handoff, ShutdownMode::Preserve] {
        let directory = tempfile::tempdir()?;
        let core = Executor::new("application-core")?;
        let (mut config, services) = inputs("cut", directory.path(), &core, None, 0).await?;
        config.shutdown = ShutdownPolicy {
            total_ms: 1_000,
            token_wait_ms: 0,
            no_progress_ms: 200,
        };
        let mut node =
            Node::start(config, services, graph(), applications("checkpoint", None)?).await?;
        let client = client()?;
        ensure!(
            call(&client, &node, reqwest::Method::POST, "/write").await? == "checkpoint:1",
            "checkpoint failed"
        );
        let (events, mut received) = mpsc::unbounded_channel();
        let signals = Arc::new(BackgroundSignals {
            events,
            first: Semaphore::new(0),
            nested: Semaphore::new(0),
        });
        node.adopt(graph(), background_applications(signals)?)
            .await?;
        let owner = node.services();
        ensure!(
            call(&client, &node, reqwest::Method::POST, "/background").await? == "accepted",
            "background response failed"
        );
        background_event(&mut received, "entered").await?;
        let outcome = tokio::time::timeout(Duration::from_secs(3), node.shutdown(mode)).await??;
        let NodeOutcome::Stopped(report) = outcome else {
            anyhow::bail!("unexpected stop {outcome:?}");
        };
        ensure!(
            report.ingress_drained
                && !report.application_work_drained
                && report.pending_application_events > 0
                && !report.cells_drained
                && report.drain_timed_out
                && !report.clean_reload_prepared,
            "forced stop claimed a background drain: {report:?}"
        );
        ensure!(
            owner.application_work().pending_events == 0,
            "supervisor failed to join retired events"
        );
        let reopened = start("cut", "checkpoint", directory.path(), &core, None, None, 0).await?;
        ensure!(
            call(&client, &reopened, reqwest::Method::GET, "/read").await? == "checkpoint:1",
            "deadline lost the committed checkpoint"
        );
        stopped(reopened).await?;
        ensure!(
            core.handle().spawn(async { 7 }).await? == 7,
            "deadline stopped the caller's executor"
        );
    }
    Ok(())
}

fn progressing_applications(
    confirmed: Arc<std::sync::atomic::AtomicU64>,
) -> Result<CompiledApplications> {
    CompiledApplications::default().register("embedded-node", "test", move || {
        let confirmed = confirmed.clone();
        Ok(Arc::new(
            Registry::new(move |context, event| {
                let confirmed = confirmed.clone();
                Box::pin(async move {
                    let Invocation::Fetch(request) = event else {
                        return Ok(Output::Done);
                    };
                    let background = context.clone();
                    context.wait_until(async move {
                        loop {
                            let cell = background
                                .namespace("ACCOUNTS")?
                                .get_by_name("same-business-id");
                            let answer = cell
                                .fetch(
                                    &background,
                                    Request {
                                        url: request.url.clone(),
                                        method: "POST".into(),
                                        headers: Vec::new(),
                                        body: Vec::new().into(),
                                        entrypoint: None,
                                    },
                                )
                                .await?;
                            let value: u64 = std::str::from_utf8(&answer.body)?
                                .split_once(':')
                                .unwrap()
                                .1
                                .parse()?;
                            confirmed.store(value, std::sync::atomic::Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    })?;
                    Ok(response("accepted"))
                })
            })
            .cell("Account", |_, _| {
                Ok(Arc::new(Account {
                    label: "progress",
                    signals: None,
                }))
            })?,
        ))
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn progressing_background_extends_the_stall_window_but_not_the_absolute_node_deadline(
) -> Result<()> {
    use std::sync::atomic::{AtomicU64, Ordering};
    let directory = tempfile::tempdir()?;
    let core = Executor::new("application-core")?;
    let confirmed = Arc::new(AtomicU64::new(0));
    let (mut config, services) = inputs("progress", directory.path(), &core, None, 0).await?;
    config.shutdown = ShutdownPolicy {
        total_ms: 2_500,
        token_wait_ms: 0,
        no_progress_ms: 800,
    };
    let node = Node::start(
        config,
        services,
        graph(),
        progressing_applications(confirmed.clone())?,
    )
    .await?;
    let client = client()?;
    ensure!(
        call(&client, &node, reqwest::Method::POST, "/background").await? == "accepted",
        "background response failed"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while confirmed.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    let owner = node.services();
    let before = owner.application_work();
    let started = tokio::time::Instant::now();
    let outcome =
        tokio::time::timeout(Duration::from_secs(5), node.shutdown(ShutdownMode::Handoff))
            .await??;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("unexpected stop {outcome:?}");
    };
    ensure!(
        started.elapsed() >= Duration::from_millis(2_300),
        "progress did not extend the stall window"
    );
    ensure!(
        report.deadline_elapsed
            && report.drain_timed_out
            && !report.application_work_drained
            && report.pending_application_events > 0,
        "progress bypassed or concealed the absolute deadline: {report:?}"
    );
    ensure!(
        owner.application_work().retired_events > before.retired_events + 3
            && owner.application_work().pending_events == 0,
        "completed SDK child events were not tracked"
    );
    let value = confirmed.load(Ordering::SeqCst);
    let reopened = start(
        "progress",
        "progress",
        directory.path(),
        &core,
        None,
        None,
        0,
    )
    .await?;
    let body = call(&client, &reopened, reqwest::Method::GET, "/read").await?;
    let restored: u64 = body.split_once(':').unwrap().1.parse()?;
    ensure!(
        restored >= value,
        "deadline lost an output-proven background write"
    );
    stopped(reopened).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn progressing_background_keeps_the_fleet_drain_token_renewed() -> Result<()> {
    use std::sync::atomic::{AtomicU64, Ordering};
    let directory = tempfile::tempdir()?;
    let core = Executor::new("application-core")?;
    let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let confirmed = Arc::new(AtomicU64::new(0));
    let (mut config, services) = inputs(
        "progress",
        &directory.path().join("node"),
        &core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    config.shutdown = ShutdownPolicy {
        total_ms: 75_000,
        token_wait_ms: 1_000,
        no_progress_ms: 5_000,
    };
    let node = Node::start(
        config,
        services,
        graph(),
        progressing_applications(confirmed.clone())?,
    )
    .await?;
    ready(&node).await?;
    let client = client()?;
    ensure!(
        call(&client, &node, reqwest::Method::POST, "/background").await? == "accepted",
        "background response failed"
    );
    tokio::time::timeout(Duration::from_secs(5), async {
        while confirmed.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("background work did not confirm its first write within five seconds")?;
    let owner = node.services();
    let reader = ExecutionDomain::new(tokio::runtime::Handle::current());
    let started = tokio::time::Instant::now();
    let mut stopping = tokio::spawn(node.shutdown(ShutdownMode::Handoff));
    let first_claim = async {
        loop {
            let current = reader
                .run(async { cellar::drain_token::read(&bucket).await })
                .await??;
            if let Some(token) = current.and_then(|current| current.token) {
                ensure!(token.node == "progress", "foreign drain token was acquired");
                return Ok::<_, anyhow::Error>(token.expires_ms);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    };
    let expires_ms = cellar::asyncrt::select! {
        outcome = &mut stopping => anyhow::bail!("shutdown ended before acquiring its token: {outcome:?}"),
        claim = tokio::time::timeout(Duration::from_secs(5), first_claim) =>
            claim.context("shutdown did not acquire its fleet drain token within five seconds")??,
    };
    let renewal = async {
        loop {
            let current = reader
                .run(async { cellar::drain_token::read(&bucket).await })
                .await??;
            if current
                .and_then(|current| current.token)
                .is_some_and(|token| {
                    token.node == "progress"
                        && token.expires_ms > expires_ms + cellar::drain_token::TOKEN_TTL_MS / 4
                })
            {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    };
    cellar::asyncrt::select! {
        outcome = &mut stopping => anyhow::bail!("shutdown ended before renewing its token: {outcome:?}"),
        renewed = tokio::time::timeout(Duration::from_secs(65), renewal) =>
            renewed.with_context(|| format!(
                "drain token was not renewed within 65 seconds; {} confirmed writes, pending work {:?}",
                confirmed.load(Ordering::SeqCst), owner.application_work(),
            ))??,
    }
    ensure!(
        owner.application_work().pending_events > 0 && confirmed.load(Ordering::SeqCst) > 3,
        "accepted work did not progress while holding the token"
    );
    let outcome = tokio::time::timeout(Duration::from_secs(25), stopping)
        .await
        .with_context(|| format!(
            "shutdown did not join within 25 seconds after token renewal; elapsed {:?}, {} confirmed writes, pending work {:?}",
            started.elapsed(), confirmed.load(Ordering::SeqCst), owner.application_work(),
        ))???;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("unexpected stop {outcome:?}");
    };
    ensure!(
        report.drain_timed_out
            && started.elapsed() >= Duration::from_secs(73)
            && !report.application_work_drained
            && report.pending_application_events > 0,
        "token renewal bypassed the absolute stop budget: {report:?}"
    );
    ensure!(
        owner.application_work().pending_events == 0,
        "unfinished events were not joined"
    );
    ensure!(
        reader
            .run(async { cellar::drain_token::read(&bucket).await })
            .await??
            .and_then(|current| current.token)
            .is_some_and(|token| token.node == "progress" && token.expires_ms == 0),
        "stop left its drain token held"
    );
    ensure!(
        core.handle().spawn(async { 7 }).await? == 7,
        "caller executor was closed"
    );
    reader.shutdown().await;
    Ok(())
}

fn graph() -> DeploymentGraph {
    let mut graph = fixture::graph("embedded-node", "", vec!["Account".into()]);
    graph.primary.options.bindings = vec![("ACCOUNTS".into(), "Account".into())];
    graph
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn public_node_applies_lifted_limits_to_cell_storage() -> Result<()> {
    struct WideSql;

    impl Cell for WideSql {
        fn invoke(self: Arc<Self>, context: Context, _: Invocation) -> HandlerFuture<Output> {
            Box::pin(async move {
                let sql = format!("SELECT {}", vec!["1"; 101].join(","));
                context.storage()?.sql(&sql, &[])?;
                Ok(response("lifted"))
            })
        }
    }
    let directory = tempfile::tempdir()?;
    let core = Executor::new("limits-core")?;
    let (mut config, services) = inputs("limits", directory.path(), &core, None, 0).await?;
    config.cell_limits = CellLimits::unrestricted();
    let applications = CompiledApplications::default().register("embedded-node", "test", || {
        Ok(Arc::new(
            Registry::new(|context, event| {
                Box::pin(async move {
                    let Invocation::Fetch(request) = event else {
                        return Ok(Output::Done);
                    };
                    let cell = context.namespace("ACCOUNTS")?.get_by_name("wide-sql");
                    Ok(Output::Http(cell.fetch(&context, request).await?))
                })
            })
            .cell("Account", |_, _| Ok(Arc::new(WideSql)))?,
        ))
    })?;
    let node = Node::start(config, services, graph(), applications).await?;
    ready(&node).await?;
    let result = call(&client()?, &node, reqwest::Method::GET, "/wide-sql").await;
    stopped(node).await?;
    assert_eq!(result?, "lifted");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_node_reports_missing_and_replaced_lease_authority() -> Result<()> {
    for reason in [
        FenceReason::NodeLeaseMissing,
        FenceReason::NodeLeaseMismatch,
    ] {
        let directory = tempfile::tempdir()?;
        let core = Executor::new("application-core")?;
        let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
        let (mut config, services) = inputs(
            "fenced",
            &directory.path().join("node"),
            &core,
            Some(bucket.clone()),
            0,
        )
        .await?;
        // Give the real store rejection and readback time to distinguish the
        // cause from expiry; do not change the ordinary lifecycle fixture.
        config.actor.lease_ttl_ms = 5_000;
        config.startup.recovery_retry_backoff = Duration::from_millis(5_000);
        let node = Node::start(config, services, graph(), applications("fenced", None)?).await?;
        ready(&node).await?;
        let owner = node.services();
        let faults = ExecutionDomain::new(tokio::runtime::Handle::current());
        faults
            .run(async {
                let key = "nodes/fenced.json";
                let (bytes, _) = bucket.get(key).await?.expect("published node lease");
                if reason == FenceReason::NodeLeaseMissing {
                    bucket.delete(key).await?;
                } else {
                    let mut lease: serde_json::Value = serde_json::from_slice(&bytes)?;
                    lease["generation"] = serde_json::json!("replacement-process");
                    bucket.put(key, serde_json::to_vec(&lease)?).await?;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
        faults.shutdown().await;
        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            node.run_until(std::future::pending()),
        )
        .await??;
        ensure!(
            outcome == NodeOutcome::Fenced { code: 3, reason },
            "lease fault returned {outcome:?}, expected {reason:?}"
        );
        ensure!(
            owner
                .configure_metrics(Arc::new(HostMetricsSample::default))
                .is_err(),
            "fencing returned before closing the node domain"
        );
        ensure!(
            core.handle().spawn(async { 7 }).await? == 7,
            "fencing stopped the caller's executor"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_node_reports_expired_authority_after_its_executor_resumes() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let core = Executor::new("application-core")?;
    let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let node = start(
        "paused",
        "paused",
        &directory.path().join("node"),
        &core,
        Some(bucket),
        None,
        0,
    )
    .await?;
    ready(&node).await?;
    let owner = node.services();
    let (entered, paused) = tokio::sync::oneshot::channel();
    // Suspend the caller's sole core worker beyond two lease lifetimes. A
    // renewal started before suspension cannot authorize work after resuming.
    let suspended = core.handle().spawn(async move {
        let _ = entered.send(());
        std::thread::sleep(Duration::from_millis(2_500));
    });
    paused.await?;
    let outcome = tokio::time::timeout(
        Duration::from_secs(10),
        node.run_until(std::future::pending()),
    )
    .await??;
    suspended.await?;
    ensure!(
        outcome
            == NodeOutcome::Fenced {
                code: 3,
                reason: FenceReason::NodeLeaseExpired
            },
        "executor suspension returned {outcome:?}"
    );
    ensure!(
        owner
            .configure_metrics(Arc::new(HostMetricsSample::default))
            .is_err(),
        "expiry returned before closing the node domain"
    );
    ensure!(
        core.handle().spawn(async { 7 }).await? == 7,
        "expiry stopped the caller's executor"
    );
    Ok(())
}

fn actor(generation: String) -> ActorOptions {
    ActorOptions {
        core: ActorPolicy {
            max_resident: 16,
            max_cached_routes: 0,
            max_activations: 8,
            max_evictions: 4,
            max_releases: 4,
            max_outbound_websockets: 8,
            ownership_on_evict: OwnershipOnEvict::Sticky,
            require_node_lease: true,
            peer_protocol: cellar::peer_auth::PROTOCOL_VERSION,
            operation_deadline_ms: Some(3_000),
            owner_log_recovery_backoff_ms: 50,
            owner_log_recovery_attempts: 3,
            alarm_resident_ms: 100,
            idle_evict_ms: None,
            pressure: Default::default(),
        },
        lease_ttl_ms: 1_000,
        placement_weight: 1,
        generation,
        resume_generation: None,
    }
}

async fn inputs(
    name: &str,
    directory: &Path,
    core: &Executor,
    bucket: Option<Bucket>,
    metrics: u64,
) -> Result<(NodeConfig, NodeServices)> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let internal_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let probe_signer = Arc::new(PeerProbeSigner::generate());
    let peer_auth = Arc::new(PeerAuth::new([11; 32], name)?);
    let peer_http = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(2))
        .tcp_nodelay(true)
        .build()?;
    let fleet = bucket
        .map(|bucket| -> Result<NodeFleet> {
            let storage = ReplicaStorage::new(
                bucket.store.clone(),
                ReplicaStorageOptions {
                    prefix: bucket.prefix.clone(),
                    ..Default::default()
                },
            )?;
            Ok(NodeFleet {
                lease_bucket: bucket.clone(),
                bucket,
                replica_storage: storage,
                replication: ReplicationOptions {
                    flush_interval: Duration::ZERO,
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
                fleet_durability: true,
            })
        })
        .transpose()?;
    let mut config = NodeConfig::new(
        ActorIdentity {
            node: name.into(),
            advertise: internal_listener.local_addr()?.to_string(),
            region: "test".into(),
        },
        actor(probe_signer.public_key_hex()),
        directory.to_path_buf(),
    );
    config.shutdown = ShutdownPolicy {
        total_ms: 8_000,
        token_wait_ms: 0,
        no_progress_ms: 5_000,
    };
    config.startup.bootstrap_timeout = Duration::from_secs(20);
    config.startup.recovery_attempt_timeout = Duration::from_secs(4);
    config.startup.authority_timeout = Duration::from_secs(4);
    config.maintenance.rebalance = false;
    config.maintenance.fleet_sample_period = Duration::from_millis(50);
    config.maintenance.load_sample_period = Duration::from_millis(50);
    config.generation_max_age = Duration::ZERO;
    let services = NodeServices {
        http_client: None,
        host_executor: tokio::runtime::Handle::current(),
        core_executor: core.handle(),
        listener,
        internal_listener,
        peer_http,
        peer_auth,
        probe_signer,
        fleet,
        containers: None,
        metrics: Some(Arc::new(move || HostMetricsSample {
            rss_bytes: metrics,
            in_use_bytes: metrics,
            fd_limit: 1024,
            ..Default::default()
        })),
    };
    Ok((config, services))
}

async fn start(
    name: &str,
    label: &'static str,
    directory: &Path,
    core: &Executor,
    bucket: Option<Bucket>,
    signals: Option<Arc<Signals>>,
    metrics: u64,
) -> Result<Node> {
    let (config, services) = inputs(name, directory, core, bucket, metrics).await?;
    Node::start(config, services, graph(), applications(label, signals)?)
        .await
        .with_context(|| format!("start node {name} in {}", directory.display()))
}

fn client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?)
}

async fn call(
    client: &reqwest::Client,
    node: &Node,
    method: reqwest::Method,
    path: &str,
) -> Result<String> {
    let response = client
        .request(
            method,
            format!("http://{}{path}", node.info().public_address),
        )
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(status == 200, "public node {path} status {status}: {body}");
    Ok(body)
}

async fn ready(node: &Node) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !node.is_ready().await? {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

async fn ready_with_completion(node: &mut Node, stage: &'static str) -> Result<()> {
    if let Err(error) = ready(node).await {
        let closed = node.is_closed();
        // Observe a completed supervisor once; never wait longer or turn a
        // failed readiness check into success. A pending join stays pending.
        let completion = if closed {
            format!(
                "{:?}",
                futures_util::FutureExt::now_or_never(node.finished())
            )
        } else {
            "not polled: domain remains open".into()
        };
        return Err(error).with_context(|| {
            format!("{stage}: domain_closed={closed}, supervisor_completion={completion}")
        });
    }
    Ok(())
}

async fn stopped(node: Node) -> Result<()> {
    let services = node.services();
    let outcome = tokio::time::timeout(
        Duration::from_secs(12),
        node.run_until(async { ShutdownMode::Handoff }),
    )
    .await??;
    let NodeOutcome::Stopped(report) = outcome else {
        anyhow::bail!("unexpected shutdown {outcome:?}");
    };
    ensure!(
        report.ingress_drained
            && report.application_work_drained
            && report.pending_application_events == 0
            && report.cells_drained
            && report.log_quiesced
            && report.local_durability_closed
            && report.pending_dispatches == 0
            && !report.drain_timed_out
            && !report.deadline_elapsed,
        "incomplete drain {report:?}"
    );
    ensure!(
        services
            .configure_metrics(Arc::new(HostMetricsSample::default))
            .is_err(),
        "completed node retained authority"
    );
    Ok(())
}

fn unhex(value: &str) -> Result<Vec<u8>> {
    ensure!(value.len().is_multiple_of(2), "invalid hex length");
    (0..value.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&value[i..i + 2], 16).map_err(Into::into))
        .collect()
}

async fn probe(client: &reqwest::Client, node: &Node) -> Result<()> {
    use ed25519_dalek::{Signature, VerifyingKey};
    let auth = PeerAuth::new([11; 32], "diagnostics")?;
    let challenge = "07".repeat(32);
    let request = auth.sign(
        client.get(format!(
            "http://{}/peer/probe",
            node.info().internal_address
        )),
        "GET",
        "/peer/probe",
        &[],
        &node.info().node,
    )?;
    let response = request
        .header("x-cells-probe-challenge", &challenge)
        .send()
        .await?;
    ensure!(
        response.status() == 200,
        "signed probe status {}",
        response.status()
    );
    let body: serde_json::Value = response.json().await?;
    ensure!(
        body["node"] == node.info().node
            && body["advertise"] == node.info().advertise
            && body["challenge"] == challenge,
        "probe identity crossed hosts"
    );
    let payload = format!(
        "cells-peer-probe-v1\n{}\n{}\n{challenge}",
        node.info().node,
        node.info().advertise
    );
    let key: [u8; 32] = unhex(&node.info().lease_generation)?.try_into().unwrap();
    let signature: [u8; 64] = unhex(body["signature"].as_str().unwrap())?
        .try_into()
        .unwrap();
    VerifyingKey::from_bytes(&key)?
        .verify_strict(payload.as_bytes(), &Signature::from_bytes(&signature))?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_nodes_drain_admitted_writes_isolate_fleets_and_leave_caller_executors_running(
) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let mut bucket_a = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    bucket_a.prefix = "fleets/a/".into();
    let mut bucket_b = bucket_a.clone();
    bucket_b.prefix = "fleets/b/".into();
    let core_a = Executor::new("application-core-a")?;
    let core_b = Executor::new("application-core-b")?;
    let (entered, mut events) = mpsc::unbounded_channel();
    let signals = Arc::new(Signals {
        entered,
        release: Semaphore::new(0),
    });
    let mut a = start(
        "same-node",
        "a",
        &directory.path().join("a"),
        &core_a,
        Some(bucket_a.clone()),
        Some(signals.clone()),
        101,
    )
    .await?;
    let mut b = start(
        "same-node",
        "b",
        &directory.path().join("b"),
        &core_b,
        Some(bucket_b),
        None,
        202,
    )
    .await?;
    ready_with_completion(&mut a, "initial fleet A readiness").await?;
    ready_with_completion(&mut b, "initial fleet B readiness").await?;
    ensure!(
        a.services().sample_metrics().rss_bytes == 101
            && b.services().sample_metrics().rss_bytes == 202,
        "caller metrics were not isolated"
    );
    let client = client()?;
    ensure!(
        a.info().lease_generation != b.info().lease_generation,
        "independent hosts shared a signing generation"
    );
    probe(&client, &a).await?;
    probe(&client, &b).await?;
    ensure!(
        call(&client, &b, reqwest::Method::POST, "/write").await? == "b:1",
        "fleet B state crossed"
    );
    let hold_client = client.clone();
    let address = a.info().public_address;
    let held = tokio::spawn(async move {
        hold_client
            .post(format!("http://{address}/hold"))
            .send()
            .await
    });
    tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await?
        .unwrap();
    let shutdown = tokio::spawn(stopped(a));
    ensure!(
        call(&client, &b, reqwest::Method::POST, "/write").await? == "b:2",
        "sibling stopped during drain"
    );
    signals.release.add_permits(1);
    let response = held.await??;
    ensure!(
        response.status() == 200 && response.text().await? == "a:1",
        "admitted write was cut off"
    );
    shutdown.await?.context("drain original fleet A")?;
    ensure!(
        core_a.handle().spawn(async { 7 }).await? == 7,
        "caller core executor stopped"
    );
    let mut reopened = start(
        "same-node",
        "a",
        &directory.path().join("a"),
        &core_a,
        Some(bucket_a),
        None,
        101,
    )
    .await?;
    ready_with_completion(&mut reopened, "restored fleet A readiness").await?;
    ensure!(
        call(&client, &reopened, reqwest::Method::POST, "/write").await? == "a:2",
        "committed fleet state was not restored"
    );
    stopped(reopened).await.context("drain restored fleet A")?;
    ensure!(
        call(&client, &b, reqwest::Method::POST, "/write").await? == "b:3",
        "sibling was closed"
    );
    stopped(b).await.context("drain fleet B")?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_nodes_adopt_compiled_generations_and_force_close_without_process_exit() -> Result<()>
{
    let directory = tempfile::tempdir()?;
    let core = Executor::new("application-core")?;
    let mut a = start("a", "a", directory.path(), &core, None, None, 0).await?;
    let client = client()?;
    ensure!(
        call(&client, &a, reqwest::Method::POST, "/write").await? == "a:1",
        "initial generation did not execute"
    );
    let (config, services) = inputs("other", directory.path(), &core, None, 0).await?;
    ensure!(
        Node::start(config, services, graph(), applications("other", None)?)
            .await
            .is_err(),
        "a live directory was shared by two nodes"
    );
    let failed = CompiledApplications::default().register("embedded-node", "test", || {
        anyhow::bail!("invalid replacement")
    })?;
    ensure!(
        a.adopt(graph(), failed).await.is_err(),
        "failed generation was adopted"
    );
    ensure!(
        call(&client, &a, reqwest::Method::GET, "/read").await? == "a:1",
        "failed adoption changed state"
    );
    ensure!(
        a.adopt(graph(), applications("new", None)?).await? == 2,
        "generation identity was reused"
    );
    let result = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let body = call(&client, &a, reqwest::Method::GET, "/read").await?;
            if body.starts_with("new:") {
                return Ok::<_, anyhow::Error>(body);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    ensure!(result == "new:1", "adoption lost committed state");
    let services = a.services();
    ensure!(
        a.cancel().await? == NodeOutcome::Cancelled,
        "forced stop claimed graceful completion"
    );
    ensure!(
        services
            .configure_peer_probe(Arc::new(PeerProbeSigner::generate()))
            .is_err(),
        "cancelled host retained authority"
    );
    let reopened = start("a", "new", directory.path(), &core, None, None, 0).await?;
    ensure!(
        call(&client, &reopened, reqwest::Method::GET, "/read").await? == "new:1",
        "forced closure lost SQLite state"
    );
    stopped(reopened).await?;
    ensure!(
        core.handle().spawn(async { 9 }).await? == 9,
        "caller executor was closed"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_bootstrap_retains_directory_until_started_application_factory_has_joined(
) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let core = Executor::new("application-core")?;
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let _release_on_failure = ReleaseFactoryOnDrop(gate.clone());
    let (entered, received) = std::sync::mpsc::channel();
    let factory_gate = gate.clone();
    let apps = CompiledApplications::default().register("embedded-node", "test", move || {
        entered.send(())?;
        let (lock, notify) = &*factory_gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = notify.wait(released).unwrap();
        }
        Ok(Arc::new(
            Registry::new(|_, _| Box::pin(async { Ok(Output::Done) })).cell(
                "Account",
                |_, _| {
                    Ok(Arc::new(Account {
                        label: "discarded",
                        signals: None,
                    }))
                },
            )?,
        ))
    })?;
    let (config, services) = inputs("cancelled", directory.path(), &core, None, 0).await?;
    let first_poll = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let polled = first_poll.clone();
    let mut startup = tokio::spawn(async move {
        polled.store(true, std::sync::atomic::Ordering::Release);
        Node::start(config, services, graph(), apps).await
    });
    let entry = tokio::task::spawn_blocking(move || received.recv_timeout(Duration::from_secs(5)))
        .await
        .context("factory-entry observer task failed")?;
    if let Err(error) = entry {
        let polled = first_poll.load(std::sync::atomic::Ordering::Acquire);
        // Observe completion before probing the lock. Keep any returned node
        // alive until this failed observation finishes, so dropping it cannot
        // change the directory state being reported.
        let result = futures_util::FutureExt::now_or_never(&mut startup);
        let completion = match &result {
            Some(Ok(Err(error))) => format!("startup failed: {error:#}"),
            Some(Err(error)) => format!("startup task failed: {error}"),
            Some(Ok(Ok(node))) => format!("startup returned a node: closed={}", node.is_closed()),
            None => "startup remains pending".into(),
        };
        let directory_state = match std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(directory.path().join("__sys_node.lock"))
        {
            Ok(lock) => match lock.try_lock() {
                Ok(()) => match lock.unlock() {
                    Ok(()) => "available".to_owned(),
                    Err(error) => format!("probe unlock failed: {error}"),
                },
                Err(std::fs::TryLockError::WouldBlock) => "held".into(),
                Err(std::fs::TryLockError::Error(error)) => format!("probe failed: {error}"),
            },
            Err(error) => format!("not opened: {error}"),
        };
        return Err(error).with_context(|| format!(
            "factory-entry wait failed: startup_polled={polled}, directory={directory_state}; {completion}",
        ));
    }
    startup.abort();
    ensure!(startup.await.is_err(), "startup was not cancelled");
    let (config, services) = inputs("replacement", directory.path(), &core, None, 0).await?;
    ensure!(
        Node::start(
            config,
            services,
            graph(),
            applications("replacement", None)?
        )
        .await
        .is_err(),
        "cancelled startup released a directory with running work"
    );
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    let restart_phase = Mutex::new(String::from("waiting for directory release"));
    let mut attempts = 0usize;
    let restarted = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            attempts += 1;
            // Observe the old directory lock independently of replacement
            // startup. This does not change its deadline or skip Node::start's
            // own exclusivity check; release any diagnostic lock immediately.
            let lock = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(directory.path().join("__sys_node.lock"))?;
            let available = match lock.try_lock() {
                Ok(()) => {
                    lock.unlock()?;
                    true
                }
                Err(std::fs::TryLockError::WouldBlock) => false,
                Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
            };
            *restart_phase.lock().unwrap() = format!(
                "attempt {attempts}: directory independently available={available}, replacement inputs pending"
            );
            let (config, services) =
                inputs("replacement", directory.path(), &core, None, 0).await?;
            *restart_phase.lock().unwrap() = format!(
                "attempt {attempts}: directory independently available={available}, replacement Node::start pending"
            );
            match Node::start(config, services, graph(), applications("replacement", None)?)
                .await
            {
                Ok(node) => return Ok::<_, anyhow::Error>(node),
                Err(error) if error.to_string().contains("directory is already owned") => {
                    *restart_phase.lock().unwrap() = format!(
                        "attempt {attempts}: old work still retains directory"
                    );
                    tokio::time::sleep(Duration::from_millis(10)).await
                }
                Err(error) => return Err(error),
            }
        }
    })
    .await
    .with_context(||format!("cancelled bootstrap recovery deadline: {}",restart_phase.lock().unwrap()))??;
    stopped(restarted).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replacement_public_node_recovers_predecessor_before_replacing_its_lease() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let core = Executor::new("application-core")?;
    let a = start(
        "leader",
        "leader",
        &directory.path().join("leader"),
        &core,
        Some(bucket.clone()),
        None,
        0,
    )
    .await?;
    let b = start(
        "follower",
        "follower",
        &directory.path().join("follower"),
        &core,
        Some(bucket.clone()),
        None,
        0,
    )
    .await?;
    ready(&a).await?;
    ready(&b).await?;
    let client = client()?;
    ensure!(
        call(&client, &a, reqwest::Method::POST, "/write").await? == "leader:1",
        "leader did not commit"
    );
    ensure!(
        call(&client, &b, reqwest::Method::GET, "/read").await? == "leader:1",
        "peer forwarding did not reach owner"
    );
    let old_generation = a.info().lease_generation.clone();
    a.cancel().await?;
    let recovered = start(
        "leader",
        "recovered",
        &directory.path().join("fresh-leader"),
        &core,
        Some(bucket.clone()),
        None,
        0,
    )
    .await?;
    ready(&recovered).await?;
    ensure!(
        recovered.info().lease_generation != old_generation,
        "replacement reused the old signing generation"
    );
    ensure!(
        call(&client, &recovered, reqwest::Method::GET, "/read").await? == "recovered:1",
        "replacement failed to recover a committed business cell"
    );
    stopped(recovered).await?;
    stopped(b).await?;
    Ok(())
}

#[test]
fn explicit_public_node_policy_ignores_invalid_process_configuration() -> Result<()> {
    let output = std::process::Command::new(std::env::current_exe()?)
        .args([
            "--exact",
            "public_nodes_drain_admitted_writes_isolate_fleets_and_leave_caller_executors_running",
            "--nocapture",
        ])
        .env("CELLAR_TTL_MS", "unusable")
        .env("CELLAR_OPERATION_DEADLINE_MS", "unusable")
        .env("CELLAR_SHUTDOWN_TOTAL_MS", "unusable")
        .env("CELLAR_ACTIVATIONS", "unusable")
        .env("CELLAR_MAX_CACHED_ROUTES", "unusable")
        .env("CELLAR_LOADER_MAX_WORKERS", "unusable")
        .env("CELLAR_MAX_STATELESS_REQUESTS", "unusable")
        .env("CELLAR_DURABILITY", "unusable")
        .env("CELLAR_REBALANCE_INTERVAL_MS", "unusable")
        .env("CELLAR_READY_FLEET_GATE_MS", "unusable")
        .env("CELLAR_LOG_PIPELINE", "unusable")
        .output()?;
    ensure!(
        output.status.success(),
        "explicit-policy child failed (the child output determines the cause):\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_predecessor_recovery_preserves_the_predecessor_generation_and_releases_the_directory(
) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let writer = ExecutionDomain::new(tokio::runtime::Handle::current());
    let prior = PeerProbeSigner::generate().public_key_hex();
    let record = serde_json::json!({
        "node":"unrecovered", "generation": prior, "probe_public_key":prior,
        "expires_ms":0, "addr":"127.0.0.1:9", "peer_protocol":cellar::peer_auth::PROTOCOL_VERSION, "paced_handoff":true,
        "log":{"state":"open", "epoch":2, "ensemble":["missing-follower"], "tiered":0, "may_have_fleet_acks":true, "claimant":null, "claimed_ms":null}
    });
    writer
        .run(async {
            cellar::wake_format::ensure_ready(&bucket).await?;
            bucket
                .put("nodes/unrecovered.json", serde_json::to_vec(&record)?)
                .await
        })
        .await??;
    let core = Executor::new("application-core")?;
    let path = directory.path().join("unrecovered");
    let (mut config, services) =
        inputs("unrecovered", &path, &core, Some(bucket.clone()), 0).await?;
    config.startup.recovery_attempts = 1;
    config.startup.recovery_attempt_timeout = Duration::from_millis(100);
    let result = Node::start(
        config,
        services,
        graph(),
        applications("unrecovered", None)?,
    )
    .await;
    ensure!(
        result.is_err(),
        "an unrecovered predecessor was overwritten"
    );
    let error = result.err().unwrap();
    ensure!(
        format!("{error:#}").contains("unrecovered predecessor")
            && format!("{error:#}").contains("no complete true witness"),
        "startup did not enforce the recovery gate: {error:#}"
    );
    let (body, _) = writer
        .run(async { bucket.get("nodes/unrecovered.json").await })
        .await??
        .unwrap();
    let preserved: serde_json::Value = serde_json::from_slice(&body)?;
    ensure!(
        preserved["generation"] == prior && preserved["addr"] == "127.0.0.1:9",
        "failed recovery installed a successor lease"
    );
    // A failed recovery must join its runtime before local storage can be reused.
    let local = start("local", "local", &path, &core, None, None, 0).await?;
    stopped(local)
        .await
        .context("local-only shutdown after failed predecessor bootstrap")?;
    writer.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conclusively_lost_fleet_proof_cannot_install_a_successor() -> Result<()> {
    for ensemble in [vec!["lost-follower"], Vec::new()] {
        let directory = tempfile::tempdir()?;
        let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
        let writer = ExecutionDomain::new(tokio::runtime::Handle::current());
        let prior = PeerProbeSigner::generate().public_key_hex();
        let record = serde_json::json!({
            "node":"lost-proof", "generation":prior, "probe_public_key":prior,
            "expires_ms":0, "addr":"127.0.0.1:9", "peer_protocol":cellar::peer_auth::PROTOCOL_VERSION, "paced_handoff":true,
            "log":{"state":"open", "epoch":2, "ensemble":ensemble, "tiered":0,
                "may_have_fleet_acks":true, "claimant":null, "claimed_ms":null}
        });
        // A persisted, long-expired member crosses the real conclusive-loss
        // branch immediately. An absent member stays undecided and does not cover it.
        let follower = serde_json::json!({
            "node":"lost-follower", "generation":"retained-follower-generation",
            "probe_public_key":"retained-follower-generation", "expires_ms":0,
            "addr":"127.0.0.1:9", "peer_protocol":cellar::peer_auth::PROTOCOL_VERSION, "paced_handoff":true
        });
        writer
            .run(async {
                cellar::wake_format::ensure_ready(&bucket).await?;
                bucket
                    .put("nodes/lost-proof.json", serde_json::to_vec(&record)?)
                    .await?;
                bucket
                    .put("nodes/lost-follower.json", serde_json::to_vec(&follower)?)
                    .await
            })
            .await??;
        let core = Executor::new("lost-proof-core")?;
        let path = directory.path().join("lost-proof");
        let (mut config, services) =
            inputs("lost-proof", &path, &core, Some(bucket.clone()), 0).await?;
        config.startup.recovery_attempts = 1;
        config.startup.recovery_attempt_timeout = Duration::from_secs(2);
        let error = match Node::start(config, services, graph(), applications("lost-proof", None)?)
            .await
        {
            Ok(node) => {
                stopped(node).await?;
                anyhow::bail!("lost acknowledged fleet proof was silently accepted as recovered");
            }
            Err(error) => error,
        };
        ensure!(
            format!("{error:#}").contains("no complete true witness"),
            "wrong refusal: {error:#}"
        );
        let (preserved, loss) = writer
            .run(async {
                let (body, _) = bucket
                    .get("nodes/lost-proof.json")
                    .await?
                    .context("predecessor disappeared")?;
                let value: serde_json::Value = serde_json::from_slice(&body)?;
                let loss = bucket
                    .get(&format!("log/lost-proof/{prior}.e2.loss.json"))
                    .await?;
                Ok::<_, anyhow::Error>((value, loss))
            })
            .await??;
        ensure!(
            preserved["generation"] == prior && preserved["log"]["state"] == "recovering",
            "lost proof sealed the predecessor or replaced its generation"
        );
        let (loss, _) = loss.context("conclusive loss was not recorded")?;
        let loss: serde_json::Value = serde_json::from_slice(&loss)?;
        ensure!(
            loss["recovery_blocked"] == true,
            "loss receipt authorized progress instead of recording a blocked recovery"
        );
        let local = start("local-after-loss", "local", &path, &core, None, None, 0).await?;
        stopped(local).await?;
        writer.shutdown().await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn management_authentication_generation_and_host_capabilities_are_enforced() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let core = Executor::new("management-test-core")?;
    let node = start(
        "managed-test",
        "management",
        directory.path(),
        &core,
        None,
        None,
        100,
    )
    .await?;
    ready(&node).await?;
    let http = client()?;
    let url = format!("http://{}/peer/management", node.info().internal_address);
    let unsigned = http.post(&url).body("{}").send().await?;
    ensure!(
        unsigned.status() == 401 || unsigned.status() == 403,
        "unsigned management request accepted"
    );
    let auth = PeerAuth::new([11; 32], "console-test")?;
    for (generation, action, status) in [
        ("stale-generation", "state", 409),
        (node.info().lease_generation.as_str(), "state", 200),
        (node.info().lease_generation.as_str(), "reload", 409),
    ] {
        let payload =
            serde_json::to_vec(&serde_json::json!({"generation":generation,"action":action}))?;
        let response = auth
            .sign(
                http.post(&url).body(payload.clone()),
                "POST",
                "/peer/management",
                &payload,
                &node.info().node,
            )?
            .send()
            .await?;
        ensure!(
            response.status().as_u16() == status,
            "unexpected management status {}",
            response.status()
        );
        if status == 200 {
            let body: serde_json::Value = response.json().await?;
            ensure!(
                body["result"]["generation"] == node.info().lease_generation,
                "generation crossed hosts"
            );
            ensure!(
                body["result"]["authoritative"] == true,
                "node did not report authority"
            );
            ensure!(
                body["result"]["capabilities"]["reload"] == false,
                "embedded host claimed a deployment watcher"
            );
        }
    }
    let payload = serde_json::to_vec(
        &serde_json::json!({"generation":node.info().lease_generation,"action":{"evict":{"scope":"Account:missing","epoch":999}}}),
    )?;
    let response = auth
        .sign(
            http.post(&url).body(payload.clone()),
            "POST",
            "/peer/management",
            &payload,
            &node.info().node,
        )?
        .send()
        .await?;
    ensure!(response.status() == 409, "absent epoch was admitted");
    let body: serde_json::Value = response.json().await?;
    ensure!(
        body["error"] == "epoch_changed",
        "wrong eviction refusal: {body}"
    );
    let response = http
        .get(format!("http://{}/", node.info().public_address))
        .send()
        .await?;
    ensure!(response.status().is_success(), "application request failed");
    response.bytes().await?;
    let payload = serde_json::json!({"generation":node.info().lease_generation,"action":{"cells":{"after":null,"limit":100}}}).to_string().into_bytes();
    let response = auth
        .sign(
            http.post(&url).body(payload.clone()),
            "POST",
            "/peer/management",
            &payload,
            &node.info().node,
        )?
        .send()
        .await?;
    ensure!(response.status() == 200, "owned-cell observation failed");
    let body: serde_json::Value = response.json().await?;
    let cells = body["result"]["items"]
        .as_array()
        .context("missing owned cells")?;
    ensure!(cells.len() == 1, "expected one owned cell: {body}");
    let payload = serde_json::json!({"generation":node.info().lease_generation,"action":{"evict":{"scope":cells[0]["scope"],"epoch":cells[0]["epoch"]}}}).to_string().into_bytes();
    let response = auth
        .sign(
            http.post(&url).body(payload.clone()),
            "POST",
            "/peer/management",
            &payload,
            &node.info().node,
        )?
        .send()
        .await?;
    let status = response.status();
    let body: serde_json::Value = response.json().await?;
    ensure!(
        status == 200 && body["result"]["completed"] == true,
        "eviction did not complete: {body}"
    );
    stopped(node).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn management_resource_writes_survive_owner_handoff() -> Result<()> {
    resource_writes_after_owner_stop(false).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn management_resource_writes_survive_forced_owner_closure() -> Result<()> {
    resource_writes_after_owner_stop(true).await
}

async fn resource_writes_after_owner_stop(abrupt: bool) -> Result<()> {
    use base64::Engine;
    use cellar::management::{EditExpiration, FleetClient, KvEdit, QueueAction, RevisionCondition};
    use serde_json::json;
    let directory = tempfile::tempdir()?;
    let management = ExecutionDomain::new(tokio::runtime::Handle::current());
    let bucket = management
        .run(async {
            let bucket = Bucket::open_dev(&directory.path().join("management-fleet.sqlite"))?;
            bucket
                .put(
                    "fleet/peer-auth.json",
                    json!({"version":1,"key":"0b".repeat(32)})
                        .to_string()
                        .into_bytes(),
                )
                .await?;
            Ok::<_, anyhow::Error>(bucket)
        })
        .await??;
    let core_a = Executor::new("management-fleet-a")?;
    let core_b = Executor::new("management-fleet-b")?;
    let resource_graph = || {
        let mut graph = graph();
        graph.primary.options.do_classes.extend([
            cellar::deploy::SQL_CLASS.into(),
            cellar::deploy::KV_CLASS.into(),
            cellar::deploy::QUEUE_CLASS.into(),
        ]);
        graph
            .primary
            .options
            .sql_bindings
            .push(("DB".into(), "dashboard-db".into()));
        graph
            .primary
            .options
            .kv_bindings
            .push(("KV".into(), "dashboard-kv".into()));
        graph
            .primary
            .options
            .queue_bindings
            .push(cellar::engine_api::QueueBinding {
                environment: "Q".into(),
                queue: "dashboard-queue".into(),
                delivery_delay: 0,
            });
        graph
    };
    let (config, services) = inputs(
        "dashboard-a",
        &directory.path().join("a"),
        &core_a,
        Some(bucket.clone()),
        0,
    )
    .await?;
    let a = Node::start(config, services, resource_graph(), applications("a", None)?).await?;
    let (config, services) = inputs(
        "dashboard-b",
        &directory.path().join("b"),
        &core_b,
        Some(bucket.clone()),
        0,
    )
    .await?;
    let b = Node::start(config, services, resource_graph(), applications("b", None)?).await?;
    ready(&a).await?;
    ready(&b).await?;
    let client = FleetClient::new(bucket, false);
    let (owner, revision) = management
        .run(async {
            client.sql(
                "dashboard-db",
                "CREATE TABLE receipts(id TEXT PRIMARY KEY); INSERT INTO receipts VALUES ('ack-before-handoff')",
                false,
            ).await?;
            client.kv_put(
                "dashboard-kv",
                "receipt",
                b"ack-before-handoff".to_vec(),
                None,
                None,
            ).await?;
            let paused = client.queue("dashboard-queue", QueueAction::Pause, 10).await?;
            ensure!(paused["paused"] == true, "queue pause was not acknowledged");
            let revision = client.kv_inspect("dashboard-kv", "receipt").await?["revision"]
                .as_str().context("KV revision absent")?.to_string();
            let scope = cellar::engine_api::kv_cell_scope(
                "dashboard-kv",
                cellar_logic::kv::shard_of("receipt", cellar_logic::kv::SHARDS),
            );
            let owner = client.cell(&scope).await?.owner.context("KV owner absent")?;
            Ok::<_, anyhow::Error>((owner, revision))
        })
        .await??;
    let (previous, remaining) = if owner == "dashboard-a" {
        (a, b)
    } else {
        ensure!(owner == "dashboard-b", "unexpected owner {owner}");
        (b, a)
    };
    if abrupt {
        ensure!(
            matches!(previous.cancel().await?, NodeOutcome::Cancelled),
            "forced close fabricated graceful proof"
        );
        // The private fixture uses a one-second node lease. Recovery must
        // wait for that authority to expire; no shutdown release was supplied.
        tokio::time::sleep(Duration::from_millis(1200)).await;
    } else {
        stopped(previous).await?;
    }
    management
        .run(async {
            let result = client
                .sql("dashboard-db", "SELECT id FROM receipts ORDER BY id", true)
                .await?;
            ensure!(
                result["results"][0]["rows"] == json!([["ack-before-handoff"]]),
                "acknowledged SQL receipt lost: {result}"
            );
            let schema = client.sql(
                "dashboard-db",
                "SELECT m.name,p.name,p.type,p.pk FROM (SELECT name FROM sqlite_schema WHERE type IN ('table','view') AND name NOT GLOB '__sys_*' AND name NOT GLOB 'sqlite_*' ORDER BY name LIMIT 100) AS m JOIN pragma_table_info(m.name) AS p ORDER BY m.name,p.cid LIMIT 1000",
                true,
            ).await?;
            ensure!(
                schema["results"][0]["rows"][0][0] == "receipts",
                "workbench schema query failed: {schema}"
            );
            let explain = client.sql(
                "dashboard-db", "EXPLAIN QUERY PLAN SELECT * FROM receipts", true,
            ).await?;
            ensure!(
                explain["results"][0]["columns"] == json!(["id", "parent", "notused", "detail"]),
                "query plan failed: {explain}"
            );
            let value = client.kv_get("dashboard-kv", "receipt").await?;
            ensure!(
                value["value"] == json!(b"ack-before-handoff".to_vec()),
                "acknowledged KV value lost: {value}"
            );
            ensure!(value["revision"] == revision, "handoff changed the inspected revision");
            let edit = KvEdit {
                value_base64: base64::engine::general_purpose::STANDARD.encode(b"ack-after-handoff"),
                tag: "bytes".into(),
                metadata: Some("null".into()),
                expiration: EditExpiration::Preserve,
                condition: RevisionCondition { revision: Some(revision.clone()) },
            };
            let applied = client.kv_edit("dashboard-kv", "receipt", edit.clone()).await?;
            ensure!(applied["ok"] == true, "successor rejected valid revision");
            let stale = client.kv_edit("dashboard-kv", "receipt", edit).await?;
            ensure!(stale["conflict"] == true, "successor accepted stale revision");
            ensure!(
                client
                    .queue("dashboard-queue", QueueAction::Info, 10)
                    .await?["paused"]
                    == true,
                "queue pause lost"
            );
            client
                .sql(
                    "dashboard-db",
                    "INSERT INTO receipts VALUES ('ack-after-handoff')",
                    false,
                )
                .await?;
            let result = client
                .sql("dashboard-db", "SELECT count(*) FROM receipts", true)
                .await?;
            ensure!(
                result["results"][0]["rows"] == json!([[2]]),
                "successor did not accept SQL write"
            );
            let current = client.kv_inspect("dashboard-kv", "receipt").await?;
            let stale = client.kv_delete_if(
                "dashboard-kv", "receipt",
                RevisionCondition { revision: Some(revision.clone()) },
            ).await?;
            ensure!(stale["conflict"] == true, "stale delete accepted");
            let deleted = client.kv_delete_if(
                "dashboard-kv", "receipt",
                RevisionCondition { revision: current["revision"].as_str().map(str::to_string) },
            ).await?;
            ensure!(deleted["ok"] == true, "conditional delete failed");
            client
                .queue("dashboard-queue", QueueAction::Resume, 10)
                .await?;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    stopped(remaining).await?;
    management.shutdown().await;
    ensure!(
        core_a.handle().spawn(async { 1 }).await? == 1
            && core_b.handle().spawn(async { 2 }).await? == 2,
        "caller executors did not survive"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn management_crons_report_pending_until_embedded_host_adopts() -> Result<()> {
    use cellar::management::{FleetClient, PageRequest};
    use serde_json::json;
    let directory = tempfile::tempdir()?;
    let management = ExecutionDomain::new(tokio::runtime::Handle::current());
    let (bucket, manifest, prefix) = management.run(async {
        let bucket = Bucket::open_dev(&directory.path().join("cron-fleet.sqlite"))?;
        bucket.put("fleet/peer-auth.json", json!({"version":1,"key":"0b".repeat(32)}).to_string().into_bytes()).await?;
        let config: cellar::native_deploy::ApplicationConfig = serde_json::from_value(json!({
            "script_name":"embedded-node","application_version":"test","cells":["Account"],
            "bindings":[{"type":"durable_object_namespace","name":"ACCOUNTS","class_name":"Account"}],
            "crons":["0 0 1 1 *"]
        }))?;
        let artifact = cellar::native_deploy::executable_identity(&std::env::current_exe()?, cellar::native_deploy::current_target())?;
        let manifest = config.manifest(vec![artifact])?;
        let prefix = format!("deploy/embedded-node/{}",manifest.version);
        bucket.put(&format!("{prefix}/manifest.json"), serde_json::to_vec(&manifest)?).await?;
        let pointer = json!({"script_name":"embedded-node","version":manifest.version,"prefix":prefix,"rollout":{"percent":100}});
        for key in ["deploy/embedded-node/current.json", "deploy/current.json"] {
            bucket.put(key,serde_json::to_vec(&pointer)?).await?;
        }
        Ok::<_, anyhow::Error>((bucket,manifest,prefix))
    }).await??;
    let core = Executor::new("cron-management-core")?;
    let (config, services) = inputs(
        "cron-node",
        &directory.path().join("node"),
        &core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    let mut initial = graph();
    initial.primary.native = manifest.native;
    initial.primary.version = manifest.version;
    initial.primary.prefix = prefix;
    initial.primary.crons = manifest.crons;
    let mut node = Node::start(config, services, initial, applications("cron", None)?).await?;
    ready(&node).await?;
    let anonymous = reqwest::Client::new()
        .post(format!(
            "http://{}/do/.cron:embedded-node",
            node.info().internal_address
        ))
        .json(&json!({"op":"history","query":{}}))
        .send()
        .await?;
    ensure!(
        anonymous.status() == reqwest::StatusCode::FORBIDDEN,
        "anonymous cron inspection was not refused"
    );
    let client = FleetClient::new(bucket.clone(), false);
    let saved = management
        .run(async {
            let empty = client.cron_history("embedded-node", None, 50, None).await?;
            ensure!(
                empty["items"] == json!([]),
                "future schedule fabricated execution history"
            );
            ensure!(
                empty["active_alarm"].is_number(),
                "inspection lost the active schedule observation"
            );
            let before = client.cron_configuration("embedded-node").await?;
            let before_nodes = client
                .cron_propagation("embedded-node", PageRequest::default())
                .await?;
            ensure!(
                before_nodes.items.len() == 1 && before_nodes.items[0].status == "adopted",
                "initial schedule observation did not match declarations"
            );
            let saved = client
                .update_crons("embedded-node", &before.deployment_revision, None, vec![])
                .await?;
            let pending = client
                .cron_propagation("embedded-node", PageRequest::default())
                .await?;
            ensure!(
                pending.items.len() == 1 && pending.items[0].status == "pending",
                "embedded host silently adopted an external configuration write"
            );
            Ok::<_, anyhow::Error>(saved)
        })
        .await??;
    let graph = management
        .run(async {
            let cache = cellar::sdk::AssetCache::new(cellar::sdk::AssetCacheOptions::new(
                directory.path().join("assets"),
            ))?;
            DeploymentGraph::load(&bucket, "cron-node".into(), &cache).await
        })
        .await??;
    node.adopt(graph, applications("cron", None)?).await?;
    management
        .run(async {
            let adopted = client
                .cron_propagation("embedded-node", PageRequest::default())
                .await?;
            ensure!(
                adopted.items.len() == 1 && adopted.items[0].status == "adopted",
                "node did not report adopted configuration"
            );
            ensure!(
                adopted.items[0].observed_revision == saved.revision,
                "node reported the wrong cron revision"
            );
            let empty = client.cron_history("embedded-node", None, 50, None).await?;
            ensure!(
                empty["items"] == json!([]),
                "adoption fabricated a completed execution"
            );
            ensure!(
                cellar::deploy::is_reserved_scope(".cron:embedded-node"),
                "cron inspection exposed an anonymous cell route"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    stopped(node).await?;
    management.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn management_stop_receipts_distinguish_draining_joined_and_incomplete_work() -> Result<()> {
    use cellar::management::{FleetClient, NodeAction, StopMode};
    use serde_json::json;
    let directory = tempfile::tempdir()?;
    let management = ExecutionDomain::new(tokio::runtime::Handle::current());
    for scenario in ["handoff", "preserve", "cancel", "deadline"] {
        let bucket = management
            .run(async {
                let bucket =
                    Bucket::open_dev(&directory.path().join(format!("{scenario}.sqlite")))?;
                bucket
                    .put(
                        "fleet/peer-auth.json",
                        json!({"version":1,"key":"0b".repeat(32)})
                            .to_string()
                            .into_bytes(),
                    )
                    .await?;
                Ok::<_, anyhow::Error>(bucket)
            })
            .await??;
        let core = Executor::new("stop-receipt-core")?;
        let (mut config, services) = inputs(
            scenario,
            &directory.path().join(scenario),
            &core,
            Some(bucket.clone()),
            0,
        )
        .await?;
        if scenario == "deadline" {
            config.shutdown = ShutdownPolicy {
                total_ms: 300,
                token_wait_ms: 0,
                no_progress_ms: 150,
            };
        }
        let (events, mut received) = mpsc::unbounded_channel();
        let signals = Arc::new(BackgroundSignals {
            events,
            first: Semaphore::new(0),
            nested: Semaphore::new(0),
        });
        let compiled = if scenario == "preserve" {
            applications("preserve", None)?
        } else {
            background_applications(signals.clone())?
        };
        let node = Node::start(config, services, graph(), compiled).await?;
        ready(&node).await?;
        let http = client()?;
        call(&http, &node, reqwest::Method::POST, "/receipt-write").await?;
        if scenario != "preserve" {
            background_event(&mut received, "entered").await?;
        }
        let generation = node.info().lease_generation.clone();
        let peer_address = node.info().internal_address;
        let client = FleetClient::new(bucket, false);
        let mode = if scenario == "preserve" {
            StopMode::Preserve
        } else {
            StopMode::Handoff
        };
        management
            .run(async {
                let result = client
                    .node_action(
                        scenario,
                        &generation,
                        NodeAction::Stop {
                            operation_id: "stop-1".into(),
                            mode,
                        },
                    )
                    .await?;
                ensure!(
                    result["receipt"]["phase"] == "accepted",
                    "stop did not return acceptance"
                );
                ensure!(
                    result["receipt"]["domain_joined"] == false,
                    "acceptance claimed joined work"
                );
                if scenario == "handoff" {
                    let receipt = client
                        .stop_receipt(scenario, &generation, "stop-1")
                        .await?
                        .unwrap();
                    ensure!(
                        !receipt.domain_joined && receipt.phase != "complete",
                        "pending application work was reported complete"
                    );
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
        if scenario == "handoff" {
            signals.first.add_permits(1);
            signals.nested.add_permits(1);
            background_event(&mut received, "nested").await?;
            background_event(&mut received, "written").await?;
        }
        let outcome = tokio::time::timeout(Duration::from_secs(15), async {
            if scenario == "cancel" {
                node.cancel().await
            } else {
                node.run_until(std::future::pending::<ShutdownMode>()).await
            }
        })
        .await??;
        if scenario == "cancel" {
            ensure!(
                matches!(outcome, NodeOutcome::Cancelled),
                "cancelled node fabricated a stopped outcome"
            );
        }
        management
            .run(async {
                let receipt = client
                    .stop_receipt(scenario, &generation, "stop-1")
                    .await?
                    .unwrap();
                ensure!(
                    receipt.domain_joined,
                    "final receipt was published before join"
                );
                match scenario {
                    "handoff" | "preserve" => {
                        ensure!(
                            receipt.phase == "complete",
                            "expected complete stop receipt: {receipt:?}"
                        );
                        ensure!(
                            receipt
                                .proof
                                .as_ref()
                                .is_some_and(|proof| proof.complete(mode)),
                            "stop has incomplete proof"
                        );
                    }
                    "cancel" => ensure!(
                        receipt.phase == "failed" && receipt.proof.is_none(),
                        "forced closure fabricated graceful proof: {receipt:?}"
                    ),
                    "deadline" => ensure!(
                        receipt.phase == "incomplete"
                            && receipt
                                .proof
                                .as_ref()
                                .is_some_and(|proof| proof.drain_timed_out
                                    && !proof.application_work_drained),
                        "deadline erased pending work: {receipt:?}"
                    ),
                    _ => unreachable!(),
                }
                Ok::<_, anyhow::Error>(())
            })
            .await??;
        ensure!(
            http.get(format!("http://{peer_address}/peer/probe"))
                .send()
                .await
                .is_err(),
            "finished node retained its peer listener"
        );
        ensure!(
            core.handle().spawn(async { 7 }).await? == 7,
            "node stopped the caller executor"
        );
    }
    management.shutdown().await;
    Ok(())
}

struct AlarmResident;

impl Cell for AlarmResident {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            let storage = context.storage()?;
            let count = storage
                .get_json("count")?
                .and_then(|v| v.as_u64())
                .unwrap_or(0)
                + 1;
            storage.put_json("count", &serde_json::json!(count))?;
            if request.url.ends_with("/armed") {
                storage.set_alarm(cellar::asyncrt::wall_ms() + 60_000)?;
            }
            Ok(response(count))
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_node_admits_cold_cells_past_a_covered_alarm() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let core = Executor::new("alarm-admission-core")?;
    let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let (mut config, services) = inputs(
        "alarm-admission",
        &directory.path().join("state"),
        &core,
        Some(bucket),
        0,
    )
    .await?;
    config.actor.core.max_resident = 2;
    config.actor.core.max_activations = 2;
    config.actor.core.max_evictions = 1;
    config.actor.core.alarm_resident_ms = 3_600_000;
    config.actor.core.operation_deadline_ms = Some(1_000);
    config.runtime.handler_budget = Duration::from_secs(4);
    let applications = CompiledApplications::default().register("embedded-node", "test", || {
        Ok(Arc::new(
            Registry::new(|context, event| {
                Box::pin(async move {
                    let Invocation::Fetch(request) = event else {
                        return Ok(Output::Done);
                    };
                    let name = request.url.rsplit('/').next().unwrap_or("new");
                    let cell = context.namespace("ACCOUNTS")?.get_by_name(name);
                    Ok(Output::Http(cell.fetch(&context, request).await?))
                })
            })
            .cell("Account", |_, _| Ok(Arc::new(AlarmResident)))?,
        ))
    })?;
    let node = Node::start(config, services, graph(), applications).await?;
    ready(&node).await?;
    let client = client()?;
    let result = async {
        ensure!(
            call(&client, &node, reqwest::Method::GET, "/armed").await? == "1",
            "alarm cell failed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        ensure!(
            call(&client, &node, reqwest::Method::GET, "/idle").await? == "1",
            "idle cell failed"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
        ensure!(
            call(&client, &node, reqwest::Method::GET, "/new")
                .await
                .map_err(|error| error.context("cold request at full residency"))?
                == "1",
            "cold cell was not admitted"
        );
        ensure!(
            call(&client, &node, reqwest::Method::GET, "/armed").await? == "2",
            "alarm cell write lost across eviction"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let outcome = node.shutdown(ShutdownMode::Preserve).await?;
    result?;
    ensure!(
        matches!(outcome, NodeOutcome::Stopped(_)),
        "unexpected stop {outcome:?}"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_workflow_deletion_preserves_gated_handler_completion() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let client = client()?;
    let mut nodes = Vec::new();
    let mut cores = Vec::new();
    for index in 0..3 {
        let core = Executor::new("workflow-rpc-core")?;
        let (config, services) = inputs(
            &format!("workflow-node-{index}"),
            &directory.path().join(format!("node-{index}")),
            &core,
            Some(bucket.clone()),
            100,
        )
        .await?;
        let mut graph = fixture::graph(
            "embedded-node",
            "",
            vec![cellar::deploy::workflow_class("embedded-node")],
        );
        graph.primary.options.workflow_bindings = vec![cellar::engine_api::WorkflowBinding {
            environment: "REPAIR".into(),
            workflow: "repair".into(),
            class: "Repair".into(),
            settings: Default::default(),
        }];
        let applications =
            CompiledApplications::default().register("embedded-node", "test", || {
                Ok(Arc::new(
                    Registry::new(|context, event| {
                        Box::pin(async move {
                            let Invocation::Fetch(request) = event else {
                                return Ok(Output::Done);
                            };
                            let workflow = context.workflow("REPAIR")?;
                            let instance = if request.url.ends_with("/create") {
                                workflow
                                    .create(
                                        &context,
                                        workflow::CreateOptions {
                                            id: Some("same-repair".into()),
                                            ..Default::default()
                                        },
                                    )
                                    .await?
                            } else {
                                workflow.get(&context, "same-repair").await?
                            };
                            let scope = instance.scope();
                            let value = if request.url.ends_with("/delete") {
                                instance.delete(&context).await?;
                                serde_json::json!({"deleted":true,"scope":scope})
                            } else {
                                serde_json::json!({"scope":scope})
                            };
                            Ok(Output::Http(
                                http::Response::from_json(&value)?.into_http(&context)?,
                            ))
                        })
                    })
                    .workflow("Repair", |_, _, steps| {
                        Box::pin(async move {
                            steps
                                .wait_for_event_with_timeout(
                                    "assignment",
                                    "assignment".into(),
                                    None,
                                )
                                .await
                        })
                    })?,
                ))
            })?;
        nodes.push(Node::start(config, services, graph, applications).await?);
        cores.push(core);
    }
    for node in &nodes {
        ready(node).await?;
    }
    let created: serde_json::Value =
        serde_json::from_str(&call(&client, &nodes[0], reqwest::Method::POST, "/create").await?)?;
    let scope = created["scope"].as_str().unwrap();
    let mut owner = None;
    for (index, node) in nodes.iter().enumerate() {
        let state: serde_json::Value = client
            .get(format!("http://{}/state", node.info().internal_address))
            .send()
            .await?
            .json()
            .await?;
        if state["published"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == scope)
        {
            owner = Some(index);
        }
    }
    let owner = owner.ok_or_else(|| anyhow::anyhow!("workflow has no authoritative owner"))?;
    let ingress = (owner + 1) % nodes.len();
    let deleted: serde_json::Value = serde_json::from_str(
        &call(&client, &nodes[ingress], reqwest::Method::POST, "/delete").await?,
    )?;
    ensure!(
        deleted["deleted"] == true,
        "remote deletion did not report completion"
    );
    let missing = client
        .get(format!(
            "http://{}/status",
            nodes[ingress].info().public_address
        ))
        .send()
        .await?;
    ensure!(
        missing.status() == 500,
        "deleted instance remained readable"
    );
    call(&client, &nodes[owner], reqwest::Method::POST, "/create").await?;
    call(&client, &nodes[owner], reqwest::Method::POST, "/delete").await?;
    for node in nodes {
        let NodeOutcome::Stopped(report) = node.run_until(async { ShutdownMode::Preserve }).await?
        else {
            anyhow::bail!("workflow test node did not stop");
        };
        ensure!(
            report.application_work_drained
                && report.cells_drained
                && report.local_durability_closed
                && !report.drain_timed_out
                && !report.deadline_elapsed,
            "incomplete workflow test shutdown {report:?}"
        );
    }
    drop(cores);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signed_cron_history_forwards_without_arming_and_refuses_other_protocols() -> Result<()> {
    use serde_json::json;
    let directory = tempfile::tempdir()?;
    let management = ExecutionDomain::new(tokio::runtime::Handle::current());
    let bucket = management
        .run(async {
            let bucket = Bucket::open_dev(&directory.path().join("history-fleet.sqlite"))?;
            bucket
                .put(
                    "fleet/peer-auth.json",
                    json!({"version":1,"key":"0b".repeat(32)})
                        .to_string()
                        .into_bytes(),
                )
                .await?;
            Ok::<_, anyhow::Error>(bucket)
        })
        .await??;
    let first_core = Executor::new("cron-history-owner")?;
    let second_core = Executor::new("cron-history-entrance")?;
    let (config, services) = inputs(
        "history-owner",
        &directory.path().join("owner"),
        &first_core,
        Some(bucket.clone()),
        0,
    )
    .await?;
    let owner = Node::start(config, services, graph(), applications("history", None)?).await?;
    ready(&owner).await?;
    let (config, services) = inputs(
        "history-entrance",
        &directory.path().join("entrance"),
        &second_core,
        Some(bucket),
        0,
    )
    .await?;
    let entrance = Node::start(config, services, graph(), applications("history", None)?).await?;
    ready(&entrance).await?;
    let auth = cellar::peer_auth::PeerAuth::new([0x0b; 32], "dashboard")?;
    let http = client()?;
    for node in [&owner, &entrance] {
        let path = "/runtime/.cron:embedded-node?inspection=cron_history_v1";
        let body = b"{}".to_vec();
        let response = auth
            .sign(
                http.post(format!("http://{}{path}", node.info().internal_address))
                    .body(body.clone()),
                "POST",
                path,
                &body,
                &node.info().node,
            )?
            .send()
            .await?;
        let status = response.status();
        let value: serde_json::Value = response.json().await?;
        ensure!(
            status == reqwest::StatusCode::OK,
            "cron history forwarding failed: {value}"
        );
        ensure!(
            value["result"]["items"] == json!([]) && value["result"]["active_alarm"].is_null(),
            "history armed an inactive schedule: {value}"
        );
    }
    for query in [
        "inspection=unsupported",
        "inspection=cron_history_v1&name=other",
    ] {
        let path = format!("/runtime/.cron:embedded-node?{query}");
        let body = b"{}".to_vec();
        let response = auth
            .sign(
                http.post(format!("http://{}{path}", entrance.info().internal_address))
                    .body(body.clone()),
                "POST",
                &path,
                &body,
                &entrance.info().node,
            )?
            .send()
            .await?;
        ensure!(
            response.status() == reqwest::StatusCode::BAD_REQUEST,
            "invalid history protocol was admitted"
        );
    }
    stopped(owner).await?;
    stopped(entrance).await?;
    management.shutdown().await;
    Ok(())
}
