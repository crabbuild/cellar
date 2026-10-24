// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Pause between polls of the original native future; never move SDK work.
use super::python_control::{call as control, observe};
use super::*;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex,
    },
    task::{Context as TaskContext, Poll},
};

#[derive(Default)]
struct Gate {
    paused: Mutex<bool>,
    blocked: AtomicBool,
}

struct ApplicationWithGate(Arc<PythonApplication>, Arc<Gate>);

impl Application for ApplicationWithGate {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        self.0.clone().invoke(context, event)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.0.supports_cell(class)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        Ok(Arc::new(CellWithGate(
            self.0.create_cell(context, identity)?,
            self.1.clone(),
        )))
    }
}

struct CellWithGate(Arc<dyn Cell>, Arc<Gate>);

impl Cell for CellWithGate {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        let pause =
            matches!(&event, Invocation::Fetch(request) if request.url.ends_with("/queued"));
        let inner = self.0.clone().invoke(context, event);
        if pause {
            Box::pin(Paused {
                inner,
                gate: self.1.clone(),
            })
        } else {
            inner
        }
    }
}

struct Paused {
    inner: HandlerFuture<Output>,
    gate: Arc<Gate>,
}

impl Future for Paused {
    type Output = Result<Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        // Synchronize pause with the complete synchronous poll. The supervisor
        // cannot close the gate while a previous poll is entering the mailbox.
        // No guard survives a poll return or crosses an await point.
        let paused = this.gate.paused.lock().unwrap();
        if *paused {
            this.gate.blocked.store(true, Ordering::Release);
            Poll::Pending
        } else {
            this.inner.as_mut().poll(cx)
        }
    }
}

async fn state(client: &reqwest::Client, url: &str) -> Result<()> {
    let value: Value = client
        .get(format!("{url}/state"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(
        value == json!({"seed":7,"queued":null,"late":null,"late-raw":null}),
        "queued/stale work changed storage: {value}"
    );
    Ok(())
}

pub(super) fn child(fence: bool) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let result = run(
        Options::default(),
        Source::new(include_str!("authority.py"), "authority.py")
            .events([EventKind::Fetch])
            .cell("Counter")
            .cell_initial_reads("Counter", EventKind::Fetch, ["seed"]),
        move |application, monitor| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let gate = Arc::new(Gate::default());
                let wrapped = Arc::new(ApplicationWithGate(application.clone(), gate.clone()));
                let bucket =
                    cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
                let (node, url) = start_configured(
                    &directory.path().join("old"),
                    wrapped.clone(),
                    "python-authority",
                    Some(bucket.clone()),
                    |config| {
                        config.runtime.handler_budget = Duration::from_secs(10);
                        config.actor.lease_ttl_ms = 5_000;
                        config.startup.recovery_retry_backoff = Duration::from_secs(5);
                    },
                )
                .await?;
                let mut node = Some(node);
                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(20))
                    .build()?;
                ensure!(
                    client
                        .get(format!("{url}/seed"))
                        .send()
                        .await?
                        .error_for_status()?
                        .json::<bool>()
                        .await?
                );
                let held_client = client.clone();
                let held_url = url.clone();
                let held = tokio::spawn(async move {
                    held_client.get(format!("{held_url}/queued")).send().await
                });
                observe(|view| view["entered"] == true && view["active"] == true).await?;
                *gate.paused.lock().unwrap() = true;
                let before = application.bridge_report().operations;
                ensure!(
                    before.queued == 0,
                    "unexpected queued work before submission: {before:?}"
                );
                control("submit")?;
                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        let report = application.bridge_report().operations;
                        if gate.blocked.load(Ordering::Acquire) && report.queued == 1 {
                            ensure!(
                                report.admitted == before.admitted + 1
                                    && report.started == before.started,
                                "queued mutation began native execution: {report:?}"
                            );
                            return Ok::<_, anyhow::Error>(());
                        }
                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                })
                .await
                .context("native poll gate did not retain queued mutation")??;
                if fence {
                    let outcome = fencing::force_node(
                        &bucket,
                        node.take().context("live authority node")?,
                        "python-authority",
                    )
                    .await?;
                    ensure!(
                        outcome
                            == NodeOutcome::Fenced {
                                code: 3,
                                reason: FenceReason::NodeLeaseMismatch
                            },
                        "queue did not actually fence: {outcome:?}"
                    );
                    monitor.record_node(outcome);
                }
                let response = held.await?;
                if fence {
                    if let Ok(response) = response {
                        ensure!(
                            response.status().is_server_error(),
                            "fenced queue returned success"
                        );
                    }
                } else {
                    ensure!(
                        response?.status().is_server_error(),
                        "deadline queue returned success"
                    );
                }
                observe(|view| view["cancelled"] == true && view["active"] == false).await?;
                control("release")?;
                observe(|view| view["finished"] == true && view["refused"] == true).await?;
                control("clear")?;
                ensure!(
                    application.bridge_report().operations.abandoned_queued
                        == before.abandoned_queued + 1,
                    "queued native mutation was not dropped"
                );
                if fence {
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let (successor, successor_url) = start_configured(
                        &directory.path().join("new"),
                        wrapped,
                        "python-authority-successor",
                        Some(bucket),
                        |config| {
                            config.runtime.handler_budget = Duration::from_secs(10);
                            config.actor.lease_ttl_ms = 5_000;
                            config.startup.recovery_retry_backoff = Duration::from_secs(5);
                        },
                    )
                    .await?;
                    state(&client, &successor_url).await?;
                    stop(successor, &monitor).await
                } else {
                    state(&client, &url).await?;
                    stop(node.take().context("live authority node")?, &monitor).await
                }
            })
        },
    );
    let report = if fence {
        let error = result.expect_err("fencing must retain its incomplete native outcome");
        let report = error
            .downcast_ref::<cellar_python::Incomplete>()
            .context("combined queued fence report")?
            .report
            .clone();
        ensure!(
            !report.runtime_failed && !report.host_failed,
            "authority failure: {error:?}"
        );
        ensure!(report.native_outcomes.len() == 2);
        report
    } else {
        let report = result?;
        ensure!(report.complete());
        report
    };
    ensure!(
        report.pending_tasks == 0
            && report.pending_objects == 0
            && report.pending_threads == 0
            && report.bridge.pending_reservations == 0
            && report.bridge.operations.settled(),
        "authority queue leaked work: {report:?}"
    );
    Ok(())
}
