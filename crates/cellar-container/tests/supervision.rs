// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Native internal work drives provider cleanup after the application returns.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::*;
use cellar_container::{Adapter, Limits, Operation, Provider};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
#[path = "support/e2b.rs"]
mod e2b;
#[path = "../../cellar-javascript/tests/support/native.rs"]
mod native;
#[path = "support/vercel.rs"]
mod vercel;

const USER_ALARM: i64 = 9_000_000_000_000;

struct Root {
    adapter: Adapter,
    alarms: AtomicUsize,
    reconcile_entered: tokio::sync::Notify,
}

impl Cell for Root {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let request = match event {
                Invocation::Fetch(request) => request,
                Invocation::Alarm { .. } => {
                    self.alarms.fetch_add(1, Ordering::SeqCst);
                    return Ok(Output::Done);
                }
                _ => return Ok(Output::Done),
            };
            let session = self.adapter.session();
            let limits = Limits {
                max_body_bytes: 65536,
            };
            let value = match request.url.rsplit('/').next().unwrap() {
                "start" | "lost-start" => {
                    context.storage()?.set_alarm(USER_ALARM)?;
                    context.storage()?.put_json("business", &json!("kept"))?;
                    let result = session
                        .execute(
                            &context,
                            Operation::Start,
                            json!({"options":{"env":[["HELLO","world"]]}}),
                            &limits,
                        )
                        .await;
                    if request.url.ends_with("/lost-start") {
                        ensure!(result.is_err());
                        json!(true)
                    } else {
                        result?
                    }
                }
                "clear" => {
                    context.storage()?.delete_all()?;
                    context.storage()?.set_alarm(USER_ALARM)?;
                    json!(true)
                }
                "state" => {
                    let checkpoint = containers::BindingJournal::open(&context, "container")?
                        .load(&context)?
                        .context("binding missing")?;
                    json!({"retired":checkpoint.record.is_retired(),"generation":checkpoint.record.generation.to_string(),"alarm":context.storage()?.get_alarm()?,"alarms":self.alarms.load(Ordering::SeqCst)})
                }
                "exec" => {
                    session
                        .execute(
                            &context,
                            Operation::ExecOutput,
                            json!({"options":{"cmd":["child-exit"]},"max_bytes":64}),
                            &limits,
                        )
                        .await?
                }
                "destroy" => {
                    session
                        .execute(&context, Operation::Destroy, json!({}), &limits)
                        .await?
                }
                "reconcile" => {
                    self.reconcile_entered.notify_one();
                    session
                        .execute(&context, Operation::Reconcile, json!({}), &limits)
                        .await?
                }
                "cancel-reconcile" => {
                    let waiting =
                        session.execute(&context, Operation::Reconcile, json!({}), &limits);
                    ensure!(
                        tokio::time::timeout(Duration::from_millis(100), waiting)
                            .await
                            .is_err(),
                        "cancelled waiter contacted the provider before lifecycle admission"
                    );
                    json!(true)
                }
                "expired-exec" => {
                    let error = session
                        .execute(
                            &context,
                            Operation::ExecOutput,
                            json!({"options":{"cmd":["child-exit"]},"max_bytes":64}),
                            &limits,
                        )
                        .await
                        .expect_err("expired resource admitted a command");
                    ensure!(error.is::<containers::RetentionExpired>(), "{error:#}");
                    json!(true)
                }
                _ => anyhow::bail!("unknown supervisor fixture route"),
            };
            Ok(Output::Http(
                http::Response::from_json(&value)?.into_http(&context)?,
            ))
        })
    }
}

struct App(Arc<Root>);

impl Application for App {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            Ok(Output::Http(
                context
                    .namespace("COUNTERS")?
                    .get_by_name("managed")
                    .fetch(&context, request)
                    .await?,
            ))
        })
    }

    fn supports_cell(&self, class: &str) -> bool {
        class == "Counter"
    }

    fn create_cell(&self, _: Context, _: &CellIdentity) -> Result<Arc<dyn Cell>> {
        Ok(self.0.clone())
    }
}

// Exercise fresh host registration at each native event, without giving the
// controller a saved application Context or issuing requests to drive cleanup.
struct Available {
    application: Arc<dyn Application>,
    registered: Arc<AtomicBool>,
}

impl Application for Available {
    fn container_supervision(&self) -> Option<Arc<containers::Supervision>> {
        self.registered
            .load(Ordering::SeqCst)
            .then(|| self.application.container_supervision())
            .flatten()
    }

    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        self.application.clone().invoke(context, event)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.application.supports_cell(class)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        self.application.create_cell(context, identity)
    }
}

enum Peer {
    E2b(e2b::Peer),
    Vercel(vercel::Peer),
}

impl Peer {
    async fn start(vercel: bool) -> Result<Self> {
        Ok(if vercel {
            Self::Vercel(vercel::Peer::start().await?)
        } else {
            Self::E2b(e2b::Peer::start().await?)
        })
    }

    fn provider(&self) -> Result<Arc<dyn Provider>> {
        match self {
            Self::E2b(p) => p.provider(),
            Self::Vercel(p) => p.provider(),
        }
    }

    fn deletes(&self) -> usize {
        match self {
            Self::E2b(p) => p.state.deletes.load(Ordering::SeqCst),
            Self::Vercel(p) => p.state.deletes.load(Ordering::SeqCst),
        }
    }

    fn creates(&self) -> usize {
        match self {
            Self::E2b(p) => p.state.creates.load(Ordering::SeqCst),
            Self::Vercel(p) => p.state.creates.load(Ordering::SeqCst),
        }
    }

    fn commands(&self) -> usize {
        match self {
            Self::E2b(p) => p.state.commands.load(Ordering::SeqCst),
            Self::Vercel(p) => p.state.commands.load(Ordering::SeqCst),
        }
    }

    fn command_gate(&self) -> (&AtomicBool, &tokio::sync::Notify, &tokio::sync::Notify) {
        match self {
            Self::E2b(p) => (
                &p.state.hold_command,
                &p.state.command_entered,
                &p.state.command_release,
            ),
            Self::Vercel(p) => (
                &p.state.hold_command,
                &p.state.command_entered,
                &p.state.command_release,
            ),
        }
    }

    fn delete_gate(&self) -> (&AtomicBool, &tokio::sync::Notify, &tokio::sync::Notify) {
        match self {
            Self::E2b(p) => (
                &p.state.hold_delete,
                &p.state.delete_entered,
                &p.state.delete_release,
            ),
            Self::Vercel(p) => (
                &p.state.hold_delete,
                &p.state.delete_entered,
                &p.state.delete_release,
            ),
        }
    }

    fn lost_create(&self) {
        match self {
            Self::E2b(p) => p.state.lost_create.store(true, Ordering::SeqCst),
            Self::Vercel(p) => p.state.lost_create.store(true, Ordering::SeqCst),
        }
    }

    fn lost_delete(&self) {
        match self {
            Self::E2b(p) => p.state.lost_delete.store(true, Ordering::SeqCst),
            Self::Vercel(p) => p.state.lost_delete.store(true, Ordering::SeqCst),
        }
    }

    fn defer_delete(&self, value: bool) {
        match self {
            Self::E2b(p) => p.state.defer_delete.store(value, Ordering::SeqCst),
            Self::Vercel(p) => p.state.defer_delete.store(value, Ordering::SeqCst),
        }
    }

    async fn stop(self) -> Result<()> {
        match self {
            Self::E2b(p) => p.stop().await,
            Self::Vercel(p) => p.stop().await,
        }
    }
}

async fn call(client: &reqwest::Client, address: &str, route: &str) -> Result<Value> {
    let response = client.get(format!("{address}/{route}")).send().await?;
    let status = response.status();
    let body = response.text().await?;
    ensure!(status.is_success(), "{route}: {status}: {body}");
    Ok(serde_json::from_str(&body)?)
}

async fn wait_deleted(peer: &Peer, count: usize) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(8), async {
        while peer.deletes() < count {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("host never attempted provider cleanup")?;
    Ok(())
}

#[derive(Clone, Copy)]
enum Scenario {
    Expiry,
    LostResponses,
    MissingBackend,
    InspectUncertainDelete,
    StaleCommand,
    ConcurrentDestroy,
}

struct InspectionOnly {
    backend: Arc<dyn containers::MaintenanceBackend>,
    inspections: Arc<AtomicUsize>,
}

impl containers::MaintenanceBackend for InspectionOnly {
    fn identity(&self) -> containers::BackendIdentity {
        self.backend.identity()
    }

    fn maintain(
        self: Arc<Self>,
        context: Context,
        expected: containers::BindingCheckpoint,
        action: containers::MaintenanceAction,
    ) -> HandlerFuture<()> {
        if action == containers::MaintenanceAction::Inspect
            && expected.record.intent == containers::BindingIntent::Deleted
        {
            self.inspections.fetch_add(1, Ordering::SeqCst);
        }
        self.backend.clone().maintain(context, expected, action)
    }
}

async fn exercise(vercel: bool, scenario: Scenario) -> Result<()> {
    let lost = matches!(scenario, Scenario::LostResponses);
    let unavailable = matches!(scenario, Scenario::MissingBackend);
    let inspection_only = matches!(scenario, Scenario::InspectUncertainDelete);
    let stale_command = matches!(scenario, Scenario::StaleCommand);
    let concurrent_destroy = matches!(scenario, Scenario::ConcurrentDestroy);
    let peer = Peer::start(vercel).await?;
    let directory = tempfile::tempdir()?;
    let provider = peer.provider()?;
    let backend = provider
        .maintenance_backend()
        .context("provider has no maintenance backend")?;
    let inspections = Arc::new(AtomicUsize::new(0));
    let backend = if inspection_only {
        Arc::new(InspectionOnly {
            backend,
            inspections: inspections.clone(),
        }) as Arc<dyn containers::MaintenanceBackend>
    } else {
        backend
    };
    let mut registry = containers::Supervision::default();
    registry.register(
        backend,
        containers::MaintenancePolicy {
            inspect_interval_ms: if stale_command { 10_000 } else { 50 },
            maximum_lifetime_ms: if stale_command || concurrent_destroy {
                20_000
            } else {
                1000
            },
            attempt_timeout_ms: 1000,
            idle_timeout_ms: None,
        },
    )?;
    let root = Arc::new(Root {
        adapter: Adapter::new(provider),
        alarms: AtomicUsize::new(0),
        reconcile_entered: tokio::sync::Notify::new(),
    });
    let application = containers::supervise(Arc::new(App(root.clone())), registry);
    let registered = Arc::new(AtomicBool::new(true));
    let application = Arc::new(Available {
        application,
        registered: registered.clone(),
    });
    let (node, address) =
        native::start_configured(directory.path(), application, |config, _, _| {
            config.runtime.handler_budget = Duration::from_secs(5)
        })
        .await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(6))
        .build()?;
    let mut pending = None;
    let mut reconciliation = None;
    let result = async {
        if lost {
            peer.lost_create();
        }
        if lost || inspection_only {
            peer.lost_delete();
            peer.defer_delete(true);
        }
        call(&client, &address, if lost { "lost-start" } else { "start" }).await?;
        if concurrent_destroy {
            let (hold, entered, release) = peer.delete_gate();
            hold.store(true, Ordering::SeqCst);
            let destroy_client = client.clone();
            let destroy_address = address.clone();
            pending = Some(tokio::spawn(async move {
                destroy_client.get(format!("{destroy_address}/destroy")).send().await
            }));
            tokio::time::timeout(Duration::from_secs(3), entered.notified())
                .await.context("provider never accepted deletion")?;
            // The provider has removed the resource but withheld its reply.
            // Another event and the due maintenance must not race its journal.
            let reconcile_client = client.clone();
            let reconcile_address = address.clone();
            reconciliation = Some(tokio::spawn(async move {
                call(&reconcile_client, &reconcile_address, "reconcile").await
            }));
            tokio::time::timeout(Duration::from_secs(3), root.reconcile_entered.notified())
                .await.context("reconciliation did not enter the cell")?;
            if let Ok(result) = tokio::time::timeout(Duration::from_millis(200), reconciliation.as_mut().unwrap()).await {
                reconciliation.take();
                anyhow::bail!("another lifecycle operation completed before the admitted deletion settled: {result:?}");
            }
            ensure!(call(&client, &address, "cancel-reconcile").await? == true);
            let state = call(&client, &address, "state").await?;
            ensure!(state["retired"] == false && peer.deletes() == 1,
                "background maintenance raced the admitted deletion: {state}");
            release.notify_one();
            let response = pending.take().unwrap().await??;
            let status = response.status();
            let body = response.text().await?;
            ensure!(status.is_success(), "destroy failed: {status}: {body}");
            reconciliation.take().unwrap().await??;
            let state = call(&client, &address, "state").await?;
            ensure!(state["retired"] == true && state["generation"] == "1");
            call(&client, &address, "start").await?;
            call(&client, &address, "destroy").await?;
            ensure!(peer.creates() == 2 && peer.deletes() == 2 && peer.commands() == 0);
            return Ok(());
        }
        if stale_command {
            let (hold, entered, release) = peer.command_gate();
            hold.store(true, Ordering::SeqCst);
            let command_client = client.clone();
            let url = format!("{address}/exec");
            pending = Some(tokio::spawn(
                async move { command_client.get(url).send().await },
            ));
            tokio::time::timeout(Duration::from_secs(3), entered.notified())
                .await
                .context("command did not reach the original resource")?;
            call(&client, &address, "destroy").await?;
            call(&client, &address, "start").await?;
            release.notify_one();
            let response = pending.take().unwrap().await??;
            let status = response.status();
            let body = response.text().await?;
            ensure!(
                status == 500 && body.contains("binding changed during operation"),
                "old generation published a success or unrelated failure: {status}: {body}"
            );
            let state = call(&client, &address, "state").await?;
            ensure!(state["generation"] == "2" && state["retired"] == false);
            let output = call(&client, &address, "exec").await?;
            ensure!(
                output["exit_code"] == 7,
                "replacement resource was invalidated"
            );
            call(&client, &address, "destroy").await?;
            ensure!(peer.creates() == 2 && peer.deletes() == 2 && peer.commands() == 2);
            return Ok(());
        }
        if unavailable {
            registered.store(false, Ordering::SeqCst);
        }
        call(&client, &address, "clear").await?;
        if unavailable {
            // The 1 s lifetime expires while no original backend is available.
            tokio::time::sleep(Duration::from_millis(1250)).await;
            let state = call(&client, &address, "state").await?;
            ensure!(state["retired"] == false && peer.deletes() == 0);
            call(&client, &address, "expired-exec").await?;
            ensure!(peer.commands() == 0, "expired work reached the provider");
            registered.store(true, Ordering::SeqCst);
        }
        // No more application calls until the native internal deadline causes
        // an actual DELETE. Inspecting a peer counter cannot drive a cell turn.
        wait_deleted(&peer, 1).await?;
        if inspection_only {
            tokio::time::timeout(Duration::from_secs(5), async {
                while inspections.load(Ordering::SeqCst) < 3 {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .context("uncertain deletion did not remain scheduled for inspection")?;
            let state = call(&client, &address, "state").await?;
            ensure!(state["retired"] == false && state["alarm"] == USER_ALARM);
            ensure!(
                peer.deletes() == 1,
                "generic backend replayed an uncertain delete"
            );
            ensure!(
                peer.creates() == 1,
                "inspection allocated replacement compute"
            );
            return Ok(());
        }
        if lost {
            peer.defer_delete(false);
            wait_deleted(&peer, 2).await?;
        }
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let state = call(&client, &address, "state").await?;
                ensure!(
                    state["alarm"] == USER_ALARM
                        && state["alarms"] == 0
                        && state["generation"] == "1",
                    "maintenance consumed application alarm or replaced generation: {state}"
                );
                if state["retired"] == true {
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .context("retirement was not durably confirmed")??;
        ensure!(peer.creates() == 1, "maintenance replayed creation");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    peer.command_gate().2.notify_one();
    peer.delete_gate().2.notify_one();
    if let Some(task) = reconciliation {
        task.abort();
        let _ = task.await;
    }
    if let Some(task) = pending {
        task.abort();
        let _ = task.await;
    }
    let stopped = node.shutdown(ShutdownMode::Preserve).await;
    let peer_stopped = peer.stop().await;
    ensure!(
        matches!(stopped?,NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out),
        "supervisor work did not join"
    );
    peer_stopped?;
    result
}

#[test]
fn remote_resources_expire_without_requests_and_keep_business_alarms() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            for vercel in [false, true] {
                exercise(vercel, Scenario::Expiry).await?;
            }
            Ok(())
        })
}

#[test]
fn uncertain_creates_and_deletes_remain_owned_until_background_retirement() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            for vercel in [false, true] {
                exercise(vercel, Scenario::LostResponses).await?;
            }
            Ok(())
        })
}

#[test]
fn missing_backend_retains_obligation_and_resumes_without_an_application_call() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            for vercel in [false, true] {
                exercise(vercel, Scenario::MissingBackend).await?;
            }
            Ok(())
        })
}

#[test]
fn generic_backend_only_inspects_after_an_uncertain_delete() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            for vercel in [false, true] {
                exercise(vercel, Scenario::InspectUncertainDelete).await?;
            }
            Ok(())
        })
}

#[test]
fn late_command_cannot_publish_success_for_a_replacement_generation() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            for vercel in [false, true] {
                exercise(vercel, Scenario::StaleCommand).await?;
            }
            Ok(())
        })
}

#[test]
fn application_and_background_lifecycle_wait_for_an_admitted_delete() -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            for vercel in [false, true] {
                exercise(vercel, Scenario::ConcurrentDestroy).await?;
            }
            Ok(())
        })
}
