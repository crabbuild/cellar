// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![cfg(any(feature = "embed", feature = "node"))]
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::*;
use cellar_javascript::{EventKind, Options, Source};
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::Duration,
};
#[allow(dead_code)]
#[path = "support/native.rs"]
mod native;
#[path = "../../cellar/tests/support/container_peer.rs"]
mod peer;

// Native host-private data must stay outside the guest KV/SQL surface.
struct PrivateApplication(Arc<dyn Application>, Arc<std::sync::atomic::AtomicU64>);

struct PrivateCell(Arc<dyn Cell>, Arc<std::sync::atomic::AtomicU64>);

impl Application for PrivateApplication {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        self.0.clone().invoke(context, event)
    }

    fn supports_cell(&self, class: &str) -> bool {
        self.0.supports_cell(class)
    }

    fn validate_deployment(&self, deployment: &cellar::fleet::LoadedDeployment) -> Result<()> {
        self.0.validate_deployment(deployment)
    }

    fn create_cell(&self, context: Context, identity: &CellIdentity) -> Result<Arc<dyn Cell>> {
        Ok(Arc::new(PrivateCell(
            self.0.create_cell(context, identity)?,
            self.1.clone(),
        )))
    }
}

impl Cell for PrivateCell {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if matches!(&event, Invocation::CellRpc { method, .. } if method == "native_fixture_json")
            {
                return Ok(Output::Rpc(cellar::engine_api::RpcData::Json(
                    "{\"fromNative\":true}".into(),
                )));
            }
            context
                .storage()?
                .put_json("__sys_workers_hidden", &serde_json::json!("host-private"))?;
            self.0.clone().invoke(context, event).await
        })
    }
}

#[derive(Clone, Copy)]
enum Backend {
    #[cfg(feature = "embed")]
    Embedded,
    #[cfg(feature = "node")]
    Node,
}

fn fixtures() -> Result<PathBuf> {
    static BUILT: OnceLock<Result<(), String>> = OnceLock::new();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tools/sandbox/compat");
    let result = BUILT.get_or_init(|| {
        let output = std::process::Command::new("node")
            .arg(root.join("build.mjs"))
            .output()
            .map_err(|error| error.to_string())?;
        if !output.status.success() {
            return Err(format!(
                "published fixture build failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        Ok(())
    });
    result
        .as_ref()
        .map_err(|error| anyhow::anyhow!(error.clone()))?;
    Ok(root.join("dist"))
}

fn run(
    backend: Backend,
    fixture: &str,
    host: impl FnOnce(Arc<dyn Application>) -> Result<()>,
) -> Result<()> {
    run_with_budget(backend, fixture, Duration::from_secs(10), host)
}

fn run_with_budget(
    backend: Backend,
    fixture: &str,
    budget: Duration,
    host: impl FnOnce(Arc<dyn Application>) -> Result<()>,
) -> Result<()> {
    let root = fixtures()?;
    let source = source(&root, fixture)?;
    let options = Options {
        turn_timeout: budget,
        ..Default::default()
    };
    match backend {
        #[cfg(feature = "embed")]
        Backend::Embedded => {
            cellar_javascript::JavaScriptRuntime::run(options, source, |app| host(app))
        }
        #[cfg(feature = "node")]
        Backend::Node => {
            let mut node = cellar_javascript::NodeOptions::new(root.join(format!("{fixture}.mjs")));
            node.heartbeat_timeout = budget;
            cellar_javascript::NodeRuntime::run(options, node, source, |app| host(app))
        }
    }
}

// Source registrations are identical in both engines; Node ignores the bundle.
fn source(root: &Path, fixture: &str) -> Result<Source> {
    let path = root.join(format!("{fixture}.bundle.js"));
    let code = if fixture == "sandbox" {
        "external Node module".to_owned()
    } else {
        std::fs::read_to_string(path)?
    };
    let events = if fixture == "rpc" {
        vec![EventKind::CellRpc]
    } else {
        vec![EventKind::Fetch, EventKind::Alarm]
    };
    let source = Source::new(code, fixture)
        .events([EventKind::Fetch])
        .cell_events("Counter", events);
    Ok(if fixture == "rpc" {
        source.cell_events("Other", [EventKind::CellRpc])
    } else {
        source
    })
}

async fn request(client: &reqwest::Client, address: &str, path: &str) -> Result<Value> {
    let reply = client.get(format!("{address}/{path}")).send().await?;
    let status = reply.status();
    let text = reply.text().await?;
    ensure!(status.is_success(), "{path}: {status}: {text}");
    serde_json::from_str(&text).context("fixture response JSON")
}

fn exercise(
    path: &Path,
    application: Arc<dyn Application>,
    fixture: &str,
    restart: bool,
) -> Result<()> {
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(async {
        tokio::time::timeout(Duration::from_secs(75), async {
            let peer = peer::Peer::start().await?;
            let calls = Arc::new(std::sync::atomic::AtomicU64::new(0));
            let (node, address) = native::start_configured(path, Arc::new(PrivateApplication(application, calls.clone())), |config, services, graph| {
                if fixture == "rpc" {
                    graph.primary.options.do_classes.push("Other".into());
                    graph.primary.options.bindings.push(("OTHERS".into(), "Other".into()));
                }
                if !matches!(fixture, "storage" | "rpc" | "callbacks") { peer.configure(config, services, graph); }
                config.runtime.handler_budget = Duration::from_secs(15);
                if fixture == "storage" { config.actor.core.idle_evict_ms = Some(50); }
            }).await?;
            let client = reqwest::Client::builder().timeout(Duration::from_secs(20)).build()?;
            match fixture {
                "callbacks" => {
                    let before = request(&client, &address, "alice/identity").await?;
                    let hold_client = client.clone();
                    let hold_address = address.clone();
                    let hold = tokio::spawn(async move { request(&hold_client, &hold_address, "alice/hold").await });
                    let mut entered = false;
                    for _ in 0..100 {
                        if request(&client, &address, "gate").await?["entered"] == true { entered = true; break; }
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    ensure!(entered, "native callback was not entered");
                    let count = calls.load(std::sync::atomic::Ordering::SeqCst);
                    let read_client = client.clone();
                    let read_address = address.clone();
                    let read = tokio::spawn(async move { request(&read_client, &read_address, "alice/read").await });
                    tokio::time::sleep(Duration::from_millis(75)).await;
                    ensure!(!read.is_finished() && calls.load(std::sync::atomic::Ordering::SeqCst) == count, "host admitted another native cell callback while input gate was held");
                    ensure!(request(&client, &address, "gate").await?["reads"] == 0);
                    request(&client, &address, "release").await?;
                    ensure!(hold.await??["ok"] == true && read.await??["ok"] == true);
                    let failed = client.get(format!("{address}/alice/fail")).send().await?;
                    ensure!(!failed.status().is_success(), "failed native block reported success");
                    let _ = failed.bytes().await?;
                    let after = request(&client, &address, "alice/identity").await?;
                    ensure!(after["id"] == before["id"] && after["generation"] != before["generation"], "failed native block did not reset the actual activation: {before:?} -> {after:?}");
                    for _ in 0..2 {
                        let broken = client.get(format!("{address}/broken/identity")).send().await?;
                        ensure!(!broken.status().is_success(), "failed initialization returned success");
                        let _ = broken.bytes().await?;
                    }
                    ensure!(request(&client, &address, "gate").await?["brokenInvocations"] == 0, "failed constructor admitted an application method");
                }
                "rpc" => {
                    let first: u64 = if restart { 3 } else { 1 };
                    ensure!(request(&client, &address, "alice/tick").await?["count"] == first);
                    ensure!(request(&client, &address, "bob/tick").await?["count"] == first.div_ceil(2));
                    ensure!(request(&client, &address, "alice/tick").await?["count"] == first + 1);
                    ensure!(request(&client, &address, "alice/check").await?["ok"] == true);
                }
                "storage" => {
                    let first: u64 = if restart { 3 } else { 1 };
                    ensure!(request(&client, &address, "alice/tick").await?["count"] == first);
                    ensure!(request(&client, &address, "bob/tick").await?["count"] == first.div_ceil(2));
                    ensure!(request(&client, &address, "alice/tick").await?["count"] == first + 1);
                    if restart { ensure!(request(&client, &address, "alice/read-types").await?["ok"] == true); }
                    for operation in ["types", "sql", "save", "borrow", "bounds", "buffers", "refuse", "crypto", "transactions", "sync-transactions", "callback-quota"] {
                        ensure!(request(&client, &address, &format!("alice/{operation}")).await?["ok"] == true);
                    }
                    let before = request(&client, &address, "alice/identity").await?;
                    let mut retired = false;
                    for _ in 0..30 {
                        tokio::time::sleep(Duration::from_millis(150)).await;
                        let after = request(&client, &address, "alice/identity").await?;
                        if after["generation"] != before["generation"] {
                            ensure!(after["id"] == before["id"] && after["epoch"] != before["epoch"], "eviction did not retain identity and fence the old epoch: {before:?} -> {after:?}");
                            retired = true;
                            break;
                        }
                    }
                    ensure!(retired, "idle eviction did not create a fresh native activation");
                }
                "container" => {
                    let before = request(&client, &address, "alice/read").await?;
                    ensure!(before["state"] == "stopped");
                    ensure!(before["configuration"].is_null() != restart);
                    ensure!(request(&client, &address, "alice/schedule").await?["configuration"]["tested"] == true);
                    ensure!(request(&client, &address, "bob/read").await?["configuration"].is_null());
                }
                "sandbox" => {
                    let before = request(&client, &address, "alice/status").await?;
                    ensure!(before["state"] == "stopped");
                    ensure!(before["name"].is_null() != restart);
                    ensure!(request(&client, &address, "alice/configure").await?["name"] == "native-fixture");
                    ensure!(request(&client, &address, "alice/read").await?["name"] == "native-fixture");
                    ensure!(request(&client, &address, "bob/status").await?["name"].is_null());
                }
                _ => anyhow::bail!("unknown fixture"),
            }
            let result = node.shutdown(ShutdownMode::Preserve).await?;
            ensure!(matches!(result, NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out));
            ensure!(peer.engine.containers.lock().unwrap().is_empty(), "constructor fixture started a container");
            Ok(())
        }).await?
    })
}

fn qualify(backend: Backend, fixture: &str) -> Result<()> {
    let directory = tempfile::tempdir()?;
    for restart in [false, true] {
        run(backend, fixture, |app| {
            exercise(directory.path(), app, fixture, restart)
        })?;
    }
    Ok(())
}

#[cfg(feature = "embed")]
#[test]
fn embedded_workers_storage_and_activation_recovery() -> Result<()> {
    qualify(Backend::Embedded, "storage")
}

#[cfg(feature = "node")]
#[test]
fn node_workers_storage_and_activation_recovery() -> Result<()> {
    qualify(Backend::Node, "storage")
}

#[cfg(feature = "embed")]
#[test]
fn published_container_constructor_storage_in_v8() -> Result<()> {
    qualify(Backend::Embedded, "container")
}

#[cfg(feature = "node")]
#[test]
fn published_container_constructor_storage_in_node() -> Result<()> {
    qualify(Backend::Node, "container")
}

#[cfg(feature = "node")]
#[test]
fn published_sandbox_constructor_metadata_recovery() -> Result<()> {
    qualify(Backend::Node, "sandbox")
}

#[cfg(feature = "embed")]
#[test]
fn embedded_workers_native_rpc_and_recovery() -> Result<()> {
    qualify(Backend::Embedded, "rpc")
}

#[cfg(feature = "node")]
#[test]
fn node_workers_native_rpc_and_recovery() -> Result<()> {
    qualify(Backend::Node, "rpc")
}

#[cfg(feature = "embed")]
#[test]
fn embedded_workers_native_callback_gates_and_reset() -> Result<()> {
    qualify(Backend::Embedded, "callbacks")
}

#[cfg(feature = "node")]
#[test]
fn node_workers_native_callback_gates_and_reset() -> Result<()> {
    qualify(Backend::Node, "callbacks")
}

fn synchronous_timeout(backend: Backend) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let completed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observation = completed.clone();
    let failed = run_with_budget(backend, "storage", Duration::from_millis(500), |app| {
        tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(async {
            let (node, address) = native::start_configured(directory.path(), app, |config, _, _| {
                config.runtime.handler_budget = Duration::from_secs(5);
            }).await?;
            let client = reqwest::Client::builder().timeout(Duration::from_secs(5)).build()?;
            let reply = client.get(format!("{address}/alice/sync-timeout")).send().await?;
            ensure!(!reply.status().is_success(), "runaway callback reported success");
            let _ = reply.bytes().await?;
            let outcome = node.shutdown(ShutdownMode::Preserve).await?;
            ensure!(matches!(outcome, NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out));
            observation.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })
    });
    // V8 termination and Node process loss are valid supervisor outcomes. The
    // authoritative checks are the refused response, joined native shutdown,
    // and a fresh runtime observing rollback rather than a partial write.
    ensure!(
        completed.load(std::sync::atomic::Ordering::SeqCst),
        "synchronous fault checks did not finish: {failed:?}"
    );
    run(backend, "storage", |app| {
        tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(async {
            let (node, address) = native::start_configured(directory.path(), app, |config, _, _| {
                config.runtime.handler_budget = Duration::from_secs(5);
            }).await?;
            ensure!(request(&reqwest::Client::new(), &address, "alice/sync-recovered").await?["ok"] == true, "synchronous callback write survived rollback");
            let outcome = node.shutdown(ShutdownMode::Preserve).await?;
            ensure!(matches!(outcome, NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out));
            Ok(())
        })
    })
}

#[cfg(feature = "embed")]
#[test]
fn embedded_sync_callback_timeout_rolls_back_and_joins() -> Result<()> {
    synchronous_timeout(Backend::Embedded)
}

#[cfg(feature = "node")]
#[test]
fn node_sync_callback_timeout_rolls_back_and_joins() -> Result<()> {
    synchronous_timeout(Backend::Node)
}
