// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![cfg(feature = "node")]
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::*;
use cellar_javascript::{EventKind, NodeOptions, NodeRuntime, Options, Source};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    time::Duration,
};
#[path = "support/native.rs"]
mod native;
#[path = "../../cellar/tests/support/container_peer.rs"]
mod peer;

fn fixture(name: &str) -> Result<PathBuf> {
    static BUILT: OnceLock<Result<(), String>> = OnceLock::new();
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tools/sandbox/compat");
    BUILT
        .get_or_init(|| {
            let output = std::process::Command::new("node")
                .arg(root.join("build.mjs"))
                .output()
                .map_err(|e| e.to_string())?;
            if output.status.success() {
                Ok(())
            } else {
                Err(String::from_utf8_lossy(&output.stderr).into_owned())
            }
        })
        .as_ref()
        .map_err(|e| anyhow::anyhow!(e.clone()))?;
    Ok(root.join("dist").join(format!("{name}.mjs")))
}

fn run(name: &str, class: &str, binding: &str) -> Result<()> {
    let directory = tempfile::tempdir()?;
    for restart in [false, true] {
        let source = Source::module()
            .workers_lifetimes()
            .events([EventKind::Fetch])
            .cell_events(
                class,
                [EventKind::Fetch, EventKind::Alarm, EventKind::CellRpc],
            );
        NodeRuntime::run(
            Options {
                max_body_bytes: 256,
                ..Default::default()
            },
            NodeOptions::new(fixture(name)?),
            source,
            |application| {
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?
                    .block_on(module_round(
                        directory.path(),
                        application,
                        class,
                        binding,
                        restart,
                    ))
            },
        )?;
    }
    Ok(())
}

async fn module_round(
    path: &Path,
    application: Arc<dyn Application>,
    class: &str,
    binding: &str,
    restart: bool,
) -> Result<()> {
    let (node, address) = native::start_with(
        path,
        application,
        vec![class.into()],
        vec![(binding.into(), class.into())],
    )
    .await?;
    // Plain module routing does not declare containers. The actual
    // published example is qualified in the configured test below.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let count_response = client.get(format!("{address}/alice/count")).send().await?;
    let count_status = count_response.status();
    let count_body = count_response.text().await?;
    ensure!(
        count_status.is_success(),
        "module count (restart={restart}): {count_status}: {count_body}"
    );
    let value: serde_json::Value = serde_json::from_str(&count_body)?;
    ensure!(value["count"] == if restart { 3 } else { 1 });
    ensure!(value["marker"] == "javascript-test");
    let parallel: serde_json::Value = client
        .get(format!("{address}/parallel-http"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(parallel == serde_json::json!(["left", "right"]));
    let response = client
        .get(format!("{address}/alice/retained"))
        .send()
        .await?
        .error_for_status()?;
    ensure!(response.headers()["x-cell"] == "peer");
    ensure!(response.headers().get_all("set-cookie").iter().count() == 2);
    ensure!(response.bytes().await?.as_ref() == [0, 255, 42]);
    let borrowed: serde_json::Value = client
        .get(format!("{address}/bob/borrow"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(borrowed["code"] == "ContextMismatch" && borrowed["outcome"] == "not_committed");
    let count_response = client.get(format!("{address}/alice/count")).send().await?;
    let count_status = count_response.status();
    let count_body = count_response.text().await?;
    ensure!(
        count_status.is_success(),
        "module count (restart={restart}): {count_status}: {count_body}"
    );
    let value: serde_json::Value = serde_json::from_str(&count_body)?;
    ensure!(value["count"] == if restart { 4 } else { 2 });
    let lost = client
        .get(format!("{address}/alice/lost-response"))
        .send()
        .await?;
    ensure!(
        lost.status().is_success(),
        "streamed reply did not publish its head"
    );
    ensure!(
        lost.bytes().await.is_err(),
        "oversized streamed result became clean EOF"
    );
    let effect: serde_json::Value = client
        .get(format!("{address}/alice/lost-effect"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(
        effect["effect"] == true,
        "failed response incorrectly erased the earlier write"
    );
    let outcome = node.shutdown(ShutdownMode::Preserve).await?;
    ensure!(
        matches!(outcome, NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out)
    );
    Ok(())
}

#[test]
fn module_http_and_activation_owned_bindings_survive_events_and_restart() -> Result<()> {
    run("module", "Counter", "COUNTERS")
}

#[test]
fn unchanged_published_container_module_routes_named_and_singleton_objects() -> Result<()> {
    let path = tempfile::tempdir()?;
    let source = Source::module()
        .workers_lifetimes()
        .events([EventKind::Fetch])
        .cell_events(
            "MyContainer",
            [EventKind::Fetch, EventKind::Alarm, EventKind::CellRpc],
        );
    NodeRuntime::run(
        Options::default(),
        NodeOptions::new(fixture("container-module")?),
        source,
        |application| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(container_routes(path.path(), application))
        },
    )
}

async fn container_routes(path: &Path, application: Arc<dyn Application>) -> Result<()> {
    let peer = peer::Peer::start().await?;
    let (node, address) = native::start_configured(path, application, |config, services, graph| {
        peer.configure(config, services, graph);
        graph.primary.options.do_classes = vec!["MyContainer".into()];
        graph.primary.options.bindings = vec![("MY_CONTAINER".into(), "MyContainer".into())];
        graph.primary.containers[0].class_name = "MyContainer".into();
        config.runtime.handler_budget = Duration::from_secs(10);
    })
    .await?;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()?;
    for route in [
        "/container/alice",
        "/container/bob",
        "/container/alice",
        "/singleton",
        "/lb",
    ] {
        let response = client.get(format!("{address}{route}")).send().await?;
        let status = response.status();
        let body = response.text().await?;
        ensure!(
            status == 302 && body == "container response",
            "{route}: {status} {body}"
        );
    }
    ensure!(
        peer.engine.containers.lock().unwrap().len() == 4,
        "distinct named/singleton/random objects did not own separate native containers"
    );
    let (release, gate) = tokio::sync::watch::channel(false);
    *peer.engine.destroy_gate.lock().unwrap() = Some(gate);
    let destroying_client = client.clone();
    let destroying_address = address.clone();
    let destroying = tokio::spawn(async move {
        destroying_client
            .get(format!("{destroying_address}/__destroy"))
            .query(&[("name", "/container/alice")])
            .send()
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while !peer
            .engine
            .destroy_entered
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    // The old native read has ended, but its full Workers monitor must
    // retain the original event until this other event's stop ack.
    tokio::time::sleep(Duration::from_millis(50)).await;
    ensure!(
        !destroying.is_finished(),
        "destroy escaped the engine acknowledgement barrier"
    );
    release.send_replace(true);
    ensure!(destroying.await??.status().is_success());
    for name in [
        "/container/alice",
        "/container/bob",
        "cf-singleton-container",
        "instance-0",
        "instance-1",
        "instance-2",
    ] {
        let response = client
            .get(format!("{address}/__destroy"))
            .query(&[("name", name)])
            .send()
            .await?;
        let status = response.status();
        let body = response.text().await?;
        ensure!(
            status.is_success() && body == "destroyed",
            "destroy {name}: {status} {body}"
        );
    }
    let remaining = peer
        .engine
        .containers
        .lock()
        .unwrap()
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    ensure!(
        remaining.is_empty(),
        "explicit cleanup left native containers: {remaining:?}"
    );
    let outcome = node.shutdown(ShutdownMode::Preserve).await?;
    ensure!(
        matches!(outcome, NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out)
    );
    ensure!(peer.engine.containers.lock().unwrap().is_empty());
    Ok(())
}

async fn qualify_alarm_queue(client: &reqwest::Client, address: &str) -> Result<()> {
    client
        .get(format!("{address}/timer/alarms/start"))
        .send()
        .await?
        .error_for_status()?;
    let first = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let state: serde_json::Value = client
                .get(format!("{address}/timer/alarms/state"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            if state["deliveries"].as_u64().unwrap_or(0) >= 2 {
                return Ok::<_, anyhow::Error>(state);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    ensure!(
        first["started"] == 1 && first["active"] == 1 && first["peak"] == 1,
        "rearmed alarm overlapped its predecessor: {first}"
    );
    client
        .get(format!("{address}/timer/alarms/release"))
        .send()
        .await?
        .error_for_status()?;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let state: serde_json::Value = client
                .get(format!("{address}/timer/alarms/state"))
                .send()
                .await?
                .error_for_status()?
                .json()
                .await?;
            if state["started"] == 2 && state["active"] == 0 {
                ensure!(state["peak"] == 1, "alarm callbacks overlapped: {state}");
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    Ok(())
}

#[test]
fn rearmed_alarm_waits_for_the_active_handler_and_drains() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let source = Source::module()
        .workers_lifetimes()
        .events([EventKind::Fetch])
        .cell_events(
            "Counter",
            [EventKind::Fetch, EventKind::Alarm, EventKind::CellRpc],
        );
    NodeRuntime::run(
        Options::default(),
        NodeOptions::new(fixture("module")?),
        source,
        |application| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(alarm_queue_node(directory.path(), application))
        },
    )
}

async fn alarm_queue_node(path: &Path, application: Arc<dyn Application>) -> Result<()> {
    let (node, address) = native::start_configured(path, application, |config, _, _| {
        // This new probe deliberately holds an alarm until another delivery is
        // observed. Keep its observation window below its own handler deadline.
        config.runtime.handler_budget = Duration::from_secs(5);
        config.actor.core.operation_deadline_ms = Some(250);
    })
    .await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    let result = qualify_alarm_queue(&client, &address).await;
    let outcome = node.shutdown(ShutdownMode::Preserve).await?;
    ensure!(
        matches!(outcome, NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out)
    );
    result
}

#[test]
fn published_container_sleeps_wakes_and_joins_lifecycle_hooks() -> Result<()> {
    let directory = tempfile::tempdir()?;
    // Recreate both the language runtime and native host on the same SQLite
    // state, so hook counters must survive activation and node replacement.
    for previous_cycles in [0, 2] {
        let source = Source::module()
            .workers_lifetimes()
            .events([EventKind::Fetch])
            .cell_events(
                "Counter",
                [EventKind::Fetch, EventKind::Alarm, EventKind::CellRpc],
            );
        NodeRuntime::run(
            Options::default(),
            NodeOptions::new(fixture("container-lifecycle")?),
            source,
            |application| {
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?
                    .block_on(lifecycle_round(
                        directory.path(),
                        application,
                        previous_cycles,
                    ))
            },
        )?;
    }
    Ok(())
}

async fn lifecycle_round(
    path: &Path,
    application: Arc<cellar_javascript::JavaScriptApplication>,
    previous_cycles: u64,
) -> Result<()> {
    let peer = peer::Peer::start().await?;
    // Model a process which handles SIGTERM and exits successfully. Other peer
    // users retain the default signal behavior; no production hook is replaced.
    peer.engine
        .exit_on_sigterm
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let (node, address) =
        native::start_configured(path, application.clone(), |config, services, graph| {
            peer.configure(config, services, graph);
            config.runtime.handler_budget = Duration::from_secs(5);
        })
        .await?;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .build()?;
    let result = async {
        for cycle in previous_cycles + 1..=previous_cycles + 2 {
            let response = client.get(format!("{address}/wake")).send().await?;
            let status = response.status();
            let body = response.text().await?;
            ensure!(
                status == 302 && body == "container response",
                "wake {cycle}: {status} {body}"
            );
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let response = client
                        .get(format!("{address}/state"))
                        .send()
                        .await?
                        .error_for_status()?;
                    let state: serde_json::Value = response.json().await?;
                    ensure!(
                        state["starts"].as_u64().unwrap_or(0) <= cycle,
                        "duplicate start hook: {state}"
                    );
                    if state["stops"] == cycle && state["alarm"].is_null() {
                        ensure!(
                            state["starts"] == cycle
                                && state["expired"] == cycle
                                && state["running"] == false
                                && state["status"] == "stopped",
                            "sleep {cycle}: {state}"
                        );
                        ensure!(
                            state["lastStop"] == serde_json::json!({"exitCode":0,"reason":"exit"}),
                            "stop hook: {state}"
                        );
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Ok::<_, anyhow::Error>(())
            })
            .await
            .context("published idle alarm did not finish its stop hook")??;
        }
        let calls = peer.engine.calls.lock().unwrap();
        let signals = calls
            .iter()
            .filter(|(_, path, _)| path.ends_with("/kill?signal=15"))
            .count();
        ensure!(
            signals == 2,
            "idle policy did not issue exactly two SIGTERMs: {signals}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let cleanup = client.get(format!("{address}/destroy")).send().await;
    let shutdown = node.shutdown(ShutdownMode::Preserve).await;
    let usage = application.bridge_usage();
    result.with_context(|| format!("lifecycle bridge={usage:?}, shutdown={shutdown:?}"))?;
    ensure!(cleanup?.status().is_success(), "lifecycle cleanup failed");
    ensure!(
        peer.engine.containers.lock().unwrap().is_empty(),
        "lifecycle left owned engine resources"
    );
    ensure!(
        matches!(shutdown?, NodeOutcome::Stopped(report) if report.cells_drained && report.application_work_drained && !report.drain_timed_out),
        "lifecycle did not drain"
    );
    ensure!(
        usage.active_invocations == 0 && usage.retained_bytes == 0,
        "lifecycle leaked: {usage:?}"
    );
    Ok(())
}
