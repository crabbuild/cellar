// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{application::Fixture, trace::Trace};
use crate::{application::Composite, crash::process::wait_groups_gone, fleet};
use anyhow::{ensure, Context as _, Result};
use cellar::{
    bucket::Bucket,
    sdk::{NodeOutcome, ShutdownMode},
};
use cellar_javascript::{
    BridgeUsage, EventKind, JavaScriptApplication, NodeOptions, NodeRuntime, Options, Source,
};
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

const OWNER: &str = "worker-owner";
const SOURCE: &str = "worker-source";
const DELTA: i64 = 9_007_199_254_740_993;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum End {
    Kill,
    GracefulShutdown,
    ForcedShutdown,
}

impl End {
    fn suite(self) -> &'static str {
        match self {
            Self::Kill => "worker-failure",
            _ => "worker-shutdown",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Kill => "kill",
            Self::GracefulShutdown => "graceful",
            Self::ForcedShutdown => "forced",
        }
    }
}

#[derive(Debug)]
struct HttpFailure {
    status: reqwest::StatusCode,
    body: String,
}

impl std::fmt::Display for HttpFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "worker HTTP {}: {}", self.status, self.body)
    }
}

impl std::error::Error for HttpFailure {}

async fn request(
    client: &reqwest::Client,
    base: &str,
    caller: &str,
    action: &str,
    query: &[(&str, &str)],
) -> Result<Value> {
    let mut url = reqwest::Url::parse(&format!("{base}/{caller}/{action}"))?;
    url.query_pairs_mut().extend_pairs(query.iter().copied());
    let reply = client.get(url).send().await?;
    let status = reply.status();
    let body = reply.text().await?;
    if !status.is_success() {
        return Err(HttpFailure { status, body }.into());
    }
    Ok(serde_json::from_str(&body)?)
}

fn shutdown_reply(reply: Result<Value>, peer: bool) -> Result<Value> {
    match reply {
        Ok(value) => {
            ensure!(
                value.as_object().is_some_and(|object| object.len() == 2)
                    && value["outcome"] == "unknown"
                    && value["code"].as_str().is_some_and(|code| [
                        "Unavailable",
                        "ContextExpired",
                        "Cancelled",
                        "DeadlineExceeded"
                    ]
                    .contains(&code)),
                "shutdown claimed a known effect or returned success: {value}"
            );
            Ok(value)
        }
        Err(error) if !peer => {
            // The local caller's own HTTP server is stopping. This records a
            // lost reply after proven submission, not a fabricated SDK code.
            if let Some(http) = error.downcast_ref::<HttpFailure>() {
                ensure!(
                    http.status.is_server_error(),
                    "unexpected shutdown HTTP status: {http}"
                );
                if let Ok(body) = serde_json::from_str::<Value>(&http.body) {
                    ensure!(
                        body.get("outcome")
                            .is_none_or(|outcome| outcome == "unknown"),
                        "shutdown HTTP failure claimed rollback"
                    );
                }
                Ok(
                    json!({"http_status":http.status.as_u16(),"rpc_reply_unavailable_after_submission":true,"outcome":"unknown"}),
                )
            } else if let Some(transport) = error.downcast_ref::<reqwest::Error>() {
                ensure!(
                    !transport.is_timeout() && !transport.is_connect(),
                    "shutdown did not close the admitted transport: {transport}"
                );
                Ok(json!({"transport_closed_after_submission":true,"outcome":"unknown"}))
            } else {
                Err(error.context("unexpected local shutdown failure"))
            }
        }
        Err(error) => {
            Err(error.context("surviving peer caller did not receive a typed shutdown failure"))
        }
    }
}

async fn admission_closed(client: &reqwest::Client, base: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let response = client
                .get(format!("{base}/.well-known/cellar/health"))
                .send()
                .await?;
            let status = response.status();
            let body: Value = response.json().await?;
            if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                ensure!(
                    body == json!({"ok":false}),
                    "shutdown health response differs"
                );
                return Ok(());
            }
            ensure!(
                status.is_success(),
                "shutdown health probe failed: {status}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("native owner did not close admission during graceful shutdown")?
}

async fn idle(worker: &JavaScriptApplication, stopped: bool) -> Result<BridgeUsage> {
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            let usage = worker.bridge_usage();
            ensure!(
                usage.stopped == stopped,
                "worker stop state differs: {usage:?}"
            );
            if usage.active_invocations == 0 && usage.retained_bytes == 0 {
                return Ok(usage);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("worker invocation/byte reservations did not drain")?
}

async fn live_case(
    base: Arc<Composite>,
    worker: Arc<JavaScriptApplication>,
    path: &std::path::Path,
    caller: &'static str,
    peer: bool,
    after: bool,
    end: End,
) -> Result<Value> {
    let trace = Arc::new(Trace::default());
    let fixture = Arc::new(Fixture {
        base,
        worker: worker.clone(),
        trace: trace.clone(),
    });
    let bucket = Bucket::open_dev(&path.join("fleet.sqlite"))?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let (owner, owner_url) =
        fleet::start(&path.join("owner"), fixture.clone(), OWNER, bucket.clone()).await?;
    let mut owner = Some(owner);
    let mut source = None;
    let result = async {
        ensure!(request(&client, &owner_url, "rust", "read", &[("target","node")]).await? == json!({"value":"0"}), "worker warmup count differs");
        request(&client, &owner_url, "rust", "read", &[("target","rust"),("cell","worker-control")]).await?;
        let pid = request(&client, &owner_url, "rust", "worker-pid", &[]).await?["pid"].as_i64().context("worker did not report its process identity")?;
        let pid = libc::pid_t::try_from(pid)?;
        ensure!(pid > 1 && pid != std::process::id() as i32, "unsafe worker process identity");
        // SAFETY: the PID is returned by the authenticated, fixed disposable
        // worker module. Observation verifies its separately owned group.
        ensure!(unsafe { libc::getpgid(pid) } == pid, "worker does not own its process group");
        let source_url = if peer {
            let (node, url) = fleet::start(&path.join("source"), fixture.clone(), SOURCE, bucket).await?;
            source = Some(node);
            url
        } else { owner_url.clone() };
        let baseline = idle(&worker, false).await?;
        trace.armed.store(true, Ordering::Release);
        let operation = if after { "worker-after" } else { "worker-before" };
        let delta = DELTA.to_string();
        let query = [("target","node"),("id",operation),("delta",delta.as_str())];
        let call = request(&client, &source_url, caller, "increment", &query);
        tokio::pin!(call);
        tokio::select! {
            reply = &mut call => anyhow::bail!("worker call completed before its nested native gate: {reply:?}"),
            _ = trace.ready.notified() => {},
            _ = tokio::time::sleep(Duration::from_secs(5)) => anyhow::bail!("worker nested native gate was never entered"),
        }
        tokio::time::timeout(Duration::from_secs(4), async {
            loop {
                trace.check_observation()?;
                if trace.checkpoint_ready(after, DELTA) { return Ok::<_, anyhow::Error>(()); }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }).await.context("worker native count/receipt checkpoint was not observed")??;
        let authority = trace.live(OWNER, if peer { SOURCE } else { OWNER })?;
        let held = worker.bridge_usage();
        ensure!(!held.stopped && held.active_invocations == 1 && held.retained_bytes > 0, "worker active reservations differ at failure: {held:?}");
        // Freeze observations before termination starts. No audit operation
        // may alter cleanup, and the saved checkpoint precedes this trigger.
        trace.disarm();
        let mut shutdown_proof = Value::Null;
        let reply = match end {
            End::Kill => {
                // SAFETY: this is the independently owned child group verified
                // above. The native host and callers use different runtimes.
                ensure!(unsafe { libc::kill(-pid, libc::SIGKILL) } == 0, "could not signal the owned worker group");
                let reply = tokio::time::timeout(Duration::from_secs(5), &mut call).await.context("worker failure did not release its RPC caller")??;
                ensure!(reply == json!({"code":"Unavailable","outcome":"unknown"}), "worker failure lost typed ambiguity: {reply}");
                tokio::time::timeout(Duration::from_secs(4), worker.stopped()).await.context("worker failure did not revoke admission")?;
                reply
            }
            End::GracefulShutdown => {
                let stopping = fleet::stop(owner.take().context("worker owner already stopped")?, ShutdownMode::Preserve);
                tokio::pin!(stopping);
                tokio::select! {
                    stopped = &mut stopping => anyhow::bail!("native owner stopped before admitted work was released: {stopped:?}"),
                    reply = &mut call => anyhow::bail!("admitted call completed before shutdown release: {reply:?}"),
                    closed = admission_closed(&client, &owner_url) => closed?,
                }
                let factories = fixture.base.audit.factories().len();
                let probe = client.get(format!("{owner_url}/rust/read?target=rust&cell=shutdown-must-not-create")).send().await?;
                ensure!(probe.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE, "draining owner admitted a fresh root request");
                ensure!(fixture.base.audit.factories().len() == factories, "draining refusal entered a fresh cell factory");
                trace.live(OWNER, if peer { SOURCE } else { OWNER })?;
                trace.release.notify_one();
                let (stopped, reply) = tokio::join!(stopping, tokio::time::timeout(Duration::from_secs(12), &mut call));
                stopped?;
                let reply = reply.context("graceful shutdown lost its admitted RPC caller")??;
                ensure!(reply == json!({"value":delta}), "graceful shutdown lost the admitted result: {reply}");
                shutdown_proof = json!({"graceful":true,"new_root_refused_before_factory":true,"admitted_call_finished_before_stop":true});
                reply
            }
            End::ForcedShutdown => {
                let stopping = owner.take().context("worker owner already stopped")?.shutdown(ShutdownMode::Preserve);
                let (stopped, reply) = tokio::join!(stopping, tokio::time::timeout(Duration::from_secs(12), &mut call));
                let outcome = stopped?;
                let NodeOutcome::Stopped(proof) = outcome else { anyhow::bail!("forced owner shutdown failed: {outcome:?}"); };
                ensure!(proof.drain_timed_out && !proof.application_work_drained && !proof.cells_drained
                    && !proof.clean_reload_prepared && proof.pending_application_events > 0 && proof.pending_dispatches > 0,
                    "forced shutdown fabricated a graceful or idle proof: {proof:?}");
                ensure!(proof.log_quiesced && proof.local_durability_closed, "forced owner shutdown did not close durability: {proof:?}");
                shutdown_proof = json!({"graceful":false,"drain_timed_out":proof.drain_timed_out,
                    "ingress_drained":proof.ingress_drained,"application_work_drained":proof.application_work_drained,
                    "cells_drained":proof.cells_drained,"pending_events_at_cut":proof.pending_application_events,
                    "pending_dispatches_at_cut":proof.pending_dispatches,"clean_reload_prepared":proof.clean_reload_prepared,
                    "log_quiesced":proof.log_quiesced,"local_durability_closed":proof.local_durability_closed});
                shutdown_reply(reply.context("forced shutdown did not release its admitted RPC caller")?, peer)?
            }
        };
        tokio::time::timeout(Duration::from_secs(4), async {
            while !trace.retired() { tokio::time::sleep(Duration::from_millis(5)).await; }
        }).await.context("worker failure left a native typed child running")?;
        trace.assert_revoked()?;
        let drained = idle(&worker, end == End::Kill).await?;
        trace.disarm();
        if let Some(owner) = &owner {
            ensure!(owner.is_ready().await?, "worker failure stopped the native owner");
            ensure!(request(&client, &owner_url, "rust", "read", &[("target","rust")]).await? == json!({"value":"0"}), "native owner could not serve a fresh business method");
        }
        Ok::<_, anyhow::Error>(json!({"caller":caller,"peer":peer,"after_sync":after,
            "trigger":end.label(),"operation_id":operation,"pid":pid,"authority":authority,"result":reply,
            "baseline":baseline,"held":held,"drained":drained,"native_owner_survived":end == End::Kill,
            "shutdown_proof":shutdown_proof,
            "native_frames_retired":true,"late_budget_refused":true}))
    }.await;
    // Every failure path releases the test gate before joining real nodes.
    trace.disarm();
    trace.release.notify_one();
    let source_stop = if let Some(node) = source {
        fleet::stop(node, ShutdownMode::Preserve).await
    } else {
        Ok(())
    };
    let owner_stop = if let Some(node) = owner {
        fleet::stop(node, ShutdownMode::Preserve).await
    } else {
        Ok(())
    };
    trace.clear_probes();
    let result = result?;
    source_stop?;
    owner_stop?;
    Ok(result)
}

fn source(base: &Composite) -> Result<Source> {
    Source::module().events([EventKind::Fetch]).cell_rpc(
        "NodeCounter",
        "example.counter.v1.Counter",
        base.contract.clone(),
    )
}

fn options() -> Options {
    Options {
        max_invocations: 4,
        max_pending_operations: 1,
        turn_timeout: Duration::from_secs(2),
        ..Default::default()
    }
}

async fn recovered(
    base: Arc<Composite>,
    worker: Arc<JavaScriptApplication>,
    path: &std::path::Path,
    after: bool,
    committed: bool,
) -> Result<Value> {
    let trace = Arc::new(Trace::default());
    let fixture = Arc::new(Fixture {
        base,
        worker: worker.clone(),
        trace,
    });
    let bucket = Bucket::open_dev(&path.join("fleet.sqlite"))?;
    let (node, url) = fleet::start(&path.join("owner"), fixture, OWNER, bucket).await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let result = async {
        let before = if committed { DELTA } else { 0 }.to_string();
        ensure!(
            request(&client, &url, "rust", "read", &[("target", "node")]).await?
                == json!({"value":before}),
            "worker recovery fabricated or lost a committed count"
        );
        let delta = DELTA.to_string();
        let operation = if after {
            "worker-after"
        } else {
            "worker-before"
        };
        for _ in 0..2 {
            ensure!(
                request(
                    &client,
                    &url,
                    "rust",
                    "increment",
                    &[("target", "node"), ("id", operation), ("delta", &delta)]
                )
                .await?
                    == json!({"value":delta}),
                "explicit worker receipt retry duplicated or lost effects"
            );
        }
        ensure!(
            request(&client, &url, "rust", "read", &[("target", "node")]).await?
                == json!({"value":delta}),
            "worker receipt recovery count changed"
        );
        idle(&worker, false).await?;
        Ok::<_, anyhow::Error>(
            json!({"before_retry":before,"after_two_explicit_retries":delta,
            "fresh_runtime":true,"fresh_native_owner":true,"receipt_verified":true}),
        )
    }
    .await;
    let stopped = fleet::stop(node, ShutdownMode::Preserve).await;
    let result = result?;
    stopped?;
    Ok(result)
}

async fn case(
    base: Arc<Composite>,
    caller: &'static str,
    peer: bool,
    after: bool,
    end: End,
) -> Result<Value> {
    tokio::task::spawn_blocking(move || {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let directory = tempfile::tempdir()?;
        let entry = root.join("tools/rpc/dist/node/worker.mjs").canonicalize()?;
        let mut evidence = None;
        let mut retained = None;
        let failed = NodeRuntime::run(
            options(),
            NodeOptions::new(entry),
            source(&base)?,
            |worker| {
                retained = Some(worker.clone());
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?;
                evidence = Some(runtime.block_on(live_case(
                    base.clone(),
                    worker,
                    directory.path(),
                    caller,
                    peer,
                    after,
                    end,
                )));
                Ok(())
            },
        );
        let mut evidence =
            evidence.context("disposable Node worker did not reach host readiness")??;
        ensure!(
            failed.is_err() == (end == End::Kill),
            "worker runtime result differs from {end:?}: {failed:?}"
        );
        let usage = retained
            .context("missing failed worker handle")?
            .bridge_usage();
        ensure!(
            usage.stopped && usage.active_invocations == 0 && usage.retained_bytes == 0,
            "joined failed worker retained reservations: {usage:?}"
        );
        let pid = libc::pid_t::try_from(evidence["pid"].as_i64().context("missing worker PID")?)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        runtime.block_on(wait_groups_gone(&[("worker", pid)]))?;
        evidence["owned_group_gone_after_join"] = json!(true);
        let entry = root.join("tools/rpc/dist/node/node.mjs").canonicalize()?;
        let recovery = NodeRuntime::run(
            options(),
            NodeOptions::new(entry),
            source(&base)?,
            |worker| {
                runtime.block_on(recovered(
                    base,
                    worker,
                    directory.path(),
                    after,
                    after || end == End::GracefulShutdown,
                ))
            },
        )?;
        evidence["recovery"] = recovery;
        Ok::<_, anyhow::Error>(evidence)
    })
    .await
    .context("Node worker lifecycle bootstrap panicked")?
}

async fn matrix(base: Arc<Composite>, ends: &[End]) -> Result<Value> {
    let suite = ends
        .first()
        .context("worker matrix has no trigger")?
        .suite();
    ensure!(
        ends.iter().all(|end| end.suite() == suite),
        "worker matrix mixes different suites"
    );
    let mut cases = Vec::new();
    for &end in ends {
        for caller in ["rust", "python", "v8", "node"] {
            for peer in [false, true] {
                for after in [false, true] {
                    base.audit.reset_after_node_drain();
                    eprintln!("worker {end:?}: {caller}->node peer={peer} after_sync={after}");
                    cases.push(case(base.clone(), caller, peer, after, end).await.with_context(|| format!("owned worker {end:?} {caller}->node peer={peer} after_sync={after}"))?);
                }
            }
        }
    }
    Ok(
        json!({"format":1,"suite":suite,"case_count":cases.len(),"cases":cases,
        "max_pending_operations":1,"isolated_provider_worker":true,"remote_fleet_qualified":false}),
    )
}

pub async fn smoke(base: Arc<Composite>) -> Result<Value> {
    let case = case(base, "rust", false, true, End::Kill).await?;
    Ok(
        json!({"format":1,"suite":"worker-failure-smoke","case_count":1,"cases":[case],
        "remote_fleet_qualified":false}),
    )
}

pub async fn run(base: Arc<Composite>) -> Result<Value> {
    matrix(base, &[End::Kill]).await
}

pub async fn shutdown(base: Arc<Composite>) -> Result<Value> {
    matrix(base, &[End::GracefulShutdown, End::ForcedShutdown]).await
}

pub async fn shutdown_smoke(base: Arc<Composite>) -> Result<Value> {
    let case = case(base, "rust", false, true, End::ForcedShutdown).await?;
    Ok(
        json!({"format":1,"suite":"worker-shutdown-smoke","case_count":1,"cases":[case],"remote_fleet_qualified":false}),
    )
}

#[cfg(test)]
#[path = "matrix_tests.rs"]
mod tests;
