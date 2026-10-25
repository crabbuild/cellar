// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{application::Fixture, backend::Backend, trace::Trace};
use crate::{application::Composite, crash::process::wait_groups_gone, fleet};
use anyhow::{ensure, Context as _, Result};
use cellar::{
    bucket::Bucket,
    sdk::{NodeOutcome, ShutdownMode},
};
use cellar_javascript::{BridgeUsage, JavaScriptApplication, Options};
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
    Terminate,
    GracefulShutdown,
    ForcedShutdown,
}

#[derive(Clone, Copy, Debug)]
struct Scenario {
    backend: Backend,
    caller: &'static str,
    peer: bool,
    after: bool,
    end: End,
}

impl End {
    fn suite(self, backend: Backend) -> &'static str {
        match self {
            Self::Kill => "worker-failure",
            Self::Terminate => "v8-termination",
            _ if backend == Backend::V8 => "v8-shutdown",
            _ => "worker-shutdown",
        }
    }

    fn is_failure(self) -> bool {
        matches!(self, Self::Kill | Self::Terminate)
    }

    fn label(self) -> &'static str {
        match self {
            Self::Kill => "kill",
            Self::Terminate => "terminate",
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

pub(super) async fn request(
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

pub(super) fn shutdown_reply(reply: Result<Value>, peer: bool) -> Result<Value> {
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
    scenario: Scenario,
) -> Result<Value> {
    let Scenario {
        backend,
        caller,
        peer,
        after,
        end,
    } = scenario;
    let trace = Arc::new(Trace::default());
    let fixture = Arc::new(Fixture {
        base,
        worker: worker.clone(),
        trace: trace.clone(),
        provider_class: backend.class(),
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
        ensure!(request(&client, &owner_url, "rust", "read", &[("target",backend.label())]).await? == json!({"value":"0"}), "worker warmup count differs");
        request(&client, &owner_url, "rust", "read", &[("target","rust"),("cell","worker-control")]).await?;
        let pid = if backend == Backend::Node {
            let pid = request(&client, &owner_url, "rust", "worker-pid", &[]).await?["pid"].as_i64().context("worker did not report its process identity")?;
            let pid = libc::pid_t::try_from(pid)?;
            ensure!(pid > 1 && pid != std::process::id() as i32, "unsafe worker process identity");
            // SAFETY: the fixed worker reports its authenticated process identity.
            ensure!(unsafe { libc::getpgid(pid) } == pid, "worker does not own its process group");
            Some(pid)
        } else {
            request(&client, &owner_url, "rust", "read", &[("target","rust"),("cell","worker-termination")]).await?;
            None
        };
        let source_url = if peer {
            let (node, url) = fleet::start(&path.join("source"), fixture.clone(), SOURCE, bucket).await?;
            source = Some(node);
            url
        } else { owner_url.clone() };
        let baseline = idle(&worker, false).await?;
        trace.armed.store(true, Ordering::Release);
        let operation = if after { "worker-after" } else { "worker-before" };
        let delta = DELTA.to_string();
        let query = [("target",backend.label()),("id",operation),("delta",delta.as_str())];
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
        let authority = trace.live(OWNER, if peer { SOURCE } else { OWNER }, backend.class())?;
        let held = worker.bridge_usage();
        ensure!(!held.stopped && held.active_invocations == 1 && held.retained_bytes > 0, "worker active reservations differ at failure: {held:?}");
        // Freeze observations before termination starts. No audit operation
        // may alter cleanup, and the saved checkpoint precedes this trigger.
        trace.disarm();
        let mut shutdown_proof = Value::Null;
        let mut termination_authority = Value::Null;
        let mut termination_result = Value::Null;
        let reply = match end {
            End::Kill => {
                let pid = pid.context("Node failure has no owned process")?;
                // SAFETY: this is the independently owned child group verified
                // above. The native host and callers use different runtimes.
                ensure!(unsafe { libc::kill(-pid, libc::SIGKILL) } == 0, "could not signal the owned worker group");
                let reply = tokio::time::timeout(Duration::from_secs(5), &mut call).await.context("worker failure did not release its RPC caller")??;
                ensure!(reply == json!({"code":"Unavailable","outcome":"unknown"}), "worker failure lost typed ambiguity: {reply}");
                tokio::time::timeout(Duration::from_secs(4), worker.stopped()).await.context("worker failure did not revoke admission")?;
                reply
            }
            End::Terminate => {
                trace.terminating.store(true, Ordering::Release);
                let trigger = request(&client, &owner_url, "rust", "worker-terminate", &[]);
                tokio::pin!(trigger);
                tokio::select! {
                    _ = trace.termination_ready.notified() => {},
                    reply = &mut call => anyhow::bail!("V8 call retired before the CPU trigger entered native code: {reply:?}"),
                    reply = &mut trigger => anyhow::bail!("V8 CPU trigger returned before native entry: {reply:?}"),
                    _ = tokio::time::sleep(Duration::from_secs(5)) => anyhow::bail!("V8 CPU trigger never reached its native child"),
                }
                termination_authority = trace.termination(OWNER)?;
                let (reply, trigger) = tokio::join!(
                    tokio::time::timeout(Duration::from_secs(8), &mut call),
                    tokio::time::timeout(Duration::from_secs(8), &mut trigger)
                );
                let reply = reply.context("V8 termination did not release its typed caller")??;
                ensure!(reply == json!({"code":"Unavailable","outcome":"unknown"}), "V8 termination lost typed ambiguity: {reply}");
                let failure = trigger.context("V8 termination did not release its CPU trigger")?
                    .err().context("V8 CPU loop returned a successful response")?;
                termination_result = shutdown_reply(Err(failure), false)?;
                tokio::time::timeout(Duration::from_secs(4), worker.stopped()).await.context("V8 termination did not revoke admission")?;
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
                trace.live(OWNER, if peer { SOURCE } else { OWNER }, backend.class())?;
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
            while !trace.retired(if end == End::Terminate { 3 } else { 2 }) { tokio::time::sleep(Duration::from_millis(5)).await; }
        }).await.context("worker failure left a native typed child running")?;
        trace.assert_revoked(if end == End::Terminate { 3 } else { 2 })?;
        let drained = idle(&worker, end.is_failure()).await?;
        trace.disarm();
        let mut caller_survived = false;
        if let Some(owner) = &owner {
            ensure!(owner.is_ready().await?, "worker failure stopped the native owner");
            ensure!(request(&client, &owner_url, "rust", "read", &[("target","rust")]).await? == json!({"value":"0"}), "native owner could not serve a fresh business method");
            ensure!(request(&client, &source_url, caller, "read", &[("target","rust")]).await? == json!({"value":"0"}), "provider loss stopped a surviving caller");
            caller_survived = true;
        }
        Ok::<_, anyhow::Error>(json!({"provider_backend":backend.label(),"caller":caller,"peer":peer,"after_sync":after,
            "trigger":end.label(),"operation_id":operation,"pid":pid,"authority":authority,"result":reply,
            "baseline":baseline,"held":held,"drained":drained,"native_owner_survived":end.is_failure(),"caller_survived":caller_survived,
            "shutdown_proof":shutdown_proof,"termination_authority":termination_authority,"termination_result":termination_result,
            "native_frames_retired":true,"late_budget_refused":true}))
    }.await;
    // Every failure path releases the test gate before joining real nodes.
    trace.disarm();
    trace.terminating.store(false, Ordering::Release);
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
    backend: Backend,
    after: bool,
    committed: bool,
) -> Result<Value> {
    let trace = Arc::new(Trace::default());
    let fixture = Arc::new(Fixture {
        base,
        worker: worker.clone(),
        trace,
        provider_class: backend.class(),
    });
    let bucket = Bucket::open_dev(&path.join("fleet.sqlite"))?;
    let (node, url) = fleet::start(&path.join("owner"), fixture, OWNER, bucket).await?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let result = async {
        let before = if committed { DELTA } else { 0 }.to_string();
        ensure!(
            request(
                &client,
                &url,
                "rust",
                "read",
                &[("target", backend.label())]
            )
            .await?
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
                    &[
                        ("target", backend.label()),
                        ("id", operation),
                        ("delta", &delta)
                    ]
                )
                .await?
                    == json!({"value":delta}),
                "explicit worker receipt retry duplicated or lost effects"
            );
        }
        ensure!(
            request(
                &client,
                &url,
                "rust",
                "read",
                &[("target", backend.label())]
            )
            .await?
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

async fn case(base: Arc<Composite>, scenario: Scenario) -> Result<Value> {
    tokio::task::spawn_blocking(move || {
        let Scenario {
            backend,
            end,
            after,
            ..
        } = scenario;
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../..");
        let directory = tempfile::tempdir()?;
        let mut evidence = None;
        let mut retained = None;
        let failed = backend.run(&root, &base, options(), false, |worker| {
            retained = Some(worker.clone());
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            evidence =
                Some(runtime.block_on(live_case(base.clone(), worker, directory.path(), scenario)));
            Ok(())
        });
        let mut evidence =
            evidence.context("disposable provider did not reach host readiness")??;
        ensure!(
            failed.is_err() == end.is_failure(),
            "worker runtime result differs from {end:?}: {failed:?}"
        );
        if end == End::Terminate {
            let error = failed
                .as_ref()
                .err()
                .context("V8 watchdog did not terminate its engine")?;
            ensure!(
                error
                    .chain()
                    .any(|cause| cause.to_string()
                        == "JavaScript engine turn exceeded execution budget"),
                "V8 failed without its execution-budget proof: {error:#}"
            );
            evidence["watchdog_termination_observed"] = json!(true);
        }
        evidence["owned_runtime_joined"] = json!(true);
        if backend == Backend::V8 {
            evidence["watchdog_joined"] = json!(true);
        }
        let usage = retained
            .context("missing failed worker handle")?
            .bridge_usage();
        ensure!(
            usage.stopped && usage.active_invocations == 0 && usage.retained_bytes == 0,
            "joined failed worker retained reservations: {usage:?}"
        );
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        if backend == Backend::Node {
            let pid =
                libc::pid_t::try_from(evidence["pid"].as_i64().context("missing worker PID")?)?;
            runtime.block_on(wait_groups_gone(&[("worker", pid)]))?;
            evidence["owned_group_gone_after_join"] = json!(true);
        }
        let recovery = backend.run(&root, &base, options(), true, |worker| {
            runtime.block_on(recovered(
                base.clone(),
                worker,
                directory.path(),
                backend,
                after,
                after || end == End::GracefulShutdown,
            ))
        })?;
        evidence["recovery"] = recovery;
        Ok::<_, anyhow::Error>(evidence)
    })
    .await
    .context("owned engine lifecycle bootstrap panicked")?
}

async fn matrix(base: Arc<Composite>, backend: Backend, ends: &[End]) -> Result<Value> {
    let suite = ends
        .first()
        .context("worker matrix has no trigger")?
        .suite(backend);
    ensure!(
        ends.iter().all(|end| end.suite(backend) == suite
            && !matches!(
                (backend, end),
                (Backend::Node, End::Terminate) | (Backend::V8, End::Kill)
            )),
        "worker matrix mixes different suites or incompatible backends"
    );
    let mut cases = Vec::new();
    for &end in ends {
        for caller in ["rust", "python", "v8", "node"] {
            for peer in [false, true] {
                for after in [false, true] {
                    base.audit.reset_after_node_drain();
                    eprintln!(
                        "worker {end:?}: {caller}->{} peer={peer} after_sync={after}",
                        backend.label()
                    );
                    cases.push(
                        case(
                            base.clone(),
                            Scenario {
                                backend,
                                caller,
                                peer,
                                after,
                                end,
                            },
                        )
                        .await
                        .with_context(|| {
                            format!(
                                "owned {backend:?} {end:?} {caller} peer={peer} after_sync={after}"
                            )
                        })?,
                    );
                }
            }
        }
    }
    Ok(
        json!({"format":1,"suite":suite,"case_count":cases.len(),"cases":cases,
        "provider_backend":backend.label(),"max_pending_operations":1,
        "isolated_provider_worker":backend == Backend::Node,
        "isolated_provider_runtime":true,"remote_fleet_qualified":false}),
    )
}

async fn smoke_case(base: Arc<Composite>, backend: Backend, end: End) -> Result<Value> {
    let case = case(
        base,
        Scenario {
            backend,
            caller: "rust",
            peer: false,
            after: true,
            end,
        },
    )
    .await?;
    Ok(
        json!({"format":1,"suite":format!("{}-smoke", end.suite(backend)),
        "provider_backend":backend.label(),"case_count":1,"cases":[case],"remote_fleet_qualified":false}),
    )
}

pub async fn smoke(base: Arc<Composite>) -> Result<Value> {
    smoke_case(base, Backend::Node, End::Kill).await
}

pub async fn run(base: Arc<Composite>) -> Result<Value> {
    matrix(base, Backend::Node, &[End::Kill]).await
}

pub async fn shutdown(base: Arc<Composite>) -> Result<Value> {
    matrix(
        base,
        Backend::Node,
        &[End::GracefulShutdown, End::ForcedShutdown],
    )
    .await
}

pub async fn shutdown_smoke(base: Arc<Composite>) -> Result<Value> {
    smoke_case(base, Backend::Node, End::ForcedShutdown).await
}

pub async fn v8_termination(base: Arc<Composite>) -> Result<Value> {
    matrix(base, Backend::V8, &[End::Terminate]).await
}

pub async fn v8_termination_smoke(base: Arc<Composite>) -> Result<Value> {
    smoke_case(base, Backend::V8, End::Terminate).await
}

pub async fn v8_shutdown(base: Arc<Composite>) -> Result<Value> {
    matrix(
        base,
        Backend::V8,
        &[End::GracefulShutdown, End::ForcedShutdown],
    )
    .await
}

pub async fn v8_shutdown_smoke(base: Arc<Composite>) -> Result<Value> {
    smoke_case(base, Backend::V8, End::ForcedShutdown).await
}

/// Preserve the same caller runtimes across both disposable-provider backends.
/// The CLI publishes this report only after those caller runtimes also join.
pub async fn lifecycle(base: Arc<Composite>) -> Result<Value> {
    let suites = [
        ("worker_failure", run(base.clone()).await?),
        ("worker_shutdown", shutdown(base.clone()).await?),
        ("v8_termination", v8_termination(base.clone()).await?),
        ("v8_shutdown", v8_shutdown(base).await?),
    ];
    let case_count = suites
        .iter()
        .try_fold(0u64, |total, (_, suite)| -> Result<u64> {
            total
                .checked_add(
                    suite["case_count"]
                        .as_u64()
                        .context("lifecycle suite has no case count")?,
                )
                .context("lifecycle case count overflow")
        })?;
    let mut report = json!({"format":1,"suite":"lifecycle","case_count":case_count,
        "remote_fleet_qualified":false});
    for (name, suite) in suites {
        report[name] = suite;
    }
    Ok(report)
}

#[cfg(test)]
#[path = "matrix_tests.rs"]
mod tests;
