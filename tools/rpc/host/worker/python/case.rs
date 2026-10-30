// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::super::{
    application::Fixture,
    matrix::{request, shutdown_reply},
    trace::Trace,
};
use super::{probes, recovery, Case, Trigger, DELTA, OWNER, SOURCE};
use crate::{application::Composite, fleet};
use anyhow::{ensure, Context as _, Result};
use cellar::{
    bucket::Bucket,
    sdk::{NodeOutcome, ShutdownMode},
};
use cellar_python::PythonApplication;
use serde_json::{json, Value};
use std::{
    path::Path,
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

pub(super) async fn run(
    base: Arc<Composite>,
    python: Arc<PythonApplication>,
    root: &Path,
    spec: Case,
) -> Result<Value> {
    spec.validate()?;
    let trace = Arc::new(Trace::default());
    let fixture = Arc::new(Fixture {
        base: base.clone(),
        worker: python.clone(),
        trace: trace.clone(),
        provider_class: "PythonCounter",
    });
    let bucket = Bucket::open_dev(&root.join("fleet.sqlite"))?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let (node, owner_url) = fleet::start_configured(
        &root.join("owner"),
        fixture.clone(),
        OWNER,
        bucket.clone(),
        None,
        super::super::configure_lifecycle,
    )
    .await?;
    let mut owner = Some(node);
    let mut source = None;
    let mut control_url = None;
    let mut guest_needs_finish = false;
    let result = async {
        if spec.stubborn {
            let worker = request(&client, &owner_url, "node", "process", &[]).await?;
            let pid = libc::pid_t::try_from(worker["pid"].as_u64().context("Python child has no Node worker PID")?)?;
            ensure!(pid > 1 && pid != std::process::id() as libc::pid_t, "unsafe Python child worker PID");
            // SAFETY: the fixed Node fixture reports its owned process identity.
            ensure!(unsafe { libc::getpgid(pid) } == pid, "Python child's Node worker has no owned group");
            let temporary = root.join("started.pending");
            std::fs::write(&temporary, serde_json::to_vec(&json!({"format":1,
                "pid":std::process::id(),"worker_pid":pid,"spec":spec}))?)?;
            std::fs::rename(temporary, root.join("started.json"))?;
        }
        ensure!(request(&client, &owner_url, "rust", "read", &[("target","python")]).await? == json!({"value":"0"}), "Python owner was not empty");
        for cell in ["worker-control", "cancel-followup", "python-late"] {
            ensure!(request(&client, &owner_url, "rust", "read", &[("target","rust"),("cell",cell)]).await? == json!({"value":"0"}), "Python native gate was not empty");
        }
        let (node, source_url) = fleet::start_configured(&root.join("source"), fixture.clone(), SOURCE, bucket, None, super::super::configure_lifecycle).await?;
        source = Some(node);
        control_url = Some(source_url.clone());
        let baseline = probes::resources(&python, 0, 0).await?;
        let operation = spec.operation();
        probes::control(&client, &source_url, &[("action","arm"),("id",operation),
            ("after", if spec.after { "true" } else { "false" }),
            ("stubborn", if spec.stubborn { "true" } else { "false" })]).await?;
        guest_needs_finish = true;
        trace.armed.store(true, Ordering::Release);
        let caller_url = if spec.peer { &source_url } else { &owner_url };
        let action = if spec.trigger == Trigger::Cancel { "proof-cancel" } else { "increment" };
        let delta = DELTA.to_string();
        let query = [("target","python"),("id",operation),("delta",delta.as_str())];
        let call = request(&client, caller_url, &spec.caller, action, &query);
        tokio::pin!(call);
        tokio::select! {
            _ = trace.ready.notified() => {},
            reply = &mut call => anyhow::bail!("Python call returned before the native gate: {reply:?}"),
            _ = tokio::time::sleep(Duration::from_secs(5)) => anyhow::bail!("Python handler never entered its native child"),
        }
        tokio::time::timeout(Duration::from_secs(4), async {
            while !trace.checkpoint_ready(spec.after, DELTA) {
                trace.check_observation()?;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Ok::<_, anyhow::Error>(())
        }).await.context("Python native count/receipt checkpoint was not observed")??;
        let authority = trace.live(OWNER, if spec.peer { SOURCE } else { OWNER }, "PythonCounter")?;
        let held = python.bridge_report();
        ensure!(held.native_invocations == if spec.caller == "python" { 2 } else { 1 }
            && held.queued_bytes > 0 && held.pending_reservations > 0,
            "Python native reservations differ while held: {held:?}");
        trace.disarm();
        let mut shutdown_proof = Value::Null;
        let mut reply = Value::Null;
        if spec.trigger == Trigger::Cancel {
            request(&client, caller_url, &spec.caller, "cancel-release", &[("name",operation)]).await?;
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let settled = request(&client, caller_url, &spec.caller, "cancel-state", &[("name",operation)]).await?;
                    if settled["settled"] == true {
                        probes::cancelled(&settled, &spec.caller)?;
                        break Ok::<_, anyhow::Error>(());
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }).await.context("Python caller did not acknowledge cancellation")??;
        } else {
            let stopping = owner.take().context("Python owner already stopped")?.shutdown(ShutdownMode::Preserve);
            let (stopped, returned) = tokio::join!(stopping, tokio::time::timeout(Duration::from_secs(12), &mut call));
            let NodeOutcome::Stopped(proof) = stopped? else { anyhow::bail!("Python owner failed during shutdown"); };
            ensure!(proof.drain_timed_out && !proof.application_work_drained && !proof.cells_drained
                && !proof.clean_reload_prepared && proof.pending_application_events > 0 && proof.pending_dispatches > 0
                && proof.log_quiesced && proof.local_durability_closed, "Python forced shutdown proof differs: {proof:?}");
            shutdown_proof = json!({"graceful":false,"drain_timed_out":proof.drain_timed_out,
                "application_work_drained":proof.application_work_drained,"cells_drained":proof.cells_drained,
                "pending_events_at_cut":proof.pending_application_events,"pending_dispatches_at_cut":proof.pending_dispatches,
                "clean_reload_prepared":proof.clean_reload_prepared,"log_quiesced":proof.log_quiesced,
                "local_durability_closed":proof.local_durability_closed});
            reply = shutdown_reply(returned.context("Python owner shutdown lost its caller")?, spec.peer)?;
        }
        tokio::time::timeout(Duration::from_secs(4), async {
            while !trace.retired(2) { tokio::time::sleep(Duration::from_millis(5)).await; }
        }).await.context("suppressed Python handler retained native frames")?;
        trace.assert_revoked(2)?;
        probes::state(&client, &source_url, 0).await?;
        let first_probe = probes::probe(&client, &source_url, 1).await?;
        let parent = usize::from(spec.trigger == Trigger::Cancel && spec.caller == "python");
        let while_parent_live = probes::resources(&python, parent, parent + 1).await?;
        if spec.trigger == Trigger::Cancel {
            request(&client, caller_url, &spec.caller, "cancel-release", &[("name",operation),("phase","finish")]).await?;
            reply = tokio::time::timeout(Duration::from_secs(5), &mut call).await.context("Python cancellation parent did not finish")??;
            probes::cancelled(&reply, &spec.caller)?;
            ensure!(reply["node"] == if spec.peer { SOURCE } else { OWNER }
                && reply["final_followup_writes"] == 0, "original caller authority did not survive target cancellation: {reply}");
        }
        let native_drained_guest_live = probes::resources(&python, 0, 1).await?;
        if let Some(node) = owner.take() { fleet::stop(node, ShutdownMode::Preserve).await?; }
        let cold = recovery::run(base.clone(), python.clone(), root, &client, operation, if spec.after { DELTA } else { 0 }).await?;
        let second_probe = probes::probe(&client, &source_url, 2).await?;
        trace.assert_revoked(2)?;
        let after_recovery = probes::resources(&python, 0, 1).await?;
        // Read again after the second stale attempt using a new native turn.
        let final_cold = recovery::run(base.clone(), python.clone(), root, &client, operation, DELTA).await?;
        let released = if spec.stubborn {
            Value::Null
        } else {
            probes::control(&client, &source_url, &[("action","finish")]).await?;
            tokio::time::timeout(Duration::from_secs(4), async {
                loop {
                    let state = probes::control(&client, &source_url, &[("action","state")]).await?;
                    if state["finished"] == true { return Ok::<_, anyhow::Error>(()); }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            }).await.context("suppression task did not finish after its explicit release")??;
            probes::control(&client, &source_url, &[("action","clear")]).await?;
            guest_needs_finish = false;
            probes::resources(&python, 0, 0).await?
        };
        Ok::<_, anyhow::Error>(json!({"spec":spec,"authority":authority,"result":reply,"shutdown_proof":shutdown_proof,
            "baseline":baseline,"held":held,"first_probe":first_probe,"while_parent_live":while_parent_live,
            "native_drained_guest_live":native_drained_guest_live,"recovery":cold,"second_probe":second_probe,
            "after_recovery":after_recovery,"final_recovery":final_cold,"released":released,
            "native_frames_retired":true,"late_budget_refused":true}))
    }.await;
    trace.disarm();
    trace.release.notify_one();
    let guest_cleanup = if guest_needs_finish && !spec.stubborn {
        if let Some(url) = &control_url {
            probes::control(&client, url, &[("action", "finish")])
                .await
                .map(drop)
        } else {
            Ok(())
        }
    } else {
        Ok(())
    };
    let owner_stop = if let Some(node) = owner {
        fleet::stop(node, ShutdownMode::Preserve).await
    } else {
        Ok(())
    };
    let source_stop = if let Some(node) = source {
        fleet::stop(node, ShutdownMode::Preserve).await
    } else {
        Ok(())
    };
    trace.clear_probes();
    let result = result?;
    guest_cleanup?;
    owner_stop?;
    source_stop?;
    Ok(result)
}
