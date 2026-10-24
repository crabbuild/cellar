// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Public management eviction, native generation replacement and forced drain.
use super::*;

async fn value(client: &reqwest::Client, url: &str, path: &str) -> Result<Value> {
    Ok(client
        .get(format!("{url}{path}"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

async fn observed(
    client: &reqwest::Client,
    url: &str,
    check: impl Fn(&Value) -> bool,
) -> Result<Value> {
    let mut last = Value::Null;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            last = value(client, url, "/inspect").await?;
            if check(&last) {
                return Ok::<_, anyhow::Error>(last.clone());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .with_context(|| format!("activation observation deadline: {last}"))?
}

pub(super) async fn evict(client: &reqwest::Client, node: &Node, identity: &Value) -> Result<()> {
    let path = "/peer/management";
    let body = serde_json::to_vec(&json!({"generation":node.info().lease_generation,
        "action":{"evict":{"scope":identity["scope"],"epoch":identity["epoch"]}}}))?;
    let auth = PeerAuth::new([31; 32], "lifetime-controller")?;
    let response = auth
        .sign(
            client
                .post(format!("http://{}{path}", node.info().internal_address))
                .body(body.clone()),
            "POST",
            path,
            &body,
            &node.info().node,
        )?
        .send()
        .await?;
    let status = response.status();
    let reply: Value = response.json().await?;
    ensure!(
        status.is_success() && reply["result"]["completed"] == true,
        "eviction failed: {status}: {reply}"
    );
    Ok(())
}

fn retired(value: &Value, serial: &Value) -> bool {
    value["dead"]
        .as_array()
        .is_some_and(|rows| rows.contains(serial))
        && value["owners"][serial
            .as_u64()
            .expect("observed instance serial")
            .to_string()]
            == false
}

async fn idle_observed(check: impl Fn(&Value) -> bool) -> Result<()> {
    let mut last = Value::Null;
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            // This caller-owned supervisor is outside a node domain. Another
            // Python handler could overwrite a stale pump local and conceal
            // retention while the interpreter is otherwise idle.
            last = pyo3::Python::attach(|py| -> Result<Value> {
                let module = py
                    .import("sys")?
                    .getattr("modules")?
                    .get_item("_cellar_application")?;
                let observed = module.getattr("observation")?.call0()?;
                let encoded: String = py
                    .import("json")?
                    .call_method1("dumps", (observed,))?
                    .extract()?;
                Ok(serde_json::from_str(&encoded)?)
            })?;
            if check(&last) {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .with_context(|| format!("idle activation collection deadline: {last}"))?
}

pub(super) fn child() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let failure = run(
        Options::default(),
        Source::new(include_str!("lifetimes.py"), "lifetimes.py")
            .events([EventKind::Fetch])
            .cell("Counter"),
        move |application, monitor| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()?;
            runtime.block_on(qualify_lifecycle(directory.path(), application, monitor))
        },
    )
    .expect_err("forced native drain must remain an incomplete combined report");
    let incomplete = failure
        .downcast_ref::<cellar_python::Incomplete>()
        .context("missing combined forced-drain report")?;
    let report = &incomplete.report;
    ensure!(
        !report.host_failed
            && !report.runtime_failed
            && report.pending_tasks == 0
            && report.pending_threads == 0
            && report.pending_objects == 0
            && report.objects_created >= 5
            && report.objects_created == report.objects_retired
            && report.bridge.operations.settled()
            && report.bridge.pending_reservations == 0
            && report.bridge.queued_bytes == 0
            && report.native_outcomes.len() == 3
            && report
                .native_outcomes
                .iter()
                .filter(|outcome| outcome["complete"] == false)
                .count()
                == 1,
        "activation cleanup or native outcome was lost: {failure:?}"
    );
    pyo3::Python::attach(|py| -> Result<()> {
        py.import("gc")?.call_method0("collect")?;
        let module = py
            .import("sys")?
            .getattr("modules")?
            .get_item("_cellar_application")?;
        let created: usize = module.getattr("created")?.extract()?;
        ensure!(
            module.getattr("dead")?.len()? == created,
            "retired activation object still reachable after joined cleanup"
        );
        Ok(())
    })?;
    Ok(())
}

/// A fatal interpreter hook closes global admission while two nodes still own
/// resident objects and one invocation is suspended. Cleanup cannot use SDK
/// admission or require a Python callback from native cell destruction.
pub(super) fn closing_child() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let failure = run(
        Options::default(),
        Source::new(include_str!("lifetimes.py"), "lifetimes.py")
            .events([EventKind::Fetch])
            .cell("Counter"),
        move |application, monitor| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()?;
            runtime.block_on(qualify_global_close(directory.path(), application, monitor))
        },
    )
    .expect_err("interpreter thread failure must remain incomplete");
    let incomplete = failure
        .downcast_ref::<cellar_python::Incomplete>()
        .context("missing global-close combined report")?;
    let report = &incomplete.report;
    ensure!(
        report.runtime_failed
            && report.thread_errors == 1
            && !report.host_failed
            && report.pending_tasks == 0
            && report.pending_threads == 0
            && report.pending_objects == 0
            && report.objects_created == 2
            && report.objects_retired == 2
            && report.bridge.operations.settled()
            && report.bridge.pending_reservations == 0
            && report.bridge.queued_bytes == 0
            && report.native_outcomes.len() == 2
            && report
                .native_outcomes
                .iter()
                .all(|outcome| outcome["complete"] == true),
        "global closure lost object cleanup or native drain evidence: {failure:?}"
    );
    pyo3::Python::attach(|py| -> Result<()> {
        py.import("gc")?.call_method0("collect")?;
        let module = py
            .import("sys")?
            .getattr("modules")?
            .get_item("_cellar_application")?;
        ensure!(
            module.getattr("dead")?.len()? == 2,
            "global admission closure retained Python cell objects"
        );
        ensure!(
            module.getattr("cancelled")?.extract::<usize>()? == 1
                && module.getattr("late_refused")?.extract::<usize>()? == 2,
            "global closure did not revoke late SDK admission"
        );
        Ok(())
    })?;
    Ok(())
}

async fn qualify_lifecycle(
    directory: &Path,
    application: Arc<PythonApplication>,
    monitor: cellar_python::Stop,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()?;
    let configure = |config: &mut NodeConfig| {
        config.actor.core.max_resident = 1;
        config.actor.lease_ttl_ms = 3000;
        config.startup.recovery_retry_backoff = Duration::from_secs(3);
        config.runtime.handler_budget = Duration::from_secs(15);
        config.generation_max_age = Duration::from_millis(200);
        config.shutdown = ShutdownPolicy {
            total_ms: 3000,
            token_wait_ms: 0,
            no_progress_ms: 2000,
        };
    };
    let a_data = directory.join("a");
    let a_bucket = cellar::bucket::Bucket::open_dev(&directory.join("a-fleet.sqlite"))?;
    let b_bucket = cellar::bucket::Bucket::open_dev(&directory.join("b-fleet.sqlite"))?;
    let (mut a, a_url) = start_configured(
        &a_data,
        application.clone(),
        "lifetimes-a",
        Some(a_bucket.clone()),
        configure,
    )
    .await?;
    let (b, b_url) = start_configured(
        &directory.join("b"),
        application.clone(),
        "lifetimes-b",
        Some(b_bucket),
        configure,
    )
    .await?;
    let first: Value = client
        .post(format!("{a_url}/cell/seed"))
        .json(&11)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(first["value"] == 11 && first["marker"] == "lifetimes-a");
    // Eviction retains the sticky owner record, but closing the
    // LTX handle requires epoch+1 on reacquisition. Generation
    // swapping below is the distinct same-epoch path.
    value(&client, &a_url, "/pressure").await?;
    observed(&client, &b_url, |view| retired(view, &first["serial"])).await?;
    let pressure_recreated = value(&client, &a_url, "/cell/state").await?;
    ensure!(pressure_recreated["serial"] != first["serial"] && pressure_recreated["scope"] == first["scope"]
        && pressure_recreated["epoch"].as_u64() == first["epoch"].as_u64().map(|epoch|epoch+1) && pressure_recreated["value"] == 11,
        "closed-handle reacquisition reused identity or lost state: {first} -> {pressure_recreated}");
    evict(&client, &a, &pressure_recreated).await?;
    observed(&client, &b_url, |view| {
        retired(view, &pressure_recreated["serial"])
    })
    .await?;
    let second = value(&client, &a_url, "/cell/state").await?;
    ensure!(
        second["serial"] != pressure_recreated["serial"]
            && second["scope"] == first["scope"]
            && second["epoch"].as_u64()
                == pressure_recreated["epoch"].as_u64().map(|epoch| epoch + 1)
            && second["value"] == 11,
        "operator eviction failed new-epoch reacquisition: {pressure_recreated} -> {second}"
    );
    let other: Value = client
        .post(format!("{b_url}/cell/seed"))
        .json(&22)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(
        other["serial"] != second["serial"]
            && other["scope"] == second["scope"]
            && other["name"] == "one"
            && other["value"] == 22
            && other["marker"] == "lifetimes-b",
        "same-name domain identity: {other}"
    );
    let held_client = client.clone();
    let held_url = a_url.clone();
    let held = tokio::spawn(async move {
        held_client
            .get(format!("{held_url}/cell/hold"))
            .send()
            .await
    });
    observed(&client, &b_url, |view| {
        view["holding"]
            .as_array()
            .is_some_and(|rows| rows.contains(&json!("cross")))
    })
    .await?;
    let crossed = value(&client, &b_url, "/cell/cross").await?;
    ensure!(
        crossed["value"] == 22
            && crossed["metadata"] == "lifetimes-b"
            && crossed["forbidden"].is_null(),
        "metadata selected foreign storage: {crossed}"
    );
    ensure!(value(&client, &b_url, "/release/cross").await? == true);
    let released: Value = held.await??.error_for_status()?.json().await?;
    ensure!(
        released["value"] == 11
            && released["metadata"].is_null()
            && released["forbidden"].is_null()
    );
    value(&client, &b_url, "/cell/expired").await?;

    // Replace only native configuration. Python source/module and
    // interpreter stay fixed; this is not Python code hot reload.
    let mut graph = fixture::graph("python-test", "", vec!["Counter".into()]);
    graph.primary.options.bindings = vec![("COUNTERS".into(), "Counter".into())];
    graph.primary.options.vars = vec![("DOMAIN_MARKER".into(), "lifetimes-a2".into())];
    let factory = application.clone();
    let applications =
        CompiledApplications::default()
            .register("python-test", "test", move || Ok(factory.clone()))?;
    ensure!(a.adopt(graph, applications).await? == 2);
    let adopted = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let result = value(&client, &a_url, "/cell/state").await?;
            if result["marker"] == "lifetimes-a2" {
                return Ok::<_, anyhow::Error>(result);
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .context("native generation replacement did not become visible")??;
    ensure!(
        adopted["serial"] != second["serial"]
            && adopted["value"] == 11
            && adopted["forbidden"].is_null()
    );
    ensure!(
        adopted["epoch"] == second["epoch"],
        "generation swap changed the ownership epoch: {second} -> {adopted}"
    );
    observed(&client, &b_url, |view| retired(view, &second["serial"])).await?;
    value(&client, &a_url, "/cell/expired").await?;
    let current: Value = client
        .post(format!("{a_url}/cell/seed"))
        .json(&33)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    ensure!(current["value"] == 33 && current["serial"] == adopted["serial"]);

    let held_client = client.clone();
    let held_url = a_url.clone();
    let held = tokio::spawn(async move {
        held_client
            .get(format!("{held_url}/cell/hold-shutdown"))
            .send()
            .await
    });
    observed(&client, &b_url, |view| {
        view["holding"]
            .as_array()
            .is_some_and(|rows| rows.contains(&json!("shutdown")))
    })
    .await?;
    let mut stopping = tokio::spawn(a.shutdown(ShutdownMode::Preserve));
    let admission_closed = async {
        loop {
            let response = client.get(format!("{a_url}/cell/state")).send().await?;
            if response.status() == 503 {
                return Ok::<_, anyhow::Error>(());
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    };
    cellar::asyncrt::select! {
        outcome = &mut stopping => anyhow::bail!("shutdown skipped ingress/drain observation: {outcome:?}"),
        closed = tokio::time::timeout(Duration::from_secs(1), admission_closed) => closed.context("ingress did not close before drain")??,
    }
    let outcome = tokio::time::timeout(Duration::from_secs(6), stopping).await???;
    let NodeOutcome::Stopped(ref native) = outcome else {
        anyhow::bail!("unexpected forced stop {outcome:?}");
    };
    ensure!(
        native.drain_timed_out
            && !native.application_work_drained
            && native.pending_application_events > 0,
        "held invocation fabricated a clean drain: {native:?}"
    );
    monitor.record_node(outcome);
    let response = held.await?;
    ensure!(
        response.is_err() || response?.status().is_server_error(),
        "retired invocation returned success"
    );
    idle_observed(|view| {
        retired(view, &adopted["serial"]) && view["cancelled"] == 1 && view["late_refused"] == 2
    })
    .await?;
    tokio::time::sleep(Duration::from_millis(3100)).await;
    let (reopened, reopened_url) = start_configured(
        &a_data,
        application,
        "lifetimes-reopened",
        Some(a_bucket),
        configure,
    )
    .await?;
    let restored = value(&client, &reopened_url, "/cell/state").await?;
    ensure!(
        restored["value"] == 33
            && restored["forbidden"].is_null()
            && restored["serial"] != adopted["serial"],
        "shutdown retirement lost data or admitted a late write: {restored}"
    );
    value(&client, &reopened_url, "/cell/expired").await?;
    stop(reopened, &monitor).await?;
    stop(b, &monitor).await
}

async fn qualify_global_close(
    directory: &Path,
    application: Arc<PythonApplication>,
    monitor: cellar_python::Stop,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    let configure = |config: &mut NodeConfig| {
        config.runtime.handler_budget = Duration::from_secs(15);
        config.shutdown = ShutdownPolicy {
            total_ms: 5000,
            token_wait_ms: 0,
            no_progress_ms: 2000,
        };
    };
    let (a, a_url) = start_configured(
        &directory.join("a"),
        application.clone(),
        "close-a",
        None,
        configure,
    )
    .await?;
    let (b, b_url) = start_configured(
        &directory.join("b"),
        application,
        "close-b",
        None,
        configure,
    )
    .await?;
    for (url, amount) in [(&a_url, 11), (&b_url, 22)] {
        let state: Value = client
            .post(format!("{url}/cell/seed"))
            .json(&amount)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        ensure!(state["value"] == amount);
    }
    let held_client = client.clone();
    let held_url = a_url.clone();
    let held = tokio::spawn(async move {
        held_client
            .get(format!("{held_url}/cell/hold-shutdown"))
            .send()
            .await
    });
    observed(&client, &b_url, |view| {
        view["holding"]
            .as_array()
            .is_some_and(|rows| rows.contains(&json!("shutdown")))
    })
    .await?;
    let a_stop = monitor.clone();
    let b_stop = monitor.clone();
    let (_, a, b) = tokio::time::timeout(
        Duration::from_secs(10),
        futures_util::future::join3(
            client.get(format!("{b_url}/fatal")).send(),
            a.run_until(async move {
                a_stop.cancelled().await;
                ShutdownMode::Preserve
            }),
            b.run_until(async move {
                b_stop.cancelled().await;
                ShutdownMode::Preserve
            }),
        ),
    )
    .await
    .context("nodes did not join after global Python admission closed")?;
    monitor.record_node(a?);
    monitor.record_node(b?);
    let response = held.await?;
    ensure!(
        response.is_err() || response?.status().is_server_error(),
        "cancelled cell fabricated success after global closure"
    );
    Ok(())
}

pub(super) fn cancelling_child() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let report = run(
        Options::default(),
        Source::new(include_str!("lifetimes.py"), "lifetimes.py")
            .events([EventKind::Fetch])
            .cell("Counter"),
        move |application, monitor| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
                let (node, url) = start_configured(&directory.path().join("node"), application, "self-cancel", Some(bucket), |config| {
                    config.runtime.handler_budget = Duration::from_secs(15);
                    config.actor.lease_ttl_ms = 3000;
                    config.startup.recovery_retry_backoff = Duration::from_secs(3);
                }).await?;
                let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build()?;
                for path in ["/cancel-root", "/cell/cancel", "/cell/cancel-after-write"] {
                    let response = client.get(format!("{url}{path}")).timeout(Duration::from_secs(2)).send().await
                        .with_context(|| format!("Python cancellation did not finish {path} before the 15-second native budget"))?;
                    ensure!(response.status().is_server_error(), "cancelled handler returned success at {path}");
                }
                ensure!(value(&client, &url, "/cancelled-expired").await? == 3,
                    "self-cancelled contexts did not revoke their retained native senders");
                let state = value(&client, &url, "/cell/state").await?;
                ensure!(state["value"] == 44 && state["forbidden"].is_null(), "cancellation incorrectly established rollback: {state}");
                stop(node, &monitor).await
            })
        },
    )?;
    ensure!(
        report.complete()
            && report.interpreter.handlers_cancelled >= 3
            && report.bridge.operations.settled(),
        "self-cancelled handlers leaked native or Python work: {report:?}"
    );
    Ok(())
}
