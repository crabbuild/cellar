//! Actual saturation at configured application and bridge boundaries.
use super::*;

fn python_call(name: &str, argument: Option<&str>) -> Result<Value> {
    // The caller-owned supervisor is outside any native execution domain.
    // These fixture controls do not execute SDK work or grant authority.
    pyo3::Python::attach(|py| -> Result<Value> {
        let module = py
            .import("sys")?
            .getattr("modules")?
            .get_item("_cellar_application")?;
        let function = module.getattr(name)?;
        let value = match argument {
            Some(value) => function.call1((value,))?,
            None => function.call0()?,
        };
        let encoded: String = py
            .import("json")?
            .call_method1("dumps", (value,))?
            .extract()?;
        Ok(serde_json::from_str(&encoded)?)
    })
}

async fn observe(check: impl Fn(&Value) -> bool) -> Result<Value> {
    let mut last = Value::Null;
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            last = python_call("snapshot", None)?;
            if check(&last) {
                return Ok::<_, anyhow::Error>(last.clone());
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .with_context(|| format!("capacity barrier was not reached: {last}"))?
}

fn active(view: &Value, key: &str) -> bool {
    view["active"]
        .as_array()
        .is_some_and(|rows| rows.contains(&json!(key)))
}

fn request(
    client: &reqwest::Client,
    url: &str,
    path: &str,
) -> tokio::task::JoinHandle<reqwest::Result<reqwest::Response>> {
    let client = client.clone();
    let address = format!("{url}{path}");
    tokio::spawn(async move { client.get(address).send().await })
}

pub(super) fn child(mode: &str) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let options = Options {
        max_invocations: if mode == "replies" {
            1
        } else if mode == "objects" {
            4
        } else if matches!(mode, "bytes" | "nested-invocations" | "nested-remote") {
            3
        } else {
            2
        },
        max_tasks: if mode == "tasks" || mode.starts_with("cleanup-") {
            4
        } else {
            16
        },
        shutdown_timeout: if mode.starts_with("cleanup-") {
            Duration::from_millis(100)
        } else {
            Duration::from_secs(5)
        },
        max_pending_operations: if mode == "replies" { 3 } else { 2 },
        max_objects: 2,
        max_value_bytes: if mode == "bytes" { 4096 } else { 65536 },
        max_bridge_bytes: if mode == "bytes" { 4096 } else { 1024 * 1024 },
        ..Options::default()
    };
    let mode = mode.to_owned();
    let host_mode = mode.clone();
    let result = run(
        options,
        Source::new(include_str!("capacity.py"), "capacity.py")
            .events([EventKind::Fetch])
            .cell("Counter"),
        move |application, monitor| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(12))
                    .build()?;
                let remote = if host_mode == "nested-remote" {
                    let bucket = cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
                    let (owner, owner_url) = start_configured(
                        &directory.path().join("owner"), application.clone(), "capacity-owner", Some(bucket.clone()),
                        |config| config.runtime.handler_budget = Duration::from_secs(15),
                    ).await?;
                    let seeded: Value = client.get(format!("{owner_url}/nested/leaf")).send().await?
                        .error_for_status()?.json().await?;
                    ensure!(seeded["domain"] == "capacity-owner", "remote target was not placed on its owner");
                    Some((owner, bucket))
                } else { None };
                let (node, url) = start_configured(
                    &directory.path().join("primary"),
                    application.clone(),
                    "capacity",
                    remote.as_ref().map(|(_, bucket)| bucket.clone()),
                    |config| config.runtime.handler_budget = Duration::from_secs(15),
                )
                .await?;
                match host_mode.as_str() {
                    "invocations" => {
                        let first = request(&client, &url, "/held/one");
                        let second = request(&client, &url, "/held/two");
                        observe(|view| active(view, "one") && active(view, "two")).await?;
                        let refused = client
                                .get(format!("{url}/health"))
                                .timeout(Duration::from_secs(2))
                                .send()
                                .await?;
                        ensure!(refused.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE,
                            "Python invocation saturation must be HTTP 503, got {}", refused.status());
                        ensure!(refused.json::<Value>().await? == json!({"error":"application admission refused", "code":"Overloaded", "outcome":"unknown"}),
                            "Python invocation refusal lost its typed overload contract");
                        ensure!(
                            python_call("snapshot", None)?["visits"] == 2,
                            "refused handler entered Python"
                        );
                        python_call("release", Some("one"))?;
                        first.await??.error_for_status()?;
                        client
                            .get(format!("{url}/health"))
                            .send()
                            .await?
                            .error_for_status()?;
                        python_call("release", Some("two"))?;
                        second.await??.error_for_status()?;
                    }
                    "nested-invocations" | "nested-remote" => {
                        let before = python_call("snapshot", None)?;
                        let first = request(&client, &url, "/held/one");
                        let second = request(&client, &url, "/held/two");
                        observe(|view| active(view, "one") && active(view, "two")).await?;
                        let nested = client.get(format!("{url}/nested/leaf")).send().await?;
                        ensure!(nested.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE);
                        let failure: Value = nested.json().await?;
                        ensure!(failure == json!({"code":"Overloaded", "outcome":"unknown"}),
                            "nested Python admission was not classified: {failure}");
                        let after = python_call("snapshot", None)?;
                        ensure!(after["created"] == before["created"] && after["cell_visits"] == before["cell_visits"],
                            "refused nested handler entered Python");
                        python_call("release", Some("one"))?;
                        first.await??.error_for_status()?;
                        let committed = client.get(format!("{url}/nested/commit")).send().await?;
                        ensure!(committed.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE);
                        ensure!(committed.json::<Value>().await? == json!({"code":"Overloaded", "outcome":"unknown"}),
                            "nested refusal fabricated a non-commit outcome");
                        for path in ["/nested/commit-unhandled", "/private-overload"] {
                            let propagated = client.get(format!("{url}{path}")).send().await?;
                            ensure!(propagated.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE,
                                "propagated overload became an application fault: {path}");
                            ensure!(propagated.json::<Value>().await? == json!({"error":"application admission refused", "code":"Overloaded", "outcome":"unknown"}),
                                "propagated overload leaked private text or fabricated rollback: {path}");
                        }
                        ensure!(python_call("snapshot", None)?["created"] == 1,
                            "refused child handler entered Python");
                        python_call("release", Some("two"))?;
                        second.await??.error_for_status()?;
                        ensure!(client.get(format!("{url}/nested/state")).send().await?
                            .error_for_status()?.json::<Value>().await? == true,
                            "parent write was lost or nested refusal claimed rollback");
                        ensure!(client.get(format!("{url}/nested/state-unhandled")).send().await?
                            .error_for_status()?.json::<Value>().await? == true,
                            "unhandled nested failure erased the parent's committed write");
                        let subsequent: Value = client.get(format!("{url}/nested/leaf")).send().await?.error_for_status()?.json().await?;
                        ensure!(subsequent["domain"] == if remote.is_some() {"capacity-owner"} else {"capacity"},
                            "subsequent call changed owner/domain after refusal");
                    }
                    "tasks" => {
                        let result: Value = client
                            .get(format!("{url}/tasks/check"))
                            .send()
                            .await?
                            .error_for_status()?
                            .json()
                            .await?;
                        ensure!(
                            result["scoped"] == true && result["unscoped"] == true,
                            "task factory bypassed through an unscoped callback: {result}"
                        );
                        let nested = client.get(format!("{url}/tasks/unhandled")).send().await?;
                        ensure!(nested.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE,
                            "unhandled child-task saturation must preserve HTTP 503");
                        ensure!(nested.json::<Value>().await? == json!({"error":"application admission refused", "code":"Overloaded", "outcome":"unknown"}),
                            "unhandled child-task saturation lost its overload classification");
                        let held = request(&client, &url, "/tasks/hold");
                        observe(|view| active(view, "tasks")).await?;
                        let refused = client
                            .get(format!("{url}/health"))
                            .timeout(Duration::from_secs(2))
                            .send()
                            .await?;
                        ensure!(refused.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE,
                            "Python task saturation must be HTTP 503, got {}", refused.status());
                        ensure!(refused.json::<Value>().await? == json!({"error":"application admission refused", "code":"Overloaded", "outcome":"unknown"}),
                            "Python task refusal lost its typed overload contract");
                        python_call("release", Some("tasks"))?;
                        held.await??.error_for_status()?;
                    }
                    "objects" => {
                        let first: Value = client
                            .get(format!("{url}/cell/one"))
                            .send()
                            .await?
                            .error_for_status()?
                            .json()
                            .await?;
                        client
                            .get(format!("{url}/cell/two"))
                            .send()
                            .await?
                            .error_for_status()?;
                        observe(|view| view["live"] == 2).await?;
                        let refused = client
                            .get(format!("{url}/cell/three"))
                            .send()
                            .await?;
                        ensure!(refused.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE,
                            "Python object saturation must be HTTP 503, got {}", refused.status());
                        ensure!(refused.json::<Value>().await? == json!({"error":"application admission refused", "code":"Overloaded", "outcome":"unknown"}),
                            "Python object refusal lost its typed overload contract");
                        ensure!(
                            python_call("snapshot", None)?["created"] == 2,
                            "refused constructor executed"
                        );
                        lifetimes::evict(&client, &node, &first).await?;
                        observe(|view| view["live"] == 1).await?;
                        let third: Value = client
                            .get(format!("{url}/cell/three"))
                            .send()
                            .await?
                            .error_for_status()?
                            .json()
                            .await?;
                        ensure!(
                            third["serial"] != first["serial"]
                                && python_call("snapshot", None)?["live"] == 2
                        );
                    }
                    "bytes" => qualify_bytes(&client, &url, &application).await?,
                    "cleanup-error" | "cleanup-full" => {
                        let suffix = if host_mode == "cleanup-error" {
                            "error"
                        } else {
                            "full"
                        };
                        client
                            .get(format!("{url}/background/{suffix}"))
                            .send()
                            .await?
                            .error_for_status()?;
                        observe(|view| view["background"] == 3).await?;
                    }
                    "replies" => {
                        let held = request(&client, &url, "/replies");
                        observe(|view| active(view, "replies")).await?;
                        tokio::time::timeout(Duration::from_secs(5), async {
                            loop {
                                let report = application.bridge_report();
                                if report.operations.undelivered > 0 {
                                    ensure!(
                                        report.operations.replies_pending == 2,
                                        "native reply capacity was not reached: {report:?}"
                                    );
                                    return Ok::<_, anyhow::Error>(());
                                }
                                tokio::time::sleep(Duration::from_millis(2)).await;
                            }
                        })
                        .await
                        .context("full result queue did not refuse its next reply")??;
                        python_call("release", Some("replies"))?;
                        ensure!(
                            held.await??.status().is_server_error(),
                            "full native reply queue fabricated success"
                        );
                    }
                    _ => anyhow::bail!("unknown capacity fixture"),
                }
                if !host_mode.starts_with("cleanup-") {
                    client
                        .get(format!("{url}/health"))
                        .send()
                        .await?
                        .error_for_status()?;
                }
                stop(node, &monitor).await?;
                if let Some((owner, _)) = remote {
                    stop(owner, &monitor).await?;
                }
                Ok(())
            })
        },
    );
    if mode.starts_with("cleanup-") {
        let failure = result.expect_err("failed or unjoined Python cleanup must remain incomplete");
        let report = &failure
            .downcast_ref::<cellar_python::Incomplete>()
            .context("missing cleanup report")?
            .report;
        ensure!(
            !report.host_failed
                && report.pending_threads == 0
                && report.bridge.operations.settled()
                && report.bridge.pending_reservations == 0
                && report
                    .native_outcomes
                    .iter()
                    .all(|outcome| outcome["complete"] == true),
            "cleanup lost native joins or reservations: {report:?}"
        );
        if mode == "cleanup-error" {
            ensure!(
                report.runtime_failed
                    && report.runtime_failure_stage.as_deref() == Some("task_cleanup")
                    && report.pending_tasks == 0,
                "completed task failures were discarded during shutdown: {report:?}"
            );
        } else {
            ensure!(
                !report.runtime_failed
                    && report.pending_tasks == 3
                    && report.peak_tasks > 4
                    && report.peak_tasks <= 6,
                "full task budget prevented bounded supervisor cleanup: {report:?}"
            );
        }
        return Ok(());
    }
    let report = result?;
    if mode == "nested-remote" {
        ensure!(
            report.native_outcomes.len() == 2,
            "both native domains must report shutdown"
        );
    }
    ensure!(
        report.complete() && report.bridge.operations.settled(),
        "capacity refusal leaked work or poisoned host: {report:?}"
    );
    if mode == "objects" {
        ensure!(report.budget_exhaustions >= 1 && report.objects_created == 3);
    }
    if mode == "tasks" {
        ensure!(
            report.budget_exhaustions >= 3 && report.peak_tasks <= 4,
            "task admission accounting: {report:?}"
        );
    }
    if mode == "bytes" {
        ensure!(
            report.bridge.peak_queued_bytes == 4096 && report.bridge.budget_exhaustions >= 2,
            "byte admission accounting: {report:?}"
        );
    }
    if mode == "replies" {
        ensure!(report.bridge.operations.undelivered >= 1);
    }
    Ok(())
}

async fn qualify_bytes(
    client: &reqwest::Client,
    url: &str,
    application: &PythonApplication,
) -> Result<()> {
    let mut padding = 0usize;
    for (index, exact) in [false, true].into_iter().enumerate() {
        let key = format!("bytes-{index}");
        let held = request(client, url, &format!("/block/{key}"));
        observe(|view| active(view, &key)).await?;
        let queued_client = client.clone();
        let address = format!("{url}/queued");
        let value = "p".repeat(padding);
        let queued = tokio::spawn(async move {
            queued_client
                .post(address)
                .header("x-padding", value)
                .body(vec![b'x'; 1000])
                .send()
                .await
        });
        let used = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let report = application.bridge_report();
                if report.pending_reservations == 1 && report.queued_bytes > 0 {
                    return report.queued_bytes;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .context("queued invocation did not retain its byte reservation")?;
        if exact {
            ensure!(
                used == 4096,
                "calibrated envelope did not reach exact byte budget: {used}"
            );
            ensure!(
                client
                    .get(format!("{url}/refused"))
                    .timeout(Duration::from_secs(2))
                    .send()
                    .await?
                    .status()
                    .is_server_error(),
                "full aggregate budget admitted another payload"
            );
            ensure!(application.bridge_report().queued_bytes == 4096);
        } else {
            ensure!(used < 4096);
            padding = 4096 - used;
        }
        python_call("release", Some(&key))?;
        let blocker = held.await??;
        if exact {
            ensure!(
                blocker.status().is_server_error(),
                "completion without byte admission fabricated success"
            );
        } else {
            blocker.error_for_status()?;
        }
        queued.await??.error_for_status()?;
    }
    Ok(())
}
