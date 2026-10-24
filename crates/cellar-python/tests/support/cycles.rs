// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exercise real A -> B -> A calls, then revoke all suspended native turns.
use super::python_control::{call as control, observe};
use super::*;

async fn state(client: &reqwest::Client, url: &str) -> Result<()> {
    let value: Value = client
        .get(format!("{url}/state"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let a = json!({"cycle-outer":1,"cycle-middle":null,"cycle-inner":1,"late-outer":null,"late-middle":null,"late-inner":null,"late-raw":null});
    let b = {
        let mut value = a.clone();
        value["cycle-outer"] = Value::Null;
        value["cycle-inner"] = Value::Null;
        value["cycle-middle"] = json!(1);
        value
    };
    ensure!(
        value == json!({"a":a,"b":b}),
        "cycle state changed: {value}"
    );
    Ok(())
}

pub(super) enum End {
    Deadline,
    Fence,
    InnerCancellation,
}

pub(super) fn child(end: End) -> Result<()> {
    let fence = matches!(end, End::Fence);
    let cancel_inner = matches!(end, End::InnerCancellation);
    let directory = tempfile::tempdir()?;
    let result = run(
        Options::default(),
        Source::new(include_str!("cycles.py"), "cycles.py")
            .events([EventKind::Fetch])
            .cell("Counter"),
        move |application, monitor| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let bucket =
                    cellar::bucket::Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
                let (node, url) = start_configured(
                    &directory.path().join("old"),
                    application.clone(),
                    "python-cycle",
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
                        .get(format!("{url}/cycle"))
                        .send()
                        .await?
                        .error_for_status()?
                        .json::<bool>()
                        .await?
                );
                state(&client, &url).await?;
                let held_client = client.clone();
                let held_url = url.clone();
                let held = tokio::spawn(async move {
                    held_client.get(format!("{held_url}/held")).send().await
                });
                let expected = json!(["inner", "middle", "outer", "root"]);
                observe(|view| {
                    view["entered"] == expected
                        && view["active"]
                            .as_object()
                            .is_some_and(|values| values.values().all(|value| value == true))
                })
                .await?;
                if cancel_inner {
                    control("expire_inner")?;
                    let view = observe(|view| view["errors"]["middle"].is_object()).await?;
                    ensure!(
                        view["errors"]["middle"]["code"] == "Ambiguous"
                            && view["errors"]["middle"]["outcome"] == "unknown",
                        "nested cancellation fabricated a known outcome: {view}"
                    );
                    ensure!(
                        view["active"]["middle"] == true,
                        "nested failure escaped the held parent before native cancellation: {view}"
                    );
                }
                if fence {
                    let outcome = fencing::force_node(
                        &bucket,
                        node.take().context("live cycle node")?,
                        "python-cycle",
                    )
                    .await?;
                    ensure!(
                        outcome
                            == NodeOutcome::Fenced {
                                code: 3,
                                reason: FenceReason::NodeLeaseMismatch
                            },
                        "cycle did not actually fence: {outcome:?}"
                    );
                    monitor.record_node(outcome);
                }
                let response = held.await?;
                if fence {
                    if let Ok(response) = response {
                        ensure!(
                            response.status().is_server_error(),
                            "fenced cycle returned success"
                        );
                    }
                } else {
                    ensure!(
                        response?.status().is_server_error(),
                        "deadline cycle returned success"
                    );
                }
                observe(|view| {
                    view["cancelled"] == expected
                        && view["active"]
                            .as_object()
                            .is_some_and(|values| values.values().all(|value| value == false))
                })
                .await?;
                control("release")?;
                observe(|view| {
                    view["completed"] == expected
                        && view["refused"].as_object().is_some_and(|values| {
                            values.len() == 4
                                && values.values().all(|value| value == "ContextExpired")
                        })
                })
                .await?;
                control("clear")?;
                if fence {
                    // The injected replacement has no renewal task. Honor its
                    // lease before asking another domain to recover both cells.
                    tokio::time::sleep(Duration::from_secs(5)).await;
                    let (successor, successor_url) = start_configured(
                        &directory.path().join("new"),
                        application,
                        "python-cycle-successor",
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
                    stop(node.take().context("live cycle node")?, &monitor).await
                }
            })
        },
    );
    let report = if fence {
        let error = result.expect_err("fencing must retain an incomplete native outcome");
        let report = error
            .downcast_ref::<cellar_python::Incomplete>()
            .context("combined cycle fence report")?
            .report
            .clone();
        ensure!(
            !report.runtime_failed && !report.host_failed,
            "cycle failure: {error:?}"
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
        "cycle leaked managed work: {report:?}"
    );
    Ok(())
}
