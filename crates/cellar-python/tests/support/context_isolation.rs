// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Preserve both sides of the context-isolation handshake under its real budget.
use super::*;
use std::time::Instant;

async fn call(client: &reqwest::Client, base: &str, path: &str) -> Result<Value> {
    response_json(client.get(format!("{base}{path}")).send().await?).await
}

pub(super) async fn check(
    client: &reqwest::Client,
    holder: &str,
    other: &str,
    application: &PythonApplication,
) -> Result<()> {
    let before = application.interpreter_snapshot();
    let started = Instant::now();
    let holding = async {
        let result = call(client, holder, "/scope-hold").await;
        (result, started.elapsed())
    };
    let mut stages = Vec::new();
    let crossing = async {
        let mut ready = false;
        for _ in 0..20 {
            let value = call(client, other, "/scope-ready").await;
            stages.push(("ready", started.elapsed()));
            if value? == true {
                ready = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        ensure!(ready, "first domain did not expose its live context");
        for path in ["/scope-steal", "/scope-direct", "/scope-rebound"] {
            let value = call(client, other, path).await;
            stages.push((path, started.elapsed()));
            let value = value?;
            ensure!(
                value == json!({"refused":true,"code":"ContextMismatch"}),
                "{path}: expected live-context ContextMismatch, got {value}"
            );
        }
        let released = call(client, other, "/scope-release").await;
        stages.push(("release", started.elapsed()));
        ensure!(released? == true, "context release failed");
        Ok::<_, anyhow::Error>(())
    };
    let ((held, held_elapsed), crossed) = futures_util::future::join(holding, crossing).await;
    let held = held.and_then(|value| {
        ensure!(value == true, "held request returned {value}");
        Ok(())
    });
    let result = match (held, crossed) {
        (Ok(()), result) => result,
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(crossed)) => {
            Err(error.context(format!("crossing also failed: {crossed:#}")))
        }
    };
    result.with_context(|| {
        format!(
            "context handshake: held={held_elapsed:?}, stages={stages:?}, interpreter={}",
            json!({"before":before,"after":application.interpreter_snapshot()})
        )
    })
}

pub(super) fn run_cases(iterations: usize) -> Result<()> {
    ensure!(
        (1..=1000).contains(&iterations),
        "iterations must be in 1..=1000"
    );
    let directory = tempfile::tempdir()?;
    let trace = first_write_trace::Trace::install()?;
    let case_trace = trace.clone();
    let root = directory.path().to_path_buf();
    let result = run(
        contract_options(),
        Source::new(include_str!("../bridge.py"), "bridge.py")
            .events([EventKind::Fetch])
            .cell_events("Counter", [EventKind::Fetch, EventKind::Alarm]),
        move |application, monitor| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build()?;
                for index in 0..iterations {
                    case_trace.clear();
                    let case = root.join(index.to_string());
                    let (node, url) = start(&case.join("a"), application.clone()).await?;
                    let (other, other_url) = start_with(&case.join("b"), application.clone(), "scope-other", None).await?;
                    let checked = check(&client, &url, &other_url, &application).await;
                    // Reset only after this handshake, so the next case still
                    // exercises both fresh hosts' cold stateless workers.
                    let reset = call(&client, &other_url, "/scope-reset").await;
                    let other_shutdown = stop(other, &monitor).await;
                    let shutdown = stop(node, &monitor).await;
                    checked.with_context(|| format!(
                        "context isolation case {index}; holder shutdown={shutdown:?}; other shutdown={other_shutdown:?}"
                    ))?;
                    ensure!(reset? == true, "context reset failed");
                    other_shutdown?;
                    shutdown?;
                }
                Ok(())
            })
        },
    );
    match result {
        Ok(report) => {
            ensure!(report.complete());
            cellar::cli_output::Output::new(cellar::cli_output::Format::Text)
                .line(format_args!("PASS {iterations} fresh-host context isolation cases; handler budget unchanged at 250 ms"))?;
            Ok(())
        }
        Err(error) => {
            let retained = directory.keep();
            let saved = trace.save(&retained.join("diagnostic-tail.log"));
            Err(error.context(format!(
                "context diagnostic retained state at {}; trace_saved={saved:?}",
                retained.display()
            )))
        }
    }
}

/// Fail the control branch while the real held handler is still active.
/// Inject at the first control request so reaching the fault does not itself
/// depend on completing three cold-host round trips within the held budget.
/// The diagnostic must retain that primary failure AND the ensuing budget failure;
/// a successful rerun cannot prove that either failure was captured correctly.
pub(super) fn failure_provenance() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let source = include_str!("../bridge.py").replace(
        "    if request.path == \"/scope-ready\":",
        "    if request.path == \"/scope-ready\":\n        return Response(b\"controlled context failure\", status=500)",
    );
    let report = run(
        contract_options(),
        Source::new(source, "context_failure.py")
            .events([EventKind::Fetch])
            .cell_events("Counter", [EventKind::Fetch, EventKind::Alarm]),
        move |application, monitor| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;
            runtime.block_on(async move {
                let client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(10))
                    .build()?;
                let (node, url) = start(&directory.path().join("a"), application.clone()).await?;
                let (other, other_url) = start_with(
                    &directory.path().join("b"),
                    application.clone(),
                    "scope-other",
                    None,
                )
                .await?;
                let checked = check(&client, &url, &other_url, &application).await;
                let other_shutdown = stop(other, &monitor).await;
                let shutdown = stop(node, &monitor).await;
                let error = format!("{:#}", checked.expect_err("controlled branch must fail"));
                for required in [
                    "controlled context failure",
                    "/scope-ready",
                    "/scope-hold",
                    "handler exceeded 250ms budget",
                    "stages=",
                    "interpreter=",
                ] {
                    ensure!(error.contains(required), "missing {required}: {error}");
                }
                other_shutdown?;
                shutdown?;
                Ok(())
            })
        },
    )?;
    ensure!(
        report.complete(),
        "controlled failures leaked work: {report:?}"
    );
    Ok(())
}
