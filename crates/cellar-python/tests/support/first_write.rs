// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Focused diagnostic: fresh SQLite fleet, one original-budget mutation, join.
use super::*;

pub(super) fn run_cases(iterations: usize) -> Result<()> {
    ensure!(
        (1..=1000).contains(&iterations),
        "iterations must be in 1..=1000"
    );
    let directory = tempfile::tempdir()?;
    let trace = first_write_trace::Trace::install()?;
    let case_trace = trace.clone();
    let root = directory.path().to_path_buf();
    let started = std::time::Instant::now();
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
                let (free_threaded, gil_enabled) = pyo3::Python::attach(|py| -> pyo3::PyResult<_> {
                    let sys = py.import("sys")?;
                    Ok((sys.getattr("abiflags")?.extract::<String>()?.contains('t'),
                        sys.getattr("_is_gil_enabled")?.call0()?.extract::<bool>()?))
                })?;
                ensure!(free_threaded != gil_enabled, "first-write probe changed interpreter mode");
                cellar::cli_output::Output::new(cellar::cli_output::Format::Text).line(format_args!("first-write interpreter: free_threaded={free_threaded}, gil_enabled={gil_enabled}"))?;
                let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build()?;
                for index in 0..iterations {
                    case_trace.clear();
                    let case = root.join(index.to_string());
                    std::fs::create_dir_all(&case)?;
                    let bucket = cellar::bucket::Bucket::open_dev(&case.join("objects.sqlite"))?;
                    let startup = std::time::Instant::now();
                    let (node, url) = start_with(&case.join("a"), application.clone(), "python-a", Some(bucket))
                        .await.with_context(|| format!("first-write case {index}: startup"))?;
                    let startup_elapsed = startup.elapsed();
                    let submitted = std::time::Instant::now();
                    let before = application.interpreter_snapshot();
                    // Exactly one mutation. A failure is never retried, and a
                    // subsequent case always owns a fresh fleet and directory.
                    let mutation = async {
                        let first: Value = response_json(client.post(format!("{url}/counter")).send().await.context("first mutation send")?).await.context("first mutation reply")?;
                        ensure!(first["count"] == 1, "unexpected first result: {first}");
                        let read: Value = response_json(client.get(format!("{url}/counter")).send().await.context("readback send")?).await.context("readback reply")?;
                        ensure!(read["count"] == 1, "first write did not remain visible: {read}");
                        Ok::<_, anyhow::Error>(())
                    }.await.with_context(|| format!("first-write case {index}: mutation/readback after {:?}, startup={startup_elapsed:?}, node_closed={}", submitted.elapsed(), node.is_closed()));
                    let observation = mutation.as_ref().err().map(|_| json!({
                        "before": before, "after": application.interpreter_snapshot()
                    }));
                    let shutdown = stop(node, &monitor).await;
                    if let Some(observation) = observation {
                        std::fs::write(case.join("interpreter.json"), serde_json::to_vec_pretty(&observation)?)?;
                    }
                    mutation?;
                    shutdown.with_context(|| format!("first-write case {index}: shutdown"))?;
                }
                Ok(())
            })
        },
    );
    let result = result.and_then(|report| {
        ensure!(
            report.complete(),
            "first-write diagnostic left incomplete work"
        );
        Ok(report)
    });
    match result {
        Ok(_) => {
            cellar::cli_output::Output::new(cellar::cli_output::Format::Text).line(format_args!("PASS {iterations} fresh-fleet first writes in {:?}; handler budget unchanged at 250 ms", started.elapsed()))?;
            Ok(())
        }
        Err(error) => {
            let retained = directory.keep();
            trace.save(&retained.join("diagnostic-tail.log"))?;
            Err(error.context(format!(
                "first-write diagnostic retained state at {}",
                retained.display()
            )))
        }
    }
}
