// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::{ensure, Result};
use cellar::sdk::{filesystem::Policy, NodeOutcome, ShutdownMode};
use cellar_python::{EventKind, Options, PythonRuntime, Source};
use std::time::Duration;
#[path = "../../cellar/tests/support/filesystem_guest.rs"]
mod conformance;
#[path = "../../cellar-javascript/tests/support/native.rs"]
mod native;
#[path = "../../cellar/tests/support/filesystem_readonly.rs"]
mod readonly;

#[allow(clippy::disallowed_methods)] // Qualification reports are supervisor-owned, outside SDK turns.
async fn task_cancellation(
    address: &str,
    recovered: bool,
    observations: &mut Vec<serde_json::Value>,
) -> Result<()> {
    let mut cases = Vec::new();
    for operation in ["write_all", "read_exact"] {
        for phase in [
            "before_second",
            "before_first",
            "after_second",
            "capabilities",
        ] {
            cases.push(format!("{operation}-{phase}"));
        }
    }
    for operation in ["read", "pread", "write", "pwrite"] {
        cases.push(format!("{operation}-after_native"));
    }
    for case in cases {
        let route = if recovered { "recovered" } else { "run" };
        let response = reqwest::get(format!("{address}/{case}/{route}")).await?;
        let status = response.status();
        let body = response.text().await?;
        let result = serde_json::from_str::<serde_json::Value>(&body)
            .unwrap_or_else(|_| serde_json::json!({"failure":body}));
        observations.push(serde_json::json!({"case":case,"recovered":recovered,"status":status.as_u16(),"result":result}));
        if let Some(path) = std::env::var_os("CELLAR_FS_PY_CANCELLATION_REPORT") {
            std::fs::write(
                path,
                serde_json::to_vec_pretty(
                    &serde_json::json!({"format":1,"observations":observations}),
                )?,
            )?;
        }
        ensure!(
            status.is_success()
                && result["checked"] == true
                && result["case"] == case
                && result["recovered"] == recovered,
            "task cancellation {case}/{route}: {status} {body}"
        );
    }
    Ok(())
}

fn main() -> Result<()> {
    let small_budget = std::env::args().any(|argument| argument == "--small-budget");
    let read_only = std::env::args().any(|argument| argument == "--read-only");
    let task_cancellation_mode = std::env::args().any(|argument| argument == "--task-cancellation");
    ensure!(
        usize::from(small_budget) + usize::from(read_only) + usize::from(task_cancellation_mode)
            <= 1,
        "select one filesystem test mode"
    );
    let code = if task_cancellation_mode {
        include_str!("filesystem_cancellation.py").to_owned()
    } else if small_budget {
        include_str!("filesystem_budget.py").to_owned()
    } else {
        format!(
            "FILESYSTEM_RESERVED_CASES = __import__('json').loads({})\nFILESYSTEM_REPLY_CASES = __import__('json').loads({})\n{}\n{}\n{}",
            serde_json::to_string(include_str!(
                "../../cellar/tests/support/filesystem_reserved_paths.json"
            ))?,
            serde_json::to_string(include_str!(
                "../../cellar/tests/support/filesystem_reply_cases.json"
            ))?,
            include_str!("filesystem_replies.py"),
            include_str!("filesystem_concurrency.py"),
            include_str!("filesystem.py")
        )
    };
    let source = Source::new(code, "filesystem.py")
        .events([EventKind::Fetch])
        .cell("Counter")
        .filesystem(
            "Counter",
            Policy {
                read_only,
                ..Policy::default()
            },
        )?;
    // SAFETY: this standalone test initializes CPython once on its main thread.
    let report = unsafe {
        PythonRuntime::run(
            Options {
                max_value_bytes: if small_budget {
                    8192
                } else {
                    Options::default().max_value_bytes
                },
                ..Options::default()
            },
            source,
            move |application, monitor| {
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?
                    .block_on(async {
                        if read_only {
                            return readonly::exercise(application, |outcome| {
                                monitor.record_node(outcome)
                            })
                            .await;
                        }
                        let (application, cancellation) = conformance::observe(application);
                        let directory = tempfile::tempdir()?;
                        let mut cancellation_observations = Vec::new();
                        for recovered in [false, true] {
                            let (node, address) = native::start_configured(
                                directory.path(),
                                application.clone(),
                                |config, _, _| {
                                    config.runtime.handler_budget = Duration::from_secs(2);
                                    if task_cancellation_mode {
                                        config.cell_limits.filesystem.max_transfer_bytes = 4;
                                        config.cell_limits.filesystem.max_host_handles = 4;
                                    } else if small_budget {
                                        config.cell_limits.filesystem.max_transfer_bytes = 1024;
                                    } else {
                                        config.cell_limits.filesystem.max_host_handles = 4;
                                    }
                                },
                            )
                            .await?;
                            let result = async {
                                if task_cancellation_mode {
                                    task_cancellation(
                                        &address,
                                        recovered,
                                        &mut cancellation_observations,
                                    )
                                    .await
                                } else if small_budget {
                                    conformance::tools_budget(&address, recovered).await
                                } else if recovered {
                                    conformance::recovered(&address).await?;
                                    conformance::tools_recovered(&address).await
                                } else {
                                    conformance::initial(&address, &cancellation).await?;
                                    let reply =
                                        reqwest::get(format!("{address}/one/reply-protocol"))
                                            .await?;
                                    let status = reply.status();
                                    let body = reply.text().await?;
                                    ensure!(
                                        status.is_success() && body == "ok",
                                        "filesystem reply protocol: {status} {body}"
                                    );
                                    conformance::tools(&address).await
                                }
                            }
                            .await;
                            let outcome = node.shutdown(ShutdownMode::Preserve).await?;
                            monitor.record_node(outcome.clone());
                            result?;
                            let NodeOutcome::Stopped(report) = outcome else {
                                anyhow::bail!("filesystem node failed")
                            };
                            ensure!(
                                report.application_work_drained && !report.drain_timed_out,
                                "filesystem work leaked: {report:?}"
                            );
                        }
                        Ok(())
                    })
            },
        )
    }?;
    ensure!(report.complete(), "Python filesystem teardown incomplete");
    Ok(())
}
