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

fn main() -> Result<()> {
    let small_budget = std::env::args().any(|argument| argument == "--small-budget");
    let read_only = std::env::args().any(|argument| argument == "--read-only");
    ensure!(
        !(small_budget && read_only),
        "select one filesystem test mode"
    );
    let code = if small_budget {
        include_str!("filesystem_budget.py").to_owned()
    } else {
        format!(
            "FILESYSTEM_RESERVED_CASES = __import__('json').loads({})\n{}",
            serde_json::to_string(include_str!(
                "../../cellar/tests/support/filesystem_reserved_paths.json"
            ))?,
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
                        for recovered in [false, true] {
                            let (node, address) = native::start_configured(
                                directory.path(),
                                application.clone(),
                                |config, _, _| {
                                    config.runtime.handler_budget = Duration::from_secs(2);
                                    if small_budget {
                                        config.cell_limits.filesystem.max_transfer_bytes = 1024;
                                    } else {
                                        config.cell_limits.filesystem.max_host_handles = 4;
                                    }
                                },
                            )
                            .await?;
                            let result = async {
                                if small_budget {
                                    conformance::tools_budget(&address, recovered).await
                                } else if recovered {
                                    conformance::recovered(&address).await?;
                                    conformance::tools_recovered(&address).await
                                } else {
                                    conformance::initial(&address, &cancellation).await?;
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
