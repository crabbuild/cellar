// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![cfg(any(feature = "node", feature = "embed"))]
use anyhow::{ensure, Result};
use cellar::sdk::{filesystem::Policy, NodeOutcome, ShutdownMode};
use cellar_javascript::{EventKind, Options, Source};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

static HOST: Mutex<()> = Mutex::new(());
#[path = "../../cellar/tests/support/filesystem_guest.rs"]
mod conformance;
#[path = "support/native.rs"]
mod native;
#[path = "../../cellar/tests/support/filesystem_readonly.rs"]
mod readonly;

fn source() -> String {
    format!(
        "const filesystemReservedCases = {};\n{}\n{}\n{}\n{}\n{}\n{}",
        include_str!("../../cellar/tests/support/filesystem_reserved_paths.json"),
        include_str!("../../../packages/cellar-sdk/index.js").replace("export ", ""),
        include_str!("../../../packages/cellar-sdk/filesystem-tools.js")
            .replace("import { bytesToBase64, utf8 } from \"./index.js\";", "")
            .replace("export ", ""),
        include_str!("../../../examples/filesystem-agent/session.js")
            .replace("import { jsonResponse, utf8 } from \"@cellar/sdk\";", "")
            .replace(
                "import { createFilesystemTools, validateFilesystemCheckpoint } from \"@cellar/sdk/filesystem-tools\";",
                ""
            )
            .replace("export ", ""),
        include_str!("../../../examples/filesystem-agent/model.js").replace("export ", ""),
        include_str!("support/filesystem-concurrency.js"),
        include_str!("support/filesystem.js")
    )
}

fn registration(source: Source) -> Result<Source> {
    source
        .events([EventKind::Fetch])
        .cell("Counter")
        .filesystem("Counter", Policy::default())?
        .cell_events("AgentSession", [EventKind::Fetch, EventKind::Alarm])
        .filesystem("AgentSession", Policy::default())
}

fn readonly_registration(source: Source) -> Result<Source> {
    source
        .events([EventKind::Fetch])
        .cell("Counter")
        .filesystem(
            "Counter",
            Policy {
                read_only: true,
                ..Policy::default()
            },
        )
}

fn exercise_readonly(application: Arc<cellar_javascript::JavaScriptApplication>) -> Result<()> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(readonly::exercise(application, |_| {}))
}

#[cfg(feature = "node")]
#[test]
#[allow(clippy::disallowed_methods)]
fn node_filesystem_readonly_recovers_writer_state() -> Result<()> {
    let _host = HOST.lock().unwrap();
    let directory = tempfile::tempdir()?;
    let entry = directory.path().join("readonly.mjs");
    std::fs::write(&entry, format!("{}\nexport default app;", source()))?;
    cellar_javascript::NodeRuntime::run(
        Options::default(),
        cellar_javascript::NodeOptions::new(entry),
        readonly_registration(Source::module())?,
        exercise_readonly,
    )
}

#[cfg(feature = "embed")]
#[test]
fn embedded_filesystem_readonly_recovers_writer_state() -> Result<()> {
    let _host = HOST.lock().unwrap();
    cellar_javascript::JavaScriptRuntime::run(
        Options {
            turn_timeout: Duration::from_secs(1),
            ..Options::default()
        },
        readonly_registration(Source::new(
            format!("{}\nvar CellarApp=app;", source()),
            "readonly.js",
        ))?,
        exercise_readonly,
    )
}

fn exercise(application: Arc<cellar_javascript::JavaScriptApplication>) -> Result<()> {
    let (application, cancellation) = conformance::observe(application);
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let directory = tempfile::tempdir()?;
            let mut agent_version = None;
            for recovered in [false, true] {
                let (node, address) = native::start_with_configuration(
                    directory.path(),
                    application.clone(),
                    vec!["Counter".into(), "AgentSession".into()],
                    vec![
                        ("COUNTERS".into(), "Counter".into()),
                        ("AGENTS".into(), "AgentSession".into()),
                    ],
                    |config, _, _| {
                        config.runtime.handler_budget = Duration::from_secs(2);
                        config.cell_limits.filesystem.max_host_handles = 4;
                    },
                )
                .await?;
                let result = if recovered {
                    conformance::recovered(&address).await
                } else {
                    conformance::initial(&address, &cancellation).await
                };
                let result = async {
                    result?;
                    if recovered {
                        conformance::tools_recovered(&address).await
                    } else {
                        conformance::tools(&address).await
                    }
                }
                .await;
                let result = async {
                    result?;
                    if recovered {
                        conformance::session_recovered(
                            &address,
                            agent_version.as_deref().expect("initial session verified"),
                        )
                        .await
                    } else {
                        agent_version = Some(conformance::session_initial(&address).await?);
                        Ok(())
                    }
                }
                .await;
                let outcome = node.shutdown(ShutdownMode::Preserve).await?;
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
}

#[cfg(feature = "node")]
#[test]
#[allow(clippy::disallowed_methods)]
fn node_filesystem_posix_batches_and_recovery() -> Result<()> {
    let _host = HOST.lock().unwrap();
    let directory = tempfile::tempdir()?;
    let entry = directory.path().join("app.mjs");
    std::fs::write(&entry, format!("{}\nexport default app;", source()))?;
    cellar_javascript::NodeRuntime::run(
        Options::default(),
        cellar_javascript::NodeOptions::new(entry),
        registration(Source::module())?,
        exercise,
    )
}

#[cfg(feature = "embed")]
#[test]
fn embedded_filesystem_posix_batches_and_recovery() -> Result<()> {
    let _host = HOST.lock().unwrap();
    cellar_javascript::JavaScriptRuntime::run(
        // This conformance fixture combines SDK/tool/session bundles and root
        // plus cell invocations in one pump. Keep its allowance explicit;
        // runtime_tests separately verifies termination at a 40 ms limit.
        Options {
            turn_timeout: Duration::from_secs(1),
            ..Options::default()
        },
        registration(Source::new(
            format!("{}\nvar CellarApp=app;", source()),
            "filesystem.js",
        ))?,
        exercise,
    )
}

#[cfg(feature = "embed")]
#[test]
fn embedded_filesystem_empty_helper_refusals_settle() -> Result<()> {
    let _host = HOST.lock().unwrap();
    let code = format!(
        "{}\n{}",
        include_str!("../../../packages/cellar-sdk/index.js").replace("export ", ""),
        include_str!("support/filesystem-helper-errors.js")
    );
    cellar_javascript::JavaScriptRuntime::run(
        Options::default(),
        Source::new(code, "filesystem-helper-errors.js")
            .events([EventKind::Fetch])
            .cell("Counter")
            .filesystem("Counter", Policy::default())?,
        |application| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(async {
                    let state = tempfile::tempdir()?;
                    let (node, address) =
                        native::start_configured(state.path(), application, |config, _, _| {
                            config.runtime.handler_budget = Duration::from_secs(2)
                        })
                        .await?;
                    let reply = reqwest::get(format!("{address}/helpers")).await?;
                    let status = reply.status();
                    let body = reply.text().await?;
                    let outcome = node.shutdown(ShutdownMode::Preserve).await?;
                    ensure!(
                        status.is_success() && body == "ok",
                        "helper refusal failed: {status} {body}; {outcome:?}"
                    );
                    let NodeOutcome::Stopped(report) = outcome else {
                        anyhow::bail!("helper node failed")
                    };
                    ensure!(
                        report.application_work_drained && !report.drain_timed_out,
                        "helper work leaked"
                    );
                    Ok(())
                })
        },
    )
}

#[cfg(feature = "node")]
#[test]
#[allow(clippy::disallowed_methods)]
fn node_filesystem_encoded_budget_preserves_offsets_and_directory_cursors() -> Result<()> {
    let _host = HOST.lock().unwrap();
    let directory = tempfile::tempdir()?;
    let entry = directory.path().join("bounded.mjs");
    std::fs::write(
        &entry,
        format!(
            "{}\n{}\n{}\nexport default budgetApp;",
            include_str!("../../../packages/cellar-sdk/index.js").replace("export ", ""),
            include_str!("../../../packages/cellar-sdk/filesystem-tools.js")
                .replace("import { bytesToBase64, utf8 } from \"./index.js\";", "")
                .replace("export ", ""),
            include_str!("support/filesystem-budget.js")
        ),
    )?;
    let options = Options {
        max_value_bytes: 8192,
        ..Options::default()
    };
    cellar_javascript::NodeRuntime::run(
        options,
        cellar_javascript::NodeOptions::new(entry),
        Source::module()
            .events([EventKind::Fetch])
            .cell("Counter")
            .filesystem("Counter", Policy::default())?,
        |application| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?
                .block_on(async {
                    let state = tempfile::tempdir()?;
                    let (node, address) =
                        native::start_configured(state.path(), application, |config, _, _| {
                            config.runtime.handler_budget = Duration::from_secs(2);
                            config.cell_limits.filesystem.max_transfer_bytes = 1024;
                            config.cell_limits.filesystem.max_file_bytes = 32768;
                        })
                        .await?;
                    let reply = reqwest::get(format!("{address}/budget")).await?;
                    let status = reply.status();
                    let body = reply.text().await?;
                    let stopped = node.shutdown(ShutdownMode::Preserve).await?;
                    ensure!(
                        status.is_success() && body == "ok",
                        "bounded filesystem failed: {status} {body}"
                    );
                    let NodeOutcome::Stopped(report) = stopped else {
                        anyhow::bail!("bounded filesystem node failed")
                    };
                    ensure!(
                        report.application_work_drained && !report.drain_timed_out,
                        "bounded filesystem leaked work"
                    );
                    Ok(())
                })
        },
    )
}
