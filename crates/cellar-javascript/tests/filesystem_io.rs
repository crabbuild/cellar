// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![cfg(all(feature = "node", unix))]
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use cellar_javascript::{
    EventKind, JavaScriptApplication, NodeOptions, NodeRuntime, Options, Source,
};
use serde_json::json;
use std::{
    os::unix::fs::PermissionsExt,
    sync::{Arc, Mutex},
};
#[path = "../../cellar/tests/support/proof_fault_gates.rs"]
#[allow(dead_code)]
mod fault_gates;
#[path = "support/filesystem_io.rs"]
mod io;
#[path = "../../cellar/tests/support/filesystem_lifetime.rs"]
mod lifetime;
#[path = "support/native.rs"]
mod native;

static HOST: Mutex<()> = Mutex::new(());

fn exercise(operation: &str, fence: bool, handoff: bool) -> Result<()> {
    let _host = HOST.lock().unwrap();
    let directory = tempfile::tempdir()?;
    let entry = directory.path().join("filesystem-io.mjs");
    std::fs::write(
        &entry,
        format!(
            "const ioOperation = {};\n{}\n{}",
            serde_json::to_string(operation)?,
            include_str!("../../../packages/cellar-sdk/index.js").replace("export ", ""),
            include_str!("support/filesystem-io.mjs")
        ),
    )?;
    let relay = directory.path().join("filesystem-io-relay.mjs");
    std::fs::write(&relay, include_str!("support/filesystem-io-relay.mjs"))?;
    std::fs::set_permissions(&relay, std::fs::Permissions::from_mode(0o700))?;
    let report = directory.path().join("relay.json");
    let release = directory.path().join("release");
    let mut node = NodeOptions::new(entry);
    node.executable = relay;
    for (key, value) in [
        (
            "CELLAR_FS_IO_OPERATION",
            if operation == "read_exact" {
                "read"
            } else if operation == "write_all" {
                "write"
            } else {
                operation
            }
            .to_owned(),
        ),
        (
            "CELLAR_FS_IO_OCCURRENCE",
            if operation.ends_with("_all") || operation.ends_with("_exact") {
                "2"
            } else {
                "1"
            }
            .into(),
        ),
        (
            "CELLAR_FS_IO_RELAY_REPORT",
            report.to_string_lossy().into_owned(),
        ),
        (
            "CELLAR_FS_IO_RELAY_RELEASE",
            release.to_string_lossy().into_owned(),
        ),
    ] {
        node.environment.insert(key.into(), value);
    }
    let source = Source::module()
        .events([EventKind::Fetch])
        .cell("Counter")
        .filesystem("Counter", cellar::sdk::filesystem::Policy::default())?;
    let mut retained: Option<Arc<JavaScriptApplication>> = None;
    let result = NodeRuntime::run(Options::default(), node, source, |app| {
        retained = Some(app.clone());
        io::exercise(
            app,
            directory.path(),
            &report,
            &release,
            operation,
            fence,
            handoff,
        )
    });
    let usage = retained.context("IO host was not admitted")?.bridge_usage();
    let mut result = result.with_context(|| {
        format!(
            "IO bridge after join: {usage:?}; relay: {:?}",
            std::fs::read_to_string(&report)
        )
    })?;
    ensure!(
        usage.stopped && usage.active_invocations == 0 && usage.retained_bytes == 0,
        "IO bridge did not join: {usage:?}"
    );
    result["bridge_after_join"] = serde_json::to_value(usage)?;
    result["operation"] = json!(operation);
    result["engine"] = json!("node");
    if let Some(path) = std::env::var_os("CELLAR_FS_JS_PROOF_REPORT_DIR") {
        let path = std::path::PathBuf::from(path);
        std::fs::create_dir_all(&path)?;
        std::fs::write(
            path.join(format!(
                "node-io-{operation}-{}-{}.json",
                if fence { "fence" } else { "deadline" },
                if handoff { "handoff" } else { "preserve" }
            )),
            serde_json::to_vec_pretty(&result)?,
        )?;
    }
    Ok(())
}

macro_rules! scenario {
    ($name:ident, $op:literal, $fence:expr, $handoff:expr) => {
        #[test]
        fn $name() -> Result<()> {
            exercise($op, $fence, $handoff)
        }
    };
}
macro_rules! operation {
    ($module:ident, $name:literal) => {
        mod $module {
            use super::*;
            scenario!(deadline_preserve, $name, false, false);
            scenario!(fence_preserve, $name, true, false);
            scenario!(deadline_handoff, $name, false, true);
            scenario!(fence_handoff, $name, true, true);
        }
    };
}
operation!(read, "read");
operation!(pread, "pread");
operation!(write, "write");
operation!(pwrite, "pwrite");
operation!(read_exact, "read_exact");
operation!(write_all, "write_all");
