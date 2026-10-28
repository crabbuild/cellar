// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

#![cfg(any(feature = "embed", feature = "node"))]
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use cellar_javascript::{EventKind, JavaScriptApplication, Options, Source};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};
#[cfg(feature = "embed")]
use std::time::Duration;
#[path = "../../cellar/tests/support/proof_fault_gates.rs"]
#[allow(dead_code)] // Shared proof fixtures select different fault modes.
mod fault_gates;
#[path = "../../cellar/tests/support/filesystem_lifetime.rs"]
mod lifetime;
#[path = "support/native.rs"]
mod native;
#[path = "support/filesystem_proof.rs"]
mod proof;

static HOST: Mutex<()> = Mutex::new(());

fn code(barrier: &str) -> Result<String> {
    let hashes: Vec<_> = (1..=3)
        .map(|generation| format!("{:x}", Sha256::digest(proof::payload(generation))))
        .collect();
    Ok(format!(
        "const proofBarrier = {};\nconst proofHashes = {};\n{}\n{}",
        serde_json::to_string(barrier)?,
        serde_json::to_string(&hashes)?,
        include_str!("../../../packages/cellar-sdk/index.js").replace("export ", ""),
        include_str!("support/filesystem-proof.js")
    ))
}

fn registration(source: Source) -> Result<Source> {
    source
        .events([EventKind::Fetch])
        .cell("Counter")
        .filesystem("Counter", cellar::sdk::filesystem::Policy::default())
}

fn joined(
    engine: &str,
    barrier: &str,
    fence: bool,
    handoff: bool,
    retained: Option<Arc<JavaScriptApplication>>,
    result: Result<Value>,
) -> Result<()> {
    let usage = retained
        .context("runtime did not admit the proof host")?
        .bridge_usage();
    let mut report =
        result.with_context(|| format!("{engine} proof bridge after join: {usage:?}"))?;
    ensure!(
        usage.stopped && usage.active_invocations == 0 && usage.retained_bytes == 0,
        "proof runtime leaked after join: {usage:?}"
    );
    report["bridge_after_join"] = serde_json::to_value(usage)?;
    report["engine"] = json!(engine);
    if let Some(directory) = std::env::var_os("CELLAR_FS_JS_PROOF_REPORT_DIR") {
        let directory = std::path::PathBuf::from(directory);
        std::fs::create_dir_all(&directory)?;
        let file = format!(
            "{engine}-{barrier}-{}-{}.json",
            if fence { "fence" } else { "deadline" },
            if handoff { "handoff" } else { "preserve" }
        );
        std::fs::write(directory.join(file), serde_json::to_vec_pretty(&report)?)?;
    }
    Ok(())
}

#[cfg(feature = "node")]
fn run_node(barrier: &str, fence: bool, handoff: bool) -> Result<()> {
    let _host = HOST.lock().unwrap();
    let directory = tempfile::tempdir()?;
    let entry = directory.path().join("filesystem-proof.mjs");
    std::fs::write(&entry, format!("{}\nexport default app;", code(barrier)?))?;
    let mut retained = None;
    let result = cellar_javascript::NodeRuntime::run(
        Options::default(),
        cellar_javascript::NodeOptions::new(entry),
        registration(Source::module())?,
        |application| {
            retained = Some(application.clone());
            proof::exercise(application, barrier, fence, handoff)
        },
    );
    joined("node", barrier, fence, handoff, retained, result)
}

#[cfg(feature = "embed")]
fn run_embedded(barrier: &str, fence: bool, handoff: bool) -> Result<()> {
    let _host = HOST.lock().unwrap();
    let mut retained = None;
    let result = cellar_javascript::JavaScriptRuntime::run(
        Options {
            turn_timeout: Duration::from_secs(1),
            ..Options::default()
        },
        registration(Source::new(
            format!("{}\nvar CellarApp=app;", code(barrier)?),
            "filesystem-proof.js",
        ))?,
        |application| {
            retained = Some(application.clone());
            proof::exercise(application, barrier, fence, handoff)
        },
    );
    joined("embedded", barrier, fence, handoff, retained, result)
}
macro_rules! scenario {
    ($run:ident, $name:ident, $barrier:literal, $fence:expr, $handoff:expr) => {
        #[test]
        fn $name() -> Result<()> {
            $run($barrier, $fence, $handoff)
        }
    };
}
macro_rules! inventory {
    ($run:ident) => {
        scenario!($run, fsync_deadline_preserve, "fsync", false, false);
        scenario!($run, fsync_fence_preserve, "fsync", true, false);
        scenario!($run, fsync_deadline_handoff, "fsync", false, true);
        scenario!($run, fsync_fence_handoff, "fsync", true, true);
        scenario!($run, fdatasync_deadline_preserve, "fdatasync", false, false);
        scenario!($run, fdatasync_fence_preserve, "fdatasync", true, false);
        scenario!($run, fdatasync_deadline_handoff, "fdatasync", false, true);
        scenario!($run, fdatasync_fence_handoff, "fdatasync", true, true);
        scenario!($run, directory_deadline_preserve, "dirsync", false, false);
        scenario!($run, directory_fence_preserve, "dirsync", true, false);
        scenario!($run, directory_deadline_handoff, "dirsync", false, true);
        scenario!($run, directory_fence_handoff, "dirsync", true, true);
        scenario!($run, sync_deadline_preserve, "sync", false, false);
        scenario!($run, sync_fence_preserve, "sync", true, false);
        scenario!($run, sync_deadline_handoff, "sync", false, true);
        scenario!($run, sync_fence_handoff, "sync", true, true);
        scenario!($run, output_deadline_preserve, "output", false, false);
        scenario!($run, output_fence_preserve, "output", true, false);
        scenario!($run, output_deadline_handoff, "output", false, true);
        scenario!($run, output_fence_handoff, "output", true, true);
    };
}
#[cfg(feature = "node")]
mod node {
    use super::*;
    inventory!(run_node);
}
#[cfg(feature = "embed")]
mod embedded {
    use super::*;
    inventory!(run_embedded);
}
