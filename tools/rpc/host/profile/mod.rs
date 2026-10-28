// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Live portable-profile qualification with independent generated language clients.
mod application;
mod matrix;
mod native;

use anyhow::{ensure, Context as _, Result};
use cellar::sdk::*;
use cellar_javascript::{EventKind, JavaScriptRuntime, NodeOptions, NodeRuntime, Options, Source};
use cellar_python::{
    EventKind as PythonEvent, Options as PythonOptions, PythonRuntime, Source as PythonSource,
};
use serde_json::Value;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

const SERVICE: &str = "cellar.rpc.conformance.v1.Profile";

#[derive(serde::Deserialize)]
struct Vector {
    id: String,
    method: String,
    r#type: String,
    bytes: Vec<u8>,
}

fn vectors(contract: &rpc::Contract) -> Result<Arc<Vec<Vector>>> {
    let vectors: Vec<Vector> =
        serde_json::from_str(include_str!("../../fixtures/profile-vectors.json"))?;
    ensure!(vectors.len() == 18, "profile corpus is incomplete");
    let mut names = std::collections::BTreeSet::new();
    let mut methods = std::collections::BTreeSet::new();
    for vector in &vectors {
        ensure!(
            names.insert(&vector.id)
                && !vector.id.is_empty()
                && vector.id.len() <= 64
                && vector
                    .id
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'),
            "invalid profile vector identity"
        );
        let method = contract
            .method(&format!("{SERVICE}/{}", vector.method))
            .context("unknown profile method")?;
        ensure!(
            method.input == vector.r#type && method.output == vector.r#type,
            "profile vector uses another schema"
        );
        contract.validate_message(&vector.r#type, &vector.bytes)?;
        native::equivalent(vector, &vector.bytes)?;
        methods.insert(vector.method.clone());
    }
    ensure!(
        methods.len() == 6,
        "profile corpus misses a generated method"
    );
    Ok(Arc::new(vectors))
}

pub(super) fn execute(expected_mode: crate::python_mode::Mode) -> Result<Value> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../..")
        .canonicalize()?;
    let contract = Arc::new(rpc::Contract::from_descriptor(
        include_bytes!("../../fixtures/profile.binpb"),
        rpc::Limits::default(),
    )?);
    let vectors = vectors(&contract)?;
    let mut python_options = PythonOptions::default();
    python_options.module_paths.push(
        root.join("tools/rpc/fixtures/profile-generated/python")
            .canonicalize()?,
    );
    python_options
        .module_paths
        .push(root.join("tools/rpc/profile").canonicalize()?);
    let site = std::env::var_os("CELLAR_RPC_PYTHON_SITE")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join(".cellar/rpc/python/lib/python3.14/site-packages"));
    python_options.module_paths.push(site.canonicalize()?);
    let python_source =
        PythonSource::new(include_str!("../../profile/live.py"), "rpc/profile/live.py")
            .events([PythonEvent::Fetch])
            .cell_rpc("PythonCounter", SERVICE, contract.clone())?
            .require_distribution("protobuf", "7.34.2");
    let source = Source::new(
        std::fs::read_to_string(root.join("tools/rpc/dist/profile.v8.js"))
            .context("build RPC fixtures before live profile qualification")?,
        "rpc/profile/v8.ts",
    )
    .events([EventKind::Fetch])
    .cell_rpc("V8Counter", SERVICE, contract.clone())?;
    let node_source = Source::module().events([EventKind::Fetch]).cell_rpc(
        "NodeCounter",
        SERVICE,
        contract.clone(),
    )?;
    let node_options = NodeOptions::new(
        root.join("tools/rpc/dist/node/profile/node.mjs")
            .canonicalize()?,
    );
    let options = Options {
        turn_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    let (send, receive) = std::sync::mpsc::sync_channel(1);
    let retained = Arc::new(Mutex::new(Vec::new()));
    let observed = retained.clone();
    // SAFETY: this dedicated suite owns process initialization on the main
    // thread. No earlier Python runtime exists; this callback owns host work.
    let runtime = unsafe {
        PythonRuntime::run(python_options, python_source, move |python, _| {
            let mode = crate::python_mode::Check::begin(expected_mode)?;
            JavaScriptRuntime::run(options.clone(), source, |v8| {
                observed.lock().unwrap().push(("v8", v8.clone()));
                NodeRuntime::run(options, node_options, node_source, |node| {
                    observed.lock().unwrap().push(("node", node.clone()));
                    let app = Arc::new(application::Composite::new(
                        vec![
                            (
                                "rust",
                                "RustCounter",
                                native::application(contract.clone())?,
                            ),
                            ("python", "PythonCounter", python),
                            ("v8", "V8Counter", v8),
                            ("node", "NodeCounter", node),
                        ],
                        contract,
                        vectors.clone(),
                    )?);
                    let mut proof = tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(4)
                        .enable_all()
                        .build()?
                        .block_on(matrix::run(app, vectors))?;
                    mode.record(&mut proof, true)?;
                    send.send(proof).context("profile result receiver closed")?;
                    Ok(())
                })
            })
        })
    };
    let usage: Vec<_> = retained
        .lock()
        .unwrap()
        .iter()
        .map(|(name, app)| (*name, app.bridge_usage()))
        .collect();
    let report = runtime.with_context(|| format!("profile joined bridges={usage:?}"))?;
    ensure!(
        report.complete(),
        "profile Python runtime did not join completely"
    );
    ensure!(
        usage.len() == 2
            && usage.iter().all(|(_, usage)| usage.stopped
                && usage.active_invocations == 0
                && usage.retained_bytes == 0),
        "profile retained JavaScript resources: {usage:?}"
    );
    let mut result = receive
        .try_recv()
        .context("profile proof missing after runtime join")?;
    result["language_runtimes_joined"] = Value::Bool(true);
    result["bridge_reservations_after_join"] = Value::from(0);
    Ok(result)
}
