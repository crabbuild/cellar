// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::super::{application::Fixture, matrix::request, trace::Trace};
use super::{DELTA, OWNER};
use crate::{application::Composite, fleet};
use anyhow::{ensure, Context as _, Result};
use cellar::{bucket::Bucket, sdk::ShutdownMode};
use cellar_python::PythonApplication;
use serde_json::{json, Value};
use std::{path::Path, sync::Arc};

pub(super) async fn run(
    base: Arc<Composite>,
    python: Arc<PythonApplication>,
    root: &Path,
    client: &reqwest::Client,
    operation: &str,
    before: i64,
) -> Result<Value> {
    let trace = Arc::new(Trace::default());
    let app = Arc::new(Fixture {
        base,
        worker: python,
        trace: trace.clone(),
        provider_class: "PythonCounter",
    });
    let bucket = Bucket::open_dev(&root.join("fleet.sqlite"))?;
    let (node, url) = fleet::start_configured(
        &root.join("owner"),
        app,
        OWNER,
        bucket,
        None,
        super::super::configure_lifecycle,
    )
    .await?;
    let result = async {
        let before = before.to_string();
        ensure!(request(client, &url, "rust", "read", &[("target","python")]).await? == json!({"value":before}),
            "Python recovery lost or fabricated an effect");
        let delta = DELTA.to_string();
        for _ in 0..2 {
            ensure!(request(client, &url, "rust", "increment", &[("target","python"),("id",operation),("delta",&delta)]).await?
                == json!({"value":delta}), "Python receipt retry duplicated an effect");
        }
        let prior = trace.late()?["ordinal"].as_u64().context("native read has no observation ordinal")?;
        ensure!(request(client, &url, "rust", "read", &[("target","python")]).await? == json!({"value":delta}),
            "Python recovery final count differs");
        let storage = trace.late()?;
        ensure!(storage["ordinal"].as_u64().is_some_and(|ordinal| ordinal > prior)
            && storage["node"] == OWNER && storage["class"] == "PythonCounter"
            && storage["epoch"].as_u64().is_some_and(|epoch| epoch > 0)
            && storage["count"] == delta && storage["late"].is_null(),
            "late Python storage write survived recovery: {storage}");
        ensure!(request(client, &url, "rust", "read", &[("target","rust"),("cell","python-late")]).await? == json!({"value":"0"}),
            "late Python RPC mutated its target");
        Ok::<_, anyhow::Error>(json!({"before_retry":before,"after_two_explicit_retries":delta,
            "fresh_native_owner":true,"native_storage":storage,"late_rpc_count":"0","receipt_verified":true}))
    }.await;
    let stopped = fleet::stop(node, ShutdownMode::Preserve).await;
    let result = result?;
    stopped?;
    Ok(result)
}
