// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independent SDK source count/byte saturation over real local and peer routes.
mod gate;
mod matrix;

use anyhow::{ensure, Context as _, Result};
use cellar::{bucket::Bucket, sdk::*};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

use crate::{application::Composite, fleet};

const SOURCE: &str = "saturation-source";
const OWNER: &str = "saturation-owner";

#[derive(Clone, Copy)]
enum Limit {
    Requests,
    Bytes,
}

impl Limit {
    fn name(self) -> &'static str {
        match self {
            Self::Requests => "requests",
            Self::Bytes => "bytes",
        }
    }

    fn policy(self) -> RuntimePolicy {
        match self {
            Self::Requests => RuntimePolicy {
                max_outbound_rpc_requests: 1,
                ..Default::default()
            },
            Self::Bytes => RuntimePolicy {
                max_outbound_rpc_bytes: 128 * 1024,
                ..Default::default()
            },
        }
    }
}

async fn request(client: &reqwest::Client, base: &str, path: &str) -> Result<Value> {
    let response = client.get(format!("{base}{path}")).send().await?;
    let status = response.status();
    let text = response.text().await?;
    ensure!(
        status.is_success(),
        "saturation ingress {path}: {status}: {text}"
    );
    serde_json::from_str(&text).context("saturation ingress returned invalid JSON")
}

async fn topology(app: Arc<Composite>, remote: bool, limit: Limit) -> Result<Value> {
    app.audit.reset_after_node_drain();
    let directory = tempfile::tempdir()?;
    let bucket = Bucket::open_dev(&directory.path().join("fleet.sqlite"))?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    let gate = Arc::new(gate::Gate::default());
    let application = Arc::new(gate::Application {
        inner: app.clone(),
        gate: gate.clone(),
    });
    let mut nodes = Vec::new();
    let result = async {
        let owner_name = if remote { OWNER } else { SOURCE };
        let policy = limit.policy();
        let (owner, owner_url) = fleet::start_configured(
            &directory.path().join("owner"),
            application.clone(),
            owner_name,
            bucket.clone(),
            None,
            |config, _| {
                if !remote {
                    config.runtime.max_outbound_rpc_requests = policy.max_outbound_rpc_requests;
                    config.runtime.max_outbound_rpc_bytes = policy.max_outbound_rpc_bytes;
                }
                Ok(())
            },
        )
        .await?;
        nodes.push(owner);
        for (provider, _, _) in fleet::CLASSES {
            ensure!(
                request(
                    &client,
                    &owner_url,
                    &format!("/rust/read?target={provider}")
                )
                .await?
                    == json!({"value":"0"}),
                "seed changed state"
            );
        }
        let base = if remote {
            let (source, base) = fleet::start_configured(
                &directory.path().join("source"),
                application,
                SOURCE,
                bucket,
                None,
                |config, _| {
                    config.runtime.max_outbound_rpc_requests = policy.max_outbound_rpc_requests;
                    config.runtime.max_outbound_rpc_bytes = policy.max_outbound_rpc_bytes;
                    Ok(())
                },
            )
            .await?;
            nodes.push(source);
            request(&client, &base, "/rust/read?target=rust").await?;
            base
        } else {
            owner_url
        };
        matrix::cases(&app, &client, &base, &gate, remote, limit).await
    }
    .await;
    let mut cleanup = Vec::new();
    for node in nodes.into_iter().rev() {
        if let Err(error) = fleet::stop(node, ShutdownMode::Preserve).await {
            cleanup.push(format!("{error:#}"));
        }
    }
    if let Ok(services) = gate.services() {
        if services.outbound_rpc_usage() != Default::default() {
            cleanup.push("source retained SDK RPC reservations after node drain".into());
        }
    }
    ensure!(
        cleanup.is_empty(),
        "saturation cleanup failed: {}; case: {result:?}",
        cleanup.join("; ")
    );
    result
}

pub async fn run(app: Arc<Composite>) -> Result<Value> {
    let mut cases = Vec::new();
    for remote in [false, true] {
        for limit in [Limit::Requests, Limit::Bytes] {
            let report = topology(app.clone(), remote, limit).await?;
            cases.extend(
                report
                    .as_array()
                    .context("saturation cases are not an array")?
                    .iter()
                    .cloned(),
            );
        }
    }
    ensure!(cases.len() == 64, "source saturation matrix incomplete");
    Ok(
        json!({"format":1,"topology":"local-and-signed-peer-source-saturation", "contract_digest":app.contract.digest(),
        "case_count":cases.len(),"cases":cases,"native_nodes_drained":true,"source_reservations_after_drain":0,
        "all_memory_qualified":false,"release_qualified":false}),
    )
}
