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

#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Source,
    Receiver,
}

impl Side {
    fn name(self) -> &'static str {
        match self {
            Self::Source => "source",
            Self::Receiver => "receiver",
        }
    }

    fn configure(self, config: &mut RuntimePolicy, limit: Limit) {
        let (requests, bytes) = match limit {
            Limit::Requests => (1, 256 * 1024 * 1024),
            Limit::Bytes => (128, 128 * 1024),
        };
        match self {
            Self::Source => {
                config.max_outbound_rpc_requests = requests;
                config.max_outbound_rpc_bytes = bytes;
            }
            Self::Receiver => {
                config.max_inbound_rpc_requests = requests;
                config.max_inbound_rpc_bytes = bytes;
            }
        }
    }

    fn usage(
        self,
        source: &cellar::host_services::HostServices,
        owner: &cellar::host_services::HostServices,
    ) -> cellar::host_services::OutboundRpcUsage {
        if self == Self::Source {
            source.outbound_rpc_usage()
        } else {
            let usage = owner.inbound_rpc_usage();
            cellar::host_services::OutboundRpcUsage {
                active_requests: usage.active_requests,
                reserved_request_bytes: usage.reserved_request_bytes,
            }
        }
    }
}

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

async fn topology(app: Arc<Composite>, remote: bool, limit: Limit, side: Side) -> Result<Value> {
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
        let (owner, owner_url) = fleet::start_configured(
            &directory.path().join("owner"),
            application.clone(),
            owner_name,
            bucket.clone(),
            None,
            |config, _| {
                if !remote || side == Side::Receiver {
                    side.configure(&mut config.runtime, limit);
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
                    if side == Side::Source {
                        side.configure(&mut config.runtime, limit);
                    }
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
        matrix::cases(&app, &client, &base, &gate, remote, limit, side).await
    }
    .await;
    let mut cleanup = Vec::new();
    for node in nodes.into_iter().rev() {
        if let Err(error) = fleet::stop(node, ShutdownMode::Preserve).await {
            cleanup.push(format!("{error:#}"));
        }
    }
    for services in gate.observed_services() {
        if services.outbound_rpc_usage() != Default::default()
            || services.inbound_rpc_usage() != Default::default()
        {
            cleanup.push("node retained SDK or peer RPC reservations after drain".into());
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
    for (side, remote) in [
        (Side::Source, false),
        (Side::Source, true),
        (Side::Receiver, true),
    ] {
        for limit in [Limit::Requests, Limit::Bytes] {
            let report = topology(app.clone(), remote, limit, side).await?;
            cases.extend(
                report
                    .as_array()
                    .context("saturation cases are not an array")?
                    .iter()
                    .cloned(),
            );
        }
    }
    ensure!(
        cases.len() == 96,
        "source/receiver saturation matrix incomplete"
    );
    Ok(
        json!({"format":1,"topology":"local-and-signed-peer-rpc-saturation", "contract_digest":app.contract.digest(),
        "case_count":cases.len(),"cases":cases,"native_nodes_drained":true,"source_reservations_after_drain":0,
        "receiver_reservations_after_drain":0,"receiver_tunnels_after_drain":0,
        "all_memory_qualified":false,"release_qualified":false}),
    )
}
