// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

// Process configuration and asset paths are CLI boundary operations.
#![allow(clippy::disallowed_methods)]

//! Process configuration and I/O belong to this binary, outside Cellar nodes.

use anyhow::{Context, Result};
use cellar_admin::{AuditStore, Console, Fleet, FleetInfo, Permission, Principal};
use serde::Deserialize;
use std::{collections::BTreeMap, net::SocketAddr, path::PathBuf};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    listen: SocketAddr,
    assets: PathBuf,
    audit: PathBuf,
    origins: Vec<String>,
    fleets: Vec<FleetConfig>,
    users: Vec<UserConfig>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FleetConfig {
    id: String,
    name: String,
    environment: String,
    bucket: Option<String>,
    endpoint: Option<String>,
    #[serde(default = "region")]
    region: String,
    local_store: Option<PathBuf>,
    #[serde(default)]
    allow_public_peers: bool,
    #[serde(default)]
    raft: Option<cellar::sdk::fleet_operations::RaftFleetConnection>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UserConfig {
    id: String,
    token_env: String,
    grants: BTreeMap<String, Vec<Permission>>,
}

fn region() -> String {
    "us-east-1".into()
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 2 || args[0] != "--config" {
        eprintln!("Usage: cellar-dashboard --config console.json");
        anyhow::ensure!(
            args.is_empty() || args == ["--help"],
            "expected --config FILE"
        );
        return Ok(());
    }
    let file = std::fs::canonicalize(&args[1]).context("locate dashboard configuration")?;
    let base = file
        .parent()
        .context("configuration has no parent directory")?;
    let config: Config = serde_json::from_slice(&std::fs::read(&file)?)?;
    let domain = std::sync::Arc::new(cellar::sdk::ExecutionDomain::new(
        tokio::runtime::Handle::current(),
    ));
    let router = domain
        .run(async {
            let mut fleets = BTreeMap::new();
            for fleet in config.fleets {
                let bucket = match (fleet.local_store, fleet.bucket) {
                    (Some(path), None) => cellar::dev::open_local_bucket(&base.join(path))?,
                    (None, Some(bucket)) => cellar::fleet::bucket_client(
                        &bucket,
                        fleet.endpoint.as_deref(),
                        &fleet.region,
                    )?,
                    _ => anyhow::bail!(
                        "fleet {} needs exactly one of bucket or local_store",
                        fleet.id
                    ),
                };
                let id = fleet.id.clone();
                let mut client =
                    cellar::management::FleetClient::new(bucket.clone(), fleet.allow_public_peers);
                if let Some(connection) = fleet.raft {
                    let control = cellar::sdk::fleet_operations::RaftFleetControl::connect(
                        &domain,
                        bucket,
                        connection,
                        std::time::Duration::from_secs(30),
                    )
                    .await?;
                    client = client.with_raft_fleet(control);
                }
                let value = Fleet {
                    info: FleetInfo {
                        id: fleet.id,
                        name: fleet.name,
                        environment: fleet.environment,
                    },
                    client,
                };
                anyhow::ensure!(fleets.insert(id, value).is_none(), "duplicate fleet ID");
            }
            let principals = config
                .users
                .into_iter()
                .map(|user| {
                    let token = std::env::var(&user.token_env).with_context(|| {
                        format!("missing token environment variable {}", user.token_env)
                    })?;
                    Principal::new(user.id, &token, user.grants)
                })
                .collect::<Result<Vec<_>>>()?;
            cellar_admin::router(Console {
                domain: domain.clone(),
                fleets,
                principals,
                audit: AuditStore::open(&base.join(config.audit))?,
                assets: base.join(config.assets),
                origins: config.origins,
            })
        })
        .await??;
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    eprintln!(
        "Cellar dashboard listening on http://{}",
        listener.local_addr()?
    );
    let result = axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await;
    domain.shutdown().await;
    result?;
    Ok(())
}
