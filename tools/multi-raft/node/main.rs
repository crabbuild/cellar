// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Disposable shared-group voters/operator for the Docker disaster drill.
//! This is qualification infrastructure, not production deployment policy.
#![allow(clippy::disallowed_methods)]
mod config;
mod managed_operator;
mod managed_store;
mod managed_voter;
mod physical;
mod recovery;
mod service;

use anyhow::{ensure, Context, Result};
use cellar::application::consensus::{
    verification::{cell_groups::*, *},
    *,
};
use cellar::sdk::ExecutionDomain;
use cellar_ltx::{
    generation::{GenerationStore, ReadServices},
    internal::{CacheLimits, NodeExtentStore, EXTENT_BYTES},
    stream_snapshot::{ScratchLimits, ScratchPool},
    DirectFileSystem,
};
use config::{Config, Options};
use serde_json::{json, Value};
use std::{path::PathBuf, sync::Arc, time::Duration};

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let options = Options::load()?;
    let domain = Arc::new(ExecutionDomain::new(tokio::runtime::Handle::current()));
    let result = domain
        .run(async {
            if options.action == "managed-node" {
                return managed_voter::run(domain.clone(), &options).await;
            }
            if options.action.starts_with("managed-") {
                return managed_operator::run(&domain, &options).await;
            }
            let config = Arc::new(Config::load(&options.config)?);
            match options.action.as_str() {
                "node" => service::run(domain.clone(), config.clone(), &options).await,
                "seed" => recovery::seed(&domain, &config, &options).await,
                action => recovery::operator(&domain, &config, &options, action).await,
            }
        })
        .await?;
    domain.shutdown().await;
    result
}

fn output(value: Value) -> Result<()> {
    cellar::cli_output::Output::new(cellar::cli_output::Format::Json)
        .line(format_args!("{}", serde_json::to_string(&value)?))
}

fn bounds() -> CellRoutingLimits {
    CellRoutingLimits {
        timeout: Duration::from_secs(20),
        max_attempts: 4,
    }
}

fn keys() -> Result<VersionKeys> {
    Ok(VersionKeys::new(
        VersionSigningKey::new([89; 32], u64::MAX)?,
        vec![],
        Duration::from_secs(600),
    )?)
}
