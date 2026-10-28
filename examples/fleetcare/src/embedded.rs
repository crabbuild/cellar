// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The measuring application owns its process, executors, clients and signals.
//! No CLI host or environment-resolving framework constructors are used here.

// These operations belong to the embedding application, outside the node's
// execution domain. In particular, post-stop tasks check the caller's runtimes.
#![allow(clippy::disallowed_methods)]

use anyhow::{ensure, Context, Result};
use base64::Engine;
use cellar::bucket::{Bucket, StorageBackend};
use cellar::sdk::{
    ActorIdentity, ActorOptions, ActorPolicy, CompiledApplications, FenceReason, LogOptions, Node,
    NodeConfig, NodeFleet, NodeOutcome, NodeServices, OwnershipOnEvict, PeerAuth, PeerProbeSigner,
    ReplicaStorage, ReplicaStorageOptions, ReplicationOptions, ShutdownMode, ShutdownPolicy,
};
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::{ClientOptions, RetryConfig};
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Arguments {
    bucket: String,
    endpoint: String,
    region: String,
    listen: String,
    internal_listen: String,
    advertise: String,
}

impl Arguments {
    fn parse() -> Result<Self> {
        let mut arguments = std::env::args().skip(1);
        let mut values = serde_json::Map::new();
        while let Some(option) = arguments.next() {
            let name = option
                .strip_prefix("--")
                .context("expected --OPTION VALUE")?;
            let value = arguments.next().context("missing option value")?;
            ensure!(
                !value.starts_with("--"),
                "missing option value for {option}"
            );
            ensure!(
                values
                    .insert(name.replace('-', "_"), value.into())
                    .is_none(),
                "duplicate option {option}"
            );
        }
        serde_json::from_value(values.into()).context("invalid embedded benchmark arguments")
    }
}

fn setting<T: std::str::FromStr>(name: &str, default: T) -> Result<T>
where
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match std::env::var(name) {
        Ok(value) => value.parse().with_context(|| format!("invalid {name}")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("invalid {name}")),
    }
}

fn report(value: serde_json::Value) -> Result<()> {
    cellar::cli_output::Output::new(cellar::cli_output::Format::Text).line(format_args!("{value}"))
}

fn failed_outcome(outcome: &NodeOutcome, executors_alive: bool) -> serde_json::Value {
    let mut value = serde_json::json!({
        "event": "embedded_node_stopped", "caller_executors_alive": executors_alive,
    });
    match outcome {
        NodeOutcome::Fenced { code, reason } => {
            value["outcome"] = "fenced".into();
            value["fence_code"] = (*code).into();
            value["reason"] = match reason {
                FenceReason::NodeLeaseExpired => "node_lease_expired",
                FenceReason::NodeLeaseMissing => "node_lease_missing",
                FenceReason::NodeLeaseMismatch => "node_lease_mismatch",
            }
            .into();
        }
        NodeOutcome::CoreFailed { error } => {
            value["outcome"] = "core_failed".into();
            value["error_present"] = (!error.is_empty()).into();
        }
        NodeOutcome::ReplicationFailed { error } => {
            value["outcome"] = "replication_failed".into();
            value["error_present"] = (!error.is_empty()).into();
        }
        NodeOutcome::Cancelled => value["outcome"] = "cancelled".into(),
        NodeOutcome::Stopped(_) => value["outcome"] = "stopped".into(),
    }
    value
}

fn bucket_clients(builder: &AmazonS3Builder, name: &str) -> Result<Bucket> {
    // Match the daemon's bounded transport policy. Every invocation builds
    // separate pools; the lease lane must not borrow the data client's pool.
    let store = Arc::new(
        builder
            .clone()
            .with_retry(RetryConfig {
                max_retries: 2,
                retry_timeout: Duration::from_secs(30),
                ..Default::default()
            })
            .build()?,
    );
    let conditional = Arc::new(
        builder
            .clone()
            .with_retry(RetryConfig {
                max_retries: 0,
                retry_timeout: Duration::from_secs(30),
                ..Default::default()
            })
            .build()?,
    );
    Bucket::from_clients(
        store.clone(),
        conditional,
        store,
        StorageBackend::S3,
        name.to_string(),
        String::new(),
    )
}

pub fn run(application_config: crate::config::AppConfig) -> Result<()> {
    let mut graph = crate::deployment::graph(&application_config);
    let application_config = Arc::new(application_config);
    let arguments = Arguments::parse()?;
    let node: Arc<str> = application_config.node.clone();
    let directory = PathBuf::from(std::env::var("CELLAR_PERF_DATA_DIR")?);
    let asset_cache = cellar::sdk::AssetCache::new(cellar::sdk::AssetCacheOptions::new(
        std::env::var("CELLAR_ASSET_CACHE_DIR")?,
    ))?;
    let threads = setting("CELLAR_PERF_THREADS", 1usize)?;
    let resident = setting("CELLAR_PERF_MAX_RESIDENT", 256usize)?;
    let cached_routes = setting("CELLAR_PERF_MAX_CACHED_ROUTES", 2_048usize)?;
    let requests = setting("CELLAR_PERF_MAX_REQUESTS", 512usize)?;
    let shutdown_ms = setting("CELLAR_PERF_SHUTDOWN_MS", 30_000u64)?;
    let wake_ms = setting("CELLAR_PERF_WAKE_MS", 1_000u64)?;
    let pressure = match std::env::var("CELLAR_PERF_MEMORY_PRESSURE")
        .as_deref()
        .unwrap_or("off")
    {
        "off" => cellar_logic::pressure::PressureConfig::default(),
        "auto" => {
            cellar_logic::pressure::PressureConfig::from_limits(Some(1792 * 1024 * 1024), None)
        }
        _ => anyhow::bail!("CELLAR_PERF_MEMORY_PRESSURE must be off or auto"),
    };
    ensure!(wake_ms > 0, "CELLAR_PERF_WAKE_MS must be positive");
    ensure!(
        threads > 0 && resident > 0 && requests > 0 && shutdown_ms > 0,
        "invalid node limits"
    );
    let fleet_durability = match std::env::var("CELLAR_PERF_DURABILITY")?.as_str() {
        "fleet" => true,
        "bucket" => false,
        _ => anyhow::bail!("CELLAR_PERF_DURABILITY must be fleet or bucket"),
    };
    let peer_key: [u8; 32] = base64::engine::general_purpose::STANDARD
        .decode(std::env::var("CELLAR_PERF_PEER_KEY")?)
        .context("invalid CELLAR_PERF_PEER_KEY encoding")?
        .try_into()
        .map_err(|_| anyhow::anyhow!("CELLAR_PERF_PEER_KEY must encode 32 bytes"))?;
    // Credentials are supplied to clients by this application. The library
    // receives the clients themselves and never resolves the AWS environment.
    let builder = AmazonS3Builder::new()
        .with_bucket_name(&arguments.bucket)
        .with_region(&arguments.region)
        .with_endpoint(&arguments.endpoint)
        .with_virtual_hosted_style_request(false)
        .with_allow_http(true)
        .with_client_options(
            ClientOptions::new()
                .with_allow_http(true)
                .with_timeout(Duration::from_secs(15))
                .with_connect_timeout(Duration::from_secs(3)),
        )
        .with_access_key_id(std::env::var("AWS_ACCESS_KEY_ID")?)
        .with_secret_access_key(std::env::var("AWS_SECRET_ACCESS_KEY")?)
        .with_conditional_put(S3ConditionalPut::ETagMatch);
    let bucket = bucket_clients(&builder, &arguments.bucket)?;
    let lease_bucket = bucket_clients(&builder, &arguments.bucket)?;
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::WARN)
        .with_writer(std::io::stderr)
        .try_init()
        .map_err(|error| anyhow::anyhow!("could not install application logger: {error}"))?;
    let host = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .thread_name("perf-application")
        .enable_all()
        .build()?;
    let core = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .thread_name("perf-core")
        .enable_all()
        .build()?;
    host.block_on(async {
        crate::assets::install(&mut graph, &bucket, &asset_cache).await?;
        let signer = Arc::new(PeerProbeSigner::generate());
        let auth = Arc::new(PeerAuth::new(peer_key, node.as_ref())?);
        let peer_http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .tcp_nodelay(true)
            .build()?;
        let stream_http = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .tcp_nodelay(true)
            .build()?;
        let replica_storage = ReplicaStorage::new(bucket.store.clone(), ReplicaStorageOptions::default())?;
        let fleet = NodeFleet {
            lease_bucket,
            bucket,
            replica_storage,
            replication: ReplicationOptions::default(),
            log_transport: Arc::new(cellar::sdk::SignedPeerTransport::from_clients(
                peer_http.clone(), stream_http, auth.clone(),
            )),
            log: LogOptions::default(),
            fleet_durability,
        };
        let actor = ActorOptions {
            core: ActorPolicy {
                max_resident: resident,
                max_cached_routes: cached_routes,
                max_activations: 64,
                max_evictions: 4,
                max_releases: 32,
                max_outbound_websockets: 32,
                ownership_on_evict: OwnershipOnEvict::Sticky,
                require_node_lease: true,
                peer_protocol: cellar::peer_auth::PROTOCOL_VERSION,
                operation_deadline_ms: Some(30_000),
                owner_log_recovery_backoff_ms: 1_000,
                owner_log_recovery_attempts: 240,
                alarm_resident_ms: 3_600_000,
                idle_evict_ms: None,
                pressure,
            },
            lease_ttl_ms: 10_000,
            placement_weight: 1,
            generation: signer.public_key_hex(),
            resume_generation: None,
        };
        let mut config = NodeConfig::new(
            ActorIdentity { node: node.to_string(), advertise: arguments.advertise, region: arguments.region },
            actor,
            directory,
        );
        config.maintenance.rebalance = true;
        config.maintenance.wake_period = Duration::from_millis(wake_ms);
        // Root requests retain admission while nested cell calls settle. Size
        // this finite application limit for the offered benchmark concurrency.
        config.runtime.max_stateless_requests = Some(requests);
        config.runtime.max_stateless_workers = 2;
        config.runtime.max_cell_requests = 16;
        config.runtime.handler_budget = Duration::from_secs(30);
        config.runtime.admission_wait = Duration::from_millis(100);
        config.shutdown = ShutdownPolicy {
            total_ms: shutdown_ms,
            token_wait_ms: 0,
            no_progress_ms: shutdown_ms.min(25_000),
        };
        let services = NodeServices {
            wall_clock: None,
            http_client: None,
            host_executor: host.handle().clone(),
            core_executor: core.handle().clone(),
            listener: tokio::net::TcpListener::bind(arguments.listen).await?,
            internal_listener: tokio::net::TcpListener::bind(arguments.internal_listen).await?,
            peer_http,
            peer_auth: auth,
            probe_signer: signer,
            fleet: Some(fleet),
            containers: Some(cellar::container::EngineOptions {
                capacity: None,
                socket: "/var/run/docker.sock".into(), namespace:None, runtime:None, dns:Vec::new(),
                network_helper: None,
            }),
            metrics: None,
        };
        let applications = CompiledApplications::default()
            .register(crate::config::SCRIPT, crate::config::VERSION,
                move || crate::application(application_config.clone()))?;
        // Signals are the embedding application's policy. Internal /shutdown
        // also completes run_until through the returned node outcome.
        #[cfg(unix)]
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        let node = Node::start(config, services, graph, applications).await?;
        report(serde_json::json!({
            "event": "embedded_node_started", "executors": "caller_owned",
            "max_resident": resident, "max_cached_routes": cached_routes,
            "max_requests": requests,
            "lease_ttl_ms": 10_000,
        }))?;
        let outcome = node.run_until(async {
            #[cfg(unix)]
            cellar::asyncrt::select! {
                _ = terminate.recv() => {},
                _ = tokio::signal::ctrl_c() => {},
            }
            #[cfg(not(unix))]
            let _ = tokio::signal::ctrl_c().await;
            ShutdownMode::Handoff
        }).await?;
        let executors_alive = tokio::time::timeout(Duration::from_secs(2), async {
            Ok::<_, anyhow::Error>(host.handle().spawn(async { true }).await?
                && core.handle().spawn(async { true }).await?)
        }).await??;
        let NodeOutcome::Stopped(stopped) = outcome else {
            report(failed_outcome(&outcome, executors_alive))?;
            anyhow::bail!("embedded node failed: {outcome:?}");
        };
        report(serde_json::json!({
            "event": "embedded_node_stopped", "outcome": "stopped",
            "mode": match stopped.mode { ShutdownMode::Handoff => "handoff", ShutdownMode::Preserve => "preserve" },
            "ingress_drained": stopped.ingress_drained,
            "application_work_drained": stopped.application_work_drained,
            "pending_application_events": stopped.pending_application_events,
            "cells_drained": stopped.cells_drained,
            "pending_dispatches": stopped.pending_dispatches,
            "log_quiesced": stopped.log_quiesced,
            "local_durability_closed": stopped.local_durability_closed,
            "clean_reload_prepared": stopped.clean_reload_prepared,
            "drain_timed_out": stopped.drain_timed_out,
            "deadline_elapsed": stopped.deadline_elapsed,
            "caller_executors_alive": executors_alive,
        }))?;
        ensure!(executors_alive, "node stopped a caller-owned executor");
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_fencing_report_preserves_exact_reason_and_code() {
        for (reason, text) in [
            (FenceReason::NodeLeaseExpired, "node_lease_expired"),
            (FenceReason::NodeLeaseMissing, "node_lease_missing"),
            (FenceReason::NodeLeaseMismatch, "node_lease_mismatch"),
        ] {
            let value = failed_outcome(&NodeOutcome::Fenced { code: 3, reason }, true);
            assert_eq!(value["outcome"], "fenced");
            assert_eq!(value["reason"], text);
            assert_eq!(value["fence_code"], 3);
            assert_eq!(value["caller_executors_alive"], true);
        }
    }

    #[test]
    fn component_failure_never_fabricates_authority_loss() {
        for outcome in [
            NodeOutcome::CoreFailed {
                error: "core stopped".into(),
            },
            NodeOutcome::ReplicationFailed {
                error: "replication stopped".into(),
            },
        ] {
            let value = failed_outcome(&outcome, true);
            assert!(value.get("reason").is_none());
            assert!(value.get("fence_code").is_none());
            assert_eq!(value["error_present"], true);
        }
    }
}
