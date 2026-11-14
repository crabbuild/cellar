// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Identical public SQL, KV and Queue requests for the constrained-node comparison.
#![allow(clippy::disallowed_methods)]
use anyhow::{ensure, Context as _, Result};
use cellar::engine_api::{Compat, WorkerConfigOptions};
use cellar::fleet::LoadedDeployment;
use cellar::sdk::{consensus::RaftClass, queue, *};
use futures_util::TryStreamExt as _;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

pub const ENTITIES: usize = 24;
pub const SCRIPT: &str = "perf-cells";
pub const VERSION: &str = "v1";

pub fn family() -> Result<String> {
    let value = std::env::var("CELLAR_PERF_PRIMITIVE")?;
    ensure!(
        matches!(value.as_str(), "sql" | "kv" | "queue"),
        "invalid primitive"
    );
    Ok(value)
}

pub fn class(family: &str) -> &'static str {
    match family {
        "sql" => cellar::deploy::SQL_CLASS,
        "kv" => cellar::deploy::KV_CLASS,
        _ => cellar::deploy::QUEUE_CLASS,
    }
}

pub fn identity(family: &str, ordinal: usize, dead: bool) -> String {
    if family == "queue" {
        return format!(
            "primitive-queue-{}{ordinal}",
            if dead { "dead-" } else { "" }
        );
    }
    format!(
        "primitive/{family}/{}{ordinal}",
        if dead { "dead/" } else { "" }
    )
}

pub fn options() -> Result<WorkerConfigOptions> {
    options_for(&family()?)
}

fn options_for(family: &str) -> Result<WorkerConfigOptions> {
    ensure!(
        matches!(family, "sql" | "kv" | "queue"),
        "invalid primitive"
    );
    let mut options = WorkerConfigOptions {
        src: String::new(),
        script_name: SCRIPT.into(),
        do_classes: vec![class(family).into()],
        bindings: vec![],
        blob_bindings: vec![],
        sql_bindings: vec![],
        kv_bindings: vec![],
        queue_bindings: vec![],
        stream_bindings: vec![],
        queue_consumers: vec![],
        workflow_bindings: vec![],
        vars: vec![],
        node: String::new(),
        modules: vec![],
        compat: Compat::default(),
    };
    for ordinal in 0..ENTITIES {
        let name = format!("P{ordinal}");
        let id = identity(family, ordinal, false);
        match family {
            "sql" => options.sql_bindings.push((name, id)),
            "kv" => options.kv_bindings.push((name, id)),
            _ => {
                let dead = identity(family, ordinal, true);
                options.queue_bindings.extend([
                    cellar::engine_api::QueueBinding {
                        environment: name,
                        queue: id.clone(),
                        delivery_delay: 0,
                    },
                    cellar::engine_api::QueueBinding {
                        environment: format!("D{ordinal}"),
                        queue: dead.clone(),
                        delivery_delay: 0,
                    },
                ]);
                options
                    .queue_consumers
                    .push(cellar::protocol::QueueConsumerConfig {
                        queue: id,
                        max_batch_size: 8,
                        max_batch_timeout: 0,
                        max_retries: 1,
                        dead_letter_queue: Some(dead),
                        max_concurrency: Some(2),
                        retry_delay: Some(1),
                    });
            }
        }
    }
    Ok(options)
}

pub fn graph() -> Result<DeploymentGraph> {
    Ok(DeploymentGraph::single(LoadedDeployment {
        event_streams: vec![],
        native: None,
        options: options()?,
        script_name: SCRIPT.into(),
        version: VERSION.into(),
        prefix: "primitive-comparison/v1".into(),
        asset_binding: None,
        loader_bindings: vec![],
        assets: None,
        services: vec![],
        crons: vec![],
        cron_revision: None,
        containers: vec![],
        fence_image: None,
        container_images: vec![],
    }))
}

struct Marker;

impl Cell for Marker {
    fn invoke(self: Arc<Self>, _: Context, _: Invocation) -> HandlerFuture<Output> {
        Box::pin(async { anyhow::bail!("the comparison invokes public bindings") })
    }
}

type Deliveries = Arc<Mutex<BTreeMap<String, (String, u64)>>>;

async fn handle(
    context: Context,
    invocation: Invocation,
    family: String,
    token: String,
    deliveries: Deliveries,
) -> Result<Output> {
    if let Invocation::Queue(batch) = invocation {
        return queue::dispatch(batch, move |event| async move {
            for message in event.messages() {
                let queue::Body::Text(text) = &message.body else {
                    anyhow::bail!("expected text Queue body")
                };
                let body: Value = serde_json::from_str(text)?;
                let id = body["id"].as_str().context("message id")?;
                let value = body["value"].as_str().context("message value")?;
                ensure!(id.len() <= 160 && value.len() == 256, "invalid Queue body");
                {
                    let mut seen = deliveries
                        .lock()
                        .map_err(|_| anyhow::anyhow!("delivery observer poisoned"))?;
                    ensure!(
                        seen.len() < 1_000_000 || seen.contains_key(id),
                        "delivery observer capacity"
                    );
                    let digest = hex::encode(Sha256::digest(value.as_bytes()));
                    let entry = seen.entry(id.into()).or_insert((digest.clone(), 0));
                    ensure!(entry.0 == digest, "Queue body changed for message identity");
                    entry.1 += 1;
                }
                message.ack()?;
            }
            Ok(())
        })
        .await;
    }
    let Invocation::Fetch(request) = invocation else {
        anyhow::bail!("HTTP or Queue invocation required")
    };
    ensure!(
        request.headers.iter().any(
            |(k, v)| k.eq_ignore_ascii_case("authorization") && v == &format!("Bearer {token}")
        ),
        "unauthorized"
    );
    let operation = request
        .url
        .rsplit('/')
        .next()
        .context("operation")?
        .to_owned();
    let bytes = context
        .take_body(request.body)?
        .try_concat()
        .await
        .map_err(anyhow::Error::msg)?;
    ensure!(bytes.len() <= 16 * 1024, "request body limit");
    let input: Value = serde_json::from_slice(&bytes)?;
    let result: Result<Value> = async {
        if operation == "ready" { return Ok(json!({"family":family,"entities":ENTITIES})); }
        if operation == "deliveries" {
            let prefix = input["prefix"].as_str().context("prefix")?;
            let seen = deliveries.lock().map_err(|_| anyhow::anyhow!("delivery observer poisoned"))?;
            return Ok(json!(seen.iter().filter(|(id,_)| id.starts_with(prefix)).collect::<BTreeMap<_,_>>()));
        }
        let ordinal = input["entity"].as_u64().context("entity")? as usize;
        ensure!(ordinal < ENTITIES, "entity outside population");
        let binding = format!("P{ordinal}");
        let key = input["key"].as_str().unwrap_or("setup");
        let value = input["value"].as_str().unwrap_or("");
        ensure!(key.len() <= 160 && value.len() <= 256, "payload bounds");
        match family.as_str() {
            "sql" => {
                let database = context.sql(&binding)?;
                match operation.as_str() {
                    "setup" => { database.exec(&context, "CREATE TABLE perf(id TEXT PRIMARY KEY, value TEXT NOT NULL)").await?; Ok(json!(true)) },
                    "write" => {
                        let result = database.prepare("INSERT INTO perf VALUES(?,?) ON CONFLICT(id) DO UPDATE SET value=excluded.value")
                            .bind_json([json!(key),json!(value)])?.run(&context).await?;
                        ensure!(result.meta.changes == 1, "SQL changed row count"); Ok(json!(true))
                    },
                    "read" => Ok(json!(database.prepare("SELECT value FROM perf WHERE id=?").bind_json([json!(key)])?.first(&context, Some("value")).await?)),
                    _ => anyhow::bail!("unknown SQL operation"),
                }
            },
            "kv" => {
                let namespace = context.kv(&binding)?;
                match operation.as_str() {
                    "write" => { namespace.put_text(&context, key, value, kv::PutOptions::default()).await?; Ok(json!(true)) },
                    "read" => match namespace.get(&context, key, kv::ReadType::Text).await? {
                        Some(kv::ReadValue::Text(value)) => Ok(json!(value)), None => Ok(Value::Null),
                        _ => anyhow::bail!("non-text KV result"),
                    },
                    _ => anyhow::bail!("unknown KV operation"),
                }
            },
            _ => match operation.as_str() {
                "write" => {
                    let entries = input["messages"].as_array().context("messages")?;
                    ensure!(entries.len() == 8, "Queue batch size must be eight");
                    let entries = entries.iter().map(|entry| queue::BatchEntry { body: queue::Body::Text(entry.to_string()), delay_seconds: None });
                    context.queue(&binding)?.send_batch(&context, entries, queue::SendOptions::default()).await?;
                    Ok(json!(true))
                },
                "metrics" => {
                    let live = context.queue(&binding)?.metrics(&context).await?;
                    let dead = context.queue(&format!("D{ordinal}"))?.metrics(&context).await?;
                    Ok(json!({"backlog":live.backlog_count,"dead":dead.backlog_count}))
                },
                _ => anyhow::bail!("unknown Queue operation"),
            }
        }
    }.await;
    let (status, body) = match result {
        Ok(value) => (200, serde_json::to_vec(&value)?),
        Err(error) => (503, format!("{error:#}").into_bytes()),
    };
    Ok(Output::Http(cellar::engine_api::HttpResponse {
        status,
        headers: vec![],
        body,
        stream: None,
        websocket: None,
        write_position: None,
        observed_position: None,
    }))
}

pub fn application(raft: bool) -> Result<Arc<dyn Application>> {
    application_for(&family()?, std::env::var("CELLAR_PERF_TOKEN")?, raft)
}

fn application_for(family: &str, token: String, raft: bool) -> Result<Arc<dyn Application>> {
    let options = options_for(family)?;
    let family = family.to_owned();
    let deliveries = Deliveries::default();
    let mut registry = Registry::new(move |context, invocation| {
        Box::pin(handle(
            context,
            invocation,
            family.clone(),
            token.clone(),
            deliveries.clone(),
        ))
    });
    if raft {
        registry = registry.raft_cell(
            RaftClass::new("PrimitiveMarker", 1, [191; 32])?
                .initializer([203; 32], |_, _| Ok(()))?
                .json_cell_rpc("Unused", [192; 32])?,
            |_, _| Ok(Arc::new(Marker)),
        )?;
    }
    registry.validate_configuration(&options)?;
    Ok(Arc::new(registry))
}

#[cfg(test)]
#[path = "primitive_app_tests.rs"]
mod tests;
