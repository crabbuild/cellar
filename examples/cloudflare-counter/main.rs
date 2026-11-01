// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! SQLite counter with an explicit bucket durability wait and request timings.
use anyhow::{bail, Context as _, Result};
use cellar::engine_api::{Compat, WorkerConfigOptions};
use cellar::fleet::LoadedDeployment;
use cellar::sdk::http::Response;
use cellar::sdk::{
    Application, Cell, Context, DeploymentGraph, HandlerFuture, Invocation, Output, Registry,
};
use serde_json::{json, Value};
use std::sync::Arc;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
const SCRIPT: &str = "cloudflare-counter";
const CLASS: &str = "Counter";
const REVISION: &str = match option_env!("CELLAR_COMMIT") {
    Some(value) => value,
    None => "development",
};

fn reply(context: &Context, status: u16, body: Value, timing: String) -> Result<Output> {
    Ok(Output::Http(
        Response::new(
            status,
            body.to_string(),
            vec![
                (
                    "content-type".into(),
                    "application/json; charset=utf-8".into(),
                ),
                ("cache-control".into(), "no-store".into()),
                ("server-timing".into(), timing),
                ("x-cellar-revision".into(), REVISION.into()),
            ],
        )
        .into_http(context)?,
    ))
}

fn row_integer(rows: &(Vec<String>, Vec<Vec<Value>>, u64), index: usize) -> Result<i64> {
    rows.1
        .first()
        .and_then(|row| row.get(index))
        .and_then(Value::as_i64)
        .context("invalid persisted counter state")
}

struct Counter;

impl Cell for Counter {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            let started = cellar::asyncrt::mono_us();
            let storage = context.storage()?;
            storage.sql("CREATE TABLE IF NOT EXISTS counter_state(id INTEGER PRIMARY KEY CHECK(id=1), value INTEGER NOT NULL, revision INTEGER NOT NULL)", &[])?;
            storage.sql("INSERT OR IGNORE INTO counter_state VALUES(1,0,0)", &[])?;
            storage.sql("CREATE TABLE IF NOT EXISTS counter_clients(client TEXT PRIMARY KEY, sequence INTEGER NOT NULL, action TEXT NOT NULL, delta INTEGER NOT NULL, value INTEGER NOT NULL, revision INTEGER NOT NULL)", &[])?;
            let (status, value) = if request.method == "GET" {
                let rows =
                    storage.sql("SELECT value, revision FROM counter_state WHERE id=1", &[])?;
                (
                    200,
                    json!({"count":row_integer(&rows,0)?.to_string(), "version":row_integer(&rows,1)?.to_string(),"duplicate":false}),
                )
            } else if request.method == "POST" {
                let url = url::Url::parse(&request.url)?;
                let params = url
                    .query_pairs()
                    .collect::<std::collections::BTreeMap<_, _>>();
                let client = params.get("client").map(|s| s.as_ref()).unwrap_or("");
                let sequence = params
                    .get("sequence")
                    .and_then(|s| s.parse::<i64>().ok())
                    .filter(|n| *n > 0);
                let action = params.get("action").map(|s| s.as_ref()).unwrap_or("");
                let delta = params
                    .get("delta")
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(0);
                if client.len() != 36
                    || !client.bytes().all(|c| c.is_ascii_hexdigit() || c == b'-')
                    || sequence.is_none()
                    || !matches!(action, "add" | "reset")
                    || !(-1_000_000..=1_000_000).contains(&delta)
                    || action == "reset" && delta != 0
                {
                    return reply(
                        &context,
                        400,
                        json!({"error":"Invalid client, sequence or action"}),
                        String::new(),
                    );
                }
                let sequence = sequence.context("sequence validated")?;
                storage.transaction_sync(|tx| {
                    let old = tx.sql("SELECT sequence,action,delta,value,revision FROM counter_clients WHERE client=?", &[json!(client)])?;
                    if let Some(row) = old.1.first() {
                        let prior=row[0].as_i64().context("invalid receipt sequence")?;
                        if sequence==prior && row[1].as_str()==Some(action) && row[2].as_i64()==Some(delta) {
                            return Ok((200,json!({"count":row[3].as_i64().context("invalid receipt")?.to_string(),"version":row[4].as_i64().context("invalid revision")?.to_string(),"duplicate":true})));
                        }
                        if prior.checked_add(1)!=Some(sequence) {
                            return Ok((409,json!({"error":"Sequence conflict; mutation was not repeated","lastSequence":prior})));
                        }
                    } else {
                        if sequence!=1 { return Ok((409,json!({"error":"A new client must start at sequence 1"}))); }
                        let count = tx.sql("SELECT count(*) FROM counter_clients",&[])?;
                        if row_integer(&count,0)? >= 256 { return Ok((429,json!({"error":"Demo client limit reached"}))); }
                    }
                    let rows=tx.sql("SELECT value,revision FROM counter_state WHERE id=1",&[])?;
                    let Some(value) = (if action=="reset" {Some(0)} else {row_integer(&rows,0)?.checked_add(delta)}) else { return Ok((422,json!({"error":"Counter overflow"}))); };
                    let revision=row_integer(&rows,1)?.checked_add(1).context("revision exhausted")?;
                    tx.sql("UPDATE counter_state SET value=?,revision=? WHERE id=1",&[json!(value),json!(revision)])?;
                    tx.sql("INSERT INTO counter_clients VALUES(?,?,?,?,?,?) ON CONFLICT(client) DO UPDATE SET sequence=excluded.sequence,action=excluded.action,delta=excluded.delta,value=excluded.value,revision=excluded.revision",&[json!(client),json!(sequence),json!(action),json!(delta),json!(value),json!(revision)])?;
                    Ok((200,json!({"count":value.to_string(),"version":revision.to_string(),"duplicate":false})))
                })?
            } else {
                return reply(
                    &context,
                    405,
                    json!({"error":"Use GET or POST"}),
                    String::new(),
                );
            };
            let sql_us = cellar::asyncrt::mono_us().saturating_sub(started);
            let gate_us = if request.method == "POST" {
                let gate = cellar::asyncrt::mono_us();
                // An explicit sync requests a write barrier even without new
                // writes. Keep it for mutations, including duplicate receipts.
                storage.sync().await?;
                Some(cellar::asyncrt::mono_us().saturating_sub(gate))
            } else {
                // The ordinary response gate still checks authority and any
                // unproven state this read observed. Do not request another
                // write barrier merely to read an already proven value.
                None
            };
            let mut timing = format!(
                "sqlite;dur={:.3}, cell;dur={:.3}",
                sql_us as f64 / 1000.0,
                (cellar::asyncrt::mono_us() - started) as f64 / 1000.0
            );
            if let Some(gate_us) = gate_us {
                timing.push_str(&format!(", durability;dur={:.3}", gate_us as f64 / 1000.0));
            }
            reply(&context, status, value, timing)
        })
    }
}

fn application() -> Result<Arc<dyn Application>> {
    Ok(Arc::new(
        Registry::new(|context, event| {
            Box::pin(async move {
                let Invocation::Fetch(request) = event else {
                    bail!("HTTP request required");
                };
                let started = cellar::asyncrt::mono_us();
                let url = url::Url::parse(&request.url)?;
                if url.path() == "/health" {
                    return reply(
                        &context,
                        200,
                        json!({"ready":true,"revision":REVISION}),
                        String::new(),
                    );
                }
                let Some(name) = url
                    .path()
                    .strip_prefix("/api/counters/")
                    .filter(|name| matches!(*name, "demo" | "check"))
                else {
                    return reply(
                        &context,
                        404,
                        json!({"error":"Unknown counter"}),
                        String::new(),
                    );
                };
                let mut response = context
                    .namespace("COUNTERS")?
                    .get_by_name(name)
                    .fetch(&context, request)
                    .await?;
                response.headers.push((
                    "server-timing".into(),
                    format!(
                        "cellar;dur={:.3}",
                        (cellar::asyncrt::mono_us() - started) as f64 / 1000.0
                    ),
                ));
                Ok(Output::Http(response))
            })
        })
        .cell(CLASS, |_, _| Ok(Arc::new(Counter)))?,
    ))
}

fn graph() -> DeploymentGraph {
    DeploymentGraph::single(LoadedDeployment {
        event_streams: Vec::new(),
        native: None,
        options: WorkerConfigOptions {
            stream_bindings: Vec::new(),
            src: String::new(),
            script_name: SCRIPT.into(),
            do_classes: vec![CLASS.into()],
            bindings: vec![("COUNTERS".into(), CLASS.into())],
            blob_bindings: Vec::new(),
            sql_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: Vec::new(),
            node: String::new(),
            modules: Vec::new(),
            compat: Compat::default(),
        },
        script_name: SCRIPT.into(),
        version: "v1".into(),
        prefix: "cloudflare-counter/v1".into(),
        asset_binding: None,
        loader_bindings: Vec::new(),
        assets: None,
        services: Vec::new(),
        crons: Vec::new(),
        cron_revision: None,
        containers: Vec::new(),
        fence_image: None,
        container_images: Vec::new(),
    })
}

fn main() -> Result<()> {
    cellar::sdk::Host::local(graph())
        .register(SCRIPT, "v1", application)?
        .run()
}
