//! Native Rust cookbook. See examples/README.md for commands and guarantees.

use anyhow::Result;
use cellar::sdk::{Application, Context, Host, Invocation, Output, Registry};
use std::sync::Arc;

#[path = "durable-agent/main.rs"]
mod agent;
#[path = "alarm/main.rs"]
mod alarm;
#[path = "async/main.rs"]
mod background;
#[path = "blob/main.rs"]
mod blob;
#[path = "body/main.rs"]
mod body;
#[path = "counter/main.rs"]
mod counter;
#[path = "facets/main.rs"]
mod facets;
#[path = "hello/main.rs"]
mod hello;
#[path = "kv/main.rs"]
mod kv;
#[path = "transactional-outbox/main.rs"]
mod outbox;
#[path = "queues/main.rs"]
mod queues;
#[path = "team-services/main.rs"]
mod services;
#[path = "sql/main.rs"]
mod sql;
#[path = "support/mod.rs"]
mod support;
#[path = "multi-tenant/main.rs"]
mod tenants;
#[path = "vectordb/main.rs"]
mod vector;
#[path = "webapi/main.rs"]
mod webapi;
#[path = "wsecho/main.rs"]
mod websocket;
#[path = "workflow/main.rs"]
mod workflow;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

async fn dispatch(context: Context, invocation: Invocation) -> Result<Output> {
    match invocation {
        Invocation::Queue(batch) => queues::consume(context, batch).await,
        Invocation::Scheduled(event) => {
            // Occurrences can retry: the sink deduplicates by cron + scheduled time.
            let args = cellar::engine_api::RpcData::Json(
                serde_json::json!({ "key": format!("{}:{}", event.cron, event.scheduled_ms) })
                    .to_string(),
            );
            context
                .namespace("COUNTERS")?
                .get_by_name("cron-ticks")
                .rpc(&context, "increment_once", args)
                .await?;
            Ok(Output::Done)
        }
        Invocation::Fetch(request) => {
            let url = url::Url::parse(&request.url)?;
            let path = url.path();
            match path.split('/').nth(1).unwrap_or_default() {
                "hello" | "health" => hello::fetch(&context, request),
                "webapi" => webapi::fetch(&context, request),
                "body" => body::fetch(&context, request).await,
                "assets-binding" => {
                    let mut request = request;
                    let mut url = url;
                    url.set_path("/");
                    request.url = url.into();
                    Ok(Output::Http(
                        context.assets("ASSETS")?.fetch(&context, request).await?,
                    ))
                }
                "counters" | "router" | "rpc" | "cron" => counter::fetch(&context, request).await,
                "alarms" => support::route(&context, request, "ALARMS").await,
                "async" => support::route(&context, request, "ASYNC").await,
                "sql" => sql::fetch(&context, request).await,
                "kv" => kv::fetch(&context, request).await,
                "blobs" => blob::fetch(&context, request).await,
                "vectors" => support::route(&context, request, "VECTORS").await,
                "queues" => queues::fetch(&context, request).await,
                "workflows" => workflow::fetch(&context, request).await,
                "rooms" => support::route(&context, request, "ROOMS").await,
                "wsclient" => {
                    support::method(&request, &["GET"])?;
                    websocket::client(&context, path.ends_with("/outbound")).await
                }
                "facets" => support::route(&context, request, "SUPERVISORS").await,
                "tenants" => tenants::fetch(&context, request).await,
                "outbox" => support::route(&context, request, "OUTBOX").await,
                "agents" => support::route(&context, request, "AGENTS").await,
                _ => support::text(&context, 404, "see examples/README.md for routes"),
            }
        }
        _ => Ok(Output::Done),
    }
}

fn application() -> Result<Arc<dyn Application>> {
    Ok(Arc::new(
        Registry::new(|context, event| {
            Box::pin(async move {
                match dispatch(context.clone(), event).await {
                    Err(error) if error.downcast_ref::<support::ClientError>().is_some() => {
                        let status = error.downcast_ref::<support::ClientError>().unwrap().status;
                        support::json_status(
                            &context,
                            status,
                            serde_json::json!({ "error": error.to_string() }),
                        )
                    }
                    result => result,
                }
            })
        })
        .cell("Counter", |_, _| Ok(support::cell(counter::Counter)))?
        .cell("Alarm", |_, _| Ok(support::cell(alarm::Alarm)))?
        .cell("Background", |_, _| {
            Ok(support::cell(background::Background))
        })?
        .cell("Vector", |_, _| Ok(support::cell(vector::Vector)))?
        .cell("Room", |_, _| Ok(support::cell(websocket::Room)))?
        .cell("Supervisor", |_, _| Ok(support::cell(facets::Supervisor)))?
        .cell("Account", |_, _| Ok(support::cell(tenants::Account)))?
        .cell("Outbox", |_, _| Ok(support::cell(outbox::Outbox)))?
        .cell("Agent", |_, _| Ok(support::cell(agent::Agent)))?
        .workflow("Report", |context, event, steps| {
            Box::pin(workflow::run(context, event, steps))
        })?,
    ))
}

const SCRIPTS: &[&str] = &[
    "native-hello",
    "native-webapi",
    "native-static-assets",
    "native-counter",
    "native-router",
    "native-rpc",
    "native-alarm",
    "native-async",
    "native-body",
    "native-sql",
    "native-kv",
    "native-blob",
    "native-vectordb",
    "native-queues",
    "native-cron",
    "native-workflow",
    "native-wsecho",
    "native-wsclient",
    "native-facets",
    "native-multi-tenant",
    "native-transactional-outbox",
    "native-durable-agent",
];

fn main() -> Result<()> {
    let mut host = Host::fleet().register("native-examples", "v1", application)?;
    for &name in SCRIPTS {
        host = host.register(name, "v1", application)?;
    }
    host.register("team-api", "v1", services::api)?
        .register("team-ledger", "v1", services::ledger)?
        .run()
}

#[cfg(test)]
#[path = "support/catalog_tests.rs"]
mod catalog_tests;
