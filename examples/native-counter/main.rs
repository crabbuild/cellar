// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A compiled business-cell application using the production node host.

use anyhow::{bail, Result};
use cellar::engine_api::{Compat, WorkerConfigOptions};
use cellar::fleet::LoadedDeployment;
use cellar::generation::DeploymentGraph;
use cellar::sdk::http::{Response, Url};
use cellar::sdk::Host as NativeDaemon;
use cellar::sdk::{Application, Cell, Context, HandlerFuture, Invocation, Output, Registry};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const SCRIPT: &str = "native-counter";
const CLASS: &str = "Counter";
static APPLICATION_INSTANCES: AtomicU64 = AtomicU64::new(0);

struct Counter;

impl Cell for Counter {
    fn invoke(self: Arc<Self>, context: Context, invocation: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                return Ok(Output::Done);
            };
            if !matches!(request.method.as_str(), "GET" | "POST") {
                return Ok(Output::Http(
                    Response::new(405, "use GET or POST", Vec::new()).into_http(&context)?,
                ));
            }
            let storage = context.storage()?;
            let mut count = storage
                .get_json("count")?
                .and_then(|value| value.as_u64())
                .unwrap_or(0);
            if request.method == "POST" {
                count = count
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("counter overflow"))?;
                storage.put_json("count", &serde_json::json!(count))?;
            }
            Ok(Output::Http(
                Response::new(
                    200,
                    serde_json::json!({ "count": count }).to_string(),
                    vec![("content-type".into(), "application/json".into())],
                )
                .into_http(&context)?,
            ))
        })
    }
}

fn application(version: &'static str) -> Result<Arc<dyn Application>> {
    let instance = APPLICATION_INSTANCES.fetch_add(1, Ordering::Relaxed);
    let application = Registry::new(move |context, invocation| {
        Box::pin(async move {
            let Invocation::Fetch(request) = invocation else {
                bail!("the counter service expects HTTP requests");
            };
            let url = Url::new(&request.url, None)?;
            let path = url.pathname();
            if let Some(asset_path) = path.strip_prefix("/assets-binding") {
                let mut request = request;
                let mut asset_url = url::Url::parse(&request.url)?;
                asset_url.set_path(if asset_path.is_empty() {
                    "/"
                } else {
                    asset_path
                });
                request.url = asset_url.to_string();
                let mut response = context.assets("ASSETS")?.fetch(&context, request).await?;
                response
                    .headers
                    .push(("x-native-version".into(), version.into()));
                return Ok(Output::Http(response));
            }
            let Some(name) = path
                .strip_prefix("/counters/")
                .filter(|name| !name.is_empty())
            else {
                return Ok(Output::Http(
                    Response::new(404, "use /counters/NAME", Vec::new()).into_http(&context)?,
                ));
            };
            let counter = context.namespace("COUNTERS")?.get_by_name(name);
            let mut response = counter.fetch(&context, request).await?;
            response
                .headers
                .push(("x-native-version".into(), version.into()));
            response
                .headers
                .push(("x-native-instance".into(), instance.to_string()));
            Ok(Output::Http(response))
        })
    })
    .cell(CLASS, |_, _| Ok(Arc::new(Counter)))?;
    Ok(Arc::new(application))
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
        prefix: "native-counter/v1".into(),
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
    NativeDaemon::local(graph())
        .register(SCRIPT, "v1", || application("v1"))?
        .register(SCRIPT, "v2", || application("v2"))?
        .run()
}
