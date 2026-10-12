//! Durable entity write workload for the RustFS performance harness.

use anyhow::{bail, Result};
use cellar::engine_api::{Compat, WorkerConfigOptions};
use cellar::fleet::LoadedDeployment;
use cellar::generation::DeploymentGraph;
use cellar::sdk::http::{Response, Url};
use cellar::sdk::{Application, Cell, Context, HandlerFuture, Invocation, Output, Registry};
use std::sync::Arc;

mod embedded;

#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

struct Entity {
    node: Arc<str>,
}

impl Cell for Entity {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                bail!("performance cells accept HTTP requests");
            };
            let storage = context.storage()?;
            let state = match request.method.as_str() {
                "POST" => {
                    let body = request.into_body_reader(&context)?.bytes(&context).await?;
                    let payload = String::from_utf8(body)?;
                    storage.transaction_sync(|transaction| {
                        let previous = transaction.get_json("state")?;
                        let writes = previous
                            .as_ref()
                            .and_then(|v| v["writes"].as_u64())
                            .unwrap_or(0);
                        let writes = writes
                            .checked_add(1)
                            .ok_or_else(|| anyhow::anyhow!("overflow"))?;
                        let state = serde_json::json!({"writes": writes, "payload": payload});
                        transaction.put_json("state", &state)?;
                        Ok(state)
                    })?
                }
                "GET" => storage
                    .get_json("state")?
                    .unwrap_or(serde_json::json!({"writes": 0, "payload": ""})),
                _ => bail!("expected GET or POST"),
            };
            let mut response = Response::from_json(&serde_json::json!({
                "writes": state["writes"],
                "payload_bytes": state["payload"].as_str().unwrap_or("").len(),
                "node": self.node.as_ref(),
            }))?
            .into_http(&context)?;
            response
                .headers
                .push(("x-cellar-owner".into(), self.node.to_string()));
            Ok(Output::Http(response))
        })
    }
}

fn application(node: Arc<str>) -> Result<Arc<dyn Application>> {
    let registry = Registry::new(|context, event| {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                bail!("expected HTTP");
            };
            let url = Url::new(&request.url, None)?;
            let name = url
                .pathname()
                .strip_prefix("/cells/")
                .filter(|name| !name.is_empty())
                .ok_or_else(|| anyhow::anyhow!("expected /cells/NAME"))?
                .to_owned();
            let entity = context.namespace("ENTITIES")?.get_by_name(&name);
            Ok(Output::Http(entity.fetch(&context, request).await?))
        })
    })
    .cell("Entity", move |_, _| {
        Ok(Arc::new(Entity { node: node.clone() }))
    })?;
    Ok(Arc::new(registry))
}

fn graph() -> DeploymentGraph {
    DeploymentGraph::single(LoadedDeployment {
        native: None,
        options: WorkerConfigOptions {
            src: String::new(),
            script_name: "perf-cells".into(),
            do_classes: vec!["Entity".into()],
            bindings: vec![("ENTITIES".into(), "Entity".into())],
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
        script_name: "perf-cells".into(),
        version: "v1".into(),
        prefix: "perf-cells/v1".into(),
        asset_binding: None,
        loader_bindings: Vec::new(),
        assets: None,
        services: Vec::new(),
        crons: Vec::new(),
        containers: Vec::new(),
        fence_image: None,
        container_images: Vec::new(),
    })
}

fn main() -> Result<()> {
    embedded::run(graph(), application)
}
