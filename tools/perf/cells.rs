// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Durable entity write workload for the RustFS performance harness.

use anyhow::{bail, Result};
use cellar::sdk::http::{Response, Url};
use cellar::sdk::{Application, Cell, Context, HandlerFuture, Invocation, Output, Registry};
use sha2::{Digest, Sha256};
use std::sync::Arc;

mod deployment;
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
            let verify_payload = request.method == "GET"
                && request.headers.iter().any(|(key, value)| {
                    key.eq_ignore_ascii_case("x-cellar-verify-payload") && value == "sha256"
                });
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
            let payload = state["payload"].as_str().unwrap_or("");
            let mut result = serde_json::json!({
                "writes": state["writes"],
                "payload_bytes": payload.len(),
                "node": self.node.as_ref(),
            });
            if verify_payload {
                // Recompute from the actual stored payload only for recovery audits.
                result["payload_sha256"] =
                    format!("{:x}", Sha256::digest(payload.as_bytes())).into();
            }
            let mut response = Response::from_json(&result)?.into_http(&context)?;
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

fn main() -> Result<()> {
    embedded::run(deployment::graph(), application)
}

#[cfg(test)]
#[path = "cells_tests.rs"]
mod tests;
