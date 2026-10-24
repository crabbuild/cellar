// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use crate::support;
use anyhow::{bail, Result};
use cellar::engine_api::RpcData;
use cellar::sdk::{decode_rpc, Cell, Context, HandlerFuture, Invocation, Output, Request};
use std::sync::Arc;

pub struct Counter;

impl Cell for Counter {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let storage = context.storage()?;
            let increment = match &event {
                Invocation::Fetch(request) => {
                    support::method(request, &["GET", "POST"])?;
                    request.method == "POST"
                }
                Invocation::CellRpc { method, .. } => {
                    matches!(method.as_str(), "increment" | "increment_once")
                }
                _ => return Ok(Output::Done),
            };
            let key = match &event {
                Invocation::CellRpc { method, args } if method == "increment_once" => {
                    let json = decode_rpc(args)?.to_json()?;
                    Some(format!(
                        "seen:{}",
                        json["key"]
                            .as_str()
                            .ok_or_else(|| support::reject(400, "key required"))?
                    ))
                }
                _ => None,
            };
            let count = if increment {
                storage.transaction_sync(|tx| {
                    let mut count = tx.get_json("count")?.and_then(|v| v.as_u64()).unwrap_or(0);
                    let duplicate = match &key {
                        Some(key) => tx.get_json(key)?.is_some(),
                        None => false,
                    };
                    if !duplicate {
                        count = count
                            .checked_add(1)
                            .ok_or_else(|| anyhow::anyhow!("counter overflow"))?;
                        tx.put_json("count", &serde_json::json!(count))?;
                        if let Some(key) = &key {
                            tx.put_json(key, &serde_json::json!(true))?;
                        }
                    }
                    Ok(count)
                })?
            } else {
                // A single read needs no transaction: BEGIN IMMEDIATE would
                // unnecessarily contend with replication's WAL writer lock.
                storage
                    .get_json("count")?
                    .and_then(|v| v.as_u64())
                    .unwrap_or(0)
            };
            match event {
                Invocation::CellRpc { method, .. } => {
                    if !matches!(method.as_str(), "increment" | "increment_once" | "get") {
                        bail!("unknown counter method");
                    }
                    Ok(Output::Rpc(RpcData::Json(
                        serde_json::json!({ "count": count }).to_string(),
                    )))
                }
                _ => support::json(&context, serde_json::json!({ "count": count })),
            }
        })
    }
}

pub async fn fetch(context: &Context, request: Request) -> Result<Output> {
    let url = url::Url::parse(&request.url)?;
    if url.path() == "/cron" {
        support::method(&request, &["GET"])?;
        let value = context
            .namespace("COUNTERS")?
            .get_by_name("cron-ticks")
            .rpc(context, "get", RpcData::Json("null".into()))
            .await?;
        return support::json(context, cellar::sdk::decode_rpc_result(&value)?.to_json()?);
    }
    let name = support::name(url.path().split('/').nth(2).unwrap_or_default())?;
    if url.path().starts_with("/rpc/") {
        support::method(&request, &["GET", "POST"])?;
        let method = if request.method == "POST" {
            "increment"
        } else {
            "get"
        };
        let args =
            cellar::sdk::primitives::clone::StructuredValue::from_json(serde_json::json!([]))?;
        let value = context
            .namespace("COUNTERS")?
            .get_by_name(name)
            .call(context, method, &args)
            .await?;
        return support::json(context, value.to_json()?);
    }
    Ok(Output::Http(
        context
            .namespace("COUNTERS")?
            .get_by_name(name)
            .fetch(context, request)
            .await?,
    ))
}
