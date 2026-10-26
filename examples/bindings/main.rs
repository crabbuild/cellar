// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Binding aliases, named identities and separate class namespaces.

use anyhow::{bail, ensure, Context as _, Result};
use cellar::sdk::{
    http::Response, Cell, Context, HandlerFuture, Host, Invocation, Output, Registry, Request,
};
use serde_json::{json, Value};
use std::sync::Arc;

// Keep counts representable by every language in this walkthrough.
const MAX_COUNT: u64 = 9_007_199_254_740_991;

fn json_output(context: &Context, status: u16, value: Value) -> Result<Output> {
    Ok(Output::Http(
        Response::new(
            status,
            value.to_string(),
            vec![("content-type".into(), "application/json".into())],
        )
        .into_http(context)?,
    ))
}

fn count(value: Option<Value>) -> Result<u64> {
    let Some(value) = value else {
        return Ok(0);
    };
    let count = value.as_u64().context("stored count is not an integer")?;
    ensure!(count <= MAX_COUNT, "stored count exceeds the example limit");
    Ok(count)
}

struct Counter;

impl Cell for Counter {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                bail!("counter expects HTTP");
            };
            let storage = context.storage()?;
            let count = match request.method.as_str() {
                "GET" => count(storage.get_json("count")?)?,
                "POST" => {
                    // The check and increment belong to one cell transaction.
                    // Different bindings to this cell share that same database.
                    let updated = storage.transaction_sync(|tx| {
                        let current = count(tx.get_json("count")?)?;
                        if current == MAX_COUNT {
                            return Ok(None);
                        }
                        tx.put_json("count", &json!(current + 1))?;
                        Ok(Some(current + 1))
                    })?;
                    let Some(updated) = updated else {
                        return json_output(&context, 409, json!({"error": "counter limit"}));
                    };
                    updated
                }
                _ => return json_output(&context, 405, json!({"error": "use GET or POST"})),
            };
            let identity = context
                .identity()
                .context("counter requires a cell identity")?;
            // These identity fields are useful teaching diagnostics. They are
            // not authentication credentials or a substitute for authorization.
            json_output(
                &context,
                200,
                json!({
                    "class": identity.class,
                    "name": identity.name,
                    "scope": identity.scope,
                    "count": count,
                }),
            )
        })
    }
}

async fn fetch(context: Context, request: Request) -> Result<Output> {
    if !matches!(request.method.as_str(), "GET" | "POST") {
        return json_output(&context, 405, json!({"error": "use GET or POST"}));
    }
    let url = url::Url::parse(&request.url)?;
    let parts: Vec<_> = url.path().split('/').collect();
    let ["", "bindings", binding, name] = parts.as_slice() else {
        return json_output(
            &context,
            404,
            json!({"error": "use /bindings/COUNTERS/demo"}),
        );
    };
    // An application allowlist selects the dependencies exposed by this API.
    // The actual binding-to-class mapping still comes from project configuration.
    if !matches!(*binding, "COUNTERS" | "COUNTER_ALIAS" | "OTHER_COUNTERS") {
        return json_output(&context, 404, json!({"error": "unknown binding"}));
    }
    if name.is_empty()
        || name.len() > 64
        || !name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
    {
        return json_output(&context, 400, json!({"error": "invalid cell name"}));
    }

    // Resolve the declared alias, select an entity, then call its current owner.
    let namespace = context.namespace(binding)?;
    let cell = namespace.get_by_name(name);
    Ok(Output::Http(cell.fetch(&context, request).await?))
}

fn main() -> Result<()> {
    Host::fleet()
        .register("bindings-example", "v1", || {
            Ok(Arc::new(
                Registry::new(|context, event| {
                    Box::pin(async move {
                        let Invocation::Fetch(request) = event else {
                            bail!("application expects HTTP");
                        };
                        fetch(context, request).await
                    })
                })
                // Reuse the behavior while giving each class its own namespace.
                .cell("Counter", |_, _| Ok(Arc::new(Counter)))?
                .cell("OtherCounter", |_, _| Ok(Arc::new(Counter)))?,
            ))
        })?
        .run()
}
