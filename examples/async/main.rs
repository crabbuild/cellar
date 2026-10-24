// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use crate::support;
use cellar::sdk::{Cell, Context, HandlerFuture, Invocation, Output};
use std::{sync::Arc, time::Duration};

pub struct Background;

impl Cell for Background {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let Invocation::Fetch(request) = event else {
                return Ok(Output::Done);
            };
            support::method(&request, &["GET", "POST"])?;
            let storage = context.storage()?;
            if request.method == "POST" {
                // Event-owned work is included in graceful drain, but is not a durable job.
                let background = context.clone();
                context.wait_until(async move {
                    cellar::asyncrt::sleep(Duration::from_millis(100)).await;
                    let storage = background.storage()?;
                    let count = storage
                        .get_json("completed")?
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0);
                    storage.put_json(
                        "completed",
                        &serde_json::json!(count
                            .checked_add(1)
                            .ok_or_else(|| anyhow::anyhow!("count overflow"))?),
                    )?;
                    storage.sync().await
                })?;
            }
            support::json(
                &context,
                serde_json::json!({
                    "completed": storage.get_json("completed")?.unwrap_or(serde_json::json!(0)),
                }),
            )
        })
    }
}
