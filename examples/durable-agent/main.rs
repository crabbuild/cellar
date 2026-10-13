//! A bounded native tool loop with persisted checkpoints and durable wakes.

use crate::support;
use cellar::sdk::{Cell, Context, HandlerFuture, Invocation, Output};
use serde_json::json;
use std::sync::Arc;

pub struct Agent;

impl Cell for Agent {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let storage = context.storage()?;
            match event {
                Invocation::Fetch(request) => {
                    support::method(&request, &["GET", "POST"])?;
                    if request.method == "POST" {
                        let value = support::payload(&context, request).await?;
                        let text = value["text"]
                            .as_str()
                            .filter(|v| v.len() <= 4096)
                            .ok_or_else(|| {
                                support::reject(400, "text must be at most 4096 bytes")
                            })?;
                        storage.transaction_sync(|tx| {
                            if tx.get_json("job")?.is_some() {
                                return Err(support::reject(
                                    409,
                                    "use a new agent name for a new run",
                                ));
                            }
                            tx.put_json(
                                "job",
                                &json!({
                                    "text": text,
                                    "stage": "count",
                                    "steps": 0,
                                }),
                            )?;
                            tx.set_alarm(support::now_ms() + 100)
                        })?;
                    }
                    support::json(
                        &context,
                        storage
                            .get_json("job")?
                            .unwrap_or(json!({ "stage": "idle" })),
                    )
                }
                Invocation::Alarm { .. } => {
                    storage.transaction_sync(|tx| {
                        let Some(mut job) = tx.get_json("job")? else {
                            return Ok(());
                        };
                        match job["stage"].as_str() {
                            Some("count") => {
                                let words = job["text"]
                                    .as_str()
                                    .unwrap_or_default()
                                    .split_whitespace()
                                    .count();
                                job["words"] = json!(words);
                                job["stage"] = json!("report");
                                job["steps"] = json!(1);
                                tx.set_alarm(support::now_ms() + 100)?;
                            }
                            Some("report") => {
                                job["summary"] =
                                    json!(format!("Document contains {} words", job["words"]));
                                job["stage"] = json!("complete");
                                job["steps"] = json!(2);
                            }
                            _ => return Ok(()),
                        }
                        tx.put_json("job", &job)
                    })?;
                    Ok(Output::Done)
                }
                _ => Ok(Output::Done),
            }
        })
    }
}
