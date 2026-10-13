use crate::support;
use anyhow::Result;
use cellar::engine_api::{QueueBatch, RpcData};
use cellar::sdk::{
    queue::{self, Body, SendOptions},
    Context, Output, Request,
};

pub async fn fetch(context: &Context, request: Request) -> Result<Output> {
    support::method(&request, &["GET", "POST"])?;
    let producer = context.queue("JOBS")?;
    if request.method == "GET" {
        let url = url::Url::parse(&request.url)?;
        let queue = if url.path() == "/queues/dead" {
            context.queue("DEAD")?
        } else {
            producer
        };
        let metrics = queue.metrics(context).await?;
        return support::json(
            context,
            serde_json::json!({
                "backlog_count": metrics.backlog_count,
                "backlog_bytes": metrics.backlog_bytes,
                "oldest_message_timestamp_ms": metrics.oldest_message_timestamp_ms,
            }),
        );
    }
    let body = support::payload(context, request).await?;
    support::name(body["id"].as_str().unwrap_or_default())?;
    producer
        .send(context, Body::Json(body), SendOptions::default())
        .await?;
    support::json_status(context, 202, serde_json::json!({ "accepted": true }))
}

pub async fn consume(context: Context, incoming: QueueBatch) -> Result<Output> {
    queue::dispatch(incoming, move |event| async move {
        for message in event.messages() {
            let Body::Json(body) = &message.body else {
                message.retry(Some(1))?;
                continue;
            };
            // fail=true always retries until DLQ; retry_once demonstrates recovery.
            if body["fail"] == true || (body["retry_once"] == true && message.attempts <= 1) {
                message.retry(Some(1))?;
                continue;
            }
            let key = body["id"]
                .as_str()
                .ok_or_else(|| support::reject(400, "job id missing"))?;
            // The sink commits effect + key atomically. A crash before ack is harmless.
            context
                .namespace("COUNTERS")?
                .get_by_name("queue-effects")
                .rpc(
                    &context,
                    "increment_once",
                    RpcData::Json(serde_json::json!({ "key": key }).to_string()),
                )
                .await?;
            message.ack()?;
        }
        Ok(())
    })
    .await
}
