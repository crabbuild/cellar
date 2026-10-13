use crate::support;
use cellar::sdk::{
    queue::{Body, SendOptions},
    Cell, Context, HandlerFuture, Invocation, Output,
};
use serde_json::json;
use std::sync::Arc;
pub struct Outbox;
impl Cell for Outbox {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let storage = context.storage()?;
            storage.sql("CREATE TABLE IF NOT EXISTS orders(id TEXT PRIMARY KEY,payload TEXT NOT NULL,sent INTEGER NOT NULL DEFAULT 0)",&[])?;
            match event {
                Invocation::Fetch(request) => {
                    support::method(&request, &["GET", "POST"])?;
                    if request.method == "POST" {
                        let value = support::payload(&context, request).await?;
                        let id = support::name(value["id"].as_str().unwrap_or_default())?;
                        let encoded = serde_json::to_string(&value)?;
                        storage.transaction_sync(|tx| {
                            let (_, rows, _) =
                                tx.sql("SELECT payload FROM orders WHERE id=?", &[json!(id)])?;
                            if let Some(row) = rows.first() {
                                if row.first().and_then(|v| v.as_str()) != Some(&encoded) {
                                    return Err(support::reject(409, "order id already used"));
                                }
                            } else {
                                // Business record and pending publication are the same atomic row.
                                tx.sql(
                                    "INSERT INTO orders(id,payload) VALUES (?,?)",
                                    &[json!(id), json!(encoded)],
                                )?;
                            }
                            tx.set_alarm(support::now_ms() + 100)
                        })?;
                        return support::json_status(
                            &context,
                            202,
                            json!({"id":id,"accepted":true}),
                        );
                    }
                    let (_, rows, _) =
                        storage.sql("SELECT id,sent FROM orders ORDER BY id LIMIT 100", &[])?;
                    support::json(&context, json!({"orders":rows}))
                }
                Invocation::Alarm { .. } => {
                    // Re-arm before egress; interrupted delivery remains discoverable.
                    storage.set_alarm(support::now_ms() + 1000)?;
                    let (_, rows, _) = storage.sql(
                        "SELECT id,payload FROM orders WHERE sent=0 ORDER BY id LIMIT 20",
                        &[],
                    )?;
                    for row in rows {
                        let id = row[0]
                            .as_str()
                            .ok_or_else(|| anyhow::anyhow!("invalid stored order id"))?;
                        let payload: serde_json::Value = serde_json::from_str(
                            row[1]
                                .as_str()
                                .ok_or_else(|| anyhow::anyhow!("invalid stored payload"))?,
                        )?;
                        let entity = context
                            .identity()
                            .ok_or_else(|| anyhow::anyhow!("missing outbox identity"))?;
                        let key = format!("outbox:{}:{id}", entity.scope);
                        context
                            .queue("JOBS")?
                            .send(
                                &context,
                                Body::Json(json!({"id":key,"order":payload})),
                                SendOptions::default(),
                            )
                            .await?;
                        // A crash between send and update can redeliver; consumer dedup is required.
                        storage.sql("UPDATE orders SET sent=1 WHERE id=?", &[json!(id)])?;
                    }
                    storage.transaction_sync(|tx| {
                        let (_, rows, _) =
                            tx.sql("SELECT 1 FROM orders WHERE sent=0 LIMIT 1", &[])?;
                        if rows.is_empty() {
                            tx.delete_alarm()?;
                        }
                        Ok(())
                    })?;
                    Ok(Output::Done)
                }
                _ => Ok(Output::Done),
            }
        })
    }
}
