use crate::support;
use cellar::sdk::{Cell, Context, HandlerFuture, Invocation, Output};
use std::sync::Arc;
pub struct Alarm;
impl Cell for Alarm {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let storage = context.storage()?;
            match event {
                Invocation::Fetch(request) => {
                    support::method(&request, &["GET", "POST", "DELETE"])?;
                    if request.method == "POST" {
                        // A persisted alarm survives eviction and process restart.
                        storage.transaction_sync(|tx| {
                            tx.put_json("status", &serde_json::json!("pending"))?;
                            tx.set_alarm(support::now_ms() + 500)
                        })?;
                    } else if request.method == "DELETE" {
                        storage.delete_alarm()?;
                    }
                    support::json(
                        &context,
                        serde_json::json!({"alarm":storage.get_alarm()?,"status":storage.get_json("status")?}),
                    )
                }
                Invocation::Alarm { scheduled_ms } => {
                    // Keep the effect and its deduplication marker in one transaction.
                    storage.transaction_sync(|tx| {
                        if tx.get_json("last")?.and_then(|v| v.as_i64()) != Some(scheduled_ms) {
                            tx.put_json("last", &serde_json::json!(scheduled_ms))?;
                            tx.put_json("status", &serde_json::json!("fired"))?;
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
