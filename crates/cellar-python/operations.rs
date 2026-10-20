use crate::{
    runtime::Options,
    values::{self, field},
};
use anyhow::{bail, ensure, Context as _, Result};
use cellar::sdk::{kv, queue, Context};
use serde_json::{json, Value};

pub(crate) struct State {
    pub queue: Option<queue::Event>,
    pub scheduled: Option<cellar::sdk::cron::Scheduled>,
}

/// Only operations known to have no business effects may overlap a mutation.
/// Arbitrary SQL and nested fetches are conservatively mutation-class operations.
pub(crate) fn is_read(name: &str) -> bool {
    matches!(
        name,
        "clock.now" | "storage.read" | "alarm.get" | "var" | "kv.get" | "kv.list"
    )
}

pub(crate) async fn execute(
    context: &Context,
    state: &State,
    name: &str,
    value: Value,
    options: &Options,
) -> Result<Value> {
    match name {
        "clock.now" => Ok(json!(cellar::asyncrt::wall_ms())),
        "storage.read" => {
            let value = context.storage()?.get_json(field(&value, "key")?)?;
            Ok(json!({"present":value.is_some(),"value":value}))
        }
        "storage.apply" => crate::storage::apply(context, value, options.max_batch_operations),
        "storage.sync" => {
            context.storage()?.sync().await?;
            Ok(Value::Null)
        }
        "storage.sql" => {
            let params: Vec<Value> = serde_json::from_value(value["params"].clone())?;
            let (columns, rows, written) =
                context.storage()?.sql(field(&value, "sql")?, &params)?;
            Ok(json!({"columns":columns,"rows":rows,"written":written}))
        }
        "alarm.get" => Ok(json!(context.storage()?.get_alarm()?)),
        "var" => Ok(json!(context.var(field(&value, "name")?))),
        "cell.fetch" => {
            let request = values::input(&value["request"], options.max_body_bytes)?;
            let result = context
                .namespace(field(&value, "binding")?)?
                .get_by_name(field(&value, "name")?)
                .fetch(context, request)
                .await?;
            values::response(result, options.max_body_bytes).await
        }
        "kv.get" => {
            let result = context
                .kv(field(&value, "binding")?)?
                .get(context, field(&value, "key")?, kv::ReadType::Json)
                .await?;
            match result {
                None => Ok(Value::Null),
                Some(kv::ReadValue::Json(value)) => Ok(value),
                _ => bail!("unexpected KV JSON result"),
            }
        }
        "kv.put" => {
            context
                .kv(field(&value, "binding")?)?
                .put_text(
                    context,
                    field(&value, "key")?,
                    &serde_json::to_string(&value["value"])?,
                    kv::PutOptions {
                        expiration_ttl: optional(&value, "ttl_seconds")?,
                        metadata: value
                            .get("metadata")
                            .filter(|value| !value.is_null())
                            .cloned(),
                        ..Default::default()
                    },
                )
                .await?;
            Ok(Value::Null)
        }
        "kv.delete" => {
            context
                .kv(field(&value, "binding")?)?
                .delete(context, field(&value, "key")?)
                .await?;
            Ok(Value::Null)
        }
        "kv.list" => {
            let result = context
                .kv(field(&value, "binding")?)?
                .list(
                    context,
                    kv::ListOptions {
                        prefix: field(&value, "prefix")?.into(),
                        limit: optional(&value, "limit")?,
                        cursor: optional(&value, "cursor")?,
                    },
                )
                .await?;
            Ok(
                json!({"keys":result.keys.into_iter().map(|key|json!({"name":key.name,"metadata":key.metadata,"expiration":key.expiration})).collect::<Vec<_>>(),"cursor":result.cursor,"list_complete":result.list_complete}),
            )
        }
        "sql.query" => {
            let params: Vec<Value> = serde_json::from_value(value["params"].clone())?;
            let result = context
                .sql(field(&value, "binding")?)?
                .prepare(field(&value, "sql")?)
                .bind_json(params)?
                .all(context)
                .await?;
            Ok(serde_json::to_value(result)?)
        }
        "queue.send" => {
            let delay_seconds: Option<u32> = optional(&value, "delay_seconds")?;
            context
                .queue(field(&value, "binding")?)?
                .send(
                    context,
                    queue::Body::Json(value["body"].clone()),
                    queue::SendOptions { delay_seconds },
                )
                .await?;
            Ok(Value::Null)
        }
        "queue.settle" => {
            let event = state
                .queue
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("no queue batch in this invocation"))?;
            let retry = value["retry"]
                .as_bool()
                .context("queue retry must be a boolean")?;
            let delay: Option<i32> = optional(&value, "delay_seconds")?;
            ensure!(delay.is_none_or(|v| v >= 0), "negative queue retry delay");
            if let Some(id) = optional::<String>(&value, "id")? {
                let message = event
                    .messages()
                    .iter()
                    .find(|message| message.id == id)
                    .ok_or_else(|| anyhow::anyhow!("unknown queue message"))?;
                if retry {
                    message.retry(delay)?;
                } else {
                    message.ack()?;
                }
            } else if retry {
                event.retry_all(delay)?;
            } else {
                event.ack_all()?;
            }
            Ok(Value::Null)
        }
        "cron.no_retry" => {
            state
                .scheduled
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("not a scheduled invocation"))?
                .no_retry();
            Ok(Value::Null)
        }
        _ => bail!("unsupported Python SDK operation: {name}"),
    }
}

fn optional<T: serde::de::DeserializeOwned>(value: &Value, name: &str) -> Result<Option<T>> {
    value
        .get(name)
        .filter(|value| !value.is_null())
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()
        .with_context(|| format!("invalid {name}"))
}
