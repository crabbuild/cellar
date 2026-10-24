// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use crate::{
    values::{self, field},
    Limits,
};
use anyhow::{bail, ensure, Context as _, Result};
use cellar::sdk::{kv, queue, Context};
use serde_json::{json, Value};

#[derive(Default)]
pub struct State {
    pub containers: crate::containers::Session,
    pub transport: std::sync::Arc<crate::transport::Session>,
    pub queue: Option<queue::Event>,
    pub scheduled: Option<cellar::sdk::cron::Scheduled>,
}

/// Only operations known to have no business effects may overlap a mutation.
/// Arbitrary SQL and nested fetches are conservatively mutation-class operations.
/// Python's bounded reply adapter implements `storage.read_many` separately.
pub fn is_read(name: &str) -> bool {
    if let Ok(operation) = crate::containers::Operation::parse(name) {
        return operation.is_read();
    }
    matches!(
        name,
        "clock.now"
            | "storage.read"
            | "storage.read_many"
            | "alarm.get"
            | "var"
            | "kv.get"
            | "kv.list"
            | "websocket.next"
            | "response.read"
    )
}

/// A binary cell fetch uses the same pre-effect validation and original SDK
/// authority as the JSON transport. Result collection can fail after effects.
pub async fn fetch_bytes(
    context: &Context,
    value: Value,
    body: Vec<u8>,
    options: &Limits,
) -> Result<(Value, Vec<u8>)> {
    let request = (|| {
        validate("cell.fetch", &value)?;
        ensure!(
            field(&value["request"], "body")?.is_empty(),
            "binary request metadata contains a body"
        );
        values::input_body(&value["request"], body, options.max_body_bytes)
    })()
    .context(crate::failure::Validation)?;
    let result = context
        .namespace(field(&value, "binding")?)?
        .get_by_name(field(&value, "name")?)
        .fetch(context, request)
        .await?;
    values::response_parts(result, options.max_body_bytes).await
}

pub async fn execute(
    context: &Context,
    state: &State,
    name: &str,
    value: Value,
    options: &Limits,
) -> Result<Value> {
    if name.starts_with("container.") {
        values::check_depth(&value).context(crate::failure::Validation)?;
        ensure!(value.is_object(), crate::failure::Validation);
        return crate::containers::execute(context, &state.containers, name, value, options).await;
    }
    if name == "cell.fetch_stream"
        || name == "http.fetch_stream"
        || name.starts_with("response.")
        || name.starts_with("websocket.")
    {
        return state.transport.execute(context, name, value, options).await;
    }
    validate(name, &value).context(crate::failure::Validation)?;
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
            let request = values::input(&value["request"], options.max_body_bytes)
                .context(crate::failure::Validation)?;
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
                .bind_json(params)
                .context(crate::failure::Validation)?
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
                .context("no queue batch in this invocation")
                .context(crate::failure::Validation)?;
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
                    .context("unknown queue message")
                    .context(crate::failure::Validation)?;
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
                .context("not a scheduled invocation")
                .context(crate::failure::Validation)?
                .no_retry();
            Ok(Value::Null)
        }
        _ => bail!("unsupported guest SDK operation: {name}"),
    }
}

/// Validate the bridge protocol before entering SDK code. This marker must not
/// wrap a backend result: a backend error can follow a committed mutation.
fn validate(name: &str, value: &Value) -> Result<()> {
    values::check_depth(value)?;
    ensure!(value.is_object(), "operation payload must be an object");
    let fields: &[&str] = match name {
        "clock.now" | "alarm.get" | "storage.sync" | "storage.apply" => &[],
        "storage.read" => &["key"],
        "var" => &["name"],
        "storage.sql" => &["sql"],
        "cell.fetch" => &["binding", "name"],
        "kv.get" | "kv.put" | "kv.delete" => &["binding", "key"],
        "kv.list" => &["binding", "prefix"],
        "sql.query" => &["binding", "sql"],
        "queue.send" => &["binding"],
        "queue.settle" => &[],
        "cron.no_retry" => &[],
        _ => bail!("unsupported operation"),
    };
    for name in fields {
        field(value, name)?;
    }
    match name {
        "storage.sql" | "sql.query" => {
            ensure!(value["params"].is_array(), "SQL params must be an array");
        }
        "kv.put" => {
            ensure!(value.get("value").is_some(), "missing KV value");
            optional::<f64>(value, "ttl_seconds")?;
        }
        "kv.list" => {
            optional::<i64>(value, "limit")?;
            optional::<String>(value, "cursor")?;
        }
        "queue.send" => {
            ensure!(value.get("body").is_some(), "missing queue body");
            optional::<u32>(value, "delay_seconds")?;
        }
        "queue.settle" => {
            ensure!(value["retry"].is_boolean(), "invalid retry decision");
            ensure!(
                optional::<i32>(value, "delay_seconds")?.is_none_or(|delay| delay >= 0),
                "negative queue retry delay"
            );
            optional::<String>(value, "id")?;
        }
        _ => {}
    }
    Ok(())
}

fn optional<T: serde::de::DeserializeOwned>(value: &Value, name: &str) -> Result<Option<T>> {
    value
        .get(name)
        .filter(|value| !value.is_null())
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()
        .with_context(|| format!("invalid {name}"))
}
