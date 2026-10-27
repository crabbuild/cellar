// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared durable Counter used by native and cross-language RPC qualification.
use super::generated::{
    increment_response, Count, CounterHandler, GetCountRequest, GetCountResponse, IncrementRequest,
    IncrementResponse, OperationConflict, OutOfRange,
};
use cellar::sdk::{rpc, Context};
use std::sync::Arc;

pub struct Counter;

fn internal(_: impl std::fmt::Display) -> rpc::Failure {
    rpc::Failure::new(rpc::Code::Internal)
}

fn rejected(reason: &str) -> IncrementResponse {
    IncrementResponse {
        result: Some(increment_response::Result::OutOfRange(OutOfRange {
            reason: reason.into(),
        })),
    }
}

impl CounterHandler for Counter {
    fn increment(
        self: Arc<Self>,
        context: Context,
        request: IncrementRequest,
    ) -> rpc::HandlerFuture<IncrementResponse> {
        increment_with_checkpoint(context, request, |_, _, _| Ok(()))
    }

    fn get_count(
        self: Arc<Self>,
        context: Context,
        _: GetCountRequest,
    ) -> rpc::HandlerFuture<GetCountResponse> {
        Box::pin(async move {
            let value = context
                .storage()
                .map_err(internal)?
                .get_json("count")
                .map_err(internal)?;
            let value = match value {
                None => 0,
                Some(value) => value
                    .as_str()
                    .ok_or_else(|| internal("invalid counter"))?
                    .parse()
                    .map_err(internal)?,
            };
            Ok(GetCountResponse {
                count: Some(Count { value }),
            })
        })
    }
}

#[derive(Clone, Copy)]
pub enum Checkpoint {
    AfterCount,
    AfterCommit,
}

/// The crash executable observes between writes and before the sync await.
/// Ordinary fixtures use the no-op callback above; no runtime fault hook or
/// alternate transaction implementation is added to the production SDK.
pub fn increment_with_checkpoint(
    context: Context,
    request: IncrementRequest,
    mut checkpoint: impl FnMut(&Context, &str, Checkpoint) -> anyhow::Result<()> + Send + 'static,
) -> rpc::HandlerFuture<IncrementResponse> {
    Box::pin(async move {
        let Some(delta) = request.delta.filter(|value| *value > 0) else {
            return Ok(rejected("positive delta is required"));
        };
        if request.operation_id.is_empty() || request.operation_id.len() > 128 {
            return Ok(rejected("operation ID is required and must fit 128 bytes"));
        }
        let storage = context.storage().map_err(internal)?;
        let receipt = format!("receipt:{}", request.operation_id);
        let (response, applied) = storage
            .transaction_sync(|storage| {
                if let Some(saved) = storage.get_json(&receipt)? {
                    if saved["delta"].as_str() != Some(delta.to_string().as_str()) {
                        return Ok((
                            IncrementResponse {
                                result: Some(increment_response::Result::OperationConflict(
                                    OperationConflict {},
                                )),
                            },
                            false,
                        ));
                    }
                    let value: i64 = saved["value"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("invalid receipt"))?
                        .parse()?;
                    return Ok((
                        IncrementResponse {
                            result: Some(increment_response::Result::Count(Count { value })),
                        },
                        false,
                    ));
                }
                let current: i64 = match storage.get_json("count")? {
                    None => 0,
                    Some(value) => value
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("invalid count"))?
                        .parse()?,
                };
                let Some(value) = current.checked_add(delta) else {
                    return Ok((rejected("counter overflow"), false));
                };
                storage.put_json("count", &serde_json::json!(value.to_string()))?;
                checkpoint(&context, &request.operation_id, Checkpoint::AfterCount)?;
                storage.put_json(
                    &receipt,
                    &serde_json::json!({"delta": delta.to_string(), "value": value.to_string()}),
                )?;
                Ok((
                    IncrementResponse {
                        result: Some(increment_response::Result::Count(Count { value })),
                    },
                    true,
                ))
            })
            .map_err(internal)?;
        if applied {
            checkpoint(&context, &request.operation_id, Checkpoint::AfterCommit)
                .map_err(internal)?;
        }
        // The proof suite deliberately omits explicit sync so the host's
        // output gate, rather than a handler await, must hold the reply.
        if !request.operation_id.starts_with("proof-no-sync-") {
            storage.sync().await.map_err(internal)?;
        }
        if applied && request.operation_id.starts_with("held-reply-quota-") {
            // Valid generated output after durable count/receipt writes. The
            // receiving transport, rather than the codec, must bound this reply.
            return Ok(rejected(&"x".repeat(64 * 1024)));
        }
        if applied && request.operation_id == "oversized-reply" {
            // Deliberate qualification-only bug after durable count/receipt
            // writes. The generated encoder must refuse with unknown effects;
            // an explicit retry returns the saved result without another write.
            return Ok(rejected(&"x".repeat(1024 * 1024 + 1)));
        }
        Ok(response)
    })
}
