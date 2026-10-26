// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::rpc;
use crate::Limits;
use anyhow::{ensure, Context as _, Result};
use cellar::{
    native::rpc::RemoteError,
    sdk::{cells::Id, Context},
};
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum Identity {
    Name { binding: String, name: Vec<u16> },
    String { binding: String, id: String },
    Unique { binding: String },
}

pub(super) fn identity(context: &Context, input: Value) -> Result<Value> {
    (|| -> Result<Value> {
        let input: Identity = serde_json::from_value(input)?;
        let id = match input {
            Identity::Name { binding, name } => {
                context.namespace(&binding)?.id_from_name_utf16(&name)
            }
            Identity::String { binding, id } => context.namespace(&binding)?.id_from_string(&id)?,
            Identity::Unique { binding } => context.namespace(&binding)?.new_unique_id()?,
        };
        Ok(descriptor(&id))
    })()
    .context(crate::failure::Validation)
}

fn descriptor(id: &Id) -> Value {
    json!({"id":id.to_string(), "name":id.name_utf16()})
}

fn stub(
    context: &Context,
    binding: &str,
    encoded: &str,
    name: Option<Vec<u16>>,
) -> Result<cellar::sdk::cells::Stub> {
    let namespace = context.namespace(binding)?;
    let mut id = namespace.id_from_string(encoded)?;
    if let Some(name) = name {
        let named = namespace.id_from_name_utf16(&name);
        ensure!(named == id, "name does not match ID");
        id = named;
    }
    namespace.get(&id)
}

pub(super) async fn fetch_response(
    context: &Context,
    input: Value,
    options: &Limits,
) -> Result<cellar::engine_api::HttpResponse> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Input {
        binding: String,
        id: String,
        name: Option<Vec<u16>>,
        request: Value,
    }
    let (stub, request) = (|| -> Result<_> {
        let input: Input = serde_json::from_value(input)?;
        let stub = stub(context, &input.binding, &input.id, input.name)?;
        let request = crate::values::input(&input.request, options.max_body_bytes)?;
        Ok((stub, request))
    })()
    .context(crate::failure::Validation)?;
    stub.fetch(context, request).await
}

pub(super) async fn fetch(
    context: &Context,
    input: Value,
    options: &Limits,
    signal: Option<cellar::sdk::web::AbortSignal>,
) -> Result<Value> {
    let call = async {
        let response = fetch_response(context, input, options).await?;
        crate::values::response(response, options.max_body_bytes).await
    };
    match signal {
        None => call.await,
        Some(signal) => {
            cellar::asyncrt::select_biased! { "cancellation revokes a namespace HTTP waiter before an undelivered reply";
                _ = signal.cancelled() => Err(cellar::sdk::rpc::Failure::new(cellar::sdk::rpc::Code::Cancelled).into()),
                result = call => result,
            }
        }
    }
}

pub(super) async fn raw_call(
    context: &Context,
    input: Value,
    options: &Limits,
) -> Result<(cellar::sdk::cells::Stub, cellar::engine_api::RpcData)> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Input {
        binding: String,
        id: String,
        name: Option<Vec<u16>>,
        method: String,
        args: String,
    }
    let (stub, method, args) = (|| -> Result<_> {
        let input: Input = serde_json::from_value(input)?;
        ensure!(
            !input.method.is_empty() && input.method.len() <= 512 && !input.method.contains('\0'),
            "invalid RPC method"
        );
        let stub = stub(context, &input.binding, &input.id, input.name)?;
        let args = rpc::arguments(&input.args, options.max_body_bytes)?;
        Ok((stub, input.method, args))
    })()
    .context(crate::failure::Validation)?;
    // This is the original native invocation's routed RPC future. It retains
    // the host's output/durability gates, cancellation and failure provenance.
    let args = clone_arguments(&args, options.max_body_bytes)?;
    let result = stub.rpc(context, method, args).await?;
    Ok((stub, result))
}

fn clone_arguments(
    args: &cellar::native::clone::StructuredValue,
    maximum: usize,
) -> Result<cellar::engine_api::RpcData> {
    Ok(cellar::engine_api::RpcData::Structured(
        cellar::native::clone::encode_with_limits(args, rpc::limits(maximum))?.into(),
    ))
}

pub(super) fn reply(data: &cellar::engine_api::RpcData, maximum: usize) -> Result<Value> {
    match cellar::sdk::decode_rpc_result_with_limits(data, rpc::limits(maximum)) {
        Ok(value) => Ok(json!({"value":rpc::encoded(&value, maximum)?, "error":false})),
        Err(error) => match error.downcast_ref::<RemoteError>() {
            Some(remote) => {
                Ok(json!({"value":rpc::encoded(&remote.value, maximum)?, "error":true}))
            }
            None => Err(error),
        },
    }
}

pub(super) async fn call(context: &Context, input: Value, options: &Limits) -> Result<Value> {
    let (_, data) = raw_call(context, input, options).await?;
    reply(&data, options.max_body_bytes)
}
