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

pub(super) async fn call(context: &Context, input: Value, options: &Limits) -> Result<Value> {
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
        let namespace = context.namespace(&input.binding)?;
        let mut id = namespace.id_from_string(&input.id)?;
        if let Some(name) = input.name {
            let named = namespace.id_from_name_utf16(&name);
            ensure!(named == id, "RPC name does not match ID");
            id = named;
        }
        let args = rpc::arguments(&input.args, options.max_body_bytes)?;
        Ok((namespace.get(&id)?, input.method, args))
    })()
    .context(crate::failure::Validation)?;
    // This is the original native invocation's routed RPC future. It retains
    // the host's output/durability gates, cancellation and failure provenance.
    match stub
        .call_with_limits(context, method, &args, rpc::limits(options.max_body_bytes))
        .await
    {
        Ok(value) => {
            Ok(json!({"value":rpc::encoded(&value, options.max_body_bytes)?, "error":false}))
        }
        Err(error) => match error.downcast_ref::<RemoteError>() {
            Some(remote) => Ok(
                json!({"value":rpc::encoded(&remote.value, options.max_body_bytes)?, "error":true}),
            ),
            None => Err(error),
        },
    }
}
