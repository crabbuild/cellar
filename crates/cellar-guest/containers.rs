// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared container and sandbox provider boundary.
pub use cellar_container::{
    Adapter, BindingCapabilities, BindingSessions, Bindings, Capabilities, Environment, Fencing,
    Limits, Native, Operation, Provider, ProviderFuture, ProviderSession, Reconciliation,
    ReconciliationState, Session,
};

pub(crate) async fn workers(
    context: &cellar::sdk::Context,
    state: &crate::operations::State,
    name: &str,
    value: serde_json::Value,
    limits: &crate::Limits,
) -> anyhow::Result<serde_json::Value> {
    use crate::failure::Validation;
    use anyhow::{ensure, Context as _};
    use serde::Deserialize;
    use serde_json::Value;
    if name == "workers.container.running" {
        ensure!(
            value.as_object().is_some_and(serde_json::Map::is_empty),
            Validation
        );
        return state.containers.running_snapshot(context).map(Value::Bool);
    }
    ensure!(state.workers_lifetimes, Validation);
    let limits = Limits {
        max_body_bytes: limits.max_body_bytes,
    };
    match name {
        "workers.container.admit" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Admission {
                operation: String,
                input: Value,
            }
            let admission: Admission = serde_json::from_value(value).context(Validation)?;
            let operation = Operation::parse(&admission.operation).context(Validation)?;
            state
                .containers
                .admit_queued(context, operation, admission.input, &limits)
                .map(Value::String)
        }
        "workers.container.perform" | "workers.container.discard" => {
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct Perform {
                ticket: String,
            }
            let request: Perform = serde_json::from_value(value).context(Validation)?;
            if name == "workers.container.discard" {
                state.containers.discard_queued(context, &request.ticket)?;
                return Ok(Value::Null);
            }
            state
                .containers
                .execute_queued(context, &request.ticket, &limits)
                .await
        }
        _ => anyhow::bail!(Validation),
    }
}

pub(crate) async fn execute(
    context: &cellar::sdk::Context,
    state: &crate::operations::State,
    name: &str,
    mut value: serde_json::Value,
    limits: &crate::Limits,
) -> anyhow::Result<serde_json::Value> {
    if name == "container.bindings" {
        anyhow::ensure!(
            value.as_object().is_some_and(serde_json::Map::is_empty),
            crate::failure::Validation
        );
        let mut bindings = Vec::new();
        for binding in std::iter::once("container").chain(state.container_bindings.names()) {
            let session = if binding == "container" {
                &state.containers
            } else {
                state.container_bindings.get(binding)?
            };
            let capabilities = session
                .execute(
                    context,
                    Operation::Capabilities,
                    serde_json::json!({}),
                    &Limits {
                        max_body_bytes: limits.max_body_bytes,
                    },
                )
                .await?;
            bindings.push(serde_json::json!({"name":binding,"capabilities":capabilities}));
        }
        let result = serde_json::Value::Array(bindings);
        anyhow::ensure!(
            serde_json::to_vec(&result)?.len() <= limits.max_body_bytes,
            "container binding discovery exceeds byte limit"
        );
        return Ok(result);
    }
    let session = select(state, &mut value)?;
    let operation = Operation::parse(name)
        .map_err(anyhow::Error::new)
        .map_err(|error| error.context(crate::failure::Validation))?;
    session
        .execute(
            context,
            operation,
            value,
            &cellar_container::Limits {
                max_body_bytes: limits.max_body_bytes,
            },
        )
        .await
}

pub(crate) fn select<'a>(
    state: &'a crate::operations::State,
    value: &mut serde_json::Value,
) -> anyhow::Result<&'a Session> {
    use crate::failure::Validation;
    let object = value
        .as_object_mut()
        .ok_or_else(|| anyhow::anyhow!(Validation))?;
    match object.remove("binding") {
        None => Ok(&state.containers),
        Some(serde_json::Value::String(name)) if name == "container" => Ok(&state.containers),
        Some(serde_json::Value::String(name)) => state
            .container_bindings
            .get(&name)
            .map_err(|error| error.context(Validation)),
        _ => anyhow::bail!(Validation),
    }
}
