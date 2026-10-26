// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared container and sandbox provider boundary.
pub use cellar_container::{
    Adapter, Capabilities, Environment, Fencing, Limits, Native, Operation, Provider,
    ProviderFuture, ProviderSession, Reconciliation, ReconciliationState, Session,
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
    session: &Session,
    name: &str,
    value: serde_json::Value,
    limits: &crate::Limits,
) -> anyhow::Result<serde_json::Value> {
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
