// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared container and sandbox provider boundary.
pub use cellar_container::{
    Adapter, Limits, Native, Operation, Provider, ProviderFuture, ProviderSession, Session,
};

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
