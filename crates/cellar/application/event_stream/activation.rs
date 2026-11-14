// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Resource bootstrap uses ordinary target commands and current acceptance proofs.

use super::{authority::Authority, Binding, BindingConfig, Definition};
use crate::application::{consensus::native_event_rpc, HandlerFuture};
use anyhow::Result;
use base64::Engine;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub(crate) trait Host {
    fn rpc(
        &self,
        principal: &str,
        authority: native_event_rpc::Authority,
        scope: String,
        name: String,
        method: &'static str,
        args: Value,
    ) -> HandlerFuture<Value>;
}

fn rpc(
    host: &impl Host,
    principal: &str,
    authority: impl Into<native_event_rpc::Authority>,
    scope: String,
    name: String,
    method: &'static str,
    args: Value,
) -> HandlerFuture<Value> {
    host.rpc(principal, authority.into(), scope, name, method, args)
}

pub(crate) async fn configure(
    host: &impl Host,
    definitions: &[(String, Definition)],
    declarations: &[BindingConfig],
) -> Result<Vec<Binding>> {
    use super as event_stream;
    for (script, definition) in definitions {
        let bytes = event_stream::encoding::json(definition, event_stream::MAX_DEFINITION_BYTES)?;
        let digest = hex::encode(Sha256::digest(&bytes));
        let operation = event_stream::raft::operation(script, definition)?;
        let authority = Authority::deployment(script, definition)?;
        let configured = rpc(host, script, authority.clone(), event_stream::catalog_scope(&definition.resource)?, event_stream::catalog_name(&definition.resource)?,
            "__catalogConfigure", json!([{"operation":operation,"definition":base64::engine::general_purpose::STANDARD.encode(bytes)}])).await?;
        anyhow::ensure!(
            configured["state"].as_str() == Some("active"),
            "event resource configuration is pending: {}",
            configured["last_error"]
        );
        // A repeated operation returns history. Check the catalog's current
        // revision/digest and lifecycle before accepting deployment readiness.
        let current = rpc(
            host,
            script,
            authority,
            event_stream::catalog_scope(&definition.resource)?,
            event_stream::catalog_name(&definition.resource)?,
            "__catalogStatus",
            json!([{}]),
        )
        .await?;
        anyhow::ensure!(
            current["state"].as_str() == Some("active")
                && current["resource"].as_str() == Some(definition.resource.id.as_str())
                && current["accepted_revision"].as_str()
                    == Some(definition.revision.to_string().as_str())
                && current["accepted_digest"].as_str() == Some(digest.as_str()),
            "event resource does not match the current deployment definition"
        );
    }
    let mut activated = Vec::new();
    for declaration in declarations {
        let script = declaration.principal.as_str();
        let binding = if declaration.resource.id.is_some() {
            declaration.explicit()?
        } else {
            let authority = event_stream::name_resolution::NameAuthority::new(declaration)?;
            let namespace = authority.namespace();
            let resolved = rpc(host, script, authority, namespace.scope()?, namespace.name()?,
                event_stream::name_resolution::METHOD, json!([{"declaration":base64::engine::general_purpose::STANDARD.encode(event_stream::encoding::json(declaration, 64<<10)?)}])).await?;
            anyhow::ensure!(
                resolved["state"] == "active",
                "named event binding is not active"
            );
            declaration.resolve(serde_json::from_value(resolved["resource"].clone())?)?
        };
        let authority = Authority::resolution(&binding)?;
        // Retirement intent closes new binding adoption before delegated
        // partition closure finishes. Check both authorities; partition-only
        // resolution could accept a still-active partition during retirement.
        let catalog = rpc(
            host,
            script,
            authority.clone(),
            event_stream::catalog_scope(&binding.resource)?,
            event_stream::catalog_name(&binding.resource)?,
            "__catalogResolve",
            json!([{}]),
        )
        .await?;
        let resource: event_stream::Resource = serde_json::from_value(catalog["resource"].clone())?;
        anyhow::ensure!(
            catalog["state"].as_str() == Some("active") && resource == binding.resource,
            "catalog binding identity differs from the deployment"
        );
        let encoded = event_stream::encoding::json(&binding, 64 << 10)?;
        let resolved = rpc(
            host,
            script,
            authority,
            event_stream::cell_scope(&binding.resource)?,
            event_stream::cell_name(&binding.resource)?,
            "__streamResolve",
            json!([{"binding":base64::engine::general_purpose::STANDARD.encode(encoded)}]),
        )
        .await?;
        let resource: event_stream::Resource =
            serde_json::from_value(resolved["resource"].clone())?;
        anyhow::ensure!(
            resolved["state"].as_str() == Some("active") && resource == binding.resource,
            "resolved event binding identity differs from the deployment"
        );
        activated.push(binding);
    }
    Ok(activated)
}
