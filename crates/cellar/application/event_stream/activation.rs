// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Resource bootstrap uses ordinary target commands and current acceptance proofs.

use super::{authority::Authority, Binding, BindingConfig, Definition};
use crate::application::{consensus::native_event_rpc, HandlerFuture};
use anyhow::Result;
use base64::Engine;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub(crate) struct ConfigurationStatus {
    pub value: Value,
    pub after: Option<crate::raft::version::CellVersionToken>,
}

pub(crate) trait Host {
    /// A startup/history hint chooses whether to try the immutable fast path.
    /// It grants no authority and never replaces the subsequent strong read.
    /// Fresh owners with unknown catalogs use ordinary durable creation;
    /// resumed owners can try a strong read while their route cache is cold.
    fn configuration_known(&self, _principal: &str, _authority: &Authority) -> Result<bool> {
        Ok(false)
    }

    /// Prepare only a declared broker's birth. This is never binding readiness;
    /// catalog reconciliation and independent target proofs still gate serving.
    fn prepare_broker(&self, _principal: &str, _definition: &Definition) -> HandlerFuture<()> {
        Box::pin(async { Ok(()) })
    }

    /// Shared-group hosts wait for an exact newer cell version before reading
    /// pending status again. The token is an observation, never readiness.
    fn configuration_status(
        &self,
        principal: &str,
        authority: Authority,
        scope: String,
        name: String,
        _after: Option<crate::raft::version::CellVersionToken>,
    ) -> HandlerFuture<ConfigurationStatus> {
        let read = self.rpc(
            principal,
            authority.into(),
            scope,
            name,
            "__catalogStatus",
            json!([{}]),
        );
        Box::pin(async move {
            Ok(ConfigurationStatus {
                value: read.await?,
                after: None,
            })
        })
    }

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

fn matches_definition(current: &Value, definition: &Definition, digest: &str) -> bool {
    current["state"].as_str() == Some("active")
        && current["resource"].as_str() == Some(definition.resource.id.as_str())
        && current["accepted_revision"].as_str() == Some(definition.revision.to_string().as_str())
        && current["accepted_digest"].as_str() == Some(digest)
}

fn missing_configuration(error: &anyhow::Error) -> bool {
    use crate::application::consensus::CellDispatchError;
    use crate::raft::completion::RuntimeError;
    error
        .downcast_ref::<super::DeliveryError>()
        .is_some_and(|error| error.code == super::ErrorCode::NotInitialized)
        || matches!(
            error.downcast_ref::<RuntimeError>(),
            Some(RuntimeError::NotFound)
        )
        || matches!(
            error.downcast_ref::<CellDispatchError>(),
            Some(CellDispatchError::Runtime(RuntimeError::NotFound))
        )
        || error
            .downcast_ref::<std::sync::Arc<CellDispatchError>>()
            .is_some_and(|error| {
                matches!(
                    error.as_ref(),
                    CellDispatchError::Runtime(RuntimeError::NotFound)
                )
            })
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
        if host.configuration_known(script, &authority)? {
            match rpc(
                host,
                script,
                authority.clone(),
                event_stream::catalog_scope(&definition.resource)?,
                event_stream::catalog_name(&definition.resource)?,
                "__catalogStatus",
                json!([{}]),
            )
            .await
            {
                // This is a fresh, authorized, linearizable observation with
                // the target's independent minimum proof, never a historical
                // configuration receipt. Retained activation needs no mutation.
                Ok(current) if matches_definition(&current, definition, &digest) => continue,
                Ok(_) => (),
                Err(error) if missing_configuration(&error) => (),
                Err(error) => return Err(error),
            }
        }
        let preparation = host.prepare_broker(script, definition);
        let reconciliation = async {
            let configured = rpc(host, script, authority.clone(), event_stream::catalog_scope(&definition.resource)?, event_stream::catalog_name(&definition.resource)?,
                "__catalogConfigure", json!([{"operation":operation,"definition":base64::engine::general_purpose::STANDARD.encode(bytes)}])).await?;
            let pending = matches!(configured["state"].as_str(), Some("creating" | "updating"));
            anyhow::ensure!(
                configured["resource"].as_str() == Some(definition.resource.id.as_str())
                    && configured["operation"].as_str() == Some(operation.as_str())
                    && (configured["state"].as_str() == Some("active") || pending),
                "event resource configuration is pending: {}",
                configured["last_error"]
            );
            // A repeated operation returns history. Check the catalog's current
            // revision/digest and lifecycle before accepting deployment readiness.
            let mut after = None;
            loop {
                let observed = host
                    .configuration_status(
                        script,
                        authority.clone(),
                        event_stream::catalog_scope(&definition.resource)?,
                        event_stream::catalog_name(&definition.resource)?,
                        after.take(),
                    )
                    .await?;
                let current = observed.value;
                if matches_definition(&current, definition, &digest) {
                    break;
                }
                anyhow::ensure!(
                    pending
                        && matches!(current["state"].as_str(), Some("creating" | "updating"))
                        && current["resource"].as_str() == Some(definition.resource.id.as_str())
                        && current["operation"].as_str() == Some(operation.as_str()),
                    "event resource does not match the current deployment definition"
                );
                after = observed.after;
                // Shared owners wait without holding the previous SQL/archive
                // response. Fleet keeps its existing bounded status polling.
                // Neither path restarts the activation deadline or mutation.
                if after.is_none() {
                    crate::asyncrt::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
            Ok::<_, anyhow::Error>(())
        };
        // Only two bounded owner operations overlap for this declaration.
        // Neither an applied birth hint nor preparation grants readiness.
        let (prepared, reconciled) = futures_util::join!(preparation, reconciliation);
        prepared?;
        reconciled?;
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
        // Name resolution already checked the current catalog lifecycle and
        // every captured grant capability. An explicit UUID has no such proof.
        // Both paths still check the independently authorized partition below;
        // neither a name hint nor an active partition can replace catalog proof.
        if declaration.resource.id.is_some() {
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
            let resource: event_stream::Resource =
                serde_json::from_value(catalog["resource"].clone())?;
            anyhow::ensure!(
                catalog["state"].as_str() == Some("active") && resource == binding.resource,
                "catalog binding identity differs from the deployment"
            );
        }
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

#[cfg(test)]
#[path = "activation_tests.rs"]
mod tests;
