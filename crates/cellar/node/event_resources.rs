//! Resource activation uses the ordinary owner route and proof gates. This
//! module does not mutate another cell's SQLite or publish a provisional bind.

use super::{AppHandle, Generation, RpcCallReq};
use crate::application::event_stream::{self, authority::Authority, DeliveryError};
use anyhow::{Context, Result};
use base64::Engine;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;

struct Pending(Option<Arc<event_stream::readiness::Readiness>>);

async fn activation_exchange<T, F, Fut>(mut exchange: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut backoff = std::time::Duration::from_millis(25);
    loop {
        match exchange().await {
            Err(error)
                if error
                    .downcast_ref::<super::RemoteRpcTransportFailure>()
                    .is_some() =>
            {
                // These activation methods are operation-idempotent or read-only.
                // The enclosing activation timeout/cancellation still owns this
                // exchange. Ordinary application RPC remains non-replayable.
                crate::asyncrt::sleep(backoff).await;
                backoff = backoff
                    .saturating_mul(2)
                    .min(std::time::Duration::from_millis(200));
            }
            result => return result,
        }
    }
}

impl Drop for Pending {
    fn drop(&mut self) {
        // A cancelled activation cannot leave readiness observers pending
        // indefinitely. The owner domain still joins its begun RPC work.
        if let Some(readiness) = &self.0 {
            readiness.finish(Err("event resource activation cancelled".into()));
        }
    }
}

pub(super) async fn activate(app: AppHandle, generation: &Generation) -> Result<()> {
    generation.begin_event_activation();
    if generation.event_streams().is_empty() && generation.event_bindings().is_empty() {
        return Ok(());
    }
    let mut pending = Pending(Some(generation.event_readiness.clone()));
    let result = crate::asyncrt::timeout(
        std::time::Duration::from_millis(app.operation_deadline_ms),
        activate_inner(&app, generation),
    )
    .await
    .context("event resource activation deadline elapsed")
    .and_then(|result| result);
    pending.0.take();
    generation.event_readiness.finish(
        result
            .as_ref()
            .map(|_| ())
            .map_err(|error| error.to_string()),
    );
    result
}

async fn rpc(
    app: &AppHandle,
    authority: impl Into<crate::runtime::RpcAuthority>,
    scope: String,
    name: String,
    method: &str,
    args: Value,
) -> Result<Value> {
    let authority = authority.into();
    let args =
        crate::application::encode_rpc(&cellar_native::clone::StructuredValue::from_json(args)?)?;
    let result = activation_exchange(|| {
        let (reply, receiver) = tokio::sync::oneshot::channel();
        let request = RpcCallReq {
            authority: Some(authority.clone()),
            // The operation/body is unchanged. A fresh dispatch identity avoids
            // an abort tombstone from the prior ambiguous call cancelling replay.
            request_id: Some(crate::engine_api::next_request_id()),
            cancel: None,
            scope: scope.clone(),
            name: Some(name.clone()),
            method: method.into(),
            args: args.clone(),
            reply,
        };
        async move {
            let started = std::time::Instant::now();
            tracing::debug!(
                event = "event_activation_exchange",
                phase = "started",
                method,
                "activation exchange started"
            );
            // Dispatch retains the routing/execution/deadline state machine.
            // Keep it off each enclosing activation/startup future's inline
            // state so unoptimized polling fits a normal executor stack.
            let dispatch = Box::pin(super::dispatch_rpc_call(app.clone(), request));
            let (_, result) = futures_util::future::join(dispatch, receiver).await;
            tracing::debug!(
                event = "event_activation_exchange",
                phase = "completed",
                method,
                elapsed_ms = started.elapsed().as_millis(),
                succeeded = result.as_ref().is_ok_and(|value| value.is_ok()),
                "activation exchange completed"
            );
            result.context("resource activation response channel closed")?
        }
    })
    .await
    .with_context(|| format!("event activation {method}"))?;
    let value = crate::application::decode_rpc_result(&result)?.to_json()?;
    if let Some(error) = value.get("error") {
        return Err(serde_json::from_value::<DeliveryError>(error.clone())?.into());
    }
    value
        .get("ok")
        .cloned()
        .context("invalid resource activation response")
}

async fn activate_inner(app: &AppHandle, generation: &Generation) -> Result<()> {
    // Event admission needs both data proof and recoverable wake discovery.
    // A local-only runtime has no inventory that can rediscover an untouched
    // source after restart, and cannot hand off an armed internal deadline.
    // Reject that host before configuration rather than acknowledge obligations
    // that only happen to progress while the current process remains resident.
    let runtime = app
        .runtime
        .as_ref()
        .context("event activation requires a cell runtime")?;
    anyhow::ensure!(runtime.replication().is_some() && runtime.wake.is_some(),
        "event deployment requires replicated SQLite and durable wake discovery; configure a single-node bucket or fleet");
    for (script, definition) in generation.event_streams() {
        let bytes = event_stream::encoding::json(definition, event_stream::MAX_DEFINITION_BYTES)?;
        let digest = hex::encode(Sha256::digest(&bytes));
        let mut identity = Sha256::new();
        identity.update(b"cellar-event-deployment-operation-v1\0");
        identity.update(u32::try_from(script.len())?.to_be_bytes());
        identity.update(script.as_bytes());
        identity.update(&bytes);
        let operation = hex::encode(&identity.finalize()[..16]);
        let authority = Authority::deployment(script, definition)?;
        let configured = rpc(app, authority.clone(), event_stream::catalog_scope(&definition.resource)?, event_stream::catalog_name(&definition.resource)?,
            "__catalogConfigure", json!([{"operation":operation,"definition":base64::engine::general_purpose::STANDARD.encode(bytes)}])).await?;
        anyhow::ensure!(
            configured["state"].as_str() == Some("active"),
            "event resource configuration is pending: {}",
            configured["last_error"]
        );
        // A repeated operation returns history. Check the catalog's current
        // revision/digest and lifecycle before accepting deployment readiness.
        let current = rpc(
            app,
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
    for declaration in generation.event_bindings() {
        let binding = if declaration.resource.id.is_some() {
            declaration.explicit()?
        } else {
            let authority = event_stream::name_resolution::NameAuthority::new(declaration)?;
            let namespace = authority.namespace();
            let resolved = rpc(app, authority, namespace.scope()?, namespace.name()?,
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
            app,
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
            app,
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
    generation.event_readiness.pin(activated)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn activation_replays_only_tagged_transport_failure() -> Result<()> {
        let domain = Arc::new(crate::asyncrt::ExecutionDomain::new(
            tokio::runtime::Handle::current(),
        ));
        let calls = AtomicUsize::new(0);
        let result = domain
            .run(activation_exchange(|| {
                let attempt = calls.fetch_add(1, Ordering::SeqCst);
                async move {
                    if attempt == 0 {
                        let failure = super::super::remote_rpc_failure(anyhow::Error::new(
                            std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "peer closed"),
                        ));
                        assert_eq!(
                            super::super::classify_remote_attempt(&failure),
                            cellar_logic::routing::Attempt::Ambiguous,
                            "tagging an activation failure must not authorize ordinary RPC replay"
                        );
                        Err(failure)
                    } else {
                        Ok(7)
                    }
                }
            }))
            .await?;
        assert_eq!(result?, 7);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        domain.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn permanent_activation_failure_is_not_replayed() -> Result<()> {
        let calls = AtomicUsize::new(0);
        let result = activation_exchange(|| {
            calls.fetch_add(1, Ordering::SeqCst);
            async {
                Err::<(), _>(
                    DeliveryError::new(event_stream::ErrorCode::Forbidden, "denied").into(),
                )
            }
        })
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            result
                .unwrap_err()
                .downcast_ref::<DeliveryError>()
                .unwrap()
                .code,
            event_stream::ErrorCode::Forbidden
        );
        Ok(())
    }

    #[tokio::test]
    async fn protocol_refusal_is_not_tagged_for_activation_replay() -> Result<()> {
        let calls = AtomicUsize::new(0);
        let result = activation_exchange(|| {
            calls.fetch_add(1, Ordering::SeqCst);
            async {
                Err::<(), _>(super::super::remote_rpc_failure(anyhow::anyhow!(
                    "incompatible protocol"
                )))
            }
        })
        .await;
        let error = result.unwrap_err();
        assert!(error
            .downcast_ref::<super::super::RemoteRpcTransportFailure>()
            .is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn activation_retry_retains_its_enclosing_deadline() -> Result<()> {
        let domain = Arc::new(crate::asyncrt::ExecutionDomain::new(
            tokio::runtime::Handle::current(),
        ));
        let calls = AtomicUsize::new(0);
        let result = domain
            .run(crate::asyncrt::timeout(
                std::time::Duration::from_millis(40),
                activation_exchange(|| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async {
                        Err::<(), _>(
                            anyhow::anyhow!("peer closed")
                                .context(super::super::RemoteRpcTransportFailure),
                        )
                    }
                }),
            ))
            .await?;
        assert!(result.is_err(), "activation extended its caller deadline");
        let stopped = calls.load(Ordering::SeqCst);
        assert!(stopped > 0);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            stopped,
            "timed-out activation retained a retry task"
        );
        domain.shutdown().await;
        Ok(())
    }
}
