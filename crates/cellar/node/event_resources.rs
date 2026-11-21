// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Resource activation uses the ordinary owner route and proof gates. This
//! module does not mutate another cell's SQLite or publish a provisional bind.

use super::{AppHandle, Generation, RpcCallReq};
use crate::application::event_stream::{self, DeliveryError};
use anyhow::{Context, Result};
use serde_json::Value;
use std::sync::Arc;

struct Pending(Option<Arc<event_stream::readiness::Readiness>>);

async fn activation_exchange<T, F, Fut>(method: &str, mut exchange: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut backoff = std::time::Duration::from_millis(25);
    loop {
        let error = match exchange().await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        let pending_resolution = matches!(
            method,
            "__catalogResolve" | "__streamResolve" | event_stream::name_resolution::METHOD
        ) && error
            .downcast_ref::<super::RoutedRequestError>()
            .is_some_and(|failure| {
                matches!(
                    failure.0,
                    cellar_logic::RequestError::AcquireFailed
                        | cellar_logic::RequestError::ResolveFailed
                )
            });
        let transport = error
            .downcast_ref::<super::RemoteRpcTransportFailure>()
            .is_some();
        if !pending_resolution && !transport {
            return Err(error);
        }
        // Only read-only resolution polls temporary admission refusals.
        // Operation-idempotent activation keeps its tagged transport rule.
        // The enclosing timeout/cancellation owns every retry; ordinary
        // application RPC remains non-replayable.
        crate::asyncrt::sleep(backoff).await;
        backoff = backoff
            .saturating_mul(2)
            .min(std::time::Duration::from_millis(200));
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
    let deadline = crate::asyncrt::mono_ms()
        .checked_add(app.operation_deadline_ms)
        .context("event activation deadline overflow")?;
    let mut pending = Pending(Some(generation.event_readiness.clone()));
    let result = crate::asyncrt::timeout(
        std::time::Duration::from_millis(app.operation_deadline_ms),
        activate_inner(&app, generation, deadline),
    )
    .await
    .context("event resource activation deadline elapsed")
    .and_then(|result| result);
    pending.0.take();
    generation.event_readiness.finish(
        result
            .as_ref()
            .map(|_| ())
            .map_err(|error| format!("{error:#}")),
    );
    result
}

async fn rpc(
    app: &AppHandle,
    authority: crate::application::consensus::native_event_rpc::Authority,
    scope: String,
    name: String,
    method: &str,
    args: Value,
    deadline: u64,
) -> Result<Value> {
    let authority = match authority {
        crate::application::consensus::native_event_rpc::Authority::Event(value) => {
            crate::runtime::RpcAuthority::Event(value)
        }
        crate::application::consensus::native_event_rpc::Authority::Name(value) => {
            crate::runtime::RpcAuthority::Name(value)
        }
        crate::application::consensus::native_event_rpc::Authority::Catalog(value) => {
            crate::runtime::RpcAuthority::Catalog(value)
        }
    };
    let args =
        crate::application::encode_rpc(&cellar_native::clone::StructuredValue::from_json(args)?)?;
    let result = activation_exchange(method, || {
        let (reply, receiver) = tokio::sync::oneshot::channel();
        let request = RpcCallReq {
            reservation: None,
            parent_cancellation: None,
            signal: None,
            deadline_ms: Some(deadline),
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
            let failure = match &result {
                Ok(Err(error)) => Some(format!("{error:#}")),
                Err(error) => Some(error.to_string()),
                Ok(Ok(_)) => None,
            };
            tracing::debug!(
                event = "event_activation_exchange",
                phase = "completed",
                method,
                elapsed_ms = started.elapsed().as_millis(),
                succeeded = result.as_ref().is_ok_and(|value| value.is_ok()),
                failure,
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

async fn activate_inner(app: &AppHandle, generation: &Generation, deadline: u64) -> Result<()> {
    let shared = generation
        .native_applications
        .iter()
        .filter(|(script, _)| {
            generation
                .event_streams()
                .iter()
                .any(|(owner, _)| owner == *script)
                || generation
                    .event_bindings()
                    .iter()
                    .any(|binding| binding.principal == **script)
        })
        .filter(|(_, application)| {
            application
                .resolve_raft_class(crate::deploy::EVENT_STREAM_CLASS)
                .is_ok()
        })
        .map(|(script, _)| script.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let needs_fleet = generation
        .event_streams()
        .iter()
        .any(|(script, _)| !shared.contains(script))
        || generation
            .event_bindings()
            .iter()
            .any(|binding| !shared.contains(&binding.principal));
    if needs_fleet {
        let runtime = app
            .runtime
            .as_ref()
            .context("event activation requires a cell runtime")?;
        anyhow::ensure!(runtime.replication().is_some() && runtime.wake.is_some(),
            "event deployment requires replicated SQLite and durable wake discovery; configure a single-node bucket or fleet");
    }
    let owner = (!shared.is_empty())
        .then(|| {
            crate::asyncrt::services()
                .raft_lifecycle()
                .application_service()
        })
        .transpose()?;
    let pending = owner
        .as_ref()
        .map(|owner| owner.begin_event_sources())
        .transpose()?;
    let host = ActivationHost {
        app: app.clone(),
        shared,
        owner,
        deadline,
    };
    let activated = event_stream::activation::configure(
        &host,
        generation.event_streams(),
        generation.event_bindings(),
    )
    .await?;
    generation.event_readiness.pin(activated.clone())?;
    if let Some(pending) = pending {
        pending.complete(&activated)?;
    }
    Ok(())
}

struct ActivationHost {
    app: AppHandle,
    shared: std::collections::BTreeSet<String>,
    owner: Option<crate::application::consensus::RaftCells>,
    deadline: u64,
}

impl event_stream::activation::Host for ActivationHost {
    fn shares_catalog(&self, owner: &str, caller: &str) -> bool {
        // rpc selects exactly these two backends. A Raft catalog proof cannot
        // authorize adoption through an independently routed Fleet catalog.
        self.shared.contains(owner) == self.shared.contains(caller)
    }

    fn configuration_known(
        &self,
        principal: &str,
        authority: &event_stream::Authority,
    ) -> Result<bool> {
        if !self.shared.contains(principal) {
            return Ok(false);
        }
        self.owner
            .as_ref()
            .context("shared event activation owner")?
            .event_configuration_known(authority)
    }

    fn prepare_broker(
        &self,
        principal: &str,
        definition: &event_stream::Definition,
    ) -> crate::application::HandlerFuture<bool> {
        if self.shared.contains(principal) {
            let preparation = self
                .owner
                .as_ref()
                .expect("shared activation retains its owner")
                .prepare_event_broker(principal, definition, self.deadline);
            Box::pin(async move {
                preparation.await?;
                Ok(true)
            })
        } else {
            Box::pin(async { Ok(false) })
        }
    }

    fn configuration_status(
        &self,
        principal: &str,
        authority: event_stream::Authority,
        scope: String,
        name: String,
        after: Option<crate::raft::version::CellVersionToken>,
    ) -> crate::application::HandlerFuture<event_stream::activation::ConfigurationStatus> {
        if self.shared.contains(principal) {
            self.owner
                .as_ref()
                .expect("shared activation retains its owner")
                .event_configuration_status(authority, scope, name, after, self.deadline)
        } else {
            let read = self.rpc(
                principal,
                authority.into(),
                scope,
                name,
                "__catalogStatus",
                serde_json::json!([{}]),
            );
            Box::pin(async move {
                Ok(event_stream::activation::ConfigurationStatus {
                    value: read.await?,
                    after: None,
                })
            })
        }
    }

    fn rpc(
        &self,
        principal: &str,
        authority: crate::application::consensus::native_event_rpc::Authority,
        scope: String,
        name: String,
        method: &'static str,
        args: Value,
    ) -> crate::application::HandlerFuture<Value> {
        if self.shared.contains(principal) {
            let owner = self
                .owner
                .as_ref()
                .expect("shared activation retains its owner");
            owner.activate_event_rpc(authority, scope, name, method, args, self.deadline)
        } else {
            let app = self.app.clone();
            let deadline = self.deadline;
            Box::pin(async move { rpc(&app, authority, scope, name, method, args, deadline).await })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn resolution_readiness_retries_do_not_extend_to_mutating_methods() -> Result<()> {
        let domain = Arc::new(crate::asyncrt::ExecutionDomain::new(
            tokio::runtime::Handle::current(),
        ));
        for (method, retry) in [
            ("__catalogResolve", true),
            ("__streamResolve", true),
            (event_stream::name_resolution::METHOD, true),
            ("__catalogConfigure", false),
            ("__streamConfigure", false),
        ] {
            for (verdict, transient) in [
                (cellar_logic::RequestError::AcquireFailed, true),
                (cellar_logic::RequestError::ResolveFailed, true),
                (cellar_logic::RequestError::NodeFenced, false),
                (cellar_logic::RequestError::DurabilityUnproven, false),
            ] {
                let calls = AtomicUsize::new(0);
                let result = domain
                    .run(activation_exchange(method, || {
                        let attempt = calls.fetch_add(1, Ordering::SeqCst);
                        async move {
                            if attempt < 2 {
                                Err(super::super::event_failure::annotate(
                                    anyhow::Error::new(super::super::RoutedRequestError(verdict)),
                                    super::super::event_failure::route_code(verdict),
                                ))
                            } else {
                                Ok(7)
                            }
                        }
                    }))
                    .await?;
                if retry && transient {
                    assert_eq!(result?, 7);
                    assert_eq!(calls.load(Ordering::SeqCst), 3, "{method}");
                } else {
                    assert_eq!(
                        result
                            .unwrap_err()
                            .downcast_ref::<super::super::RoutedRequestError>()
                            .unwrap()
                            .0,
                        verdict
                    );
                    assert_eq!(calls.load(Ordering::SeqCst), 1, "{method}");
                }
            }
        }
        domain.shutdown().await;
        Ok(())
    }

    #[tokio::test]
    async fn activation_replays_only_tagged_transport_failure() -> Result<()> {
        let domain = Arc::new(crate::asyncrt::ExecutionDomain::new(
            tokio::runtime::Handle::current(),
        ));
        let calls = AtomicUsize::new(0);
        let result = domain
            .run(activation_exchange("__catalogConfigure", || {
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
        let result = activation_exchange("__streamResolve", || {
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
        let result = activation_exchange("__streamResolve", || {
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
        for (method, pending_resolution) in
            [("__catalogConfigure", false), ("__streamResolve", true)]
        {
            let calls = AtomicUsize::new(0);
            let result = domain
                .run(crate::asyncrt::timeout(
                    std::time::Duration::from_millis(40),
                    activation_exchange(method, || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        async {
                            let error = if pending_resolution {
                                anyhow::Error::new(super::super::RoutedRequestError(
                                    cellar_logic::RequestError::AcquireFailed,
                                ))
                            } else {
                                anyhow::anyhow!("peer closed")
                                    .context(super::super::RemoteRpcTransportFailure)
                            };
                            Err::<(), _>(error)
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
        }
        domain.shutdown().await;
        Ok(())
    }
}
