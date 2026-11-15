// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Activation pins shared-cell declarations only after current target proofs.

use super::*;
use crate::application::{consensus::native_event_rpc::Authority, event_stream, HandlerFuture};
use anyhow::{Context as _, Result};
use serde_json::Value;

pub(crate) struct EventSources {
    pending: Vec<(
        Arc<crate::application::storage::EventSourceConfiguration>,
        u64,
    )>,
}

impl EventSources {
    pub(crate) fn complete(mut self, resolved: &[event_stream::Binding]) -> Result<()> {
        for (source, epoch) in &self.pending {
            source.pin_activation(*epoch, resolved)?;
        }
        for (source, epoch) in &self.pending {
            anyhow::ensure!(
                source.finish_activation(*epoch, Ok(())),
                "event activation was superseded"
            );
        }
        self.pending.clear();
        Ok(())
    }
}

impl Drop for EventSources {
    fn drop(&mut self) {
        for (source, epoch) in &self.pending {
            source.finish_activation(
                *epoch,
                Err("event resource activation cancelled or refused".into()),
            );
        }
    }
}

struct Pending(Option<Arc<event_stream::readiness::Readiness>>);

impl Drop for Pending {
    fn drop(&mut self) {
        if let Some(readiness) = self.0.take() {
            readiness.finish(Err("event resource activation cancelled or refused".into()));
        }
    }
}

struct ActivationHost {
    owner: RaftCells,
    deadline: u64,
}

impl event_stream::activation::Host for ActivationHost {
    fn configuration_known(
        &self,
        _principal: &str,
        authority: &event_stream::Authority,
    ) -> Result<bool> {
        self.owner.event_configuration_known(authority)
    }

    fn rpc(
        &self,
        _principal: &str,
        authority: Authority,
        scope: String,
        name: String,
        method: &'static str,
        args: Value,
    ) -> HandlerFuture<Value> {
        self.owner
            .activate_event_rpc(authority, scope, name, method, args, self.deadline)
    }
}

impl RaftCells {
    pub(crate) fn event_configuration_known(
        &self,
        authority: &event_stream::Authority,
    ) -> Result<bool> {
        self.check_open()?;
        let route = self.handle.core.host.event_query_route(
            &event_stream::catalog_scope(&authority.resource)?,
            "__catalogStatus",
            &Authority::Event(authority.clone()),
            &self.handle.core.log,
        )?;
        // A resumed owner starts with an empty range cache, even when its
        // retained groups contain this catalog. Try current immutable status
        // before allocating another configuration command. Resume selects only
        // the optimization; it never supplies identity, authority or proof.
        Ok(matches!(
            self.handle.core.log.limits().mode,
            crate::raft::options::LogOpenMode::Resume
        ) || route.history_hint.is_some())
    }

    pub(crate) fn begin_event_sources(&self) -> Result<EventSources> {
        self.check_open()?;
        let mut pending = EventSources {
            pending: Vec::new(),
        };
        for source in self.handle.core.host.event_sources()? {
            let epoch = source.begin_activation()?;
            pending.pending.push((source, epoch));
        }
        Ok(pending)
    }

    /// Reconcile captured EventStream definitions and pin named bindings before
    /// admitting SDK work in an embedded shared-group generation. Node hosts do
    /// this during startup. All participating applications must select Raft;
    /// target namespaces must already be installed on this joined owner.
    pub async fn activate_event_generation(
        &self,
        generation: &crate::generation::Generation,
        timeout: Duration,
    ) -> Result<()> {
        self.check_open()?;
        let shared = |script: &str| {
            generation
                .native_applications
                .get(script)
                .is_some_and(|app| app.uses_raft_primitives())
        };
        anyhow::ensure!(
            generation
                .event_streams()
                .iter()
                .all(|(script, _)| shared(script))
                && generation
                    .event_bindings()
                    .iter()
                    .all(|binding| shared(&binding.principal)),
            super::super::CommandFailure::UnsupportedCapability
        );
        let deadline = self
            .handle
            .domain
            .mono_ms()?
            .checked_add(u64::try_from(timeout.as_millis())?)
            .context("event activation deadline overflow")?;
        generation.begin_event_activation();
        let mut pending = Pending(Some(generation.event_readiness.clone()));
        let sources = self.begin_event_sources()?;
        let host = ActivationHost {
            owner: self.internal_observer(),
            deadline,
        };
        let result = crate::asyncrt::timeout(timeout, async {
            let bindings = event_stream::activation::configure(
                &host,
                generation.event_streams(),
                generation.event_bindings(),
            )
            .await?;
            generation.event_readiness.pin(bindings.clone())?;
            sources.complete(&bindings)
        })
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

    pub(crate) fn activate_event_rpc(
        &self,
        authority: Authority,
        scope: String,
        name: String,
        method: &'static str,
        args: Value,
        deadline: u64,
    ) -> HandlerFuture<Value> {
        let domain = self.handle.domain.clone();
        let started = domain.mono_ms().unwrap_or(deadline);
        tracing::debug!(target: "cellar::raft::sql_ingress", method,
            phase="activation-start", remaining_ms=deadline.saturating_sub(started),
            "[DEBUG-cg17-activation-227]");
        let prepared = (|| -> Result<_> {
            self.check_open()?;
            anyhow::ensure!(
                self.handle.domain.mono_ms()? < deadline,
                crate::raft::completion::RuntimeError::DeadlineExceeded
            );
            let args = crate::application::encode_rpc(
                &cellar_native::clone::StructuredValue::from_json(args)?,
            )?;
            let services = crate::asyncrt::services();
            let admitted = services
                .admit_outbound_rpc(&scope, Some(&name), method, &args)
                .ok_or(super::super::CommandFailure::Overloaded)?;
            let call_scope = crate::host_rpc::CallScope::default().with_deadline(deadline);
            Ok(if super::super::native_event_query::is_metadata(method) {
                self.event_read(admitted, call_scope, deadline, authority)
            } else {
                self.event_ingress(admitted, call_scope, deadline, authority)
            })
        })();
        Box::pin(async move {
            let result = prepared?.await;
            let now = domain.mono_ms().unwrap_or(deadline);
            tracing::debug!(target: "cellar::raft::sql_ingress", method,
                phase="activation-response", elapsed_ms=now.saturating_sub(started),
                remaining_ms=deadline.saturating_sub(now), success=result.is_ok(),
                "[DEBUG-cg17-activation-227]");
            let result = result?;
            let value = crate::application::decode_rpc_result(&result)?.to_json()?;
            if let Some(error) = value.get("error") {
                return Err(
                    serde_json::from_value::<event_stream::DeliveryError>(error.clone())?.into(),
                );
            }
            value
                .get("ok")
                .cloned()
                .context("invalid resource activation response")
        })
    }
}
