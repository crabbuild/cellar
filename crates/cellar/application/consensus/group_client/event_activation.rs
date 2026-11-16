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

    fn prepare_broker(
        &self,
        principal: &str,
        definition: &event_stream::Definition,
    ) -> HandlerFuture<()> {
        self.owner
            .prepare_event_broker(principal, definition, self.deadline)
    }

    fn configuration_status(
        &self,
        _principal: &str,
        authority: event_stream::Authority,
        scope: String,
        name: String,
        after: Option<crate::raft::version::CellVersionToken>,
    ) -> HandlerFuture<event_stream::activation::ConfigurationStatus> {
        self.owner
            .event_configuration_status(authority, scope, name, after, self.deadline)
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

    /// Create only a declared broker birth, never update or invoke its code.
    /// Existing applied locators may skip preparation; catalog reconciliation
    /// and fresh independent proofs still decide serving readiness.
    pub(crate) fn prepare_event_broker(
        &self,
        principal: &str,
        definition: &event_stream::Definition,
        deadline: u64,
    ) -> HandlerFuture<()> {
        use crate::raft::completion::RuntimeError;
        use cellar_logic::consensus::{IncarnationId, ProposalId};
        use rand::RngCore as _;
        let prepared = (|| -> Result<_> {
            self.check_open()?;
            let caller = event_stream::Authority::deployment(principal, definition)?;
            let scope = event_stream::cell_scope(&definition.resource)?;
            let route = self.handle.core.host.event_ingress_route(
                &scope,
                "__streamConfigure",
                &Authority::Event(caller),
                &self.handle.core.log,
            )?;
            if route.history_hint.is_some() {
                return Ok(None);
            }
            let remaining = deadline
                .checked_sub(self.handle.domain.mono_ms()?)
                .filter(|n| *n != 0)
                .ok_or(RuntimeError::DeadlineExceeded)?;
            // Only routing/creation metadata is retained here. The registered
            // initializer and routed enrollment own their separate byte banks.
            let (_, memory) = self.metadata_admission(Duration::from_millis(remaining), 8192)?;
            let mut creation = [0; 16];
            let mut incarnation = [0; 16];
            let mut rng = crate::asyncrt::rng("declared-broker-preparation");
            rng.fill_bytes(&mut creation);
            rng.fill_bytes(&mut incarnation);
            creation[0] |= 1;
            incarnation[0] |= 1;
            let candidate = CellHistory {
                cluster: route.cluster,
                namespace: route.namespace,
                cell: route.cell,
                incarnation: IncarnationId::new(incarnation)?,
            };
            Ok(Some((
                candidate,
                ProposalId::new(creation)?,
                route.schema,
                route.durability,
                memory,
            )))
        })();
        let owner = self.internal_observer();
        Box::pin(async move {
            let Some((candidate, creation, schema, durability, _memory)) = prepared? else {
                return Ok(());
            };
            // A name observation chooses only whether creation is needed. It
            // cannot adopt configuration, grant a binding, or acknowledge data.
            if super::sql_ingress::ready_name(&owner, &candidate, deadline)
                .await?
                .is_some()
            {
                return Ok(());
            }
            let remaining = deadline
                .checked_sub(owner.handle.domain.mono_ms()?)
                .filter(|n| *n != 0)
                .ok_or(RuntimeError::DeadlineExceeded)?;
            let request = CellEnrollmentRequest {
                creation,
                durability,
            };
            match owner
                .cell(candidate.clone())?
                .enroll(request, Duration::from_millis(remaining))
                .await
            {
                Ok(proof) => {
                    anyhow::ensure!(
                        proof.record().cell() == &candidate
                            && proof.record().creation() == creation
                            && proof.record().schema() == schema
                            && proof.durability() >= durability,
                        super::super::CommandFailure::CorruptState
                    );
                    Ok(())
                }
                Err(error) => loop {
                    owner.check_open()?;
                    // A concurrent trusted owner can win the same name. Join
                    // its current locator without retransmitting an unknown
                    // creation or changing this attempt's original identity.
                    match super::sql_ingress::ready_name(&owner, &candidate, deadline).await {
                        Ok(Some(_)) => return Ok(()),
                        Ok(None) => (),
                        Err(_) => return Err(error.into()),
                    }
                    let now = owner.handle.domain.mono_ms()?;
                    let next = now
                        .saturating_add(owner.handle.core.controller.limits().retry_interval_ms)
                        .min(deadline);
                    owner.handle.domain.sleep_until(next)?.await;
                },
            }
        })
    }

    pub(crate) fn event_configuration_status(
        &self,
        authority: event_stream::Authority,
        scope: String,
        name: String,
        after: Option<crate::raft::version::CellVersionToken>,
        deadline: u64,
    ) -> HandlerFuture<event_stream::activation::ConfigurationStatus> {
        let prepared = (|| -> Result<_> {
            self.check_open()?;
            anyhow::ensure!(
                self.handle.domain.mono_ms()? < deadline,
                crate::raft::completion::RuntimeError::DeadlineExceeded
            );
            let args = crate::application::encode_rpc(
                &cellar_native::clone::StructuredValue::from_json(serde_json::json!([{}]))?,
            )?;
            let admitted = crate::asyncrt::services()
                .admit_outbound_rpc(&scope, Some(&name), "__catalogStatus", &args)
                .ok_or(super::super::CommandFailure::Overloaded)?;
            Ok(self.event_read_observed(
                admitted,
                crate::host_rpc::CallScope::default().with_deadline(deadline),
                deadline,
                Authority::Event(authority),
                after,
            ))
        })();
        Box::pin(async move {
            let (response, token) = prepared?.await?;
            // Consume and release the prior SQL/archive output before parking
            // another change read. The token cannot grant readiness or admission.
            let value = activation_value(&response)?;
            Ok(event_stream::activation::ConfigurationStatus {
                value,
                after: Some(token),
            })
        })
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
            let result = prepared?.await?;
            activation_value(&result)
        })
    }
}

fn activation_value(result: &crate::engine_api::RpcData) -> Result<Value> {
    let value = crate::application::decode_rpc_result(result)?.to_json()?;
    if let Some(error) = value.get("error") {
        return Err(serde_json::from_value::<event_stream::DeliveryError>(error.clone())?.into());
    }
    value
        .get("ok")
        .cloned()
        .context("invalid resource activation response")
}
