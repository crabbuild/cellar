// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A stateless request has one target command identity and no source transaction.

use super::super::sql_ingress_identity::{self, Observation};
use super::*;
use crate::application::{cell_bindings::CallError, consensus::native_rpc, HandlerFuture};
use crate::engine_api::RpcData;
use crate::raft::completion::RuntimeError;
use cellar_logic::consensus::{IncarnationId, ProposalId};
use rand::RngCore as _;

#[derive(Clone, Copy)]
enum Origin {
    Binding,
    Native { configuration: [u8; 32] },
    Operator,
    QueueOperator,
    KvOperator,
    KvPreparation { digest: [u8; 32], length: u64 },
}

impl RaftCells {
    pub(crate) fn native_ingress(
        &self,
        admitted: crate::outbound_rpc::Admitted,
        call_scope: crate::host_rpc::CallScope,
        deadline: u64,
        configuration: [u8; 32],
    ) -> HandlerFuture<RpcData> {
        self.submit_sql(
            admitted,
            call_scope,
            deadline,
            Origin::Native { configuration },
        )
    }

    /// Stateless ingress may durably enroll an unknown target, then construct
    /// its pending data locator without consuming a command-stream sequence.
    /// Stateful callers retain their source-journaled enrollment path.
    pub(crate) fn prepare_kv_ingress(
        &self,
        admitted: crate::outbound_rpc::Admitted,
        call_scope: crate::host_rpc::CallScope,
        deadline: u64,
        digest: [u8; 32],
        length: u64,
    ) -> HandlerFuture<RpcData> {
        match crate::storage::capture_raft_dispatch() {
            Ok(None) => (),
            Ok(Some(_)) => {
                return Box::pin(async {
                    Err(super::super::CommandFailure::UnsupportedCapability.into())
                })
            }
            Err(error) => return Box::pin(async move { Err(error) }),
        }
        self.submit_sql(
            admitted,
            call_scope,
            deadline,
            Origin::KvPreparation { digest, length },
        )
    }

    pub(crate) fn prepare_kv_payload(
        &self,
        scope: &str,
        digest: [u8; 32],
        length: u64,
    ) -> anyhow::Result<
        Option<(
            CellHistory,
            crate::raft::cell_groups::CellGroupPayloadReference,
        )>,
    > {
        self.check_open()?;
        Ok(self.handle.core.host.prepare_ingress_kv_payload(
            scope,
            digest,
            length,
            &self.handle.core.log,
        )?)
    }

    pub(crate) fn kv_wire_input_limit(&self, scope: &str, method: &str) -> anyhow::Result<usize> {
        self.check_open()?;
        anyhow::ensure!(
            scope
                .split_once(':')
                .is_some_and(|(class, _)| class == crate::deploy::KV_CLASS),
            super::super::CommandFailure::UnsupportedCapability
        );
        Ok(self
            .handle
            .core
            .host
            .sql_ingress_route(scope, method, &self.handle.core.log)?
            .max_input_bytes)
    }

    pub(crate) fn kv_wire_store(
        &self,
        history: &CellHistory,
    ) -> anyhow::Result<crate::raft::cell_groups::CellGroupPayloadStore> {
        self.check_open()?;
        Ok(self
            .handle
            .core
            .host
            .kv_ingress_payload_store(history, &self.handle.core.log)?)
    }

    pub(crate) fn sql_ingress(
        &self,
        admitted: crate::outbound_rpc::Admitted,
        call_scope: crate::host_rpc::CallScope,
        deadline: u64,
    ) -> HandlerFuture<RpcData> {
        self.submit_sql(admitted, call_scope, deadline, Origin::Binding)
    }

    /// Called only by the node's peer-authenticated operator entrance.
    pub(crate) fn sql_operator(
        &self,
        admitted: crate::outbound_rpc::Admitted,
        call_scope: crate::host_rpc::CallScope,
        deadline: u64,
    ) -> HandlerFuture<RpcData> {
        self.submit_sql(admitted, call_scope, deadline, Origin::Operator)
    }

    pub(crate) fn queue_operator(
        &self,
        admitted: crate::outbound_rpc::Admitted,
        call_scope: crate::host_rpc::CallScope,
        deadline: u64,
    ) -> HandlerFuture<RpcData> {
        self.submit_sql(admitted, call_scope, deadline, Origin::QueueOperator)
    }

    pub(crate) fn kv_operator(
        &self,
        admitted: crate::outbound_rpc::Admitted,
        call_scope: crate::host_rpc::CallScope,
        deadline: u64,
    ) -> HandlerFuture<RpcData> {
        self.submit_sql(admitted, call_scope, deadline, Origin::KvOperator)
    }

    fn submit_sql(
        &self,
        admitted: crate::outbound_rpc::Admitted,
        call_scope: crate::host_rpc::CallScope,
        deadline: u64,
        origin: Origin,
    ) -> HandlerFuture<RpcData> {
        let crate::outbound_rpc::Admitted {
            reservation,
            scope,
            name,
            method,
            args,
        } = admitted;
        let prepared = (|| -> anyhow::Result<_> {
            self.check_open()?;
            let route = match origin {
                Origin::KvPreparation { .. } => {
                    anyhow::ensure!(
                        method == crate::application::kv::raft_wire::PREPARE,
                        super::super::CommandFailure::UnsupportedCapability
                    );
                    self.kv_wire_input_limit(&scope, &method)?;
                    native_rpc::validate_identity(
                        "cellar-kv-service",
                        crate::deploy::KV_CLASS,
                        &scope,
                        name.as_deref(),
                    )?;
                    self.handle.core.host.sql_ingress_route(
                        &scope,
                        &method,
                        &self.handle.core.log,
                    )?
                }
                Origin::Binding => self.handle.core.host.sql_ingress_route(
                    &scope,
                    &method,
                    &self.handle.core.log,
                )?,
                Origin::Native { configuration } => self.handle.core.host.native_ingress_route(
                    configuration,
                    &scope,
                    &method,
                    &self.handle.core.log,
                )?,
                Origin::QueueOperator => {
                    anyhow::ensure!(
                        method == crate::application::queue::OPERATOR,
                        super::super::CommandFailure::UnsupportedCapability
                    );
                    self.handle
                        .core
                        .host
                        .queue_operator_route(&scope, &self.handle.core.log)?
                }
                Origin::KvOperator => {
                    anyhow::ensure!(
                        method == crate::application::kv::OPERATOR,
                        super::super::CommandFailure::UnsupportedCapability
                    );
                    self.handle
                        .core
                        .host
                        .kv_operator_route(&scope, &self.handle.core.log)?
                }
                Origin::Operator => {
                    anyhow::ensure!(
                        method == crate::application::sql::operator::METHOD,
                        super::super::CommandFailure::UnsupportedCapability
                    );
                    self.handle
                        .core
                        .host
                        .sql_operator_route(&scope, &self.handle.core.log)?
                }
            };
            if route.history_hint.is_some() {
                self.handle.core.log.performance().increment(
                    crate::raft::performance::Counter::CellBindingAppliedHistoryHints,
                    1,
                );
            }
            let bytes = route
                .max_input_bytes
                .checked_mul(2)
                .and_then(|n| {
                    route
                        .max_result_bytes
                        .checked_mul(4)
                        .and_then(|m| n.checked_add(m))
                })
                .and_then(|n| n.checked_add(8192))
                .ok_or(super::super::CommandFailure::Overloaded)?;
            let now = self.handle.domain.mono_ms()?;
            let timeout = Duration::from_millis(
                deadline
                    .checked_sub(now)
                    .filter(|n| *n != 0)
                    .ok_or(RuntimeError::DeadlineExceeded)?,
            );
            let (_, memory) = self.metadata_admission(timeout, bytes)?;
            let mut rng = crate::asyncrt::rng("stateless-sql-command");
            let mut nonce = [0; 16];
            let mut creation = [0; 16];
            let mut incarnation = [0; 16];
            rng.fill_bytes(&mut nonce);
            rng.fill_bytes(&mut creation);
            rng.fill_bytes(&mut incarnation);
            creation[0] |= 1;
            incarnation[0] |= 1;
            let input = native_rpc::encode_ingress_request(
                name.as_deref(),
                &args,
                nonce,
                route.max_input_bytes,
            )?;
            let creation = ProposalId::new(creation)?;
            let candidate = CellHistory {
                cluster: route.cluster,
                namespace: route.namespace,
                cell: route.cell,
                incarnation: IncarnationId::new(incarnation)?,
            };
            let retry_until_ms = u64::try_from(crate::asyncrt::wall_ms())?
                .checked_add(route.max_retry_window_ms.min(60_000))
                .ok_or(RuntimeError::DeadlineExceeded)?;
            Ok((
                route.contract,
                route.durability,
                route.history_hint,
                candidate,
                creation,
                retry_until_ms,
                input,
                deadline,
                memory,
            ))
        })();
        tracing::debug!(target: "cellar::raft::sql_ingress", phase = "prepared", success = prepared.is_ok());
        let owner = self.internal_observer();
        Box::pin(async move {
            let (
                contract,
                durability,
                history_hint,
                candidate,
                creation,
                retry_until_ms,
                input,
                deadline,
                _memory,
            ) = prepared?;
            let selected = Mutex::new(None);
            let command = Mutex::new(None);
            let submitted = std::sync::atomic::AtomicBool::new(false);
            let started = owner.handle.domain.mono_ms()?;
            let remaining = || -> anyhow::Result<Duration> {
                let now = owner.handle.domain.mono_ms()?;
                Ok(Duration::from_millis(
                    deadline
                        .checked_sub(now)
                        .filter(|n| *n != 0)
                        .ok_or(RuntimeError::DeadlineExceeded)?,
                ))
            };
            let operation = async {
                // A local applied history is only a locator. The sequence query
                // and command still require the exact target's fresh quorum.
                let (history, newly_enrolled) = if let Some(history) = history_hint {
                    (history, false)
                } else {
                    match ready_name(&owner, &candidate, deadline).await? {
                        Some(history) => (history, false),
                        None => {
                            match owner
                                .cell(candidate.clone())?
                                .enroll(
                                    CellEnrollmentRequest {
                                        creation,
                                        durability,
                                    },
                                    remaining()?,
                                )
                                .await
                            {
                                Ok(_) => (candidate.clone(), true),
                                Err(error) => loop {
                                    match ready_name(&owner, &candidate, deadline).await {
                                        Ok(Some(history)) => break (history, false),
                                        Ok(None) => {
                                            let delay = (remaining()?.as_millis() as u64).min(
                                                owner
                                                    .handle
                                                    .core
                                                    .controller
                                                    .limits()
                                                    .retry_interval_ms,
                                            );
                                            let now = owner.handle.domain.mono_ms()?;
                                            owner
                                                .handle
                                                .domain
                                                .sleep_until(
                                                    now.saturating_add(delay).min(deadline),
                                                )?
                                                .await;
                                        }
                                        Err(_) => return Err(error.into()),
                                    }
                                },
                            }
                        }
                    }
                };
                *selected.lock().unwrap() = Some(history.clone());
                tracing::debug!(target: "cellar::raft::sql_ingress", %method,
                    phase="history", newly_enrolled, elapsed_ms=owner.handle.domain.mono_ms()?.saturating_sub(started));
                if let Origin::KvPreparation { digest, length } = origin {
                    let store = owner.kv_wire_store(&history)?;
                    let reference = store.prepare_reference(&history, digest, length)?;
                    let encoded = crate::application::kv::raft_wire::prepared(&history, reference)?;
                    let response = crate::application::encode_rpc(
                        &cellar_native::clone::StructuredValue::from_json(serde_json::json!(
                            encoded
                        ))?,
                    )?;
                    if let Some(limit) = reservation.reply_limit() {
                        anyhow::ensure!(
                            matches!(&response, RpcData::Structured(bytes) if bytes.len() <= limit),
                            crate::application::rpc::Failure::new(
                                crate::application::rpc::Code::InvalidResponse
                            )
                        );
                    }
                    remaining()?;
                    owner.handle.core.log.performance().increment(
                        crate::raft::performance::Counter::CellKvResolvedPayloadPreparations,
                        1,
                    );
                    return Ok(response);
                }
                let cell = owner.cell(history.clone())?;
                // The runtime-owned initializer creates an empty stream. This
                // is a hint at the enrollment cut, never an exclusive sequence
                // reservation: another caller can still consume it first.
                let next = if newly_enrolled {
                    1
                } else {
                    observe(&cell, 0, remaining()?).await?.next
                };
                *command.lock().unwrap() =
                    Some(CommandId::new(sql_ingress_identity::client(), next)?);
                tracing::debug!(target: "cellar::raft::sql_ingress", %method,
                    phase="sequence", next, elapsed_ms=owner.handle.domain.mono_ms()?.saturating_sub(started));
                let fingerprint = super::super::command_request_digest(&input);
                let mut attempt = 0u64;
                let receipt = loop {
                    let id = command
                        .lock()
                        .unwrap()
                        .ok_or(super::super::CommandFailure::CorruptState)?;
                    let mut options = CommandOptions::new(id, retry_until_ms);
                    options.durability = durability;
                    options.timeout = remaining()?;
                    let dispatch_budget_ms = options.timeout.as_millis();
                    attempt = attempt
                        .checked_add(1)
                        .ok_or(super::super::CommandFailure::Overloaded)?;
                    submitted.store(true, Ordering::Release);
                    let result = cell
                        .command_retaining(
                            &contract,
                            input.clone(),
                            options,
                            Some(reservation.clone()),
                        )
                        .await;
                    tracing::debug!(target: "cellar::raft::sql_ingress", %method,
                        phase="command", attempt, sequence=id.sequence(), success=result.is_ok(),
                        elapsed_ms=owner.handle.domain.mono_ms()?.saturating_sub(started));
                    match result {
                        Ok(receipt) => break receipt,
                        Err(error)
                            if matches!(
                                error.cause.refusal(),
                                Some(super::super::CommandFailure::CommandConflict)
                            ) =>
                        {
                            // The dispatch error alone is not non-commit proof. A
                            // fresh quorum read must find a different immutable
                            // fingerprint at this exact occupied sequence.
                            let observed = match observe(&cell, id.sequence(), remaining()?).await {
                                Ok(observed) => observed,
                                Err(proof_error) => {
                                    return Err(anyhow::Error::from(error).context(format!(
                                        "ingress sequence conflict proof failed: {proof_error}"
                                    )))
                                }
                            };
                            if !observed.consumed_other(id.sequence(), fingerprint) {
                                return Err(dispatch_failure(error, attempt, dispatch_budget_ms));
                            }
                            submitted.store(false, Ordering::Release);
                            *command.lock().unwrap() = Some(CommandId::new(
                                sql_ingress_identity::client(),
                                observed.next,
                            )?);
                        }
                        Err(error) => {
                            return Err(dispatch_failure(error, attempt, dispatch_budget_ms))
                        }
                    }
                };
                let id = command
                    .lock()
                    .unwrap()
                    .ok_or(super::super::CommandFailure::CorruptState)?;
                anyhow::ensure!(
                    receipt.id == id
                        && receipt.version.history == history
                        && receipt.durability >= durability,
                    super::super::CommandFailure::CorruptState
                );
                let response = native_rpc::decode_response(&receipt.result)?;
                if let Some(limit) = reservation.reply_limit() {
                    anyhow::ensure!(
                        matches!(&response, RpcData::Structured(bytes) if bytes.len() <= limit),
                        crate::application::rpc::Failure::new(
                            crate::application::rpc::Code::InvalidResponse
                        )
                    );
                }
                remaining()?;
                Ok(response)
            };
            let timed = async {
                let timer = owner.handle.domain.sleep_until(deadline)?;
                crate::asyncrt::select! {
                    result = operation => result,
                    _ = timer => Err(RuntimeError::DeadlineExceeded.into()),
                }
            };
            crate::runtime::cancellation::receive(
                call_scope.parent_cancellation.as_ref(),
                call_scope.signal.as_ref(),
                timed,
            )
            .await
            .map_err(|cause| {
                let Some(command) = command.into_inner().unwrap() else {
                    return cause;
                };
                let ambiguous = cause.downcast_ref::<Arc<CellCommandError>>().map_or_else(
                    || submitted.load(Ordering::Acquire),
                    |error| error.ambiguous,
                );
                CallError {
                    command,
                    target: selected.into_inner().unwrap(),
                    ambiguous,
                    cause,
                }
                .into()
            })
        })
    }
}

/// Read-only readiness precedes enrollment/business dispatch. Reuse the same
/// logical candidate and absolute deadline; no submitted mutation is retried.
async fn ready_name(
    owner: &RaftCells,
    candidate: &CellHistory,
    deadline: u64,
) -> anyhow::Result<Option<CellHistory>> {
    let mut noted = false;
    loop {
        let now = owner.handle.domain.mono_ms()?;
        let remaining = deadline
            .checked_sub(now)
            .filter(|remaining| *remaining != 0)
            .ok_or(RuntimeError::DeadlineExceeded)?;
        let result = owner
            .resolve_cell_history(
                candidate.cluster,
                candidate.namespace,
                candidate.cell.clone(),
                QueryOptions {
                    timeout: Duration::from_millis(remaining),
                    ..QueryOptions::default()
                },
            )
            .await;
        match result {
            Err(error)
                if matches!(
                    error.as_ref(),
                    CellDispatchError::Runtime(RuntimeError::NotReady)
                ) =>
            {
                if !noted {
                    tracing::debug!(target: "cellar::raft::sql_ingress", phase = "discovery-pending");
                    noted = true;
                }
                let now = owner.handle.domain.mono_ms()?;
                let next = now
                    .saturating_add(owner.handle.core.controller.limits().retry_interval_ms)
                    .min(deadline);
                owner.handle.domain.sleep_until(next)?.await;
            }
            result => return result.map_err(anyhow::Error::from),
        }
    }
}

async fn observe(
    cell: &RaftCellHandle,
    sequence: u64,
    timeout: Duration,
) -> anyhow::Result<Observation> {
    let mut nonce = [0; 16];
    crate::asyncrt::rng("sql-ingress-sequence-query").fill_bytes(&mut nonce);
    nonce[0] |= 1;
    let result = cell
        .query(
            ProposalId::new(nonce)?,
            &cell.contract(sql_ingress_identity::OPERATION)?,
            sequence.to_be_bytes().to_vec(),
            QueryOptions {
                consistency: QueryConsistency::Linearizable,
                timeout,
                ..QueryOptions::default()
            },
        )
        .await?;
    anyhow::ensure!(
        result.version.history == *cell.history(),
        super::super::CommandFailure::CorruptState
    );
    Observation::decode(&result.result)
}

fn dispatch_failure(error: Arc<CellCommandError>, attempt: u64, budget_ms: u128) -> anyhow::Error {
    // Preserve the typed original cause and ambiguity while exposing whether
    // discovery consumed the budget or the target itself missed its deadline.
    let message =
        format!("Raft target attempt {attempt} started with {budget_ms}ms remaining: {error}");
    anyhow::Error::from(error).context(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dispatch_context_keeps_the_typed_unknown_outcome() -> anyhow::Result<()> {
        let original = Arc::new(CellCommandError {
            command: CommandId::new(sql_ingress_identity::client(), 2)?,
            ambiguous: true,
            cause: Box::new(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)),
        });
        let contextual = dispatch_failure(original.clone(), 2, 987);
        assert!(contextual.to_string().contains("987ms remaining"));
        assert!(contextual.to_string().contains("DeadlineExceeded"));
        assert!(Arc::ptr_eq(
            contextual.downcast_ref::<Arc<CellCommandError>>().unwrap(),
            &original
        ));
        Ok(())
    }
}
