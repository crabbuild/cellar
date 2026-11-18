// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A stateless request has one target command identity and no source transaction.

use super::super::sql_ingress_identity::{self, Observation};
use super::*;
use crate::application::{
    cell_bindings::{CallError, EnrollmentCallError},
    consensus::native_rpc,
    HandlerFuture,
};
use crate::engine_api::RpcData;
use crate::raft::completion::RuntimeError;
use cellar_logic::consensus::{IncarnationId, ProposalId};
use rand::RngCore as _;

#[derive(Clone)]
enum Origin {
    Binding,
    Native { configuration: [u8; 32] },
    Operator,
    QueueOperator,
    KvOperator,
    WorkflowOperator,
    KvPreparation { digest: [u8; 32], length: u64 },
    Event(Box<super::super::native_event_rpc::Authority>),
}

enum Input {
    Rpc(RpcData),
    Fetch(crate::application::Request),
}

struct Submission {
    reservation: crate::rpc_reservation::Reservation,
    scope: String,
    name: Option<String>,
    method: String,
    input: Input,
}

enum Reply {
    Rpc(RpcData),
    Fetch(crate::engine_api::HttpResponse),
}

impl Input {
    fn rpc(&self) -> anyhow::Result<&RpcData> {
        match self {
            Self::Rpc(args) => Ok(args),
            Self::Fetch(_) => Err(super::super::CommandFailure::UnsupportedCapability.into()),
        }
    }
}

impl RaftCells {
    pub(crate) fn event_ingress(
        &self,
        admitted: crate::outbound_rpc::Admitted,
        call_scope: crate::host_rpc::CallScope,
        deadline: u64,
        authority: super::super::native_event_rpc::Authority,
    ) -> HandlerFuture<RpcData> {
        self.submit_sql(
            admitted,
            call_scope,
            deadline,
            Origin::Event(Box::new(authority)),
        )
    }

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

    pub(crate) fn native_fetch(
        &self,
        admitted: crate::outbound_rpc::AdmittedFetch,
        call_scope: crate::host_rpc::CallScope,
        deadline: u64,
        configuration: [u8; 32],
    ) -> HandlerFuture<crate::engine_api::HttpResponse> {
        let crate::outbound_rpc::AdmittedFetch {
            reservation,
            scope,
            name,
            request,
        } = admitted;
        let call = self.submit_command(
            Submission {
                reservation,
                scope,
                name,
                method: super::super::native_fetch::OPERATION.into(),
                input: Input::Fetch(request),
            },
            call_scope,
            deadline,
            Origin::Native { configuration },
        );
        Box::pin(async move {
            match call.await? {
                Reply::Fetch(response) => Ok(response),
                Reply::Rpc(_) => Err(super::super::CommandFailure::CorruptState.into()),
            }
        })
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

    pub(super) fn queue_operator_command(
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

    pub(crate) fn workflow_operator(
        &self,
        admitted: crate::outbound_rpc::Admitted,
        call_scope: crate::host_rpc::CallScope,
        deadline: u64,
    ) -> HandlerFuture<RpcData> {
        self.submit_sql(admitted, call_scope, deadline, Origin::WorkflowOperator)
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
        let call = self.submit_command(
            Submission {
                reservation,
                scope,
                name,
                method,
                input: Input::Rpc(args),
            },
            call_scope,
            deadline,
            origin,
        );
        Box::pin(async move {
            match call.await? {
                Reply::Rpc(response) => Ok(response),
                Reply::Fetch(_) => Err(super::super::CommandFailure::CorruptState.into()),
            }
        })
    }

    fn submit_command(
        &self,
        submission: Submission,
        call_scope: crate::host_rpc::CallScope,
        deadline: u64,
        origin: Origin,
    ) -> HandlerFuture<Reply> {
        let Submission {
            reservation,
            scope,
            name,
            method,
            input: payload,
        } = submission;
        let fetch = matches!(payload, Input::Fetch(_));
        let prepared = (|| -> anyhow::Result<_> {
            self.check_open()?;
            let route = match &origin {
                Origin::Event(authority) => self.handle.core.host.event_ingress_route(
                    &scope,
                    &method,
                    authority,
                    &self.handle.core.log,
                )?,
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
                    *configuration,
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
                Origin::WorkflowOperator => {
                    anyhow::ensure!(
                        method == crate::application::workflow::OPERATOR,
                        super::super::CommandFailure::UnsupportedCapability
                    );
                    self.handle
                        .core
                        .host
                        .workflow_operator_route(&scope, &self.handle.core.log)?
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
            let input = match (&payload, &origin) {
                (Input::Fetch(request), Origin::Native { .. }) => {
                    anyhow::ensure!(
                        method == super::super::native_fetch::OPERATION,
                        super::super::CommandFailure::UnsupportedCapability
                    );
                    super::super::native_fetch::encode_ingress_request(
                        name.as_deref(),
                        request,
                        route.max_input_bytes,
                        nonce,
                    )?
                }
                (Input::Fetch(_), _) => {
                    anyhow::bail!(super::super::CommandFailure::UnsupportedCapability)
                }
                (_, Origin::Event(authority)) => super::super::native_event_rpc::encode_ingress(
                    &scope,
                    name.as_deref(),
                    &method,
                    authority.as_ref().clone(),
                    payload.rpc()?,
                    route.max_input_bytes,
                    nonce,
                )?,
                (_, _) => native_rpc::encode_ingress_request(
                    name.as_deref(),
                    payload.rpc()?,
                    nonce,
                    route.max_input_bytes,
                )?,
            };
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
                route.admission,
                route.schema,
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
                admission,
                schema,
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
            let enrollment_pending = std::sync::atomic::AtomicBool::new(false);
            let enrollment_ambiguous = std::sync::atomic::AtomicBool::new(false);
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
                // A local applied history is only a locator. Atomic enrollment
                // already checks current routing, authority and foreign births
                // before initialization. Do not first prove absence by query.
                // Every business command still requires its own target proof.
                let (history, enrollment) = if let Some(history) = history_hint {
                    (history, None)
                } else {
                    'enrollment: loop {
                        let cell = owner.cell(candidate.clone())?;
                        let call = cell.enroll(
                            CellEnrollmentRequest {
                                creation,
                                durability,
                            },
                            remaining()?,
                        );
                        enrollment_pending.store(true, Ordering::Release);
                        enrollment_ambiguous.store(true, Ordering::Release);
                        match call.await {
                            Ok(response) => {
                                enrollment_pending.store(false, Ordering::Release);
                                break (candidate.clone(), Some(response));
                            }
                            Err(error) => {
                                enrollment_ambiguous.store(error.ambiguous, Ordering::Release);
                                let retry = retry_unsubmitted_enrollment(&error);
                                loop {
                                    if !retry {
                                        match ready_name(&owner, &candidate, deadline).await {
                                            Ok(Some(history)) => {
                                                enrollment_pending.store(false, Ordering::Release);
                                                break 'enrollment (history, None);
                                            }
                                            Ok(None) => (),
                                            Err(_) => return Err(error.into()),
                                        }
                                    }
                                    let delay = (remaining()?.as_millis() as u64).min(
                                        owner.handle.core.controller.limits().retry_interval_ms,
                                    );
                                    let now = owner.handle.domain.mono_ms()?;
                                    owner
                                        .handle
                                        .domain
                                        .sleep_until(now.saturating_add(delay).min(deadline))?
                                        .await;
                                    // A typed pre-entry refusal proves this attempt
                                    // did not enter initialization. Retry only the
                                    // same creation; no name/absence cut is needed.
                                    // Every other failure still reconciles above.
                                    if retry {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                };
                let newly_enrolled = enrollment.is_some();
                *selected.lock().unwrap() = Some(history.clone());
                tracing::debug!(target: "cellar::raft::sql_ingress", %method,
                    phase="history", newly_enrolled, elapsed_ms=owner.handle.domain.mono_ms()?.saturating_sub(started));
                // Enrollment itself can acknowledge its exact compiled intent.
                // Keep that owned minimum proof through response construction;
                // accepted/creating grants no current catalog/broker readiness.
                if method == "__catalogConfigure"
                    && matches!(&origin, Origin::Event(authority)
                        if matches!(authority.as_ref(), super::super::native_event_rpc::Authority::Event(caller)
                            if caller.require_provisioning().is_ok()))
                {
                    let response = if let Some(proof) = &enrollment {
                        owner
                            .handle
                            .core
                            .host
                            .enrolled_event_configuration(
                                &history,
                                CellEnrollmentRequest {
                                    creation,
                                    durability,
                                },
                                proof,
                                &input,
                            )?
                            .map(|status| {
                                crate::application::encode_rpc(
                                    &cellar_native::clone::StructuredValue::from_json(
                                        serde_json::json!({"ok":status}),
                                    )?,
                                )
                            })
                            .transpose()?
                    } else {
                        owner
                            .joined_event_configuration(&history, &input, durability, deadline)
                            .await?
                    };
                    if let Some(response) = response {
                        if let Some(limit) = reservation.reply_limit() {
                            anyhow::ensure!(
                                matches!(&response, RpcData::Structured(bytes) if bytes.len() <= limit),
                                crate::application::rpc::Failure::new(
                                    crate::application::rpc::Code::InvalidResponse
                                )
                            );
                        }
                        remaining()?;
                        return Ok((Reply::Rpc(response), None));
                    }
                }
                // Updates and other primitives need the same bounded SQL/image
                // admissions. Do not retain enrollment output beside a producer.
                drop(enrollment);
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
                    return Ok((Reply::Rpc(response), None));
                }
                let cell = owner.cell(history.clone())?;
                let cursor = call_scope.ingress_cursor.as_ref().map(|cursor| {
                    (
                        cursor,
                        super::super::ingress_cursor::Key::new(&history, schema),
                    )
                });
                // The runtime-owned initializer creates an empty stream. This
                // is a hint at the enrollment cut, never an exclusive sequence
                // reservation: another caller can still consume it first.
                let now = owner.handle.domain.mono_ms()?;
                let next = if newly_enrolled {
                    1
                } else if let Some(next) = cursor
                    .as_ref()
                    .and_then(|(cursor, key)| cursor.take(*key, now))
                {
                    // A previous verified receipt only supplies a candidate.
                    // Command admission and the fresh conflict proof below are
                    // unchanged; no read/serving authority is cached here.
                    owner.handle.core.log.performance().increment(
                        crate::raft::performance::Counter::CellIngressSequenceHints,
                        1,
                    );
                    next
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
                        .command_retaining_admitted(
                            &contract,
                            input.clone(),
                            options,
                            Some(reservation.clone()),
                            admission,
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
                if fetch {
                    let response = super::super::native_fetch::decode_response(&receipt.result)?;
                    remaining()?;
                    return Ok((
                        Reply::Fetch(response),
                        cursor.map(|(_, key)| (key, id.sequence())),
                    ));
                }
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
                Ok((
                    Reply::Rpc(response),
                    cursor.map(|(_, key)| (key, id.sequence())),
                ))
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
            .and_then(|(reply, completed)| {
                // Replenish only after the original cancellation/deadline
                // selection delivers success. An undelivered receipt cannot
                // advance the invocation's candidate after an unknown result.
                if let (Some(cursor), Some((key, sequence))) =
                    (call_scope.ingress_cursor.as_ref(), completed)
                {
                    cursor.remember(key, sequence, deadline, owner.handle.domain.mono_ms()?);
                }
                Ok(reply)
            })
            .map_err(|cause| {
                let Some(command) = command.into_inner().unwrap() else {
                    if enrollment_pending.load(Ordering::Acquire) {
                        return EnrollmentCallError {
                            creation,
                            target: candidate,
                            minimum: durability,
                            ambiguous: enrollment_ambiguous.load(Ordering::Acquire),
                            cause,
                        }
                        .into();
                    }
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

/// Fresh name selection is read-only, including recovery of another creator's
/// history. An absence cut never proves a pending creation cannot still commit.
pub(super) async fn ready_name(
    owner: &RaftCells,
    candidate: &CellHistory,
    deadline: u64,
) -> anyhow::Result<Option<CellHistory>> {
    ready_name_scoped(owner, candidate, deadline, false).await
}

pub(super) async fn ready_name_scoped(
    owner: &RaftCells,
    candidate: &CellHistory,
    deadline: u64,
    source_proven: bool,
) -> anyhow::Result<Option<CellHistory>> {
    if source_proven {
        super::super::require_framework_dispatch()?;
    }
    let mut noted = false;
    loop {
        let now = owner.handle.domain.mono_ms()?;
        let remaining = deadline
            .checked_sub(now)
            .filter(|remaining| *remaining != 0)
            .ok_or(RuntimeError::DeadlineExceeded)?;
        let options = QueryOptions {
            timeout: Duration::from_millis(remaining),
            ..QueryOptions::default()
        };
        let result = if source_proven {
            owner
                .resolve_cell_history_direct(
                    candidate.cluster,
                    candidate.namespace,
                    candidate.cell.clone(),
                    options,
                )
                .await
        } else {
            owner
                .resolve_cell_history(
                    candidate.cluster,
                    candidate.namespace,
                    candidate.cell.clone(),
                    options,
                )
                .await
        };
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

fn retry_unsubmitted_enrollment(error: &CellEnrollmentError) -> bool {
    !error.ambiguous
        && matches!(
            error.cause.as_ref(),
            CellDispatchError::Runtime(
                RuntimeError::Overloaded
                    | RuntimeError::NotReady
                    | RuntimeError::NotFound
                    | RuntimeError::NotLeader
            )
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enrollment_retry_requires_a_typed_pre_entry_refusal() -> anyhow::Result<()> {
        for reason in [
            RuntimeError::Overloaded,
            RuntimeError::NotReady,
            RuntimeError::NotFound,
            RuntimeError::NotLeader,
            RuntimeError::DeadlineExceeded,
            RuntimeError::Closed,
        ] {
            let retry = matches!(
                reason,
                RuntimeError::Overloaded
                    | RuntimeError::NotReady
                    | RuntimeError::NotFound
                    | RuntimeError::NotLeader
            );
            let mut error = CellEnrollmentError {
                creation: ProposalId::new([1; 16])?,
                ambiguous: false,
                cause: Box::new(CellDispatchError::Runtime(reason)),
            };
            assert_eq!(retry_unsubmitted_enrollment(&error), retry);
            error.ambiguous = true;
            assert!(!retry_unsubmitted_enrollment(&error));
        }
        let error = CellEnrollmentError {
            creation: ProposalId::new([1; 16])?,
            ambiguous: false,
            cause: Box::new(CellDispatchError::Execution(
                crate::raft::execution::ExecutionError::Refused(
                    super::super::super::CommandFailure::Overloaded,
                ),
            )),
        };
        assert!(!retry_unsubmitted_enrollment(&error));
        Ok(())
    }

    #[test]
    fn enrollment_error_keeps_the_original_creation_history_minimum_and_cause() -> anyhow::Result<()>
    {
        let original = Arc::new(CellEnrollmentError {
            creation: ProposalId::new([1; 16])?,
            ambiguous: true,
            cause: Box::new(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)),
        });
        let history = CellHistory {
            cluster: cellar_logic::consensus::ClusterId::new([2; 16])?,
            namespace: cellar_logic::consensus::NamespaceId::new([3; 16])?,
            cell: cellar_logic::cell_group::CellId::new("FetchCell:target")?,
            incarnation: IncarnationId::new([4; 16])?,
        };
        let error: anyhow::Error = EnrollmentCallError {
            creation: original.creation,
            target: history.clone(),
            minimum: Durability::S3,
            ambiguous: true,
            cause: original.clone().into(),
        }
        .into();
        let stored = error
            .downcast_ref::<crate::sdk::CellBindingEnrollmentError>()
            .unwrap();
        assert_eq!(stored.creation, original.creation);
        assert_eq!(stored.target, history);
        assert_eq!(stored.minimum, Durability::S3);
        assert!(stored.ambiguous);
        assert!(Arc::ptr_eq(
            stored
                .cause
                .downcast_ref::<Arc<CellEnrollmentError>>()
                .unwrap(),
            &original
        ));
        Ok(())
    }

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
