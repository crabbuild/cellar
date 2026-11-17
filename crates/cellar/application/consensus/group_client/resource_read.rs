// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared immutable resource queries retain the original deadline and private admission.
use super::super::{native_rpc, CommandFailure, QueryAdmission};
use super::*;
use crate::application::{cell_bindings::Route, HandlerFuture};
use crate::engine_api::RpcData;
use crate::raft::completion::RuntimeError;
use cellar_logic::consensus::ProposalId;
use rand::RngCore as _;

impl RaftCells {
    pub(super) fn query_resource_after(
        &self,
        route: Route,
        input: Vec<u8>,
        deadline: u64,
        source_proven: bool,
        after: Option<crate::raft::version::CellVersionToken>,
    ) -> HandlerFuture<(RpcData, crate::raft::version::CellVersionToken)> {
        let prepared = (|| -> anyhow::Result<_> {
            self.check_open()?;
            let class = route
                .cell
                .as_str()
                .split_once(':')
                .ok_or(CommandFailure::UnsupportedCapability)?
                .0;
            let admission = match route.admission {
                super::super::CommandAdmission::Event
                    if route.contract == super::super::native_event_query::contract(class) =>
                {
                    QueryAdmission::Event(route.durability)
                }
                super::super::CommandAdmission::Public
                    if class == crate::deploy::QUEUE_CLASS
                        && route.contract == super::super::native_queue_query::contract() =>
                {
                    QueryAdmission::QueueOperator(route.durability)
                }
                super::super::CommandAdmission::Public
                    if class == crate::deploy::QUEUE_CLASS
                        && route.contract == super::super::native_queue_metrics::contract() =>
                {
                    QueryAdmission::QueueMetrics(route.durability)
                }
                _ => anyhow::bail!(CommandFailure::UnsupportedCapability),
            };
            if source_proven {
                super::super::require_framework_dispatch()?;
            }
            let mut id = [0; 16];
            crate::asyncrt::rng("resource-metadata-read").fill_bytes(&mut id);
            Ok((ProposalId::new(id)?, admission))
        })();
        let owner = self.internal_observer();
        Box::pin(async move {
            let (invocation, admission) = prepared?;
            let remaining = || -> anyhow::Result<Duration> {
                owner.check_open()?;
                Ok(Duration::from_millis(
                    deadline
                        .checked_sub(owner.handle.domain.mono_ms()?)
                        .filter(|n| *n > 0)
                        .ok_or(RuntimeError::DeadlineExceeded)?,
                ))
            };
            let operation = async {
                let trace_event = matches!(admission, QueryAdmission::Event(_));
                if trace_event {
                    tracing::debug!(target: "cellar::raft::sql_ingress", probe="DEBUG-cg17-event-progress-1080", phase="resource-start", invocation=?invocation, cell=%route.cell, hinted=route.history_hint.is_some(), after=after.is_some(), remaining=deadline.saturating_sub(owner.handle.domain.mono_ms()?));
                }
                let mut backoff_ms = 10;
                loop {
                    remaining()?;
                    let result = async {
                        let history = match route.history_hint.clone() {
                            Some(history) => history,
                            None => {
                                let options = QueryOptions {
                                    timeout: remaining()?,
                                    ..Default::default()
                                };
                                let found = owner
                                    .locate_resource_history(
                                        route.cluster,
                                        route.namespace,
                                        route.cell.clone(),
                                        options,
                                        source_proven,
                                    )
                                    .await;
                                found?.ok_or(RuntimeError::NotFound)?
                            }
                        };
                        if trace_event {
                            tracing::debug!(target: "cellar::raft::sql_ingress", probe="DEBUG-cg17-event-progress-1080", phase="resource-located", invocation=?invocation, history=?history, remaining=deadline.saturating_sub(owner.handle.domain.mono_ms()?));
                        }
                        let response = owner
                            .handle
                            .core
                            .host
                            .cell(history)?
                            .query_routed_admitted(
                                CellQueryRequest {
                                    invocation,
                                    contract: route.contract.clone(),
                                    input: input.clone(),
                                    consistency: after.clone().map_or(
                                        CellQueryConsistency::Linearizable,
                                        CellQueryConsistency::LinearizableAfter,
                                    ),
                                },
                                CellRoutingLimits {
                                    timeout: remaining()?,
                                    max_attempts: 4,
                                },
                                CellQueryRouting::Automatic,
                                admission,
                            )?
                            .response()
                            .await;
                        if trace_event {
                            tracing::debug!(target: "cellar::raft::sql_ingress", probe="DEBUG-cg17-event-progress-1080", phase="resource-query-result", invocation=?invocation, version=?response.as_ref().ok().map(|r|r.observation().version.sequence.get()), error=?response.as_ref().err(), remaining=deadline.saturating_sub(owner.handle.domain.mono_ms()?));
                        }
                        let response = response?;
                        remaining()?;
                        anyhow::ensure!(
                            response.result().len() <= route.max_result_bytes,
                            CommandFailure::Overloaded
                        );
                        let result = native_rpc::decode_response(response.result())?;
                        let RpcData::Structured(bytes) = result else {
                            anyhow::bail!(CommandFailure::CorruptState);
                        };
                        let token = response.token().clone();
                        let retained =
                            crate::rpc_reservation::Reservation::shared(Arc::new(response));
                        Ok((RpcData::Structured(retained.retain_bytes(bytes)), token))
                    }
                    .await;
                    match result {
                        Err(error) if super::read_admission::retryable(&error) => {
                            // A cold selected image can temporarily refuse a
                            // reader. Retry only immutable admission, retaining
                            // the original deadline, authority and source proof.
                            remaining()?;
                            let now = owner.handle.domain.mono_ms()?;
                            owner
                                .handle
                                .domain
                                .sleep_until(now.saturating_add(backoff_ms).min(deadline))?
                                .await;
                            backoff_ms = (backoff_ms * 2).min(100);
                        }
                        result => return result,
                    }
                }
            };
            let timer = owner.handle.domain.sleep_until(deadline)?;
            crate::asyncrt::select_biased! { "original resource metadata deadline";
                _ = timer => Err(anyhow::Error::from(RuntimeError::DeadlineExceeded)),
                result = operation => result,
            }
        })
    }
}
