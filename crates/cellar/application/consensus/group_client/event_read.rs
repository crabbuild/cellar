// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Event metadata uses immutable strong reads, without enrollment or a
//! command sequence. All discovery, archive proof and output share one deadline.

use super::super::{native_event_rpc, native_rpc, CommandFailure, QueryAdmission};
use super::*;
use crate::application::{cell_bindings::Route, HandlerFuture};
use crate::engine_api::RpcData;
use crate::raft::completion::RuntimeError;
use cellar_logic::consensus::ProposalId;
use rand::RngCore as _;

impl RaftCells {
    pub(crate) fn event_read(
        &self,
        admitted: crate::outbound_rpc::Admitted,
        call_scope: crate::host_rpc::CallScope,
        deadline: u64,
        authority: native_event_rpc::Authority,
    ) -> HandlerFuture<RpcData> {
        let prepared = (|| -> anyhow::Result<_> {
            self.check_open()?;
            let route = self.handle.core.host.event_query_route(
                &admitted.scope,
                &admitted.method,
                &authority,
                &self.handle.core.log,
            )?;
            let bytes = route
                .max_input_bytes
                .checked_mul(2)
                .and_then(|bytes| {
                    route
                        .max_result_bytes
                        .checked_mul(4)
                        .and_then(|result| bytes.checked_add(result))
                })
                .and_then(|bytes| bytes.checked_add(8192))
                .ok_or(CommandFailure::Overloaded)?;
            let timeout = Duration::from_millis(
                deadline
                    .checked_sub(self.handle.domain.mono_ms()?)
                    .filter(|n| *n > 0)
                    .ok_or(RuntimeError::DeadlineExceeded)?,
            );
            let (_, memory) = self.metadata_admission(timeout, bytes)?;
            let input = native_event_rpc::encode(
                &admitted.scope,
                admitted.name.as_deref(),
                &admitted.method,
                authority,
                &admitted.args,
                route.max_input_bytes,
            )?;
            Ok((route, input, admitted.reservation, memory))
        })();
        let owner = self.internal_observer();
        Box::pin(async move {
            let (route, input, reservation, memory) = prepared?;
            let operation = owner.query_event_resource(route, input, deadline, false);
            let result = crate::runtime::cancellation::receive(
                call_scope.parent_cancellation.as_ref(),
                call_scope.signal.as_ref(),
                operation,
            )
            .await?;
            let RpcData::Structured(bytes) = result else {
                anyhow::bail!(CommandFailure::CorruptState);
            };
            if let Some(limit) = call_scope.reply_limit() {
                anyhow::ensure!(
                    bytes.len() <= limit,
                    crate::application::rpc::Failure::new(
                        crate::application::rpc::Code::InvalidResponse
                    )
                );
            }
            let retained =
                crate::rpc_reservation::Reservation::shared(Arc::new((reservation, memory)));
            Ok(RpcData::Structured(retained.retain_bytes(bytes)))
        })
    }

    /// Source callers enter through their owned read dispatch, after proving
    /// preceding writes. Neither source nor target acquires a mutation identity.
    pub(super) fn query_event_resource(
        &self,
        route: Route,
        input: Vec<u8>,
        deadline: u64,
        source_proven: bool,
    ) -> HandlerFuture<RpcData> {
        let prepared = (|| -> anyhow::Result<_> {
            self.check_open()?;
            let class = route
                .cell
                .as_str()
                .split_once(':')
                .ok_or(CommandFailure::UnsupportedCapability)?
                .0;
            anyhow::ensure!(
                route.admission == super::super::CommandAdmission::Event
                    && route.contract == super::super::native_event_query::contract(class),
                CommandFailure::UnsupportedCapability
            );
            if source_proven {
                super::super::require_framework_dispatch()?;
            }
            let mut id = [0; 16];
            crate::asyncrt::rng("event-metadata-read").fill_bytes(&mut id);
            Ok(ProposalId::new(id)?)
        })();
        let owner = self.internal_observer();
        Box::pin(async move {
            let invocation = prepared?;
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
                                let found = if source_proven {
                                    owner
                                        .resolve_cell_history_direct(
                                            route.cluster,
                                            route.namespace,
                                            route.cell.clone(),
                                            options,
                                        )
                                        .await
                                } else {
                                    owner
                                        .resolve_cell_history(
                                            route.cluster,
                                            route.namespace,
                                            route.cell.clone(),
                                            options,
                                        )
                                        .await
                                };
                                found?.ok_or(RuntimeError::NotFound)?
                            }
                        };
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
                                    consistency: CellQueryConsistency::Linearizable,
                                },
                                CellRoutingLimits {
                                    timeout: remaining()?,
                                    max_attempts: 4,
                                },
                                CellQueryRouting::Automatic,
                                QueryAdmission::Event(route.durability),
                            )?
                            .response()
                            .await?;
                        remaining()?;
                        anyhow::ensure!(
                            response.result().len() <= route.max_result_bytes,
                            CommandFailure::Overloaded
                        );
                        let result = native_rpc::decode_response(response.result())?;
                        let RpcData::Structured(bytes) = result else {
                            anyhow::bail!(CommandFailure::CorruptState);
                        };
                        let retained =
                            crate::rpc_reservation::Reservation::shared(Arc::new(response));
                        Ok(RpcData::Structured(retained.retain_bytes(bytes)))
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
            crate::asyncrt::select_biased! { "original EventStream metadata deadline";
                _ = timer => Err(anyhow::Error::from(RuntimeError::DeadlineExceeded)),
                result = operation => result,
            }
        })
    }
}
