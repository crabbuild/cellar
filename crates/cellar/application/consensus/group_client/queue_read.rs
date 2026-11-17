// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Authenticated Queue inspection reads existing histories without a command.

use super::super::{native_queue_query, CommandFailure};
use super::*;
use crate::application::{cell_bindings::Route, HandlerFuture};
use crate::engine_api::RpcData;
use crate::raft::completion::RuntimeError;
use cellar_logic::consensus::{IncarnationId, ProposalId};
use rand::RngCore as _;

fn retained_bytes(route: &Route) -> anyhow::Result<usize> {
    route
        .max_input_bytes
        .checked_mul(2)
        .and_then(|input| {
            route
                .max_result_bytes
                .checked_mul(4)
                .and_then(|result| input.checked_add(result))
        })
        .and_then(|bytes| bytes.checked_add(8192))
        .ok_or_else(|| CommandFailure::Overloaded.into())
}

impl RaftCells {
    pub(crate) fn queue_operator(
        &self,
        admitted: crate::outbound_rpc::Admitted,
        call_scope: crate::host_rpc::CallScope,
        deadline: u64,
    ) -> HandlerFuture<RpcData> {
        let prepared = (|| -> anyhow::Result<_> {
            self.check_open()?;
            anyhow::ensure!(
                admitted.method == crate::application::queue::OPERATOR,
                CommandFailure::UnsupportedCapability
            );
            let route = self
                .handle
                .core
                .host
                .queue_operator_route(&admitted.scope, &self.handle.core.log)?;
            let timeout = Duration::from_millis(
                deadline
                    .checked_sub(self.handle.domain.mono_ms()?)
                    .filter(|ms| *ms > 0)
                    .ok_or(RuntimeError::DeadlineExceeded)?,
            );
            // Classify only while admitted. Release this turn before discovery
            // or the normal mutating ingress needs the same metadata bank.
            let (_, memory) = self.metadata_admission(timeout, retained_bytes(&route)?)?;
            let read = native_queue_query::is_metadata(&admitted.args)?;
            if read {
                // Validate bounded private query bytes before a missing name
                // may create anything. The final read encodes under its own
                // admission after discovery/enrollment releases this bank.
                native_queue_query::encode(
                    &admitted.scope,
                    admitted.name.as_deref(),
                    &admitted.args,
                    route.max_input_bytes,
                )?;
            }
            drop(memory);
            Ok((route, read))
        })();
        let owner = self.internal_observer();
        Box::pin(async move {
            let (mut route, read) = prepared?;
            if !read {
                return owner
                    .queue_operator_command(admitted, call_scope, deadline)
                    .await;
            }
            let parent_cancellation = call_scope.parent_cancellation.clone();
            let signal = call_scope.signal.clone();
            let operation = async move {
                let remaining = || -> anyhow::Result<Duration> {
                    owner.check_open()?;
                    Ok(Duration::from_millis(
                        deadline
                            .checked_sub(owner.handle.domain.mono_ms()?)
                            .filter(|ms| *ms > 0)
                            .ok_or(RuntimeError::DeadlineExceeded)?,
                    ))
                };
                let history = match route.history_hint.clone() {
                    Some(history) => Some(history),
                    None => {
                        owner
                            .locate_resource_history(
                                route.cluster,
                                route.namespace,
                                route.cell.clone(),
                                QueryOptions {
                                    timeout: remaining()?,
                                    ..Default::default()
                                },
                                false,
                            )
                            .await?
                    }
                };
                let history = match history {
                    Some(history) => history,
                    // Only fresh, verified absence admits creation. Enrollment
                    // installs the inspection schema; no metadata command is
                    // needed after its original minimum is proven.
                    None => owner.enroll_queue_inspection(&route, deadline).await?,
                };
                route.history_hint = Some(history);
                route.contract = native_queue_query::contract();
                let (_, memory) =
                    owner.metadata_admission(remaining()?, retained_bytes(&route)?)?;
                let input = native_queue_query::encode(
                    &admitted.scope,
                    admitted.name.as_deref(),
                    &admitted.args,
                    route.max_input_bytes,
                )?;
                let (reply, _) = owner
                    .query_resource_after(route, input, deadline, false, None)
                    .await?;
                let RpcData::Structured(bytes) = reply else {
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
                let retained = crate::rpc_reservation::Reservation::shared(Arc::new((
                    admitted.reservation,
                    memory,
                )));
                Ok(RpcData::Structured(retained.retain_bytes(bytes)))
            };
            crate::runtime::cancellation::receive(
                parent_cancellation.as_ref(),
                signal.as_ref(),
                operation,
            )
            .await
        })
    }

    async fn enroll_queue_inspection(
        &self,
        route: &Route,
        deadline: u64,
    ) -> anyhow::Result<CellHistory> {
        let remaining = || -> anyhow::Result<Duration> {
            self.check_open()?;
            Ok(Duration::from_millis(
                deadline
                    .checked_sub(self.handle.domain.mono_ms()?)
                    .filter(|ms| *ms > 0)
                    .ok_or(RuntimeError::DeadlineExceeded)?,
            ))
        };
        let (_, _memory) = self.metadata_admission(remaining()?, 8192)?;
        let mut creation = [0; 16];
        let mut incarnation = [0; 16];
        let mut rng = crate::asyncrt::rng("queue-inspection-enrollment");
        rng.fill_bytes(&mut creation);
        rng.fill_bytes(&mut incarnation);
        creation[0] |= 1;
        incarnation[0] |= 1;
        let creation = ProposalId::new(creation)?;
        let candidate = CellHistory {
            cluster: route.cluster,
            namespace: route.namespace,
            cell: route.cell.clone(),
            incarnation: IncarnationId::new(incarnation)?,
        };
        match self
            .cell(candidate.clone())?
            .enroll(
                CellEnrollmentRequest {
                    creation,
                    durability: route.durability,
                },
                remaining()?,
            )
            .await
        {
            Ok(proof) => {
                anyhow::ensure!(
                    proof.record().cell() == &candidate
                        && proof.record().creation() == creation
                        && proof.record().schema() == route.schema
                        && proof.durability() >= route.durability,
                    CommandFailure::CorruptState
                );
                // Release enrollment output and its physical admission before
                // the query selects and independently proves its current image.
                Ok(candidate)
            }
            Err(error) => loop {
                // An unknown creation never permits another candidate or a
                // command retry. A concurrent creator may own the logical name;
                // only fresh discovery can select it, then the query rechecks
                // current history/route/quorum and its own S3 minimum.
                match super::sql_ingress::ready_name(self, &candidate, deadline).await {
                    Ok(Some(history)) => return Ok(history),
                    Ok(None) => (),
                    Err(_) => return Err(error.into()),
                }
                let delay = (remaining()?.as_millis() as u64)
                    .min(self.handle.core.controller.limits().retry_interval_ms);
                let now = self.handle.domain.mono_ms()?;
                self.handle
                    .domain
                    .sleep_until(now.saturating_add(delay).min(deadline))?
                    .await;
            },
        }
    }
}
