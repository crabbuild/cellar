// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Value-only SQL and Workflow resources use the linearizable pinned reader.

use super::super::CommandFailure;
use super::*;
use crate::application::{cell_bindings::Route, consensus::native_rpc, HandlerFuture};
use crate::engine_api::RpcData;
use crate::raft::completion::RuntimeError;
use anyhow::Context as _;
use rand::RngCore as _;

impl RaftCells {
    pub(crate) fn resource_read(
        &self,
        admitted: crate::outbound_rpc::Admitted,
        call_scope: crate::host_rpc::CallScope,
        deadline: u64,
        configuration: Option<[u8; 32]>,
    ) -> HandlerFuture<RpcData> {
        let prepared = (|| -> anyhow::Result<_> {
            self.check_open()?;
            let route = self.handle.core.host.resource_read_route(
                None,
                configuration,
                &admitted.scope,
                &admitted.method,
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
                    .filter(|remaining| *remaining != 0)
                    .ok_or(RuntimeError::DeadlineExceeded)?,
            );
            let (_, memory) = self.metadata_admission(timeout, bytes)?;
            let input = native_rpc::encode_request(
                admitted.name.as_deref(),
                &admitted.args,
                route.max_input_bytes,
            )?;
            let held = crate::rpc_reservation::Reservation::shared(Arc::new((
                admitted.reservation,
                memory,
            )));
            Ok((route, input, timeout, held))
        })();
        let owner = self.internal_observer();
        Box::pin(async move {
            let (route, input, timeout, held) = prepared?;
            let operation = owner.query_resource(route, input, timeout, false);
            let operation = crate::runtime::cancellation::receive(
                call_scope.parent_cancellation.as_ref(),
                call_scope.signal.as_ref(),
                operation,
            );
            let timer = owner.handle.domain.sleep_until(deadline)?;
            let response = crate::asyncrt::select_biased! { "original resource read deadline";
                _=timer=>Err(anyhow::Error::from(RuntimeError::DeadlineExceeded)),
                result=operation=>result,
            }?;
            let RpcData::Structured(bytes) = response else {
                anyhow::bail!(CommandFailure::CorruptState)
            };
            if let Some(limit) = call_scope.reply_limit() {
                anyhow::ensure!(
                    bytes.len() <= limit,
                    crate::application::rpc::Failure::new(
                        crate::application::rpc::Code::InvalidResponse
                    )
                );
            }
            Ok(RpcData::Structured(held.retain_bytes(bytes)))
        })
    }

    /// The route is minted by this host's compiled binding authorization. Source
    /// callers enter only after their existing dispatch task proves its root;
    /// ingress discovery retains the ordinary callback/admission checks.
    pub(super) fn query_resource(
        &self,
        route: Route,
        input: Vec<u8>,
        timeout: Duration,
        source_proven: bool,
    ) -> HandlerFuture<RpcData> {
        let owner = self.internal_observer();
        let prepared = (|| -> anyhow::Result<_> {
            self.check_open()?;
            anyhow::ensure!(
                route.contract.kind == super::super::OperationKind::Query,
                CommandFailure::UnsupportedCapability
            );
            let deadline = owner
                .handle
                .domain
                .mono_ms()?
                .checked_add(u64::try_from(timeout.as_millis())?)
                .ok_or(RuntimeError::DeadlineExceeded)?;
            let mut id = [0; 16];
            crate::asyncrt::rng("resource-read").fill_bytes(&mut id);
            Ok((deadline, super::super::ProposalId::new(id)?))
        })();
        Box::pin(async move {
            let (deadline, invocation) = prepared?;
            let remaining = || -> anyhow::Result<Duration> {
                owner.check_open()?;
                Ok(Duration::from_millis(
                    deadline
                        .checked_sub(owner.handle.domain.mono_ms()?)
                        .filter(|remaining| *remaining != 0)
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
                                    consistency: QueryConsistency::Linearizable,
                                    timeout: remaining()?,
                                    ..Default::default()
                                };
                                // Binding I/O is already inside a source-proof permit;
                                // ingress has no source and must use ordinary discovery.
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
                                found
                                    .map_err(anyhow::Error::from)
                                    .context("resource history discovery")?
                                    .ok_or(RuntimeError::NotFound)?
                            }
                        };
                        let result = owner
                            .cell(history)
                            .context("resource handle")?
                            .query_direct(
                                invocation,
                                &route.contract,
                                input.clone(),
                                QueryOptions {
                                    consistency: QueryConsistency::Linearizable,
                                    timeout: remaining()?,
                                    ..Default::default()
                                },
                            )
                            .await
                            .context("pinned resource query")?;
                        remaining()?;
                        anyhow::ensure!(
                            result.result.len() <= route.max_result_bytes,
                            CommandFailure::Overloaded
                        );
                        native_rpc::decode_response(&result.result)
                    }
                    .await;
                    match result {
                        Err(error) if super::read_admission::retryable(&error) => {
                            // A cold selected image may not be locally ready yet.
                            // Only typed read admission refusals retry; the outer
                            // original timer and source permit remain authoritative.
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
            crate::asyncrt::select_biased! { "bounded resource pinned read";
                _=timer=>Err(anyhow::Error::from(RuntimeError::DeadlineExceeded)),
                result=operation=>result,
            }
        })
    }
}
