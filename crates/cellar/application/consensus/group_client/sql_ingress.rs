// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A stateless request has one target command identity and no source transaction.

use super::*;
use crate::application::{cell_bindings::CallError, consensus::native_rpc, HandlerFuture};
use crate::engine_api::RpcData;
use crate::raft::completion::RuntimeError;
use cellar_logic::consensus::{ClientId, IncarnationId, ProposalId};
use rand::RngCore as _;

impl RaftCells {
    pub(crate) fn sql_ingress(
        &self,
        scope: String,
        name: Option<String>,
        method: String,
        args: RpcData,
        call_scope: crate::host_rpc::CallScope,
        deadline: u64,
    ) -> HandlerFuture<RpcData> {
        let prepared = (|| -> anyhow::Result<_> {
            self.check_open()?;
            let route = self.handle.core.host.sql_ingress_route(&scope, &method)?;
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
            let input = native_rpc::encode_request(name.as_deref(), &args, route.max_input_bytes)?;
            let mut rng = crate::asyncrt::rng("stateless-sql-command");
            let mut client = [0; 16];
            let mut creation = [0; 16];
            let mut incarnation = [0; 16];
            rng.fill_bytes(&mut client);
            rng.fill_bytes(&mut creation);
            rng.fill_bytes(&mut incarnation);
            client[0] |= 1;
            creation[0] |= 1;
            incarnation[0] |= 1;
            let id = CommandId::new(ClientId::new(client)?, 1)?;
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
                candidate,
                creation,
                id,
                retry_until_ms,
                input,
                deadline,
                memory,
            ))
        })();
        let owner = self.internal_observer();
        Box::pin(async move {
            let (
                contract,
                durability,
                candidate,
                creation,
                id,
                retry_until_ms,
                input,
                deadline,
                _memory,
            ) = prepared?;
            let selected = Mutex::new(None);
            let submitted = std::sync::atomic::AtomicBool::new(false);
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
                let history = match owner
                    .resolve_cell_history(
                        candidate.cluster,
                        candidate.namespace,
                        candidate.cell.clone(),
                        QueryOptions {
                            timeout: remaining()?,
                            ..QueryOptions::default()
                        },
                    )
                    .await?
                {
                    Some(history) => history,
                    None => {
                        let enrolled = owner
                            .cell(candidate.clone())?
                            .enroll(
                                CellEnrollmentRequest {
                                    creation,
                                    durability,
                                },
                                remaining()?,
                            )
                            .await;
                        match enrolled {
                            Ok(_) => candidate.clone(),
                            Err(error) => {
                                // A concurrent creator may win. No business command
                                // has been dispatched, so selecting that history is safe.
                                loop {
                                    match owner
                                        .resolve_cell_history(
                                            candidate.cluster,
                                            candidate.namespace,
                                            candidate.cell.clone(),
                                            QueryOptions {
                                                timeout: remaining()?,
                                                ..QueryOptions::default()
                                            },
                                        )
                                        .await
                                    {
                                        Ok(Some(history)) => break history,
                                        Ok(None) => {
                                            // Absence is only a read at this cut. The
                                            // winning creation may still be pending.
                                            // Wait for its name; do not create another
                                            // candidate or dispatch any business input.
                                            let delay = remaining()?.as_millis() as u64;
                                            let delay = delay.min(
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
                                }
                            }
                        }
                    }
                };
                *selected.lock().unwrap() = Some(history.clone());
                let cell = owner.cell(history.clone())?;
                let mut options = CommandOptions::new(id, retry_until_ms);
                options.durability = durability;
                options.timeout = remaining()?;
                // SDK submission retains admitted work if the response is lost.
                // Never choose another history or invent a mutation retry identity.
                submitted.store(true, Ordering::Release);
                let receipt = cell.command(&contract, input, options).await?;
                anyhow::ensure!(
                    receipt.id == id
                        && receipt.version.history == history
                        && receipt.durability >= durability,
                    super::super::CommandFailure::CorruptState
                );
                let response = native_rpc::decode_response(&receipt.result)?;
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
                let ambiguous = cause.downcast_ref::<Arc<CellCommandError>>().map_or_else(
                    || submitted.load(Ordering::Acquire),
                    |error| error.ambiguous,
                );
                CallError {
                    command: id,
                    target: selected.into_inner().unwrap(),
                    ambiguous,
                    cause,
                }
                .into()
            })
        })
    }
}
