// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A suspended SDK RPC retains one private transaction on its SQL owner.

use super::*;
use crate::application::{consensus::CommandTarget, HandlerFuture};
use crate::storage::ActiveCommand;
use std::sync::Arc;
use std::task::{Context, Poll};

pub(crate) enum CellRpcStart {
    Open(Box<CellRpcTurn>),
    Duplicate(Box<CellCommittedReceipt>),
}

pub(crate) struct CellRpcDispatch {
    pub domain: crate::asyncrt::DomainToken,
    pub deadline: u64,
    pub bindings: Option<Arc<dyn crate::application::cell_bindings::Host>>,
}

pub(crate) struct CellRpcEnvironment<'a> {
    pub system: bool,
    pub activation: &'a mut Option<crate::application::consensus::CellRpcActivation>,
    pub reserve: &'a mut dyn FnMut() -> Result<(), ExecutionError>,
    pub dispatch: Option<CellRpcDispatch>,
}

pub(crate) struct CellRpcTurn {
    future: HandlerFuture<Vec<u8>>,
    output: Option<anyhow::Result<Vec<u8>>>,
    transaction: ActiveCommand,
    attempt: CommandAttempt,
    preparation: Option<StagePreparation>,
    step: u64,
}

impl CellRpcTurn {
    pub(crate) fn retain_parked_image(
        &self,
        executor: CellPrivateExecutor,
    ) -> Result<(), ExecutionError> {
        let connection = executor
            .working
            .as_ref()
            .ok_or(ExecutionError::RebuildRequired)?
            .database()
            .framework();
        // SAFETY: the whole executor moves into the cursor arena below. Its
        // SQLite, capture VFS and admitted workspace outlive every retained VM.
        let database = unsafe { connection.handle() };
        self.transaction
            .retain_cursor_image(
                database,
                Box::new(move || executor.close().map_err(anyhow::Error::from)),
            )
            .map_err(private_failure)
    }

    pub(crate) fn can_park(&self) -> Result<bool, ExecutionError> {
        Ok(self.step != 0
            && self.output.as_ref().is_none_or(Result::is_ok)
            && self.transaction.can_park().map_err(private_failure)?)
    }

    pub(crate) fn can_poll_parked_io(&self) -> bool {
        self.step != 0 && self.transaction.can_poll_parked_io()
    }

    pub(crate) fn poll_parked_io(&self, cx: &mut Context<'_>) -> Result<bool, ExecutionError> {
        self.transaction.poll_parked_io(cx).map_err(private_failure)
    }

    pub(crate) fn close_parked(self) -> Result<(), ExecutionError> {
        let dropped = drop_handler(self.future);
        self.transaction
            .close_parked()
            .and(dropped)
            .map_err(private_failure)
    }

    pub(crate) fn take_checkpoint(
        &self,
    ) -> Option<tokio::sync::oneshot::Sender<Result<(), String>>> {
        self.transaction.take_checkpoint()
    }

    pub(crate) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<anyhow::Result<Vec<u8>>> {
        if self.output.is_none() {
            match self
                .transaction
                .turn(|_| Ok(RaftClass::poll_cell_rpc(&mut self.future, cx)))
            {
                Ok(Poll::Ready(result)) => self.output = Some(result),
                Ok(Poll::Pending) => (),
                Err(error) => return Poll::Ready(Err(error)),
            }
        }
        if self.output.as_ref().is_some_and(Result::is_err) {
            return Poll::Ready(self.output.take().expect("terminal RPC output"));
        }
        match self.transaction.poll_work(cx) {
            Ok(true) => Poll::Pending,
            Ok(false) => self.output.take().map_or(Poll::Pending, Poll::Ready),
            Err(error) => Poll::Ready(Err(error)),
        }
    }
}

impl CellPrivateExecutor {
    pub(crate) fn start_rpc(
        &mut self,
        attempt: CommandAttempt,
        class: &RaftClass,
        input: &[u8],
        allow_new: bool,
        independent: bool,
        environment: CellRpcEnvironment<'_>,
    ) -> Result<CellRpcStart, ExecutionError> {
        let contract = super::super::registered::validate(
            self.options.limits,
            self.options.application_digest,
            &self.authority.cell().version.history,
            &attempt.request,
            class,
            input,
            environment.system,
        )?;
        let mut preparation = match self.prepare_stage(&attempt, allow_new)? {
            StageAdmission::Duplicate(receipt) => return Ok(CellRpcStart::Duplicate(receipt)),
            StageAdmission::New(preparation) => preparation,
        };
        (environment.reserve)()?;
        let database = self
            .working
            .as_mut()
            .ok_or(ExecutionError::RebuildRequired)?
            .database_mut();
        let transaction = database
            .begin_with_activation(
                attempt.inputs,
                if independent {
                    self.options.limits.max_pending_transactions
                } else {
                    0
                },
                environment
                    .activation
                    .as_ref()
                    .map(|activation| activation.resources()),
            )
            .map_err(private_failure)?;
        if let Some(CellRpcDispatch {
            domain,
            deadline,
            bindings,
        }) = environment.dispatch
        {
            transaction
                .bind_dispatch(crate::application::storage::RaftDispatchContext {
                    domain,
                    deadline,
                    result_limit: self.options.limits.max_result_bytes,
                    fence: self.authority.activation_fence(),
                    binding_source: bindings.and_then(|host| {
                        class.cell_configuration_digest().map(|configuration| {
                            crate::application::cell_bindings::Source {
                                host,
                                history: self.authority.cell().version.history.clone(),
                                group: self.authority.route().history,
                                term: self.authority.term().get(),
                                parent: attempt.request.id,
                                parent_fingerprint: attempt.request.request_digest,
                                configuration,
                                retry_until_ms: attempt.request.retry_until_ms,
                                now_unix_millis: attempt.inputs.now_unix_millis,
                                durability: attempt.request.durability,
                                deadline_unix_ms: None,
                                hint: Default::default(),
                            }
                        })
                    }),
                })
                .map_err(private_failure)?;
        }
        let future = transaction.turn(|tx| {
            command::maintain(&attempt, preparation.maintenance.take(), tx)?;
            self.authority.check().map_err(authority_failure)?;
            class.start_cell_rpc(
                &contract,
                CommandTarget {
                    cell: &self.authority.cell().version.history,
                    route_epoch: Some(self.authority.route().epoch),
                },
                input,
                tx,
                environment.activation,
            )
        });
        let future = match future {
            Ok(future) => future,
            Err(error) => {
                database.abort(transaction).map_err(private_failure)?;
                return Err(private_failure(error));
            }
        };
        Ok(CellRpcStart::Open(Box::new(CellRpcTurn {
            future,
            output: None,
            transaction,
            attempt,
            preparation: Some(preparation),
            step: 0,
        })))
    }

    pub(crate) fn check_rpc(&self) -> Result<(), ExecutionError> {
        self.authority.check().map_err(authority_failure)
    }

    pub(crate) fn rpc_activation_fence(&self) -> crate::raft::cell_groups::CellActivationFence {
        self.authority.activation_fence()
    }

    pub(crate) fn rpc_term_current(&self) -> bool {
        self.authority.term_current()
    }

    pub(crate) fn finish_rpc(
        &mut self,
        turn: CellRpcTurn,
        result: anyhow::Result<Vec<u8>>,
    ) -> Result<CellStageOutcome, ExecutionError> {
        let CellRpcTurn {
            future,
            output: _,
            transaction,
            attempt,
            preparation,
            step,
        } = turn;
        // A callback destructor runs with its Storage view uninstalled.
        let dropped = drop_handler(future);
        let cancelled = transaction.cancel_work();
        let failure = result
            .as_ref()
            .err()
            .and_then(|error| {
                error.downcast_ref::<crate::application::consensus::native_work::Failure>()
            })
            .map(|failure| (failure.work.clone(), failure.leased_until, failure.at_ms));
        if let Some((work, leased_until, at_ms)) = failure {
            let cleanup = dropped.and(cancelled);
            // Reinstall the connection and roll back only unsynced application
            // writes. Earlier independently proven checkpoints remain intact.
            self.working
                .as_mut()
                .ok_or(ExecutionError::RebuildRequired)?
                .database_mut()
                .abort(transaction)
                .map_err(private_failure)?;
            cleanup.map_err(private_failure)?;
            self.check_rpc()?;
            if step == 0
                || attempt.request.operation
                    != crate::application::consensus::native_work::OPERATION
            {
                return Err(ExecutionError::Invalid("unproven native alarm failure"));
            }
            let limits = self.options.limits;
            self.working
                .as_mut()
                .ok_or(ExecutionError::RebuildRequired)?
                .database_mut()
                .transaction(attempt.inputs, |tx| {
                    if limits.max_result_bytes < 1 {
                        return Err(CommandFailure::Overloaded.into());
                    }
                    let connection = tx.framework()?;
                    crate::application::consensus::native_work::retry_failed(
                        &connection,
                        &work,
                        leased_until,
                        at_ms,
                        true,
                    )?;
                    // This is a completed private maintenance command, not a
                    // successful application result. Output 2 records failure.
                    receipts::complete_rpc(&connection, &attempt.request, &limits, step, vec![2])?;
                    command::check_capacity(limits, &connection)
                })
                .map_err(private_failure)?;
        } else {
            let result = result.and_then(|result| {
                dropped?;
                cancelled?;
                self.check_rpc()?;
                transaction.turn(|tx| {
                    if result.len() > self.options.limits.max_result_bytes {
                        return Err(CommandFailure::Overloaded.into());
                    }
                    let connection = tx.framework()?;
                    receipts::complete_rpc(
                        &connection,
                        &attempt.request,
                        &self.options.limits,
                        step,
                        result,
                    )?;
                    command::check_capacity(self.options.limits, &connection)
                })
            });
            self.working
                .as_mut()
                .ok_or(ExecutionError::RebuildRequired)?
                .database_mut()
                .settle(transaction, result)
                .map_err(private_failure)?;
        }
        let outcome = self.capture_stage(
            attempt,
            preparation.ok_or(ExecutionError::Invalid("RPC preparation missing"))?,
        )?;
        self.pending
            .as_mut()
            .ok_or(ExecutionError::Uncertain)?
            .rpc_prior_steps = step;
        Ok(outcome)
    }

    pub(crate) fn stage_rpc_checkpoint(
        &mut self,
        turn: &mut CellRpcTurn,
    ) -> Result<CellPreparedCommand, ExecutionError> {
        self.check_rpc()?;
        let step = turn
            .step
            .checked_add(1)
            .ok_or(ExecutionError::Invalid("RPC checkpoint exhausted"))?;
        turn.transaction
            .turn(|tx| {
                let connection = tx.framework()?;
                receipts::record_rpc_step(
                    &connection,
                    &turn.attempt.request,
                    &self.options.limits,
                    step,
                )?;
                command::check_capacity(self.options.limits, &connection)
            })
            .map_err(private_failure)?;
        self.working
            .as_mut()
            .ok_or(ExecutionError::RebuildRequired)?
            .database_mut()
            .checkpoint(&turn.transaction)
            .map_err(private_failure)?;
        use rand::RngCore as _;
        let mut rng = crate::asyncrt::rng("cell-rpc-checkpoint");
        let mut proposal = [0; 16];
        let mut preview = [0; 16];
        rng.fill_bytes(&mut proposal);
        rng.fill_bytes(&mut preview);
        let attempt = CommandAttempt {
            request: turn.attempt.request.clone(),
            proposal: ProposalId::new(proposal)
                .map_err(|_| ExecutionError::Invalid("RPC checkpoint proposal"))?,
            preview: cellar_ltx::generation::ImageId::new(preview)
                .map_err(|_| ExecutionError::Invalid("RPC checkpoint preview"))?,
            inputs: turn.attempt.inputs,
        };
        let preparation = turn
            .preparation
            .take()
            .ok_or(ExecutionError::Invalid("RPC checkpoint preparation"))?;
        let CellStageOutcome::Prepared(prepared) = self.capture_stage(attempt, preparation)? else {
            return Err(ExecutionError::Invalid(
                "RPC checkpoint unexpectedly duplicate",
            ));
        };
        self.pending
            .as_mut()
            .ok_or(ExecutionError::Invalid("RPC checkpoint pending"))?
            .rpc_step = Some(step);
        turn.step = step;
        Ok(prepared)
    }

    pub(crate) fn rpc_checkpoint_version(
        &self,
    ) -> Result<cellar_logic::cell_group::CellVersion, ExecutionError> {
        self.pending
            .as_ref()
            .map(|pending| pending.result.clone())
            .ok_or(ExecutionError::Uncertain)
    }

    pub(crate) fn finish_rpc_checkpoint(
        &mut self,
        committed: CellExecutionAuthority,
        reconciliation: CellReconciliationAuthority,
    ) -> Result<CellCommittedReceipt, ExecutionError> {
        let pending = self.pending.as_ref().ok_or(ExecutionError::Uncertain)?;
        let step = pending
            .rpc_step
            .ok_or(ExecutionError::Invalid("RPC checkpoint kind"))?;
        if !pending.submitted
            || committed.route() != self.authority.route()
            || committed.term() != self.authority.term()
            || committed.cell().version != pending.result
            || reconciliation.term() != committed.term()
            || reconciliation.cell().version != pending.result
        {
            return Err(ExecutionError::Uncertain);
        }
        committed.check().map_err(authority_failure)?;
        let proposal = pending.prepared.proposal();
        let CellReconciliation::Committed(receipt) = self.reconcile(proposal, reconciliation)?
        else {
            return Err(ExecutionError::Uncertain);
        };
        if receipt.receipt.rpc_step != Some(step) || receipt.version != committed.cell().version {
            return Err(ExecutionError::Invalid("RPC checkpoint receipt binding"));
        }
        committed.check().map_err(authority_failure)?;
        self.authority = committed;
        Ok(*receipt)
    }

    pub(crate) fn continue_rpc_checkpoint(
        &mut self,
        turn: &mut CellRpcTurn,
    ) -> Result<(), ExecutionError> {
        self.check_rpc()?;
        if self.pending.is_some() || turn.preparation.is_some() {
            return Err(ExecutionError::Invalid("RPC checkpoint remains pending"));
        }
        let log_reservation = self
            .authority
            .reserve_log(self.options.limits.envelope)
            .map_err(authority_failure)?;
        let sequence = self
            .authority
            .cell()
            .version
            .sequence
            .checked_next()
            .map_err(|_| ExecutionError::Invalid("cell mutation sequence exhausted"))?;
        let working = self
            .working
            .as_mut()
            .ok_or(ExecutionError::RebuildRequired)?;
        working
            .trim_cell(&self.options, self.authority.image())
            .map_err(storage)?;
        working.begin_mutation();
        working
            .database_mut()
            .continue_checkpoint(&turn.transaction)
            .map_err(private_failure)?;
        turn.preparation = Some(StagePreparation {
            sequence,
            log_reservation,
            maintenance: None,
        });
        Ok(())
    }

    pub(crate) fn park_rpc(&mut self, turn: &mut CellRpcTurn) -> Result<(), ExecutionError> {
        self.check_rpc()?;
        if self.pending.is_some() || !turn.can_park()? {
            return Err(ExecutionError::Invalid("RPC is not between proven roots"));
        }
        self.working
            .as_mut()
            .ok_or(ExecutionError::RebuildRequired)?
            .database_mut()
            .park(&turn.transaction)
            .map_err(private_failure)?;
        turn.preparation = None;
        Ok(())
    }

    pub(crate) fn resume_rpc(&mut self, turn: &mut CellRpcTurn) -> Result<(), ExecutionError> {
        self.check_rpc()?;
        if turn.step == 0 || turn.preparation.is_some() || self.pending.is_some() {
            return Err(ExecutionError::Invalid("RPC resume state"));
        }
        let working = self
            .working
            .as_mut()
            .ok_or(ExecutionError::RebuildRequired)?;
        let receipt = receipts::lookup_rpc_identity(
            working.database_mut().framework(),
            &turn.attempt.request,
            &self.options.limits,
        )?;
        if receipt.as_ref().and_then(|receipt| receipt.rpc_step) != Some(turn.step) {
            return Err(ExecutionError::Uncertain);
        }
        self.continue_rpc_checkpoint(turn)
    }

    pub(crate) fn abort_rpc(&mut self, turn: CellRpcTurn) -> Result<(), ExecutionError> {
        let CellRpcTurn {
            future,
            transaction,
            ..
        } = turn;
        let dropped = drop_handler(future);
        let cancelled = transaction.cancel_work();
        self.working
            .as_mut()
            .ok_or(ExecutionError::RebuildRequired)?
            .database_mut()
            .abort(transaction)
            .and(dropped)
            .and(cancelled)
            .map_err(private_failure)
    }
}

fn drop_handler(future: HandlerFuture<Vec<u8>>) -> anyhow::Result<()> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(future)))
        .map_err(crate::native_worker::panic_error)
}
