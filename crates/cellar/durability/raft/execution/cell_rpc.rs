// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A suspended SDK RPC retains one private transaction on its SQL owner.

use super::*;
use crate::application::{consensus::CommandTarget, HandlerFuture};
use crate::storage::ActiveCommand;
use std::task::{Context, Poll};

pub(crate) enum CellRpcStart {
    Open(Box<CellRpcTurn>),
    Duplicate(Box<CellCommittedReceipt>),
}

pub(crate) struct CellRpcTurn {
    future: HandlerFuture<Vec<u8>>,
    transaction: ActiveCommand,
    attempt: CommandAttempt,
    preparation: StagePreparation,
}

impl CellRpcTurn {
    pub(crate) fn poll(&mut self, cx: &mut Context<'_>) -> Poll<anyhow::Result<Vec<u8>>> {
        self.transaction
            .turn(|_| Ok(RaftClass::poll_cell_rpc(&mut self.future, cx)))
            .unwrap_or_else(|error| Poll::Ready(Err(error)))
    }
}

impl CellPrivateExecutor {
    pub(crate) fn start_rpc(
        &mut self,
        attempt: CommandAttempt,
        class: &RaftClass,
        input: &[u8],
        allow_new: bool,
    ) -> Result<CellRpcStart, ExecutionError> {
        let contract = super::super::registered::validate(
            self.options.limits,
            self.options.application_digest,
            &self.authority.cell().version.history,
            &attempt.request,
            class,
            input,
            false,
        )?;
        let mut preparation = match self.prepare_stage(&attempt, allow_new)? {
            StageAdmission::Duplicate(receipt) => return Ok(CellRpcStart::Duplicate(receipt)),
            StageAdmission::New(preparation) => preparation,
        };
        let database = self
            .working
            .as_mut()
            .ok_or(ExecutionError::RebuildRequired)?
            .database_mut();
        let transaction = database.begin(attempt.inputs).map_err(private_failure)?;
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
            transaction,
            attempt,
            preparation,
        })))
    }

    pub(crate) fn check_rpc(&self) -> Result<(), ExecutionError> {
        self.authority.check().map_err(authority_failure)
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
            transaction,
            attempt,
            preparation,
        } = turn;
        // A callback destructor runs with its Storage view uninstalled.
        let dropped = drop_handler(future);
        let result = result.and_then(|result| {
            dropped?;
            self.check_rpc()?;
            transaction.turn(|tx| command::record(self.options.limits, &attempt, result, tx))
        });
        self.working
            .as_mut()
            .ok_or(ExecutionError::RebuildRequired)?
            .database_mut()
            .settle(transaction, result)
            .map_err(private_failure)?;
        self.capture_stage(attempt, preparation)
    }

    pub(crate) fn abort_rpc(&mut self, turn: CellRpcTurn) -> Result<(), ExecutionError> {
        let CellRpcTurn {
            future,
            transaction,
            ..
        } = turn;
        let dropped = drop_handler(future);
        self.working
            .as_mut()
            .ok_or(ExecutionError::RebuildRequired)?
            .database_mut()
            .abort(transaction)
            .and(dropped)
            .map_err(private_failure)
    }
}

fn drop_handler(future: HandlerFuture<Vec<u8>>) -> anyhow::Result<()> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(future)))
        .map_err(crate::native_worker::panic_error)
}
