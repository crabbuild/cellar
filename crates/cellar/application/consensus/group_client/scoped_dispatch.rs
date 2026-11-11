// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Dependent SDK calls retain the source invocation and its original deadlines.

use super::*;
use crate::application::storage::RaftDispatch;
use crate::raft::completion::RuntimeError;
use futures_util::future::BoxFuture;

fn failure(error: anyhow::Error) -> CellDispatchError {
    let error = match error.downcast::<CellDispatchError>() {
        Ok(error) => return error,
        Err(error) => error,
    };
    let error = match error.downcast::<RuntimeError>() {
        Ok(error) => return CellDispatchError::Runtime(error),
        Err(error) => error,
    };
    let error = match error.downcast::<crate::raft::execution::ExecutionError>() {
        Ok(error) => return CellDispatchError::Execution(error),
        Err(error) => error,
    };
    let error = match error.downcast::<cellar_logic::consensus::CommandFailure>() {
        Ok(error) => {
            return CellDispatchError::Execution(crate::raft::execution::ExecutionError::Refused(
                error,
            ))
        }
        Err(error) => error,
    };
    CellDispatchError::Execution(crate::raft::execution::ExecutionError::Storage(error))
}

impl RaftCellHandle {
    fn dispatch_scope(
        &self,
        timeout: Duration,
        mutation: bool,
    ) -> Result<Option<RaftDispatch>, CellDispatchError> {
        crate::storage::capture_raft_dispatch()
            .and_then(|source| {
                source
                    .map(|source| {
                        source.limit(
                            self.history(),
                            self.owner.handle.domain.clone(),
                            timeout,
                            mutation,
                        )
                    })
                    .transpose()
            })
            .map_err(failure)
    }

    /// Execute one independently durable target command. Inside an ordinary
    /// Raft RPC, first prove preceding source writes and retain the child work
    /// even if its response future is dropped. The caller supplies and persists
    /// the target request ID/input; this does not join two cell transactions.
    /// Open source transactions and recursive same-cell mutations refuse.
    pub fn command(
        &self,
        contract: &OperationContract,
        input: Vec<u8>,
        mut options: CommandOptions,
    ) -> impl Future<Output = Result<CommandReceipt, Arc<CellCommandError>>> + Send + 'static {
        let id = options.id;
        let future: BoxFuture<'static, _> = match self.dispatch_scope(options.timeout, true) {
            Ok(None) => Box::pin(self.command_direct(contract, input, options)),
            Err(cause) => Box::pin(async move {
                Err(Arc::new(CellCommandError {
                    command: id,
                    ambiguous: false,
                    cause: Box::new(cause),
                }))
            }),
            Ok(Some(scope)) => {
                let target = self.clone();
                let contract = contract.clone();
                let call = scope.schedule(move |timeout| {
                    options.timeout = timeout;
                    target.command_direct(&contract, input, options)
                });
                Box::pin(async move {
                    call.future.await.map_err(|error| {
                        Arc::new(CellCommandError {
                            command: id,
                            ambiguous: call.started.load(Ordering::Acquire),
                            cause: Box::new(failure(error)),
                        })
                    })?
                })
            }
        };
        future
    }

    /// A dependent query waits for the source proof too. Target consistency
    /// remains explicit and independent of the source's durability selection.
    pub fn query(
        &self,
        invocation: super::super::ProposalId,
        contract: &OperationContract,
        input: Vec<u8>,
        mut options: QueryOptions,
    ) -> impl Future<Output = Result<QueryResult, Arc<CellDispatchError>>> + Send + 'static {
        let future: BoxFuture<'static, _> = match self.dispatch_scope(options.timeout, false) {
            Ok(None) => Box::pin(self.query_direct(invocation, contract, input, options)),
            Err(cause) => Box::pin(async move { Err(Arc::new(cause)) }),
            Ok(Some(scope)) => {
                let target = self.clone();
                let contract = contract.clone();
                let call = scope.schedule(move |timeout| {
                    options.timeout = timeout;
                    target.query_direct(invocation, &contract, input, options)
                });
                Box::pin(async move {
                    call.future
                        .await
                        .map_err(|error| Arc::new(failure(error)))?
                })
            }
        };
        future
    }

    pub fn enroll(
        &self,
        request: CellEnrollmentRequest,
        timeout: Duration,
    ) -> impl Future<Output = Result<CellEnrollmentResponse, Arc<CellEnrollmentError>>> + Send + 'static
    {
        let creation = request.creation;
        let future: BoxFuture<'static, _> = match self.dispatch_scope(timeout, true) {
            Ok(None) => Box::pin(self.enroll_direct(request, timeout)),
            Err(cause) => Box::pin(async move {
                Err(Arc::new(CellEnrollmentError {
                    creation,
                    ambiguous: false,
                    cause: Box::new(cause),
                }))
            }),
            Ok(Some(scope)) => {
                let target = self.clone();
                let call = scope.schedule(move |timeout| target.enroll_direct(request, timeout));
                Box::pin(async move {
                    call.future.await.map_err(|error| {
                        Arc::new(CellEnrollmentError {
                            creation,
                            ambiguous: call.started.load(Ordering::Acquire),
                            cause: Box::new(failure(error)),
                        })
                    })?
                })
            }
        };
        future
    }

    pub fn receipt(
        &self,
        request: CellReceiptRequest,
        timeout: Duration,
    ) -> impl Future<Output = Result<CellReceiptResponse, Arc<CellDispatchError>>> + Send + 'static
    {
        let future: BoxFuture<'static, _> = match self.dispatch_scope(timeout, false) {
            Ok(None) => Box::pin(self.receipt_direct(request, timeout)),
            Err(cause) => Box::pin(async move { Err(Arc::new(cause)) }),
            Ok(Some(scope)) => {
                let target = self.clone();
                let call = scope.schedule(move |timeout| target.receipt_direct(request, timeout));
                Box::pin(async move {
                    call.future
                        .await
                        .map_err(|error| Arc::new(failure(error)))?
                })
            }
        };
        future
    }
}
