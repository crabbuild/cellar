// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Dependent SDK calls retain source authority, target admission and deadlines.

use super::*;
use crate::application::storage::RaftDispatch;
use crate::raft::completion::RuntimeError;
use futures_util::future::BoxFuture;

type PreparedCall<T, E> = Result<BoxFuture<'static, Result<T, Arc<E>>>, Arc<E>>;

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
    fn dispatch_scope(&self, timeout: Duration) -> Result<Option<RaftDispatch>, CellDispatchError> {
        crate::storage::capture_raft_dispatch()
            .and_then(|source| {
                source
                    .map(|source| source.limit(self.owner.handle.domain.clone(), timeout))
                    .transpose()
            })
            .map_err(failure)
    }

    /// Execute one independently durable target command. Inside an ordinary
    /// Raft RPC, first prove preceding source writes and retain the child work
    /// even if its response future is dropped. The caller supplies and persists
    /// the target request ID/input; this does not join two cell transactions.
    /// Open source transactions refuse. Proven SDK waits release the writer,
    /// including when a child invokes the same cell.
    pub fn command(
        &self,
        contract: &OperationContract,
        input: Vec<u8>,
        options: CommandOptions,
    ) -> impl Future<Output = Result<CommandReceipt, Arc<CellCommandError>>> + Send + 'static {
        let id = options.id;
        let future: BoxFuture<'static, _> = match self.dispatch_scope(options.timeout) {
            Ok(None) => Box::pin(self.command_direct(contract, input, options)),
            Err(cause) => Box::pin(async move {
                Err(Arc::new(CellCommandError {
                    command: id,
                    ambiguous: false,
                    cause: Box::new(cause),
                }))
            }),
            Ok(Some(scope)) => match self.prepare_sdk_command(contract, input, options) {
                Err(error) => Box::pin(async move { Err(error) }),
                Ok(prepared) => {
                    let call = scope.schedule(move |_| prepared);
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
            },
        };
        future
    }

    /// A dependent query waits for the source proof too. Its requested target
    /// consistency remains independent of source durability.
    pub fn query(
        &self,
        invocation: super::super::ProposalId,
        contract: &OperationContract,
        input: Vec<u8>,
        options: QueryOptions,
    ) -> impl Future<Output = Result<QueryResult, Arc<CellDispatchError>>> + Send + 'static {
        let future: BoxFuture<'static, _> = match self.dispatch_scope(options.timeout) {
            Ok(None) => Box::pin(self.query_direct(invocation, contract, input, options)),
            Err(cause) => Box::pin(async move { Err(Arc::new(cause)) }),
            Ok(Some(scope)) => match self.prepare_sdk_query(invocation, contract, input, options) {
                Err(error) => Box::pin(async move { Err(error) }),
                Ok(prepared) => {
                    let call = scope.schedule(move |_| prepared);
                    Box::pin(async move {
                        call.future
                            .await
                            .map_err(|error| Arc::new(failure(error)))?
                    })
                }
            },
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
        let future: BoxFuture<'static, _> = match self.dispatch_scope(timeout) {
            Ok(None) => Box::pin(self.enroll_direct(request, timeout)),
            Err(cause) => Box::pin(async move {
                Err(Arc::new(CellEnrollmentError {
                    creation,
                    ambiguous: false,
                    cause: Box::new(cause),
                }))
            }),
            Ok(Some(scope)) => match self.prepare_sdk_enrollment(request, timeout) {
                Err(error) => Box::pin(async move { Err(error) }),
                Ok(prepared) => {
                    let call = scope.schedule(move |_| prepared);
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
            },
        };
        future
    }

    pub fn receipt(
        &self,
        request: CellReceiptRequest,
        timeout: Duration,
    ) -> impl Future<Output = Result<CellReceiptResponse, Arc<CellDispatchError>>> + Send + 'static
    {
        let future: BoxFuture<'static, _> = match self.dispatch_scope(timeout) {
            Ok(None) => Box::pin(self.receipt_direct(request, timeout)),
            Err(cause) => Box::pin(async move { Err(Arc::new(cause)) }),
            Ok(Some(scope)) => match self.prepare_sdk_receipt(request, timeout) {
                Err(error) => Box::pin(async move { Err(error) }),
                Ok(prepared) => {
                    let call = scope.schedule(move |_| prepared);
                    Box::pin(async move {
                        call.future
                            .await
                            .map_err(|error| Arc::new(failure(error)))?
                    })
                }
            },
        };
        future
    }

    fn prepare_sdk_command(
        &self,
        contract: &OperationContract,
        input: Vec<u8>,
        options: CommandOptions,
    ) -> PreparedCall<CommandReceipt, CellCommandError> {
        let id = options.id;
        self.owner.check_open().map_err(|cause| {
            Arc::new(CellCommandError {
                command: id,
                ambiguous: false,
                cause: Box::new(cause),
            })
        })?;
        let response = self
            .cell
            .prepare_sdk_command(
                CellCommandRequest {
                    id,
                    contract: contract.clone(),
                    input,
                    durability: options.durability,
                    retry_until_ms: options.retry_until_ms,
                },
                CellRoutingLimits {
                    timeout: options.timeout,
                    max_attempts: 4,
                },
            )
            .map_err(Arc::new)?;
        let owner = self.owner.clone();
        Ok(Box::pin(async move {
            let _owner = owner;
            let response = response.await?;
            let CellVersionObservation { version, witness } = response.observation();
            Ok(CommandReceipt {
                id,
                result: response.result().to_vec(),
                version,
                witness,
                version_token: response.token().clone(),
                durability: response.durability(),
            })
        }))
    }

    fn prepare_sdk_query(
        &self,
        invocation: super::super::ProposalId,
        contract: &OperationContract,
        input: Vec<u8>,
        options: QueryOptions,
    ) -> PreparedCall<QueryResult, CellDispatchError> {
        self.owner.check_open().map_err(Arc::new)?;
        let (request, limits, routing) = self
            .query_request(invocation, contract, input, options)
            .map_err(Arc::new)?;
        let response = self
            .cell
            .prepare_sdk_query(request, limits, routing)
            .map_err(Arc::new)?;
        let owner = self.owner.clone();
        Ok(Box::pin(async move {
            let _owner = owner;
            let response = response.await?;
            let CellVersionObservation { version, witness } = response.observation().clone();
            Ok(QueryResult {
                result: response.result().to_vec(),
                version,
                witness,
                version_token: response.token().clone(),
                served_by: response.replica().member,
                served_by_node: response.replica().node_identity(),
            })
        }))
    }

    fn prepare_sdk_receipt(
        &self,
        request: CellReceiptRequest,
        timeout: Duration,
    ) -> PreparedCall<CellReceiptResponse, CellDispatchError> {
        self.owner.check_open().map_err(Arc::new)?;
        let response = self
            .cell
            .prepare_sdk_receipt(
                request,
                CellRoutingLimits {
                    timeout,
                    max_attempts: 4,
                },
                self.owner.handle.core.transport.clone(),
            )
            .map_err(Arc::new)?;
        let owner = self.owner.clone();
        Ok(Box::pin(async move {
            let _owner = owner;
            response.await
        }))
    }

    fn prepare_sdk_enrollment(
        &self,
        request: CellEnrollmentRequest,
        timeout: Duration,
    ) -> PreparedCall<CellEnrollmentResponse, CellEnrollmentError> {
        self.owner.check_open().map_err(|cause| {
            Arc::new(CellEnrollmentError {
                creation: request.creation,
                ambiguous: false,
                cause: Box::new(cause),
            })
        })?;
        let response = self
            .cell
            .prepare_sdk_enrollment(
                request,
                CellRoutingLimits {
                    timeout,
                    max_attempts: 4,
                },
            )
            .map_err(Arc::new)?;
        let owner = self.owner.clone();
        Ok(Box::pin(async move {
            let _owner = owner;
            response.await
        }))
    }
}
