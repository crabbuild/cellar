// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded invocation ownership is independent of response-waiter lifetime.
use super::admission::Retention;
use super::calls::Delivery;
use super::*;
use crate::raft::budget::Budget;
use crate::raft::cell_groups::{
    CellCommandError, CellCommandInvocation, CellCommandRequest, CellCommandResponse,
    CellDispatchError, CellGroupDispatcher, CellRoutingLimits, CellSqlOptions, MutationRouting,
    PendingCellCommand,
};
use cellar_logic::consensus::CommandId;
use tokio::sync::{mpsc, oneshot, watch};

enum Invocation {
    Ready(Box<CellCommandInvocation>),
    Pending(Box<PendingCellCommand>),
}

pub(super) struct Request {
    invocation: Invocation,
    options: CellSqlOptions,
    reply: oneshot::Sender<Delivery<CellCommandResponse, CellCommandError, Retention>>,
    permit: Arc<Retention>,
}

pub(super) struct Completed {
    pub invocation: Option<CellCommandInvocation>,
    pub error: Option<Arc<CellCommandError>>,
    _permit: Arc<Retention>,
}

/// Dropping or timing out this response never cancels the admitted command.
/// An unpolled reply retains node admission, including error-only replies.
pub struct CellCommandCall {
    inner: calls::Call<CommandId, CellCommandResponse, CellCommandError, Retention>,
}

/// Admission did not transfer ownership. In particular, a rejected non-fresh
/// invocation is returned with its original recovery state intact.
pub struct CellCommandAdmissionError {
    pub error: CellCommandError,
    pub invocation: Box<CellCommandInvocation>,
}

impl std::fmt::Debug for CellCommandAdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CellCommandAdmissionError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for CellCommandAdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.error, f)
    }
}

impl std::error::Error for CellCommandAdmissionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

fn error(command: CommandId, ambiguous: bool, reason: RuntimeError) -> CellCommandError {
    CellCommandError {
        command,
        ambiguous,
        cause: Box::new(CellDispatchError::Runtime(reason)),
    }
}

impl CellCommandCall {
    pub async fn response(self) -> Result<CellCommandResponse, Arc<CellCommandError>> {
        self.inner.response().await
    }
}

pub(super) fn option_bytes(options: &mut CellSqlOptions) -> Result<usize, RuntimeError> {
    let (path, vfs) = match options {
        CellSqlOptions::Materialized(options) => (&mut options.directory, &mut options.sqlite_vfs),
        CellSqlOptions::Extents(options) => (&mut options.cache_directory, &mut options.sqlite_vfs),
    };
    if path.as_os_str().len() > 4096 || vfs.as_ref().is_some_and(|vfs| vfs.len() > 128) {
        return Err(RuntimeError::Invalid("command SQL option bounds".into()));
    }
    *path = std::mem::take(path).into_boxed_path().into_path_buf();
    *vfs = vfs.take().map(|vfs| vfs.into_boxed_str().into_string());
    Ok(2 * (path.as_os_str().len() + vfs.as_ref().map_or(0, String::len)))
}

impl CellGroupController {
    /// Admit a fresh local invocation. Discovery and destination authority still
    /// run inside the owner; catalog routing never grants leader authority.
    pub fn command(
        &self,
        invocation: CellCommandInvocation,
        options: CellSqlOptions,
    ) -> Result<CellCommandCall, CellCommandAdmissionError> {
        admit(&self.handle.0, invocation, options)
    }

    /// Capture and queue a registered cell command before returning its response
    /// waiter. Construction, routing, SQL and publication remain node-owned even
    /// if that waiter is never polled or is dropped. The timeout starts here.
    pub fn dispatch_command(
        &self,
        dispatcher: &CellGroupDispatcher,
        cell: cellar_logic::cell_group::CellHistory,
        request: CellCommandRequest,
        routing: CellRoutingLimits,
        options: CellSqlOptions,
    ) -> Result<CellCommandCall, CellCommandError> {
        let pending = dispatcher.prepare_command(cell, request, routing, None, None, false)?;
        self.dispatch_prepared_command(pending, options)
    }

    /// Transfer a fully charged preparation without restarting its deadline or
    /// acquiring a second namespace reservation. Routing stays controller-owned.
    pub(crate) fn dispatch_prepared_command(
        &self,
        pending: PendingCellCommand,
        mut options: CellSqlOptions,
    ) -> Result<CellCommandCall, CellCommandError> {
        let shared = &self.handle.0;
        let (command, deadline, bytes) = pending.controller_admission(&shared.domain)?;
        let mut registration = shared
            .registration()
            .map_err(|reason| error(command, false, reason))?;
        if shared.closing.load(Ordering::Acquire) {
            return Err(error(command, false, RuntimeError::Closed));
        }
        let option_bytes = option_bytes(&mut options).map_err(|e| error(command, false, e))?;
        let bytes = bytes
            .checked_add(option_bytes)
            .ok_or_else(|| error(command, false, RuntimeError::Overloaded))?;
        let permit = shared
            .commands
            .acquire(bytes)
            .ok_or_else(|| error(command, false, RuntimeError::Overloaded))?;
        let (reply, receiver) = oneshot::channel();
        shared
            .requests
            .try_send(requests::Request::Command(Box::new(Request {
                invocation: Invocation::Pending(Box::new(pending)),
                options,
                reply,
                permit: Arc::new(Retention {
                    _permit: permit,
                    _work: registration.work.take(),
                }),
            })))
            .map_err(|e| {
                error(
                    command,
                    false,
                    match e {
                        mpsc::error::TrySendError::Closed(_) => RuntimeError::Closed,
                        mpsc::error::TrySendError::Full(_) => RuntimeError::Overloaded,
                    },
                )
            })?;
        Ok(CellCommandCall {
            inner: calls::Call {
                receiver,
                identity: command,
                deadline,
                domain: shared.domain.clone(),
                failure: |id, reason| error(id, true, reason),
            },
        })
    }
}

pub(super) fn admit(
    shared: &Arc<Shared>,
    invocation: CellCommandInvocation,
    mut options: CellSqlOptions,
) -> Result<CellCommandCall, CellCommandAdmissionError> {
    let admitted = (|| {
        let (command, deadline, bytes) = invocation.controller_admission(&shared.domain)?;
        let registration = shared
            .registration()
            .map_err(|reason| error(command, false, reason))?;
        if shared.closing.load(Ordering::Acquire) {
            return Err(error(command, false, RuntimeError::Closed));
        }
        let bytes = bytes
            .checked_add(option_bytes(&mut options).map_err(|e| error(command, false, e))?)
            .ok_or_else(|| error(command, false, RuntimeError::Overloaded))?;
        // The sole internal callback may await a normal child operation. Its
        // own admission must not occupy that child's progress capacity.
        let budget = if invocation.is_native_callback() {
            &shared.callbacks
        } else {
            &shared.commands
        };
        let permit = budget
            .acquire(bytes)
            .ok_or_else(|| error(command, false, RuntimeError::Overloaded))?;
        Ok((command, deadline, permit, registration))
    })();
    let (command, deadline, permit, mut registration) = match admitted {
        Ok(admitted) => admitted,
        Err(error) => {
            return Err(CellCommandAdmissionError {
                error,
                invocation: Box::new(invocation),
            })
        }
    };
    let (reply, receiver) = oneshot::channel();
    shared
        .requests
        .try_send(requests::Request::Command(Box::new(Request {
            invocation: Invocation::Ready(Box::new(invocation)),
            options,
            reply,
            permit: Arc::new(Retention {
                _permit: permit,
                _work: registration.work.take(),
            }),
        })))
        .map_err(|e| {
            let (request, reason) = match e {
                mpsc::error::TrySendError::Closed(request) => (request, RuntimeError::Closed),
                mpsc::error::TrySendError::Full(request) => (request, RuntimeError::Overloaded),
            };
            CellCommandAdmissionError {
                error: error(command, false, reason),
                invocation: match request {
                    requests::Request::Command(request) => match request.invocation {
                        Invocation::Ready(invocation) => invocation,
                        Invocation::Pending(_) => unreachable!("ready command admission envelope"),
                    },
                    _ => unreachable!("command admission envelope"),
                },
            }
        })?;
    Ok(CellCommandCall {
        inner: calls::Call {
            receiver,
            identity: command,
            deadline,
            domain: shared.domain.clone(),
            failure: |id, reason| error(id, true, reason),
        },
    })
}

pub(super) async fn drive(
    request: Request,
    runtime: CellGroupRuntime,
    sql: CellGroupSqlPool,
    shared: Arc<Shared>,
    mut progress: watch::Receiver<()>,
    transport: RaftTransport,
) -> Completed {
    let Request {
        invocation,
        options,
        reply,
        permit,
    } = request;
    let (mut invocation, transport) = match invocation {
        Invocation::Ready(invocation) => (*invocation, None),
        Invocation::Pending(pending) => match (*pending).finish().await {
            Ok(invocation) => (invocation, Some(transport)),
            Err(error) => {
                let error = Arc::new(error);
                let _ = reply.send(Delivery {
                    result: Err(error.clone()),
                    _permit: permit.clone(),
                });
                return Completed {
                    invocation: None,
                    error: Some(error),
                    _permit: permit,
                };
            }
        },
    };
    let result = invocation
        .run_owned(
            &runtime,
            &sql,
            options,
            &shared.groups,
            &mut progress,
            transport.as_ref().map(|transport| MutationRouting {
                transport,
                retry_interval_ms: shared.limits.retry_interval_ms,
            }),
        )
        .await
        .map_err(Arc::new);
    let error = result.as_ref().err().cloned();
    let _ = reply.send(Delivery {
        result,
        _permit: permit.clone(),
    });
    Completed {
        invocation: Some(invocation),
        error,
        _permit: permit,
    }
}

pub(super) fn budget(limits: CellGroupControllerLimits) -> Arc<Budget> {
    Budget::new(limits.max_command_bytes, limits.max_commands)
}
