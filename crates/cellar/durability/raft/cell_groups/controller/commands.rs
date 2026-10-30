// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded invocation ownership is independent of response-waiter lifetime.
use super::calls::Delivery;
use super::*;
use crate::raft::budget::{Budget, Permit};
use crate::raft::cell_groups::{
    CellCommandError, CellCommandInvocation, CellCommandResponse, CellDispatchError, CellSqlOptions,
};
use cellar_logic::consensus::CommandId;
use tokio::sync::{mpsc, oneshot, watch};

pub(super) struct Request {
    invocation: CellCommandInvocation,
    options: CellSqlOptions,
    reply: oneshot::Sender<Delivery<CellCommandResponse, CellCommandError>>,
    permit: Arc<Permit>,
}

pub(super) struct Completed {
    pub invocation: CellCommandInvocation,
    pub error: Option<Arc<CellCommandError>>,
    _permit: Arc<Permit>,
}

/// Dropping or timing out this response never cancels the admitted command.
/// An unpolled reply retains node admission, including error-only replies.
pub struct CellCommandCall {
    inner: calls::Call<CommandId, CellCommandResponse, CellCommandError>,
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
        mut options: CellSqlOptions,
    ) -> Result<CellCommandCall, CellCommandAdmissionError> {
        let shared = &self.handle.0;
        let admitted = (|| {
            let (command, deadline, bytes) = invocation.controller_admission(&shared.domain)?;
            if shared.closing.load(Ordering::Acquire) {
                return Err(error(command, false, RuntimeError::Closed));
            }
            let bytes = bytes
                .checked_add(option_bytes(&mut options).map_err(|e| error(command, false, e))?)
                .ok_or_else(|| error(command, false, RuntimeError::Overloaded))?;
            let permit = shared
                .commands
                .acquire(bytes)
                .ok_or_else(|| error(command, false, RuntimeError::Overloaded))?;
            Ok((command, deadline, permit))
        })();
        let (command, deadline, permit) = match admitted {
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
                invocation,
                options,
                reply,
                permit: Arc::new(permit),
            })))
            .map_err(|e| {
                let (request, reason) = match e {
                    mpsc::error::TrySendError::Closed(request) => (request, RuntimeError::Closed),
                    mpsc::error::TrySendError::Full(request) => (request, RuntimeError::Overloaded),
                };
                CellCommandAdmissionError {
                    error: error(command, false, reason),
                    invocation: match request {
                        requests::Request::Command(request) => Box::new(request.invocation),
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
}

pub(super) async fn drive(
    request: Request,
    runtime: CellGroupRuntime,
    sql: CellGroupSqlPool,
    shared: Arc<Shared>,
    mut progress: watch::Receiver<()>,
) -> Completed {
    let Request {
        mut invocation,
        options,
        reply,
        permit,
    } = request;
    let result = invocation
        .run_owned(&runtime, &sql, options, &shared.groups, &mut progress)
        .await
        .map_err(Arc::new);
    let error = result.as_ref().err().cloned();
    let _ = reply.send(Delivery {
        result,
        _permit: permit.clone(),
    });
    Completed {
        invocation,
        error,
        _permit: permit,
    }
}

pub(super) fn budget(limits: CellGroupControllerLimits) -> Arc<Budget> {
    Budget::new(limits.max_command_bytes, limits.max_commands)
}
