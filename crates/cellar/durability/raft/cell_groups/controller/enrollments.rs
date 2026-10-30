// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded invocation ownership is independent of response-waiter lifetime.
use super::calls::Delivery;
use super::*;
use crate::raft::budget::Permit;
use crate::raft::cell_groups::{
    CellDispatchError, CellEnrollmentError, CellEnrollmentInvocation, CellSqlEnrollment,
    CellSqlOptions,
};
use cellar_logic::consensus::ProposalId;
use tokio::sync::{mpsc, oneshot, watch};

pub(super) struct Request {
    invocation: CellEnrollmentInvocation,
    options: CellSqlOptions,
    reply: oneshot::Sender<Delivery<CellSqlEnrollment, CellEnrollmentError>>,
    permit: Arc<Permit>,
}

pub(super) struct Completed {
    pub invocation: CellEnrollmentInvocation,
    pub error: Option<Arc<CellEnrollmentError>>,
    _permit: Arc<Permit>,
}

/// Dropping or timing out this response never cancels the admitted enrollment.
/// An unpolled reply retains node admission, including error-only replies.
pub struct CellEnrollmentCall {
    inner: calls::Call<ProposalId, CellSqlEnrollment, CellEnrollmentError>,
}

/// Admission did not transfer ownership. In particular, a rejected non-fresh
/// invocation is returned with its original recovery state intact.
pub struct CellEnrollmentAdmissionError {
    pub error: CellEnrollmentError,
    pub invocation: Box<CellEnrollmentInvocation>,
}

impl std::fmt::Debug for CellEnrollmentAdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CellEnrollmentAdmissionError")
            .field("error", &self.error)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for CellEnrollmentAdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.error, f)
    }
}

impl std::error::Error for CellEnrollmentAdmissionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

fn error(command: ProposalId, ambiguous: bool, reason: RuntimeError) -> CellEnrollmentError {
    CellEnrollmentError {
        creation: command,
        ambiguous,
        cause: Box::new(CellDispatchError::Runtime(reason)),
    }
}

impl CellEnrollmentCall {
    pub async fn response(self) -> Result<CellSqlEnrollment, Arc<CellEnrollmentError>> {
        self.inner.response().await
    }
}

impl CellGroupController {
    /// Admit a fresh local invocation. Discovery and destination authority still
    /// run inside the owner; catalog routing never grants leader authority.
    pub fn enroll(
        &self,
        invocation: CellEnrollmentInvocation,
        mut options: CellSqlOptions,
    ) -> Result<CellEnrollmentCall, CellEnrollmentAdmissionError> {
        let shared = &self.handle.0;
        let admitted = (|| {
            let (command, deadline) = invocation.controller_admission(&shared.domain)?;
            if shared.closing.load(Ordering::Acquire) {
                return Err(error(command, false, RuntimeError::Closed));
            }
            let bytes = 8192_usize
                .checked_add(
                    commands::option_bytes(&mut options).map_err(|e| error(command, false, e))?,
                )
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
                return Err(CellEnrollmentAdmissionError {
                    error,
                    invocation: Box::new(invocation),
                })
            }
        };
        let (reply, receiver) = oneshot::channel();
        shared
            .requests
            .try_send(requests::Request::Enrollment(Box::new(Request {
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
                CellEnrollmentAdmissionError {
                    error: error(command, false, reason),
                    invocation: match request {
                        requests::Request::Enrollment(request) => Box::new(request.invocation),
                        _ => unreachable!("enrollment admission envelope"),
                    },
                }
            })?;
        Ok(CellEnrollmentCall {
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
