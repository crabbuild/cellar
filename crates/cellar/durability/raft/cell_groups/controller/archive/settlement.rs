// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Operation proofs share the archive lane; waiting never owns a SQL producer.
use super::*;
use crate::raft::budget::Permit;
use crate::raft::cell_groups::{CellDispatchError, CellSqlReceipt};
use crate::raft::execution::ExecutionLimits;
use cellar_logic::cell_group::GroupRoute;
use std::sync::{atomic::AtomicBool, Weak};

#[derive(Clone)]
pub(crate) struct Client(Weak<Shared>);

pub(crate) struct CommandProof {
    _coverage: CellGroupArchiveCoverage,
    // The exclusive journal lock survives response buffering and node close.
    // A replacement owner cannot retire this hold while a consumer still uses it.
    _journal: Arc<journal::Journal>,
    _permit: Arc<Permit>,
    directory: RangeDirectory,
    birth: CellGroupBirth,
}

impl CommandProof {
    pub(crate) async fn require_current(&self) -> std::result::Result<(), CellDispatchError> {
        self.directory
            .require_current_group_birth(&self.birth)
            .await
            .map_err(|error| CellDispatchError::Archive(Arc::new(Error::Binding(error.into()))))
    }
}

pub(crate) struct Settled {
    pub receipt: Box<CellSqlReceipt>,
    pub proof: Box<CommandProof>,
}

pub(in crate::raft::cell_groups::controller) struct Request {
    pub route: GroupRoute,
    receipt: Box<CellSqlReceipt>,
    limits: ExecutionLimits,
    deadline: u64,
    cancelled: Arc<AtomicBool>,
    reply: oneshot::Sender<std::result::Result<Settled, CellDispatchError>>,
    permit: Arc<Permit>,
}

struct Cancel(Arc<AtomicBool>);

impl Drop for Cancel {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

impl CellGroupController {
    pub(crate) fn s3_client(
        &self,
        domain: &crate::asyncrt::DomainToken,
    ) -> std::result::Result<Client, RuntimeError> {
        if !self.handle.0.domain.same_owner(domain) {
            return Err(RuntimeError::Invalid("S3 controller domain".into()));
        }
        Ok(Client(Arc::downgrade(&self.handle.0)))
    }
}

impl Client {
    pub(crate) fn require_runtime(
        &self,
        runtime: &CellGroupRuntime,
    ) -> std::result::Result<(), CellDispatchError> {
        let shared = self
            .0
            .upgrade()
            .ok_or(CellDispatchError::Runtime(RuntimeError::Closed))?;
        if !shared.runtime.same_owner(runtime) {
            return Err(CellDispatchError::Runtime(RuntimeError::Invalid(
                "S3 runtime owner".into(),
            )));
        }
        Ok(())
    }

    pub(crate) fn require_group(
        &self,
        route: &GroupRoute,
    ) -> std::result::Result<(), CellDispatchError> {
        let shared = self
            .0
            .upgrade()
            .ok_or(CellDispatchError::Runtime(RuntimeError::Closed))?;
        if shared.closing.load(Ordering::Acquire) {
            return Err(CellDispatchError::Runtime(RuntimeError::Closed));
        }
        let states = shared
            .archives
            .lock()
            .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?;
        let state = states
            .get(&route.history)
            .ok_or(CellDispatchError::Runtime(RuntimeError::NotReady))?;
        if state.stopped {
            return Err(state.error.clone().map_or(
                CellDispatchError::Runtime(RuntimeError::Closed),
                CellDispatchError::Archive,
            ));
        }
        Ok(())
    }

    pub(crate) async fn command(
        &self,
        runtime: &CellGroupRuntime,
        route: GroupRoute,
        receipt: Box<CellSqlReceipt>,
        limits: ExecutionLimits,
        deadline: u64,
    ) -> std::result::Result<Settled, CellDispatchError> {
        self.require_runtime(runtime)?;
        self.require_group(&route)?;
        let shared = self
            .0
            .upgrade()
            .ok_or(CellDispatchError::Runtime(RuntimeError::Closed))?;
        let size = receipt
            .receipt()
            .receipt
            .result()
            .len()
            .checked_mul(2)
            .and_then(|n| n.checked_add(8192))
            .ok_or(CellDispatchError::Runtime(RuntimeError::Overloaded))?;
        let permit = shared
            .s3
            .acquire(size)
            .map(Arc::new)
            .ok_or(CellDispatchError::Runtime(RuntimeError::Overloaded))?;
        let receipt = receipt.retain_for_archive(permit.clone());
        let (reply, receiver) = oneshot::channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        let _cancel = Cancel(cancelled.clone());
        let now = shared
            .domain
            .mono_ms()
            .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?;
        if now >= deadline {
            return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded));
        }
        shared
            .requests
            .try_send(requests::Request::S3(Box::new(Request {
                route,
                receipt,
                limits,
                deadline,
                cancelled,
                reply,
                permit,
            })))
            .map_err(|e| {
                CellDispatchError::Runtime(match e {
                    tokio::sync::mpsc::error::TrySendError::Closed(_) => RuntimeError::Closed,
                    tokio::sync::mpsc::error::TrySendError::Full(_) => RuntimeError::Overloaded,
                })
            })?;
        let timer = shared
            .domain
            .sleep_until(deadline)
            .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?;
        let result = crate::asyncrt::select! {
            result = receiver => result.map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?,
            _ = timer => return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)),
        };
        if shared.closing.load(Ordering::Acquire) {
            return Err(CellDispatchError::Runtime(RuntimeError::Closed));
        }
        if shared
            .domain
            .mono_ms()
            .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?
            >= deadline
        {
            return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded));
        }
        result
    }
}

impl Request {
    pub(super) fn fail(self, error: Arc<Error>) {
        let _ = self.reply.send(Err(CellDispatchError::Archive(error)));
    }

    fn expired(&self, now: u64) -> bool {
        self.cancelled.load(Ordering::Acquire) || self.reply.is_closed() || now >= self.deadline
    }
}

pub(super) fn prune(requests: &mut Vec<Request>, now: u64) {
    let mut live = Vec::new();
    for request in requests.drain(..) {
        if now >= request.deadline {
            let _ = request.reply.send(Err(CellDispatchError::Runtime(
                RuntimeError::DeadlineExceeded,
            )));
        } else if !request.expired(now) {
            live.push(request);
        }
    }
    *requests = live;
}

pub(super) async fn prove(
    requests: Vec<Request>,
    context: &job::Context,
    registration: &CellGroupArchiveRegistration,
    coverage: &CellGroupArchiveCoverage,
    execution: &ControllerExecution,
) -> Vec<Request> {
    let mut waiting = Vec::new();
    {
        let mut uses = context.uses.lock().unwrap_or_else(|e| e.into_inner());
        uses.retain(|usage| usage.live());
        uses.push(coverage.retain_use());
    }
    for request in requests {
        let now = execution
            .run(async { crate::asyncrt::mono_ms() })
            .await
            .unwrap_or(u64::MAX);
        if request.expired(now) {
            continue;
        }
        if request.route != *coverage.marker().route() {
            request.fail(Arc::new(Error::Invalid("S3 command route")));
            continue;
        }
        let store = registration.store.clone();
        let physical = coverage.clone();
        let read = execution.blocking(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                crate::raft::cell_groups::archive::receipt::command(
                    &physical,
                    &store,
                    request.receipt.receipt(),
                    request.limits,
                    request.deadline,
                    request.cancelled.clone(),
                )
            }))
            .unwrap_or_else(|panic| {
                Err(CellGroupArchiveError::Replay(
                    crate::raft::execution::storage(crate::native_worker::panic_error(panic)),
                ))
            });
            (request, result)
        });
        // A refused submission drops the closure and its owned request. Entered
        // closures retain both until the controller-specific physical join.
        let Ok(read) = read else {
            continue;
        };
        let Ok((request, result)) = read.await else {
            continue;
        };
        let now = execution
            .run(async { crate::asyncrt::mono_ms() })
            .await
            .unwrap_or(u64::MAX);
        if request.expired(now) {
            continue;
        }
        match result {
            Ok(false) | Err(CellGroupArchiveError::Busy) => waiting.push(request),
            Err(error) => request.fail(Arc::new(Error::Archive(error))),
            Ok(true) => {
                if let Err(error) = registration
                    .directory
                    .require_current_group_birth(&registration.birth)
                    .await
                {
                    request.fail(Arc::new(Error::Binding(error.into())));
                    continue;
                }
                let Request {
                    receipt,
                    reply,
                    permit,
                    ..
                } = request;
                let _ = reply.send(Ok(Settled {
                    receipt,
                    proof: Box::new(CommandProof {
                        _coverage: coverage.clone(),
                        _journal: context.journal.clone(),
                        _permit: permit,
                        directory: registration.directory.clone(),
                        birth: registration.birth.clone(),
                    }),
                }));
            }
        }
    }
    waiting
}
