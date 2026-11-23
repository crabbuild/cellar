// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Retain admitted starts while their disposable workspace's physical owner runs.

use super::super::workspace_opening::SqlOpening;
use super::*;
use std::task::{Context, Poll, Waker};

struct CommandStart {
    stage: Capture,
    authority: Option<CellExecutionAuthority>,
    activation: Option<RpcActivation>,
}

struct EnrollmentStart {
    options: CellSqlOptions,
    authority: Option<CellEnrollmentAuthority>,
    attempt: CellEnrollmentAttempt,
    callback: Initializer,
    lease: CellLease,
    bytes: Arc<Bytes>,
}

enum Starting {
    Command(Box<CommandStart>),
    Enrollment(Box<EnrollmentStart>),
}

impl Starting {
    fn check(&self, closed: bool, stopped: bool, now: u64) -> Result<(), ExecutionError> {
        if closed || stopped {
            return Err(unavailable());
        }
        match self {
            Self::Command(command) => {
                if command.stage.mode.expired(now) {
                    return Err(crate::raft::execution::authority_failure(
                        RuntimeError::DeadlineExceeded,
                    ));
                }
                command
                    .authority
                    .as_ref()
                    .ok_or_else(unavailable)?
                    .check()
                    .map_err(crate::raft::execution::authority_failure)?;
                if command
                    .activation
                    .as_ref()
                    .and_then(|a| a.activation.as_ref())
                    .is_some_and(|a| !a.live())
                {
                    return Err(unavailable());
                }
                Ok(())
            }
            Self::Enrollment(enrollment) => enrollment
                .authority
                .as_ref()
                .ok_or_else(unavailable)?
                .check()
                .map_err(crate::raft::execution::authority_failure),
        }
    }

    fn close_activation(&mut self) -> Result<(), ExecutionError> {
        match self {
            Self::Command(command) => command
                .activation
                .take()
                .map_or(Ok(()), RpcActivation::close),
            Self::Enrollment(_) => Ok(()),
        }
    }
}

enum Start {
    Opening(Box<(CellSqlOptions, Starting)>),
    Replay(Box<CellSqlEnrollment>),
}

enum Opened {
    Command(Box<CellPrivateExecutor>),
    Enrollment(Box<CellPrivateEnrollment>),
}

pub(super) struct Opening {
    // Join physical custody before request admission, including unwinding.
    // SQLite, continuation and callback execution stay on this SQL worker.
    task: SqlOpening,
    pub(super) cell: CellHistory,
    starting: Starting,
    reply: oneshot::Sender<SqlReply>,
    timer: crate::raft::performance::Timer,
    waker: Waker,
    _slot: super::cleanup::Reservation,
}

impl Opening {
    pub(super) fn close(mut self, closure: rpc::Closure) -> Result<(), ExecutionError> {
        let joined = self.task.close(&self.waker);
        let retired = self.starting.close_activation();
        self.timer.finish(false);
        closure.complete(self.reply);
        joined.and(retired)
    }
}

pub(super) struct Completion {
    pub(super) reply: oneshot::Sender<SqlReply>,
    pub(super) slot: WorkerReservation,
    pub(super) timer: crate::raft::performance::Timer,
}

impl Cells {
    pub(super) fn start_opening(
        &mut self,
        cell: CellHistory,
        action: Action,
        completion: Completion,
        max: usize,
        domain: &crate::asyncrt::DomainToken,
        now: u64,
    ) {
        let Completion { reply, slot, timer } = completion;
        let start = (|| {
            self.check_capacity(&cell, max)?;
            if reply.is_closed() {
                return Err(unavailable());
            }
            match action {
                Action::Stage(stage) => {
                    if stage.authority.cell().version.history != cell {
                        return Err(ExecutionError::Invalid("SQL cell history"));
                    }
                    stage
                        .authority
                        .check()
                        .map_err(crate::raft::execution::authority_failure)?;
                    if matches!(
                        stage.attempt.request.operation.as_str(),
                        crate::raft::execution::binding_seal::OPERATION
                            | crate::storage::raft_binding_recovery::CLAIM
                            | crate::storage::raft_binding_recovery::SETTLE
                            | crate::storage::raft_binding_recovery::CONFIRM
                            | crate::storage::raft_payloads::RETIRE
                    ) && self.suspended.iter().any(|rpc| rpc.belongs_to(&cell))
                    {
                        return Err(ExecutionError::Busy);
                    }
                    // Reject only a validated cached input conflict, before
                    // workspace I/O or admission of another native instance.
                    let activation = if stage.is_cell_rpc() {
                        Some(self.start_activation(&cell, &stage, now)?)
                    } else {
                        None
                    };
                    let Stage {
                        options,
                        authority,
                        attempt,
                        mode,
                        callback,
                        lease,
                        bytes,
                    } = *stage;
                    let capture = Capture {
                        options: options.clone(),
                        attempt,
                        mode,
                        callback,
                        lease,
                        bytes,
                    };
                    Ok(Start::Opening(Box::new((
                        options,
                        Starting::Command(Box::new(CommandStart {
                            stage: capture,
                            authority: Some(authority),
                            activation,
                        })),
                    ))))
                }
                Action::Initialize(request) => {
                    let Initialize {
                        options,
                        authority,
                        attempt,
                        callback,
                        lease,
                        bytes,
                    } = *request;
                    if authority.state().cell() != &cell {
                        return Err(ExecutionError::Invalid("initialization cell history"));
                    }
                    authority
                        .check()
                        .map_err(crate::raft::execution::authority_failure)?;
                    if let Some(record) = authority.state().record() {
                        if record.cell() != &cell || record.creation() != attempt.creation {
                            return Err(ExecutionError::Refused(CommandFailure::CommandConflict));
                        }
                        return Ok(Start::Replay(Box::new(CellSqlEnrollment {
                            record: record.clone(),
                            _retention: OutputRetention::Sql { _bytes: bytes },
                            s3: None,
                        })));
                    }
                    Ok(Start::Opening(Box::new((
                        options.clone(),
                        Starting::Enrollment(Box::new(EnrollmentStart {
                            options,
                            authority: Some(authority),
                            attempt,
                            callback,
                            lease,
                            bytes,
                        })),
                    ))))
                }
                _ => Err(ExecutionError::Invalid("workspace opening dispatch")),
            }
        })();
        match start {
            Ok(Start::Opening(starting)) => {
                let (options, starting) = *starting;
                self.opening.push_back(Opening {
                    task: SqlOpening::new(&options, &cell, domain, self.workspace_lane.clone()),
                    cell,
                    starting,
                    reply,
                    timer,
                    waker: Waker::from(Arc::new(crate::raft::host_sql::ApplyWake(
                        self.wake.clone(),
                    ))),
                    _slot: slot.into(),
                });
            }
            Ok(Start::Replay(enrollment)) => {
                timer.finish(true);
                let _ = reply.send(Ok(CellSqlOutcome::Enrollment(enrollment)));
            }
            Err(error) => {
                timer.finish(false);
                let _ = reply.send(Err(error));
            }
        }
    }

    fn start_activation(
        &self,
        cell: &CellHistory,
        stage: &Stage,
        now: u64,
    ) -> Result<RpcActivation, ExecutionError> {
        let Callback::Registered {
            class,
            input,
            admission,
            commits,
        } = &stage.callback
        else {
            return Err(ExecutionError::Invalid("cell RPC callback"));
        };
        let activation = self.activations.take(cell, &stage.authority, class, now)?;
        if commits.is_some() && activation.conflicts_with(&stage.attempt.request) {
            let refusal = (|| {
                stage
                    .authority
                    .check()
                    .map_err(crate::raft::execution::authority_failure)?;
                if stage.authority.route().schema_digest != stage.options.application_digest() {
                    return Err(ExecutionError::Invalid("cell execution schema"));
                }
                crate::raft::execution::validate_admitted_registered_command(
                    stage.options.limits(),
                    stage.options.application_digest(),
                    cell,
                    &stage.attempt.request,
                    class,
                    input,
                    *admission,
                )?;
                stage
                    .authority
                    .check()
                    .map_err(crate::raft::execution::authority_failure)?;
                Err(ExecutionError::Refused(CommandFailure::CommandConflict))
            })();
            return self
                .activations
                .put(cell.clone(), activation, now)
                .and(refusal);
        }
        Ok(activation)
    }

    pub(super) fn poll_opening(
        &mut self,
        domain: &crate::asyncrt::DomainToken,
        stopped: &AtomicBool,
        now: u64,
    ) -> Result<bool, ExecutionError> {
        for _ in 0..self.opening.len() {
            let Some(mut frame) = self.opening.pop_front() else {
                break;
            };
            let admission = frame.starting.check(
                frame.reply.is_closed(),
                stopped.load(Ordering::Acquire),
                now,
            );
            let mut cx = Context::from_waker(&frame.waker);
            let opening =
                frame
                    .task
                    .poll_with(&mut cx, admission, |root| match &mut frame.starting {
                        Starting::Command(command) => command
                            .stage
                            .options
                            .clone()
                            .open(
                                root,
                                command.authority.take().ok_or_else(unavailable)?,
                                self.root_apply_limits,
                            )
                            .map(Box::new)
                            .map(Opened::Command),
                        Starting::Enrollment(enrollment) => enrollment
                            .options
                            .clone()
                            .open_enrollment(
                                root,
                                enrollment.authority.take().ok_or_else(unavailable)?,
                                self.root_apply_limits,
                            )
                            .map(Box::new)
                            .map(Opened::Enrollment),
                    });
            let opened = match opening {
                Poll::Pending => {
                    self.opening.push_back(frame);
                    continue;
                }
                Poll::Ready(Err(error)) if preparation::task_failed(&error) => {
                    self.opening.push_front(frame);
                    return Err(error);
                }
                Poll::Ready(result) => result,
            };
            if matches!(frame.starting, Starting::Command(_))
                && matches!(
                    opened,
                    Err(ExecutionError::Refused(CommandFailure::Overloaded))
                )
            {
                self.performance
                    .increment(crate::raft::performance::Counter::SqlPrivateOpenRefusals, 1);
            }
            let success = match opened {
                Err(error) => {
                    let retired = frame.starting.close_activation();
                    let _ = frame.reply.send(retired.and(Err(error)));
                    false
                }
                Ok(Opened::Command(executor)) => {
                    let Starting::Command(command) = frame.starting else {
                        unreachable!("command opening context")
                    };
                    let CommandStart {
                        stage, activation, ..
                    } = *command;
                    match activation {
                        Some(activation) => self.capture_rpc(
                            frame.cell,
                            stage,
                            frame.reply,
                            (*executor, activation),
                            domain,
                            now,
                        ),
                        None => self.capture_stage(
                            frame.cell,
                            stage,
                            frame.reply,
                            *executor,
                            domain,
                            now,
                        ),
                    }
                }
                Ok(Opened::Enrollment(executor)) => {
                    let Starting::Enrollment(enrollment) = frame.starting else {
                        unreachable!("enrollment opening context")
                    };
                    let EnrollmentStart {
                        attempt,
                        callback,
                        lease,
                        bytes,
                        ..
                    } = *enrollment;
                    let result = self
                        .capture_enrollment(frame.cell, *executor, attempt, callback, lease, bytes);
                    let success = result.is_ok();
                    let _ = frame.reply.send(result);
                    success
                }
            };
            frame.timer.finish(success);
            return Ok(true);
        }
        Ok(false)
    }

    fn capture_enrollment(
        &mut self,
        cell: CellHistory,
        mut executor: CellPrivateEnrollment,
        attempt: CellEnrollmentAttempt,
        callback: Initializer,
        lease: CellLease,
        mut bytes: Arc<Bytes>,
    ) -> SqlReply {
        let prepared = match executor.prepare(attempt, callback) {
            Ok(prepared) => prepared,
            Err(error) => {
                executor.close()?;
                return Err(error);
            }
        };
        let retained = prepared
            .bytes()
            .len()
            .checked_add(4096)
            .ok_or_else(overloaded)?;
        Bytes::shrink(&mut bytes, retained)?;
        let prepared = CellSqlPrepared {
            prepared,
            _bytes: bytes,
        };
        self.pending.insert(
            cell,
            Pending {
                producer: Producer::Enrollment(Box::new(executor)),
                prepared: prepared.clone(),
                submitted: false,
                _lease: lease,
                activation: None,
            },
        );
        Ok(CellSqlOutcome::Prepared(prepared))
    }
}
