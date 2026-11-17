// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Registered and verification SQL capture shares the native RPC image owner.

use super::*;
use crate::raft::execution::CellStageCapture;

impl Cells {
    pub(super) fn start_stage(
        &mut self,
        cell: CellHistory,
        stage: Stage,
        reply: oneshot::Sender<SqlReply>,
        max: usize,
        domain: &crate::asyncrt::DomainToken,
        now: u64,
    ) -> bool {
        let started = (|| {
            self.check_capacity(&cell, max)?;
            // Retirement must join every same-cell continuation before sealing
            // its sequence, including a parked RPC with no writer lease.
            if matches!(
                stage.attempt.request.operation.as_str(),
                crate::raft::execution::binding_seal::OPERATION
                    | crate::storage::raft_binding_recovery::CLAIM
                    | crate::storage::raft_binding_recovery::SETTLE
                    | crate::storage::raft_payloads::RETIRE
            ) && self.suspended.iter().any(|rpc| rpc.belongs_to(&cell))
            {
                return Err(ExecutionError::Busy);
            }
            if reply.is_closed() {
                return Err(unavailable());
            }
            let Stage {
                options,
                authority,
                attempt,
                mode,
                callback,
                lease,
                bytes,
            } = stage;
            if authority.cell().version.history != cell {
                return Err(ExecutionError::Invalid("SQL cell history"));
            }
            let max_result = options.limits().max_result_bytes;
            let mut executor = options.open(authority)?;
            let captured = match callback {
                Callback::Verification(callback) => {
                    executor.capture(attempt, mode.allows_new(), callback)
                }
                Callback::Registered {
                    class,
                    input,
                    admission,
                    ..
                } => {
                    if matches!(admission, CommandAdmission::Event) && mode.allows_new() {
                        Err(ExecutionError::Invalid("event requires the RPC owner"))
                    } else {
                        executor.capture_registered(
                            attempt,
                            &class,
                            &input,
                            mode.allows_new(),
                            admission,
                        )
                    }
                }
            };
            Ok((
                executor,
                captured,
                mode,
                StageRetention {
                    lease,
                    bytes,
                    max_result,
                },
            ))
        })();
        let result = match started {
            Ok((executor, Ok(CellStageCapture::Captured(input)), mode, retention)) => {
                self.preparing.push_back(Preparing {
                    task: Preparation::new(*input, domain.clone(), mode),
                    executor,
                    cell,
                    mode,
                    reply,
                    retention,
                    activation: None,
                    waker: std::task::Waker::from(Arc::new(crate::raft::host_sql::ApplyWake(
                        self.wake.clone(),
                    ))),
                });
                return true;
            }
            Ok((executor, captured, _, retention)) => {
                let outcome = captured.map(|capture| match capture {
                    CellStageCapture::Duplicate(receipt) => CellStageOutcome::Duplicate(receipt),
                    CellStageCapture::Captured(_) => unreachable!("captured image dispatched"),
                });
                self.publish_stage(cell, executor, outcome, retention, None, now)
            }
            Err(error) => Err(error),
        };
        let success = result.is_ok();
        let _ = reply.send(result);
        success
    }
}
