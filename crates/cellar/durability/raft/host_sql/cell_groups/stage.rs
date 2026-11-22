// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Registered and verification SQL capture shares the native RPC image owner.

use super::*;
use crate::raft::execution::CellStageCapture;

impl Cells {
    pub(super) fn capture_stage(
        &mut self,
        cell: CellHistory,
        stage: Capture,
        reply: oneshot::Sender<SqlReply>,
        mut executor: CellPrivateExecutor,
        domain: &crate::asyncrt::DomainToken,
        now: u64,
    ) -> bool {
        let Capture {
            options,
            attempt,
            mode,
            callback,
            lease,
            bytes,
        } = stage;
        let max_result = options.limits().max_result_bytes;
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
        let retention = StageRetention {
            lease,
            bytes,
            max_result,
        };
        let result = match captured {
            Ok(CellStageCapture::Captured(input)) => {
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
            captured => {
                let outcome = captured.map(|capture| match capture {
                    CellStageCapture::Duplicate(receipt) => CellStageOutcome::Duplicate(receipt),
                    CellStageCapture::Captured(_) => unreachable!("captured image dispatched"),
                });
                self.publish_stage(cell, executor, outcome, retention, None, now)
            }
        };
        let success = result.is_ok();
        let _ = reply.send(result);
        success
    }
}
