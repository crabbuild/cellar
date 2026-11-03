// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One bounded turn per physical group. The actual Raft owner rechecks term,
//! membership revision, recent activity and catch-up before admitting transfer.
use super::*;
use crate::raft::{
    completion::{LeadershipTransfer, RuntimeError},
    operations::RaftDrainReport,
};
use cellar_logic::consensus::CommandFailure;

impl RaftCells {
    pub(crate) async fn drain_node(
        &self,
        deadline: u64,
    ) -> Result<RaftDrainReport, CommandFailure> {
        self.begin_node_drain();
        let core = &self.handle.core;
        let domain = &self.handle.domain;
        let groups = core
            .log
            .cell_groups()
            .map_err(|_| CommandFailure::Unavailable)?;
        let mut report = RaftDrainReport::default();
        for group in groups {
            let now = domain.mono_ms().map_err(|_| CommandFailure::Unavailable)?;
            if now >= deadline {
                report.deadline_elapsed = true;
                break;
            }
            report.groups_examined += 1;
            let through = deadline.min(now.saturating_add(1000));
            let history = group.identity().scope;
            let local = group.identity().member.get();
            let mut turn = Turn::default();
            let outcome = within(domain, through, async {
                loop {
                    let status = core.runtime.status(history).await?;
                    if status.leader != Some(local) {
                        return Ok(());
                    }
                    turn.needed = true;
                    if status.ready && status.applied.index == status.committed {
                        break;
                    }
                    pause(domain, through).await?;
                }
                let progress = core.runtime.replication_status(history).await?;
                for member in progress
                    .members
                    .iter()
                    .filter(|p| {
                        p.voter
                            && p.member.get() != local
                            && p.recent_active
                            && p.matched >= progress.committed
                    })
                    .take(8)
                {
                    turn.attempts += 1;
                    match core
                        .runtime
                        .transfer_leadership(
                            history,
                            LeadershipTransfer {
                                target: member.member,
                                expected_term: progress.term,
                                expected_revision: progress.membership_revision,
                            },
                        )
                        .await
                    {
                        Ok(()) => {
                            // Admission remains a separate observation if this
                            // deadline expires before a successor is visible.
                            turn.accepted = true;
                            turn.released = within(domain, through, async {
                                loop {
                                    let status = core.runtime.status(history).await?;
                                    if status.term >= progress.term
                                        && status.leader.is_some_and(|id| id != local)
                                    {
                                        return Ok(());
                                    }
                                    pause(domain, through).await?;
                                }
                            })
                            .await
                            .is_ok();
                            return Ok(());
                        }
                        Err(RuntimeError::NotReady) => (),
                        Err(error) => return Err(error),
                    }
                }
                Ok(())
            })
            .await;
            report.handoff_attempts += turn.attempts;
            report.handoffs_accepted += usize::from(turn.accepted);
            report.leadership_released += usize::from(turn.released);
            if outcome.is_err() || turn.needed && !turn.accepted {
                report.unavailable_groups += 1;
            }
        }
        report.deadline_elapsed |=
            domain.mono_ms().map_err(|_| CommandFailure::Unavailable)? >= deadline;
        Ok(report)
    }
}

#[derive(Default)]
struct Turn {
    needed: bool,
    attempts: usize,
    accepted: bool,
    released: bool,
}

pub(super) async fn within<T>(
    domain: &crate::asyncrt::DomainToken,
    deadline: u64,
    future: impl Future<Output = Result<T, RuntimeError>>,
) -> Result<T, RuntimeError> {
    if domain.mono_ms().map_err(|_| RuntimeError::Closed)? >= deadline {
        return Err(RuntimeError::DeadlineExceeded);
    }
    let timer = domain
        .sleep_until(deadline)
        .map_err(|_| RuntimeError::Closed)?;
    crate::asyncrt::select! { result = future => result, _ = timer => Err(RuntimeError::DeadlineExceeded) }
}

async fn pause(domain: &crate::asyncrt::DomainToken, deadline: u64) -> Result<(), RuntimeError> {
    let now = domain.mono_ms().map_err(|_| RuntimeError::Closed)?;
    if now >= deadline {
        return Err(RuntimeError::DeadlineExceeded);
    }
    domain
        .sleep_until(deadline.min(now.saturating_add(25)))
        .map_err(|_| RuntimeError::Closed)?
        .await;
    Ok(())
}
