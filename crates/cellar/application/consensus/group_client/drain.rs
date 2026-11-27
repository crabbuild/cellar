// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One bounded turn per physical group. The actual Raft owner rechecks term,
//! membership revision, recent activity and catch-up before admitting transfer.
//! A fair share of the caller's deadline is available only while the group
//! keeps making observable progress, so a peer that is itself stopping cannot
//! hold every later group for the whole budget.
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
        let total = groups.len();
        for (index, group) in groups.into_iter().enumerate() {
            let now = domain.mono_ms().map_err(|_| CommandFailure::Unavailable)?;
            if now >= deadline {
                report.deadline_elapsed = true;
                break;
            }
            report.groups_examined += 1;
            let remaining_groups = (total - index) as u64;
            let share = (deadline - now).div_ceil(remaining_groups).max(PROGRESS_MS);
            let through = deadline.min(now.saturating_add(share));
            let history = group.identity().scope;
            let local = group.identity().member.get();
            let mut watch = Progress::new(now);
            let mut turn = Turn::default();
            let outcome = within(domain, through, async {
                loop {
                    let status = core.runtime.status(history).await?;
                    watch.observe(domain, &status)?;
                    if status.leader != Some(local) {
                        if status.applied.index == status.committed {
                            return Ok(());
                        }
                        pause(domain, through.min(watch.through())).await?;
                        continue;
                    }
                    turn.needed = true;
                    if status.ready && status.applied.index == status.committed {
                        break;
                    }
                    pause(domain, through.min(watch.through())).await?;
                }
                let replication = core.runtime.replication_status(history).await?;
                for member in replication
                    .members
                    .iter()
                    .filter(|p| {
                        p.voter
                            && p.member.get() != local
                            && p.recent_active
                            && p.matched >= replication.committed
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
                                expected_term: replication.term,
                                expected_revision: replication.membership_revision,
                            },
                        )
                        .await
                    {
                        Ok(()) => {
                            // Admission remains a separate observation if this
                            // deadline expires before a successor is visible.
                            turn.accepted = true;
                            watch.reset(domain)?;
                            within(domain, through, async {
                                loop {
                                    let status = core.runtime.status(history).await?;
                                    watch.observe(domain, &status)?;
                                    if status.term >= replication.term
                                        && status.leader.is_some_and(|id| id != local)
                                    {
                                        turn.released = true;
                                        // Observing a successor can precede local
                                        // commit and apply of its fresh-term entry.
                                        if status.applied.term == status.term
                                            && status.applied.index == status.committed
                                        {
                                            return Ok(());
                                        }
                                    }
                                    pause(domain, through.min(watch.through())).await?;
                                }
                            })
                            .await?;
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

/// A group may use its fair share of the node drain budget only while it shows
/// observable progress. Once leadership, readiness, commit or apply stop
/// changing for this window, the turn gives up instead of holding shutdown for
/// the caller's whole deadline. A progressing handoff can still consume its
/// fair share; a peer that is itself stopping cannot stall every later group.
const PROGRESS_MS: u64 = 1_000;

struct Progress {
    observed: Option<(Option<u64>, bool, u64, u64)>,
    through: u64,
}

impl Progress {
    fn new(now: u64) -> Self {
        Self {
            observed: None,
            through: now.saturating_add(PROGRESS_MS),
        }
    }

    fn observe(
        &mut self,
        domain: &crate::asyncrt::DomainToken,
        status: &crate::raft::cell_groups::CellGroupStatus,
    ) -> Result<(), RuntimeError> {
        let now = domain.mono_ms().map_err(|_| RuntimeError::Closed)?;
        let observed = (
            status.leader,
            status.ready,
            status.committed.get(),
            status.applied.index.get(),
        );
        if self.observed != Some(observed) {
            self.observed = Some(observed);
            self.through = now.saturating_add(PROGRESS_MS);
        } else if now >= self.through {
            return Err(RuntimeError::DeadlineExceeded);
        }
        Ok(())
    }

    /// Transfer acceptance is progress by itself; give the successor a fresh
    /// no-progress window without extending the caller's overall deadline.
    fn reset(&mut self, domain: &crate::asyncrt::DomainToken) -> Result<(), RuntimeError> {
        let now = domain.mono_ms().map_err(|_| RuntimeError::Closed)?;
        self.observed = None;
        self.through = now.saturating_add(PROGRESS_MS);
        Ok(())
    }

    fn through(&self) -> u64 {
        self.through
    }
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
