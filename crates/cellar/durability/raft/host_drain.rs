// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A single owned, bounded handoff turn after public admission is sealed and
//! the Node has drained accepted application work. Consensus still runs while
//! each candidate is checked. Missing inspection permission means no handoff.
use super::*;
use crate::raft::inspection::{ReplicaOccupancy, RESPONSE_RESERVATION};
use crate::raft::operations::RaftDrainReport;
use std::ops::Bound::{Excluded, Unbounded};

pub(super) struct Drain {
    request: Request,
    cursor: Option<GroupScope>,
    waiting: Option<(GroupScope, u64)>,
    busy: bool,
    report: RaftDrainReport,
}

#[derive(Default)]
struct Outcome {
    attempts: usize,
    accepted: usize,
    released: bool,
    unavailable: bool,
}

pub(super) struct Completed {
    scope: GroupScope,
    outcome: Outcome,
    _memory: Permit,
}

impl Driver {
    pub(super) fn start_node_drain(&mut self, mut request: Request) {
        if !self.local.admission.is_draining()
            || self.closing.load(Ordering::Acquire)
            || self.drain.is_some()
        {
            reply(&mut request, Err(CommandFailure::Unavailable));
            return;
        }
        self.drain = Some(Drain {
            request,
            cursor: None,
            waiting: None,
            busy: false,
            report: RaftDrainReport::default(),
        });
    }

    pub(super) fn pump_node_drain(&mut self) {
        let Some(drain) = self.drain.as_mut() else {
            return;
        };
        if drain.busy {
            return;
        }
        let now = match self.log.domain().mono_ms() {
            Ok(now) => now,
            Err(_) => {
                if let Some(mut drain) = self.drain.take() {
                    reply(&mut drain.request, Err(CommandFailure::Unavailable));
                }
                return;
            }
        };
        let expired = now >= drain.request.control.deadline() || drain.request.control.cancelled();
        if expired {
            let Some(mut drain) = self.drain.take() else {
                return;
            };
            drain.report.deadline_elapsed = true;
            drain.report.unavailable_groups += usize::from(drain.waiting.is_some());
            reply(&mut drain.request, Ok(Response::Drained(drain.report)));
            return;
        }
        let (scope, deadline) = if let Some(waiting) = drain.waiting.take() {
            waiting
        } else {
            let next = self
                .cells
                .range((drain.cursor.as_ref().map_or(Unbounded, Excluded), Unbounded))
                .next();
            let Some((scope, _)) = next else {
                let Some(mut drain) = self.drain.take() else {
                    return;
                };
                reply(&mut drain.request, Ok(Response::Drained(drain.report)));
                return;
            };
            let scope = scope.clone();
            drain.cursor = Some(scope.clone());
            drain.report.groups_examined += 1;
            // Waiting for admitted SQL/apply work and inspecting a successor
            // share one turn. A busy group cannot renew its shutdown budget.
            let deadline = drain
                .request
                .control
                .deadline()
                .min(now.saturating_add(1000));
            (scope, deadline)
        };
        let Some(cell) = self.cells.get(&scope) else {
            drain.report.unavailable_groups += 1;
            return;
        };
        if cell.failed || cell.retired || now >= deadline {
            drain.report.unavailable_groups += 1;
            return;
        }
        if cell.pending.is_some()
            || cell.sql_busy
            || cell.configuration.is_some()
            || cell.incoming.is_some()
            || cell.apply.is_some()
            || cell.publication.is_some()
        {
            // Completion jobs and the existing host poll wake this turn.
            // In particular, a brief archive capture is not a failed group.
            drain.waiting = Some((scope, deadline));
            return;
        }
        let Some(memory) = self.control_budget.acquire(2 * RESPONSE_RESERVATION) else {
            drain.report.unavailable_groups += 1;
            return;
        };
        let group = cell.group.clone();
        let candidates = (|| -> Result<Vec<_>, RuntimeError> {
            let cache = group.read()?;
            // Work and metadata stay bounded even in an unusually large
            // configuration. This best-effort turn inspects at most 8 voters.
            let mut result = Vec::new();
            for &id in cache
                .conf_state
                .voters
                .iter()
                .filter(|&&id| id != group.identity().member.get())
                .take(8)
            {
                let member = MemberId::new(id)
                    .map_err(|_| RuntimeError::Invalid("invalid voter identity".into()))?;
                let joined = cache.membership.joined(member)?;
                let node = joined
                    .as_ref()
                    .map(|(binding, _)| binding.node)
                    .or_else(|| {
                        cell.birth
                            .voters()
                            .iter()
                            .find(|initial| initial.member == member)
                            .map(|initial| initial.node)
                    });
                let Some(peer) = node.and_then(|node| self.peers.get(&node)) else {
                    continue;
                };
                if joined.is_some_and(|(binding, _)| binding.disk != peer.disk) {
                    continue;
                }
                result.push((member, peer.clone()));
            }
            Ok(result)
        })();
        let Ok(candidates) = candidates else {
            drain.report.unavailable_groups += 1;
            return;
        };
        let schema = cell.schema_digest;
        let birth = cell.birth.digest();
        let runtime = self.runtime.clone();
        let transport = self.transport.clone();
        let domain = self.log.domain();
        drain.busy = true;
        self.jobs.push(Box::pin(async move {
            let mut outcome = Outcome::default();
            let completed = {
                let work = Handoff {runtime:&runtime,transport:&transport,domain:&domain,group:&group,candidates:&candidates,schema,birth,deadline}.run(&mut outcome);
                match domain.sleep_until(deadline) {
                    Ok(timer) => crate::asyncrt::select_biased! { "the original drain turn deadline precedes a late handoff";
                        _=timer=>false,
                        result=work=>result.is_ok(),
                    },
                    Err(_)=>false,
                }
            };
            outcome.unavailable |= !completed;
            Job::Drained(Box::new(Completed {scope,outcome,_memory:memory}))
        }));
    }

    pub(super) fn finish_node_drain(&mut self, completed: Completed) {
        let Some(drain) = self.drain.as_mut() else {
            return;
        };
        if !drain.busy || drain.cursor.as_ref() != Some(&completed.scope) {
            return;
        }
        drain.busy = false;
        drain.report.handoff_attempts += completed.outcome.attempts;
        drain.report.handoffs_accepted += completed.outcome.accepted;
        drain.report.leadership_released += usize::from(completed.outcome.released);
        drain.report.unavailable_groups += usize::from(completed.outcome.unavailable);
    }
}

// References keep one small turn under the driver's reservation. All transport
// work is owned by the existing transport; a timed-out waiter grants no result.
struct Handoff<'a> {
    runtime: &'a RaftRuntime,
    transport: &'a RaftTransport,
    domain: &'a crate::asyncrt::DomainToken,
    group: &'a GroupLog,
    candidates: &'a [(MemberId, RaftPeerNode)],
    schema: [u8; 32],
    birth: [u8; 32],
    deadline: u64,
}

impl Handoff<'_> {
    async fn run(self, outcome: &mut Outcome) -> Result<(), RuntimeError> {
        let Self {
            runtime,
            transport,
            domain,
            group,
            candidates,
            schema,
            birth,
            deadline,
        } = self;
        let scope = &group.identity().scope;
        let status = runtime.status(scope).await?;
        if status.replica != *group.identity() {
            return Err(RuntimeError::NotReady);
        }
        if !status.is_leader {
            return Ok(());
        }
        let progress = runtime.replication_status(scope).await?;
        if progress.replica != status.replica
            || progress.term != status.term
            || progress.membership_revision != status.membership_revision
        {
            return Err(RuntimeError::NotReady);
        }
        for (member, peer) in candidates {
            if !progress.members.iter().any(|item| {
                item.member == *member
                    && item.voter
                    && item.recent_active
                    && item.matched >= progress.committed
            }) {
                continue;
            }
            let received = match transport.inspect_peer(
                scope.clone(),
                schema,
                birth,
                peer.clone(),
                deadline,
            ) {
                Ok(delivery) => delivery.receive().await,
                Err(error) => Err(error),
            };
            let Ok(received) = received else {
                continue;
            };
            let view = &received.snapshot.view;
            let Some(remote) = view.member.as_ref() else {
                continue;
            };
            if view.scope != *scope
                || view.schema_digest != schema
                || view.birth_digest != birth
                || view.node.identity
                    != (NodeIdentity {
                        node: peer.node,
                        disk: peer.disk,
                        process: peer.process,
                    })
                || view.node.profile.draining
                || view.occupancy != ReplicaOccupancy::Member(*member)
                || remote.member != *member
                || remote.term != status.term
                || remote.membership_revision != status.membership_revision
                || remote.applied.index < progress.committed
                || remote
                    .published
                    .is_none_or(|position| position.index < progress.committed)
                    && group.read()?.applied.image.is_some()
            {
                continue;
            }
            outcome.attempts += 1;
            if runtime
                .transfer_leadership(
                    scope,
                    LeadershipTransfer {
                        target: *member,
                        expected_term: status.term,
                        expected_revision: status.membership_revision,
                    },
                )
                .await
                .is_err()
            {
                continue;
            }
            outcome.accepted += 1;
            loop {
                let current = runtime.status(scope).await?;
                if current.replica != status.replica {
                    return Err(RuntimeError::NotReady);
                }
                if !current.is_leader && current.term >= status.term {
                    outcome.released = true;
                    return Ok(());
                }
                domain
                    .sleep_until(
                        domain
                            .mono_ms()
                            .map_err(|_| RuntimeError::Closed)?
                            .saturating_add(10),
                    )
                    .map_err(|_| RuntimeError::Closed)?
                    .await;
            }
        }
        // There may simply be no compatible, published, authorized destination.
        // The final close report still exposes retained local archive obligations.
        outcome.unavailable = true;
        Ok(())
    }
}
