// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded invocation-owned reads. No pending read retains peer/apply capacity.

use super::super::read_authority::ReadSelection;
use super::super::seed_authority::{CellGroupSeedAuthority, SeedSelection};
use super::*;
use cellar_logic::{
    cell_group::CellHistory,
    consensus::{LogIndex, Term},
};
use std::{collections::BTreeMap, sync::Weak};

const PREFIX: &[u8] = b"cellar-cell-read-1\0";
const CELL_FORWARD_PREFIX: &[u8] = b"cellar-cell-forward-read-1\0";
const MEMBERSHIP_PREFIX: &[u8] = b"cellar-group-membership-read-1\0";
const FORWARDED_RETRY_MS: u64 = 100;

pub(in crate::raft::cell_groups) fn reserved_context(context: &[u8]) -> bool {
    context.starts_with(PREFIX)
        || context.starts_with(MEMBERSHIP_PREFIX)
        || context.starts_with(CELL_FORWARD_PREFIX)
}

/// Forwarded metadata reads have a separate namespace from local execution
/// authority. Bind the exact origin member as well as this invocation's nonce.
pub(in crate::raft::cell_groups) fn membership_origin(context: &[u8]) -> Option<u64> {
    forwarded_origin(context, MEMBERSHIP_PREFIX)
}

pub(in crate::raft::cell_groups) fn cell_origin(context: &[u8]) -> Option<u64> {
    forwarded_origin(context, CELL_FORWARD_PREFIX)
}

fn forwarded_origin(context: &[u8], prefix: &[u8]) -> Option<u64> {
    let rest = context.strip_prefix(prefix)?;
    if rest.len() != 32 {
        return None;
    }
    let member = u64::from_be_bytes(rest[..8].try_into().ok()?);
    let sequence = u64::from_be_bytes(rest[24..].try_into().ok()?);
    cellar_logic::consensus::MemberId::new(member).ok()?;
    (sequence > 0).then_some(member)
}

pub(super) struct ReadRequest {
    pub kind: ReadKind,
    pub deadline: u64,
    pub reservation: Permit,
}

pub(super) enum ReadKind {
    Cell(CellHistory, CellReadConsistency),
    Seed(cellar_logic::consensus::ReplicaIdentity<GroupHistory>),
    Membership {
        minimum: LogIndex,
        mode: MembershipRead,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum MembershipRead {
    Ordinary,
    ParkAdmission,
    Wake(cellar_logic::cell_group::ParkGeneration),
}

impl ReadKind {
    pub(super) fn membership_mode(&self) -> MembershipRead {
        match self {
            Self::Membership { mode, .. } => *mode,
            _ => MembershipRead::Ordinary,
        }
    }

    pub fn reservation_bytes(&self) -> usize {
        match self {
            Self::Membership { .. } => super::super::membership_observation::MEMBERSHIP_BYTES,
            _ => 4096,
        }
    }

    pub fn linearizable(&self) -> bool {
        matches!(self, Self::Seed(_)) || self.forwards_read_index()
    }

    fn forwards_read_index(&self) -> bool {
        matches!(
            self,
            Self::Membership { .. } | Self::Cell(_, CellReadConsistency::Linearizable)
        )
    }
}

pub(super) enum Selection {
    Cell(ReadSelection),
    Seed(SeedSelection),
    Membership(super::super::membership_observation::MembershipSelection),
}

struct Pending {
    history: GroupHistory,
    request: ReadRequest,
    reply: oneshot::Sender<Result<Response, RuntimeError>>,
    group: Permit,
    term: Option<Term>,
    barrier: Option<LogIndex>,
    retry_at: Option<u64>,
}

pub(super) struct Reads {
    pending: BTreeMap<Vec<u8>, Pending>,
    groups: BTreeMap<GroupHistory, Weak<Budget>>,
    nonce: [u8; 16],
    sequence: u64,
    max_bytes: usize,
    max_items: usize,
}

impl Reads {
    pub fn new(options: &RaftRuntimeOptions) -> Self {
        use rand::RngCore as _;
        let mut nonce = [0; 16];
        crate::asyncrt::rng("cell-group-read-context").fill_bytes(&mut nonce);
        Self {
            pending: BTreeMap::new(),
            groups: BTreeMap::new(),
            nonce,
            sequence: 0,
            max_bytes: options.max_read_pin_bytes,
            max_items: (options.max_pending_reads / 8).clamp(1, 32),
        }
    }

    pub fn start(
        &mut self,
        history: GroupHistory,
        owner: &mut CellGroupOwner,
        request: ReadRequest,
        reply: oneshot::Sender<Result<Response, RuntimeError>>,
        now: u64,
    ) {
        if reply.is_closed() {
            return;
        }
        let begin = (|| {
            if now >= request.deadline {
                return Err(RuntimeError::DeadlineExceeded);
            }
            match &request.kind {
                ReadKind::Cell(cell, consistency) => {
                    if !history.same_namespace(cell) {
                        return Err(RuntimeError::Invalid("cell read namespace".into()));
                    }
                    if let CellReadConsistency::AtLeast(minimum) = consistency {
                        if &minimum.history != cell {
                            return Err(RuntimeError::Invalid("cell read version history".into()));
                        }
                    }
                }
                ReadKind::Seed(target) => owner.check_seed_target(target)?,
                ReadKind::Membership { .. } => (),
            }
            // A pending recovery is an admission refusal, not a read barrier
            // that may occupy the ordinary wait queue until its deadline.
            if let MembershipRead::Wake(generation) = request.kind.membership_mode() {
                owner.check_park_wake(generation)?;
            } else {
                owner.require_application_active()?;
            }
            self.groups.retain(|_, budget| budget.strong_count() != 0);
            let budget = self
                .groups
                .get(&history)
                .and_then(Weak::upgrade)
                .unwrap_or_else(|| Budget::new(self.max_bytes, self.max_items));
            let group = budget
                .acquire(request.kind.reservation_bytes())
                .ok_or(RuntimeError::Overloaded)?;
            self.groups.insert(history, Arc::downgrade(&budget));
            self.sequence = self
                .sequence
                .checked_add(1)
                .ok_or(RuntimeError::Overloaded)?;
            let membership = matches!(request.kind, ReadKind::Membership { .. });
            let cell_read = matches!(
                request.kind,
                ReadKind::Cell(_, CellReadConsistency::Linearizable)
            );
            let mut context = if membership || cell_read {
                let mut context = if membership {
                    MEMBERSHIP_PREFIX
                } else {
                    CELL_FORWARD_PREFIX
                }
                .to_vec();
                context.extend_from_slice(&owner.membership_read_origin().to_be_bytes());
                context
            } else {
                PREFIX.to_vec()
            };
            context.extend_from_slice(&self.nonce);
            context.extend_from_slice(&self.sequence.to_be_bytes());
            let term = if request.kind.linearizable() {
                let status = owner.status()?;
                if matches!(request.kind, ReadKind::Membership { minimum, .. } if status.applied.index < minimum)
                {
                    // The configuration proposal is only admitted. Start this
                    // invocation's quorum barrier after its durable apply floor.
                    None
                } else {
                    if membership {
                        match owner.read_membership_index(context.clone()) {
                            Ok(()) => Some(status.term),
                            Err(RuntimeError::NotReady) => None,
                            Err(error) => return Err(error),
                        }
                    } else if cell_read {
                        match owner.read_forwarded_cell_index(context.clone()) {
                            Ok(()) => Some(status.term),
                            Err(RuntimeError::NotReady) => None,
                            Err(error) => return Err(error),
                        }
                    } else {
                        if !status.ready {
                            return Err(RuntimeError::NotReady);
                        }
                        owner.read_cell_index(context.clone())?;
                        Some(status.term)
                    }
                }
            } else {
                None
            };
            Ok((context, group, term))
        })();
        match begin {
            Ok((context, group, term)) => {
                let retry_at = request
                    .kind
                    .forwards_read_index()
                    .then(|| now.saturating_add(FORWARDED_RETRY_MS));
                self.pending.insert(
                    context,
                    Pending {
                        history,
                        request,
                        reply,
                        group,
                        term,
                        barrier: None,
                        retry_at,
                    },
                );
            }
            Err(error) => {
                let _ = reply.send(Err(error));
            }
        }
    }

    pub fn observe(&mut self, history: GroupHistory, term: Term, index: LogIndex, context: &[u8]) {
        if let Some(pending) = self.pending.get_mut(context) {
            if pending.history == history && pending.term == Some(term) {
                pending.barrier = Some(pending.barrier.map_or(index, |seen| seen.max(index)));
            }
        }
    }

    /// Pending reads and issued read pins both retain this group's permit.
    pub(super) fn busy(&self, history: GroupHistory) -> bool {
        self.groups
            .get(&history)
            .is_some_and(|budget| budget.strong_count() != 0)
    }

    pub fn deadline(&self) -> Option<u64> {
        self.pending
            .values()
            .map(|pending| match (pending.barrier, pending.retry_at) {
                (None, Some(retry)) => pending.request.deadline.min(retry),
                _ => pending.request.deadline,
            })
            .min()
    }

    pub fn progress(
        &mut self,
        now: u64,
        domain: &crate::asyncrt::DomainToken,
        mut select: impl FnMut(
            GroupHistory,
            &ReadRequest,
            &mut Option<Term>,
            &mut Option<LogIndex>,
            &[u8],
            bool,
        ) -> Result<Selection, RuntimeError>,
    ) -> bool {
        let mut made = false;
        self.pending.retain(|_, pending| {
            // Taking a sender requires removing the entry; collect below instead.
            if pending.reply.is_closed() {
                made = true;
                false
            } else {
                true
            }
        });
        let mut finished = Vec::new();
        for (context, pending) in &mut self.pending {
            let before = (pending.term, pending.barrier);
            let result = if now >= pending.request.deadline {
                Err(RuntimeError::DeadlineExceeded)
            } else {
                let retry =
                    pending.barrier.is_none() && pending.retry_at.is_some_and(|retry| now >= retry);
                if retry {
                    pending.retry_at = Some(now.saturating_add(FORWARDED_RETRY_MS));
                    // A retransmitted context may have produced another Ready.
                    made = true;
                }
                select(
                    pending.history,
                    &pending.request,
                    &mut pending.term,
                    &mut pending.barrier,
                    context,
                    retry,
                )
            };
            // A newly issued barrier must drive the owner's Ready work even
            // when selection still waits. Waiting without a state change sleeps.
            made |= before != (pending.term, pending.barrier);
            if !matches!(result, Err(RuntimeError::NotReady)) {
                finished.push((context.clone(), result));
            }
        }
        for (context, result) in finished {
            let pending = self
                .pending
                .remove(&context)
                .expect("selected pending read");
            let result = result.and_then(|selection| match selection {
                Selection::Cell(selection) => CellReadAuthority::new(
                    selection,
                    pending.request.deadline,
                    pending.request.reservation,
                    pending.group,
                )
                .map(|authority| Response::Read(Box::new(authority))),
                Selection::Seed(selection) => CellGroupSeedAuthority::new(
                    selection,
                    pending.request.deadline,
                    domain.clone(),
                    pending.request.reservation,
                    pending.group,
                )
                .map(|authority| Response::Seed(Box::new(authority))),
                Selection::Membership(selection) => CellGroupQuorumMembership::new(
                    selection,
                    pending.request.reservation,
                    pending.group,
                )
                .map(|observation| Response::Membership(Box::new(observation))),
            });
            let _ = pending.reply.send(result);
            made = true;
        }
        made
    }
}
