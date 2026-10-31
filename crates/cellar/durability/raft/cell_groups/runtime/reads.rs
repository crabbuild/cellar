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

pub(in crate::raft::cell_groups) fn reserved_context(context: &[u8]) -> bool {
    context.starts_with(PREFIX)
}

pub(super) struct ReadRequest {
    pub kind: ReadKind,
    pub deadline: u64,
    pub reservation: Permit,
}

pub(super) enum ReadKind {
    Cell(CellHistory, CellReadConsistency),
    Seed(cellar_logic::consensus::ReplicaIdentity<GroupHistory>),
}

impl ReadKind {
    pub fn linearizable(&self) -> bool {
        matches!(
            self,
            Self::Seed(_) | Self::Cell(_, CellReadConsistency::Linearizable)
        )
    }
}

pub(super) enum Selection {
    Cell(ReadSelection),
    Seed(SeedSelection),
}

struct Pending {
    history: GroupHistory,
    request: ReadRequest,
    reply: oneshot::Sender<Result<Response, RuntimeError>>,
    group: Permit,
    term: Option<Term>,
    barrier: Option<LogIndex>,
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
            }
            self.groups.retain(|_, budget| budget.strong_count() != 0);
            let budget = self
                .groups
                .get(&history)
                .and_then(Weak::upgrade)
                .unwrap_or_else(|| Budget::new(self.max_bytes, self.max_items));
            let group = budget.acquire(4096).ok_or(RuntimeError::Overloaded)?;
            self.groups.insert(history, Arc::downgrade(&budget));
            self.sequence = self
                .sequence
                .checked_add(1)
                .ok_or(RuntimeError::Overloaded)?;
            let mut context = PREFIX.to_vec();
            context.extend_from_slice(&self.nonce);
            context.extend_from_slice(&self.sequence.to_be_bytes());
            let term = if request.kind.linearizable() {
                let status = owner.status()?;
                if !status.ready {
                    return Err(RuntimeError::NotReady);
                }
                owner.read_cell_index(context.clone())?;
                Some(status.term)
            } else {
                None
            };
            Ok((context, group, term))
        })();
        match begin {
            Ok((context, group, term)) => {
                self.pending.insert(
                    context,
                    Pending {
                        history,
                        request,
                        reply,
                        group,
                        term,
                        barrier: None,
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
                pending.barrier = Some(index);
            }
        }
    }

    pub fn deadline(&self) -> Option<u64> {
        self.pending.values().map(|p| p.request.deadline).min()
    }

    pub fn progress(
        &mut self,
        now: u64,
        domain: &crate::asyncrt::DomainToken,
        mut select: impl FnMut(
            GroupHistory,
            &ReadRequest,
            Option<Term>,
            Option<LogIndex>,
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
        for (context, pending) in &self.pending {
            let result = if now >= pending.request.deadline {
                Err(RuntimeError::DeadlineExceeded)
            } else {
                select(
                    pending.history,
                    &pending.request,
                    pending.term,
                    pending.barrier,
                )
            };
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
            });
            let _ = pending.reply.send(result);
            made = true;
        }
        made
    }
}
