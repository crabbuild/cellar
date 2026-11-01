// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Reserve before RawNode can append; retain exact entries through persistence.
use super::log_retention::LogReservation;
use crate::raft::{
    cache::{ConsensusStorage, GroupLog},
    completion::{entry_digest, RuntimeError},
};
use ::raft::{prelude::*, RawNode, StateRole};
use cellar_logic::cell_group::GroupHistory;
use protobuf::Message as _;
use std::{collections::BTreeMap, sync::Arc};

#[derive(Default)]
pub(super) struct LogAdmission {
    entries: BTreeMap<u64, ([u8; 32], Arc<LogReservation>)>,
    automatic_leave: Option<Arc<LogReservation>>,
}

#[derive(Default)]
pub(super) struct Prospective {
    entries: BTreeMap<u64, ([u8; 32], Arc<LogReservation>)>,
    control: Option<Arc<LogReservation>>,
    automatic_leave: Option<Arc<LogReservation>>,
}

fn retention(
    group: &GroupLog<GroupHistory>,
) -> Result<&Arc<super::log_retention::GroupLogRetention>, RuntimeError> {
    group
        .log_retention
        .as_ref()
        .ok_or_else(|| RuntimeError::Protocol("missing group log admission".into()))
}

impl LogAdmission {
    pub(super) fn prepare(
        &self,
        group: &GroupLog<GroupHistory>,
        entries: &[Entry],
        supplied: Option<&Arc<LogReservation>>,
    ) -> Result<Prospective, RuntimeError> {
        let mut next = Prospective::default();
        if supplied.is_some() && entries.len() != 1 {
            return Err(RuntimeError::Invalid(
                "prepared log reservation shape".into(),
            ));
        }
        let cache = group.read()?;
        for entry in entries {
            let digest = entry_digest(entry)?;
            if self
                .entries
                .get(&entry.index)
                .is_some_and(|(old, _)| old == &digest)
            {
                continue;
            }
            if cache.entries.get(&entry.index) == Some(entry) {
                continue;
            }
            let lease = if let Some(lease) = supplied {
                lease.bind(entry)?;
                lease.validate(retention(group)?, entry)?;
                lease.clone()
            } else {
                retention(group)?
                    .reserve_entry(entry)
                    .map_err(admission_error)?
            };
            next.entries.insert(entry.index, (digest, lease));
        }
        Ok(next)
    }

    pub(super) fn election(
        &self,
        group: &GroupLog<GroupHistory>,
        node: &RawNode<ConsensusStorage>,
        mut next: Prospective,
    ) -> Result<Prospective, RuntimeError> {
        next.control = Some(
            retention(group)?
                .reserve(1024, true)
                .map_err(admission_error)?,
        );
        if self.automatic_leave.is_none() && needs_automatic_leave(group, node)? {
            next.automatic_leave = Some(
                retention(group)?
                    .reserve(1024, true)
                    .map_err(admission_error)?,
            );
        }
        Ok(next)
    }

    pub(super) fn configuration(
        &self,
        group: &GroupLog<GroupHistory>,
        mut next: Prospective,
        auto_leave: bool,
    ) -> Result<Prospective, RuntimeError> {
        if auto_leave && self.automatic_leave.is_none() {
            next.automatic_leave = Some(
                retention(group)?
                    .reserve(1024, true)
                    .map_err(admission_error)?,
            );
        }
        Ok(next)
    }

    /// A promised automatic entry must never seek admission after collected
    /// apply work has already selected its new durable configuration.
    pub(super) fn applying(&mut self, automatic: bool) -> Result<Prospective, RuntimeError> {
        let mut next = Prospective::default();
        if automatic {
            next.control = Some(self.automatic_leave.take().ok_or_else(|| {
                RuntimeError::Protocol("unreserved automatic joint leave".into())
            })?);
        }
        Ok(next)
    }

    pub(super) fn finish(
        &mut self,
        group: &GroupLog<GroupHistory>,
        node: &RawNode<ConsensusStorage>,
        mut next: Prospective,
    ) -> Result<(), RuntimeError> {
        let mut entries = BTreeMap::new();
        let cache = group.read()?;
        for entry in node.raft.raft_log.unstable_entries() {
            let digest = entry_digest(entry)?;
            if cache.entries.get(&entry.index) == Some(entry) {
                continue;
            }
            let old = self
                .entries
                .get(&entry.index)
                .filter(|(old, _)| old == &digest);
            let fresh = next
                .entries
                .remove(&entry.index)
                .filter(|(new, _)| new == &digest);
            let lease = if let Some((_, lease)) = old {
                lease.clone()
            } else if let Some((_, lease)) = fresh {
                lease
            } else if entry.data.is_empty() && entry.context.is_empty() {
                let lease = next.control.take().ok_or_else(|| {
                    RuntimeError::Protocol("unreserved Raft control entry".into())
                })?;
                lease.bind(entry)?;
                lease
            } else {
                return Err(RuntimeError::Protocol("unreserved Raft entry".into()));
            };
            entries.insert(entry.index, (digest, lease));
        }
        drop(cache);
        self.entries = entries;
        if node.raft.state == StateRole::Leader && needs_automatic_leave(group, node)? {
            if let Some(lease) = next.automatic_leave {
                self.automatic_leave = Some(lease);
            }
        } else {
            self.automatic_leave = None;
        }
        Ok(())
    }

    pub(super) fn persistence(
        &self,
        entries: &[Entry],
    ) -> Result<BTreeMap<u64, Arc<LogReservation>>, RuntimeError> {
        let mut leases = BTreeMap::new();
        for entry in entries {
            if let Some((digest, lease)) = self.entries.get(&entry.index) {
                if *digest != entry_digest(entry)? {
                    return Err(RuntimeError::Protocol("changed reserved entry".into()));
                }
                leases.insert(entry.index, lease.clone());
            }
        }
        Ok(leases)
    }
}

fn needs_automatic_leave(
    group: &GroupLog<GroupHistory>,
    node: &RawNode<ConsensusStorage>,
) -> Result<bool, RuntimeError> {
    if *node.raft.prs().conf().auto_leave() {
        return Ok(true);
    }
    let cache = group.read()?;
    for entry in cache
        .entries
        .values()
        .chain(node.raft.raft_log.unstable_entries())
    {
        if entry.index > node.raft.raft_log.applied
            && entry.get_entry_type() == EntryType::EntryConfChangeV2
        {
            let change = ConfChangeV2::parse_from_bytes(&entry.data)
                .map_err(|e| RuntimeError::Invalid(e.to_string()))?;
            if change.enter_joint() == Some(true) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

fn admission_error(error: crate::raft::storage::LogError) -> RuntimeError {
    match error {
        crate::raft::storage::LogError::Overloaded => RuntimeError::Overloaded,
        other => RuntimeError::Storage(other),
    }
}
