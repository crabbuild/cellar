// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Replay committed configuration transitions without starting a Raft node or
//! granting any membership authority. The exact final state must match the tip.

use super::{manifest, ArchiveError, ConfState, Entry, EntryType, Position};
use crate::raft::member_state::{GroupMembership, MAX_CHANGE_BYTES};
use ::raft::{
    prelude::{ConfChange, ConfChangeSingle, ConfChangeV2},
    restore_configuration, Changer, ProgressTracker,
};
use cellar_logic::consensus::LogIndex;
use protobuf::Message as _;

#[cfg(test)]
#[path = "configuration_tests.rs"]
mod tests;

pub(super) struct Configuration {
    position: Position,
    conf: ConfState,
    membership: GroupMembership,
}

impl Configuration {
    pub fn new(
        position: Position,
        conf: &[u8],
        membership: GroupMembership,
    ) -> Result<Self, ArchiveError> {
        let conf = manifest::decode_conf(conf)?;
        membership
            .validate(&conf, position.index)
            .map_err(|_| ArchiveError::Corrupt("archive configuration base bindings"))?;
        Ok(Self {
            position,
            conf,
            membership,
        })
    }

    pub fn step(&mut self, entry: &Entry) -> Result<(), ArchiveError> {
        if self.position.index.checked_add(1) != Some(entry.index)
            || entry.term < self.position.term
            || entry.get_unknown_fields().iter().next().is_some()
        {
            return Err(ArchiveError::Corrupt("archive entry coverage/domain"));
        }
        if entry.get_entry_type() != EntryType::EntryNormal {
            let change = decode_change(entry)?;
            self.membership
                .admit(&change, &self.conf)
                .map_err(|_| ArchiveError::Corrupt("archive configuration intent"))?;
            // Use the same configuration changer as the running Raft node.
            // Progress numbers are irrelevant to this pure membership replay;
            // no elections, clocks, transport or external I/O are constructed.
            let mut tracker = ProgressTracker::new(1);
            restore_configuration(&mut tracker, 1, &self.conf)
                .map_err(|_| ArchiveError::Corrupt("archive configuration base"))?;
            let mut changer = Changer::new(&tracker);
            let changed = if change.leave_joint() {
                changer.leave_joint()
            } else if let Some(auto_leave) = change.enter_joint() {
                changer.enter_joint(auto_leave, &change.changes)
            } else {
                changer.simple(&change.changes)
            };
            let (conf, progress) =
                changed.map_err(|_| ArchiveError::Corrupt("archive configuration transition"))?;
            tracker.apply_conf(conf, progress, 1);
            let conf = tracker.conf().to_conf_state();
            let membership = self
                .membership
                .applied(&change, &self.conf, &conf, LogIndex::new(entry.index))
                .map_err(|_| ArchiveError::Corrupt("archive configuration result bindings"))?;
            self.conf = conf;
            self.membership = membership;
        }
        self.position = Position {
            index: entry.index,
            term: entry.term,
        };
        Ok(())
    }

    pub fn matches(
        &self,
        position: Position,
        conf: &[u8],
        membership: &GroupMembership,
    ) -> Result<bool, ArchiveError> {
        Ok(self.position == position
            && manifest::encode_conf(&self.conf)? == conf
            && self.membership == *membership)
    }
}

fn decode_change(entry: &Entry) -> Result<ConfChangeV2, ArchiveError> {
    if entry.data.len() > MAX_CHANGE_BYTES {
        return Err(ArchiveError::Limit);
    }
    let invalid = || ArchiveError::Corrupt("archive configuration protobuf");
    let change = match entry.get_entry_type() {
        EntryType::EntryConfChangeV2 => {
            ConfChangeV2::parse_from_bytes(&entry.data).map_err(|_| invalid())?
        }
        EntryType::EntryConfChange => {
            let change = ConfChange::parse_from_bytes(&entry.data).map_err(|_| invalid())?;
            if change.get_unknown_fields().iter().next().is_some() {
                return Err(invalid());
            }
            ConfChangeV2 {
                changes: vec![ConfChangeSingle {
                    node_id: change.node_id,
                    change_type: change.get_change_type(),
                    ..Default::default()
                }]
                .into(),
                context: change.context,
                ..Default::default()
            }
        }
        EntryType::EntryNormal => return Err(invalid()),
    };
    if change.get_unknown_fields().iter().next().is_some() {
        return Err(invalid());
    }
    Ok(change)
}
