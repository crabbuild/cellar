// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Group-bound configuration entries and deterministic, authority-free replay.
//! Raft progress changes only after the complete configuration/root write syncs.
use super::GroupManifest;
use crate::raft::{
    member_state::{GroupMembership, JoiningMember},
    storage::LogError,
};
use cellar_logic::{cell_group::GroupRoute, consensus::LogIndex};
use protobuf::Message as _;
use raft::{
    prelude::{ConfChangeV2, ConfState, Entry, EntryType},
    restore_configuration, Changer, ProgressTracker,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const MAX_CHANGE_BYTES: usize = 40 * 1024;
const MAX_CONTEXT_BYTES: usize = 36 * 1024;

fn invalid() -> LogError {
    LogError::Invalid("cell-group configuration binding or transition".into())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    domain: String,
    route: [u8; 32],
    intent: String,
}

fn binding(route: &GroupRoute) -> Result<[u8; 32], LogError> {
    // Canonical marker-1 genesis projects the complete route, including range,
    // epoch, birth, schema and all history IDs, without a mutable applied cut.
    let genesis = GroupManifest::genesis(route.clone()).map_err(|_| invalid())?;
    Ok(Sha256::digest(serde_json::to_vec(&genesis).map_err(|_| invalid())?).into())
}

/// An explicit proposal bound to one route and observed membership revision.
/// Losing the admission reply requires inspecting committed membership; callers
/// must not manufacture a new revision to blindly repeat an ambiguous change.
#[derive(Clone, Debug)]
pub struct CellGroupMembershipChange {
    pub(super) change: ConfChangeV2,
}

impl CellGroupMembershipChange {
    pub fn new(
        route: &GroupRoute,
        membership: &GroupMembership,
        mut change: ConfChangeV2,
        joins: &[JoiningMember],
    ) -> Result<Self, LogError> {
        if !change.context.is_empty() || change.changes.len() > 32 {
            return Err(invalid());
        }
        membership.bind_change(&mut change, joins)?;
        let intent = Intent {
            domain: "cellar-cell-group-membership-1".into(),
            route: binding(route)?,
            intent: String::from_utf8(change.context.to_vec()).map_err(|_| invalid())?,
        };
        change.context = serde_json::to_vec(&intent).map_err(|_| invalid())?.into();
        validate(&change, route)?;
        Ok(Self { change })
    }

    pub(crate) fn bytes(&self) -> usize {
        self.change.compute_size() as usize + 4096
    }
}

/// Bound repeated fields before protobuf allocation. Unknown fields and duplicate
/// singular fields refuse; each nested member change has a fixed small bound.
fn scan(bytes: &[u8]) -> Result<(), LogError> {
    fn varint(bytes: &[u8], at: &mut usize) -> Result<u64, LogError> {
        let mut value = 0;
        for shift in (0..70).step_by(7) {
            let byte = *bytes.get(*at).ok_or_else(invalid)?;
            *at += 1;
            if shift == 63 && byte > 1 {
                return Err(invalid());
            }
            value |= u64::from(byte & 127) << shift;
            if byte < 128 {
                return Ok(value);
            }
        }
        Err(invalid())
    }
    if bytes.len() > MAX_CHANGE_BYTES {
        return Err(LogError::Overloaded);
    }
    let (mut at, mut count, mut transition, mut context) = (0, 0, false, false);
    while at < bytes.len() {
        match varint(bytes, &mut at)? {
            8 if !transition => {
                transition = true;
                if varint(bytes, &mut at)? > 2 {
                    return Err(invalid());
                }
            }
            tag @ (18 | 26) => {
                let length = usize::try_from(varint(bytes, &mut at)?).map_err(|_| invalid())?;
                if tag == 18 {
                    count += 1;
                    if count > 32 || length > 32 {
                        return Err(invalid());
                    }
                } else {
                    if context || length > MAX_CONTEXT_BYTES {
                        return Err(invalid());
                    }
                    context = true;
                }
                at = at
                    .checked_add(length)
                    .filter(|end| *end <= bytes.len())
                    .ok_or_else(invalid)?;
            }
            _ => return Err(invalid()),
        }
    }
    Ok(())
}

fn validate(change: &ConfChangeV2, route: &GroupRoute) -> Result<ConfChangeV2, LogError> {
    if change.compute_size() as usize > MAX_CHANGE_BYTES
        || change.changes.len() > 32
        || change.get_unknown_fields().iter().next().is_some()
        || change
            .changes
            .iter()
            .any(|item| item.get_unknown_fields().iter().next().is_some())
    {
        return Err(invalid());
    }
    let mut decoded = change.clone();
    if change.context.is_empty() {
        if !change.changes.is_empty()
            || change.get_transition() != raft::prelude::ConfChangeTransition::Auto
        {
            return Err(invalid());
        }
        return Ok(decoded); // Only a committed auto-leave is accepted by transition().
    }
    if change.context.len() > MAX_CONTEXT_BYTES {
        return Err(invalid());
    }
    let intent: Intent = serde_json::from_slice(&change.context).map_err(|_| invalid())?;
    if intent.domain != "cellar-cell-group-membership-1"
        || intent.route != binding(route)?
        || serde_json::to_vec(&intent).map_err(|_| invalid())? != change.context
    {
        return Err(invalid());
    }
    decoded.context = intent.intent.into_bytes().into();
    Ok(decoded)
}

pub(crate) fn decode(entry: &Entry, route: &GroupRoute) -> Result<ConfChangeV2, LogError> {
    if entry.get_entry_type() != EntryType::EntryConfChangeV2 {
        return Err(invalid());
    }
    scan(&entry.data)?;
    let change = ConfChangeV2::parse_from_bytes(&entry.data).map_err(|_| invalid())?;
    // Reject duplicate nested fields and noncanonical encodings as well as
    // the outer framing checked before allocation. All local writers use this
    // same protobuf serializer, including Raft's empty automatic leave.
    if change.write_to_bytes().map_err(|_| invalid())? != entry.data {
        return Err(invalid());
    }
    validate(&change, route)
}

pub(crate) struct Transition {
    pub conf: ConfState,
    pub membership: GroupMembership,
}

pub(crate) fn transition(
    entry: &Entry,
    route: &GroupRoute,
    before: &ConfState,
    membership: &GroupMembership,
) -> Result<Transition, LogError> {
    let change = decode(entry, route)?;
    if change.context.is_empty() && (before.voters_outgoing.is_empty() || !before.auto_leave) {
        return Err(invalid());
    }
    membership.admit_group(&change, before)?;
    let mut tracker = ProgressTracker::new(1);
    restore_configuration(&mut tracker, 1, before).map_err(|_| invalid())?;
    let mut changer = Changer::new(&tracker);
    let (conf, progress) = if change.leave_joint() {
        changer.leave_joint()
    } else if let Some(auto) = change.enter_joint() {
        changer.enter_joint(auto, &change.changes)
    } else {
        changer.simple(&change.changes)
    }
    .map_err(|_| invalid())?;
    tracker.apply_conf(conf, progress, 1);
    let conf = tracker.conf().to_conf_state();
    let membership = membership.applied(&change, before, &conf, LogIndex::new(entry.index))?;
    Ok(Transition { conf, membership })
}
