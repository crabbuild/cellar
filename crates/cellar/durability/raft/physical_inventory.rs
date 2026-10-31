// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Compact complete physical occupancy, including unattached/retired histories.
//! Immutable identities need no Raft quorum or eager application registration.
use super::cache::{GroupLog, StoredGroup};
use super::host::RaftHostObservation;
use super::storage::LogError;
use super::transport::TransportError;
use cellar_logic::cell_group::GroupHistory;
use cellar_logic::consensus::{GroupScope, MemberId};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

const MAX_GROUPS: usize = 4096;
const PREFIX: usize = 45;
const ENTRY_BYTES: usize = 81;

pub(crate) type PhysicalNodeObservation = (RaftHostObservation, RaftPhysicalInventory);

pub(crate) const RESPONSE_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_BODY: usize =
    super::node_inspection_wire::MAX_BODY + PREFIX + MAX_GROUPS * ENTRY_BYTES;

#[cfg(test)]
#[path = "physical_inventory_tests.rs"]
mod tests;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RaftPhysicalHistory {
    scope: [u8; 32],
    birth: [u8; 32],
    // Byte-aligned scalars keep 4,096 decoded rows inside the existing response
    // reservation. Construction validates member; no untrusted raw constructor.
    member: [u8; 8],
    completed_sequence: [u8; 8],
    in_configuration: bool,
}

impl RaftPhysicalHistory {
    /// This local member is present in the durable incoming/outgoing voter or
    /// learner sets. False is retained occupancy, never physical release proof.
    pub fn in_configuration(&self) -> bool {
        self.in_configuration
    }

    /// Locally durable Complete phase for this birth/history. A fresh quorum
    /// must independently bind the exact permit at this consumed sequence.
    pub fn completed_sequence(&self) -> u64 {
        u64::from_be_bytes(self.completed_sequence)
    }

    pub fn scope_digest(&self) -> [u8; 32] {
        self.scope
    }

    pub fn birth_digest(&self) -> [u8; 32] {
        self.birth
    }

    pub fn member(&self) -> MemberId {
        MemberId::new(u64::from_be_bytes(self.member)).expect("validated physical member")
    }
}

/// A complete bounded occupancy snapshot for the accompanying authenticated
/// node observation. This is not live membership, absence, release or a lease.
/// The caller owns retained response memory after bounded SDK delivery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RaftPhysicalInventory {
    histories: Vec<RaftPhysicalHistory>,
    initializing: Option<[u8; 32]>,
}

impl RaftPhysicalInventory {
    pub fn histories(&self) -> &[RaftPhysicalHistory] {
        &self.histories
    }

    pub fn initializing(&self) -> Option<[u8; 32]> {
        self.initializing
    }

    /// Canonical compact input digest, not a quorum or absence certificate.
    /// The surrounding observation supplies the exact physical boot binding.
    pub fn digest(&self) -> [u8; 32] {
        let mut digest = Sha256::new();
        digest.update(b"cellar-physical-inventory-1");
        digest.update((self.histories.len() as u32).to_be_bytes());
        digest.update([u8::from(self.initializing.is_some())]);
        digest.update(self.initializing.unwrap_or([0; 32]));
        for history in &self.histories {
            digest.update(history.scope);
            digest.update(history.birth);
            digest.update(history.member);
            digest.update(history.completed_sequence);
            digest.update([u8::from(history.in_configuration)]);
        }
        digest.finalize().into()
    }

    pub fn history(
        &self,
        scope: &GroupScope,
    ) -> Result<Option<&RaftPhysicalHistory>, cellar_logic::consensus::CommandFailure> {
        let digest = scope_digest(scope)
            .map_err(|_| cellar_logic::consensus::CommandFailure::CommandConflict)?;
        Ok(self
            .histories
            .binary_search_by_key(&digest, |h| h.scope)
            .ok()
            .map(|index| &self.histories[index]))
    }

    /// Look up a cell-group history without inventing an application cell.
    /// Its digest domain is distinct from the temporary single-cell adapter.
    pub fn group_history(&self, history: &GroupHistory) -> Option<&RaftPhysicalHistory> {
        let digest = group_digest(history);
        self.histories
            .binary_search_by_key(&digest, |h| h.scope)
            .ok()
            .map(|index| &self.histories[index])
    }

    pub(crate) fn validate(&self, view: &RaftHostObservation) -> Result<(), TransportError> {
        super::node_inspection_wire::validate(view)?;
        if self.histories.len() > MAX_GROUPS
            || self.histories.iter().any(|h| {
                h.scope == [0; 32]
                    || h.birth == [0; 32]
                    || MemberId::new(u64::from_be_bytes(h.member)).is_err()
                    || h.completed_sequence() >= u64::MAX - 2
            })
            || self.histories.windows(2).any(|w| w[0].scope >= w[1].scope)
            || self.initializing == Some([0; 32])
        {
            return Err(TransportError::Protocol);
        }
        let extra = usize::from(self.initializing.is_some_and(|digest| {
            self.histories
                .binary_search_by_key(&digest, |h| h.scope)
                .is_err()
        }));
        if self.histories.len() + extra != view.node.groups as usize
            || view.unattached_engine_histories as usize > self.histories.len()
        {
            return Err(TransportError::Protocol);
        }
        Ok(())
    }
}

pub(crate) fn scope_digest(scope: &GroupScope) -> Result<[u8; 32], LogError> {
    scope.validate().map_err(|_| LogError::WrongIdentity)?;
    let bytes = serde_json::to_vec(&crate::application::consensus::protocol::WireScope::from(
        scope,
    ))
    .map_err(|e| LogError::Invalid(e.to_string()))?;
    Ok(Sha256::digest(bytes).into())
}

fn group_digest(history: &GroupHistory) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"cellar-cell-group-physical-history-1\0");
    digest.update(history.cluster.bytes());
    digest.update(history.namespace.bytes());
    digest.update(history.group.bytes());
    digest.update(history.incarnation.bytes());
    digest.finalize().into()
}

pub(crate) fn capture_cells(
    groups: &BTreeMap<GroupHistory, StoredGroup>,
    pending: Option<&GroupHistory>,
) -> Result<RaftPhysicalInventory, LogError> {
    // A group-native owner cannot attach to a legacy application engine.
    if groups.values().any(|group| !group.is_cells()) {
        return Err(LogError::WrongIdentity);
    }
    capture_histories(groups, pending.map(group_digest))
}

pub(crate) fn capture(
    groups: &BTreeMap<GroupHistory, StoredGroup>,
    pending: Option<&GroupScope>,
) -> Result<RaftPhysicalInventory, LogError> {
    if groups.len() > MAX_GROUPS {
        return Err(LogError::Overloaded);
    }
    if let Some(scope) = pending {
        scope.validate().map_err(|_| LogError::WrongIdentity)?;
        if let Some(group) = groups.get(&GroupHistory::from(scope)) {
            if &group.as_cell()?.identity().scope != scope {
                return Err(LogError::WrongIdentity);
            }
        }
    }
    capture_histories(groups, pending.map(scope_digest).transpose()?)
}

fn capture_histories(
    groups: &BTreeMap<GroupHistory, StoredGroup>,
    initializing: Option<[u8; 32]>,
) -> Result<RaftPhysicalInventory, LogError> {
    if groups.len() > MAX_GROUPS {
        return Err(LogError::Overloaded);
    }
    let mut histories = Vec::with_capacity(groups.len());
    for (history, group) in groups {
        if history != &group.history() {
            return Err(LogError::WrongIdentity);
        }
        histories.push(match group {
            StoredGroup::Cell(group) => capture_row(group, scope_digest(&group.identity().scope)?)?,
            StoredGroup::Cells(group) => capture_row(group, group_digest(history))?,
        });
    }
    histories.sort_by_key(|h| h.scope);
    if histories.windows(2).any(|w| w[0].scope == w[1].scope) {
        return Err(LogError::Corrupt(
            "physical inventory digest collision".into(),
        ));
    }
    Ok(RaftPhysicalInventory {
        histories,
        initializing,
    })
}

fn capture_row<S>(group: &GroupLog<S>, scope: [u8; 32]) -> Result<RaftPhysicalHistory, LogError> {
    if group.birth_digest == [0; 32] {
        return Err(LogError::WrongIdentity);
    }
    let cache = group.read()?;
    let completed_sequence = cache
        .membership
        .fleet_status()
        .filter(|status| {
            status.phase == Some(super::member_state::FleetPermitPhase::Complete)
                && status
                    .permit
                    .as_ref()
                    .is_some_and(|permit| permit.sequence() == status.consumed_sequence)
        })
        .map_or(0, |status| status.consumed_sequence);
    Ok(RaftPhysicalHistory {
        scope,
        birth: group.birth_digest,
        member: group.identity().member.get().to_be_bytes(),
        completed_sequence: completed_sequence.to_be_bytes(),
        in_configuration: contains_member(&cache.conf_state, group.identity().member),
    })
}

fn contains_member(configuration: &::raft::prelude::ConfState, member: MemberId) -> bool {
    configuration
        .voters
        .iter()
        .chain(&configuration.voters_outgoing)
        .chain(&configuration.learners)
        .chain(&configuration.learners_next)
        .any(|id| *id == member.get())
}

pub(crate) fn encode(
    view: &RaftHostObservation,
    inventory: &RaftPhysicalInventory,
) -> Result<Vec<u8>, TransportError> {
    inventory.validate(view)?;
    let node = super::node_inspection_wire::encode(view)?;
    let mut bytes =
        Vec::with_capacity(PREFIX + node.len() + inventory.histories.len() * ENTRY_BYTES);
    bytes.extend_from_slice(&1u32.to_be_bytes());
    bytes.extend_from_slice(&(node.len() as u32).to_be_bytes());
    bytes.extend_from_slice(&(inventory.histories.len() as u32).to_be_bytes());
    bytes.push(u8::from(inventory.initializing.is_some()));
    bytes.extend_from_slice(&inventory.initializing.unwrap_or([0; 32]));
    bytes.extend_from_slice(&node);
    for history in &inventory.histories {
        bytes.extend_from_slice(&history.scope);
        bytes.extend_from_slice(&history.birth);
        bytes.extend_from_slice(&history.member);
        bytes.extend_from_slice(&history.completed_sequence);
        bytes.push(u8::from(history.in_configuration));
    }
    Ok(bytes)
}

pub(crate) fn decode(
    bytes: &[u8],
) -> Result<(Box<RaftHostObservation>, Box<RaftPhysicalInventory>), TransportError> {
    if bytes.len() < PREFIX || bytes.len() > MAX_BODY {
        return Err(TransportError::Limit);
    }
    // The fixed prefix is checked before reading counts; both lengths are
    // bounded and their exact sum is checked before allocating decoded rows.
    let word = |i| u32::from_be_bytes([bytes[i], bytes[i + 1], bytes[i + 2], bytes[i + 3]]);
    let node = word(4) as usize;
    let count = word(8) as usize;
    if word(0) != 1
        || node == 0
        || node > super::node_inspection_wire::MAX_BODY
        || count > MAX_GROUPS
        || PREFIX + node + count * ENTRY_BYTES != bytes.len()
    {
        return Err(TransportError::Protocol);
    }
    let mut pending = [0; 32];
    pending.copy_from_slice(&bytes[13..45]);
    let initializing = match bytes[12] {
        0 if pending == [0; 32] => None,
        1 if pending != [0; 32] => Some(pending),
        _ => return Err(TransportError::Protocol),
    };
    let view = super::node_inspection_wire::decode(&bytes[PREFIX..PREFIX + node])?;
    let mut histories = Vec::with_capacity(count);
    for row in bytes[PREFIX + node..].as_chunks::<ENTRY_BYTES>().0 {
        let mut scope = [0; 32];
        scope.copy_from_slice(&row[..32]);
        let mut birth = [0; 32];
        birth.copy_from_slice(&row[32..64]);
        let mut member = [0; 8];
        member.copy_from_slice(&row[64..72]);
        let mut sequence = [0; 8];
        sequence.copy_from_slice(&row[72..80]);
        MemberId::new(u64::from_be_bytes(member)).map_err(|_| TransportError::Identity)?;
        histories.push(RaftPhysicalHistory {
            scope,
            birth,
            member,
            completed_sequence: sequence,
            in_configuration: match row[80] {
                0 => false,
                1 => true,
                _ => return Err(TransportError::Protocol),
            },
        });
    }
    let inventory = RaftPhysicalInventory {
        histories,
        initializing,
    };
    inventory.validate(&view)?;
    Ok((view, Box::new(inventory)))
}
