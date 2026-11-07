// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::super::protocol::{ImageCommitment, LtxImage, ProtocolError};
use super::super::{group_contracts::cell_routing_key, protocol::EnvelopeLimits};
use cellar_logic::{
    cell_group::*,
    consensus::{DiskLineage, LogPosition, MemberId, NodeId, ProposalId, Term},
};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupEntryLimits {
    pub envelope: EnvelopeLimits,
    pub max_members: usize,
    pub max_transition_groups: usize,
}

impl Default for GroupEntryLimits {
    fn default() -> Self {
        Self {
            envelope: EnvelopeLimits::default(),
            max_members: 64,
            max_transition_groups: 64,
        }
    }
}

impl GroupEntryLimits {
    pub fn validate(self) -> Result<(), ProtocolError> {
        self.envelope.validate()?;
        if !(1..=64).contains(&self.max_members) || !(1..=64).contains(&self.max_transition_groups)
        {
            return Err(ProtocolError::Limit);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupEntryBinding {
    pub group: GroupHistory,
    pub route_epoch: RouteEpoch,
    pub birth_digest: [u8; 32],
    pub schema_digest: [u8; 32],
    pub proposal: ProposalId,
}

impl GroupEntryBinding {
    pub fn check(&self, route: &GroupRoute) -> Result<(), ProtocolError> {
        if self.group != route.history
            || self.route_epoch != route.epoch
            || self.birth_digest != route.birth_digest
            || self.schema_digest != route.schema_digest
        {
            return Err(ProtocolError::Identity);
        }
        check_range_keys(route)?;
        Ok(())
    }
}

/// The entry's original encoded buffer owns these bytes until staging finishes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupLtx<'a> {
    pub min_txid: u64,
    pub max_txid: u64,
    pub page_count: u64,
    pub decoded_bytes: u64,
    pub bytes: &'a [u8],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupMutation<'a> {
    pub cell: CellHistory,
    pub sequence: MutationSequence,
    pub base: LtxImage,
    pub result: LtxImage,
    pub files: Vec<GroupLtx<'a>>,
}

impl GroupMutation<'_> {
    /// Call against the coherent manifest leaf before allocating image work.
    /// Valid framing or another cell's progress cannot establish this base.
    pub fn check_base(&self, current: &CellVersion) -> Result<(), ProtocolError> {
        if self.cell != current.history
            || current.sequence.checked_next().ok() != Some(self.sequence)
            || CellImage::from(self.base) != current.image
        {
            return Err(ProtocolError::Chain);
        }
        Ok(())
    }

    pub(super) fn validate(&self, limits: EnvelopeLimits) -> Result<usize, ProtocolError> {
        LtxImage::try_from(CellImage::from(self.base))?;
        LtxImage::try_from(CellImage::from(self.result))?;
        if self.sequence.get() <= 1
            || self.base.epoch != self.result.epoch
            || self.result.txid <= self.base.txid
            || !matches!(
                (self.base.commitment, self.result.commitment),
                (ImageCommitment::Sha256(_), ImageCommitment::Sha256(_))
                    | (ImageCommitment::Extents(_), ImageCommitment::Extents(_))
            )
        {
            return Err(ProtocolError::Chain);
        }
        validate_files(&self.files, limits, self.base.txid, self.result.txid)
    }
}

pub(super) fn validate_files(
    files: &[GroupLtx<'_>],
    limits: EnvelopeLimits,
    base: u64,
    result: u64,
) -> Result<usize, ProtocolError> {
    if files.len() > limits.max_files || (files.is_empty() && (base != 0 || result != 0)) {
        return Err(ProtocolError::Limit);
    }
    let (mut txid, mut payload, mut decoded, mut pages) = (base, 0_usize, 0_u64, 0_u64);
    for file in files {
        if txid.checked_add(1) != Some(file.min_txid)
            || file.max_txid < file.min_txid
            || file.bytes.is_empty()
        {
            return Err(ProtocolError::Chain);
        }
        txid = file.max_txid;
        payload = payload
            .checked_add(file.bytes.len())
            .ok_or(ProtocolError::Limit)?;
        decoded = decoded
            .checked_add(file.decoded_bytes)
            .ok_or(ProtocolError::Limit)?;
        pages = pages
            .checked_add(file.page_count)
            .ok_or(ProtocolError::Limit)?;
        if payload > limits.max_payload_bytes
            || decoded > limits.max_decoded_bytes
            || pages > limits.max_pages
        {
            return Err(ProtocolError::Limit);
        }
    }
    if txid != result {
        return Err(ProtocolError::Chain);
    }
    Ok(payload)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupMemberRole {
    Voter,
    Learner,
    Remove,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupMemberChange {
    pub member: MemberId,
    pub node: NodeId,
    pub disk: DiskLineage,
    pub role: GroupMemberRole,
}

/// Matches the raft-rs configuration transition semantics. The owning runtime
/// must encode this command as the actual ConfChangeV2 entry, not a data no-op.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GroupMembershipTransition {
    Auto,
    JointImplicit,
    JointExplicit,
    LeaveJoint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GroupSeal {
    pub group: GroupHistory,
    pub position: LogPosition,
    pub manifest: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GroupCommand<'a> {
    Mutation(GroupMutation<'a>),
    /// Enrollment starts at sequence one. A nonempty image carries a complete
    /// LTX chain starting at txid one, so majority log persistence protects its
    /// bytes. Publication still requires independent image verification/sync.
    Enroll {
        cell: CellHistory,
        image: LtxImage,
        files: Vec<GroupLtx<'a>>,
    },
    Membership {
        previous: MembershipRevision,
        next: MembershipRevision,
        transition: GroupMembershipTransition,
        changes: Vec<GroupMemberChange>,
    },
    Checkpoint {
        position: LogPosition,
        manifest: [u8; 32],
        membership: MembershipRevision,
    },
    Seal {
        transition: TransitionId,
        authorization: [u8; 32],
        children: Vec<GroupRoute>,
    },
    Install {
        transition: TransitionId,
        sources: Vec<GroupSeal>,
        manifest: [u8; 32],
    },
    Activate {
        transition: TransitionId,
        publication: [u8; 32],
        manifest: [u8; 32],
    },
    PreparePark {
        generation: ParkGeneration,
        term: Term,
        membership: MembershipRevision,
    },
    Retire {
        transition: TransitionId,
        publication: [u8; 32],
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellGroupEntry<'a> {
    pub binding: GroupEntryBinding,
    pub command: GroupCommand<'a>,
}

impl CellGroupEntry<'_> {
    pub(super) fn validate(
        &self,
        route: &GroupRoute,
        limits: GroupEntryLimits,
    ) -> Result<usize, ProtocolError> {
        limits.validate()?;
        self.binding.check(route)?;
        match &self.command {
            GroupCommand::Mutation(update) => {
                check_cell(route, &update.cell)?;
                return update.validate(limits.envelope);
            }
            GroupCommand::Enroll { cell, image, files } => {
                check_cell(route, cell)?;
                LtxImage::try_from(CellImage::from(*image))?;
                if image.txid == 0 && image.checksum != 0 {
                    return Err(ProtocolError::Chain);
                }
                return validate_files(files, limits.envelope, 0, image.txid);
            }
            GroupCommand::Membership {
                previous,
                next,
                transition,
                changes,
            } => {
                if previous.checked_next().ok() != Some(*next) {
                    return Err(ProtocolError::Chain);
                }
                if changes.len() > limits.max_members {
                    return Err(ProtocolError::Limit);
                }
                if (*transition == GroupMembershipTransition::LeaveJoint) != changes.is_empty() {
                    return Err(ProtocolError::Header);
                }
                let mut members = BTreeSet::new();
                for change in changes {
                    if !members.insert(change.member) {
                        return Err(ProtocolError::Identity);
                    }
                }
            }
            GroupCommand::Checkpoint { position, .. } => check_position(*position)?,
            GroupCommand::Seal { children, .. } => {
                if children.is_empty() || children.len() > limits.max_transition_groups {
                    return Err(ProtocolError::Limit);
                }
                let mut groups = BTreeSet::new();
                let mut last: Option<&GroupRoute> = None;
                for child in children {
                    check_related(self.binding.group, child.history)?;
                    if child.schema_digest != route.schema_digest {
                        return Err(ProtocolError::Identity);
                    }
                    if !groups.insert(child.history.group) {
                        return Err(ProtocolError::Identity);
                    }
                    if let Some(previous) = last {
                        if previous.range.upper() != child.range.lower() {
                            return Err(ProtocolError::Chain);
                        }
                    }
                    check_range_keys(child)?;
                    last = Some(child);
                }
                // A merge child may include neighboring parent ranges. The
                // transition owner verifies the complete parent/child union.
                if children[0].range.lower() > route.range.lower()
                    || children[children.len() - 1].range.upper() < route.range.upper()
                {
                    return Err(ProtocolError::Chain);
                }
            }
            GroupCommand::Install { sources, .. } => {
                if sources.is_empty() || sources.len() > limits.max_transition_groups {
                    return Err(ProtocolError::Limit);
                }
                let mut groups = BTreeSet::new();
                for source in sources {
                    check_related(self.binding.group, source.group)?;
                    if !groups.insert(source.group.group) {
                        return Err(ProtocolError::Identity);
                    }
                    check_position(source.position)?;
                }
            }
            GroupCommand::PreparePark { term, .. } if term.get() == 0 => {
                return Err(ProtocolError::Identity)
            }
            GroupCommand::Activate { .. }
            | GroupCommand::PreparePark { .. }
            | GroupCommand::Retire { .. } => {}
        }
        Ok(0)
    }
}

pub(super) fn check_cell(route: &GroupRoute, cell: &CellHistory) -> Result<(), ProtocolError> {
    route
        .check_cell(cell, &cell_routing_key(cell.namespace, &cell.cell))
        .map_err(|_| ProtocolError::Identity)
}

pub(super) fn check_range_keys(route: &GroupRoute) -> Result<(), ProtocolError> {
    for bound in [route.range.lower(), route.range.upper()] {
        if let CellBound::Key(key) = bound {
            if &cell_routing_key(route.history.namespace, key.cell()) != key {
                return Err(ProtocolError::Identity);
            }
        }
    }
    Ok(())
}

fn check_related(owner: GroupHistory, other: GroupHistory) -> Result<(), ProtocolError> {
    if owner.cluster != other.cluster
        || owner.namespace != other.namespace
        || owner.group == other.group
    {
        return Err(ProtocolError::Identity);
    }
    Ok(())
}

fn check_position(position: LogPosition) -> Result<(), ProtocolError> {
    position.validate().map_err(|_| ProtocolError::Chain)?;
    if position.index.get() == 0 {
        return Err(ProtocolError::Chain);
    }
    Ok(())
}
