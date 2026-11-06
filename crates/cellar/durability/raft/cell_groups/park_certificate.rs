// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Complete voter evidence for one exact park generation. Transport must
//! authenticate each remote reply before it enters certificate assembly.

use super::hibernate::{GroupParkCut, ParkAcknowledgement};
use crate::raft::{member_state::GroupMembership, storage::LogError};
use ::raft::prelude::ConfState;
use cellar_logic::{
    cell_group::{CellBound, GroupHistory, GroupRoute},
    consensus::{DiskLineage, MemberId, NodeId, NodeIdentity, ProcessGeneration, ReplicaIdentity},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

const MAX_PARK_VOTERS: usize = 16;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ParkCertificate {
    route: [u8; 32],
    cut: GroupParkCut,
    leader: u64,
    voters: Vec<VoterWitness>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VoterWitness {
    member: u64,
    node: [u8; 16],
    disk: [u8; 16],
    process: [u8; 16],
    sequence: u64,
}

impl VoterWitness {
    fn replica(&self, scope: GroupHistory) -> Result<ReplicaIdentity<GroupHistory>, LogError> {
        Ok(ReplicaIdentity {
            scope,
            member: MemberId::new(self.member).map_err(|_| LogError::WrongIdentity)?,
            node: NodeId::new(self.node).map_err(|_| LogError::WrongIdentity)?,
            disk: DiskLineage::new(self.disk).map_err(|_| LogError::WrongIdentity)?,
            process: ProcessGeneration::new(self.process).map_err(|_| LogError::WrongIdentity)?,
        })
    }
}

impl ParkCertificate {
    /// The roster comes from the active physical registration. Each
    /// acknowledgement is supplied by the local runtime or an authenticated
    /// reply tied to that same registration.
    pub(crate) fn assemble(
        route: &GroupRoute,
        cut: GroupParkCut,
        leader: &ReplicaIdentity<GroupHistory>,
        roster: &[ReplicaIdentity<GroupHistory>],
        acknowledgements: Vec<ParkAcknowledgement>,
    ) -> Result<Self, LogError> {
        if !(1..=MAX_PARK_VOTERS).contains(&roster.len()) || roster.len() != acknowledgements.len()
        {
            return Err(LogError::WrongIdentity);
        }
        let mut expected = BTreeMap::new();
        let mut nodes = BTreeSet::new();
        let mut disks = BTreeSet::new();
        for replica in roster {
            if replica.scope != route.history
                || expected.insert(replica.member, replica).is_some()
                || !nodes.insert(replica.node)
                || !disks.insert(replica.disk)
            {
                return Err(LogError::WrongIdentity);
            }
        }
        if expected.get(&leader.member).copied() != Some(leader) {
            return Err(LogError::WrongIdentity);
        }

        let mut voters = Vec::with_capacity(roster.len());
        for acknowledgement in acknowledgements {
            let voter = acknowledgement.voter;
            if acknowledgement.leader != leader.member
                || acknowledgement.cut != cut
                || acknowledgement.sync_sequence == 0
                || expected.remove(&voter.member) != Some(&voter)
            {
                return Err(LogError::WrongIdentity);
            }
            voters.push(VoterWitness {
                member: voter.member.get(),
                node: voter.node.bytes(),
                disk: voter.disk.bytes(),
                process: voter.process.bytes(),
                sequence: acknowledgement.sync_sequence,
            });
        }
        voters.sort_unstable_by_key(|voter| voter.member);
        Ok(Self {
            route: route_binding(route),
            cut,
            leader: leader.member.get(),
            voters,
        })
    }

    pub(crate) fn cut(&self) -> &GroupParkCut {
        &self.cut
    }

    pub(crate) fn leader(&self) -> Result<MemberId, LogError> {
        MemberId::new(self.leader).map_err(|_| LogError::WrongIdentity)
    }

    pub(crate) fn reserved_bytes(&self) -> usize {
        1024_usize.saturating_add(self.voters.len().saturating_mul(512))
    }

    pub(super) fn validate(
        &self,
        route: &GroupRoute,
        conf: &ConfState,
        membership: &GroupMembership,
    ) -> Result<(), LogError> {
        if self.route != route_binding(route)
            || self.cut.membership_revision() != membership.revision().get()
            || !(1..=MAX_PARK_VOTERS).contains(&self.voters.len())
            || self.voters.len() != conf.voters.len()
            || !conf.voters.contains(&self.leader)
            || !conf.voters_outgoing.is_empty()
            || !conf.learners.is_empty()
            || !conf.learners_next.is_empty()
            || conf.auto_leave
        {
            return Err(LogError::WrongIdentity);
        }
        let mut previous = 0;
        let mut nodes = BTreeSet::new();
        let mut disks = BTreeSet::new();
        for witness in &self.voters {
            let replica = witness.replica(route.history)?;
            if witness.member <= previous
                || witness.sequence == 0
                || !conf.voters.contains(&witness.member)
                || !membership.permits_group(&replica)
                || !nodes.insert(replica.node)
                || !disks.insert(replica.disk)
            {
                return Err(LogError::WrongIdentity);
            }
            previous = witness.member;
        }
        Ok(())
    }

    pub(super) fn matches_leader(&self, member: MemberId, identity: NodeIdentity) -> bool {
        self.leader == member.get()
            && self.voters.iter().any(|voter| {
                voter.member == self.leader
                    && voter.node == identity.node.bytes()
                    && voter.disk == identity.disk.bytes()
                    && voter.process == identity.process.bytes()
            })
    }

    pub(super) fn matches_journal(&self, process: [u8; 16]) -> bool {
        self.voters
            .iter()
            .any(|voter| voter.member == self.leader && voter.process == process)
    }
}

fn route_binding(route: &GroupRoute) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"cellar-park-certificate-route-1");
    digest.update(route.history.cluster.bytes());
    digest.update(route.history.namespace.bytes());
    digest.update(route.history.group.bytes());
    digest.update(route.history.incarnation.bytes());
    digest.update(route.epoch.get().to_be_bytes());
    for bound in [route.range.lower(), route.range.upper()] {
        match bound {
            CellBound::NegativeInfinity => digest.update([0]),
            CellBound::Key(key) => {
                digest.update([1]);
                digest.update(key.digest());
                let cell = key.cell().as_str().as_bytes();
                digest.update((cell.len() as u64).to_be_bytes());
                digest.update(cell);
            }
            CellBound::PositiveInfinity => digest.update([2]),
        }
    }
    digest.update(route.birth_digest);
    digest.update(route.schema_digest);
    digest.finalize().into()
}
