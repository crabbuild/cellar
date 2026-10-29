// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Historical paged evidence. No decoder can mint a runtime verification.
use super::*;
use serde::Deserialize;

pub(super) const ENTRY_KIND: u8 = 82;
pub(super) const CANDIDATE_KIND: u8 = 83;
pub(super) const LIMIT: usize = 4096;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Physical {
    pub member: u64,
    pub in_configuration: bool,
    pub completed_sequence: u64,
}

impl From<RaftPhysicalHistory> for Physical {
    fn from(value: RaftPhysicalHistory) -> Self {
        Self {
            member: value.member().get(),
            in_configuration: value.in_configuration(),
            completed_sequence: value.completed_sequence(),
        }
    }
}

#[derive(Serialize, Deserialize)]
pub(super) enum Evidence {
    Cancelled { operation: [u8; 16] },
    Unrelated,
    Retained,
    Voting(Box<Voting>),
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::raft::fleet_catalog::maintenance) struct Voter {
    pub node: [u8; 16],
    pub boot: [u8; 32],
    pub epoch: u64,
    pub member: u64,
    pub publication: [u8; 32],
    #[serde(deserialize_with = "nullable")]
    pub leader_load: Option<[u32; 2]>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::raft::fleet_catalog::maintenance) struct Voting {
    pub leadership: Leadership,
    pub fixed: Publication,
    pub voters: [Voter; 3],
    pub leader: u64,
    pub term: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Entry {
    pub format: u8,
    pub record: [u8; 32],
    pub physical: Option<Physical>,
    pub evidence: Evidence,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Candidate {
    pub format: u8,
    pub basis: Basis,
    pub physical: [u8; 32],
    pub index: Option<[u8; 32]>,
    pub count: u64,
    pub covered_physical: u64,
    pub voting: u64,
    pub retained: u64,
    pub matched: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(in crate::raft::fleet_catalog::maintenance) enum Leadership {
    Inspected,
    Matched { through: u64 },
}

fn nullable<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<Option<T>, D::Error> {
    Option::deserialize(decoder)
}
