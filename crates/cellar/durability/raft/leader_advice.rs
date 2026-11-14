// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Owner observations select destinations; they never grant serving authority.
use cellar_logic::{cell_group::GroupHistory, consensus::MemberId};

/// The authenticated RPC route supplies the history and issuer endpoint binding.
/// The committed configuration, Raft term and range epoch remain independent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LeaderAdvice {
    pub history: GroupHistory,
    pub membership_revision: u64,
    pub configuration_digest: [u8; 32],
    pub term: u64,
    pub leader: MemberId,
}

pub(crate) struct RoutingObservation {
    pub status: crate::raft::cell_groups::CellGroupStatus,
    pub park: Option<cellar_logic::cell_group::ParkGeneration>,
    pub advice: Option<LeaderAdvice>,
}

/// A rejection is proof about this attempt only. It cannot settle a previous
/// transmission of the same command, including one whose response was lost.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MutationDisposition {
    Applied,
    RejectedBeforeAdmission,
    AdmittedOrUnknown,
}
