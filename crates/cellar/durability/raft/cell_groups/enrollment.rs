// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Deterministic enrollment admission. Decisions and decoded records are data,
//! never proof of leadership, a committed enrollment or a published image.

use crate::application::consensus::{
    group_protocol::{CellGroupEntry, GroupCommand, GroupEntryLimits},
    ImageCommitment, LtxImage, ProtocolError,
};
use cellar_logic::{cell_group::*, consensus::*};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const RECORD_DOMAIN: &str = "cellar-cell-enrollment-1";
const MAX_RECORD_BYTES: usize = 4096;

/// The group owner's committed lifecycle state. Recheck it at log application;
/// an earlier decision made while active cannot authorize an entry after a seal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnrollmentPhase {
    Active,
    Sealed,
    Retired,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EnrollmentError {
    Protocol(ProtocolError),
    NotEnrollment,
    Sealed,
    Retired,
    Conflict,
    InvalidRecord,
    Limit,
}

impl std::fmt::Display for EnrollmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cell enrollment: {self:?}")
    }
}

impl std::error::Error for EnrollmentError {}

/// Immutable creation identity retained alongside the cell's committed state.
/// It survives subsequent cell mutations and normal group transfers. The host
/// must publish it atomically with the initial cell image and carry it through
/// snapshots/archives; the latest image cannot stand in for this original image.
/// Decoding only checks shape and canonical encoding, not commitment authority.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(try_from = "WireRecord")]
pub struct EnrollmentRecord {
    cell: CellHistory,
    creation: ProposalId,
    schema: [u8; 32],
    initial: CellImage,
}

impl EnrollmentRecord {
    /// Construct bounded creation data. This grants no admission or commitment;
    /// the owner must still validate the replicated enrollment against its route.
    pub fn new(
        cell: CellHistory,
        creation: ProposalId,
        schema: [u8; 32],
        initial: CellImage,
    ) -> Result<Self, EnrollmentError> {
        let record = Self {
            cell,
            creation,
            schema,
            initial,
        };
        record.encode()?;
        Ok(record)
    }

    pub fn cell(&self) -> &CellHistory {
        &self.cell
    }

    pub fn creation(&self) -> ProposalId {
        self.creation
    }

    pub fn schema(&self) -> [u8; 32] {
        self.schema
    }

    pub fn initial(&self) -> CellImage {
        self.initial
    }

    pub fn encode(&self) -> Result<Vec<u8>, EnrollmentError> {
        let image = LtxImage::try_from(self.initial).map_err(EnrollmentError::Protocol)?;
        if image.txid == 0
            && (image.checksum != 0
                || image.commitment != ImageCommitment::Sha256(Sha256::digest([]).into()))
        {
            return Err(EnrollmentError::InvalidRecord);
        }
        let bytes = serde_json::to_vec(self).map_err(|_| EnrollmentError::InvalidRecord)?;
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(EnrollmentError::Limit);
        }
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EnrollmentError> {
        if bytes.len() > MAX_RECORD_BYTES {
            return Err(EnrollmentError::Limit);
        }
        let record: Self =
            serde_json::from_slice(bytes).map_err(|_| EnrollmentError::InvalidRecord)?;
        if record.encode()? != bytes {
            return Err(EnrollmentError::InvalidRecord);
        }
        Ok(record)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EnrollmentDecision {
    /// No committed creation exists. The host still needs current-term authority,
    /// physical image verification and replicated atomic publication.
    Propose(EnrollmentRecord),
    /// Exact retry of the supplied committed creation. Do not rerun initialization
    /// or replace the cell's current image with its original enrollment image.
    Existing(EnrollmentRecord),
}

/// Validate the bounded cell-addressed wire request against the current route
/// and the group's committed lifecycle and creation record. Payload is borrowed
/// by the codec; admission never allocates another copy of the LTX image bytes.
/// The caller supplies only committed records, not pending proposals. Concurrent
/// proposals are rechecked in log order, so the first publication fixes identity.
pub fn decide_enrollment(
    route: &GroupRoute,
    phase: EnrollmentPhase,
    request: &[u8],
    committed: Option<&EnrollmentRecord>,
    limits: GroupEntryLimits,
) -> Result<EnrollmentDecision, EnrollmentError> {
    match phase {
        EnrollmentPhase::Sealed => return Err(EnrollmentError::Sealed),
        EnrollmentPhase::Retired => return Err(EnrollmentError::Retired),
        EnrollmentPhase::Active => {}
    }
    let entry =
        CellGroupEntry::decode(request, route, limits).map_err(EnrollmentError::Protocol)?;
    let GroupCommand::Enroll { cell, image, .. } = entry.command else {
        return Err(EnrollmentError::NotEnrollment);
    };
    let requested = EnrollmentRecord {
        cell,
        creation: entry.binding.proposal,
        schema: entry.binding.schema_digest,
        initial: image.into(),
    };
    // Bound the durable creation record before an owner reserves image work.
    requested.encode()?;
    match committed {
        Some(existing) if existing == &requested => {
            Ok(EnrollmentDecision::Existing(existing.clone()))
        }
        Some(_) => Err(EnrollmentError::Conflict),
        None => Ok(EnrollmentDecision::Propose(requested)),
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRecord {
    domain: String,
    cluster: [u8; 16],
    namespace: [u8; 16],
    cell: String,
    incarnation: [u8; 16],
    creation: [u8; 16],
    schema: [u8; 32],
    initial_epoch: u64,
    initial_txid: u64,
    initial_checksum: u64,
    initial_commitment: ImageCommitment,
}

impl TryFrom<WireRecord> for EnrollmentRecord {
    type Error = EnrollmentError;

    fn try_from(wire: WireRecord) -> Result<Self, Self::Error> {
        if wire.domain != RECORD_DOMAIN {
            return Err(EnrollmentError::InvalidRecord);
        }
        let record = EnrollmentRecord {
            cell: CellHistory {
                cluster: ClusterId::new(wire.cluster)
                    .map_err(|_| EnrollmentError::InvalidRecord)?,
                namespace: NamespaceId::new(wire.namespace)
                    .map_err(|_| EnrollmentError::InvalidRecord)?,
                cell: CellId::new(wire.cell).map_err(|_| EnrollmentError::InvalidRecord)?,
                incarnation: IncarnationId::new(wire.incarnation)
                    .map_err(|_| EnrollmentError::InvalidRecord)?,
            },
            creation: ProposalId::new(wire.creation).map_err(|_| EnrollmentError::InvalidRecord)?,
            schema: wire.schema,
            initial: LtxImage {
                epoch: wire.initial_epoch,
                txid: wire.initial_txid,
                checksum: wire.initial_checksum,
                commitment: wire.initial_commitment,
            }
            .into(),
        };
        record.encode()?;
        Ok(record)
    }
}

impl Serialize for EnrollmentRecord {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let record = self;
        let image = LtxImage::try_from(record.initial).map_err(serde::ser::Error::custom)?;
        WireRecord {
            domain: RECORD_DOMAIN.into(),
            cluster: record.cell.cluster.bytes(),
            namespace: record.cell.namespace.bytes(),
            cell: record.cell.cell.as_str().into(),
            incarnation: record.cell.incarnation.bytes(),
            creation: record.creation.bytes(),
            schema: record.schema,
            initial_epoch: record.initial.epoch,
            initial_txid: record.initial.txid,
            initial_checksum: record.initial.checksum,
            initial_commitment: image.commitment,
        }
        .serialize(serializer)
    }
}
