// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Logical cell identity is independent of the group that currently replicates it.
//!
//! These are pure contracts. Constructing a route, witness or version does not
//! prove ownership, persistence or quorum; adapters must supply those proofs.

use crate::consensus::{
    ClusterId, GroupId, IncarnationId, LogPosition, NamespaceId, ProposalId, Refusal,
};
use std::num::NonZeroU64;

pub const MAX_CELL_ID_BYTES: usize = 1024;
pub const CELL_ROUTE_DOMAIN: &[u8] = b"cellar-cell-route-1\0";

/// Exact UTF-8 bytes, without Unicode or locale normalization.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CellId(String);

impl CellId {
    pub fn new(value: impl AsRef<str>) -> Result<Self, Refusal> {
        let value = value.as_ref();
        Self::validate(value)?;
        Ok(Self(value.to_owned()))
    }

    pub fn validate(value: &str) -> Result<(), Refusal> {
        if value.is_empty()
            || value.len() > MAX_CELL_ID_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(Refusal::InvalidIdentity);
        }
        Ok(())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Canonical SHA-256 input; the adapter performs hashing outside the core.
    pub fn routing_preimage(&self, namespace: NamespaceId) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(CELL_ROUTE_DOMAIN.len() + 20 + self.0.len());
        bytes.extend_from_slice(CELL_ROUTE_DOMAIN);
        bytes.extend_from_slice(&namespace.bytes());
        bytes.extend_from_slice(&(self.0.len() as u32).to_be_bytes());
        bytes.extend_from_slice(self.0.as_bytes());
        bytes
    }
}

/// Preserved across elections, placement changes and normal split/merge.
/// Disaster recovery must allocate a new cell incarnation.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CellHistory {
    pub cluster: ClusterId,
    pub namespace: NamespaceId,
    pub cell: CellId,
    pub incarnation: IncarnationId,
}

/// Projects the actual logical binding from the temporary single-cell host.
/// No physical group identity becomes part of the logical history. This does
/// not migrate stored state or confer execution authority.
impl TryFrom<&crate::consensus::GroupScope> for CellHistory {
    type Error = Refusal;

    fn try_from(scope: &crate::consensus::GroupScope) -> Result<Self, Refusal> {
        scope.validate()?;
        Ok(Self {
            cluster: scope.cluster,
            namespace: scope.namespace,
            cell: CellId::new(&scope.cell)?,
            incarnation: scope.incarnation,
        })
    }
}

/// A physical Raft history; deliberately contains no cell identifier.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupHistory {
    pub cluster: ClusterId,
    pub namespace: NamespaceId,
    pub group: GroupId,
    pub incarnation: IncarnationId,
}

impl GroupHistory {
    pub fn same_namespace(&self, cell: &CellHistory) -> bool {
        self.cluster == cell.cluster && self.namespace == cell.namespace
    }
}

/// Projects the physical history from the temporary single-cell adapter.
/// This does not validate or authorize the adapter's cell binding.
impl From<&crate::consensus::GroupScope> for GroupHistory {
    fn from(scope: &crate::consensus::GroupScope) -> Self {
        Self {
            cluster: scope.cluster,
            namespace: scope.namespace,
            group: scope.group,
            incarnation: scope.incarnation,
        }
    }
}

impl From<&GroupHistory> for GroupHistory {
    fn from(history: &GroupHistory) -> Self {
        *history
    }
}

macro_rules! nonzero_counter {
    ($($name:ident),+ $(,)?) => {$ (
        #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(NonZeroU64);

        impl $name {
            pub fn new(value: u64) -> Result<Self, Refusal> {
                NonZeroU64::new(value).map(Self).ok_or(Refusal::InvalidIdentity)
            }

            pub const fn get(self) -> u64 {
                self.0.get()
            }

            /// Overflow is a refusal, never a wraparound or saturated version.
            pub fn checked_next(self) -> Result<Self, Refusal> {
                Self::new(self.get().checked_add(1).ok_or(Refusal::InvalidPosition)?)
            }
        }
    )+};
}

nonzero_counter!(
    MutationSequence,
    RouteEpoch,
    MembershipRevision,
    ParkGeneration
);

/// Non-reusable identity of a namespace topology operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TransitionId([u8; 16]);

impl TransitionId {
    pub fn new(bytes: [u8; 16]) -> Result<Self, Refusal> {
        if bytes == [0; 16] {
            return Err(Refusal::InvalidIdentity);
        }
        Ok(Self(bytes))
    }

    pub const fn bytes(self) -> [u8; 16] {
        self.0
    }
}

/// Ordered hash plus the full ID. Digest collisions never alias distinct cells.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CellKey {
    digest: [u8; 32],
    cell: CellId,
}

impl CellKey {
    /// The adapter must verify this digest against the namespace and canonical
    /// routing preimage before using a key received over the wire.
    pub fn from_digest(digest: [u8; 32], cell: CellId) -> Self {
        Self { digest, cell }
    }

    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }

    pub fn cell(&self) -> &CellId {
        &self.cell
    }
}

/// The declaration order is the canonical ordering, including infinity bounds.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CellBound {
    NegativeInfinity,
    Key(CellKey),
    PositiveInfinity,
}

/// A nonempty half-open interval of whole cell routing keys.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellRange {
    lower: CellBound,
    upper: CellBound,
}

impl CellRange {
    pub fn new(lower: CellBound, upper: CellBound) -> Result<Self, Refusal> {
        if lower >= upper {
            return Err(Refusal::InvalidIdentity);
        }
        Ok(Self { lower, upper })
    }

    pub fn all() -> Self {
        Self {
            lower: CellBound::NegativeInfinity,
            upper: CellBound::PositiveInfinity,
        }
    }

    pub fn lower(&self) -> &CellBound {
        &self.lower
    }

    pub fn upper(&self) -> &CellBound {
        &self.upper
    }

    pub fn contains(&self, key: &CellKey) -> bool {
        let lower = match &self.lower {
            CellBound::NegativeInfinity => true,
            CellBound::Key(bound) => key >= bound,
            CellBound::PositiveInfinity => false,
        };
        let upper = match &self.upper {
            CellBound::PositiveInfinity => true,
            CellBound::Key(bound) => key < bound,
            CellBound::NegativeInfinity => false,
        };
        lower && upper
    }

    pub fn split(&self, at: CellKey) -> Result<(Self, Self), Refusal> {
        let at = CellBound::Key(at);
        Ok((
            Self::new(self.lower.clone(), at.clone())?,
            Self::new(at, self.upper.clone())?,
        ))
    }

    /// Merge only adjacent intervals in order; overlap and gaps both refuse.
    pub fn merge(&self, right: &Self) -> Result<Self, Refusal> {
        if self.upper != right.lower {
            return Err(Refusal::NonContiguous);
        }
        Self::new(self.lower.clone(), right.upper.clone())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GroupRoute {
    pub history: GroupHistory,
    pub range: CellRange,
    pub epoch: RouteEpoch,
    pub birth_digest: [u8; 32],
    pub schema_digest: [u8; 32],
}

impl GroupRoute {
    /// The supplied key must already have its namespace hash verified.
    pub fn check_cell(&self, cell: &CellHistory, key: &CellKey) -> Result<(), Refusal> {
        if !self.history.same_namespace(cell)
            || key.cell() != &cell.cell
            || !self.range.contains(key)
        {
            return Err(Refusal::WrongHistory);
        }
        Ok(())
    }
}

/// An LTX position with a complete, domain-specific image commitment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CellImage {
    pub epoch: u64,
    pub txid: u64,
    pub checksum: u64,
    pub commitment: CellImageCommitment,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CellImageCommitment {
    Materialized([u8; 32]),
    Extents { root: [u8; 32], manifest_bytes: u32 },
}

impl CellImage {
    pub fn validate(self) -> Result<(), Refusal> {
        if self.epoch == 0
            || matches!(
                self.commitment,
                CellImageCommitment::Extents {
                    manifest_bytes: 0,
                    ..
                }
            )
        {
            return Err(Refusal::InvalidPosition);
        }
        Ok(())
    }
}

/// Authenticated cell progress, independent of the current group's log index.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CellVersion {
    pub history: CellHistory,
    pub sequence: MutationSequence,
    pub image: CellImage,
}

impl CellVersion {
    /// Call only after authenticating the token and validating its schema and
    /// installed lineage. A witness from an old group is not current authority.
    pub fn covers(&self, required: &Self) -> Result<bool, Refusal> {
        self.image.validate()?;
        required.image.validate()?;
        if self.history != required.history
            || (self.sequence == required.sequence && self.image != required.image)
        {
            return Err(Refusal::WrongHistory);
        }
        Ok(self.sequence >= required.sequence)
    }
}

/// Locates the original decision, including after transfer to a new group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CommitWitness {
    pub group: GroupHistory,
    pub position: LogPosition,
    pub proposal: ProposalId,
}

impl CommitWitness {
    pub fn validate(&self, cell: &CellHistory) -> Result<(), Refusal> {
        self.position.validate()?;
        if !self.group.same_namespace(cell) {
            return Err(Refusal::WrongHistory);
        }
        if self.position.index.get() == 0 {
            return Err(Refusal::InvalidPosition);
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "cell_group_tests.rs"]
mod tests;
