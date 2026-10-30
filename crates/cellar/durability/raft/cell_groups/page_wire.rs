// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::manifest::{
    CellImageGeometry, CellManifestLeaf, EffectSummary, ManifestError, ManifestLimits,
};
use crate::application::consensus::{ImageCommitment, LtxImage};
use cellar_logic::{cell_group::*, consensus::*};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[cfg(test)]
#[path = "page_wire_tests.rs"]
mod tests;

const MAGIC: &[u8; 8] = b"CLRCGM01";
pub(super) const MAX_PAGE_BYTES: usize = 16 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PageRef {
    pub digest: [u8; 32],
    pub height: u8,
    pub cells: u32,
}

pub(super) struct Page {
    pub leaf: CellManifestLeaf,
    pub left: Option<PageRef>,
    pub right: Option<PageRef>,
}

impl Page {
    pub fn reference(
        &self,
        bytes: &[u8],
        limits: ManifestLimits,
    ) -> Result<PageRef, ManifestError> {
        let height = |p: Option<PageRef>| p.map_or(0, |p| p.height);
        let count = |p: Option<PageRef>| p.map_or(0, |p| p.cells);
        for child in [self.left, self.right].into_iter().flatten() {
            if child.height == 0
                || child.height > limits.max_depth
                || child.cells == 0
                || child.cells > limits.max_cells
            {
                return Err(ManifestError::Invalid("child bounds"));
            }
        }
        if height(self.left).abs_diff(height(self.right)) > 1 {
            return Err(ManifestError::Invalid("unbalanced page"));
        }
        let height = height(self.left)
            .max(height(self.right))
            .checked_add(1)
            .ok_or(ManifestError::Limit)?;
        let cells = count(self.left)
            .checked_add(count(self.right))
            .and_then(|n| n.checked_add(1))
            .ok_or(ManifestError::Limit)?;
        if height > limits.max_depth || cells > limits.max_cells {
            return Err(ManifestError::Limit);
        }
        Ok(PageRef {
            digest: digest(bytes),
            height,
            cells,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WirePage {
    enrollment: super::EnrollmentRecord,
    schema: [u8; 32],
    cluster: [u8; 16],
    namespace: [u8; 16],
    cell: String,
    incarnation: [u8; 16],
    sequence: u64,
    commitment: ImageCommitment,
    epoch: u64,
    txid: u64,
    checksum: u64,
    geometry: CellImageGeometry,
    mutation_index: u64,
    mutation_term: u64,
    witness: WireWitness,
    effects: EffectSummary,
    left: Option<PageRef>,
    right: Option<PageRef>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireWitness {
    cluster: [u8; 16],
    namespace: [u8; 16],
    group: [u8; 16],
    incarnation: [u8; 16],
    index: u64,
    term: u64,
    proposal: [u8; 16],
}

impl From<CommitWitness> for WireWitness {
    fn from(witness: CommitWitness) -> Self {
        Self {
            cluster: witness.group.cluster.bytes(),
            namespace: witness.group.namespace.bytes(),
            group: witness.group.group.bytes(),
            incarnation: witness.group.incarnation.bytes(),
            index: witness.position.index.get(),
            term: witness.position.term.get(),
            proposal: witness.proposal.bytes(),
        }
    }
}

impl TryFrom<WireWitness> for CommitWitness {
    type Error = ManifestError;

    fn try_from(wire: WireWitness) -> Result<Self, Self::Error> {
        let invalid = |_| ManifestError::Invalid("cell commit witness");
        Ok(Self {
            group: GroupHistory {
                cluster: ClusterId::new(wire.cluster).map_err(invalid)?,
                namespace: NamespaceId::new(wire.namespace).map_err(invalid)?,
                group: GroupId::new(wire.group).map_err(invalid)?,
                incarnation: IncarnationId::new(wire.incarnation).map_err(invalid)?,
            },
            position: LogPosition {
                index: LogIndex::new(wire.index),
                term: Term::new(wire.term),
            },
            proposal: ProposalId::new(wire.proposal).map_err(invalid)?,
        })
    }
}

pub(super) fn digest(bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(b"cellar-cell-group-manifest-page-1\0");
    hash.update(bytes);
    hash.finalize().into()
}

pub(super) fn encode(
    page: &Page,
    route: &GroupRoute,
    limits: ManifestLimits,
) -> Result<Vec<u8>, ManifestError> {
    page.leaf.validate(route)?;
    let leaf = &page.leaf;
    let history = &leaf.version.history;
    let wire = WirePage {
        enrollment: leaf.enrollment.clone(),
        schema: route.schema_digest,
        cluster: history.cluster.bytes(),
        namespace: history.namespace.bytes(),
        cell: history.cell.as_str().to_owned(),
        incarnation: history.incarnation.bytes(),
        sequence: leaf.version.sequence.get(),
        commitment: LtxImage::try_from(leaf.version.image)
            .map_err(|_| ManifestError::Invalid("image commitment"))?
            .commitment,
        epoch: leaf.version.image.epoch,
        txid: leaf.version.image.txid,
        checksum: leaf.version.image.checksum,
        geometry: leaf.geometry,
        mutation_index: leaf.last_mutation.index.get(),
        mutation_term: leaf.last_mutation.term.get(),
        witness: leaf.witness.into(),
        effects: leaf.effects,
        left: page.left,
        right: page.right,
    };
    let mut bytes = MAGIC.to_vec();
    bytes.extend(serde_json::to_vec(&wire).map_err(|_| ManifestError::Invalid("page encoding"))?);
    if bytes.len() > MAX_PAGE_BYTES {
        return Err(ManifestError::Limit);
    }
    page.reference(&bytes, limits)?;
    Ok(bytes)
}

pub(super) fn decode(
    bytes: &[u8],
    route: &GroupRoute,
    limits: ManifestLimits,
) -> Result<Page, ManifestError> {
    // Reject length before JSON allocation. The schema has no unbounded vectors
    // or recursive fields; serde's recursion limit remains enabled.
    if bytes.len() > MAX_PAGE_BYTES {
        return Err(ManifestError::Limit);
    }
    if !bytes.starts_with(MAGIC) {
        return Err(ManifestError::Invalid("page domain"));
    }
    let wire: WirePage = serde_json::from_slice(&bytes[MAGIC.len()..])
        .map_err(|_| ManifestError::Invalid("page decoding"))?;
    if wire.schema != route.schema_digest {
        return Err(ManifestError::Invalid("page schema"));
    }
    let history = CellHistory {
        cluster: ClusterId::new(wire.cluster).map_err(|_| ManifestError::Invalid("cluster"))?,
        namespace: NamespaceId::new(wire.namespace)
            .map_err(|_| ManifestError::Invalid("namespace"))?,
        cell: CellId::new(wire.cell).map_err(|_| ManifestError::Invalid("cell ID"))?,
        incarnation: IncarnationId::new(wire.incarnation)
            .map_err(|_| ManifestError::Invalid("incarnation"))?,
    };
    let image = LtxImage {
        epoch: wire.epoch,
        txid: wire.txid,
        checksum: wire.checksum,
        commitment: wire.commitment,
    };
    let page = Page {
        leaf: CellManifestLeaf {
            enrollment: wire.enrollment,
            version: CellVersion {
                history,
                sequence: MutationSequence::new(wire.sequence)
                    .map_err(|_| ManifestError::Invalid("sequence"))?,
                image: image.into(),
            },
            geometry: wire.geometry,
            last_mutation: LogPosition {
                index: LogIndex::new(wire.mutation_index),
                term: Term::new(wire.mutation_term),
            },
            witness: wire.witness.try_into()?,
            effects: wire.effects,
        },
        left: wire.left,
        right: wire.right,
    };
    if encode(&page, route, limits)? != bytes {
        return Err(ManifestError::Invalid("noncanonical page"));
    }
    Ok(page)
}
