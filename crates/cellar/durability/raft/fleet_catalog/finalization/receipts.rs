// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Immutable paged receipt data. Verification capabilities are never decoded
//! from these records; the runtime must independently establish every proof.
use super::super::evacuation::ActiveEvacuation;
use super::super::*;
use crate::raft::archive::{ArchiveReference, VerifiedArchiveCoverage};
use crate::raft::host::FleetPublishedCut;
use crate::raft::manifest::{Image, Position};
use cellar_logic::consensus::{DiskLineage, EngineInstanceId, NodeId, ProcessGeneration};
use serde::Deserialize;
use std::collections::BTreeSet;

const CANDIDATE_KIND: u8 = 55;
const ENTRY_KIND: u8 = 56;
const ENTRY_LIMIT: usize = 4096;
const APPEND_LIMIT: usize = 16;
const BASIS_TABLES: [FleetTable; 4] = [
    FleetTable::Policy,
    FleetTable::Nodes,
    FleetTable::Histories,
    FleetTable::Admissions,
];

fn nullable<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<Option<T>, D::Error> {
    Option::deserialize(decoder)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Basis {
    format: u8,
    catalog: [u8; 32],
    cluster: [u8; 16],
    fleet: [u8; 16],
    pub(super) operation: [u8; 16],
    pub(super) node: [u8; 16],
    disk: [u8; 16],
    engine: [u8; 16],
    pub(super) epoch: u64,
    pub(super) revision: u64,
    tables: [Option<[u8; 32]>; 4],
    slot: [u8; 32],
    pub(super) histories: u64,
    namespaces: u64,
    separation: FleetReplicaSeparation,
}

impl Basis {
    pub(super) fn slot(&self) -> [u8; 32] {
        self.slot
    }

    pub(super) async fn check_current(&self, catalog: &FleetCatalog) -> Result<()> {
        let current = catalog.inner.current().await?;
        self.check(catalog, &current)?;
        // Operations may advance independently, but the original active
        // admission must still be rooted and linked to its canonical journal.
        let operation = cellar_logic::consensus::ProposalId::new(self.operation)
            .map_err(|_| FleetCatalogError::Corrupt("receipt operation"))?;
        catalog
            .checked_active_evacuation(&current, operation)
            .await?;
        Ok(())
    }

    pub(super) fn capture(
        catalog: &FleetCatalog,
        snapshot: &FleetSnapshot,
        active: &ActiveEvacuation,
        inventory: &FleetInventory,
    ) -> Result<Self> {
        catalog.check_snapshot(snapshot)?;
        let admission = active.admission();
        if admission.observed_revision() != snapshot.revision()
            || inventory.revision() != snapshot.revision()
            || inventory.histories() != snapshot.head.history_count
            || inventory.namespaces() as u64 != snapshot.head.namespace_count
            || snapshot.head.disruption != Some(active.slot())
        {
            return Err(FleetCatalogError::Conflict);
        }
        let result = Self {
            format: 1,
            catalog: catalog.birth_owner()?.catalog,
            cluster: snapshot.head.cluster,
            fleet: snapshot.head.incarnation,
            operation: admission.operation().bytes(),
            node: admission.node().bytes(),
            disk: admission.disk().bytes(),
            engine: admission.storage_instance().bytes(),
            epoch: admission.enrollment_epoch(),
            revision: snapshot.revision(),
            tables: BASIS_TABLES.map(|table| snapshot.head.tables[table.slot()]),
            slot: active.slot(),
            histories: inventory.histories(),
            namespaces: inventory.namespaces() as u64,
            separation: snapshot.replica_separation(),
        };
        result.validate()?;
        Ok(result)
    }

    fn validate(&self) -> Result<()> {
        if self.format != 1
            || self.catalog == [0; 32]
            || self.slot == [0; 32]
            || self.cluster == [0; 16]
            || self.fleet == [0; 16]
            || self.operation == [0; 16]
            || self.node == [0; 16]
            || self.disk == [0; 16]
            || self.engine == [0; 16]
            || self.epoch == 0
            || self.epoch >= u64::MAX - 2
            || self.revision == 0
            || self.histories > HISTORY_LIMIT
            || !(1..=namespaces::MAX_NAMESPACES as u64).contains(&self.namespaces)
            || self.tables[0].is_none()
            || self.tables[1].is_none()
            || self.histories != 0 && self.tables[2].is_none()
            || self
                .tables
                .iter()
                .flatten()
                .any(|digest| *digest == [0; 32])
        {
            return Err(FleetCatalogError::Corrupt("receipt basis"));
        }
        Ok(())
    }

    /// Own progress may advance Operations/journal. None of the inputs that
    /// established inventory, placement eligibility or permits may change.
    pub(super) fn check(&self, catalog: &FleetCatalog, snapshot: &FleetSnapshot) -> Result<()> {
        self.check_scope_and_inventory(catalog, snapshot)?;
        if self.tables != BASIS_TABLES.map(|table| snapshot.head.tables[table.slot()]) {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(())
    }

    /// Finalizing changed the target node row. Other boots do not restore a
    /// retired target's votes, but new inventory, policy or permits require
    /// replanning before a fresh stop can be admitted.
    pub(super) fn check_stop_inputs(
        &self,
        catalog: &FleetCatalog,
        snapshot: &FleetSnapshot,
    ) -> Result<()> {
        self.check_scope_and_inventory(catalog, snapshot)?;
        for index in [0, 2, 3] {
            if self.tables[index] != snapshot.head.tables[BASIS_TABLES[index].slot()] {
                return Err(FleetCatalogError::Conflict);
            }
        }
        Ok(())
    }

    fn check_scope_and_inventory(
        &self,
        catalog: &FleetCatalog,
        snapshot: &FleetSnapshot,
    ) -> Result<()> {
        catalog.check_snapshot(snapshot)?;
        self.validate()?;
        if self.catalog != catalog.birth_owner()?.catalog
            || self.cluster != snapshot.head.cluster
            || self.fleet != snapshot.head.incarnation
            || self.revision > snapshot.revision()
            || snapshot.head.disruption != Some(self.slot)
            || self.histories != snapshot.head.history_count
            || self.namespaces != snapshot.head.namespace_count
            || self.separation != snapshot.replica_separation()
        {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Publication {
    pub(super) applied: Position,
    pub(super) revision: u64,
    #[serde(deserialize_with = "nullable")]
    image_position: Option<Position>,
    #[serde(deserialize_with = "nullable")]
    image: Option<Image>,
}

impl From<&FleetPublishedCut> for Publication {
    fn from(cut: &FleetPublishedCut) -> Self {
        Self {
            applied: cut.applied().into(),
            revision: cut.membership_revision().get(),
            image_position: cut.image().map(|image| image.position().into()),
            image: cut.image().map(|image| Image {
                epoch: image.epoch(),
                txid: image.txid(),
                checksum: image.checksum(),
                page_size: image.page_size(),
                length: image.length(),
                digest: image.digest(),
            }),
        }
    }
}

fn validate_image(
    position: Position,
    origin: Option<Position>,
    image: Option<Image>,
) -> Result<()> {
    position
        .decode()
        .map_err(|_| FleetCatalogError::Corrupt("receipt position"))?;
    match (origin, image) {
        (None, None) => Ok(()),
        (Some(origin), Some(image)) => {
            origin
                .decode()
                .map_err(|_| FleetCatalogError::Corrupt("receipt publication"))?;
            image
                .decode()
                .map_err(|_| FleetCatalogError::Corrupt("receipt image"))?;
            if origin.index > position.index
                || origin.term > position.term
                || origin.index == position.index && origin.term != position.term
            {
                return Err(FleetCatalogError::Corrupt("receipt publication prefix"));
            }
            Ok(())
        }
        _ => Err(FleetCatalogError::Corrupt("receipt image binding")),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Archive {
    reference: ArchiveReference,
    position: Position,
    #[serde(deserialize_with = "nullable")]
    image_position: Option<Position>,
    #[serde(deserialize_with = "nullable")]
    image: Option<Image>,
}

impl From<&VerifiedArchiveCoverage> for Archive {
    fn from(proof: &VerifiedArchiveCoverage) -> Self {
        Self {
            reference: proof.reference().clone(),
            position: proof.position().into(),
            image_position: proof.image_position().map(Into::into),
            image: proof.image_state().map(Into::into),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Survivor {
    pub(super) node: [u8; 16],
    pub(super) disk: [u8; 16],
    pub(super) engine: [u8; 16],
    pub(super) process: [u8; 16],
    pub(super) physical_host: [u8; 16],
    pub(super) epoch: u64,
    pub(super) member: u64,
    pub(super) applied: Position,
    pub(super) publication_digest: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Evidence {
    Live(Box<LiveEvidence>),
    Cancelled { operation: [u8; 16] },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LiveEvidence {
    pub(super) fixed: Publication,
    pub(super) archive: Archive,
    pub(super) survivors: [Survivor; 3],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Entry {
    pub(super) history: [u8; 32],
    pub(super) record: [u8; 32],
    pub(super) evidence: Evidence,
}

impl Entry {
    fn validate(&self, basis: &Basis) -> Result<()> {
        if self.history == [0; 32] || self.record == [0; 32] {
            return Err(FleetCatalogError::Corrupt("receipt history"));
        }
        match &self.evidence {
            Evidence::Cancelled { operation } if *operation != [0; 16] => Ok(()),
            Evidence::Cancelled { .. } => Err(FleetCatalogError::Corrupt("receipt cancellation")),
            Evidence::Live(live) => {
                let LiveEvidence {
                    fixed,
                    archive,
                    survivors,
                } = live.as_ref();
                validate_image(fixed.applied, fixed.image_position, fixed.image)?;
                validate_image(archive.position, archive.image_position, archive.image)?;
                let selected = archive
                    .reference
                    .position()
                    .map_err(|_| FleetCatalogError::Corrupt("receipt archive reference"))?;
                if Position::from(selected) != archive.position
                    || fixed.revision == 0
                    || archive.position.index < fixed.applied.index
                    || archive.position.term < fixed.applied.term
                    || archive.position.index == fixed.applied.index
                        && archive.position.term != fixed.applied.term
                {
                    return Err(FleetCatalogError::Corrupt("receipt archive cut"));
                }
                if fixed.image.is_some() && archive.image.is_none()
                    || archive.image.is_some()
                        && archive.image != fixed.image
                        && archive
                            .image_position
                            .is_none_or(|origin| origin.index <= fixed.applied.index)
                {
                    return Err(FleetCatalogError::Corrupt("receipt archive publication"));
                }
                let mut nodes = BTreeSet::new();
                let mut members = BTreeSet::new();
                let mut hosts = BTreeSet::new();
                for survivor in survivors {
                    NodeId::new(survivor.node)
                        .map_err(|_| FleetCatalogError::Corrupt("receipt node"))?;
                    DiskLineage::new(survivor.disk)
                        .map_err(|_| FleetCatalogError::Corrupt("receipt disk"))?;
                    EngineInstanceId::new(survivor.engine)
                        .map_err(|_| FleetCatalogError::Corrupt("receipt engine"))?;
                    ProcessGeneration::new(survivor.process)
                        .map_err(|_| FleetCatalogError::Corrupt("receipt process"))?;
                    survivor
                        .applied
                        .decode()
                        .map_err(|_| FleetCatalogError::Corrupt("receipt survivor cut"))?;
                    if survivor.node == basis.node
                        || survivor.epoch == 0
                        || survivor.epoch >= u64::MAX - 2
                        || survivor.member == 0
                        || survivor.physical_host == [0; 16]
                        || survivor.publication_digest == [0; 32]
                        || !nodes.insert(survivor.node)
                        || !members.insert(survivor.member)
                        || survivor.applied.index < fixed.applied.index
                        || survivor.applied.term < fixed.applied.term
                        || survivor.applied.index == fixed.applied.index
                            && survivor.applied.term != fixed.applied.term
                    {
                        return Err(FleetCatalogError::Corrupt("receipt survivor"));
                    }
                    hosts.insert(survivor.physical_host);
                }
                if basis.separation == FleetReplicaSeparation::PhysicalHosts && hosts.len() != 3 {
                    return Err(FleetCatalogError::Corrupt("receipt physical separation"));
                }
                Ok(())
            }
        }
    }
}

/// An incremental immutable index, not an inventory or verification capability.
#[derive(Clone)]
pub(super) struct Book {
    pub(super) basis: Basis,
    index: Option<[u8; 32]>,
    count: u64,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Candidate {
    pub(super) basis: Basis,
    #[serde(deserialize_with = "nullable")]
    pub(super) index: Option<[u8; 32]>,
    pub(super) count: u64,
}

impl Book {
    pub(super) fn new(basis: Basis) -> Self {
        Self {
            basis,
            index: None,
            count: 0,
        }
    }

    /// At most sixteen bounded entries per turn. A failed batch leaves the
    /// previous book intact; orphan index pages admit no receipt or authority.
    pub(super) async fn append(&mut self, catalog: &FleetCatalog, entries: &[Entry]) -> Result<()> {
        self.basis.validate()?;
        if entries.is_empty() || entries.len() > APPEND_LIMIT {
            return Err(FleetCatalogError::Limit);
        }
        let mut index = self.index;
        let mut count = self.count;
        for entry in entries {
            entry.validate(&self.basis)?;
            let key = hex::encode(entry.history);
            let bytes = encode(ENTRY_KIND, entry, ENTRY_LIMIT)?;
            if let Some(previous) = catalog.inner.lookup(index, &key).await? {
                if previous != bytes {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                continue;
            }
            count = count
                .checked_add(1)
                .filter(|count| *count <= self.basis.histories)
                .ok_or(FleetCatalogError::Limit)?;
            index = catalog.inner.update(index, &key, Some(bytes), 0).await?;
        }
        self.index = index;
        self.count = count;
        Ok(())
    }

    /// Persist candidate material only. The terminal verifier must establish
    /// exact inventory/key coverage and consume its real in-memory proofs.
    pub(super) async fn candidate(&self, catalog: &FleetCatalog) -> Result<([u8; 32], Candidate)> {
        if self.count != self.basis.histories {
            return Err(FleetCatalogError::InventoryIncomplete);
        }
        let candidate = Candidate {
            basis: self.basis.clone(),
            index: self.index,
            count: self.count,
        };
        candidate.validate()?;
        let bytes = encode(CANDIDATE_KIND, &candidate, PAGE_LIMIT)?;
        let digest = hash(&bytes);
        catalog
            .inner
            .create_exact(
                &format!("receipts/{}", hex::encode(digest)),
                &bytes,
                PAGE_LIMIT,
            )
            .await?;
        // Confirm the selected immutable key. A lost create reply alone is
        // ambiguous, and a decoded descriptor never recreates verification.
        Ok((digest, Candidate::read(catalog, digest).await?))
    }
}

impl Candidate {
    fn validate(&self) -> Result<()> {
        self.basis.validate()?;
        if self.count != self.basis.histories
            || self.count > HISTORY_LIMIT
            || (self.count == 0) != self.index.is_none()
            || self.index == Some([0; 32])
        {
            return Err(FleetCatalogError::Corrupt("receipt candidate index"));
        }
        Ok(())
    }

    pub(super) async fn read(catalog: &FleetCatalog, digest: [u8; 32]) -> Result<Self> {
        let (candidate, _) = catalog
            .inner
            .read::<Self>(
                &format!("receipts/{}", hex::encode(digest)),
                CANDIDATE_KIND,
                PAGE_LIMIT,
            )
            .await?
            .ok_or(FleetCatalogError::Corrupt("missing receipt candidate"))?;
        candidate.validate()?;
        if hash(&encode(CANDIDATE_KIND, &candidate, PAGE_LIMIT)?) != digest
            || candidate.basis.catalog != catalog.birth_owner()?.catalog
        {
            return Err(FleetCatalogError::Corrupt("receipt candidate identity"));
        }
        Ok(candidate)
    }
}
