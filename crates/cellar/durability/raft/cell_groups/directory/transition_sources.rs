// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Independently replay each held parent closure before rooting the sealed set.
use super::*;
use crate::raft::cell_groups::{
    CellGroupArchiveCheckpointReceiver, CellGroupArchiveCoverage, CellGroupArchiveExecution,
    CellGroupArchiveHold, CellGroupArchivePool, CellGroupArchiveTailPool, GroupManifest,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const SOURCE_HOLD: u8 = 199;
const SOURCE_SET: u8 = 200;
const HOLDS: &str = "transition-source-holds";
const SETS: &str = "transition-source-sets";

type Result<T> = std::result::Result<T, CellGroupArchiveBindingError>;

/// The explicit archive hold belongs to the transition owner until retirement.
/// The receiver must have its own physical recovery admission and stores.
pub struct CellGroupTransitionSource {
    pub birth: CellGroupBirth,
    pub hold: CellGroupArchiveHold,
    pub receiver: CellGroupArchiveCheckpointReceiver,
}

/// Independently recovered source images and their selected catalog decision.
/// This grants no child boot, installation, publication or serving authority.
pub struct VerifiedCellGroupTransitionSources {
    journal: TransitionJournal,
    sources: Vec<CellGroupArchiveCoverage>,
}

impl VerifiedCellGroupTransitionSources {
    pub fn journal(&self) -> &TransitionJournal {
        &self.journal
    }

    pub fn sources(&self) -> &[CellGroupArchiveCoverage] {
        &self.sources
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourceSet {
    namespace: [u8; 32],
    request: [u8; 32],
    holds: Vec<[u8; 32]>,
}

impl RangeDirectory {
    fn check_source_marker(
        &self,
        mutation: &DirectoryMutation,
        journal: &TransitionJournal,
        route: &GroupRoute,
        marker: &GroupManifest,
    ) -> std::result::Result<(), CatalogError> {
        let (transition, authorization, entry) =
            self.source_seal_entry(mutation, journal, route)?;
        let payload = Sha256::digest(&entry).into();
        if marker.route() != route
            || marker
                .source_seal()
                .is_none_or(|seal| !seal.matches(transition, authorization, payload))
        {
            return Err(CatalogError::Fenced);
        }
        Ok(())
    }

    /// Verify every exact held parent cut in plan order, then select the whole
    /// set in the namespace CAS. Persist the hold allocations before calling.
    /// Keep those remote holds until the transition owner retires all users.
    /// Retry with the identical set after a lost response and independently
    /// verify the restored holds again before using their data.
    pub async fn verify_and_record_sealed_sources(
        &self,
        domain: &impl CellGroupArchiveExecution,
        archives: &CellGroupArchivePool,
        tails: &CellGroupArchiveTailPool,
        mutation: &DirectoryMutation,
        sources: Vec<CellGroupTransitionSource>,
    ) -> Result<VerifiedCellGroupTransitionSources> {
        self.unmanaged_birth()?;
        self.check_transition_plan(mutation)?;
        if sources.len() != mutation.request.parents.len() {
            return Err(CatalogError::Invalid("incomplete transition source set").into());
        }
        domain
            .run(async {
                let (view, journal) = self
                    .bounded(async {
                        let (view, _) = self.current().await?;
                        let journal = self
                            .matching_transition(&view.head, mutation)
                            .await?
                            .ok_or(CatalogError::Fenced)?;
                        journal.seal_authorization()?;
                        Ok((view, journal))
                    })
                    .await?;
                let mut encoded = Vec::with_capacity(sources.len());
                let mut admitted = Vec::with_capacity(sources.len());
                for (parent, source) in mutation.request.parents.iter().zip(sources) {
                    let route = parent.decode(self)?;
                    if source.birth.route() != &route {
                        return Err(CatalogError::Fenced.into());
                    }
                    self.bounded(self.selected_route_in(&view, &route)).await?;
                    source.hold.check_reference()?;
                    self.check_source_marker(mutation, &journal, &route, source.hold.marker())?;
                    let bytes = record::encode(SOURCE_HOLD, &source.hold, wire::LIMIT)?;
                    encoded.push((record::digest(&bytes), bytes));
                    admitted.push(source);
                }
                let set = SourceSet {
                    namespace: self.namespace.root_digest,
                    request: mutation.digest,
                    holds: encoded.iter().map(|(digest, _)| *digest).collect(),
                };
                let bytes = record::encode(SOURCE_SET, &set, wire::LIMIT)?;
                let digest = record::digest(&bytes);
                if journal
                    .sealed_sources()
                    .is_some_and(|selected| selected != digest)
                {
                    return Err(CatalogError::Conflict("transition source set changed").into());
                }
                let mut verified = Vec::with_capacity(admitted.len());
                for source in admitted {
                    let registry = self
                        .open_group_archive(domain, archives, &source.birth)
                        .await?;
                    let coverage = registry
                        .verify_hold(domain, &source.hold, tails, source.receiver)
                        .await?;
                    self.check_source_marker(
                        mutation,
                        &journal,
                        source.birth.route(),
                        coverage.marker(),
                    )?;
                    verified.push(coverage);
                }
                self.bounded(async {
                    for (id, bytes) in encoded {
                        self.immutable(HOLDS, id, &bytes).await?;
                    }
                    self.immutable(SETS, digest, &bytes).await
                })
                .await?;
                let journal = self.record_verified_sources(mutation, digest).await?;
                Ok(VerifiedCellGroupTransitionSources {
                    journal,
                    sources: verified,
                })
            })
            .await
            .map_err(|_| crate::raft::cell_groups::CellGroupArchiveError::Stopped)?
    }

    /// Reopen the original hold descriptors after a lost controller. They are
    /// references to verify again, never recovered images or install authority.
    pub async fn sealed_source_holds(
        &self,
        mutation: &DirectoryMutation,
    ) -> Result<Vec<CellGroupArchiveHold>> {
        self.check_transition_plan(mutation)?;
        self.bounded(async {
            let (view, _) = self.current().await?;
            let journal = self
                .matching_transition(&view.head, mutation)
                .await?
                .ok_or(CatalogError::Fenced)?;
            let digest = journal.sealed_sources().ok_or(CatalogError::Fenced)?;
            let bytes = self
                .read(&format!("{SETS}/{}", hex::encode(digest)))
                .await?
                .ok_or(CatalogError::Corrupt("missing transition source set"))?;
            if record::digest(&bytes) != digest {
                return Err(CatalogError::Corrupt("transition source set digest"));
            }
            let set: SourceSet = record::decode(SOURCE_SET, &bytes, wire::LIMIT)?;
            if set.namespace != self.namespace.root_digest
                || set.request != mutation.digest
                || set.holds.len() != mutation.request.parents.len()
            {
                return Err(CatalogError::Corrupt("transition source set binding"));
            }
            let mut holds = Vec::with_capacity(set.holds.len());
            for (parent, id) in mutation.request.parents.iter().zip(set.holds) {
                let bytes = self
                    .read(&format!("{HOLDS}/{}", hex::encode(id)))
                    .await?
                    .ok_or(CatalogError::Corrupt("missing transition source hold"))?;
                if record::digest(&bytes) != id {
                    return Err(CatalogError::Corrupt("transition source hold digest"));
                }
                let hold: CellGroupArchiveHold = record::decode(SOURCE_HOLD, &bytes, wire::LIMIT)?;
                hold.check_reference()
                    .map_err(|_| CatalogError::Corrupt("transition source hold"))?;
                self.check_source_marker(mutation, &journal, &parent.decode(self)?, hold.marker())?;
                holds.push(hold);
            }
            Ok(holds)
        })
        .await
        .map_err(Into::into)
    }
}
