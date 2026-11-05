// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A remote transfer closure references the exact held parent data and the
//! independently reconstructed child roots selected by the installation CAS.
use super::{transition_sources::TransitionSourceCut, *};
use crate::raft::cell_groups::{
    CellGroupArchiveError, CellGroupArchiveExecution, CellGroupArchiveHold, CellGroupArchivePool,
    GroupManifest,
};
use crate::raft::fleet_catalog::ManagedTransitionPermit;
use serde::{Deserialize, Serialize};

const CLOSURE: u8 = 207;
const CLOSURES: &str = "transfer-archive-closures";

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Closure {
    namespace: [u8; 32],
    plan: [u8; 32],
    sealed_sources: [u8; 32],
    installed_children: [u8; 32],
}

/// Immutable archive references selected by the directory. Recovery must
/// independently replay the holds before using their data.
pub struct CellGroupTransferArchive {
    pub digest: [u8; 32],
    pub source_holds: Vec<CellGroupArchiveHold>,
    pub children: Vec<GroupManifest>,
}

impl RangeDirectory {
    /// Independently recover every held parent archive and rebuild the complete
    /// child partition in dedicated verification stores. Persist the closure
    /// only if the rebuilt roots match the installed children exactly. Keep
    /// the original remote holds until the transition owner retires them.
    pub async fn verify_and_record_transfer_archive(
        &self,
        domain: &impl CellGroupArchiveExecution,
        plan: &DirectoryMutation,
        inputs: CellGroupTransitionPreparationInputs,
    ) -> Result<TransitionJournal, CellGroupArchiveBindingError> {
        self.unmanaged_birth()?;
        self.verify_and_record_transfer_archive_inner(domain, plan, inputs, None)
            .await
    }

    pub(crate) async fn verify_and_record_managed_transfer_archive(
        &self,
        domain: &impl CellGroupArchiveExecution,
        plan: &DirectoryMutation,
        inputs: CellGroupTransitionPreparationInputs,
        permit: &ManagedTransitionPermit,
    ) -> Result<TransitionJournal, CellGroupArchiveBindingError> {
        permit.check(self, plan)?;
        self.verify_and_record_transfer_archive_inner(domain, plan, inputs, Some(permit))
            .await
    }

    async fn verify_and_record_transfer_archive_inner(
        &self,
        domain: &impl CellGroupArchiveExecution,
        plan: &DirectoryMutation,
        inputs: CellGroupTransitionPreparationInputs,
        permit: Option<&ManagedTransitionPermit>,
    ) -> Result<TransitionJournal, CellGroupArchiveBindingError> {
        domain
            .run(Box::pin(async {
                let children = self
                    .installed_transfer_children_inner(plan, permit)
                    .await?
                    .ok_or(CatalogError::Fenced)?;
                let holds = inputs
                    .sources
                    .iter()
                    .map(|source| (source.birth.clone(), source.hold.clone()))
                    .collect::<Vec<_>>();
                let sources = Box::pin(self.verify_and_record_sealed_sources_inner(
                    domain,
                    &inputs.archives,
                    &inputs.tails,
                    plan,
                    inputs.sources,
                    permit,
                ))
                .await?;
                let journal = sources.journal();
                let sealed_sources = journal.sealed_sources().ok_or(CatalogError::Fenced)?;
                let installed_children =
                    journal.installed_children().ok_or(CatalogError::Fenced)?;
                let partition = inputs
                    .restore
                    .prepare_transfer_partition(domain, sources, inputs.targets)
                    .await?;
                if partition.children().len() != children.len()
                    || partition
                        .children()
                        .iter()
                        .zip(&children)
                        .any(|(rebuilt, installed)| {
                            rebuilt.genesis().is_none_or(|genesis| {
                                let marker = genesis.marker();
                                marker.route() != installed.route()
                                    || !installed.preserves_transfer(marker)
                            })
                        })
                {
                    return Err(CatalogError::Conflict("rebuilt transfer child root").into());
                }

                let closure = Closure {
                    namespace: self.namespace.root_digest,
                    plan: plan.digest(),
                    sealed_sources,
                    installed_children,
                };
                let bytes = record::encode(CLOSURE, &closure, wire::LIMIT)?;
                let digest = record::digest(&bytes);
                self.bounded(self.immutable(CLOSURES, digest, &bytes))
                    .await?;
                for (birth, hold) in holds {
                    let registry = self
                        .open_group_archive(domain, &inputs.archives, &birth)
                        .await?;
                    registry.require_retained_hold(domain, &hold).await?;
                }
                self.record_verified_transfer_archive(
                    plan,
                    sealed_sources,
                    installed_children,
                    digest,
                    permit,
                )
                .await
                .map_err(Into::into)
            }))
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }

    /// Atomically select the archived child ranges. An exact retry confirms
    /// the same publication, including when the first CAS response was lost.
    /// Children remain fenced until their own replicated activation.
    pub async fn publish_transfer(
        &self,
        domain: &impl CellGroupArchiveExecution,
        archives: &CellGroupArchivePool,
        plan: &DirectoryMutation,
    ) -> Result<DirectoryPublication, CellGroupArchiveBindingError> {
        self.unmanaged_birth()?;
        self.check_transition_plan(plan)?;
        domain
            .run(Box::pin(async {
                self.namespace.require_cell_group_unmanaged().await?;
                if let Some(publication) = self.confirmed_transfer_publication(plan).await? {
                    return Ok(publication);
                }
                let attempt = async {
                    let closure = self
                        .transfer_archive(plan)
                        .await?
                        .ok_or(CatalogError::Fenced)?;
                    for hold in &closure.source_holds {
                        let birth = self
                            .materialize_selected_history(hold.marker().route().history)
                            .await?;
                        let registry = self.open_group_archive(domain, archives, &birth).await?;
                        registry.require_retained_hold(domain, hold).await?;
                    }
                    self.bounded(
                        self.publish_inner(plan, PublicationAuthority::Transfer(closure.digest)),
                    )
                    .await
                    .map_err(Into::into)
                }
                .await;
                match attempt {
                    Ok(publication) => Ok(publication),
                    // Another controller may have published this exact plan
                    // while this attempt was reopening the parent archives.
                    Err(error) => match self.confirmed_transfer_publication(plan).await {
                        Ok(Some(publication)) => Ok(publication),
                        _ => Err(error),
                    },
                }
            }))
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }

    pub(super) async fn confirmed_transfer_publication(
        &self,
        plan: &DirectoryMutation,
    ) -> Result<Option<DirectoryPublication>, CatalogError> {
        let publication = self.publication(plan).await?;
        if publication
            .as_ref()
            .is_some_and(|publication| publication.transfer_archive().is_none())
        {
            return Err(CatalogError::Fenced);
        }
        Ok(publication)
    }

    /// Reopen the exact closure selected by an earlier CAS, including after a
    /// lost response. This reads remote references without opening voter disks.
    pub async fn transfer_archive(
        &self,
        plan: &DirectoryMutation,
    ) -> Result<Option<CellGroupTransferArchive>, CatalogError> {
        self.unmanaged_birth()?;
        self.transfer_archive_inner(plan, None).await
    }

    pub(crate) async fn managed_transfer_archive(
        &self,
        plan: &DirectoryMutation,
        permit: &ManagedTransitionPermit,
    ) -> Result<Option<CellGroupTransferArchive>, CatalogError> {
        permit.check(self, plan)?;
        self.transfer_archive_inner(plan, Some(permit)).await
    }

    async fn transfer_archive_inner(
        &self,
        plan: &DirectoryMutation,
        permit: Option<&ManagedTransitionPermit>,
    ) -> Result<Option<CellGroupTransferArchive>, CatalogError> {
        self.check_transition_plan(plan)?;
        self.bounded(async {
            let (view, _) = self.current().await?;
            let journal = self
                .matching_transition(&view.head, plan)
                .await?
                .ok_or(CatalogError::Fenced)?;
            let Some(digest) = journal.transfer_archive() else {
                return Ok(None);
            };
            let bytes = self
                .read(&format!("{CLOSURES}/{}", hex::encode(digest)))
                .await?
                .ok_or(CatalogError::Corrupt("missing transfer archive closure"))?;
            if record::digest(&bytes) != digest {
                return Err(CatalogError::Corrupt("transfer archive closure digest"));
            }
            let closure: Closure = record::decode(CLOSURE, &bytes, wire::LIMIT)?;
            if closure.namespace != self.namespace.root_digest
                || closure.plan != plan.digest()
                || journal.sealed_sources() != Some(closure.sealed_sources)
                || journal.installed_children() != Some(closure.installed_children)
            {
                return Err(CatalogError::Corrupt("transfer archive closure binding"));
            }
            let source_holds = self
                .read_transition_source_holds(
                    plan,
                    &journal,
                    closure.sealed_sources,
                    TransitionSourceCut::Sealed,
                )
                .await?;
            let children = self
                .installed_transfer_children_inner(plan, permit)
                .await?
                .ok_or(CatalogError::Corrupt("missing archived child install set"))?;
            Ok(Some(CellGroupTransferArchive {
                digest,
                source_holds,
                children,
            }))
        })
        .await
    }
}
