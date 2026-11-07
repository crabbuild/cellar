// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Conditional ownership of archive acquisitions during a range transition.

use super::transition_sources::TransitionSourceCut;
use super::*;
use crate::raft::{
    cell_groups::{
        CellGroupArchiveAcquireOutcome, CellGroupArchiveError, CellGroupArchiveExecution,
        CellGroupArchiveHold, CellGroupArchiveIntent, CellGroupArchivePool,
        CellGroupArchiveRegistry, GroupManifest,
    },
    fleet_catalog::ManagedTransitionPermit,
};
use serde::{Deserialize, Serialize};

const STAGED: &str = "transition-source-progress";
const KIND: u8 = 215;

type Result<T> = std::result::Result<T, CellGroupArchiveBindingError>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) enum ArchiveSlot {
    Acquiring(CellGroupArchiveIntent),
    Verified(CellGroupArchiveHold),
}

impl RangeDirectory {
    /// Root an exact acquire before touching the archive registry. An already
    /// verified slot returns its hold with no further physical work required.
    pub(crate) async fn acquire_managed_sealed_source(
        &self,
        domain: &impl CellGroupArchiveExecution,
        archives: &CellGroupArchivePool,
        plan: &DirectoryMutation,
        permit: &ManagedTransitionPermit,
        history: GroupHistory,
    ) -> Result<(CellGroupArchiveHold, Option<CellGroupArchiveIntent>)> {
        permit.check(self, plan)?;
        let (parents, _) = self.transition_routes(plan)?;
        let (index, route) = parents
            .iter()
            .enumerate()
            .find(|(_, route)| route.history == history)
            .ok_or(CatalogError::Fenced)?;
        let (journal, staged) = self
            .bounded(async {
                let (view, _) = self.current().await?;
                let journal = self
                    .matching_transition(&view.head, plan)
                    .await?
                    .ok_or(CatalogError::Fenced)?;
                journal.seal_authorization()?;
                self.selected_route_in(&view, route).await?;
                let staged = self.read_source_slot(plan, index).await?;
                Ok((journal, staged))
            })
            .await?;
        let staged = match staged {
            Some((ArchiveSlot::Verified(hold), _)) => {
                self.check_sealed_hold(plan, &journal, route, &hold)?;
                return Ok((hold, None));
            }
            Some((ArchiveSlot::Acquiring(intent), version)) => Some((intent, version)),
            None => None,
        };
        if journal.phase() != TransitionPhase::SealAuthorized {
            return Err(CatalogError::Fenced.into());
        }

        let birth = self.materialize_selected_history(history).await?;
        if birth.route() != route {
            return Err(CatalogError::Fenced.into());
        }
        let registry = self.open_group_archive(domain, archives, &birth).await?;
        let (hold, intent) = self
            .acquire_transition_archive_hold(
                domain,
                &registry,
                &source_slot_key(plan, index),
                staged,
                |marker| self.check_sealed_marker(plan, &journal, route, marker),
            )
            .await?;
        self.check_sealed_hold(plan, &journal, route, &hold)?;
        Ok((hold, Some(intent)))
    }

    /// Root the exact acquire before issuing it. An expired intent can be
    /// replaced only after the registry proves its nonce owns no hold.
    pub(super) async fn acquire_transition_archive_hold(
        &self,
        domain: &impl CellGroupArchiveExecution,
        registry: &CellGroupArchiveRegistry,
        key: &str,
        staged: Option<(CellGroupArchiveIntent, UpdateVersion)>,
        check: impl Fn(&GroupManifest) -> Result<()> + Send + Sync,
    ) -> Result<(CellGroupArchiveHold, CellGroupArchiveIntent)> {
        let (intent, version) = match staged {
            Some(staged) => staged,
            None => {
                let intent = registry.prepare_acquire(domain).await?;
                check(intent.acquire_marker()?)?;
                let version = self
                    .bounded(self.write_archive_slot(
                        key,
                        None,
                        &ArchiveSlot::Acquiring(intent.clone()),
                    ))
                    .await?;
                (intent, version)
            }
        };
        check(intent.acquire_marker()?)?;
        let hold = match registry.acquire(domain, &intent).await {
            Ok(hold) => hold,
            Err(error @ (CellGroupArchiveError::Expired | CellGroupArchiveError::Conflict)) => {
                match registry.acquire_outcome(domain, &intent).await? {
                    CellGroupArchiveAcquireOutcome::Held(hold) => *hold,
                    CellGroupArchiveAcquireOutcome::Pending => return Err(error.into()),
                    CellGroupArchiveAcquireOutcome::Superseded => {
                        let next = registry.prepare_acquire(domain).await?;
                        check(next.acquire_marker()?)?;
                        self.bounded(self.write_archive_slot(
                            key,
                            Some(version),
                            &ArchiveSlot::Acquiring(next),
                        ))
                        .await?;
                        return Err(CatalogError::Unconfirmed.into());
                    }
                }
            }
            Err(error) => return Err(error.into()),
        };
        Ok((hold, intent))
    }

    pub(super) fn check_sealed_hold(
        &self,
        plan: &DirectoryMutation,
        journal: &TransitionJournal,
        route: &GroupRoute,
        hold: &CellGroupArchiveHold,
    ) -> std::result::Result<(), CatalogError> {
        hold.check_reference()
            .map_err(|_| CatalogError::Corrupt("staged source hold"))?;
        self.check_source_marker(
            plan,
            journal,
            route,
            hold.marker(),
            TransitionSourceCut::Sealed,
        )
    }

    pub(super) fn check_sealed_intent(
        &self,
        plan: &DirectoryMutation,
        journal: &TransitionJournal,
        route: &GroupRoute,
        intent: &CellGroupArchiveIntent,
    ) -> Result<()> {
        self.check_sealed_marker(plan, journal, route, intent.acquire_marker()?)
    }

    fn check_sealed_marker(
        &self,
        plan: &DirectoryMutation,
        journal: &TransitionJournal,
        route: &GroupRoute,
        marker: &GroupManifest,
    ) -> Result<()> {
        self.check_source_marker(plan, journal, route, marker, TransitionSourceCut::Sealed)
            .map_err(|error| match error {
                // Archive publication can lag the already applied seal.
                CatalogError::Fenced if marker.route() == route && marker.is_active() => {
                    CatalogError::Unconfirmed.into()
                }
                error => error.into(),
            })
    }

    pub(super) async fn claim_staged_source_hold(
        &self,
        plan: &DirectoryMutation,
        index: usize,
        hold: &CellGroupArchiveHold,
        intent: Option<&CellGroupArchiveIntent>,
    ) -> std::result::Result<(), CatalogError> {
        let version = match (self.read_source_slot(plan, index).await?, intent) {
            (Some((ArchiveSlot::Verified(selected), _)), _) if &selected == hold => return Ok(()),
            (Some((ArchiveSlot::Acquiring(selected), version)), Some(intent))
                if &selected == intent =>
            {
                Some(version)
            }
            (None, None) => None,
            _ => return Err(CatalogError::Conflict("staged source hold owner changed")),
        };
        self.write_archive_slot(
            &source_slot_key(plan, index),
            version,
            &ArchiveSlot::Verified(hold.clone()),
        )
        .await?;
        Ok(())
    }

    pub(super) async fn read_source_slot(
        &self,
        plan: &DirectoryMutation,
        index: usize,
    ) -> std::result::Result<Option<(ArchiveSlot, UpdateVersion)>, CatalogError> {
        self.read_archive_slot(&source_slot_key(plan, index)).await
    }

    pub(super) async fn read_archive_slot(
        &self,
        key: &str,
    ) -> std::result::Result<Option<(ArchiveSlot, UpdateVersion)>, CatalogError> {
        self.namespace
            .inner
            .read_versioned_bounded(&self.key(key), wire::LIMIT)
            .await?
            .map(|(bytes, version)| {
                if version.e_tag.is_none() && version.version.is_none() {
                    return Err(CatalogError::Corrupt("source slot conditional version"));
                }
                Ok((record::decode(KIND, &bytes, wire::LIMIT)?, version))
            })
            .transpose()
    }

    async fn write_archive_slot(
        &self,
        suffix: &str,
        version: Option<UpdateVersion>,
        slot: &ArchiveSlot,
    ) -> std::result::Result<UpdateVersion, CatalogError> {
        let bytes = record::encode(KIND, slot, wire::LIMIT)?;
        let key = self.key(suffix);
        let store = &self.namespace.inner;
        if let Some(version) = version {
            let _ = store
                .store
                .put_opts(
                    &key,
                    bytes.into(),
                    PutOptions::from(PutMode::Update(version)),
                )
                .await;
        } else {
            store
                .create_and_observe_bounded(&key, &bytes, wire::LIMIT)
                .await?;
        }
        let (observed, version) = self
            .read_archive_slot(suffix)
            .await?
            .ok_or(CatalogError::Unconfirmed)?;
        if &observed != slot {
            return Err(CatalogError::Conflict("staged source hold owner changed"));
        }
        Ok(version)
    }
}

fn source_slot_key(plan: &DirectoryMutation, index: usize) -> String {
    format!("{STAGED}/{}/{index}", hex::encode(plan.digest()))
}
