// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One conditional slot owns each irreversible parent archive acquisition.

use super::transition_sources::TransitionSourceCut;
use super::*;
use crate::raft::{
    cell_groups::{
        CellGroupArchiveAcquireOutcome, CellGroupArchiveError, CellGroupArchiveExecution,
        CellGroupArchiveHold, CellGroupArchiveIntent, CellGroupArchivePool,
    },
    fleet_catalog::ManagedTransitionPermit,
};
use serde::{Deserialize, Serialize};

const STAGED: &str = "transition-source-progress";
const KIND: u8 = 215;

type Result<T> = std::result::Result<T, CellGroupArchiveBindingError>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) enum SourceSlot {
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
            Some((SourceSlot::Verified(hold), _)) => {
                self.check_sealed_hold(plan, &journal, route, &hold)?;
                return Ok((hold, None));
            }
            Some((SourceSlot::Acquiring(intent), version)) => Some((intent, version)),
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
        let (intent, version) = match staged {
            Some(staged) => staged,
            None => {
                let intent = registry.prepare_acquire(domain).await?;
                self.check_sealed_intent(plan, &journal, route, &intent)?;
                let version = self
                    .bounded(self.write_source_slot(
                        plan,
                        index,
                        None,
                        &SourceSlot::Acquiring(intent.clone()),
                    ))
                    .await?;
                (intent, version)
            }
        };
        self.check_sealed_intent(plan, &journal, route, &intent)?;
        let hold = match registry.acquire(domain, &intent).await {
            Ok(hold) => hold,
            Err(error @ (CellGroupArchiveError::Expired | CellGroupArchiveError::Conflict)) => {
                match registry.acquire_outcome(domain, &intent).await? {
                    CellGroupArchiveAcquireOutcome::Held(hold) => *hold,
                    CellGroupArchiveAcquireOutcome::Pending => return Err(error.into()),
                    CellGroupArchiveAcquireOutcome::Superseded => {
                        let next = registry.prepare_acquire(domain).await?;
                        self.check_sealed_intent(plan, &journal, route, &next)?;
                        self.bounded(self.write_source_slot(
                            plan,
                            index,
                            Some(version),
                            &SourceSlot::Acquiring(next),
                        ))
                        .await?;
                        return Err(CatalogError::Unconfirmed.into());
                    }
                }
            }
            Err(error) => return Err(error.into()),
        };
        self.check_sealed_hold(plan, &journal, route, &hold)?;
        Ok((hold, Some(intent)))
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
        let marker = intent.acquire_marker()?;
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
            (Some((SourceSlot::Verified(selected), _)), _) if &selected == hold => return Ok(()),
            (Some((SourceSlot::Acquiring(selected), version)), Some(intent))
                if &selected == intent =>
            {
                Some(version)
            }
            (None, None) => None,
            _ => return Err(CatalogError::Conflict("staged source hold owner changed")),
        };
        self.write_source_slot(plan, index, version, &SourceSlot::Verified(hold.clone()))
            .await?;
        Ok(())
    }

    pub(super) async fn read_source_slot(
        &self,
        plan: &DirectoryMutation,
        index: usize,
    ) -> std::result::Result<Option<(SourceSlot, UpdateVersion)>, CatalogError> {
        self.namespace
            .inner
            .read_versioned_bounded(&self.key(&source_slot_key(plan, index)), wire::LIMIT)
            .await?
            .map(|(bytes, version)| {
                if version.e_tag.is_none() && version.version.is_none() {
                    return Err(CatalogError::Corrupt("source slot conditional version"));
                }
                Ok((record::decode(KIND, &bytes, wire::LIMIT)?, version))
            })
            .transpose()
    }

    async fn write_source_slot(
        &self,
        plan: &DirectoryMutation,
        index: usize,
        version: Option<UpdateVersion>,
        slot: &SourceSlot,
    ) -> std::result::Result<UpdateVersion, CatalogError> {
        let bytes = record::encode(KIND, slot, wire::LIMIT)?;
        let key = self.key(&source_slot_key(plan, index));
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
            .read_source_slot(plan, index)
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
