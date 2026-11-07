// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Rebuild the immutable publication baseline without granting live authority.
use super::{transition_sources::TransitionSourceCut, *};
use crate::raft::{
    cell_groups::{CellGroupArchiveExecution, CellGroupArchiveHold, PreparedCellGroupTransfer},
    fleet_catalog::ManagedTransitionPermit,
};

type Result<T> = std::result::Result<T, CellGroupArchiveBindingError>;

impl RangeDirectory {
    // Historical metadata is readable only for this still-owned publication.
    // It cannot satisfy ordinary selected-birth or archive admission.
    pub(crate) async fn managed_transfer_baseline_sources(
        &self,
        plan: &DirectoryMutation,
        permit: &ManagedTransitionPermit,
    ) -> Result<Vec<(CellGroupBirth, CellGroupArchiveHold)>> {
        permit.check(self, plan)?;
        let closure = self
            .managed_transfer_archive(plan, permit)
            .await?
            .ok_or(CatalogError::Fenced)?;
        self.bounded(async {
            let (view, _) = self.current().await?;
            let journal = self
                .matching_transition(&view.head, plan)
                .await?
                .ok_or(CatalogError::Fenced)?;
            let publication = self
                .confirmed_transfer_publication(plan)
                .await?
                .ok_or(CatalogError::Fenced)?;
            if journal.phase() != TransitionPhase::Published
                || journal.parent_holds_released()
                || publication.transfer_archive() != Some(closure.digest)
            {
                return Err(CatalogError::Fenced);
            }
            let (parents, _) = self.transition_routes(plan)?;
            if parents.len() != closure.source_holds.len() {
                return Err(CatalogError::Corrupt("published transfer source count"));
            }
            let mut sources = Vec::with_capacity(parents.len());
            for (route, hold) in parents.iter().zip(closure.source_holds) {
                if hold.marker().route() != route {
                    return Err(CatalogError::Fenced);
                }
                sources.push((self.open_historical_birth(route).await?, hold));
            }
            Ok(sources)
        })
        .await
        .map_err(Into::into)
    }

    pub(crate) async fn replay_managed_transfer_baseline(
        &self,
        domain: &impl CellGroupArchiveExecution,
        plan: &DirectoryMutation,
        permit: &ManagedTransitionPermit,
        inputs: CellGroupTransitionPreparationInputs,
    ) -> Result<PreparedCellGroupTransfer> {
        permit.check(self, plan)?;
        let (before, journal) = self
            .bounded(async {
                let (view, _) = self.current().await?;
                let journal = self
                    .matching_transition(&view.head, plan)
                    .await?
                    .ok_or(CatalogError::Fenced)?;
                if journal.phase() != TransitionPhase::Published || journal.parent_holds_released()
                {
                    return Err(CatalogError::Fenced);
                }
                Ok((view, journal))
            })
            .await?;
        let publication = self
            .confirmed_transfer_publication(plan)
            .await?
            .ok_or(CatalogError::Fenced)?;
        let closure = self
            .managed_transfer_archive(plan, permit)
            .await?
            .ok_or(CatalogError::Fenced)?;
        if publication.transfer_archive() != Some(closure.digest) {
            return Err(CatalogError::Fenced.into());
        }
        let (parents, children) = self.transition_routes(plan)?;
        if parents.len() != closure.source_holds.len() || children.len() != closure.children.len() {
            return Err(CatalogError::Corrupt("published transfer closure domain").into());
        }
        let verified = self
            .verify_transition_source_set(
                domain,
                &inputs.archives,
                &inputs.tails,
                plan,
                inputs.sources,
                TransitionSourceCut::PublishedBaseline,
            )
            .await?;
        let sources = VerifiedCellGroupTransitionSources::from_replay(plan, verified);
        let restored = inputs
            .restore
            .prepare_transfer_partition(domain, sources, inputs.targets)
            .await?;
        if restored.children().len() != closure.children.len()
            || restored
                .children()
                .iter()
                .zip(&closure.children)
                .any(|(rebuilt, installed)| {
                    rebuilt.genesis().is_none_or(|genesis| {
                        let marker = genesis.marker();
                        marker.route() != installed.route() || !installed.preserves_transfer(marker)
                    })
                })
        {
            return Err(CatalogError::Conflict("published transfer baseline roots").into());
        }
        // The original remote holds remain owned by the transition. Reading a
        // historical birth here grants no ordinary archive or bootstrap access.
        for (route, hold) in parents.iter().zip(&closure.source_holds) {
            self.open_retired_source_archive(domain, &inputs.archives, route)
                .await?
                .require_retained_hold(domain, hold)
                .await?;
        }
        let (after, current) = self
            .bounded(async {
                let (view, _) = self.current().await?;
                let current = self
                    .matching_transition(&view.head, plan)
                    .await?
                    .ok_or(CatalogError::Fenced)?;
                Ok((view, current))
            })
            .await?;
        if before.digest != after.digest
            || current != journal
            || self.confirmed_transfer_publication(plan).await?.as_ref() != Some(&publication)
        {
            return Err(CatalogError::Fenced.into());
        }
        Ok(restored)
    }
}
