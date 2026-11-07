// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Root operation-owned archive acquires before allocating pre-copy holds.
use super::*;
use crate::raft::{
    cell_groups::{
        CellGroupArchiveAcquireOutcome, CellGroupArchiveError, CellGroupArchiveExecution,
        CellGroupArchiveIntent, CellGroupArchivePool,
    },
    fleet_catalog::ManagedTransitionPermit,
};
use serde::{Deserialize, Serialize};

const ACQUIRES: &str = "transition-preparation-acquires";
const KIND: u8 = 214;

type Result<T> = std::result::Result<T, CellGroupArchiveBindingError>;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Acquires {
    namespace: [u8; 32],
    plan: [u8; 32],
    intents: Vec<CellGroupArchiveIntent>,
}

struct Selected {
    digest: [u8; 32],
    intents: Vec<CellGroupArchiveIntent>,
}

impl RangeDirectory {
    /// Acquire holds using only the intents selected by the directory. A
    /// proven superseded intent can be replaced while the operation is Planned.
    pub(crate) async fn allocate_managed_preparation_holds(
        &self,
        domain: &impl CellGroupArchiveExecution,
        archives: &CellGroupArchivePool,
        plan: &DirectoryMutation,
        permit: &ManagedTransitionPermit,
    ) -> Result<TransitionJournal> {
        permit.check(self, plan)?;
        let (journal, selected) = self.preparation_acquires(plan).await?;
        if !matches!(
            journal.phase(),
            TransitionPhase::Planned | TransitionPhase::Prepared
        ) {
            return Err(CatalogError::Fenced.into());
        }
        if journal.preparation_sources().is_some() {
            return if selected.is_some() {
                Ok(journal)
            } else {
                Err(CatalogError::Conflict("preparation has caller-owned holds").into())
            };
        }
        if journal.phase() != TransitionPhase::Planned {
            return Err(CatalogError::Conflict("preparation already selected").into());
        }

        let (parents, _) = self.transition_routes(plan)?;
        let mut registries = Vec::with_capacity(parents.len());
        for parent in &parents {
            let birth = self.materialize_selected_history(parent.history).await?;
            if birth.route() != parent {
                return Err(CatalogError::Fenced.into());
            }
            registries.push(self.open_group_archive(domain, archives, &birth).await?);
        }
        let selected = match selected {
            Some(selected) => selected,
            None => {
                let mut intents = Vec::with_capacity(registries.len());
                for registry in &registries {
                    intents.push(registry.prepare_acquire(domain).await?);
                }
                self.choose_preparation_acquires(plan, permit, None, intents)
                    .await?
            }
        };

        let mut holds = Vec::with_capacity(parents.len());
        for (index, registry) in registries.iter().enumerate() {
            let intent = &selected.intents[index];
            let hold = match registry.acquire(domain, intent).await {
                Ok(hold) => hold,
                Err(error @ (CellGroupArchiveError::Expired | CellGroupArchiveError::Conflict)) => {
                    match registry.acquire_outcome(domain, intent).await? {
                        CellGroupArchiveAcquireOutcome::Held(hold) => *hold,
                        CellGroupArchiveAcquireOutcome::Pending => return Err(error.into()),
                        CellGroupArchiveAcquireOutcome::Superseded => {
                            // The old revision is consumed and owns no hold.
                            // A successor controller must reopen the new set.
                            let mut intents = selected.intents;
                            intents[index] = registry.prepare_acquire(domain).await?;
                            self.choose_preparation_acquires(
                                plan,
                                permit,
                                Some(selected.digest),
                                intents,
                            )
                            .await?;
                            return Err(CatalogError::Unconfirmed.into());
                        }
                    }
                }
                Err(error) => return Err(error.into()),
            };
            holds.push(hold);
        }
        self.stage_managed_preparation_holds(
            domain,
            archives,
            plan,
            permit,
            &holds,
            Some(selected.digest),
        )
        .await
    }

    /// Reconcile each selected acquire after cancellation. The caller joins
    /// entered physical pre-copy work before using this cleanup boundary.
    pub(crate) async fn release_cancelled_preparation_acquires(
        &self,
        domain: &impl CellGroupArchiveExecution,
        archives: &CellGroupArchivePool,
        plan: &DirectoryMutation,
    ) -> Result<()> {
        let (journal, selected) = self.preparation_acquires(plan).await?;
        if journal.phase() != TransitionPhase::Cancelled {
            return Err(CatalogError::Fenced.into());
        }
        if let Some(selected) = selected {
            let (parents, _) = self.transition_routes(plan)?;
            for (route, intent) in parents.iter().zip(&selected.intents) {
                let registry = self
                    .open_retired_source_archive(domain, archives, route)
                    .await?;
                registry.cancel_acquire(domain, intent).await?;
            }
        }
        Ok(())
    }

    async fn preparation_acquires(
        &self,
        plan: &DirectoryMutation,
    ) -> Result<(TransitionJournal, Option<Selected>)> {
        if self.namespace_config().managed_fleet.is_none() {
            return Err(CatalogError::ManagedAdmissionRequired.into());
        }
        self.check_transition_plan(plan)?;
        let (journal, stored) = self
            .bounded(async {
                let (view, _) = self.current().await?;
                let journal = self
                    .matching_transition(&view.head, plan)
                    .await?
                    .ok_or(CatalogError::Fenced)?;
                let stored = if let Some(digest) = journal.preparation_acquires() {
                    let bytes = self
                        .read(&format!("{ACQUIRES}/{}", hex::encode(digest)))
                        .await?
                        .ok_or(CatalogError::Corrupt("missing preparation acquires"))?;
                    if record::digest(&bytes) != digest {
                        return Err(CatalogError::Corrupt("preparation acquire digest"));
                    }
                    let acquires: Acquires = record::decode(KIND, &bytes, wire::LIMIT)?;
                    if acquires.namespace != self.namespace.root_digest
                        || acquires.plan != plan.digest()
                    {
                        return Err(CatalogError::Corrupt("preparation acquire binding"));
                    }
                    Some(Selected {
                        digest,
                        intents: acquires.intents,
                    })
                } else {
                    None
                };
                Ok((journal, stored))
            })
            .await?;
        if let Some(selected) = &stored {
            self.check_preparation_acquires(plan, &selected.intents)?;
        }
        Ok((journal, stored))
    }

    async fn choose_preparation_acquires(
        &self,
        plan: &DirectoryMutation,
        permit: &ManagedTransitionPermit,
        previous: Option<[u8; 32]>,
        intents: Vec<CellGroupArchiveIntent>,
    ) -> Result<Selected> {
        self.check_preparation_acquires(plan, &intents)?;
        let acquires = Acquires {
            namespace: self.namespace.root_digest,
            plan: plan.digest(),
            intents,
        };
        let bytes = record::encode(KIND, &acquires, wire::LIMIT)?;
        let digest = record::digest(&bytes);
        self.bounded(self.immutable(ACQUIRES, digest, &bytes))
            .await?;
        self.record_managed_preparation_acquires(plan, permit, previous, digest)
            .await?;
        Ok(Selected {
            digest,
            intents: acquires.intents,
        })
    }

    fn check_preparation_acquires(
        &self,
        plan: &DirectoryMutation,
        intents: &[CellGroupArchiveIntent],
    ) -> Result<()> {
        let (parents, _) = self.transition_routes(plan)?;
        if intents.len() != parents.len() {
            return Err(CatalogError::Corrupt("preparation acquire count").into());
        }
        for (route, intent) in parents.iter().zip(intents) {
            let marker = intent.acquire_marker()?;
            if marker.route() != route || !marker.is_active() {
                return Err(CatalogError::Fenced.into());
            }
        }
        Ok(())
    }
}
