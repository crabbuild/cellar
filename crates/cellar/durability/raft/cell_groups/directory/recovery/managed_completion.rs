// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Select the complete Fleet settlement and supersede its old transition.
use super::{
    managed_publication::{ManagedRecoveryPublication, ManagedRecoveryPublicationInput},
    *,
};
use crate::raft::fleet_catalog::FleetManagedRecoveryCompletion;

const KIND: u8 = 231;
const LIMIT: usize = 4096;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    namespace: [u8; 32],
    operation: [u8; 16],
    transition: [u8; 16],
    transition_plan: [u8; 32],
    intent: [u8; 32],
    plan: [u8; 32],
    publication: [u8; 32],
    fleet: [u8; 32],
    fleet_revision: u64,
    fleet_settlement: [u8; 32],
}

impl RangeDirectory {
    /// Transfer directory ownership from the superseded transition to the
    /// rooted recovery completion. The serving and Fleet fences stay held.
    pub(crate) async fn record_managed_recovery_completion(
        &self,
        input: &ManagedRecoveryPublicationInput<'_>,
        proof: &FleetManagedRecoveryCompletion,
    ) -> Result<[u8; 32], CatalogError> {
        self.bounded(async {
            let (mut view, version) = self.current().await?;
            let publication = self
                .confirmed(&view.head, input.mutation)
                .await?
                .ok_or(CatalogError::Fenced)?;
            ManagedRecoveryPublication::new(input)
                .check_published(self, &view, &publication)
                .await?;
            for birth in input.births {
                // Fleet Live requires the immutable directory birth even
                // while the recovery lock refuses ordinary startup.
                self.open_historical_birth(birth.route()).await?;
            }
            let (fleet, fleet_revision, fleet_settlement) =
                proof.directory_binding(self, input, publication.journal_digest())?;
            let lock = view.head.recovery.as_deref().ok_or(CatalogError::Fenced)?;
            let RecoveryPhase::ManagedPublished {
                plan: transition_plan,
                ..
            } = &lock.phase
            else {
                return Err(CatalogError::Fenced);
            };
            let operation = input.mutation.operation().bytes();
            let selected = Record {
                namespace: self.namespace.root_digest,
                operation,
                transition: input.transition.bytes(),
                transition_plan: *transition_plan,
                intent: input.intent,
                plan: input.plan,
                publication: publication.journal_digest(),
                fleet,
                fleet_revision,
                fleet_settlement,
            };
            let bytes = record::encode(KIND, &selected, LIMIT)?;
            let completion = record::digest(&bytes);
            let key = format!("recovery-completion/{}", hex::encode(operation));
            let index = Index::new(self);
            if let Some(existing) = index.lookup(view.head.operations, &key).await? {
                if existing != bytes {
                    return Err(CatalogError::Conflict(
                        "managed recovery completion changed",
                    ));
                }
                self.verify_recovered_transition(
                    &view.head,
                    input.transition,
                    selected.transition_plan,
                    completion,
                )
                .await?;
                return Ok(completion);
            }

            self.supersede_transition_for_recovery(
                &mut view.head,
                input.transition,
                selected.transition_plan,
                completion,
            )
            .await?;
            view.head.operations = index
                .update(view.head.operations, &key, Some(bytes.clone()), 0)
                .await?;
            let after = self.commit_transition_head(&view.head, version).await?;
            if index.lookup(after.head.operations, &key).await?.as_deref() != Some(bytes.as_slice())
            {
                return Err(CatalogError::Unconfirmed);
            }
            ManagedRecoveryPublication::new(input)
                .check_published(self, &after, &publication)
                .await?;
            self.verify_recovered_transition(
                &after.head,
                input.transition,
                selected.transition_plan,
                completion,
            )
            .await?;
            Ok(completion)
        })
        .await
    }
}
