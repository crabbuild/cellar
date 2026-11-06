// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Select the complete Fleet settlement and reopen routing through one directory CAS.
use super::{
    managed_publication::{ManagedRecoveryPublication, ManagedRecoveryPublicationInput},
    *,
};
use crate::{
    asyncrt::DomainToken,
    raft::{
        catalog::CellGroupArchiveBindingError,
        cell_groups::CellGroupArchivePool,
        fleet_catalog::{FleetCatalogError, FleetManagedRecoveryCompletion},
    },
};

const KIND: u8 = 231;
const LIMIT: usize = 4096;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Record {
    namespace: [u8; 32],
    operation: [u8; 16],
    transition: [u8; 16],
    transition_plan: [u8; 32],
    intent: [u8; 32],
    pub(super) plan: [u8; 32],
    seeds: [u8; 32],
    pub(super) publication: [u8; 32],
    fleet: [u8; 32],
    fleet_revision: u64,
    fleet_settlement: [u8; 32],
}

impl Record {
    pub(super) fn matches_publication(
        &self,
        input: &ManagedRecoveryPublicationInput<'_>,
        publication: [u8; 32],
    ) -> bool {
        self.transition == input.transition.bytes()
            && self.transition_plan == input.transition_plan
            && self.intent == input.intent
            && self.plan == input.plan
            && self.seeds == input.seeds
            && self.publication == publication
    }
}

impl RangeDirectory {
    /// A rooted release is historical evidence. Replica startup must also
    /// verify the current route, Fleet boot and local activation.
    pub(super) async fn managed_recovery_serving_record(
        &self,
        view: &DirectoryView,
        operation: ProposalId,
    ) -> Result<Option<(Record, [u8; 32])>, CatalogError> {
        let Some(bytes) = Index::new(self)
            .lookup(view.head.operations, &serving_key(operation))
            .await?
        else {
            return Ok(None);
        };
        let selected: Record = record::decode(KIND, &bytes, LIMIT)?;
        if selected.namespace != self.namespace.root_digest
            || selected.operation != operation.bytes()
        {
            return Err(CatalogError::Corrupt("managed recovery serving receipt"));
        }
        let transition = ProposalId::new(selected.transition)
            .map_err(|_| CatalogError::Corrupt("managed recovery transition identity"))?;
        if view
            .head
            .recovery
            .as_deref()
            .is_some_and(|lock| lock.owns_transition(transition))
        {
            return Err(CatalogError::Corrupt(
                "managed recovery release still fenced",
            ));
        }
        let digest = record::digest(&bytes);
        self.verify_recovered_transition(&view.head, transition, selected.transition_plan, digest)
            .await?;
        Ok(Some((selected, digest)))
    }

    /// Root the settlement, or release the recovery lock using its exact
    /// previously rooted receipt. Both decisions survive lost confirmation.
    pub(crate) async fn record_managed_recovery_completion(
        &self,
        input: &ManagedRecoveryPublicationInput<'_>,
        proof: &FleetManagedRecoveryCompletion,
        release: Option<([u8; 32], &DomainToken, &CellGroupArchivePool)>,
    ) -> Result<[u8; 32], CatalogError> {
        self.bounded(async {
            let (mut view, version) = self.current().await?;
            let publication = self
                .confirmed(&view.head, input.mutation)
                .await?
                .ok_or(CatalogError::Fenced)?;
            let (fleet, fleet_revision, fleet_settlement) =
                proof.directory_binding(self, input, publication.journal_digest())?;
            let operation = input.mutation.operation();
            let selected = Record {
                namespace: self.namespace.root_digest,
                operation: operation.bytes(),
                transition: input.transition.bytes(),
                transition_plan: input.transition_plan,
                intent: input.intent,
                plan: input.plan,
                seeds: input.seeds,
                publication: publication.journal_digest(),
                fleet,
                fleet_revision,
                fleet_settlement,
            };
            let bytes = record::encode(KIND, &selected, LIMIT)?;
            let completion = record::digest(&bytes);
            if release.is_some_and(|(expected, _, _)| expected != completion) {
                return Err(CatalogError::Conflict(
                    "managed recovery completion changed",
                ));
            }
            if let Some((_, recorded)) = self
                .managed_recovery_serving_record(&view, operation)
                .await?
            {
                return if recorded == completion {
                    Ok(completion)
                } else {
                    Err(CatalogError::Conflict(
                        "managed recovery serving receipt changed",
                    ))
                };
            }

            let authority = ManagedRecoveryPublication::new(input);
            authority.check_published(self, &view, &publication).await?;
            let key = format!("recovery-completion/{}", hex::encode(operation.bytes()));
            let index = Index::new(self);
            let existing = index.lookup(view.head.operations, &key).await?;
            if let Some(existing) = &existing {
                if *existing != bytes {
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
            }
            if let Some((_, domain, archives)) = release {
                if existing.is_none() {
                    return Err(CatalogError::Unconfirmed);
                }
                // Recovered histories need independent archives before normal
                // voter boot can enable archiving or local log reclamation.
                for birth in input.births {
                    let birth = self.open_historical_birth(birth.route()).await?;
                    Box::pin(self.initialize_recovered_group_archive(domain, archives, &birth))
                        .await
                        .map_err(|error| match error {
                            CellGroupArchiveBindingError::Catalog(error) => error,
                            CellGroupArchiveBindingError::Archive(error) => {
                                CatalogError::from(FleetCatalogError::from(error))
                            }
                        })?;
                }
                view.head.operations = index
                    .update(
                        view.head.operations,
                        &serving_key(operation),
                        Some(bytes),
                        0,
                    )
                    .await?;
                view.head.recovery = None;
                let after = self.commit_transition_head(&view.head, version).await?;
                if self
                    .managed_recovery_serving_record(&after, operation)
                    .await?
                    .is_none_or(|(_, recorded)| recorded != completion)
                {
                    return Err(CatalogError::Unconfirmed);
                }
                return Ok(completion);
            }
            if existing.is_some() {
                return Ok(completion);
            }
            for birth in input.births {
                self.open_historical_birth(birth.route()).await?;
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
            authority.check_selected(self, &after, &publication).await?;
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

fn serving_key(operation: ProposalId) -> String {
    format!("recovery-serving/{}", hex::encode(operation.bytes()))
}
