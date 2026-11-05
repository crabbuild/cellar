// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Pre-seal split/merge decisions rooted in the same CAS as the range tree.
use super::*;
use serde::{Deserialize, Serialize};

// Normal operation keys are 32 lowercase hexadecimal digits. These keys share
// their immutable index and head CAS without changing its wire shape.
const ACTIVE: &str = "transition/active";
const COUNT: &str = "transition/count";
const JOURNAL: u8 = 198;
const LIMIT: usize = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransitionPhase {
    Planned,
    Prepared,
    SealAuthorized,
    Cancelled,
}

/// A historical topology decision. A source still needs authenticated
/// admission and a committed seal before its cut can be used for transfer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransitionJournal {
    request: [u8; 32],
    preparation: Option<[u8; 32]>,
    phase: TransitionPhase,
}

impl TransitionJournal {
    pub fn phase(&self) -> TransitionPhase {
        self.phase
    }

    pub fn preparation(&self) -> Option<[u8; 32]> {
        self.preparation
    }
}

#[derive(Clone, Copy)]
enum Advance {
    Prepare([u8; 32]),
    Authorize,
    Cancel,
}

impl Advance {
    fn apply(self, mut journal: TransitionJournal) -> Result<TransitionJournal, CatalogError> {
        use TransitionPhase::*;
        match (self, journal.phase) {
            (Self::Prepare(digest), Planned) if digest != [0; 32] => {
                journal.preparation = Some(digest);
                journal.phase = Prepared;
            }
            (Self::Prepare(digest), Prepared | SealAuthorized)
                if journal.preparation == Some(digest) => {}
            (Self::Authorize, Prepared) => journal.phase = SealAuthorized,
            (Self::Cancel, Planned | Prepared) => journal.phase = Cancelled,
            (Self::Authorize, SealAuthorized) | (Self::Cancel, Cancelled) => {}
            _ => return Err(CatalogError::Conflict("transition decision")),
        }
        Ok(journal)
    }
}

fn key(operation: ProposalId) -> String {
    format!("transition/{}", hex::encode(operation.bytes()))
}

impl RangeDirectory {
    pub(super) async fn transition_active(
        &self,
        head: &wire::Head,
    ) -> Result<Option<ProposalId>, CatalogError> {
        Index::new(self)
            .lookup(head.operations, ACTIVE)
            .await?
            .map(|bytes| {
                let id = bytes
                    .try_into()
                    .map_err(|_| CatalogError::Corrupt("transition owner"))?;
                ProposalId::new(id).map_err(|_| CatalogError::Corrupt("transition owner"))
            })
            .transpose()
    }

    pub(super) async fn transition_record(
        &self,
        head: &wire::Head,
        operation: ProposalId,
    ) -> Result<Option<TransitionJournal>, CatalogError> {
        Index::new(self)
            .lookup(head.operations, &key(operation))
            .await?
            .map(|bytes| {
                let journal: TransitionJournal = record::decode(JOURNAL, &bytes, LIMIT)?;
                let required = matches!(
                    journal.phase,
                    TransitionPhase::Prepared | TransitionPhase::SealAuthorized
                );
                if journal.request == [0; 32]
                    || journal.preparation == Some([0; 32])
                    || (required && journal.preparation.is_none())
                    || (journal.phase == TransitionPhase::Planned && journal.preparation.is_some())
                {
                    return Err(CatalogError::Corrupt("transition journal"));
                }
                Ok(journal)
            })
            .transpose()
    }

    fn check_transition_plan(&self, mutation: &DirectoryMutation) -> Result<(), CatalogError> {
        self.check_request(&mutation.request)?;
        match (
            mutation.request.parents.len(),
            mutation.request.children.len(),
        ) {
            (1, 2..=64) | (2..=64, 1) => Ok(()),
            _ => Err(CatalogError::Invalid("transition split or merge shape")),
        }
    }

    async fn matching_transition(
        &self,
        head: &wire::Head,
        mutation: &DirectoryMutation,
    ) -> Result<Option<TransitionJournal>, CatalogError> {
        let found = self.transition_record(head, mutation.operation).await?;
        if let Some(journal) = &found {
            if journal.request != mutation.digest {
                return Err(CatalogError::Conflict("transition operation reused"));
            }
            let active = self.transition_active(head).await? == Some(mutation.operation);
            if active == (journal.phase == TransitionPhase::Cancelled) {
                return Err(CatalogError::Corrupt(
                    "transition owner and journal disagree",
                ));
            }
        }
        Ok(found)
    }

    async fn commit_transition_head(
        &self,
        head: &wire::Head,
        version: UpdateVersion,
    ) -> Result<DirectoryView, CatalogError> {
        self.check_head(head)?;
        let bytes = record::encode(wire::HEAD, head, wire::LIMIT)?;
        // A transport failure can follow the committed conditional update.
        // Observe the rooted winner before returning a seal decision.
        let _ = self
            .namespace
            .inner
            .store
            .put_opts(
                &self.key("head"),
                bytes.into(),
                PutOptions::from(PutMode::Update(version)),
            )
            .await;
        self.current()
            .await
            .map(|(view, _)| view)
            .map_err(|_| CatalogError::Unconfirmed)
    }

    /// Reserve one exact split or merge against the selected range tree.
    /// Children must already have immutable birth reservations. No voter or
    /// resource admission is granted by the journal alone.
    pub async fn plan_transition(
        &self,
        mutation: &DirectoryMutation,
    ) -> Result<TransitionJournal, CatalogError> {
        self.unmanaged_birth()?;
        self.check_transition_plan(mutation)?;
        self.bounded(async {
            let (mut view, version) = self.current().await?;
            if let Some(journal) = self.matching_transition(&view.head, mutation).await? {
                return Ok(journal);
            }
            if self.namespace.cell_group_owner().await?.is_some() {
                return Err(CatalogError::ManagedAdmissionRequired);
            }
            if view.head.import.is_some()
                || view.head.recovery.is_some()
                || self.transition_active(&view.head).await?.is_some()
            {
                return Err(CatalogError::Fenced);
            }
            let index = Index::new(self);
            if mutation.request.expected != Some(view.digest)
                || index
                    .lookup(
                        view.head.operations,
                        &hex::encode(mutation.operation.bytes()),
                    )
                    .await?
                    .is_some()
            {
                return Err(CatalogError::Conflict("stale transition root or operation"));
            }
            let count = index
                .lookup(view.head.operations, COUNT)
                .await?
                .map(|value| {
                    value
                        .try_into()
                        .map(u64::from_be_bytes)
                        .map_err(|_| CatalogError::Corrupt("transition count"))
                })
                .transpose()?
                .unwrap_or(0);
            if count >= self.limits.max_mutations {
                return Err(CatalogError::Limit);
            }
            for parent in &mutation.request.parents {
                self.selected_route_in(&view, &parent.decode(self)?).await?;
            }
            for child in &mutation.request.children {
                self.require_ordinary_birth_reservation(&child.decode(self)?)
                    .await?;
                if index
                    .lookup(view.head.groups, &hex::encode(child.group))
                    .await?
                    .is_some()
                {
                    return Err(CatalogError::Conflict(
                        "transition child group is already assigned",
                    ));
                }
            }
            self.immutable("transition-plans", mutation.digest, mutation.encode())
                .await?;
            let journal = TransitionJournal {
                request: mutation.digest,
                preparation: None,
                phase: TransitionPhase::Planned,
            };
            for (key, value) in [
                (COUNT, (count + 1).to_be_bytes().to_vec()),
                (ACTIVE, mutation.operation.bytes().to_vec()),
                (
                    &key(mutation.operation),
                    record::encode(JOURNAL, &journal, LIMIT)?,
                ),
            ] {
                view.head.operations = index
                    .update(view.head.operations, key, Some(value), 0)
                    .await?;
            }
            let after = self.commit_transition_head(&view.head, version).await?;
            self.matching_transition(&after.head, mutation)
                .await?
                .ok_or(CatalogError::Unconfirmed)
        })
        .await
    }

    /// Recover the original request and decision after losing the controller.
    pub async fn active_transition(
        &self,
    ) -> Result<Option<(DirectoryMutation, TransitionJournal)>, CatalogError> {
        self.bounded(async {
            let (view, _) = self.current().await?;
            let Some(operation) = self.transition_active(&view.head).await? else {
                return Ok(None);
            };
            let journal = self
                .transition_record(&view.head, operation)
                .await?
                .ok_or(CatalogError::Corrupt("missing active transition"))?;
            let bytes = self
                .read(&format!(
                    "transition-plans/{}",
                    hex::encode(journal.request)
                ))
                .await?
                .ok_or(CatalogError::Corrupt("missing transition plan"))?;
            let mutation = self.decode_mutation(&bytes)?;
            self.check_transition_plan(&mutation)?;
            if mutation.operation != operation
                || mutation.digest != journal.request
                || journal.phase == TransitionPhase::Cancelled
            {
                return Err(CatalogError::Corrupt("active transition plan binding"));
            }
            Ok(Some((mutation, journal)))
        })
        .await
    }

    /// Record the exact digest of the controller's child preparation evidence.
    /// This metadata boundary does not verify copied images or admit a child.
    pub async fn prepare_transition(
        &self,
        mutation: &DirectoryMutation,
        preparation: [u8; 32],
    ) -> Result<TransitionJournal, CatalogError> {
        self.advance_transition(mutation, Advance::Prepare(preparation))
            .await
    }

    pub async fn authorize_transition_seal(
        &self,
        mutation: &DirectoryMutation,
    ) -> Result<TransitionJournal, CatalogError> {
        self.advance_transition(mutation, Advance::Authorize).await
    }

    pub async fn cancel_transition(
        &self,
        mutation: &DirectoryMutation,
    ) -> Result<TransitionJournal, CatalogError> {
        self.advance_transition(mutation, Advance::Cancel).await
    }

    async fn advance_transition(
        &self,
        mutation: &DirectoryMutation,
        advance: Advance,
    ) -> Result<TransitionJournal, CatalogError> {
        self.unmanaged_birth()?;
        self.check_transition_plan(mutation)?;
        self.bounded(async {
            let (mut view, version) = self.current().await?;
            let before = self
                .matching_transition(&view.head, mutation)
                .await?
                .ok_or(CatalogError::Conflict("unplanned transition"))?;
            let requested = advance.apply(before.clone())?;
            if requested == before {
                return Ok(before);
            }
            let index = Index::new(self);
            let bytes = record::encode(JOURNAL, &requested, LIMIT)?;
            view.head.operations = index
                .update(
                    view.head.operations,
                    &key(mutation.operation),
                    Some(bytes),
                    0,
                )
                .await?;
            if requested.phase == TransitionPhase::Cancelled {
                view.head.operations = index.update(view.head.operations, ACTIVE, None, 0).await?;
            }
            let after = self.commit_transition_head(&view.head, version).await?;
            let winner = self
                .matching_transition(&after.head, mutation)
                .await?
                .ok_or(CatalogError::Corrupt("lost transition journal"))?;
            if advance.apply(winner.clone())? != winner {
                return Err(CatalogError::Unconfirmed);
            }
            Ok(winner)
        })
        .await
    }
}
