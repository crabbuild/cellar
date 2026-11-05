// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Split/merge decisions rooted in the same CAS as the range tree.
use super::*;
use crate::application::consensus::group_protocol::{
    CellGroupEntry, GroupCommand, GroupEntryBinding, GroupEntryLimits,
};
use crate::raft::fleet_catalog::ManagedTransitionPermit;
use crate::raft::{cache::GroupLog, completion::RuntimeError, storage::ReplicaLog};
use cellar_logic::consensus::EngineInstanceId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

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
    Sealed,
    Cancelled,
}

/// A historical topology decision. A source still needs authenticated
/// admission and a committed seal before its cut can be used for transfer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransitionJournal {
    request: [u8; 32],
    preparation: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_set: Option<[u8; 32]>,
    phase: TransitionPhase,
}

impl TransitionJournal {
    pub fn phase(&self) -> TransitionPhase {
        self.phase
    }

    pub fn preparation(&self) -> Option<[u8; 32]> {
        self.preparation
    }

    pub fn sealed_sources(&self) -> Option<[u8; 32]> {
        self.source_set
    }

    pub(super) fn seal_authorization(&self) -> Result<[u8; 32], CatalogError> {
        if !matches!(
            self.phase,
            TransitionPhase::SealAuthorized | TransitionPhase::Sealed
        ) {
            return Err(CatalogError::Fenced);
        }
        let authorized = Self {
            phase: TransitionPhase::SealAuthorized,
            source_set: None,
            ..self.clone()
        };
        Ok(record::digest(&record::encode(
            JOURNAL,
            &authorized,
            LIMIT,
        )?))
    }
}

/// Short-lived local capability minted only from the selected irreversible journal.
/// It binds the encoded seal to one physically open source replica.
pub struct CellGroupSealPermit {
    group: GroupLog<GroupHistory>,
    engine: EngineInstanceId,
    deadline: u64,
    transition: TransitionId,
    authorization: [u8; 32],
    payload: [u8; 32],
    entry: Vec<u8>,
}

impl CellGroupSealPermit {
    pub(crate) fn history(&self) -> GroupHistory {
        self.group.identity().scope
    }

    pub(crate) fn check(
        &self,
        log: &ReplicaLog,
        group: &GroupLog<GroupHistory>,
    ) -> Result<(), RuntimeError> {
        if !Arc::ptr_eq(&group.owner, &self.group.owner)
            || group.identity() != self.group.identity()
            || log.storage_instance()? != self.engine
            || log.domain().mono_ms().map_err(|_| RuntimeError::Closed)? >= self.deadline
        {
            return Err(RuntimeError::StaleCompletion);
        }
        drop(group.read()?);
        Ok(())
    }

    pub(crate) fn matches(&self, seal: &crate::raft::cell_groups::GroupSourceSeal) -> bool {
        seal.matches(self.transition, self.authorization, self.payload)
    }

    pub(crate) fn payload(&self) -> [u8; 32] {
        self.payload
    }

    pub(crate) fn entry_bytes(&self) -> usize {
        self.entry.len()
    }

    pub(crate) fn into_entry(self) -> Vec<u8> {
        self.entry
    }
}

#[derive(Clone, Copy)]
enum Advance {
    Prepare([u8; 32]),
    Authorize,
    RecordSources([u8; 32]),
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
            (Self::Prepare(digest), Prepared | SealAuthorized | Sealed)
                if journal.preparation == Some(digest) => {}
            (Self::Authorize, Prepared) => journal.phase = SealAuthorized,
            (Self::RecordSources(digest), SealAuthorized) if digest != [0; 32] => {
                journal.phase = Sealed;
                journal.source_set = Some(digest);
            }
            (Self::RecordSources(digest), Sealed) if journal.source_set == Some(digest) => {}
            (Self::Cancel, Planned | Prepared) => journal.phase = Cancelled,
            (Self::Authorize, SealAuthorized | Sealed) | (Self::Cancel, Cancelled) => {}
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
                    TransitionPhase::Prepared
                        | TransitionPhase::SealAuthorized
                        | TransitionPhase::Sealed
                );
                if journal.request == [0; 32]
                    || journal.preparation == Some([0; 32])
                    || journal.source_set == Some([0; 32])
                    || (journal.phase == TransitionPhase::Sealed) != journal.source_set.is_some()
                    || (required && journal.preparation.is_none())
                    || (journal.phase == TransitionPhase::Planned && journal.preparation.is_some())
                {
                    return Err(CatalogError::Corrupt("transition journal"));
                }
                Ok(journal)
            })
            .transpose()
    }

    pub(super) fn check_transition_plan(
        &self,
        mutation: &DirectoryMutation,
    ) -> Result<(), CatalogError> {
        self.check_request(&mutation.request)?;
        match (
            mutation.request.parents.len(),
            mutation.request.children.len(),
        ) {
            (1, 2..=64) | (2..=64, 1) => Ok(()),
            _ => Err(CatalogError::Invalid("transition split or merge shape")),
        }
    }

    pub(crate) fn transition_routes(
        &self,
        mutation: &DirectoryMutation,
    ) -> Result<(Vec<GroupRoute>, Vec<GroupRoute>), CatalogError> {
        self.check_transition_plan(mutation)?;
        let decode = |routes: &[wire::Route]| {
            routes
                .iter()
                .map(|route| route.decode(self))
                .collect::<Result<_, _>>()
        };
        Ok((
            decode(&mutation.request.parents)?,
            decode(&mutation.request.children)?,
        ))
    }

    pub(super) async fn matching_transition(
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
        self.plan_transition_inner(mutation, false).await
    }

    pub(crate) async fn plan_managed_transition(
        &self,
        mutation: &DirectoryMutation,
        permit: &ManagedTransitionPermit,
    ) -> Result<TransitionJournal, CatalogError> {
        permit.check(self, mutation)?;
        self.plan_transition_inner(mutation, true).await
    }

    async fn plan_transition_inner(
        &self,
        mutation: &DirectoryMutation,
        managed: bool,
    ) -> Result<TransitionJournal, CatalogError> {
        self.check_transition_plan(mutation)?;
        self.bounded(async {
            let (mut view, version) = self.current().await?;
            if let Some(journal) = self.matching_transition(&view.head, mutation).await? {
                return Ok(journal);
            }
            if self.namespace.cell_group_owner().await?.is_some() != managed {
                return Err(CatalogError::ManagedAdmissionRequired);
            }
            let count = self.check_new_transition(&view, mutation).await?;
            let index = Index::new(self);
            self.immutable("transition-plans", mutation.digest, mutation.encode())
                .await?;
            let journal = TransitionJournal {
                request: mutation.digest,
                preparation: None,
                source_set: None,
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

    pub(crate) async fn check_managed_transition_basis(
        &self,
        mutation: &DirectoryMutation,
        permit: &ManagedTransitionPermit,
    ) -> Result<(), CatalogError> {
        permit.check(self, mutation)?;
        self.check_transition_plan(mutation)?;
        self.bounded(async {
            let (view, _) = self.current().await?;
            if self
                .matching_transition(&view.head, mutation)
                .await?
                .is_some()
            {
                return Err(CatalogError::Conflict("transition already decided"));
            }
            self.check_new_transition(&view, mutation).await?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn managed_transition_status(
        &self,
        mutation: &DirectoryMutation,
        permit: &ManagedTransitionPermit,
    ) -> Result<Option<TransitionJournal>, CatalogError> {
        permit.check(self, mutation)?;
        self.check_transition_plan(mutation)?;
        self.bounded(async {
            let (view, _) = self.current().await?;
            self.matching_transition(&view.head, mutation).await
        })
        .await
    }

    async fn check_new_transition(
        &self,
        view: &DirectoryView,
        mutation: &DirectoryMutation,
    ) -> Result<u64, CatalogError> {
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
        let count = self.available_transition_slot(&view.head).await?;
        for parent in &mutation.request.parents {
            self.selected_route_in(view, &parent.decode(self)?).await?;
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
        Ok(count)
    }

    async fn available_transition_slot(&self, head: &wire::Head) -> Result<u64, CatalogError> {
        let index = Index::new(self);
        let count = index
            .lookup(head.operations, COUNT)
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
        Ok(count)
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

    /// Issue the selected transition's exact seal for a local source owner.
    /// Retrying after a lost response uses the original plan and journal digest.
    pub async fn prepare_source_seal(
        &self,
        mutation: &DirectoryMutation,
        source: GroupHistory,
        log: &ReplicaLog,
    ) -> Result<CellGroupSealPermit, CatalogError> {
        self.unmanaged_birth()?;
        self.check_transition_plan(mutation)?;
        self.bounded(async {
            let (view, _) = self.current().await?;
            let journal = self
                .matching_transition(&view.head, mutation)
                .await?
                .ok_or(CatalogError::Fenced)?;
            let route = mutation
                .request
                .parents
                .iter()
                .find(|route| {
                    route.group == source.group.bytes()
                        && route.incarnation == source.incarnation.bytes()
                })
                .ok_or(CatalogError::Fenced)?
                .decode(self)?;
            if route.history != source {
                return Err(CatalogError::Fenced);
            }
            self.selected_route_in(&view, &route).await?;
            let group = log.cell_group(&source).map_err(CatalogError::Log)?;
            if group
                .group_manifest()
                .map_err(CatalogError::Log)?
                .as_ref()
                .is_none_or(|manifest| manifest.route() != &route)
            {
                return Err(CatalogError::Fenced);
            }
            let (transition, authorization, entry) =
                self.source_seal_entry(mutation, &journal, &route)?;
            let deadline = log
                .domain()
                .mono_ms()
                .map_err(|_| CatalogError::Unconfirmed)?
                .checked_add(self.limits.operation_timeout_ms)
                .ok_or(CatalogError::Limit)?;
            Ok(CellGroupSealPermit {
                group,
                engine: log.storage_instance().map_err(CatalogError::Log)?,
                deadline,
                transition,
                authorization,
                payload: Sha256::digest(&entry).into(),
                entry,
            })
        })
        .await
    }

    pub(super) fn source_seal_entry(
        &self,
        mutation: &DirectoryMutation,
        journal: &TransitionJournal,
        route: &GroupRoute,
    ) -> Result<(TransitionId, [u8; 32], Vec<u8>), CatalogError> {
        let transition = TransitionId::new(mutation.operation.bytes())
            .map_err(|_| CatalogError::Invalid("transition identity"))?;
        let authorization = journal.seal_authorization()?;
        let children = mutation
            .request
            .children
            .iter()
            .map(|route| route.decode(self))
            .collect::<Result<_, _>>()?;
        let entry = CellGroupEntry {
            binding: GroupEntryBinding {
                group: route.history,
                route_epoch: route.epoch,
                birth_digest: route.birth_digest,
                schema_digest: route.schema_digest,
                proposal: mutation.operation,
            },
            command: GroupCommand::Seal {
                transition,
                authorization,
                children,
            },
        }
        .encode(route, GroupEntryLimits::default())
        .map_err(|_| CatalogError::Invalid("source seal entry"))?;
        Ok((transition, authorization, entry))
    }

    pub(super) async fn record_verified_sources(
        &self,
        mutation: &DirectoryMutation,
        digest: [u8; 32],
    ) -> Result<TransitionJournal, CatalogError> {
        self.advance_transition(mutation, Advance::RecordSources(digest))
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

    pub(crate) async fn cancel_managed_transition(
        &self,
        mutation: &DirectoryMutation,
        permit: &ManagedTransitionPermit,
    ) -> Result<TransitionJournal, CatalogError> {
        permit.check(self, mutation)?;
        self.advance_transition_inner(mutation, Advance::Cancel, true)
            .await
    }

    async fn advance_transition(
        &self,
        mutation: &DirectoryMutation,
        advance: Advance,
    ) -> Result<TransitionJournal, CatalogError> {
        self.unmanaged_birth()?;
        self.advance_transition_inner(mutation, advance, false)
            .await
    }

    async fn advance_transition_inner(
        &self,
        mutation: &DirectoryMutation,
        advance: Advance,
        cancel_unplanned: bool,
    ) -> Result<TransitionJournal, CatalogError> {
        self.check_transition_plan(mutation)?;
        self.bounded(async {
            let (mut view, version) = self.current().await?;
            let Some(before) = self.matching_transition(&view.head, mutation).await? else {
                if !cancel_unplanned {
                    return Err(CatalogError::Conflict("unplanned transition"));
                }
                // The fleet slot can commit before the directory plan. Root a
                // cancellation even then, so a delayed planner cannot reopen it.
                return self
                    .cancel_unplanned_transition(mutation, view, version)
                    .await;
            };
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

    async fn cancel_unplanned_transition(
        &self,
        mutation: &DirectoryMutation,
        mut view: DirectoryView,
        version: UpdateVersion,
    ) -> Result<TransitionJournal, CatalogError> {
        let index = Index::new(self);
        let journal = TransitionJournal {
            request: mutation.digest,
            preparation: None,
            source_set: None,
            phase: TransitionPhase::Cancelled,
        };
        let count = self.available_transition_slot(&view.head).await?;
        view.head.operations = index
            .update(
                view.head.operations,
                COUNT,
                Some((count + 1).to_be_bytes().to_vec()),
                0,
            )
            .await?;
        view.head.operations = index
            .update(
                view.head.operations,
                &key(mutation.operation),
                Some(record::encode(JOURNAL, &journal, LIMIT)?),
                0,
            )
            .await?;
        let after = self.commit_transition_head(&view.head, version).await?;
        let winner = self.matching_transition(&after.head, mutation).await?;
        if winner.as_ref() != Some(&journal) {
            return Err(CatalogError::Unconfirmed);
        }
        Ok(journal)
    }
}
