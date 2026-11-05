// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Split/merge decisions rooted in the same CAS as the range tree.
use super::*;
use crate::application::consensus::group_protocol::{
    CellGroupEntry, GroupCommand, GroupEntryBinding, GroupEntryLimits,
};
use crate::raft::{cache::GroupLog, completion::RuntimeError, storage::ReplicaLog};
use crate::raft::{
    fleet_catalog::{FleetGroupAuthorization, FleetHostAuthorization, ManagedTransitionPermit},
    fleet_stop::BootBinding,
};
use cellar_logic::consensus::EngineInstanceId;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

// Normal operation keys are 32 lowercase hexadecimal digits. These keys share
// their immutable index and head CAS without changing its wire shape.
const ACTIVE: &str = "transition/active";
const PLANS: &str = "transition-plans";
const COUNT: &str = "transition/count";
const JOURNAL: u8 = 198;
// A retired journal can carry nine 32-byte digests. Each JSON byte array
// takes up to 129 bytes, before field names, phase and the record envelope.
const JOURNAL_LIMIT: usize = 1536;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransitionPhase {
    Planned,
    Prepared,
    SealAuthorized,
    Sealed,
    Installed,
    Archived,
    Published,
    Active,
    Retired,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    installed_set: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    archive: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    publication: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    activation_set: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    child_archives: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    retirement_set: Option<[u8; 32]>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    parent_holds_released: bool,
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

    pub fn installed_children(&self) -> Option<[u8; 32]> {
        self.installed_set
    }

    pub fn transfer_archive(&self) -> Option<[u8; 32]> {
        self.archive
    }

    pub fn activated_children(&self) -> Option<[u8; 32]> {
        self.activation_set
    }

    pub fn child_archives(&self) -> Option<[u8; 32]> {
        self.child_archives
    }

    pub fn retired_parents(&self) -> Option<[u8; 32]> {
        self.retirement_set
    }

    pub fn parent_holds_released(&self) -> bool {
        self.parent_holds_released
    }

    fn owns_topology(&self) -> bool {
        self.phase != TransitionPhase::Cancelled && !self.parent_holds_released
    }

    pub(super) fn seal_authorization(&self) -> Result<[u8; 32], CatalogError> {
        if !matches!(
            self.phase,
            TransitionPhase::SealAuthorized
                | TransitionPhase::Sealed
                | TransitionPhase::Installed
                | TransitionPhase::Archived
                | TransitionPhase::Published
                | TransitionPhase::Active
                | TransitionPhase::Retired
        ) {
            return Err(CatalogError::Fenced);
        }
        let authorized = Self {
            phase: TransitionPhase::SealAuthorized,
            source_set: None,
            installed_set: None,
            archive: None,
            publication: None,
            activation_set: None,
            child_archives: None,
            retirement_set: None,
            parent_holds_released: false,
            ..self.clone()
        };
        Ok(record::digest(&record::encode(
            JOURNAL,
            &authorized,
            JOURNAL_LIMIT,
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
    managed: Option<ManagedSealOwner>,
}

struct ManagedSealOwner {
    boot: BootBinding,
    membership_revision: u64,
}

struct ManagedSealAdmission<'a> {
    host: &'a FleetHostAuthorization,
    group: &'a FleetGroupAuthorization,
    deadline: u64,
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
        if let Some(managed) = &self.managed {
            if log
                .lifecycle_guard()?
                .is_none_or(|guard| !guard.admits_group_start(&managed.boot))
            {
                return Err(RuntimeError::StaleCompletion);
            }
            let cache = group.read()?;
            if cache.group_manifest.as_ref().is_none_or(|manifest| {
                manifest.membership_revision() != managed.membership_revision
            }) || !cache
                .conf_state
                .voters
                .contains(&group.identity().member.get())
            {
                return Err(RuntimeError::StaleCompletion);
            }
        } else {
            drop(group.read()?);
        }
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
    RecordInstalled([u8; 32]),
    RecordArchive {
        sources: [u8; 32],
        installed: [u8; 32],
        digest: [u8; 32],
    },
    RecordActivations {
        publication: [u8; 32],
        installed: [u8; 32],
        digest: [u8; 32],
    },
    RecordChildArchives {
        publication: [u8; 32],
        activations: [u8; 32],
        digest: [u8; 32],
    },
    RecordRetired {
        publication: [u8; 32],
        child_archives: [u8; 32],
        digest: [u8; 32],
    },
    ReleaseParentHolds([u8; 32]),
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
            (Self::Prepare(digest), Prepared | SealAuthorized | Sealed | Installed | Archived)
                if journal.preparation == Some(digest) => {}
            (Self::Authorize, Prepared) => journal.phase = SealAuthorized,
            (Self::RecordSources(digest), SealAuthorized) if digest != [0; 32] => {
                journal.phase = Sealed;
                journal.source_set = Some(digest);
            }
            (Self::RecordSources(digest), Sealed | Installed | Archived)
                if journal.source_set == Some(digest) => {}
            (Self::RecordInstalled(digest), Sealed) if digest != [0; 32] => {
                journal.installed_set = Some(digest);
                journal.phase = Installed;
            }
            (Self::RecordInstalled(digest), Installed | Archived)
                if journal.installed_set == Some(digest) => {}
            (
                Self::RecordArchive {
                    sources,
                    installed,
                    digest,
                },
                Installed | Archived,
            ) if journal.source_set == Some(sources)
                && journal.installed_set == Some(installed)
                && digest != [0; 32]
                && journal.archive.is_none_or(|selected| selected == digest) =>
            {
                journal.archive = Some(digest);
                journal.phase = Archived;
            }
            (
                Self::RecordActivations {
                    publication,
                    installed,
                    digest,
                },
                Published | Active,
            ) if journal.publication == Some(publication)
                && journal.installed_set == Some(installed)
                && digest != [0; 32]
                && journal
                    .activation_set
                    .is_none_or(|selected| selected == digest) =>
            {
                journal.activation_set = Some(digest);
                journal.phase = Active;
            }
            (
                Self::RecordChildArchives {
                    publication,
                    activations,
                    digest,
                },
                Active,
            ) if journal.publication == Some(publication)
                && journal.activation_set == Some(activations)
                && digest != [0; 32]
                && journal
                    .child_archives
                    .is_none_or(|selected| selected == digest) =>
            {
                journal.child_archives = Some(digest);
            }
            (
                Self::RecordRetired {
                    publication,
                    child_archives,
                    digest,
                },
                Active | Retired,
            ) if journal.publication == Some(publication)
                && journal.child_archives == Some(child_archives)
                && digest != [0; 32]
                && journal
                    .retirement_set
                    .is_none_or(|selected| selected == digest) =>
            {
                journal.retirement_set = Some(digest);
                journal.phase = Retired;
            }
            (Self::ReleaseParentHolds(digest), Retired)
                if journal.retirement_set == Some(digest) =>
            {
                journal.parent_holds_released = true;
            }
            (Self::Cancel, Planned | Prepared) => journal.phase = Cancelled,
            (
                Self::Authorize,
                SealAuthorized | Sealed | Installed | Archived | Published | Active | Retired,
            )
            | (Self::Cancel, Cancelled) => {}
            _ => return Err(CatalogError::Conflict("transition decision")),
        }
        Ok(journal)
    }
}

fn key(operation: ProposalId) -> String {
    format!("transition/{}", hex::encode(operation.bytes()))
}

fn child_key(history: GroupHistory) -> String {
    format!(
        "transition/child/{}/{}",
        hex::encode(history.group.bytes()),
        hex::encode(history.incarnation.bytes()),
    )
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
                let journal: TransitionJournal = record::decode(JOURNAL, &bytes, JOURNAL_LIMIT)?;
                let required = matches!(
                    journal.phase,
                    TransitionPhase::Prepared
                        | TransitionPhase::SealAuthorized
                        | TransitionPhase::Sealed
                        | TransitionPhase::Installed
                        | TransitionPhase::Archived
                        | TransitionPhase::Published
                        | TransitionPhase::Active
                        | TransitionPhase::Retired
                );
                if journal.request == [0; 32]
                    || journal.preparation == Some([0; 32])
                    || journal.source_set == Some([0; 32])
                    || journal.installed_set == Some([0; 32])
                    || journal.archive == Some([0; 32])
                    || journal.publication == Some([0; 32])
                    || journal.activation_set == Some([0; 32])
                    || journal.child_archives == Some([0; 32])
                    || journal.retirement_set == Some([0; 32])
                    || (journal.child_archives.is_some()
                        && !matches!(
                            journal.phase,
                            TransitionPhase::Active | TransitionPhase::Retired
                        ))
                    || (journal.phase == TransitionPhase::Retired)
                        != journal.retirement_set.is_some()
                    || (journal.phase == TransitionPhase::Retired
                        && journal.child_archives.is_none())
                    || (journal.parent_holds_released && journal.phase != TransitionPhase::Retired)
                    || matches!(
                        journal.phase,
                        TransitionPhase::Sealed
                            | TransitionPhase::Installed
                            | TransitionPhase::Archived
                            | TransitionPhase::Published
                            | TransitionPhase::Active
                            | TransitionPhase::Retired
                    ) != journal.source_set.is_some()
                    || matches!(
                        journal.phase,
                        TransitionPhase::Installed
                            | TransitionPhase::Archived
                            | TransitionPhase::Published
                            | TransitionPhase::Active
                            | TransitionPhase::Retired
                    ) != journal.installed_set.is_some()
                    || matches!(
                        journal.phase,
                        TransitionPhase::Archived
                            | TransitionPhase::Published
                            | TransitionPhase::Active
                            | TransitionPhase::Retired
                    ) != journal.archive.is_some()
                    || matches!(
                        journal.phase,
                        TransitionPhase::Published
                            | TransitionPhase::Active
                            | TransitionPhase::Retired
                    ) != journal.publication.is_some()
                    || matches!(
                        journal.phase,
                        TransitionPhase::Active | TransitionPhase::Retired
                    ) != journal.activation_set.is_some()
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
            if active != journal.owns_topology() {
                return Err(CatalogError::Corrupt(
                    "transition owner and journal disagree",
                ));
            }
        }
        Ok(found)
    }

    pub(super) async fn check_transfer_publication(
        &self,
        view: &DirectoryView,
        mutation: &DirectoryMutation,
        archive: [u8; 32],
    ) -> Result<TransitionJournal, CatalogError> {
        if view.head.recovery.is_some() {
            return Err(CatalogError::Fenced);
        }
        self.matching_transition(&view.head, mutation)
            .await?
            .filter(|journal| {
                journal.phase == TransitionPhase::Archived && journal.archive == Some(archive)
            })
            .ok_or(CatalogError::Fenced)
    }

    pub(super) async fn root_transfer_publication(
        &self,
        operations: Option<[u8; 32]>,
        mutation: &DirectoryMutation,
        mut journal: TransitionJournal,
        publication: [u8; 32],
    ) -> Result<Option<[u8; 32]>, CatalogError> {
        journal.publication = Some(publication);
        journal.phase = TransitionPhase::Published;
        // Root the birth fence with route selection. Startup must still refuse
        // ordinary genesis if a detached install receipt goes missing.
        let index = Index::new(self);
        let mut operations = operations;
        for child in &mutation.request.children {
            operations = index
                .update(
                    operations,
                    &child_key(child.decode(self)?.history),
                    Some(publication.to_vec()),
                    0,
                )
                .await?;
        }
        // Keep the operation active until activation and retirement settle.
        index
            .update(
                operations,
                &key(mutation.operation),
                Some(record::encode(JOURNAL, &journal, JOURNAL_LIMIT)?),
                0,
            )
            .await
    }

    pub(super) async fn require_non_transfer_birth(
        &self,
        route: &GroupRoute,
    ) -> Result<(), CatalogError> {
        let (view, _) = self.current().await?;
        if Index::new(self)
            .lookup(view.head.operations, &child_key(route.history))
            .await?
            .is_some()
        {
            return Err(CatalogError::Fenced);
        }
        Ok(())
    }

    pub(super) async fn check_confirmed_transfer(
        &self,
        head: &wire::Head,
        journal: &wire::Journal,
        publication: [u8; 32],
    ) -> Result<(), CatalogError> {
        let Some(archive) = journal.transfer_archive else {
            return Ok(());
        };
        let operation = ProposalId::new(journal.operation)
            .map_err(|_| CatalogError::Corrupt("transfer publication operation"))?;
        let selected = self
            .transition_record(head, operation)
            .await?
            .ok_or(CatalogError::Corrupt("missing published transfer decision"))?;
        if !matches!(
            selected.phase,
            TransitionPhase::Published | TransitionPhase::Active | TransitionPhase::Retired
        ) || selected.request != journal.request
            || selected.archive != Some(archive)
            || selected.publication != Some(publication)
            || (self.transition_active(head).await? == Some(operation)) != selected.owns_topology()
        {
            return Err(CatalogError::Corrupt("transfer publication binding"));
        }
        Ok(())
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
            self.immutable(PLANS, mutation.digest, mutation.encode())
                .await?;
            let journal = TransitionJournal {
                request: mutation.digest,
                preparation: None,
                source_set: None,
                installed_set: None,
                archive: None,
                publication: None,
                activation_set: None,
                child_archives: None,
                retirement_set: None,
                parent_holds_released: false,
                phase: TransitionPhase::Planned,
            };
            for (key, value) in [
                (COUNT, (count + 1).to_be_bytes().to_vec()),
                (ACTIVE, mutation.operation.bytes().to_vec()),
                (
                    &key(mutation.operation),
                    record::encode(JOURNAL, &journal, JOURNAL_LIMIT)?,
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

    pub(crate) async fn stage_managed_transition_plan(
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
            // Stage the bytes before the fleet can select their digest. A
            // controller may lose its reply before entering the directory CAS.
            self.immutable(PLANS, mutation.digest, mutation.encode())
                .await
        })
        .await
    }

    pub(crate) async fn reopen_managed_transition_plan(
        &self,
        operation: ProposalId,
        digest: [u8; 32],
        permit: &ManagedTransitionPermit,
    ) -> Result<(DirectoryMutation, Option<TransitionJournal>), CatalogError> {
        self.bounded(async {
            let plan = self.load_transition_plan(operation, digest).await?;
            permit.check(self, &plan)?;
            let (view, _) = self.current().await?;
            let journal = self.matching_transition(&view.head, &plan).await?;
            Ok((plan, journal))
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
            let mutation = self
                .load_transition_plan(operation, journal.request)
                .await?;
            if !journal.owns_topology() {
                return Err(CatalogError::Corrupt("active transition plan binding"));
            }
            Ok(Some((mutation, journal)))
        })
        .await
    }

    async fn load_transition_plan(
        &self,
        operation: ProposalId,
        digest: [u8; 32],
    ) -> Result<DirectoryMutation, CatalogError> {
        let bytes = self
            .read(&format!("{PLANS}/{}", hex::encode(digest)))
            .await?
            .ok_or(CatalogError::Corrupt("missing transition plan"))?;
        let mutation = self.decode_mutation(&bytes)?;
        self.check_transition_plan(&mutation)?;
        if mutation.operation != operation || mutation.digest != digest {
            return Err(CatalogError::Corrupt("transition plan binding"));
        }
        Ok(mutation)
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
        self.prepare_source_seal_inner(mutation, source, log, None)
            .await
    }

    pub(crate) async fn prepare_managed_source_seal(
        &self,
        mutation: &DirectoryMutation,
        source: GroupHistory,
        log: &ReplicaLog,
        host: &FleetHostAuthorization,
        authority: (&ManagedTransitionPermit, &FleetGroupAuthorization),
        deadline: u64,
    ) -> Result<CellGroupSealPermit, CatalogError> {
        let (permit, group) = authority;
        permit.check(self, mutation)?;
        self.prepare_source_seal_inner(
            mutation,
            source,
            log,
            Some(ManagedSealAdmission {
                host,
                group,
                deadline,
            }),
        )
        .await
    }

    async fn prepare_source_seal_inner(
        &self,
        mutation: &DirectoryMutation,
        source: GroupHistory,
        log: &ReplicaLog,
        managed: Option<ManagedSealAdmission<'_>>,
    ) -> Result<CellGroupSealPermit, CatalogError> {
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
            let managed_owner = if let Some(admission) = &managed {
                let birth = self.materialize_selected_history(source).await?;
                let boot = self.require_managed_group_boot(log, admission.host).await?;
                let cache = group.read().map_err(CatalogError::Log)?;
                let manifest = cache.group_manifest.as_ref().ok_or(CatalogError::Fenced)?;
                if birth.route() != &route
                    || manifest.route() != &route
                    || !admission.group.check_birth(self, &birth)
                    || cache
                        .membership
                        .fleet_status()
                        .is_none_or(|status| &status.binding != admission.group.binding())
                    || !admission
                        .group
                        .matches_replica(group.identity(), &cache.membership)
                    || !cache
                        .conf_state
                        .voters
                        .contains(&group.identity().member.get())
                {
                    return Err(CatalogError::Fenced);
                }
                Some(ManagedSealOwner {
                    boot,
                    membership_revision: manifest.membership_revision(),
                })
            } else {
                None
            };
            let now = log
                .domain()
                .mono_ms()
                .map_err(|_| CatalogError::Unconfirmed)?;
            let deadline = now
                .checked_add(self.limits.operation_timeout_ms)
                .ok_or(CatalogError::Limit)?;
            let deadline = managed.map_or(deadline, |admission| deadline.min(admission.deadline));
            if deadline <= now {
                return Err(CatalogError::Unconfirmed);
            }
            Ok(CellGroupSealPermit {
                group,
                engine: log.storage_instance().map_err(CatalogError::Log)?,
                deadline,
                transition,
                authorization,
                payload: Sha256::digest(&entry).into(),
                entry,
                managed: managed_owner,
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

    pub(super) async fn record_verified_child_quorums(
        &self,
        mutation: &DirectoryMutation,
        digest: [u8; 32],
    ) -> Result<TransitionJournal, CatalogError> {
        self.advance_transition(mutation, Advance::RecordInstalled(digest))
            .await
    }

    pub(super) async fn record_verified_transfer_archive(
        &self,
        mutation: &DirectoryMutation,
        sources: [u8; 32],
        installed: [u8; 32],
        digest: [u8; 32],
    ) -> Result<TransitionJournal, CatalogError> {
        self.advance_transition(
            mutation,
            Advance::RecordArchive {
                sources,
                installed,
                digest,
            },
        )
        .await
    }

    pub(super) async fn record_verified_transfer_activations(
        &self,
        mutation: &DirectoryMutation,
        publication: [u8; 32],
        installed: [u8; 32],
        digest: [u8; 32],
    ) -> Result<TransitionJournal, CatalogError> {
        self.advance_transition(
            mutation,
            Advance::RecordActivations {
                publication,
                installed,
                digest,
            },
        )
        .await
    }

    pub(super) async fn record_verified_child_archives(
        &self,
        mutation: &DirectoryMutation,
        publication: [u8; 32],
        activations: [u8; 32],
        digest: [u8; 32],
    ) -> Result<TransitionJournal, CatalogError> {
        self.advance_transition(
            mutation,
            Advance::RecordChildArchives {
                publication,
                activations,
                digest,
            },
        )
        .await
    }

    pub(super) async fn record_verified_retirement(
        &self,
        mutation: &DirectoryMutation,
        publication: [u8; 32],
        child_archives: [u8; 32],
        digest: [u8; 32],
    ) -> Result<TransitionJournal, CatalogError> {
        self.advance_transition(
            mutation,
            Advance::RecordRetired {
                publication,
                child_archives,
                digest,
            },
        )
        .await
    }

    pub(super) async fn record_released_parent_holds(
        &self,
        mutation: &DirectoryMutation,
        digest: [u8; 32],
    ) -> Result<TransitionJournal, CatalogError> {
        self.advance_transition(mutation, Advance::ReleaseParentHolds(digest))
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

    pub(super) async fn record_managed_preparation(
        &self,
        mutation: &DirectoryMutation,
        permit: &ManagedTransitionPermit,
        preparation: [u8; 32],
    ) -> Result<TransitionJournal, CatalogError> {
        permit.check(self, mutation)?;
        self.advance_transition_inner(mutation, Advance::Prepare(preparation), false)
            .await
    }

    pub async fn authorize_transition_seal(
        &self,
        mutation: &DirectoryMutation,
    ) -> Result<TransitionJournal, CatalogError> {
        self.advance_transition(mutation, Advance::Authorize).await
    }

    pub(crate) async fn authorize_managed_transition_seal(
        &self,
        mutation: &DirectoryMutation,
        permit: &ManagedTransitionPermit,
    ) -> Result<TransitionJournal, CatalogError> {
        permit.check(self, mutation)?;
        self.advance_transition_inner(mutation, Advance::Authorize, false)
            .await
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
            let bytes = record::encode(JOURNAL, &requested, JOURNAL_LIMIT)?;
            view.head.operations = index
                .update(
                    view.head.operations,
                    &key(mutation.operation),
                    Some(bytes),
                    0,
                )
                .await?;
            if before.owns_topology() && !requested.owns_topology() {
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
            installed_set: None,
            archive: None,
            publication: None,
            activation_set: None,
            child_archives: None,
            retirement_set: None,
            parent_holds_released: false,
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
                Some(record::encode(JOURNAL, &journal, JOURNAL_LIMIT)?),
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
