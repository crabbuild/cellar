//! Birth intent is rooted before the immutable namespace decision. Pending and
//! cancelled histories retain their node references; neither timeout nor an
//! empty runtime scan proves that those references can be released.
use super::*;
use crate::raft::catalog::{
    CatalogError, InitialMember, ManagedBirthOwner, NamespaceCatalog, NamespaceConfig, VoterPolicy,
};
use crate::raft::cell_groups::{
    CellGroupBirth, CellGroupBirthRequest, CellGroupBirthSpec, RangeDirectory,
};
use crate::raft::member_state::FleetGroupBinding;
use cellar_logic::{
    cell_group::{CellBound, CellId, CellRange, GroupHistory, GroupRoute, RouteEpoch},
    consensus::{MemberId, NodeId, ProposalId},
};
use serde::Deserialize;
use std::collections::BTreeSet;

const HISTORY_KIND: u8 = 48;

#[cfg(test)]
#[path = "histories_tests.rs"]
mod tests;

fn bound_key(bound: &CellBound) -> Option<String> {
    match bound {
        CellBound::Key(key) => Some(key.cell().as_str().into()),
        CellBound::NegativeInfinity | CellBound::PositiveInfinity => None,
    }
}

fn required_bound<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<Option<String>, D::Error> {
    Option::deserialize(decoder)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FleetHistoryState {
    Reserved,
    /// The immutable birth was confirmed; this does not attest to live quorum.
    Live,
    Cancelled {
        operation: [u8; 16],
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Member {
    pub(super) member: u64,
    pub(super) node: [u8; 16],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum Origin {
    Managed,
    Observed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetHistory {
    pub(super) binding: FleetGroupBinding,
    pub(super) schema_digest: [u8; 32],
    pub(super) namespace_root: [u8; 32],
    pub(super) birth_digest: [u8; 32],
    #[serde(deserialize_with = "required_bound")]
    lower: Option<String>,
    #[serde(deserialize_with = "required_bound")]
    upper: Option<String>,
    epoch: u64,
    creation: [u8; 16],
    pub(super) admission: [u8; 16],
    pub(super) operation: [u8; 16],
    pub(super) voters: Vec<Member>,
    pub(super) learners: Vec<Member>,
    pub(super) state: FleetHistoryState,
    pub(super) origin: Origin,
}

impl FleetHistory {
    pub(super) fn cancelled_group(
        binding: FleetGroupBinding,
        directory: &RangeDirectory,
        request: &CellGroupBirthRequest,
        admission: ProposalId,
        operation: ProposalId,
        cancellation: ProposalId,
    ) -> Result<Self> {
        let decoded = directory
            .decode_group_birth(request.encode())
            .map_err(catalog_error)?;
        if !binding.matches_scope(&decoded.route().history) {
            return Err(FleetCatalogError::Conflict);
        }
        let value = Self {
            binding,
            schema_digest: directory.namespace_config().schema_digest,
            namespace_root: directory.namespace().root_digest(),
            birth_digest: decoded.route().birth_digest,
            lower: bound_key(decoded.route().range.lower()),
            upper: bound_key(decoded.route().range.upper()),
            epoch: decoded.route().epoch.get(),
            creation: decoded.creation().bytes(),
            admission: admission.bytes(),
            operation: operation.bytes(),
            voters: decoded.voters().iter().copied().map(Member::from).collect(),
            learners: decoded
                .learners()
                .iter()
                .copied()
                .map(Member::from)
                .collect(),
            state: FleetHistoryState::Cancelled {
                operation: cancellation.bytes(),
            },
            origin: Origin::Observed,
        };
        value.validate()?;
        Ok(value)
    }

    /// An exact observed birth is import material, not proof that the namespace
    /// inventory is complete or that legacy creation has been quiesced.
    pub fn observed(
        binding: FleetGroupBinding,
        directory: &RangeDirectory,
        birth: &CellGroupBirth,
        admission: ProposalId,
        operation: ProposalId,
    ) -> Result<Self> {
        let namespace = directory.namespace();
        let decoded = directory
            .decode_group_birth(birth.encode())
            .map_err(catalog_error)?;
        if !binding.matches_scope(&birth.route().history) || decoded.route() != birth.route() {
            return Err(FleetCatalogError::Conflict);
        }
        if namespace
            .config()
            .managed_fleet
            .as_ref()
            .is_some_and(|owner| {
                owner.fleet != binding.fleet() || owner.catalog != binding.catalog()
            })
        {
            return Err(FleetCatalogError::Conflict);
        }
        let value = Self {
            binding,
            schema_digest: namespace.config().schema_digest,
            namespace_root: namespace.root_digest(),
            birth_digest: birth.route().birth_digest,
            lower: bound_key(birth.route().range.lower()),
            upper: bound_key(birth.route().range.upper()),
            epoch: birth.route().epoch.get(),
            creation: birth.creation().bytes(),
            admission: admission.bytes(),
            operation: operation.bytes(),
            voters: birth.voters().iter().copied().map(Member::from).collect(),
            learners: birth.learners().iter().copied().map(Member::from).collect(),
            state: FleetHistoryState::Live,
            origin: Origin::Observed,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn binding(&self) -> &FleetGroupBinding {
        &self.binding
    }

    pub fn state(&self) -> &FleetHistoryState {
        &self.state
    }

    pub fn birth_digest(&self) -> [u8; 32] {
        self.birth_digest
    }

    pub fn schema_digest(&self) -> [u8; 32] {
        self.schema_digest
    }

    pub(crate) fn allows_managed_genesis(&self) -> bool {
        self.origin == Origin::Managed
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        encode(HISTORY_KIND, self, 4096)
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Self = decode(HISTORY_KIND, bytes, 4096)?;
        value.validate()?;
        Ok(value)
    }

    pub(super) fn validate(&self) -> Result<()> {
        self.initial_placement()?;
        self.binding
            .scope()
            .map_err(|_| FleetCatalogError::Corrupt("history binding"))?;
        ProposalId::new(self.admission)
            .map_err(|_| FleetCatalogError::Corrupt("history admission"))?;
        ProposalId::new(self.operation)
            .map_err(|_| FleetCatalogError::Corrupt("history operation"))?;
        if let FleetHistoryState::Cancelled { operation } = self.state {
            ProposalId::new(operation)
                .map_err(|_| FleetCatalogError::Corrupt("birth cancellation"))?;
        }
        if self.schema_digest == [0; 32]
            || self.namespace_root == [0; 32]
            || self.birth_digest == [0; 32]
            || self.voters.is_empty()
            || self.voters.len() + self.learners.len() > 32
        {
            return Err(FleetCatalogError::Corrupt("history descriptor"));
        }
        let mut members = BTreeSet::new();
        let mut nodes = BTreeSet::new();
        for list in [&self.voters, &self.learners] {
            if list.windows(2).any(|w| w[0].member >= w[1].member) {
                return Err(FleetCatalogError::Corrupt("history member ordering"));
            }
            for entry in list {
                entry.initial()?;
                if !members.insert(entry.member) || !nodes.insert(entry.node) {
                    return Err(FleetCatalogError::Corrupt("history duplicate member"));
                }
            }
        }
        Ok(())
    }

    /// Recorded genesis references, never a current-membership or absence proof.
    pub fn initial_placement(&self) -> Result<CellGroupBirthSpec> {
        let history = self
            .binding
            .scope()
            .map_err(|_| FleetCatalogError::Corrupt("history scope"))?;
        let key = |name: &str| {
            CellId::new(name)
                .map(|cell| {
                    crate::application::consensus::cell_routing_key(history.namespace, &cell)
                })
                .map(CellBound::Key)
                .map_err(|_| FleetCatalogError::Corrupt("history range"))
        };
        let range = CellRange::new(
            self.lower
                .as_deref()
                .map(key)
                .transpose()?
                .unwrap_or(CellBound::NegativeInfinity),
            self.upper
                .as_deref()
                .map(key)
                .transpose()?
                .unwrap_or(CellBound::PositiveInfinity),
        )
        .map_err(|_| FleetCatalogError::Corrupt("history range"))?;
        Ok(CellGroupBirthSpec {
            history,
            range,
            epoch: RouteEpoch::new(self.epoch)
                .map_err(|_| FleetCatalogError::Corrupt("history route epoch"))?,
            creation: ProposalId::new(self.creation)
                .map_err(|_| FleetCatalogError::Corrupt("history creation"))?,
            voters: self
                .voters
                .iter()
                .map(Member::initial)
                .collect::<Result<_>>()?,
            learners: self
                .learners
                .iter()
                .map(Member::initial)
                .collect::<Result<_>>()?,
        })
    }

    // Multiple admitted candidates can share a range/epoch before the directory
    // selects one. Every losing physical identity must remain inventoried too.
    pub(super) fn name_key(&self) -> Result<String> {
        let scope = self
            .binding
            .scope()
            .map_err(|_| FleetCatalogError::Corrupt("history scope"))?;
        Ok(format!(
            "range/{}/{}",
            hex::encode(scope.namespace.bytes()),
            hex::encode(hash(
                &serde_json::to_vec(&(
                    &self.lower,
                    &self.upper,
                    self.epoch,
                    scope.group.bytes(),
                    scope.incarnation.bytes(),
                ))
                .map_err(|_| FleetCatalogError::Corrupt("history range key"))?
            ))
        ))
    }

    pub(super) fn group_key(&self) -> Result<String> {
        let scope = self
            .binding
            .scope()
            .map_err(|_| FleetCatalogError::Corrupt("history scope"))?;
        Ok(format!("group/{}", hex::encode(scope.group.bytes())))
    }

    pub(crate) fn check_birth(&self, directory: &RangeDirectory, birth: &CellGroupBirth) -> bool {
        let namespace = directory.namespace();
        directory.decode_group_birth(birth.encode()).is_ok()
            && namespace
                .config()
                .managed_fleet
                .as_ref()
                .is_none_or(|owner| owner.fleet == self.binding.fleet())
            && self.binding.matches_scope(&birth.route().history)
            && self.namespace_root == namespace.root_digest()
            && self.schema_digest == namespace.config().schema_digest
            && self.birth_digest == birth.route().birth_digest
            && self.lower == bound_key(birth.route().range.lower())
            && self.upper == bound_key(birth.route().range.upper())
            && self.epoch == birth.route().epoch.get()
            && self.creation == birth.creation().bytes()
            && self.voters.len() == birth.voters().len()
            && self.learners.len() == birth.learners().len()
            && self
                .voters
                .iter()
                .zip(birth.voters())
                .all(|(a, b)| a.member == b.member.get() && a.node == b.node.bytes())
            && self
                .learners
                .iter()
                .zip(birth.learners())
                .all(|(a, b)| a.member == b.member.get() && a.node == b.node.bytes())
    }
}

impl From<InitialMember> for Member {
    fn from(value: InitialMember) -> Self {
        Self {
            member: value.member.get(),
            node: value.node.bytes(),
        }
    }
}

impl Member {
    fn initial(&self) -> Result<InitialMember> {
        Ok(InitialMember {
            member: MemberId::new(self.member)
                .map_err(|_| FleetCatalogError::Corrupt("history member"))?,
            node: NodeId::new(self.node).map_err(|_| FleetCatalogError::Corrupt("history node"))?,
        })
    }
}

/// Minted only after its exact intent is in the root's permanent journal.
/// A saved capability can finish after a node starts draining: the inventory
/// reference already exists, so the drain must reconcile that pending birth.
#[derive(Clone, Debug)]
pub struct FleetBirthAuthorization {
    history: FleetHistory,
    proposal: CellGroupBirthRequest,
    owner: ManagedBirthOwner,
}

impl FleetBirthAuthorization {
    pub(crate) fn check_birth(&self, directory: &RangeDirectory, birth: &CellGroupBirth) -> bool {
        self.history.check_birth(directory, birth)
    }

    pub(super) fn matches_active_reservation(&self, history: &FleetHistory) -> bool {
        match history.state() {
            FleetHistoryState::Reserved => history == &self.history,
            FleetHistoryState::Live => {
                let mut expected = self.history.clone();
                expected.state = FleetHistoryState::Live;
                history == &expected
            }
            FleetHistoryState::Cancelled { .. } => false,
        }
    }

    pub fn scope(&self) -> &GroupHistory {
        &self.proposal.route().history
    }

    /// Exact reserved metadata for the first range publication. Selection still
    /// requires the directory CAS; this grants no member or quorum authority.
    pub fn route(&self) -> &GroupRoute {
        self.proposal.route()
    }

    pub(crate) fn proposal(&self) -> CellGroupBirthRequest {
        self.proposal.clone()
    }

    pub(crate) fn owner(&self) -> &ManagedBirthOwner {
        &self.owner
    }

    pub(crate) fn check_namespace(
        &self,
        namespace: &NamespaceCatalog,
    ) -> std::result::Result<(), CatalogError> {
        if self.owner.fleet != self.history.binding.fleet()
            || namespace
                .config()
                .managed_fleet
                .as_ref()
                .is_some_and(|owner| owner != &self.owner)
            || namespace.root_digest() != self.history.namespace_root
            || namespace.config().schema_digest != self.history.schema_digest
        {
            return Err(CatalogError::Conflict("managed birth namespace"));
        }
        Ok(())
    }
}

pub struct FleetBirthReservation {
    pub commit: FleetCommit,
    pub authorization: FleetBirthAuthorization,
}

impl FleetCatalog {
    /// Bind a fresh namespace root to this exact coordination key scope. A
    /// different catalog prefix cannot issue an untracked competing birth.
    pub fn bind_namespace(&self, mut config: NamespaceConfig) -> Result<NamespaceConfig> {
        if config.cluster.bytes() != self.inner.cluster
            || config.mode != cellar_logic::NamespaceMode::Raft
            || config.voters != VoterPolicy::ThreeVoters
            || config.schema_digest == [0; 32]
        {
            return Err(FleetCatalogError::Invalid("managed namespace"));
        }
        let owner = self.birth_owner()?;
        if config
            .managed_fleet
            .as_ref()
            .is_some_and(|old| old != &owner)
        {
            return Err(FleetCatalogError::Conflict);
        }
        config.managed_fleet = Some(owner);
        Ok(config)
    }

    pub(super) fn birth_owner(&self) -> Result<ManagedBirthOwner> {
        let bytes = serde_json::to_vec(&(
            "cellar-fleet-birth-owner-1",
            self.inner.cluster,
            self.inner.incarnation,
            &self.inner.bucket.prefix,
            &self.inner.prefix,
        ))
        .map_err(|_| FleetCatalogError::Invalid("catalog identity"))?;
        Ok(ManagedBirthOwner {
            fleet: self.inner.incarnation,
            catalog: hash(&bytes),
        })
    }

    /// Import one observed history. This never marks an inventory complete;
    /// cut establishment and completeness certification remain separate work.
    pub async fn enroll_history(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        directory: &RangeDirectory,
        birth: &CellGroupBirth,
        class: &str,
        timeout: Duration,
    ) -> Result<FleetCommit> {
        self.check_snapshot(snapshot)?;
        validate_request(request)?;
        let namespace = directory.namespace();
        if namespace.config().managed_fleet.is_some()
            && self.bind_namespace(namespace.config().clone())? != *namespace.config()
        {
            return Err(FleetCatalogError::Conflict);
        }
        let history = FleetHistory::observed(
            self.binding(&birth.route().history)?,
            directory,
            birth,
            ProposalId::new(request.id).map_err(|_| FleetCatalogError::Invalid("admission"))?,
            ProposalId::new(request.operation)
                .map_err(|_| FleetCatalogError::Invalid("operation"))?,
        )?;
        self.run(
            timeout,
            self.insert_history(snapshot, request, &history, directory, class, false),
        )
        .await
    }

    pub async fn reserve_birth(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        directory: &RangeDirectory,
        proposal: CellGroupBirthSpec,
        class: &str,
        timeout: Duration,
    ) -> Result<FleetBirthReservation> {
        self.check_snapshot(snapshot)?;
        validate_request(request)?;
        let namespace = directory.namespace();
        if namespace.config().cluster.bytes() != self.inner.cluster
            || namespace.config().voters != VoterPolicy::ThreeVoters
            || proposal.voters.len() != 3
            || !proposal.learners.is_empty()
        {
            return Err(FleetCatalogError::Invalid("managed initial placement"));
        }
        self.run(timeout, async {
            let owner = self.managed_owner_for_birth(snapshot, directory).await?;
            let proposal = directory
                .prepare_fleet_group_birth(proposal, &owner)
                .map_err(catalog_error)?;
            let history = FleetHistory {
                binding: self.binding(&proposal.route().history)?,
                schema_digest: namespace.config().schema_digest,
                namespace_root: namespace.root_digest(),
                birth_digest: proposal.route().birth_digest,
                lower: bound_key(proposal.route().range.lower()),
                upper: bound_key(proposal.route().range.upper()),
                epoch: proposal.route().epoch.get(),
                creation: proposal.creation().bytes(),
                admission: request.id,
                operation: request.operation,
                voters: proposal
                    .voters()
                    .iter()
                    .copied()
                    .map(Member::from)
                    .collect(),
                learners: vec![],
                state: FleetHistoryState::Reserved,
                origin: Origin::Managed,
            };
            history.validate()?;
            let commit = self
                .insert_history(snapshot, request, &history, directory, class, true)
                .await?;
            Ok(FleetBirthReservation {
                commit,
                authorization: FleetBirthAuthorization {
                    history,
                    proposal,
                    owner,
                },
            })
        })
        .await
    }

    /// Recover the original birth capability after losing its in-memory handle.
    /// The immutable admission journal, namespace and exact history must agree.
    /// A later Live/Cancelled disposition does not erase the reservation's
    /// identity or override the permanent namespace winner.
    pub async fn recover_birth_reservation(
        &self,
        snapshot: &FleetSnapshot,
        directory: &RangeDirectory,
        scope: &GroupHistory,
        timeout: Duration,
    ) -> Result<FleetBirthAuthorization> {
        self.check_snapshot(snapshot)?;
        let namespace = directory.namespace();
        let binding = self.binding(scope)?;
        let key = binding
            .key()
            .map_err(|_| FleetCatalogError::Invalid("history key"))?;
        self.run(timeout, async {
            if self.inner.current().await?.token != snapshot.token {
                return Err(FleetCatalogError::Conflict);
            }
            let mut history = self.history_inner(snapshot, &key).await?;
            if history.binding != binding || history.origin != Origin::Managed {
                return Err(FleetCatalogError::Conflict);
            }
            // Only state changes during confirmation. Reconstruct the exact
            // reservation and verify all three original index changes below.
            history.state = FleetHistoryState::Reserved;
            let (mutation, _) = self
                .inner
                .read::<FleetMutation>(
                    &format!("requests/{}", hex::encode(history.admission)),
                    REQUEST_KIND,
                    REQUEST_LIMIT,
                )
                .await?
                .ok_or(FleetCatalogError::Corrupt("missing birth admission"))?;
            mutation.validate()?;
            let request = FleetRequestIdentity {
                id: mutation.id,
                operation: mutation.operation,
                principal: mutation.principal.clone(),
                expected_revision: mutation.expected_revision,
            };
            let expected = history_mutation(
                &request,
                vec![
                    FleetChange {
                        table: FleetTable::Histories,
                        key: key.clone(),
                        value: Some(history.encode()?),
                    },
                    FleetChange {
                        table: FleetTable::Policy,
                        key: history.name_key()?,
                        value: Some(key.as_bytes().to_vec()),
                    },
                    FleetChange {
                        table: FleetTable::Policy,
                        key: history.group_key()?,
                        value: Some(key.as_bytes().to_vec()),
                    },
                ],
            );
            if mutation.id != history.admission
                || mutation.operation != history.operation
                || mutation != expected
            {
                return Err(FleetCatalogError::Corrupt("birth admission projection"));
            }
            let applied = self
                .inner
                .committed(
                    &snapshot.head,
                    &hex::encode(mutation.id),
                    hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?),
                )
                .await?;
            if applied.is_none() || applied != request.expected_revision.checked_add(1) {
                return Err(FleetCatalogError::Corrupt("unadmitted birth reservation"));
            }
            let owner = self.managed_owner_for_birth(snapshot, directory).await?;
            let proposal = directory
                .prepare_fleet_group_birth(history.initial_placement()?, &owner)
                .map_err(catalog_error)?;
            let digest = proposal.route().birth_digest;
            let authorization = FleetBirthAuthorization {
                history,
                proposal,
                owner,
            };
            authorization
                .check_namespace(namespace)
                .map_err(catalog_error)?;
            if digest != authorization.history.birth_digest {
                return Err(FleetCatalogError::Corrupt("birth reservation digest"));
            }
            Ok(authorization)
        })
        .await
    }

    /// Exact-key reconciliation, followed by the original revision's CAS.
    /// Missing or mismatched births preserve the reserved inventory reference.
    pub async fn confirm_birth(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        directory: &RangeDirectory,
        authorization: &FleetBirthAuthorization,
        timeout: Duration,
    ) -> Result<FleetCommit> {
        self.check_snapshot(snapshot)?;
        validate_request(request)?;
        let namespace = directory.namespace();
        authorization
            .check_namespace(namespace)
            .map_err(catalog_error)?;
        if authorization.history.binding.fleet() != self.inner.incarnation {
            return Err(FleetCatalogError::Conflict);
        }
        self.run(timeout, async {
            if self.managed_owner_for_birth(snapshot, directory).await? != authorization.owner {
                return Err(FleetCatalogError::Conflict);
            }
            let mut history = authorization.history.clone();
            let birth = self
                .inner
                .domain
                .run(Box::pin(
                    directory.materialize_fleet_group_birth(authorization),
                ))
                .await
                .map_err(|_| FleetCatalogError::Closed)?
                .map_err(catalog_error)?;
            if !history.check_birth(directory, &birth) {
                return Err(FleetCatalogError::Conflict);
            }
            history.state = FleetHistoryState::Live;
            let key = history
                .binding
                .key()
                .map_err(|_| FleetCatalogError::Invalid("history key"))?;
            let mutation = history_mutation(
                request,
                vec![FleetChange {
                    table: FleetTable::Histories,
                    key: key.clone(),
                    value: Some(history.encode()?),
                }],
            );
            mutation.validate()?;
            let current = self.inner.current().await?;
            let digest = hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?);
            if self
                .inner
                .committed(&current.head, &hex::encode(mutation.id), digest)
                .await?
                .is_none()
            {
                let saved = self.history_inner(snapshot, &key).await?;
                if saved != authorization.history {
                    return Err(FleetCatalogError::Conflict);
                }
            }
            self.commit_inner(snapshot, &mutation).await
        })
        .await
    }

    pub async fn history(
        &self,
        snapshot: &FleetSnapshot,
        scope: &GroupHistory,
        timeout: Duration,
    ) -> Result<FleetHistory> {
        self.check_snapshot(snapshot)?;
        let binding = self.binding(scope)?;
        let key = binding
            .key()
            .map_err(|_| FleetCatalogError::Invalid("history key"))?;
        self.run(timeout, async {
            let value = self.history_inner(snapshot, &key).await?;
            if value.binding != binding {
                return Err(FleetCatalogError::Conflict);
            }
            Ok(value)
        })
        .await
    }

    pub(super) async fn history_inner(
        &self,
        snapshot: &FleetSnapshot,
        key: &str,
    ) -> Result<FleetHistory> {
        let bytes = self
            .inner
            .lookup(snapshot.head.tables[FleetTable::Histories.slot()], key)
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        let history = FleetHistory::decode(&bytes)?;
        if history.binding.fleet() != self.inner.incarnation
            || history
                .binding
                .scope()
                .map_err(|_| FleetCatalogError::Corrupt("history scope"))?
                .cluster
                .bytes()
                != self.inner.cluster
            || history
                .binding
                .key()
                .map_err(|_| FleetCatalogError::Corrupt("history key"))?
                != key
        {
            return Err(FleetCatalogError::Corrupt("history index binding"));
        }
        Ok(history)
    }

    async fn insert_history(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        history: &FleetHistory,
        directory: &RangeDirectory,
        class: &str,
        placement: bool,
    ) -> Result<FleetCommit> {
        let namespace = directory.namespace();
        let key = history
            .binding
            .key()
            .map_err(|_| FleetCatalogError::Invalid("history key"))?;
        let scope = history
            .binding
            .scope()
            .map_err(|_| FleetCatalogError::Invalid("history scope"))?;
        if self.binding(&scope)? != history.binding {
            return Err(FleetCatalogError::Conflict);
        }
        let mutation = history_mutation(
            request,
            vec![
                FleetChange {
                    table: FleetTable::Histories,
                    key: key.clone(),
                    value: Some(history.encode()?),
                },
                FleetChange {
                    table: FleetTable::Policy,
                    key: history.name_key()?,
                    value: Some(key.as_bytes().to_vec()),
                },
                FleetChange {
                    table: FleetTable::Policy,
                    key: history.group_key()?,
                    value: Some(key.as_bytes().to_vec()),
                },
            ],
        );
        mutation.validate()?;
        let current = self.inner.current().await?;
        let digest = hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?);
        if self
            .inner
            .committed(&current.head, &hex::encode(mutation.id), digest)
            .await?
            .is_none()
        {
            self.require_namespace_declaration(snapshot, namespace, Some(class))
                .await?;
            if current.token != snapshot.token || request.expected_revision != snapshot.revision() {
                return Err(FleetCatalogError::Conflict);
            }
            if snapshot.head.history_count >= HISTORY_LIMIT {
                return Err(FleetCatalogError::Limit);
            }
            if self
                .inner
                .lookup(snapshot.head.tables[FleetTable::Histories.slot()], &key)
                .await?
                .is_some()
                || self
                    .inner
                    .lookup(
                        snapshot.head.tables[FleetTable::Policy.slot()],
                        &history.name_key()?,
                    )
                    .await?
                    .is_some()
                || self
                    .inner
                    .lookup(
                        snapshot.head.tables[FleetTable::Policy.slot()],
                        &history.group_key()?,
                    )
                    .await?
                    .is_some()
            {
                return Err(FleetCatalogError::Conflict);
            }
            if placement {
                let nodes = self.node_records(snapshot).await?;
                let mut hosts = BTreeSet::new();
                for member in &history.voters {
                    let node = nodes
                        .iter()
                        .find(|node| node.node == member.node)
                        .ok_or(FleetCatalogError::Conflict)?;
                    if !node.state.eligibility().receive_replicas
                        || node.storage_instance.is_none()
                        || !hosts.insert(match snapshot.replica_separation() {
                            FleetReplicaSeparation::Nodes => node.node,
                            FleetReplicaSeparation::PhysicalHosts => node.physical_host,
                        })
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                    // Active is a placement hint. An ordinary retained boot
                    // invalidates the prior capacity receipt without erasing
                    // that state or its existing group ownership.
                    let target = NodeId::new(member.node)
                        .map_err(|_| FleetCatalogError::Corrupt("birth capacity node"))?;
                    if !self
                        .capacity_from(snapshot, target)
                        .await?
                        .receive_replicas()
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                }
                // This claim grants no birth authority. Confirming it before
                // the root CAS closes races with other namespace catalogs that
                // share the global group-ID key, including unmanaged imports.
                let owner = self.birth_owner()?;
                let proposal = directory
                    .prepare_fleet_group_birth(history.initial_placement()?, &owner)
                    .map_err(catalog_error)?;
                self.inner
                    .domain
                    .run(Box::pin(
                        directory.reserve_fleet_group_identity(&proposal, &owner),
                    ))
                    .await
                    .map_err(|_| FleetCatalogError::Closed)?
                    .map_err(catalog_error)?;
            }
        }
        self.commit_inner(snapshot, &mutation).await
    }
}

pub(super) fn history_mutation(
    request: &FleetRequestIdentity,
    changes: Vec<FleetChange>,
) -> FleetMutation {
    FleetMutation {
        id: request.id,
        operation: request.operation,
        expected_revision: request.expected_revision,
        principal: request.principal.clone(),
        changes,
    }
}

pub(super) fn validate_request(request: &FleetRequestIdentity) -> Result<()> {
    if request.id == [0; 16]
        || request.operation == [0; 16]
        || request.expected_revision == 0
        || request.principal.is_empty()
        || request.principal.len() > 256
        || request.principal.chars().any(char::is_control)
    {
        return Err(FleetCatalogError::Invalid("history mutation identity"));
    }
    Ok(())
}

pub(super) fn catalog_error(error: CatalogError) -> FleetCatalogError {
    match error {
        CatalogError::Invalid(reason) => FleetCatalogError::Invalid(reason),
        CatalogError::Corrupt(reason) => FleetCatalogError::Corrupt(reason),
        CatalogError::Limit => FleetCatalogError::Limit,
        CatalogError::Unconfirmed => FleetCatalogError::Unconfirmed,
        CatalogError::Storage(error) => FleetCatalogError::Storage(error.into()),
        CatalogError::FleetAuthority(error) => *error,
        _ => FleetCatalogError::Conflict,
    }
}
