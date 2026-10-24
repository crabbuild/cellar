//! Birth intent is rooted before the immutable namespace decision. Pending and
//! cancelled histories retain their node references; neither timeout nor an
//! empty runtime scan proves that those references can be released.
use super::*;
use crate::raft::catalog::{
    BirthDisposition, BirthProposal, CatalogError, GroupBirth, InitialMember, ManagedBirthOwner,
    NamespaceCatalog, NamespaceConfig, VoterPolicy,
};
use crate::raft::member_state::FleetGroupBinding;
use cellar_logic::consensus::{GroupScope, MemberId, NodeId, ProposalId};
use serde::Deserialize;
use std::collections::BTreeSet;

const HISTORY_KIND: u8 = 48;

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
struct Member {
    member: u64,
    node: [u8; 16],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FleetHistory {
    binding: FleetGroupBinding,
    schema_digest: [u8; 32],
    namespace_root: [u8; 32],
    birth_digest: [u8; 32],
    admission: [u8; 16],
    operation: [u8; 16],
    voters: Vec<Member>,
    learners: Vec<Member>,
    state: FleetHistoryState,
}

impl FleetHistory {
    /// An exact observed birth is import material, not proof that the namespace
    /// inventory is complete or that legacy creation has been quiesced.
    pub fn observed(
        fleet: IncarnationId,
        namespace: &NamespaceCatalog,
        birth: &GroupBirth,
        admission: ProposalId,
        operation: ProposalId,
    ) -> Result<Self> {
        namespace.check_birth(birth).map_err(catalog_error)?;
        if namespace
            .config()
            .managed_fleet
            .as_ref()
            .is_some_and(|owner| owner.fleet != fleet.bytes())
        {
            return Err(FleetCatalogError::Conflict);
        }
        let value = Self {
            binding: FleetGroupBinding::new(fleet, birth.scope())
                .map_err(|_| FleetCatalogError::Invalid("history scope"))?,
            schema_digest: namespace.config().schema_digest,
            namespace_root: namespace.root_digest(),
            birth_digest: birth.digest(),
            admission: admission.bytes(),
            operation: operation.bytes(),
            voters: birth.voters().iter().copied().map(Member::from).collect(),
            learners: birth.learners().iter().copied().map(Member::from).collect(),
            state: FleetHistoryState::Live,
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

    pub fn encode(&self) -> Result<Vec<u8>> {
        self.validate()?;
        encode(HISTORY_KIND, self, 4096)
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Self = decode(HISTORY_KIND, bytes, 4096)?;
        value.validate()?;
        Ok(value)
    }

    fn validate(&self) -> Result<()> {
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
    pub fn initial_placement(&self) -> Result<BirthProposal> {
        Ok(BirthProposal {
            scope: self
                .binding
                .scope()
                .map_err(|_| FleetCatalogError::Corrupt("history scope"))?,
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

    fn name_key(&self) -> Result<String> {
        let scope = self
            .binding
            .scope()
            .map_err(|_| FleetCatalogError::Corrupt("history scope"))?;
        Ok(format!(
            "birth/{}/{}",
            hex::encode(scope.namespace.bytes()),
            hex::encode(hash(scope.cell.as_bytes()))
        ))
    }

    pub(crate) fn check_birth(&self, namespace: &NamespaceCatalog, birth: &GroupBirth) -> bool {
        namespace.check_birth(birth).is_ok()
            && namespace
                .config()
                .managed_fleet
                .as_ref()
                .is_none_or(|owner| owner.fleet == self.binding.fleet())
            && self.binding.matches_scope(birth.scope())
            && self.namespace_root == namespace.root_digest()
            && self.schema_digest == namespace.config().schema_digest
            && self.birth_digest == birth.digest()
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
    proposal: BirthProposal,
}

impl FleetBirthAuthorization {
    pub(crate) fn check_birth(&self, namespace: &NamespaceCatalog, birth: &GroupBirth) -> bool {
        self.history.check_birth(namespace, birth)
    }

    pub fn scope(&self) -> &GroupScope {
        &self.proposal.scope
    }

    pub(crate) fn proposal(&self) -> BirthProposal {
        self.proposal.clone()
    }

    pub(crate) fn check_namespace(
        &self,
        namespace: &NamespaceCatalog,
    ) -> std::result::Result<(), CatalogError> {
        if namespace
            .config()
            .managed_fleet
            .as_ref()
            .map(|owner| owner.fleet)
            != Some(self.history.binding.fleet())
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
        let bytes = serde_json::to_vec(&(
            "cellar-fleet-birth-owner-1",
            self.inner.cluster,
            self.inner.incarnation,
            &self.inner.bucket.prefix,
            &self.inner.prefix,
        ))
        .map_err(|_| FleetCatalogError::Invalid("catalog identity"))?;
        let owner = ManagedBirthOwner {
            fleet: self.inner.incarnation,
            catalog: hash(&bytes),
        };
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

    /// Import one observed history. This never marks an inventory complete;
    /// cut establishment and completeness certification remain separate work.
    pub async fn enroll_history(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        namespace: &NamespaceCatalog,
        birth: &GroupBirth,
        timeout: Duration,
    ) -> Result<FleetCommit> {
        self.check_snapshot(snapshot)?;
        validate_request(request)?;
        if namespace.config().managed_fleet.is_some()
            && self.bind_namespace(namespace.config().clone())? != *namespace.config()
        {
            return Err(FleetCatalogError::Conflict);
        }
        let history = FleetHistory::observed(
            IncarnationId::new(self.inner.incarnation)
                .map_err(|_| FleetCatalogError::Corrupt("fleet"))?,
            namespace,
            birth,
            ProposalId::new(request.id).map_err(|_| FleetCatalogError::Invalid("admission"))?,
            ProposalId::new(request.operation)
                .map_err(|_| FleetCatalogError::Invalid("operation"))?,
        )?;
        self.run(
            timeout,
            self.insert_history(snapshot, request, &history, false),
        )
        .await
    }

    pub async fn reserve_birth(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        namespace: &NamespaceCatalog,
        proposal: BirthProposal,
        timeout: Duration,
    ) -> Result<FleetBirthReservation> {
        self.check_snapshot(snapshot)?;
        validate_request(request)?;
        if self.bind_namespace(namespace.config().clone())? != *namespace.config()
            || namespace.config().cluster.bytes() != self.inner.cluster
            || namespace.config().voters != VoterPolicy::ThreeVoters
            || proposal.voters.len() != 3
            || !proposal.learners.is_empty()
        {
            return Err(FleetCatalogError::Invalid("managed initial placement"));
        }
        let (proposal, digest) = namespace.prepare_birth(proposal).map_err(catalog_error)?;
        let history = FleetHistory {
            binding: self.binding(&proposal.scope)?,
            schema_digest: namespace.config().schema_digest,
            namespace_root: namespace.root_digest(),
            birth_digest: digest,
            admission: request.id,
            operation: request.operation,
            voters: proposal.voters.iter().copied().map(Member::from).collect(),
            learners: vec![],
            state: FleetHistoryState::Reserved,
        };
        history.validate()?;
        let commit = self
            .run(
                timeout,
                self.insert_history(snapshot, request, &history, true),
            )
            .await?;
        Ok(FleetBirthReservation {
            commit,
            authorization: FleetBirthAuthorization { history, proposal },
        })
    }

    /// Exact-key reconciliation, followed by the original revision's CAS.
    /// Missing or mismatched births preserve the reserved inventory reference.
    pub async fn confirm_birth(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        namespace: &NamespaceCatalog,
        authorization: &FleetBirthAuthorization,
        timeout: Duration,
    ) -> Result<FleetCommit> {
        self.check_snapshot(snapshot)?;
        validate_request(request)?;
        authorization
            .check_namespace(namespace)
            .map_err(catalog_error)?;
        if authorization.history.binding.fleet() != self.inner.incarnation
            || self.bind_namespace(namespace.config().clone())? != *namespace.config()
        {
            return Err(FleetCatalogError::Conflict);
        }
        self.run(timeout, async {
            let mut history = authorization.history.clone();
            match namespace
                .birth_disposition(&authorization.scope().cell)
                .await
                .map_err(catalog_error)?
            {
                Some(BirthDisposition::Live(birth)) if history.check_birth(namespace, &birth) => {
                    history.state = FleetHistoryState::Live
                }
                Some(BirthDisposition::Cancelled { scope, operation })
                    if scope == *authorization.scope() =>
                {
                    history.state = FleetHistoryState::Cancelled {
                        operation: operation.bytes(),
                    }
                }
                _ => return Err(FleetCatalogError::Conflict),
            }
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
        scope: &GroupScope,
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
        placement: bool,
    ) -> Result<FleetCommit> {
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
                        || !hosts.insert(node.physical_host)
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                }
            }
        }
        self.commit_inner(snapshot, &mutation).await
    }
}

fn history_mutation(request: &FleetRequestIdentity, changes: Vec<FleetChange>) -> FleetMutation {
    FleetMutation {
        id: request.id,
        operation: request.operation,
        expected_revision: request.expected_revision,
        principal: request.principal.clone(),
        changes,
    }
}

fn validate_request(request: &FleetRequestIdentity) -> Result<()> {
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

fn catalog_error(error: CatalogError) -> FleetCatalogError {
    match error {
        CatalogError::Invalid(reason) => FleetCatalogError::Invalid(reason),
        CatalogError::Corrupt(reason) => FleetCatalogError::Corrupt(reason),
        CatalogError::Limit => FleetCatalogError::Limit,
        CatalogError::Unconfirmed => FleetCatalogError::Unconfirmed,
        CatalogError::Storage(error) => FleetCatalogError::Storage(error.into()),
        _ => FleetCatalogError::Conflict,
    }
}
