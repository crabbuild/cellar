//! Opaque capabilities minted only from an admitted immutable catalog index.
//! A capability never replaces the group's current quorum or consumed sequence.
use super::*;
use crate::raft::member_state::{FleetGroupBinding, FleetMembershipPermit};
use cellar_logic::{
    cell_group::GroupHistory,
    consensus::{DiskLineage, MemberId, NodeId},
};

#[derive(Clone, Debug)]
pub struct FleetGroupAuthorization {
    binding: FleetGroupBinding,
    history: Box<super::histories::FleetHistory>,
    owner: super::super::catalog::ManagedBirthOwner,
}

pub(crate) struct RouterVoter {
    pub(crate) member: MemberId,
    pub(crate) node: NodeId,
    pub(crate) joined_disk: Option<DiskLineage>,
}

impl FleetGroupAuthorization {
    pub(crate) fn matches_replica<S>(
        &self,
        replica: &cellar_logic::consensus::ReplicaIdentity<S>,
        membership: &crate::raft::member_state::GroupMembership,
    ) -> bool
    where
        for<'a> GroupHistory: From<&'a S>,
    {
        if !self.matches_scope(&replica.scope) || !membership.permits(replica) {
            return false;
        }
        match membership.joined(replica.member) {
            Ok(Some((joined, _))) => joined.node == replica.node && joined.disk == replica.disk,
            Ok(None) => self.initial_member_node(replica.member) == Some(replica.node),
            Err(_) => false,
        }
    }

    /// Resolve all current physical occupants from the immutable birth or the
    /// committed joining bindings. An advisory route may omit an isolated peer.
    pub(crate) fn placement_nodes(
        &self,
        observed: &crate::raft::cell_groups::CellGroupQuorumMembership,
    ) -> std::result::Result<Vec<NodeId>, crate::raft::storage::LogError> {
        let invalid =
            || crate::raft::storage::LogError::Invalid("group placement authority".into());
        if !self.matches_replica(observed.replica(), observed.membership())
            || self.history_digests()
                != (
                    observed.root().route().birth_digest,
                    observed.root().route().schema_digest,
                )
        {
            return Err(invalid());
        }
        crate::raft::member_state::members(observed.configuration())
            .into_iter()
            .map(|id| {
                let member = cellar_logic::consensus::MemberId::new(id).map_err(|_| invalid())?;
                if let Some((binding, _)) = observed.membership().joined(member)? {
                    return Ok(binding.node);
                }
                self.initial_member_node(member).ok_or_else(invalid)
            })
            .collect()
    }

    /// Resolve initial or joined voters from one authenticated quorum sample.
    /// A member number alone supplies no endpoint or boot authority.
    pub(crate) fn router_voters(
        &self,
        observed: &crate::raft::host::FleetMembershipObservation,
    ) -> Result<Vec<RouterVoter>> {
        if !self.matches_scope(&observed.replica().scope)
            || self.binding() != &observed.status().binding
            || self.history_digests() != (observed.birth_digest(), observed.schema_digest())
        {
            return Err(FleetCatalogError::Conflict);
        }
        let conf = observed.configuration();
        let members = conf
            .voters()
            .iter()
            .chain(conf.outgoing_voters())
            .copied()
            .collect::<std::collections::BTreeSet<_>>();
        members
            .into_iter()
            .map(|member| {
                if let Some((joined, _)) = observed
                    .joined_member(member)
                    .map_err(|_| FleetCatalogError::Invalid("router member binding"))?
                {
                    return Ok(RouterVoter {
                        member,
                        node: joined.node,
                        joined_disk: Some(joined.disk),
                    });
                }
                let node = self
                    .initial_member_node(member)
                    .ok_or(FleetCatalogError::Conflict)?;
                Ok(RouterVoter {
                    member,
                    node,
                    joined_disk: None,
                })
            })
            .collect()
    }

    fn initial_member_node(&self, member: MemberId) -> Option<NodeId> {
        self.history
            .voters
            .iter()
            .chain(&self.history.learners)
            .find(|initial| initial.member == member.get())
            .and_then(|initial| NodeId::new(initial.node).ok())
    }

    pub(crate) fn history_digests(&self) -> ([u8; 32], [u8; 32]) {
        (self.history.birth_digest(), self.history.schema_digest())
    }

    pub(crate) fn allows_managed_genesis(&self) -> bool {
        self.history.allows_managed_genesis()
    }

    pub(crate) fn check_birth(
        &self,
        directory: &crate::raft::cell_groups::RangeDirectory,
        birth: &crate::raft::cell_groups::CellGroupBirth,
    ) -> bool {
        self.history.check_birth(directory, birth)
            && directory
                .namespace()
                .config()
                .managed_fleet
                .as_ref()
                .is_none_or(|owner| owner == &self.owner)
    }

    pub(crate) fn binding(&self) -> &FleetGroupBinding {
        &self.binding
    }

    pub(crate) fn matches_scope<S>(&self, scope: &S) -> bool
    where
        for<'a> GroupHistory: From<&'a S>,
    {
        self.binding.matches_scope(scope)
    }
}

#[derive(Clone, Debug)]
pub struct FleetPermitAuthorization {
    permit: FleetMembershipPermit,
    minimum_trust_revision: u64,
    // Low-level unmanaged permits have no physical reservation. Such a
    // capability cannot authorize managed placement or excluded-slot creation.
    target: Option<crate::raft::fleet_stop::BootBinding>,
}

impl FleetPermitAuthorization {
    pub(super) fn admitted(
        permit: FleetMembershipPermit,
        minimum_trust_revision: u64,
        target: Option<crate::raft::fleet_stop::BootBinding>,
    ) -> Self {
        Self {
            permit,
            minimum_trust_revision,
            target,
        }
    }

    pub(crate) fn target_boot(&self) -> Option<&crate::raft::fleet_stop::BootBinding> {
        self.target.as_ref()
    }

    pub(crate) fn minimum_trust_revision(&self) -> u64 {
        self.minimum_trust_revision
    }

    pub(crate) fn permit(&self) -> &FleetMembershipPermit {
        &self.permit
    }

    pub(crate) fn matches_scope<S>(&self, scope: &S) -> bool
    where
        for<'a> GroupHistory: From<&'a S>,
    {
        self.permit.binding().matches_scope(scope)
    }
}

impl FleetCatalog {
    /// Resolve an enrollment capability from this adapter's rooted history
    /// snapshot. A saved snapshot is not a lease; group state owns fencing.
    pub async fn authorize_group(
        &self,
        snapshot: &FleetSnapshot,
        scope: &GroupHistory,
        timeout: Duration,
    ) -> Result<FleetGroupAuthorization> {
        self.check_snapshot(snapshot)?;
        let expected = self.binding(scope)?;
        let key = expected
            .key()
            .map_err(|_| FleetCatalogError::Invalid("group binding"))?;
        self.run(timeout, self.authorize_live_group(snapshot, expected, &key))
            .await
    }

    pub(super) async fn authorize_live_group(
        &self,
        snapshot: &FleetSnapshot,
        expected: FleetGroupBinding,
        key: &str,
    ) -> Result<FleetGroupAuthorization> {
        let bytes = self
            .inner
            .lookup(snapshot.head.tables[FleetTable::Histories.slot()], key)
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        let history = super::histories::FleetHistory::decode(&bytes)?;
        let actual = history.binding().clone();
        if actual != expected || *history.state() != FleetHistoryState::Live {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(FleetGroupAuthorization {
            binding: actual,
            history: Box::new(history),
            owner: self.birth_owner()?,
        })
    }

    /// Resolve the exact rooted admission for a registered history. Replaying
    /// this capability after cancellation is refused by its committed sequence.
    pub async fn authorize_membership(
        &self,
        snapshot: &FleetSnapshot,
        scope: &GroupHistory,
        timeout: Duration,
    ) -> Result<FleetPermitAuthorization> {
        self.check_snapshot(snapshot)?;
        let expected = self.binding(scope)?;
        let key = expected
            .key()
            .map_err(|_| FleetCatalogError::Invalid("group binding"))?;
        self.run(timeout, async {
            let group = self
                .inner
                .lookup(snapshot.head.tables[FleetTable::Histories.slot()], &key)
                .await?
                .ok_or(FleetCatalogError::Conflict)?;
            let history = super::histories::FleetHistory::decode(&group)?;
            if history.binding() != &expected || *history.state() != FleetHistoryState::Live {
                return Err(FleetCatalogError::Conflict);
            }
            let bytes = self
                .inner
                .lookup(snapshot.head.tables[FleetTable::Admissions.slot()], &key)
                .await?
                .ok_or(FleetCatalogError::Conflict)?;
            let permit = FleetMembershipPermit::decode(&bytes)
                .map_err(|_| FleetCatalogError::Corrupt("membership permit"))?;
            if permit.binding() != &expected {
                return Err(FleetCatalogError::Conflict);
            }
            self.resource_permit_authorization(snapshot, permit).await
        })
        .await
    }

    pub(super) fn binding<S>(&self, scope: &S) -> Result<FleetGroupBinding>
    where
        for<'a> GroupHistory: From<&'a S>,
    {
        let scope = GroupHistory::from(scope);
        if scope.cluster.bytes() != self.inner.cluster {
            return Err(FleetCatalogError::Conflict);
        }
        FleetGroupBinding::new(
            IncarnationId::new(self.inner.incarnation)
                .map_err(|_| FleetCatalogError::Corrupt("fleet incarnation"))?,
            &scope,
            self.birth_owner()?.catalog,
        )
        .map_err(|_| FleetCatalogError::Invalid("group scope"))
    }

    /// Exact catalog-scoped descriptor. The value itself grants no authority.
    pub fn group_binding(&self, scope: &GroupHistory) -> Result<FleetGroupBinding> {
        self.binding(scope)
    }
}
