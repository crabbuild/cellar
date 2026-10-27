//! Opaque capabilities minted only from an admitted immutable catalog index.
//! A capability never replaces the group's current quorum or consumed sequence.
use super::*;
use crate::raft::member_state::{FleetGroupBinding, FleetMembershipPermit};
use cellar_logic::consensus::GroupScope;

#[derive(Clone, Debug)]
pub struct FleetGroupAuthorization {
    binding: FleetGroupBinding,
    history: Box<super::histories::FleetHistory>,
    owner: super::super::catalog::ManagedBirthOwner,
}

impl FleetGroupAuthorization {
    pub(crate) fn matches_replica(
        &self,
        replica: &cellar_logic::consensus::ReplicaIdentity,
        membership: &crate::raft::member_state::GroupMembership,
    ) -> bool {
        if !self.matches_scope(&replica.scope) || !membership.permits(replica) {
            return false;
        }
        match membership.joined(replica.member) {
            Ok(Some((joined, _))) => joined.node == replica.node && joined.disk == replica.disk,
            Ok(None) => self
                .history
                .voters
                .iter()
                .chain(&self.history.learners)
                .any(|initial| {
                    initial.member == replica.member.get() && initial.node == replica.node.bytes()
                }),
            Err(_) => false,
        }
    }

    pub(crate) fn history_digests(&self) -> ([u8; 32], [u8; 32]) {
        (self.history.birth_digest(), self.history.schema_digest())
    }

    pub(crate) fn allows_managed_genesis(&self) -> bool {
        self.history.allows_managed_genesis()
    }

    pub(crate) fn check_birth(
        &self,
        namespace: &crate::raft::catalog::NamespaceCatalog,
        birth: &crate::raft::catalog::GroupBirth,
    ) -> bool {
        self.history.check_birth(namespace, birth)
            && birth
                .managed_owner()
                .is_none_or(|owner| owner == &self.owner)
    }

    pub(crate) fn matches_birth(
        &self,
        birth: &crate::raft::catalog::GroupBirth,
        schema: [u8; 32],
    ) -> bool {
        self.matches_scope(birth.scope())
            && self.history.birth_digest() == birth.digest()
            && self.history.schema_digest() == schema
            && birth
                .managed_owner()
                .is_none_or(|owner| owner == &self.owner)
    }

    pub(crate) fn binding(&self) -> &FleetGroupBinding {
        &self.binding
    }

    pub(crate) fn matches_scope(&self, scope: &GroupScope) -> bool {
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

    pub(crate) fn matches_scope(&self, scope: &GroupScope) -> bool {
        self.permit.binding().matches_scope(scope)
    }
}

impl FleetCatalog {
    /// Resolve an enrollment capability from this adapter's rooted history
    /// snapshot. A saved snapshot is not a lease; group state owns fencing.
    pub async fn authorize_group(
        &self,
        snapshot: &FleetSnapshot,
        scope: &GroupScope,
        timeout: Duration,
    ) -> Result<FleetGroupAuthorization> {
        self.check_snapshot(snapshot)?;
        let expected = self.binding(scope)?;
        let key = expected
            .key()
            .map_err(|_| FleetCatalogError::Invalid("group binding"))?;
        self.run(timeout, async {
            let bytes = self
                .inner
                .lookup(snapshot.head.tables[FleetTable::Histories.slot()], &key)
                .await?
                .ok_or(FleetCatalogError::Conflict)?;
            let history = super::histories::FleetHistory::decode(&bytes)?;
            let actual = history.binding().clone();
            if actual != expected {
                return Err(FleetCatalogError::Conflict);
            }
            if *history.state() != FleetHistoryState::Live {
                return Err(FleetCatalogError::Conflict);
            }
            Ok(FleetGroupAuthorization {
                binding: actual,
                history: Box::new(history),
                owner: self.birth_owner()?,
            })
        })
        .await
    }

    /// Resolve the exact rooted admission for a registered history. Replaying
    /// this capability after cancellation is refused by its committed sequence.
    pub async fn authorize_membership(
        &self,
        snapshot: &FleetSnapshot,
        scope: &GroupScope,
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

    pub(super) fn binding(&self, scope: &GroupScope) -> Result<FleetGroupBinding> {
        if scope.cluster.bytes() != self.inner.cluster {
            return Err(FleetCatalogError::Conflict);
        }
        FleetGroupBinding::new(
            IncarnationId::new(self.inner.incarnation)
                .map_err(|_| FleetCatalogError::Corrupt("fleet incarnation"))?,
            scope,
            self.birth_owner()?.catalog,
        )
        .map_err(|_| FleetCatalogError::Invalid("group scope"))
    }

    /// Exact catalog-scoped descriptor. The value itself grants no authority.
    pub fn group_binding(&self, scope: &GroupScope) -> Result<FleetGroupBinding> {
        self.binding(scope)
    }
}
