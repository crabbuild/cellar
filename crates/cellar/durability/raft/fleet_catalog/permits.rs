//! Opaque capabilities minted only from an admitted immutable catalog index.
//! A capability never replaces the group's current quorum or consumed sequence.
use super::*;
use crate::raft::member_state::{FleetGroupBinding, FleetMembershipPermit};
use cellar_logic::consensus::GroupScope;

#[derive(Clone, Debug)]
pub struct FleetGroupAuthorization {
    binding: FleetGroupBinding,
}

impl FleetGroupAuthorization {
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
}

impl FleetPermitAuthorization {
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
            let actual = FleetGroupBinding::decode(&bytes)
                .map_err(|_| FleetCatalogError::Corrupt("group binding"))?;
            if actual != expected {
                return Err(FleetCatalogError::Conflict);
            }
            Ok(FleetGroupAuthorization { binding: actual })
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
            if FleetGroupBinding::decode(&group)
                .map_err(|_| FleetCatalogError::Corrupt("group binding"))?
                != expected
            {
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
            Ok(FleetPermitAuthorization { permit })
        })
        .await
    }

    fn binding(&self, scope: &GroupScope) -> Result<FleetGroupBinding> {
        if scope.cluster.bytes() != self.inner.cluster {
            return Err(FleetCatalogError::Conflict);
        }
        FleetGroupBinding::new(
            IncarnationId::new(self.inner.incarnation)
                .map_err(|_| FleetCatalogError::Corrupt("fleet incarnation"))?,
            scope,
        )
        .map_err(|_| FleetCatalogError::Invalid("group scope"))
    }
}
