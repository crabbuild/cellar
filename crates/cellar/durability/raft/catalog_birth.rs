//! One immutable cell-name decision: birth or cancellation. Both writers
//! contend on the same conditional-create key, never an advisory side record.
use super::*;
use cellar_logic::consensus::ProposalId;
use std::time::Duration;

/// Exact observed winner at the permanent cell-name key. A live winner follows
/// the ordinary quorum-backed lifecycle; cancellation cannot erase it.
#[derive(Clone, Debug)]
pub enum BirthDisposition {
    Live(GroupBirth),
    Cancelled {
        scope: GroupScope,
        operation: ProposalId,
    },
}

impl BirthDisposition {
    fn live(self) -> Result<GroupBirth, CatalogError> {
        match self {
            Self::Live(birth) => Ok(birth),
            Self::Cancelled { .. } => Err(CatalogError::BirthCancelled),
        }
    }
}

impl NamespaceCatalog {
    /// Concurrent creators follow the winning birth, including its placement;
    /// retry never overwrites that record or substitutes another incarnation.
    /// A permanent cancelled-name tombstone refuses late creation.
    pub async fn resolve_birth(&self, proposal: BirthProposal) -> Result<GroupBirth, CatalogError> {
        if self.config.managed_fleet.is_some() {
            return Err(CatalogError::ManagedAdmissionRequired);
        }
        self.select_birth(proposal, None).await?.live()
    }

    /// Fence an uncreated cell name with an immutable conditional-create
    /// tombstone. A lost/expired reply can follow commitment: inspect the exact
    /// name or retry this same proposal/operation. Never release an inventory
    /// reservation from a timeout or a missing reply.
    ///
    /// If birth or another cancellation won, return that exact observed winner.
    /// The caller must reconcile its identity rather than claim this request
    /// cancelled a different incarnation. The name remains reserved permanently.
    pub async fn cancel_birth(
        &self,
        proposal: BirthProposal,
        operation: ProposalId,
        timeout: Duration,
    ) -> Result<BirthDisposition, CatalogError> {
        if self.config.managed_fleet.is_some() {
            return Err(CatalogError::ManagedAdmissionRequired);
        }
        self.cancel_selected_birth(proposal, operation, timeout)
            .await
    }

    /// A reservation remains inventoried even if drain wins a later revision.
    /// Cancellation contends on the same birth key and never erases a live birth.
    pub async fn cancel_managed_birth(
        &self,
        authorization: &super::super::fleet_catalog::FleetBirthAuthorization,
        operation: ProposalId,
        timeout: Duration,
    ) -> Result<BirthDisposition, CatalogError> {
        authorization.check_namespace(self)?;
        self.cancel_selected_birth(authorization.proposal(), operation, timeout)
            .await
    }

    pub async fn resolve_managed_birth(
        &self,
        authorization: &super::super::fleet_catalog::FleetBirthAuthorization,
        timeout: Duration,
    ) -> Result<GroupBirth, CatalogError> {
        authorization.check_namespace(self)?;
        if timeout.is_zero() || timeout > Duration::from_secs(60) {
            return Err(CatalogError::Invalid("managed birth timeout"));
        }
        let birth =
            crate::asyncrt::timeout(timeout, self.select_birth(authorization.proposal(), None))
                .await
                .map_err(|_| CatalogError::Unconfirmed)??
                .live()?;
        if !authorization.check_birth(self, &birth) {
            return Err(CatalogError::Conflict("reserved birth winner"));
        }
        Ok(birth)
    }

    async fn cancel_selected_birth(
        &self,
        proposal: BirthProposal,
        operation: ProposalId,
        timeout: Duration,
    ) -> Result<BirthDisposition, CatalogError> {
        if timeout.is_zero() || timeout > Duration::from_secs(60) {
            return Err(CatalogError::Invalid("birth cancellation timeout"));
        }
        crate::asyncrt::timeout(timeout, self.select_birth(proposal, Some(operation)))
            .await
            .map_err(|_| CatalogError::Unconfirmed)?
    }

    async fn select_birth(
        &self,
        mut proposal: BirthProposal,
        cancelled: Option<ProposalId>,
    ) -> Result<BirthDisposition, CatalogError> {
        self.require_mode(NamespaceMode::Raft)?;
        self.check_scope(&proposal.scope)?;
        // Bound synchronous admission work before sorting caller-owned vectors;
        // a future deadline cannot preempt an unbounded synchronous sort.
        if proposal.voters.len() > 32 || proposal.learners.len() > 32 {
            return Err(CatalogError::Invalid("initial member count"));
        }
        proposal.voters.sort_by_key(|m| m.member);
        proposal.learners.sort_by_key(|m| m.member);
        self.check_members(&proposal.voters, &proposal.learners)?;
        let requested = proposal.scope.clone();
        let mut birth = self.admit_birth_intent(proposal).await?;
        let scope = birth
            .scope
            .clone()
            .decode()
            .map_err(|_| CatalogError::Corrupt("birth intent scope"))?;
        let key = self.birth_key(&scope.cell);
        if let Some(bytes) = self.inner.read(&key).await? {
            return self.decode_birth_disposition(&scope.cell, &bytes).await;
        }
        if cancelled.is_some() && scope != requested {
            return Err(CatalogError::Conflict("birth intent changed"));
        }
        self.verify_group_identity(&scope).await?;
        birth.cancelled_by = cancelled.map(|operation| operation.bytes());
        let bytes = wire::encode(wire::BIRTH, &birth, self.inner.limit)?;
        let observed = self.inner.create_and_observe(&key, &bytes).await?;
        self.decode_birth_disposition(&scope.cell, &observed).await
    }

    /// A permanent identity claim grants neither birth nor membership authority.
    /// Fleet admission confirms this exact claim before exposing a capability,
    /// so another namespace cannot steal the ID after root admission. A claim
    /// left by a losing root CAS is safe to retain and retry, never to release.
    pub(crate) async fn reserve_group_identity(
        &self,
        scope: &GroupScope,
    ) -> Result<(), CatalogError> {
        self.require_mode(NamespaceMode::Raft)?;
        self.check_scope(scope)?;
        let reservation = wire::Reservation {
            root_digest: self.root_digest,
            scope: WireScope::from(scope),
        };
        let reserved = wire::encode(wire::RESERVATION, &reservation, self.inner.limit)?;
        let observed = self
            .inner
            .create_and_observe(&self.reservation_key(scope), &reserved)
            .await?;
        if observed != reserved {
            return Err(CatalogError::Conflict("group identity already reserved"));
        }
        Ok(())
    }

    pub(crate) async fn verify_group_identity(
        &self,
        scope: &GroupScope,
    ) -> Result<(), CatalogError> {
        let reservation = self
            .inner
            .read(&self.reservation_key(scope))
            .await?
            .ok_or(CatalogError::Corrupt("missing group reservation"))?;
        let value: wire::Reservation =
            wire::decode(wire::RESERVATION, &reservation, self.inner.limit)?;
        if value.root_digest != self.root_digest
            || value
                .scope
                .decode()
                .map_err(|_| CatalogError::Corrupt("reservation scope"))?
                != *scope
        {
            return Err(CatalogError::Conflict("group reservation"));
        }
        Ok(())
    }

    /// Canonical expected live decision, before any storage mutation. This is
    /// also used to bind the rooted reservation to its exact delayed writer.
    pub(crate) fn prepare_birth(
        &self,
        mut proposal: BirthProposal,
    ) -> Result<(BirthProposal, [u8; 32]), CatalogError> {
        self.require_mode(NamespaceMode::Raft)?;
        self.check_scope(&proposal.scope)?;
        if proposal.voters.len() > 32 || proposal.learners.len() > 32 {
            return Err(CatalogError::Invalid("initial member count"));
        }
        proposal.voters.sort_by_key(|m| m.member);
        proposal.learners.sort_by_key(|m| m.member);
        self.check_members(&proposal.voters, &proposal.learners)?;
        let birth = wire::Birth {
            root_digest: self.root_digest,
            scope: WireScope::from(&proposal.scope),
            voters: proposal
                .voters
                .iter()
                .copied()
                .map(wire::Member::encode)
                .collect(),
            learners: proposal
                .learners
                .iter()
                .copied()
                .map(wire::Member::encode)
                .collect(),
            empty_digest: wire::digest(&[]),
            cancelled_by: None,
        };
        let digest = wire::digest(&wire::encode(wire::BIRTH, &birth, self.inner.limit)?);
        Ok((proposal, digest))
    }

    pub async fn birth(&self, cell: &str) -> Result<Option<GroupBirth>, CatalogError> {
        self.birth_disposition(cell)
            .await?
            .map(BirthDisposition::live)
            .transpose()
    }

    /// Exact-key birth/cancellation observation. `None` is an observation of a
    /// missing key, not a fence against a concurrent or delayed create.
    pub async fn birth_disposition(
        &self,
        cell: &str,
    ) -> Result<Option<BirthDisposition>, CatalogError> {
        self.require_mode(NamespaceMode::Raft)?;
        if cell.is_empty() || cell.len() > 1024 || cell.chars().any(char::is_control) {
            return Err(CatalogError::Invalid("cell identity"));
        }
        match self.inner.read(&self.birth_key(cell)).await? {
            Some(bytes) => Ok(Some(self.decode_birth_disposition(cell, &bytes).await?)),
            None => Ok(None),
        }
    }

    async fn decode_birth_disposition(
        &self,
        cell: &str,
        bytes: &[u8],
    ) -> Result<BirthDisposition, CatalogError> {
        let birth: wire::Birth = wire::decode(wire::BIRTH, bytes, self.inner.limit)?;
        let scope = birth
            .scope
            .clone()
            .decode()
            .map_err(|_| CatalogError::Corrupt("birth scope"))?;
        self.check_scope(&scope)?;
        if scope.cell != cell
            || birth.root_digest != self.root_digest
            || birth.empty_digest != wire::digest(&[])
        {
            return Err(CatalogError::Conflict("birth namespace/cell/empty state"));
        }
        let voters = birth
            .voters
            .iter()
            .map(wire::Member::decode)
            .collect::<Result<Vec<_>, _>>()?;
        let learners = birth
            .learners
            .iter()
            .map(wire::Member::decode)
            .collect::<Result<Vec<_>, _>>()?;
        self.check_members(&voters, &learners)?;
        self.verify_group_identity(&scope).await?;
        self.verify_birth_intent(&birth).await?;
        if let Some(operation) = birth.cancelled_by {
            return Ok(BirthDisposition::Cancelled {
                scope,
                operation: ProposalId::new(operation)
                    .map_err(|_| CatalogError::Corrupt("birth cancellation identity"))?,
            });
        }
        Ok(BirthDisposition::Live(GroupBirth {
            scope,
            voters,
            learners,
            digest: wire::digest(bytes),
            root_digest: self.root_digest,
            catalog_owner: self.inner.owner.clone(),
        }))
    }
}
