// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cell-independent immutable group births. A selected birth is metadata; only
//! durable genesis plus permanent member claims can authorize voter startup.

use super::super::InitialMember;
use super::*;

pub struct CellGroupBirthSpec {
    pub history: GroupHistory,
    pub range: CellRange,
    pub epoch: RouteEpoch,
    pub creation: ProposalId,
    pub voters: Vec<InitialMember>,
    pub learners: Vec<InitialMember>,
}

/// Persist the canonical request before reserving its global group ID. Retrying
/// must keep these bytes, including placement, creation identity and incarnation.
#[derive(Clone)]
pub struct CellGroupBirthRequest {
    bytes: Vec<u8>,
    route: GroupRoute,
    creation: ProposalId,
    voters: Vec<InitialMember>,
    learners: Vec<InitialMember>,
}

impl CellGroupBirthRequest {
    pub fn encode(&self) -> &[u8] {
        &self.bytes
    }

    pub fn route(&self) -> &GroupRoute {
        &self.route
    }

    pub fn creation(&self) -> ProposalId {
        self.creation
    }

    pub fn voters(&self) -> &[InitialMember] {
        &self.voters
    }

    pub fn learners(&self) -> &[InitialMember] {
        &self.learners
    }
}

/// Read-back of an immutable birth selected by an observed directory root.
/// This grants neither membership nor permission to initialize a blank disk.
#[derive(Clone)]
pub struct CellGroupBirth {
    pub(super) request: CellGroupBirthRequest,
    pub(super) owner: Arc<()>,
}

impl CellGroupBirth {
    pub fn route(&self) -> &GroupRoute {
        self.request.route()
    }

    pub fn creation(&self) -> ProposalId {
        self.request.creation()
    }

    pub fn voters(&self) -> &[InitialMember] {
        self.request.voters()
    }

    pub fn learners(&self) -> &[InitialMember] {
        self.request.learners()
    }
}

impl RangeDirectory {
    /// Recover the immutable reservation for an already selected range. This
    /// never chooses placement or allocates another group in response to a cell.
    pub async fn materialize_selected_group(
        &self,
        route: &GroupRoute,
    ) -> Result<CellGroupBirth, CatalogError> {
        self.unmanaged_birth()?;
        self.bounded(async {
            self.selected_birth_route(route).await?;
            let request = self.verify_birth_reservation(route).await?;
            self.materialize_group_birth(&request).await
        })
        .await
    }

    pub(crate) fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.cache, &other.cache)
    }

    pub(super) fn unmanaged_birth(&self) -> Result<(), CatalogError> {
        if self.namespace.config.managed_fleet.is_some() {
            return Err(CatalogError::ManagedAdmissionRequired);
        }
        Ok(())
    }

    pub fn prepare_group_birth(
        &self,
        mut spec: CellGroupBirthSpec,
    ) -> Result<CellGroupBirthRequest, CatalogError> {
        self.unmanaged_birth()?;
        if spec.history.cluster != self.namespace.config.cluster
            || spec.history.namespace != self.namespace.config.namespace
        {
            return Err(CatalogError::Conflict("group birth namespace"));
        }
        if spec.voters.len() > 32 || spec.learners.len() > 32 {
            return Err(CatalogError::Limit);
        }
        spec.voters.sort_by_key(|m| m.member);
        spec.learners.sort_by_key(|m| m.member);
        self.namespace.check_members(&spec.voters, &spec.learners)?;
        let value = wire::Birth {
            namespace: self.namespace.root_digest,
            group: spec.history.group.bytes(),
            incarnation: spec.history.incarnation.bytes(),
            lower: wire::Bound::from_bound(spec.range.lower()),
            upper: wire::Bound::from_bound(spec.range.upper()),
            epoch: spec.epoch.get(),
            creation: spec.creation.bytes(),
            voters: spec
                .voters
                .into_iter()
                .map(record::Member::encode)
                .collect(),
            learners: spec
                .learners
                .into_iter()
                .map(record::Member::encode)
                .collect(),
        };
        let request =
            self.decode_group_birth(&record::encode(wire::GROUP_BIRTH, &value, wire::LIMIT)?)?;
        if request.route.range != spec.range {
            return Err(CatalogError::Invalid("noncanonical group birth range"));
        }
        Ok(request)
    }

    pub fn decode_group_birth(&self, bytes: &[u8]) -> Result<CellGroupBirthRequest, CatalogError> {
        self.unmanaged_birth()?;
        let value: wire::Birth = record::decode(wire::GROUP_BIRTH, bytes, wire::LIMIT)?;
        if value.namespace != self.namespace.root_digest {
            return Err(CatalogError::Conflict("group birth namespace"));
        }
        let voters = value
            .voters
            .iter()
            .map(record::Member::decode)
            .collect::<Result<Vec<_>, _>>()?;
        let learners = value
            .learners
            .iter()
            .map(record::Member::decode)
            .collect::<Result<Vec<_>, _>>()?;
        self.namespace.check_members(&voters, &learners)?;
        let route = wire::Route {
            group: value.group,
            incarnation: value.incarnation,
            lower: value.lower,
            upper: value.upper,
            epoch: value.epoch,
            birth: record::digest(bytes),
        }
        .decode(self)?;
        let creation = ProposalId::new(value.creation)
            .map_err(|_| CatalogError::Corrupt("group birth creation"))?;
        Ok(CellGroupBirthRequest {
            bytes: bytes.to_vec(),
            route,
            creation,
            voters,
            learners,
        })
    }

    fn reservation_key(&self, route: &GroupRoute) -> Path {
        // Same permanent global key as the prior catalog: an occupied identity
        // cannot be reused across namespaces, incarnations or semantic domains.
        Path::from(format!(
            "{}groups/{}",
            self.namespace.inner.prefix,
            hex::encode(route.history.group.bytes())
        ))
    }

    pub(super) fn group_birth_key(&self, route: &GroupRoute) -> Path {
        self.key(&format!(
            "births/{}/{}",
            hex::encode(route.history.group.bytes()),
            hex::encode(route.history.incarnation.bytes())
        ))
    }

    /// Permanently reserve before directory publication. An orphan reservation
    /// is retained; a failed topology CAS does not release its identity.
    pub async fn reserve_group_birth(
        &self,
        request: &CellGroupBirthRequest,
    ) -> Result<(), CatalogError> {
        self.reserve_birth_record(
            request,
            &wire::BirthReservation {
                birth: request.encode().to_vec(),
                recovery: None,
            },
        )
        .await
    }

    pub(super) async fn reserve_recovery_birth(
        &self,
        request: &super::CellGroupRecoveryRequest,
    ) -> Result<(), CatalogError> {
        self.reserve_birth_record(
            request.replacement(),
            &wire::BirthReservation {
                birth: request.replacement().encode().to_vec(),
                recovery: Some(request.digest()),
            },
        )
        .await
    }

    async fn reserve_birth_record(
        &self,
        request: &CellGroupBirthRequest,
        reservation: &wire::BirthReservation,
    ) -> Result<(), CatalogError> {
        self.decode_group_birth(request.encode())?;
        let bytes = record::encode(wire::GROUP_BIRTH_RESERVATION, reservation, wire::LIMIT)?;
        self.bounded(async {
            let observed = self
                .namespace
                .inner
                .create_and_observe_bounded(
                    &self.reservation_key(request.route()),
                    &bytes,
                    wire::LIMIT,
                )
                .await?;
            if observed != bytes {
                return Err(CatalogError::Conflict("group birth identity reserved"));
            }
            Ok(())
        })
        .await
    }

    pub(super) async fn verify_birth_reservation(
        &self,
        route: &GroupRoute,
    ) -> Result<CellGroupBirthRequest, CatalogError> {
        self.birth_reservation(route).await.map(|record| record.0)
    }

    pub(super) async fn require_ordinary_birth_reservation(
        &self,
        route: &GroupRoute,
    ) -> Result<CellGroupBirthRequest, CatalogError> {
        let (request, recovery) = self.birth_reservation(route).await?;
        if recovery.is_some() {
            return Err(CatalogError::Fenced);
        }
        Ok(request)
    }

    pub(super) async fn birth_reservation(
        &self,
        route: &GroupRoute,
    ) -> Result<(CellGroupBirthRequest, Option<[u8; 32]>), CatalogError> {
        let bytes = self
            .namespace
            .inner
            .read_versioned_bounded(&self.reservation_key(route), wire::LIMIT)
            .await?
            .ok_or(CatalogError::Corrupt("missing group birth reservation"))?
            .0;
        let reservation: wire::BirthReservation =
            record::decode(wire::GROUP_BIRTH_RESERVATION, &bytes, wire::LIMIT)?;
        if reservation.recovery == Some([0; 32]) {
            return Err(CatalogError::Corrupt("recovery reservation identity"));
        }
        let request = self.decode_group_birth(&reservation.birth)?;
        if request.route() != route {
            return Err(CatalogError::Conflict("group birth reservation binding"));
        }
        Ok((request, reservation.recovery))
    }

    async fn selected_birth_route(&self, route: &GroupRoute) -> Result<(), CatalogError> {
        let (view, _) = self.current().await?;
        if let Some(lock) = &view.head.recovery {
            if lock.source_route(self)? == *route {
                return Err(CatalogError::Fenced);
            }
        }
        self.selected_route_in(&view, route).await
    }

    pub(super) async fn selected_route_in(
        &self,
        view: &DirectoryView,
        route: &GroupRoute,
    ) -> Result<(), CatalogError> {
        let mut next = Some(view.head.tree.clone());
        for _ in 0..32 {
            let reference = next.take().ok_or(CatalogError::Fenced)?;
            let page = self.page(&reference).await?;
            let found = page.route.decode(self)?;
            match route.range.lower().cmp(found.range.lower()) {
                std::cmp::Ordering::Less => next = page.left,
                std::cmp::Ordering::Greater => next = page.right,
                std::cmp::Ordering::Equal => {
                    return if found == *route {
                        Ok(())
                    } else {
                        Err(CatalogError::Fenced)
                    }
                }
            }
        }
        Err(CatalogError::Limit)
    }

    /// Materialize the exact reserved birth only after directory selection.
    /// This is lazy metadata creation, not local genesis or a bootstrap permit.
    pub async fn materialize_group_birth(
        &self,
        request: &CellGroupBirthRequest,
    ) -> Result<CellGroupBirth, CatalogError> {
        self.decode_group_birth(request.encode())?;
        self.bounded(async {
            self.selected_birth_route(request.route()).await?;
            self.verify_birth_reservation(request.route()).await?;
            let observed = self
                .namespace
                .inner
                .create_and_observe_bounded(
                    &self.group_birth_key(request.route()),
                    request.encode(),
                    wire::LIMIT,
                )
                .await?;
            if observed != request.encode() {
                return Err(CatalogError::Conflict("selected group birth"));
            }
            self.selected_birth_route(request.route()).await?;
            Ok(CellGroupBirth {
                request: request.clone(),
                owner: self.namespace.inner.owner.clone(),
            })
        })
        .await
    }

    /// Recheck a birth observation's current directory and immutable metadata.
    /// Future voter admission must additionally verify disk claims and quorum.
    pub async fn require_current_group_birth(
        &self,
        birth: &CellGroupBirth,
    ) -> Result<(), CatalogError> {
        if !Arc::ptr_eq(&birth.owner, &self.namespace.inner.owner) {
            return Err(CatalogError::Conflict("foreign group birth observation"));
        }
        self.bounded(async {
            self.selected_birth_route(birth.route()).await?;
            self.verify_birth_reservation(birth.route()).await?;
            let bytes = self
                .namespace
                .inner
                .read_versioned_bounded(&self.group_birth_key(birth.route()), wire::LIMIT)
                .await?
                .ok_or(CatalogError::Corrupt("missing selected group birth"))?
                .0;
            if bytes != birth.request.encode() {
                return Err(CatalogError::Corrupt("selected group birth changed"));
            }
            Ok(())
        })
        .await
    }
}
