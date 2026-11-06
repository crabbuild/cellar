// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A catalog grant admits one activation proposal. Only the real Raft owner can
//! produce an applied acknowledgement and release the topology recovery lock.
use super::*;
use crate::raft::{
    cache::GroupLog,
    cell_groups::GroupManifest,
    completion::RuntimeError,
    fleet_catalog::{FleetHostAuthorization, FleetRecoveryAttachRequest},
    fleet_stop::BootBinding,
    storage::ReplicaLog,
};
use cellar_logic::consensus::{EngineInstanceId, LogIndex, LogPosition, Term};
use std::sync::atomic::Ordering;

pub struct CellGroupRecoveryActivationPermit {
    route: GroupRoute,
    request: [u8; 32],
    operation: ProposalId,
    group: GroupLog<GroupHistory>,
    engine: EngineInstanceId,
    managed_boot: Option<BootBinding>,
    publication: [u8; 32],
    manifest: [u8; 32],
    deadline: u64,
}

impl CellGroupRecoveryActivationPermit {
    pub(crate) fn deadline(&self) -> u64 {
        self.deadline
    }

    pub fn history(&self) -> GroupHistory {
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
        if let Some(boot) = &self.managed_boot {
            if log
                .lifecycle_guard()?
                .is_none_or(|guard| !guard.admits_group_start(boot))
            {
                return Err(RuntimeError::StaleCompletion);
            }
        }
        drop(group.read()?);
        Ok(())
    }

    pub(crate) fn matches(&self, marker: &GroupManifest) -> Result<bool, RuntimeError> {
        if marker.route() != &self.route
            || marker.recovery_request() != Some(self.request)
            || marker
                .recovery_commitment()
                .map_err(|e| RuntimeError::Invalid(e.to_string()))?
                != Some(self.manifest)
        {
            return Err(RuntimeError::StaleCompletion);
        }
        match marker.recovery_activation() {
            Some((publication, manifest, _))
                if publication == self.publication && manifest == self.manifest =>
            {
                Ok(true)
            }
            Some(_) => Err(RuntimeError::StaleCompletion),
            None => Ok(false),
        }
    }

    pub(crate) fn entry(&self) -> Result<Vec<u8>, RuntimeError> {
        use crate::application::consensus::group_protocol::{
            CellGroupEntry, GroupCommand, GroupEntryBinding, GroupEntryLimits,
        };
        let route = &self.route;
        CellGroupEntry {
            binding: GroupEntryBinding {
                group: route.history,
                route_epoch: route.epoch,
                birth_digest: route.birth_digest,
                schema_digest: route.schema_digest,
                proposal: self.operation,
            },
            command: GroupCommand::Activate {
                transition: TransitionId::new(self.operation.bytes())
                    .map_err(|e| RuntimeError::Invalid(e.to_string()))?,
                publication: self.publication,
                manifest: self.manifest,
            },
        }
        .encode(route, GroupEntryLimits::default())
        .map_err(|e| RuntimeError::Invalid(e.to_string()))
    }

    pub(crate) fn acknowledgement(
        &self,
        group: &GroupLog<GroupHistory>,
        selected: &GroupManifest,
    ) -> Result<CellGroupRecoveryActivationAck, RuntimeError> {
        let cache = group.read()?;
        let marker = cache
            .group_manifest
            .as_ref()
            .ok_or(RuntimeError::NotReady)?;
        if marker != selected || !self.matches(marker)? {
            return Err(RuntimeError::NotReady);
        }
        let (_, _, position) = marker.recovery_activation().ok_or(RuntimeError::NotReady)?;
        if cache.hard_state.commit < position.index.get() {
            return Err(RuntimeError::NotReady);
        }
        Ok(CellGroupRecoveryActivationAck {
            group: group.clone(),
            request: self.request,
            publication: self.publication,
            manifest: self.manifest,
            position,
        })
    }
}

/// An observation of activation committed and selected by an actual local Raft
/// owner. This is neither a transferable serving grant nor an archive witness.
pub struct CellGroupRecoveryActivationAck {
    group: GroupLog<GroupHistory>,
    request: [u8; 32],
    publication: [u8; 32],
    manifest: [u8; 32],
    position: LogPosition,
}

impl CellGroupRecoveryActivationAck {
    pub fn position(&self) -> LogPosition {
        self.position
    }

    fn check(
        &self,
        route: &GroupRoute,
        request: [u8; 32],
        publication: [u8; 32],
        manifest: [u8; 32],
    ) -> Result<(), CatalogError> {
        if self.request != request
            || self.publication != publication
            || self.manifest != manifest
            || self.group.closed.load(Ordering::Acquire)
            || self.group.identity().scope != route.history
        {
            return Err(CatalogError::Fenced);
        }
        let cache = self.group.read().map_err(CatalogError::Log)?;
        let root = cache
            .group_manifest
            .as_ref()
            .ok_or(CatalogError::LostStableState)?;
        if root.route() != route
            || root.recovery_request() != Some(self.request)
            || root.recovery_activation() != Some((self.publication, self.manifest, self.position))
            || cache.hard_state.commit < self.position.index.get()
        {
            return Err(CatalogError::Fenced);
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Activated {
    request: [u8; 32],
    publication: [u8; 32],
    manifest: [u8; 32],
    index: u64,
    term: u64,
}

impl Activated {
    fn position_for(
        &self,
        request: [u8; 32],
        publication: [u8; 32],
        manifest: [u8; 32],
    ) -> Result<LogPosition, CatalogError> {
        let position = LogPosition {
            index: LogIndex::new(self.index),
            term: Term::new(self.term),
        };
        if self.request != request
            || self.publication != publication
            || self.manifest != manifest
            || self.index <= 1
            || self.term <= 1
            || position.validate().is_err()
        {
            return Err(CatalogError::Corrupt("recovery activation binding"));
        }
        Ok(position)
    }
}

impl RangeDirectory {
    pub(super) async fn require_recovery_activation(
        &self,
        request: &CellGroupRecoveryRequest,
        publication: &DirectoryPublication,
        manifest: [u8; 32],
    ) -> Result<(), CatalogError> {
        let bytes = self
            .namespace
            .inner
            .read_versioned_bounded(&self.recovery_key(request, "activation"), wire::LIMIT)
            .await?
            .ok_or(CatalogError::Corrupt("missing recovery activation"))?
            .0;
        let activated: Activated =
            record::decode(wire::GROUP_RECOVERY_ACTIVATED, &bytes, wire::LIMIT)?;
        activated.position_for(request.digest(), publication.journal_digest(), manifest)?;
        Ok(())
    }

    /// Recheck publication, physical member identity and the permanent external
    /// fence before admitting the exact activation to the actual Raft owner.
    pub fn prepare_group_recovery_activation<'a>(
        &'a self,
        request: &'a CellGroupRecoveryRequest,
        log: &'a ReplicaLog,
        authority: &'a dyn DisasterFenceAuthority<CellGroupDisasterFenceRequest>,
    ) -> futures_util::future::BoxFuture<'a, Result<CellGroupRecoveryActivationPermit, CatalogError>>
    {
        Box::pin(async move {
            let request = self.decode_group_recovery(request.encode())?;
            self.bounded(async {
                self.verify_recovery_fence(&request, authority).await?;
                let group = self.recovered_member(&request, log).await?;
                let (publication, binding, _) = self.published_recovery(&request).await?;
                let deadline = log
                    .domain()
                    .mono_ms()
                    .map_err(|_| CatalogError::Unconfirmed)?
                    .checked_add(self.limits.operation_timeout_ms)
                    .ok_or(CatalogError::Limit)?;
                Ok(CellGroupRecoveryActivationPermit {
                    route: request.replacement().route().clone(),
                    request: request.digest(),
                    operation: request.operation(),
                    group,
                    managed_boot: None,
                    engine: log.storage_instance().map_err(CatalogError::Log)?,
                    publication: publication.journal_digest(),
                    manifest: binding.commitment,
                    deadline,
                })
            })
            .await
        })
    }

    pub(crate) fn prepare_managed_recovery_activation<'a>(
        &'a self,
        request: &'a FleetRecoveryAttachRequest,
        history: GroupHistory,
        log: &'a ReplicaLog,
        host: &'a FleetHostAuthorization,
        deadline: u64,
    ) -> futures_util::future::BoxFuture<'a, Result<CellGroupRecoveryActivationPermit, CatalogError>>
    {
        Box::pin(self.bounded(async move {
            let (group, authority) = self
                .verify_published_managed_recovery_member(request, history, log, host, deadline)
                .await?;
            Ok(CellGroupRecoveryActivationPermit {
                route: authority.birth.route().clone(),
                request: request.plan_digest,
                operation: request.operation,
                group,
                engine: log.storage_instance().map_err(CatalogError::Log)?,
                managed_boot: Some(authority.current_boot),
                publication: authority.publication.journal_digest(),
                manifest: authority.seeds.genesis_commitment(),
                deadline,
            })
        }))
    }

    /// Record an applied activation under the exact selected range. The
    /// recovery lock stays selected until the complete Fleet settlement.
    pub(crate) async fn record_managed_recovery_activation(
        &self,
        permit: &CellGroupRecoveryActivationPermit,
        acknowledgement: &CellGroupRecoveryActivationAck,
        log: &ReplicaLog,
    ) -> Result<LogPosition, CatalogError> {
        if permit.managed_boot.is_none() {
            return Err(CatalogError::Fenced);
        }
        self.bounded(async {
            permit
                .check(log, &acknowledgement.group)
                .map_err(CatalogError::Runtime)?;
            acknowledgement.check(
                &permit.route,
                permit.request,
                permit.publication,
                permit.manifest,
            )?;
            let position = acknowledgement.position();
            if position.index.get() <= 1 || position.term.get() <= 1 {
                return Err(CatalogError::Fenced);
            }
            let bytes = record::encode(
                wire::GROUP_RECOVERY_ACTIVATED,
                &Activated {
                    request: permit.request,
                    publication: permit.publication,
                    manifest: permit.manifest,
                    index: position.index.get(),
                    term: position.term.get(),
                },
                wire::LIMIT,
            )?;
            let key = self.managed_seed_key(&permit.route, "activation");
            if self
                .namespace
                .inner
                .create_and_observe_bounded(&key, &bytes, wire::LIMIT)
                .await?
                != bytes
            {
                return Err(CatalogError::Conflict(
                    "managed recovery activation receipt",
                ));
            }
            Ok(position)
        })
        .await
    }

    pub(crate) async fn observe_managed_recovery_activation(
        &self,
        route: &GroupRoute,
        request: [u8; 32],
        publication: [u8; 32],
        manifest: [u8; 32],
    ) -> Result<Option<LogPosition>, CatalogError> {
        self.bounded(async {
            let key = self.managed_seed_key(route, "activation");
            let Some((bytes, _)) = self
                .namespace
                .inner
                .read_versioned_bounded(&key, wire::LIMIT)
                .await?
            else {
                return Ok(None);
            };
            let observed: Activated =
                record::decode(wire::GROUP_RECOVERY_ACTIVATED, &bytes, wire::LIMIT)?;
            observed
                .position_for(request, publication, manifest)
                .map(Some)
        })
        .await
    }

    /// Release topology only after committed/applied activation. Unknown S3
    /// responses retain the original request and are reconciled by read-back.
    pub fn complete_group_recovery<'a>(
        &'a self,
        request: &'a CellGroupRecoveryRequest,
        acknowledgement: &'a CellGroupRecoveryActivationAck,
        authority: &'a dyn DisasterFenceAuthority<CellGroupDisasterFenceRequest>,
    ) -> futures_util::future::BoxFuture<'a, Result<DirectoryPublication, CatalogError>> {
        Box::pin(async move {
            let request = self.decode_group_recovery(request.encode())?;
            self.bounded(async {
                self.verify_recovery_fence(&request, authority).await?;
                let (publication, binding, _) = self.published_recovery(&request).await?;
                acknowledgement.check(
                    request.replacement().route(),
                    request.digest(),
                    publication.journal_digest(),
                    binding.commitment,
                )?;
                let position = acknowledgement.position();
                let complete = record::encode(
                    wire::GROUP_RECOVERY_ACTIVATED,
                    &Activated {
                        request: request.digest(),
                        publication: publication.journal_digest(),
                        manifest: binding.commitment,
                        index: position.index.get(),
                        term: position.term.get(),
                    },
                    wire::LIMIT,
                )?;
                if self
                    .namespace
                    .inner
                    .create_and_observe_bounded(
                        &self.recovery_key(&request, "activation"),
                        &complete,
                        wire::LIMIT,
                    )
                    .await?
                    != complete
                {
                    return Err(CatalogError::Conflict("recovery activation receipt"));
                }
                let (mut view, version) = self.current().await?;
                self.selected_route_in(&view, request.replacement().route())
                    .await?;
                if view
                    .head
                    .recovery
                    .as_deref()
                    .is_none_or(|lock| lock.request != request.digest())
                {
                    self.require_recovery_activation(&request, &publication, binding.commitment)
                        .await?;
                    return Ok(publication);
                }
                if view.head.recovery.as_deref().is_none_or(|lock| {
                    lock.request != request.digest()
                        || lock.phase
                            != (RecoveryPhase::Published {
                                publication: publication.journal_digest(),
                                manifest: binding.commitment,
                            })
                }) {
                    return Err(CatalogError::Fenced);
                }
                acknowledgement.check(
                    request.replacement().route(),
                    request.digest(),
                    publication.journal_digest(),
                    binding.commitment,
                )?;
                view.head.recovery = None;
                let bytes = record::encode(wire::HEAD, &view.head, wire::LIMIT)?;
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
                let (after, _) = self
                    .current()
                    .await
                    .map_err(|_| CatalogError::Unconfirmed)?;
                self.require_recovery_activation(&request, &publication, binding.commitment)
                    .await?;
                if after.head.recovery.is_some() {
                    return Err(CatalogError::Unconfirmed);
                }
                self.confirmed(&after.head, request.mutation())
                    .await?
                    .ok_or(CatalogError::Unconfirmed)
            })
            .await
        })
    }
}
