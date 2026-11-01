// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A catalog grant admits one activation proposal. Only the real Raft owner can
//! produce an applied acknowledgement and release the topology recovery lock.
use super::*;
use crate::raft::{
    cache::GroupLog, cell_groups::GroupManifest, completion::RuntimeError, storage::ReplicaLog,
};
use cellar_logic::consensus::{EngineInstanceId, LogIndex, LogPosition, Term};
use std::sync::atomic::Ordering;

pub struct CellGroupRecoveryActivationPermit {
    request: Arc<CellGroupRecoveryRequest>,
    group: GroupLog<GroupHistory>,
    engine: EngineInstanceId,
    publication: [u8; 32],
    manifest: [u8; 32],
    deadline: u64,
}

impl CellGroupRecoveryActivationPermit {
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
        drop(group.read()?);
        Ok(())
    }

    pub(crate) fn matches(&self, marker: &GroupManifest) -> Result<bool, RuntimeError> {
        if marker.route() != self.request.replacement().route()
            || marker.recovery_request() != Some(self.request.digest())
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
        let route = self.request.replacement().route();
        CellGroupEntry {
            binding: GroupEntryBinding {
                group: route.history,
                route_epoch: route.epoch,
                birth_digest: route.birth_digest,
                schema_digest: route.schema_digest,
                proposal: self.request.operation(),
            },
            command: GroupCommand::Activate {
                transition: TransitionId::new(self.request.operation().bytes())
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
            request: self.request.digest(),
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
        request: &CellGroupRecoveryRequest,
        publication: [u8; 32],
        manifest: [u8; 32],
    ) -> Result<(), CatalogError> {
        if self.request != request.digest()
            || self.publication != publication
            || self.manifest != manifest
            || self.group.closed.load(Ordering::Acquire)
            || self.group.identity().scope != request.replacement().route().history
        {
            return Err(CatalogError::Fenced);
        }
        let cache = self.group.read().map_err(CatalogError::Log)?;
        let root = cache
            .group_manifest
            .as_ref()
            .ok_or(CatalogError::LostStableState)?;
        if root.recovery_request() != Some(self.request)
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
        if activated.request != request.digest()
            || activated.publication != publication.journal_digest()
            || activated.manifest != manifest
            || activated.index <= 1
            || activated.term <= 1
            || (LogPosition {
                index: LogIndex::new(activated.index),
                term: Term::new(activated.term),
            })
            .validate()
            .is_err()
        {
            return Err(CatalogError::Corrupt("recovery activation binding"));
        }
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
            let request = Arc::new(self.decode_group_recovery(request.encode())?);
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
                    request,
                    group,
                    engine: log.storage_instance().map_err(CatalogError::Log)?,
                    publication: publication.journal_digest(),
                    manifest: binding.commitment,
                    deadline,
                })
            })
            .await
        })
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
                    &request,
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
                if view.head.recovery.is_none() {
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
                    &request,
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
