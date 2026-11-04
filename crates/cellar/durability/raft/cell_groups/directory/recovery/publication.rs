// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! The same complete genesis must be durably seeded everywhere before the
//! topology CAS. Keep its lock until replicated activation has been confirmed.
use super::*;
use crate::raft::cell_groups::GroupManifest;

pub(in crate::raft::catalog::range_directory) struct RecoveryPublication {
    request: Arc<CellGroupRecoveryRequest>,
    manifest: [u8; 32],
}

impl RecoveryPublication {
    pub(in crate::raft::catalog::range_directory) fn check(
        &self,
        directory: &RangeDirectory,
        view: &DirectoryView,
        mutation: &DirectoryMutation,
    ) -> Result<(), CatalogError> {
        if mutation.encode() != self.request.mutation().encode() {
            return Err(CatalogError::Fenced);
        }
        directory.check_recovery_view(view, &self.request)?;
        if view.head.recovery.is_none() {
            return Err(CatalogError::Fenced);
        }
        Ok(())
    }

    pub(in crate::raft::catalog::range_directory) fn check_child(
        &self,
        route: &GroupRoute,
    ) -> Result<(), CatalogError> {
        if route != self.request.replacement().route() {
            return Err(CatalogError::Fenced);
        }
        Ok(())
    }

    pub(in crate::raft::catalog::range_directory) fn published_lock(
        &self,
        publication: [u8; 32],
    ) -> Result<Box<RecoveryLock>, CatalogError> {
        Ok(Box::new(RecoveryLock {
            request: self.request.digest(),
            base: self
                .request
                .mutation()
                .request
                .expected
                .ok_or(CatalogError::Invalid("recovery base"))?,
            source: self
                .request
                .mutation()
                .request
                .parents
                .first()
                .ok_or(CatalogError::Invalid("recovery source"))?
                .clone(),
            phase: RecoveryPhase::Published {
                publication,
                manifest: self.manifest,
            },
        }))
    }
}

impl RangeDirectory {
    pub(super) async fn recovery_binding(
        &self,
        request: &CellGroupRecoveryRequest,
    ) -> Result<(seed::Binding, [u8; 32]), CatalogError> {
        let (_, origin) = self
            .birth_reservation(request.replacement().route())
            .await?;
        if origin != Some(request.digest()) {
            return Err(CatalogError::Fenced);
        }
        let bytes = self
            .namespace
            .inner
            .read_versioned_bounded(
                &self.recovery_binding_key(request.replacement().route()),
                wire::LIMIT,
            )
            .await?
            .ok_or(CatalogError::Conflict("recovery genesis not bound"))?
            .0;
        let binding: seed::Binding =
            record::decode(wire::GROUP_RECOVERY_BINDING, &bytes, wire::LIMIT)?;
        let genesis: &GroupManifest = &binding.genesis;
        if binding.request != request.encode()
            || !genesis.is_recovery_genesis()
            || genesis.route() != request.replacement().route()
            || genesis.recovery_request() != Some(request.digest())
            || genesis
                .recovery_commitment()
                .map_err(|_| CatalogError::Corrupt("recovery genesis"))?
                != Some(binding.commitment)
        {
            return Err(CatalogError::Corrupt("recovery genesis binding"));
        }
        Ok((binding, record::digest(&bytes)))
    }

    pub(super) async fn recovery_seed_receipt(
        &self,
        request: &CellGroupRecoveryRequest,
        selected: &crate::raft::catalog::InitialMember,
        binding: [u8; 32],
    ) -> Result<seed::MemberSeed, CatalogError> {
        let bytes = self
            .namespace
            .inner
            .read_versioned_bounded(
                &self.recovery_seed_key(request, selected.member.get(), "complete"),
                wire::LIMIT,
            )
            .await?
            .ok_or(CatalogError::Conflict("recovery member not seeded"))?
            .0;
        let receipt: seed::MemberSeed =
            record::decode(wire::GROUP_RECOVERY_SEED_COMPLETE, &bytes, wire::LIMIT)?;
        receipt.check(selected, binding)?;
        let intent = record::encode(wire::GROUP_RECOVERY_SEED_INTENT, &receipt, wire::LIMIT)?;
        let observed = self
            .namespace
            .inner
            .read_versioned_bounded(
                &self.recovery_seed_key(request, selected.member.get(), "intent"),
                wire::LIMIT,
            )
            .await?
            .ok_or(CatalogError::Corrupt("missing recovery seed intent"))?
            .0;
        if observed != intent {
            return Err(CatalogError::Corrupt("recovery seed intent binding"));
        }
        self.namespace
            .verify_node_storage_identity(selected.node, receipt.disk()?, receipt.engine()?)
            .await?;
        Ok(receipt)
    }

    /// Publish only after all initial members have synced the same complete
    /// pending genesis. This metadata result never grants voting or serving.
    /// Retry the original request bytes after an unknown CAS outcome.
    pub fn publish_group_recovery<'a>(
        &'a self,
        request: &'a CellGroupRecoveryRequest,
        authority: &'a dyn DisasterFenceAuthority<CellGroupDisasterFenceRequest>,
    ) -> futures_util::future::BoxFuture<'a, Result<DirectoryPublication, CatalogError>> {
        Box::pin(async move {
            let request = Arc::new(self.decode_group_recovery(request.encode())?);
            self.bounded(async {
                if let Some(publication) = self.publication(request.mutation()).await? {
                    return Ok(publication);
                }
                let (view, _) = self.current().await?;
                self.check_recovery_view(&view, &request)?;
                if view.head.recovery.is_none() {
                    return Err(CatalogError::Fenced);
                }
                self.recovery_source(&request).await?;
                self.verify_recovery_fence(&request, authority).await?;
                let (binding, digest) = self.recovery_binding(&request).await?;
                let mut disks = BTreeSet::new();
                let mut engines = BTreeSet::new();
                for member in request
                    .replacement()
                    .voters()
                    .iter()
                    .chain(request.replacement().learners())
                {
                    let receipt = self.recovery_seed_receipt(&request, member, digest).await?;
                    if !disks.insert(receipt.disk()?) || !engines.insert(receipt.engine()?) {
                        return Err(CatalogError::Conflict("duplicate recovery storage"));
                    }
                }
                let permit = RecoveryPublication {
                    request: request.clone(),
                    manifest: binding.commitment,
                };
                self.publish_inner(request.mutation(), Some(&permit)).await
            })
            .await
        })
    }

    pub(super) async fn published_recovery(
        &self,
        request: &CellGroupRecoveryRequest,
    ) -> Result<(DirectoryPublication, seed::Binding, [u8; 32]), CatalogError> {
        let (view, _) = self.current().await?;
        let publication = self
            .confirmed(&view.head, request.mutation())
            .await?
            .ok_or(CatalogError::Fenced)?;
        self.selected_route_in(&view, request.replacement().route())
            .await?;
        let (binding, digest) = self.recovery_binding(request).await?;
        match view.head.recovery.as_deref() {
            Some(lock)
                if lock.request == request.digest()
                    && Some(lock.base) == request.mutation().request.expected
                    && lock.source_route(self)? == *request.source().route()
                    && lock.phase
                        == (RecoveryPhase::Published {
                            publication: publication.journal_digest(),
                            manifest: binding.commitment,
                        }) => {}
            // An unrelated recovery can own the topology lock after this
            // group completed. Its exact activation receipt and selected route
            // must still verify; pending groups cannot use this lane.
            Some(_) | None => {
                self.require_recovery_activation(request, &publication, binding.commitment)
                    .await?
            }
        }
        Ok((publication, binding, digest))
    }

    /// Read-only status for this exact published intent. False requires its
    /// durable activation receipt; another group's lock is never completion.
    pub async fn group_recovery_pending(
        &self,
        request: &CellGroupRecoveryRequest,
    ) -> Result<bool, CatalogError> {
        self.bounded(async {
            let (publication, binding, _) = self.published_recovery(request).await?;
            let (view, _) = self.current().await?;
            self.selected_route_in(&view, request.replacement().route())
                .await?;
            let pending = view
                .head
                .recovery
                .as_deref()
                .is_some_and(|lock| lock.request == request.digest());
            if !pending {
                self.require_recovery_activation(request, &publication, binding.commitment)
                    .await?;
            }
            Ok(pending)
        })
        .await
    }
}
