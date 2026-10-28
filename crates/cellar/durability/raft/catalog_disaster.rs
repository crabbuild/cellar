// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Append-only disaster decisions. Catalog records prevent stale admission;
//! a trusted infrastructure authority must additionally fence running processes.

use super::*;
use crate::raft::archive::{ArchiveLimits, ArchiveRecoverySet, ArchiveReference};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};

const INTENT: u8 = 6;
const COMPLETE: u8 = 7;
const MAX_HISTORY: usize = 64;

/// One explicit recovery cut. The replacement keeps the logical cell but uses
/// fresh group, incarnation and physical node identities. No latest-head fallback.
#[derive(Clone, Debug)]
pub struct DisasterRecoveryRequest {
    pub source: GroupScope,
    pub archive: ArchiveReference,
    pub replacement: BirthProposal,
}

/// Exact operation binding supplied to a trusted infrastructure controller.
/// The controller must use its authoritative deployment inventory, including
/// members added after the archive cut, routers and effect workers.
#[derive(Clone, Debug)]
pub struct DisasterFenceRequest {
    request: DisasterRecoveryRequest,
    digest: [u8; 32],
}

impl DisasterFenceRequest {
    pub fn recovery(&self) -> &DisasterRecoveryRequest {
        &self.request
    }

    pub fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

/// Infrastructure trust boundary, analogous to the host's storage/TLS adapters.
/// Implementations must stop execution and external effects for the entire old
/// history, revoke its ability to restart or acquire replacement hosts, and wait
/// for that fence to take effect. An S3 marker, timeout, unreachable host or caller
/// boolean is NOT sufficient. Receipts must be independently verified against the
/// controller and bound to `request.digest()`. Fail closed on partial/unknown
/// outcomes. Fences are permanent; retries must be idempotent and cancellation
/// must never undo a fence. Cellar supplies no permissive default implementation.
pub trait DisasterFenceAuthority: Send + Sync {
    fn fence<'a>(
        &'a self,
        request: &'a DisasterFenceRequest,
    ) -> BoxFuture<'a, anyhow::Result<Vec<u8>>>;

    fn verify<'a>(
        &'a self,
        request: &'a DisasterFenceRequest,
        evidence: &'a [u8],
    ) -> BoxFuture<'a, anyhow::Result<()>>;
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    source_digest: [u8; 32],
    target: wire::Birth,
    archive: ArchiveReference,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Complete {
    intent_digest: [u8; 32],
    evidence: Vec<u8>,
}

#[derive(Clone, Debug)]
pub(super) struct Recovery {
    pub source: GroupBirth,
    pub archive: ArchiveReference,
}

/// Process-local seed authority bound to the exact empty destination engine.
pub(crate) struct DisasterSeedPermit {
    pub(crate) local: ReplicaIdentity,
    pub(crate) birth: GroupBirth,
    owner: Arc<()>,
}

impl DisasterSeedPermit {
    pub(crate) fn authorized(&self, owner: &Arc<()>, local: &ReplicaIdentity) -> bool {
        Arc::ptr_eq(&self.owner, owner) && local == &self.local
    }

    pub(crate) fn validate_set(&self, set: &ArchiveRecoverySet) -> Result<(), CatalogError> {
        let recovery = self.birth.recovery.as_ref().ok_or(CatalogError::Fenced)?;
        if set.scope() != recovery.source.scope()
            || set.birth_digest() != recovery.source.digest()
            || set.reference() != recovery.archive
        {
            return Err(CatalogError::Conflict("disaster archive binding"));
        }
        Ok(())
    }
}

impl GroupBirth {
    pub(crate) fn disaster_source(&self) -> Option<(&GroupBirth, &ArchiveReference)> {
        self.recovery.as_ref().map(|r| (&r.source, &r.archive))
    }
}

impl NamespaceCatalog {
    /// Pin and validate an archive, permanently fence the old deployment, then
    /// publish its successor. This does not start voters or grant leadership.
    /// A failed/ambiguous attempt leaves the old history closed to new admission;
    /// retry the SAME request. Never roll back a partially completed fence.
    /// Multi-cell callers must choose mutually consistent cuts themselves.
    pub async fn recover_from_disaster(
        &self,
        request: DisasterRecoveryRequest,
        authority: &dyn DisasterFenceAuthority,
        host: cellar_ltx::LtxHost,
        limits: ArchiveLimits,
    ) -> Result<GroupBirth, CatalogError> {
        let (source, intent, request) = self.prepare_request(request).await?;
        let bytes = wire::encode(INTENT, &intent, self.inner.limit)?;
        let intent_key = self.disaster_key(&source, "intent");
        // Detect an incompatible operation before performing any infrastructure
        // action. A valid existing intent remains the only resumable request.
        if self
            .inner
            .read(&intent_key)
            .await?
            .is_some_and(|old| old != bytes)
        {
            return Err(CatalogError::Conflict("different disaster recovery intent"));
        }
        let archive = self.cell_archive(&source, host, limits)?;
        let set = archive
            .recovery_set_at(&request.archive)
            .await
            .map_err(CatalogError::Archive)?;
        if set.scope() != &request.source || set.birth_digest() != source.digest() {
            return Err(CatalogError::Conflict("disaster source archive"));
        }
        drop(set);
        let reserved = wire::encode(
            wire::RESERVATION,
            &wire::Reservation {
                root_digest: self.root_digest,
                scope: WireScope::from(&request.replacement.scope),
            },
            self.inner.limit,
        )?;
        if self
            .inner
            .create_and_observe(&self.reservation_key(&request.replacement.scope), &reserved)
            .await?
            != reserved
        {
            return Err(CatalogError::Conflict("disaster group identity reserved"));
        }
        if self.inner.create_and_observe(&intent_key, &bytes).await? != bytes {
            return Err(CatalogError::Conflict("different disaster recovery intent"));
        }
        let fence = DisasterFenceRequest {
            request,
            digest: wire::digest(&bytes),
        };
        let complete_key = self.disaster_key(&source, "complete");
        if let Some(bytes) = self.inner.read(&complete_key).await? {
            let complete = self.decode_complete(&bytes, fence.digest)?;
            authority
                .verify(&fence, &complete.evidence)
                .await
                .map_err(|e| CatalogError::Fence(e.to_string()))?;
        } else {
            let evidence = authority
                .fence(&fence)
                .await
                .map_err(|e| CatalogError::Fence(e.to_string()))?;
            if evidence.is_empty() || evidence.len() > 4096 {
                return Err(CatalogError::Invalid("disaster fence evidence length"));
            }
            authority
                .verify(&fence, &evidence)
                .await
                .map_err(|e| CatalogError::Fence(e.to_string()))?;
            let complete = wire::encode(
                COMPLETE,
                &Complete {
                    intent_digest: fence.digest,
                    evidence,
                },
                self.inner.limit,
            )?;
            let observed = self
                .inner
                .create_and_observe(&complete_key, &complete)
                .await?;
            // Concurrent controller calls may produce different valid receipts.
            // Independently verify the winning receipt before using its result.
            let observed = self.decode_complete(&observed, fence.digest)?;
            authority
                .verify(&fence, &observed.evidence)
                .await
                .map_err(|e| CatalogError::Fence(e.to_string()))?;
        }
        let birth = self.successor(&source, intent).await?;
        self.require_current(&birth).await?;
        Ok(birth)
    }

    /// Read-only canonical plan binding for an external fencing controller.
    /// This does not reserve the decision or authorize any replica.
    pub async fn disaster_fence_request(
        &self,
        request: DisasterRecoveryRequest,
    ) -> Result<DisasterFenceRequest, CatalogError> {
        let (_, intent, request) = self.prepare_request(request).await?;
        let bytes = wire::encode(INTENT, &intent, self.inner.limit)?;
        Ok(DisasterFenceRequest {
            request,
            digest: wire::digest(&bytes),
        })
    }

    async fn prepare_request(
        &self,
        mut request: DisasterRecoveryRequest,
    ) -> Result<(GroupBirth, Intent, DisasterRecoveryRequest), CatalogError> {
        self.require_mode(NamespaceMode::Raft)?;
        if self.config.image_storage != RaftImageStorage::Materialized {
            return Err(CatalogError::Invalid(
                "disaster recovery requires materialized image storage",
            ));
        }
        self.check_scope(&request.source)?;
        request.replacement.voters.sort_by_key(|m| m.member);
        request.replacement.learners.sort_by_key(|m| m.member);
        let source = self.historical_birth(&request.source).await?;
        let target = wire::Birth {
            root_digest: self.root_digest,
            scope: WireScope::from(&request.replacement.scope),
            voters: request
                .replacement
                .voters
                .iter()
                .copied()
                .map(wire::Member::encode)
                .collect(),
            learners: request
                .replacement
                .learners
                .iter()
                .copied()
                .map(wire::Member::encode)
                .collect(),
            empty_digest: wire::digest(&[]),
        };
        self.validate_replacement(&source, &target)?;
        request.archive.position().map_err(CatalogError::Archive)?;
        let intent = Intent {
            source_digest: source.digest(),
            target,
            archive: request.archive.clone(),
        };
        Ok((source, intent, request))
    }

    pub(crate) async fn require_current(&self, birth: &GroupBirth) -> Result<(), CatalogError> {
        self.check_birth(birth)?;
        let current = self
            .birth(&birth.scope.cell)
            .await?
            .ok_or(CatalogError::Fenced)?;
        if current.scope != birth.scope || current.digest != birth.digest {
            return Err(CatalogError::Fenced);
        }
        Ok(())
    }

    fn disaster_key(&self, birth: &GroupBirth, suffix: &str) -> Path {
        Path::from(format!(
            "{}disaster/{}/{suffix}",
            self.cell_key(&birth.scope.cell),
            hex::encode(birth.scope.incarnation.bytes())
        ))
    }

    fn validate_replacement(
        &self,
        source: &GroupBirth,
        target: &wire::Birth,
    ) -> Result<(), CatalogError> {
        let scope = target
            .scope
            .clone()
            .decode()
            .map_err(|_| CatalogError::Corrupt("disaster target scope"))?;
        self.check_scope(&scope)?;
        let voters = target
            .voters
            .iter()
            .map(wire::Member::decode)
            .collect::<Result<Vec<_>, _>>()?;
        let learners = target
            .learners
            .iter()
            .map(wire::Member::decode)
            .collect::<Result<Vec<_>, _>>()?;
        self.check_members(&voters, &learners)?;
        let mut ancestor = Some(source);
        let mut depth = 0;
        while let Some(old) = ancestor {
            depth += 1;
            if depth >= MAX_HISTORY
                || scope.group == old.scope.group
                || scope.incarnation == old.scope.incarnation
                || voters.iter().chain(&learners).any(|new| {
                    old.voters
                        .iter()
                        .chain(&old.learners)
                        .any(|member| member.node == new.node)
                })
            {
                return Err(CatalogError::Invalid(
                    "disaster history/host identity reused or history limit reached",
                ));
            }
            ancestor = old.recovery.as_ref().map(|recovery| &recovery.source);
        }
        if scope.cell != source.scope.cell
            || scope.group == source.scope.group
            || scope.incarnation == source.scope.incarnation
            || target.root_digest != self.root_digest
            || target.empty_digest != wire::digest(&[])
            || voters.iter().chain(&learners).any(|new| {
                source
                    .voters
                    .iter()
                    .chain(&source.learners)
                    .any(|old| old.node == new.node)
            })
        {
            return Err(CatalogError::Invalid(
                "disaster requires fresh history and hosts",
            ));
        }
        Ok(())
    }

    fn decode_complete(&self, bytes: &[u8], expected: [u8; 32]) -> Result<Complete, CatalogError> {
        let complete: Complete = wire::decode(COMPLETE, bytes, self.inner.limit)?;
        if complete.intent_digest != expected
            || complete.evidence.is_empty()
            || complete.evidence.len() > 4096
        {
            return Err(CatalogError::Corrupt("disaster fence receipt"));
        }
        Ok(complete)
    }

    async fn next_recovery(&self, birth: &GroupBirth) -> Result<Option<Intent>, CatalogError> {
        let Some(bytes) = self.inner.read(&self.disaster_key(birth, "intent")).await? else {
            return Ok(None);
        };
        let intent: Intent = wire::decode(INTENT, &bytes, self.inner.limit)?;
        if intent.source_digest != birth.digest() {
            return Err(CatalogError::Corrupt("disaster predecessor"));
        }
        let complete = self
            .inner
            .read(&self.disaster_key(birth, "complete"))
            .await?
            .ok_or(CatalogError::Fenced)?;
        self.decode_complete(&complete, wire::digest(&bytes))?;
        self.validate_replacement(birth, &intent.target)?;
        intent.archive.position().map_err(CatalogError::Archive)?;
        Ok(Some(intent))
    }

    async fn successor(
        &self,
        source: &GroupBirth,
        intent: Intent,
    ) -> Result<GroupBirth, CatalogError> {
        let bytes = wire::encode(wire::BIRTH, &intent.target, self.inner.limit)?;
        let mut next = self.decode_birth(&source.scope.cell, &bytes).await?;
        next.recovery = Some(Arc::new(Recovery {
            source: source.clone(),
            archive: intent.archive,
        }));
        Ok(next)
    }

    // Catalog lookup is nested in every replica/learner startup path. Keep
    // the bounded traversal on the heap instead of multiplying its future
    // state through those enclosing startup futures.
    pub(super) fn follow_recovery<'a>(
        &'a self,
        mut birth: GroupBirth,
    ) -> BoxFuture<'a, Result<GroupBirth, CatalogError>> {
        Box::pin(async move {
            let mut seen = BTreeSet::new();
            for _ in 0..MAX_HISTORY {
                if !seen.insert(birth.scope.incarnation) {
                    return Err(CatalogError::Corrupt("cyclic disaster history"));
                }
                match self.next_recovery(&birth).await? {
                    Some(intent) => birth = self.successor(&birth, intent).await?,
                    None => return Ok(birth),
                }
            }
            Err(CatalogError::Limit)
        })
    }

    async fn historical_birth(&self, scope: &GroupScope) -> Result<GroupBirth, CatalogError> {
        let bytes = self
            .inner
            .read(&self.birth_key(&scope.cell))
            .await?
            .ok_or(CatalogError::Fenced)?;
        let mut birth = self.decode_birth(&scope.cell, &bytes).await?;
        for _ in 0..MAX_HISTORY {
            if birth.scope() == scope {
                return Ok(birth);
            }
            let next = self
                .next_recovery(&birth)
                .await?
                .ok_or(CatalogError::Fenced)?;
            birth = self.successor(&birth, next).await?;
        }
        Err(CatalogError::Limit)
    }

    pub(crate) async fn disaster_seed_permit(
        &self,
        birth: &GroupBirth,
        log: &ReplicaLog,
    ) -> Result<Option<DisasterSeedPermit>, CatalogError> {
        self.require_current(birth).await?;
        if birth.recovery.is_none() {
            return Ok(None);
        }
        let member = birth
            .voters
            .iter()
            .chain(&birth.learners)
            .find(|m| m.node == log.limits().node)
            .ok_or(CatalogError::NotInitialMember)?;
        self.require_node_storage(log).await?;
        match log.group(&birth.scope) {
            Ok(_) => return Ok(None),
            Err(LogError::NotFound) => (),
            Err(error) => return Err(CatalogError::Log(error)),
        }
        if self
            .read_claim(&self.claim_key(birth, member.member), birth)
            .await?
            .is_some()
        {
            return Err(CatalogError::LostStableState);
        }
        let local = log.limits();
        Ok(Some(DisasterSeedPermit {
            local: ReplicaIdentity {
                scope: birth.scope.clone(),
                member: member.member,
                node: local.node,
                disk: local.disk,
                process: local.process,
            },
            birth: birth.clone(),
            owner: log.owner(),
        }))
    }
}
