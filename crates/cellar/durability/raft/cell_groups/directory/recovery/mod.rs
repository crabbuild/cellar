// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exact, irreversible disaster fencing before fresh-genesis installation.
//! The returned data proof grants no bootstrap, voting or serving permission.
mod request;
use super::*;
use crate::raft::{
    catalog::DisasterFenceAuthority,
    cell_groups::{
        CellGroupArchiveBindingError, CellGroupArchiveCheckpointReceiver, CellGroupArchiveCoverage,
        CellGroupArchiveError, CellGroupArchiveExecution, CellGroupArchivePool,
        CellGroupArchiveTailPool,
    },
};
pub use request::{CellGroupDisasterFenceRequest, CellGroupRecoveryRequest};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub enum CellGroupRecoveryError {
    Catalog(CatalogError),
    Archive(CellGroupArchiveError),
}

impl std::fmt::Display for CellGroupRecoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cell group recovery: {self:?}")
    }
}

impl std::error::Error for CellGroupRecoveryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Catalog(error) => error,
            Self::Archive(error) => error,
        })
    }
}

impl From<CatalogError> for CellGroupRecoveryError {
    fn from(error: CatalogError) -> Self {
        Self::Catalog(error)
    }
}

impl From<CellGroupArchiveError> for CellGroupRecoveryError {
    fn from(error: CellGroupArchiveError) -> Self {
        Self::Archive(error)
    }
}

impl From<CellGroupArchiveBindingError> for CellGroupRecoveryError {
    fn from(error: CellGroupArchiveBindingError) -> Self {
        match error {
            CellGroupArchiveBindingError::Catalog(error) => Self::Catalog(error),
            CellGroupArchiveBindingError::Archive(error) => Self::Archive(error),
        }
    }
}

/// A topology-head CAS closes old startup and all competing topology mutations
/// atomically. It never stands in for stopping a running deployment.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(in crate::raft::catalog::range_directory) struct RecoveryLock {
    request: [u8; 32],
    base: [u8; 32],
    source: wire::Route,
}

impl RecoveryLock {
    pub(super) fn check(&self, directory: &RangeDirectory) -> Result<(), CatalogError> {
        if self.request == [0; 32] || self.base == [0; 32] {
            return Err(CatalogError::Corrupt("group recovery lock"));
        }
        self.source_route(directory).map(|_| ())
    }

    pub(in crate::raft::catalog::range_directory) fn source_route(
        &self,
        directory: &RangeDirectory,
    ) -> Result<GroupRoute, CatalogError> {
        self.source.decode(directory)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Complete {
    request: [u8; 32],
    evidence: Vec<u8>,
}

/// A complete selected archive and independently verified permanent fence.
/// Retain its dedicated remote hold until recovery and entered work finish.
/// This deliberately cannot be supplied to ordinary bootstrap or live apply.
pub struct FencedCellGroupRecovery {
    request: CellGroupRecoveryRequest,
    coverage: CellGroupArchiveCoverage,
}

impl FencedCellGroupRecovery {
    pub fn request(&self) -> &CellGroupRecoveryRequest {
        &self.request
    }

    pub fn coverage(&self) -> &CellGroupArchiveCoverage {
        &self.coverage
    }
}

impl RangeDirectory {
    fn recovery_key(&self, request: &CellGroupRecoveryRequest, suffix: &str) -> Path {
        self.key(&format!(
            "recovery/{}/{suffix}",
            hex::encode(request.digest())
        ))
    }

    async fn recovery_source(
        &self,
        request: &CellGroupRecoveryRequest,
    ) -> Result<CellGroupBirth, CatalogError> {
        let (view, _) = self.current().await?;
        self.check_recovery_view(&view, request)?;
        if Index::new(self)
            .lookup(
                view.head.operations,
                &hex::encode(request.operation().bytes()),
            )
            .await?
            .is_some()
        {
            return Err(CatalogError::Conflict(
                "group recovery operation already published",
            ));
        }
        self.selected_route_in(&view, request.source().route())
            .await?;
        self.verify_birth_reservation(request.source().route())
            .await?;
        let bytes = self
            .namespace
            .inner
            .read_versioned_bounded(&self.group_birth_key(request.source().route()), wire::LIMIT)
            .await?
            .ok_or(CatalogError::Corrupt("missing recovery source birth"))?
            .0;
        if bytes != request.source().encode() {
            return Err(CatalogError::Corrupt("recovery source birth binding"));
        }
        Ok(CellGroupBirth {
            request: request.source().clone(),
            owner: self.namespace.inner.owner.clone(),
        })
    }

    fn check_recovery_view(
        &self,
        view: &DirectoryView,
        request: &CellGroupRecoveryRequest,
    ) -> Result<(), CatalogError> {
        match &view.head.recovery {
            Some(lock)
                if lock.request == request.digest()
                    && Some(lock.base) == request.mutation().request.expected
                    && lock.source_route(self)? == *request.source().route() =>
            {
                Ok(())
            }
            Some(_) => Err(CatalogError::Conflict(
                "different group recovery owns topology",
            )),
            None if Some(view.digest) == request.mutation().request.expected => Ok(()),
            None => Err(CatalogError::Conflict("stale group recovery topology")),
        }
    }

    async fn lock_recovery(&self, request: &CellGroupRecoveryRequest) -> Result<(), CatalogError> {
        self.bounded(async {
            let (mut view, version) = self.current().await?;
            self.check_recovery_view(&view, request)?;
            if view.head.recovery.is_some() {
                return Ok(());
            }
            self.selected_route_in(&view, request.source().route())
                .await?;
            view.head.recovery = Some(RecoveryLock {
                request: request.digest(),
                base: view.digest,
                source: wire::Route::encode(request.source().route(), self)?,
            });
            let bytes = record::encode(wire::HEAD, &view.head, wire::LIMIT)?;
            // A lost response can follow a committed CAS. Observe the exact
            // lock; never issue another request or rebase onto a newer head.
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
            self.check_recovery_view(&after, request)?;
            if after.head.recovery.is_none() {
                return Err(CatalogError::Unconfirmed);
            }
            Ok(())
        })
        .await
    }

    /// Verify the exact held closure, serialize recovery with fleet import and
    /// topology, then permanently fence every possible old writer. Persist
    /// `request.encode()` before calling. A failed/unknown outcome is resumable
    /// ONLY with those bytes; there is no abort or unfence operation.
    ///
    /// The authority must use external deployment/launch inventory, including
    /// later joins, removed hosts, routers and effects; independently verify
    /// evidence bound to the request digest and exclude every replacement node
    /// from the old lineage. A marker or caller assertion cannot satisfy it.
    /// This static unmanaged boundary does not publish a replacement or seed a
    /// Raft engine. Fresh genesis, restored histories and quorum remain required.
    pub async fn fence_group_recovery(
        &self,
        domain: &impl CellGroupArchiveExecution,
        archives: &CellGroupArchivePool,
        tails: &CellGroupArchiveTailPool,
        request: &CellGroupRecoveryRequest,
        receiver: CellGroupArchiveCheckpointReceiver,
        authority: &dyn DisasterFenceAuthority<CellGroupDisasterFenceRequest>,
    ) -> Result<FencedCellGroupRecovery, CellGroupRecoveryError> {
        domain
            .run(async {
                let request = self.decode_group_recovery(request.encode())?;
                let birth = self.bounded(self.recovery_source(&request)).await?;
                let registry = self.open_recovery_archive(domain, archives, &birth).await?;
                // No inventory claim, topology lock or infrastructure action until
                // every cell/base/control dependency has been physically verified.
                let coverage = registry
                    .verify_hold(domain, request.hold(), tails, receiver)
                    .await?;
                coverage
                    .recovery()
                    .check_disaster_target(request.replacement())?;
                self.namespace.claim_disaster_namespace().await?;
                self.reserve_group_birth(request.replacement()).await?;
                self.bounded(async {
                    let observed = self
                        .namespace
                        .inner
                        .create_and_observe_bounded(
                            &self.recovery_key(&request, "intent"),
                            request.encode(),
                            wire::LIMIT,
                        )
                        .await?;
                    if observed != request.encode() {
                        return Err(CatalogError::Conflict("group recovery intent"));
                    }
                    Ok(())
                })
                .await?;
                self.lock_recovery(&request).await?;
                let fence = CellGroupDisasterFenceRequest::new(request);
                let complete_key = self.recovery_key(fence.recovery(), "fence");
                let existing = self
                    .bounded(async {
                        Ok(self
                            .namespace
                            .inner
                            .read_versioned_bounded(&complete_key, wire::LIMIT)
                            .await?
                            .map(|v| v.0))
                    })
                    .await?;
                let bytes = if let Some(bytes) = existing {
                    bytes
                } else {
                    let evidence = authority
                        .fence(&fence)
                        .await
                        .map_err(|e| CatalogError::Fence(e.to_string()))?;
                    check_evidence(&evidence)?;
                    authority
                        .verify(&fence, &evidence)
                        .await
                        .map_err(|e| CatalogError::Fence(e.to_string()))?;
                    let bytes = record::encode(
                        wire::GROUP_FENCE,
                        &Complete {
                            request: fence.digest(),
                            evidence,
                        },
                        wire::LIMIT,
                    )?;
                    self.bounded(self.namespace.inner.create_and_observe_bounded(
                        &complete_key,
                        &bytes,
                        wire::LIMIT,
                    ))
                    .await?
                };
                // Concurrent controllers may return different valid evidence. Verify
                // the winning durable receipt independently, including on reopen.
                let complete: Complete = record::decode(wire::GROUP_FENCE, &bytes, wire::LIMIT)?;
                if complete.request != fence.digest() {
                    return Err(CatalogError::Corrupt("group fence request binding").into());
                }
                check_evidence(&complete.evidence)?;
                authority
                    .verify(&fence, &complete.evidence)
                    .await
                    .map_err(|e| CatalogError::Fence(e.to_string()))?;
                self.bounded(self.recovery_source(fence.recovery())).await?;
                Ok(FencedCellGroupRecovery {
                    request: fence.into_recovery(),
                    coverage,
                })
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }
}

fn check_evidence(evidence: &[u8]) -> Result<(), CatalogError> {
    if evidence.is_empty() || evidence.len() > 4096 {
        return Err(CatalogError::Invalid("group fence evidence length"));
    }
    Ok(())
}
