// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Invocation-local read reuse owns actual, evictable archive verification.

use super::{job, journal, settlement};
use crate::raft::cell_groups::{
    archive::{CachedCoverage, WeakCoverage},
    CellDispatchError, CellGroupArchiveCoverage, CellGroupArchiveTailPool, CellGroupCheckpointPool,
};
use cellar_logic::cell_group::{CellVersion, GroupRoute};
use futures_util::future::BoxFuture;
use std::sync::Arc;

/// Constructed only from a settled S3 receipt. The exact source version and
/// real verification pins cannot be supplied by a caller or publication flag.
pub(crate) struct SourceReadProof {
    version: CellVersion,
    route: GroupRoute,
    coverage: WeakCoverage,
    _idle: Arc<CachedCoverage>,
    _journal: Arc<journal::Journal>,
    authority: Arc<job::Authority>,
}

impl SourceReadProof {
    pub(super) fn new(
        version: CellVersion,
        coverage: CellGroupArchiveCoverage,
        journal: Arc<journal::Journal>,
        authority: Arc<job::Authority>,
        tails: &CellGroupArchiveTailPool,
        checkpoints: &CellGroupCheckpointPool,
    ) -> Arc<Self> {
        let route = coverage.marker().route().clone();
        let observed = coverage.downgrade();
        let idle = CachedCoverage::new(coverage);
        // A dependent S3 operation may need this same one-slot replay bank.
        // Its normal admission evicts idle custody before reserving a workspace.
        // The retained verification remains fully charged until then.
        tails.remember_idle(&idle);
        checkpoints.remember_archive_idle(&idle);
        Arc::new(Self {
            version,
            route,
            coverage: observed,
            _idle: idle,
            _journal: journal,
            authority,
        })
    }

    pub(crate) fn version(&self) -> &CellVersion {
        &self.version
    }

    pub(crate) fn route(&self) -> &GroupRoute {
        &self.route
    }

    /// Capture custody synchronously, while the SQL owner checks its baseline.
    /// A missing/evicted verification requires the ordinary source checkpoint.
    /// This future releases its active pins before dependent target I/O begins.
    pub(crate) fn confirm(
        self: &Arc<Self>,
        domain: crate::asyncrt::DomainToken,
        deadline: u64,
    ) -> Option<BoxFuture<'static, Result<(), CellDispatchError>>> {
        // A controller may hand the same admitted snapshot between idle cache
        // and active work. Follow actual custody, never cache-object identity.
        let coverage = self.coverage.retain()?;
        let source = self.clone();
        Some(Box::pin(async move {
            let result = settlement::require_current(&source.authority, &domain, deadline).await;
            drop(coverage);
            drop(source);
            result
        }))
    }
}
