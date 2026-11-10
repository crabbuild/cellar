// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Joined, bounded physical collection after permanent engine refusal.
use super::*;
use crate::raft::cell_groups::{
    source_reclaim::Permit, DirectoryMutation, ManifestPageStore, RangeDirectory,
};
use cellar_logic::cell_group::GroupHistory;
use tokio::sync::oneshot;

#[derive(Clone)]
enum Reclamation {
    Source(Permit),
    Member(super::super::member_reclaim::Permit),
}

enum Ack {
    Source(crate::raft::storage::SourceReclaimAck),
    Member(crate::raft::storage::MemberReclaimAck),
}

impl Reclamation {
    fn history(&self) -> Result<GroupHistory, crate::raft::storage::LogError> {
        match self {
            Self::Source(p) => p.source.scope(),
            Self::Member(p) => p.member.scope(),
        }
    }

    async fn status(
        &self,
        log: &crate::raft::storage::ReplicaLog,
    ) -> Result<Option<Ack>, crate::raft::storage::LogError> {
        let history = self.history()?;
        match self {
            Self::Source(_) => log
                .source_reclamation(history)
                .await
                .map(|ack| ack.map(Ack::Source)),
            Self::Member(_) => log
                .member_reclamation(history)
                .await
                .map(|ack| ack.map(Ack::Member)),
        }
    }

    fn matches(&self, ack: &Ack) -> bool {
        match (self, ack) {
            (Self::Source(p), Ack::Source(a)) => p.source == a.record().source,
            (Self::Member(p), Ack::Member(a)) => p.member == a.record().member,
            _ => false,
        }
    }

    fn replaces(&self, previous: &Ack) -> Result<bool, crate::raft::storage::LogError> {
        match (self, previous) {
            (Self::Member(permit), Ack::Member(previous)) => Ok(previous.record().permits_mapping(
                permit.member.scope()?,
                permit.member.birth,
                permit.member.member,
            )),
            _ => Ok(false),
        }
    }

    async fn reclaim(
        &self,
        log: &crate::raft::storage::ReplicaLog,
    ) -> Result<Ack, crate::raft::storage::LogError> {
        match self {
            Self::Source(p) => log.reclaim_source(p.clone()).await.map(Ack::Source),
            Self::Member(p) => log.reclaim_member(p.clone()).await.map(Ack::Member),
        }
    }

    fn prepare_physical(&self, applier: &CellGroupApplier) -> Result<(), ExecutionError> {
        match self {
            Self::Source(p) => super::super::source_reclaim::prepare_physical(applier, &p.source),
            Self::Member(p) => super::super::member_reclaim::prepare_physical(applier, &p.member),
        }
    }

    fn reclaim_physical(&self, applier: &CellGroupApplier) -> Result<bool, ExecutionError> {
        match self {
            Self::Source(p) => super::super::source_reclaim::reclaim_physical(applier, &p.source),
            Self::Member(p) => super::super::member_reclaim::reclaim_physical(applier, &p.member),
        }
    }
}

impl Ack {
    fn complete(&self) -> bool {
        match self {
            Self::Source(a) => a.record().complete,
            Self::Member(a) => a.record().complete,
        }
    }

    async fn finish(
        self,
        log: &crate::raft::storage::ReplicaLog,
    ) -> Result<(), crate::raft::storage::LogError> {
        match self {
            Self::Source(a) => log.finish_source_reclamation(a).await.map(|_| ()),
            Self::Member(a) => log.finish_member_reclamation(a).await.map(|_| ()),
        }
    }
}

pub(super) struct Request {
    permit: Reclamation,
    config: Arc<provision::Config>,
    preparation: Arc<crate::raft::budget::Permit>,
    parallel: Arc<tokio::sync::OwnedSemaphorePermit>,
    pub(super) memory: Arc<crate::raft::budget::Permit>,
    pub(super) reply: oneshot::Sender<super::calls::Delivery<bool, CellGroupControllerError>>,
}

impl Request {
    /// Physical completion also releases the finite SQL/seed retirement index.
    /// Re-read the selected engine receipt so abandoned waiters and retries use
    /// the same proof. Incomplete, mismatched or still-entered owners retain it.
    pub(super) async fn release_retirement(
        &self,
        shared: &Shared,
        sql: &CellGroupSqlPool,
    ) -> Result<(), CellGroupControllerError> {
        let log = shared.runtime.log();
        let history = self.permit.history().map_err(RuntimeError::from)?;
        let proof = self
            .permit
            .status(log)
            .await
            .map_err(RuntimeError::from)?
            .ok_or(RuntimeError::NotReady)?;
        if !self.permit.matches(&proof) || !proof.complete() {
            return Err(RuntimeError::NotReady.into());
        }
        match proof {
            Ack::Source(proof) => {
                sql.release_reclaimed_source(&proof, log)
                    .map_err(|error| CellGroupControllerError::Apply(Arc::new(error)))?;
                shared
                    .archive_seeds
                    .release_reclaimed_source(&proof, log)
                    .map_err(|error| CellGroupControllerError::Archive(Arc::new(error)))?;
            }
            Ack::Member(proof) => {
                sql.resume_reclaimed_member(&proof, log)
                    .map_err(|error| CellGroupControllerError::Apply(Arc::new(error)))?;
                shared
                    .archive_seeds
                    .resume_reclaimed_member(&proof, log)
                    .map_err(|error| CellGroupControllerError::Archive(Arc::new(error)))?;
            }
        }
        shared
            .archives
            .lock()
            .map_err(|_| CellGroupControllerError::Closed)?
            .remove(&history);
        Ok(())
    }
}

impl CellGroupController {
    pub(crate) async fn reclaim_retired_source(
        &self,
        directory: &RangeDirectory,
        plan: &DirectoryMutation,
        history: GroupHistory,
    ) -> Result<bool, CellGroupControllerError> {
        let shared = &self.handle.0;
        if shared.closing.load(Ordering::Acquire) {
            return Err(CellGroupControllerError::Closed);
        }
        let memory = Arc::new(
            shared
                .commands
                .acquire(32 * 1024)
                .ok_or(RuntimeError::Overloaded)?,
        );
        let config = shared
            .provisioning
            .lock()
            .map_err(|_| CellGroupControllerError::Closed)?
            .clone()
            .ok_or(CellGroupControllerError::Invalid(
                "reclamation provisioning",
            ))?;
        let preparation = Arc::new(
            config
                .preparations
                .acquire(config.bytes)
                .ok_or(RuntimeError::Overloaded)?,
        );
        let parallel = Arc::new(
            config
                .parallel
                .clone()
                .try_acquire_owned()
                .map_err(|_| RuntimeError::Overloaded)?,
        );
        let permit = directory
            .prepare_managed_source_reclamation(plan, history, shared.runtime.log())
            .await
            .map_err(|error| CellGroupControllerError::Catalog(Arc::new(error)))?;
        self.submit_reclamation(
            Reclamation::Source(permit),
            config,
            preparation,
            parallel,
            memory,
        )
        .await
    }

    pub(crate) async fn reclaim_removed_member(
        &self,
        permit: super::super::member_reclaim::Permit,
    ) -> Result<(bool, Arc<tokio::sync::OwnedSemaphorePermit>), CellGroupControllerError> {
        let shared = &self.handle.0;
        if shared.closing.load(Ordering::Acquire) {
            return Err(CellGroupControllerError::Closed);
        }
        let history = permit.member.scope().map_err(RuntimeError::from)?;
        if shared
            .runtime
            .log()
            .member_reclamation(history)
            .await
            .map_err(RuntimeError::from)?
            .is_none_or(|ack| ack.record().member != permit.member)
        {
            self.quiesce_removed_member(permit.member.clone()).await?;
        }
        let memory = Arc::new(
            shared
                .commands
                .acquire(32 * 1024)
                .ok_or(RuntimeError::Overloaded)?,
        );
        let config = shared
            .provisioning
            .lock()
            .map_err(|_| CellGroupControllerError::Closed)?
            .clone()
            .ok_or(CellGroupControllerError::Invalid(
                "reclamation provisioning",
            ))?;
        let preparation = Arc::new(
            config
                .preparations
                .acquire(config.bytes)
                .ok_or(RuntimeError::Overloaded)?,
        );
        // Same-group page paths cannot be reused until bounded physical GC is
        // complete. Take the existing node preparation lane exclusively.
        let parallel = Arc::new(
            config
                .parallel
                .clone()
                .try_acquire_many_owned(config.settings.max_parallel_preparations as u32)
                .map_err(|_| RuntimeError::Overloaded)?,
        );
        let done = self
            .submit_reclamation(
                Reclamation::Member(permit),
                config,
                preparation,
                parallel.clone(),
                memory,
            )
            .await?;
        // The SDK holds this same lane through the local-proof/root-CAS
        // handoff; neither new provisioning nor another GC can change occupancy.
        Ok((done, parallel))
    }

    async fn submit_reclamation(
        &self,
        permit: Reclamation,
        config: Arc<provision::Config>,
        preparation: Arc<crate::raft::budget::Permit>,
        parallel: Arc<tokio::sync::OwnedSemaphorePermit>,
        memory: Arc<crate::raft::budget::Permit>,
    ) -> Result<bool, CellGroupControllerError> {
        let shared = &self.handle.0;
        let (reply, response) = oneshot::channel();
        shared
            .requests
            .try_send(requests::Request::Reclamation(Box::new(Request {
                permit,
                config,
                preparation,
                parallel,
                memory,
                reply,
            })))
            .map_err(|error| match error {
                tokio::sync::mpsc::error::TrySendError::Full(_) => RuntimeError::Overloaded,
                tokio::sync::mpsc::error::TrySendError::Closed(_) => RuntimeError::Closed,
            })?;
        response
            .await
            .map_err(|_| CellGroupControllerError::Closed)?
            .result
            .map_err(|error| (*error).clone())
    }
}

pub(super) async fn drive(
    shared: &Arc<Shared>,
    request: &Request,
) -> Result<bool, CellGroupControllerError> {
    let log = shared.runtime.log();
    let history = request.permit.history().map_err(RuntimeError::from)?;
    let previous = request
        .permit
        .status(log)
        .await
        .map_err(RuntimeError::from)?;
    let previous = match previous {
        Some(previous) if request.permit.matches(&previous) => Some(previous),
        Some(previous)
            if request
                .permit
                .replaces(&previous)
                .map_err(RuntimeError::from)? =>
        {
            None
        }
        Some(_) => return Err(RuntimeError::StaleCompletion.into()),
        None => None,
    };
    // A completed older fence is not a receipt for the freshly removed member.
    // Join and collect the current owner before replacing its high-water record.
    shared
        .groups
        .require_reclaimable(&history, previous.is_some())?;
    if previous.as_ref().is_some_and(|ack| ack.complete()) {
        shared.groups.finish_reclamation(&history)?;
        shared
            .retired_sources
            .lock()
            .map_err(|_| CellGroupControllerError::Closed)?
            .remove(&history);
        return Ok(true);
    }
    let existing = shared
        .retired_sources
        .lock()
        .map_err(|_| CellGroupControllerError::Closed)?
        .get(&history)
        .cloned();
    let applier = match existing {
        Some(applier) => Some(applier),
        None => request.config.take_retired_applier(history)?,
    };
    if let Some(applier) = &applier {
        shared
            .retired_sources
            .lock()
            .map_err(|_| CellGroupControllerError::Closed)?
            .insert(history, applier.clone());
    }
    if previous.is_none() {
        let applier = applier.clone().ok_or(RuntimeError::NotReady)?;
        let permit = request.permit.clone();
        let memory = request.preparation.clone();
        let parallel = request.parallel.clone();
        log.domain()
            .blocking(move || {
                let _memory = memory;
                let _parallel = parallel;
                permit.prepare_physical(&applier)
            })
            .map_err(|_| CellGroupControllerError::Closed)?
            .await
            .map_err(|_| CellGroupControllerError::Closed)?
            .map_err(|error| CellGroupControllerError::Apply(Arc::new(error)))?;
    }
    // The queue owns this operation until physical work returns, even if its
    // caller times out. A durable incomplete receipt is sufficient for recovery.
    let ack = request
        .permit
        .reclaim(log)
        .await
        .map_err(RuntimeError::from)?;
    let permit = request.permit.clone();
    let config = request.config.clone();
    let memory = request.preparation.clone();
    let parallel = request.parallel.clone();
    let done = log
        .domain()
        .blocking(move || {
            let _memory = memory;
            let _parallel = parallel;
            if let Some(applier) = applier {
                return permit.reclaim_physical(&applier);
            }
            // A retained restart already adopted every remaining engine-selected
            // image. Reopen only this tombstoned history's private page inventory.
            let settings = &config.settings;
            let mut pages = ManifestPageStore::open(
                &ManifestPageStore::group_pages_path(&settings.directory, history),
                settings.filesystem.clone(),
                settings.max_pages,
                settings.max_page_bytes,
                settings.max_retained_roots,
            )
            .map_err(crate::raft::execution::storage)?;
            pages
                .reconcile_retained(&[])
                .map_err(crate::raft::execution::storage)?;
            pages
                .reclaim(256)
                .map_err(crate::raft::execution::storage)?;
            let images = settings
                .namespaces
                .iter()
                .find(|profile| {
                    let ns = profile.directory.namespace_config();
                    ns.cluster == history.cluster && ns.namespace == history.namespace
                })
                .ok_or(ExecutionError::Invalid("reclamation namespace images"))?;
            super::super::source_reclaim::collect_images(&images.images)?;
            Ok(pages.usage().pages == 0)
        })
        .map_err(|_| CellGroupControllerError::Closed)?
        .await
        .map_err(|_| CellGroupControllerError::Closed)?
        .map_err(|error| CellGroupControllerError::Apply(Arc::new(error)))?;
    if !done {
        return Ok(false);
    }
    ack.finish(log).await.map_err(RuntimeError::from)?;
    shared.groups.finish_reclamation(&history)?;
    shared
        .retired_sources
        .lock()
        .map_err(|_| CellGroupControllerError::Closed)?
        .remove(&history);
    Ok(true)
}

impl CellGroupController {
    pub(crate) async fn source_reclamation_proof(
        &self,
        history: GroupHistory,
    ) -> Result<crate::raft::storage::SourceReclaimAck, CellGroupControllerError> {
        let shared = &self.handle.0;
        let ack = shared
            .runtime
            .log()
            .source_reclamation(history)
            .await
            .map_err(RuntimeError::from)?
            .ok_or(RuntimeError::NotReady)?;
        if !ack.record().complete
            || !shared.groups.reclaimed(&history)?
            || shared
                .retired_sources
                .lock()
                .map_err(|_| CellGroupControllerError::Closed)?
                .contains_key(&history)
        {
            return Err(RuntimeError::NotReady.into());
        }
        ack.check(shared.runtime.log())
            .map_err(RuntimeError::from)?;
        Ok(ack)
    }
}

impl CellGroupController {
    pub(crate) async fn member_reclamation_proof(
        &self,
        history: GroupHistory,
    ) -> Result<crate::raft::storage::MemberReclaimAck, CellGroupControllerError> {
        let shared = &self.handle.0;
        let ack = shared
            .runtime
            .log()
            .member_reclamation(history)
            .await
            .map_err(RuntimeError::from)?
            .ok_or(RuntimeError::NotReady)?;
        if !ack.record().complete
            || !shared.groups.reclaimed(&history)?
            || shared
                .retired_sources
                .lock()
                .map_err(|_| CellGroupControllerError::Closed)?
                .contains_key(&history)
        {
            return Err(RuntimeError::NotReady.into());
        }
        ack.check(shared.runtime.log())
            .map_err(RuntimeError::from)?;
        Ok(ack)
    }
}
