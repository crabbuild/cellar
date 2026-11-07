// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Joined, bounded physical collection after permanent engine refusal.
use super::*;
use crate::raft::cell_groups::{
    source_reclaim::Permit, DirectoryMutation, ManifestPageStore, RangeDirectory,
};
use cellar_logic::cell_group::GroupHistory;
use tokio::sync::oneshot;

pub(super) struct Request {
    permit: Permit,
    config: Arc<provision::Config>,
    preparation: Arc<crate::raft::budget::Permit>,
    parallel: Arc<tokio::sync::OwnedSemaphorePermit>,
    pub(super) memory: Arc<crate::raft::budget::Permit>,
    pub(super) reply: oneshot::Sender<super::calls::Delivery<bool, CellGroupControllerError>>,
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
    let source = request.permit.source.clone();
    let history = source.scope().map_err(RuntimeError::from)?;
    let previous = log
        .source_reclamation(history)
        .await
        .map_err(RuntimeError::from)?;
    shared
        .groups
        .require_reclaimable(&history, previous.is_some())?;
    if previous
        .as_ref()
        .is_some_and(|ack| ack.record().source != source)
    {
        return Err(RuntimeError::StaleCompletion.into());
    }
    if previous.as_ref().is_some_and(|ack| ack.record().complete) {
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
        let source = source.clone();
        let memory = request.preparation.clone();
        let parallel = request.parallel.clone();
        log.domain()
            .blocking(move || {
                let _memory = memory;
                let _parallel = parallel;
                super::super::source_reclaim::prepare_physical(&applier, &source)
            })
            .map_err(|_| CellGroupControllerError::Closed)?
            .await
            .map_err(|_| CellGroupControllerError::Closed)?
            .map_err(|error| CellGroupControllerError::Apply(Arc::new(error)))?;
    }
    // The queue owns this operation until physical work returns, even if its
    // caller times out. A durable incomplete receipt is sufficient for recovery.
    let ack = log
        .reclaim_source(request.permit.clone())
        .await
        .map_err(RuntimeError::from)?;
    let config = request.config.clone();
    let memory = request.preparation.clone();
    let parallel = request.parallel.clone();
    let done = log
        .domain()
        .blocking(move || {
            let _memory = memory;
            let _parallel = parallel;
            if let Some(applier) = applier {
                return super::super::source_reclaim::reclaim_physical(&applier, &source);
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
    log.finish_source_reclamation(ack)
        .await
        .map_err(RuntimeError::from)?;
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
