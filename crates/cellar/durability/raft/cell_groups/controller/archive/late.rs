// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Demand-driven attachment of existing archives to already hosted groups.
//! No initialization, membership admission, replica recovery or quorum fallback.
use super::*;
use crate::raft::budget::Permit;
use std::collections::BTreeSet;
use std::ops::Bound::{Excluded, Unbounded};

type Outcome = std::result::Result<CellGroupArchiveRegistration, CellGroupControllerError>;

pub(in crate::raft::cell_groups::controller) struct Completed {
    group: GroupHistory,
    result: Outcome,
    _memory: Permit,
}

#[derive(Default)]
pub(in crate::raft::cell_groups::controller) struct Manager {
    pending: BTreeSet<GroupHistory>,
    after: Option<GroupHistory>,
    job: Option<BoxFuture<'static, Completed>>,
    active_group: Option<GroupHistory>,
}

impl Manager {
    pub fn request(
        &mut self,
        group: GroupHistory,
        archives: &super::Manager,
        shared: &Shared,
    ) -> std::result::Result<(), CellGroupControllerError> {
        if archives.contains(&group) || !shared.groups.contains(&group)? {
            self.release(group, shared);
        } else if self.pending.len() < shared.limits.max_groups {
            self.pending.insert(group);
        } else {
            self.release(group, shared);
        }
        Ok(())
    }

    pub fn running(&self) -> bool {
        self.job.is_some()
    }

    pub async fn next(&mut self) -> Completed {
        self.job.as_mut().expect("entered archive discovery").await
    }

    pub fn launch(
        &mut self,
        archives: &super::Manager,
        shared: &Arc<Shared>,
    ) -> std::result::Result<(), CellGroupControllerError> {
        if self.running() || shared.closing.load(Ordering::Acquire) {
            return Ok(());
        }
        let group = self
            .after
            .and_then(|after| {
                self.pending
                    .range((Excluded(after), Unbounded))
                    .next()
                    .copied()
            })
            .or_else(|| self.pending.first().copied());
        let Some(group) = group else {
            return Ok(());
        };
        self.after = Some(group);
        if archives.contains(&group) || !shared.groups.contains(&group)? {
            self.pending.remove(&group);
            self.release(group, shared);
            return Ok(());
        }
        let config = shared
            .provisioning
            .lock()
            .map_err(|_| CellGroupControllerError::Closed)?
            .clone();
        let Some(config) = config.filter(|config| config.archives.is_some()) else {
            self.pending.remove(&group);
            self.release(group, shared);
            return Ok(());
        };
        let Some(memory) = config.preparations.acquire(config.bytes) else {
            return Ok(());
        };
        let runtime = shared.runtime.clone();
        self.active_group = Some(group);
        self.job = Some(Box::pin(async move {
            let result = async {
                let _parallel = config
                    .parallel
                    .acquire()
                    .await
                    .map_err(|_| CellGroupControllerError::Closed)?;
                let directory = config
                    .settings
                    .namespaces
                    .iter()
                    .find(|namespace| {
                        let scope = namespace.directory.namespace_config();
                        scope.cluster == group.cluster && scope.namespace == group.namespace
                    })
                    .ok_or(CellGroupControllerError::Invalid(
                        "archive discovery namespace",
                    ))?
                    .directory
                    .clone();
                let birth = directory
                    .materialize_selected_history(group)
                    .await
                    .map_err(|error| {
                        CellGroupControllerError::Archive(Arc::new(Error::Binding(
                            CellGroupArchiveBindingError::Catalog(error),
                        )))
                    })?;
                let local = runtime
                    .log()
                    .cell_group(&group)
                    .map_err(RuntimeError::from)?;
                let marker = runtime
                    .log()
                    .domain()
                    .blocking(move || local.group_manifest())
                    .map_err(|_| CellGroupControllerError::Closed)?
                    .await
                    .map_err(|_| CellGroupControllerError::Closed)?
                    .map_err(RuntimeError::from)?
                    .ok_or(RuntimeError::NotFound)?;
                if marker.route() != birth.route()
                    || !marker.is_active()
                    || marker.source_seal().is_some()
                {
                    return Err(RuntimeError::NotReady.into());
                }
                // Open only the selected immutable identity and existing registry
                // counters. Missing setup fails without minting replacement state.
                config
                    .archives
                    .as_ref()
                    .expect("checked archive resources")
                    .prepare(&directory, &birth, &config.settings, &runtime, None)
                    .await
            }
            .await;
            Completed {
                group,
                result,
                _memory: memory,
            }
        }));
        Ok(())
    }

    pub fn complete(
        &mut self,
        completed: Completed,
        archives: &mut super::Manager,
        shared: &Shared,
    ) {
        self.job = None;
        self.active_group = None;
        self.pending.remove(&completed.group);
        match completed.result {
            Ok(registration) => {
                if shared.closing.load(Ordering::Acquire)
                    || archives.contains(&completed.group)
                    || !shared.groups.contains(&completed.group).unwrap_or(false)
                {
                    registration.store.close();
                } else {
                    // Keep a clone until the internal handoff has either taken
                    // custody or refused. A refused private verifier is closed.
                    let store = registration.store.clone();
                    if let Err(error) = archives.provision(registration, shared) {
                        store.close();
                        tracing::debug!(group=?completed.group, %error, "late archive attachment refused");
                    }
                }
            }
            Err(error) => {
                tracing::debug!(group=?completed.group, %error, "archive identity not ready for local attachment")
            }
        }
        self.release(completed.group, shared);
    }

    fn release(&self, group: GroupHistory, shared: &Shared) {
        shared
            .archive_discovery
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&group);
    }

    pub(in crate::raft::cell_groups::controller) fn retire(
        &mut self,
        group: GroupHistory,
        shared: &Shared,
    ) -> bool {
        self.pending.remove(&group);
        if self.active_group == Some(group) {
            return false;
        }
        self.release(group, shared);
        true
    }

    pub async fn join(&mut self, shared: &Shared) {
        if let Some(job) = self.job.take() {
            let completed = job.await;
            if let Ok(registration) = completed.result {
                registration.store.close();
            }
            self.release(completed.group, shared);
        }
        self.pending.clear();
        shared
            .archive_discovery
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
    }
}
