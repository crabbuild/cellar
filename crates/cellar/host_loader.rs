// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Adapted for Cellar from Celld: crates/celld/js.rs.

//! Host ownership, admission and asynchronous startup of dynamically loaded workers.

use crate::worker::Worker;
use crate::worker_config::WorkerConfig;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

pub(crate) mod calls;

// Default to workerd's module-byte ceiling. A fleet operator may admit larger
// compiled runtimes; loaded guest code cannot change this host policy.
#[allow(dead_code)]
pub(crate) const DEFAULT_MAX_DYNAMIC_WORKER_CODE_SIZE: usize = 64 * 1024 * 1024;
pub(crate) const MAX_DYNAMIC_WORKER_ENV_SIZE: usize = 1024 * 1024;

#[allow(dead_code)]
pub(crate) fn dynamic_worker_code_limit() -> usize {
    static LIMIT: OnceLock<usize> = OnceLock::new();
    *LIMIT.get_or_init(|| {
        crate::env_vars::positive_or(
            "CELLAR_MAX_DYNAMIC_WORKER_CODE_BYTES",
            DEFAULT_MAX_DYNAMIC_WORKER_CODE_SIZE,
        )
        .expect("validated CELLAR_MAX_DYNAMIC_WORKER_CODE_BYTES")
    })
}

/// The largest `props` value one loaded entrypoint or Durable Object class can
/// carry, as structured-clone bytes. Workerd does not impose this bound, but
/// cellar holds the value on a host job for the whole call. The harness shares
/// the caller's isolate and cannot prevent that unbounded host allocation, so
/// every op that accepts call props must enforce the limit.
pub(crate) const MAX_DYNAMIC_WORKER_PROPS_SIZE: usize = 1024 * 1024;

#[derive(Clone)]
pub(crate) enum LoaderState {
    Loading,
    Ready(Arc<crate::pool::Slot>),
    Failed(Arc<str>),
}

#[derive(Clone, Eq, PartialEq)]
pub(crate) struct LoaderPrincipal {
    pub(crate) script: Arc<str>,
    pub(crate) generation: crate::generation::GenerationId,
}

#[derive(Clone)]
pub(crate) struct LoaderOwner {
    pub(crate) service: Weak<dyn LoaderService>,
    pub(crate) id: u64,
    pub(crate) principal: LoaderPrincipal,
}

impl LoaderOwner {
    pub(crate) fn fresh(script: &str, generation: crate::generation::GenerationId) -> Self {
        let service = crate::asyncrt::services().loader();
        Self {
            service: Arc::downgrade(&service),
            id: service.next_owner(),
            principal: LoaderPrincipal {
                script: Arc::from(script),
                generation,
            },
        }
    }
}

#[doc(hidden)]
pub struct LoaderEntry {
    pub(crate) owner: LoaderOwner,
    pub(crate) state: tokio::sync::watch::Receiver<LoaderState>,
}

impl PartialEq for LoaderOwner {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.principal == other.principal
            && Weak::ptr_eq(&self.service, &other.service)
    }
}
impl Eq for LoaderOwner {}

type LoadedWorkers = HashMap<u64, LoaderEntry>;
// Erase the recursive worker/turn graph at the service boundary, so importing
// Context does not require applications to raise their compiler recursion limit.
pub(crate) trait LoaderService: Send + Sync {
    fn bind_domain(&self, domain: crate::asyncrt::DomainToken);
    fn accepts_work(&self) -> bool;
    fn close(&self);
    fn registry(&self) -> Arc<Mutex<LoadedWorkers>>;
    fn next_id(&self) -> u64;
    fn next_owner(&self) -> u64;
    fn state(&self, id: u64) -> Option<tokio::sync::watch::Receiver<LoaderState>>;
    fn take(&self, id: u64) -> Option<tokio::sync::watch::Receiver<LoaderState>>;
    fn defer_drop(&self, state: Box<dyn Send>);
}
pub(crate) struct LoaderRegistry {
    registry: Arc<Mutex<LoadedWorkers>>,
    next_id: AtomicU64,
    next_owner: AtomicU64,
    closed: AtomicBool,
    domain: OnceLock<crate::asyncrt::DomainToken>,
}
impl Default for LoaderRegistry {
    fn default() -> Self {
        Self {
            registry: Arc::new(Mutex::new(HashMap::new())),
            next_id: AtomicU64::new(1),
            next_owner: AtomicU64::new(1),
            closed: AtomicBool::new(false),
            domain: OnceLock::new(),
        }
    }
}
impl LoaderService for LoaderRegistry {
    fn registry(&self) -> Arc<Mutex<LoadedWorkers>> {
        self.registry.clone()
    }
    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
    fn next_owner(&self) -> u64 {
        self.next_owner.fetch_add(1, Ordering::Relaxed)
    }
    fn bind_domain(&self, domain: crate::asyncrt::DomainToken) {
        if let Err(candidate) = self.domain.set(domain) {
            assert!(self.domain.get().unwrap().same_owner(&candidate));
        }
    }
    fn accepts_work(&self) -> bool {
        !self.closed.load(Ordering::Acquire)
            && self
                .domain
                .get()
                .is_none_or(|domain| domain.mono_ms().is_ok())
    }
    fn close(&self) {
        let entries = {
            let mut registry = self.registry.lock().unwrap();
            self.closed.store(true, Ordering::Release);
            std::mem::take(&mut *registry)
        };
        drop(entries);
    }
    fn state(&self, id: u64) -> Option<tokio::sync::watch::Receiver<LoaderState>> {
        self.registry
            .lock()
            .unwrap()
            .get(&id)
            .map(|entry| entry.state.clone())
    }
    fn take(&self, id: u64) -> Option<tokio::sync::watch::Receiver<LoaderState>> {
        let entry = self.registry.lock().unwrap().remove(&id);
        entry.map(|entry| entry.state)
    }
    fn defer_drop(&self, state: Box<dyn Send>) {
        if let Some(domain) = self.domain.get() {
            // Refused work drops its resources under the captured owner.
            let _ = domain.spawn_detached("loader cleanup", async move { drop(state) });
        } else {
            drop(state);
        }
    }
}

#[doc(hidden)]
pub fn loader_registry() -> Arc<Mutex<LoadedWorkers>> {
    crate::asyncrt::services().loader().registry()
}

/// Remove references for this captured owner, then destroy children outside
/// the registry lock.
pub(crate) fn take_loader_owner(
    owner: &LoaderOwner,
) -> Vec<tokio::sync::watch::Receiver<LoaderState>> {
    let Some(service) = owner.service.upgrade() else {
        return Vec::new();
    };
    let registry_handle = service.registry();
    let mut registry = registry_handle.lock().unwrap();
    let ids: Vec<u64> = registry
        .iter()
        .filter_map(|(id, entry)| (&entry.owner == owner).then_some(*id))
        .collect();
    ids.into_iter()
        .filter_map(|id| registry.remove(&id).map(|entry| entry.state))
        .collect()
}

// Bound live Dynamic Workers so a runaway loader cannot exhaust workers.
// Keep admission policy inside the registry: a caller-selected bound would let
// different entry points disagree about how much host capacity remains.
pub(crate) const MAX_LOADED_WORKERS: usize = 256;
// Reserve only the final host slot. A fixed fraction needlessly reduces
// single-script capacity without giving every competing principal a share.
pub(crate) const MAX_LOADED_WORKERS_PER_PRINCIPAL: usize = MAX_LOADED_WORKERS - 1;

/// Check both limits and install the live state under one registry lock. A
/// separate check and insert lets concurrent isolates exceed either limit.
pub(crate) fn admit_loaded_worker(
    owner: LoaderOwner,
    state: tokio::sync::watch::Receiver<LoaderState>,
) -> Result<u64, String> {
    let service = owner
        .service
        .upgrade()
        .ok_or_else(|| "worker loader: host has retired".to_string())?;
    let registry_handle = service.registry();
    let mut registry = registry_handle.lock().unwrap();
    if !service.accepts_work() {
        return Err("worker loader: execution domain is closed".into());
    }
    let principal_limit = MAX_LOADED_WORKERS_PER_PRINCIPAL;
    let principal_count = registry
        .values()
        .filter(|entry| entry.owner.principal == owner.principal)
        .count();
    if principal_count >= principal_limit {
        return Err(format!(
            "worker loader: too many loaded workers for script \"{}\" in generation {} (limit {principal_limit})",
            owner.principal.script, owner.principal.generation
        ));
    }
    if registry.len() >= MAX_LOADED_WORKERS {
        return Err(format!(
            "worker loader: too many loaded workers (limit {MAX_LOADED_WORKERS})"
        ));
    }

    let id = service.next_id();
    registry.insert(id, LoaderEntry { owner, state });
    Ok(id)
}

pub(crate) async fn loaded_worker_slot(
    mut state: tokio::sync::watch::Receiver<LoaderState>,
) -> Result<Arc<crate::pool::Slot>, String> {
    loop {
        match state.borrow().clone() {
            LoaderState::Ready(slot) => return Ok(slot),
            LoaderState::Failed(error) => return Err(error.to_string()),
            LoaderState::Loading => {}
        }
        state
            .changed()
            .await
            .map_err(|_| "worker loader: load task dropped".to_string())?;
    }
}

/// Remove one ownership reference. In-flight callers retain their own state.
#[cfg(any(test, cellar_internal_tests))]
pub(crate) fn take_loaded_worker(id: u64) -> Option<tokio::sync::watch::Receiver<LoaderState>> {
    crate::asyncrt::services().loader().take(id)
}

// Worker construction runs outside invocation execution on the caller's host
// runtime, exactly as the transitional loader's startup did.
#[allow(clippy::disallowed_methods)]
pub(crate) fn spawn_loaded_worker(
    handle: tokio::runtime::Handle,
    loaded: tokio::sync::watch::Sender<LoaderState>,
    config: Arc<WorkerConfig>,
) {
    #[cfg(not(cellar_internal_tests))]
    crate::asyncrt::spawn_on(&handle, async move {
        let state = match crate::asyncrt::blocking(move || Worker::load_config(config)).await {
            Ok(Ok(worker)) => LoaderState::Ready(crate::pool::Slot::loaded_worker(worker)),
            Ok(Err(error)) => LoaderState::Failed(Arc::from(format!("{error}"))),
            Err(error) => LoaderState::Failed(Arc::from(format!(
                "worker loader: load task failed: {error}"
            ))),
        };
        loaded.send_replace(state);
    });
    #[cfg(cellar_internal_tests)]
    handle.spawn(async move {
        let state = match tokio::task::spawn_blocking(move || Worker::load_config(config)).await {
            Ok(Ok(worker)) => LoaderState::Ready(crate::pool::Slot::loaded_worker(worker)),
            Ok(Err(error)) => LoaderState::Failed(Arc::from(format!("{error}"))),
            Err(error) => LoaderState::Failed(Arc::from(format!(
                "worker loader: load task failed: {error}"
            ))),
        };
        loaded.send_replace(state);
    });
}

#[cfg(all(test, not(cellar_internal_tests)))]
pub(crate) mod tests;
