// Copyright 2026 Deno Land Inc. Apache-2.0 license.
// Adapted for Cellar from Celld: crates/celld/js.rs.

//! Facet addressing, root ownership and output proof independent of an executor.

use crate::engine_api::{durable_object_id_hex, HttpResponse, HttpResponseWebSocket};
use crate::host_channels::{FacetReq, FACET_TX};
use crate::host_gates::prove_facet;
use crate::host_loader::{loaded_worker_slot, loader_registry, LoaderState};
use crate::storage;
use crate::ws_registry::{ws_attach_facet, ws_close_facet, ws_release_unanswered, FacetSocket};
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

mod calls;
pub(crate) use calls::{abort_facet, delete_facet_files};

pub(crate) fn facet_scope(class_name: &str, parent_scope: &str, owner: &str, name: &str) -> String {
    use sha2::Digest as _;
    let mut digest = sha2::Sha256::new();
    digest.update(parent_scope.as_bytes());
    digest.update([0]);
    digest.update(owner.as_bytes());
    digest.update([0]);
    digest.update(name.as_bytes());
    let digest: [u8; 32] = digest.finalize().into();
    format!("{class_name}:{}", durable_object_id_hex(&digest))
}

/// Activate a facet's stream through the host loop, which owns the
/// replicator, and answer its database file and whether it was restored.
pub(crate) async fn open_facet_file(
    parent: &storage::StorageIdentity,
    name: &str,
) -> Result<(std::path::PathBuf, bool), String> {
    let mut names = parent.facet_path.clone();
    names.push(name.to_string());
    let Some(sender) = FACET_TX.get() else {
        // No host loop runs the replicator, so the file sits beside the
        // root's database and is named after it, as workerd names a facet
        // `<id>.<facet>.sqlite`.
        return Ok((standalone_facet_file(parent, &names), false));
    };
    let (reply, receive) = tokio::sync::oneshot::channel();
    let request = FacetReq::Open {
        root: parent.root_scope.clone(),
        epoch: parent.epoch,
        names,
        reply,
    };
    sender
        .send(request)
        .map_err(|_| "the facet channel closed".to_string())?;
    receive
        .await
        .map_err(|_| "the facet open was dropped".to_string())?
        .map_err(|error| format!("open the facet database: {error:#}"))
}

fn standalone_facet_file(
    parent: &storage::StorageIdentity,
    names: &[String],
) -> std::path::PathBuf {
    let stream = crate::engine_api::facet_cell(&parent.root_scope, names);
    let facet = stream[parent.root_scope.len()..]
        .trim_start_matches('/')
        .replace('/', "-");
    let root = std::path::Path::new(&parent.root_path);
    let file = root.file_name().unwrap().to_string_lossy();
    root.with_file_name(format!("{file}.{facet}.sqlite"))
}

pub(crate) struct FacetStart {
    pub(crate) class_name: String,
    pub(crate) parent_scope: String,
    pub(crate) owner: String,
    pub(crate) name: String,
    pub(crate) id_sc: Vec<u8>,
    pub(crate) props_sc: Vec<u8>,
    pub(crate) parent: storage::StorageIdentity,
}

/// A facet ready for one call, with what its reply must wait for.
pub(crate) struct LoadedFacet {
    service: Weak<dyn FacetService>,
    pub(crate) slot: Arc<crate::pool::Slot>,
    pub(crate) scope: String,
    /// The cell whose database holds the facet's, which owns the facet.
    pub(crate) root: String,
    pub(crate) stream: String,
    pub(crate) epoch: u64,
    /// See `storage::facet_opening`.
    pub(crate) opening: u64,
}

impl LoadedFacet {
    fn check_host(&self) -> Result<(), String> {
        let service = self
            .service
            .upgrade()
            .ok_or_else(|| "facet execution domain has retired".to_string())?;
        check_facet_service(&service)
    }

    /// Route the socket of a facet's upgrade through the facet's root.
    ///
    /// Recorded here, where the host knows which facet answered, because the
    /// response's target names only the facet's scope. A target that the
    /// facet did not accept keeps its own route.
    pub(crate) fn attach_websocket(&self, response: &HttpResponse) -> Result<(), String> {
        self.check_host()?;
        if let Some(HttpResponseWebSocket::Cell(target)) = &response.websocket {
            ws_attach_facet(
                target.id,
                &self.scope,
                FacetSocket {
                    root: self.root.clone(),
                    stream: self.stream.clone(),
                    epoch: self.epoch,
                    opening: self.opening,
                },
            );
        }
        Ok(())
    }

    /// Prove the facet's stream before its response leaves, and release a
    /// socket that the facet accepted for a response that fails this proof.
    ///
    /// The response carries the only reference the host has to that socket.
    /// A dropped response would keep the registration and its regular-socket
    /// count for the life of the process. Only the proof is covered: the
    /// later encode fails only when the stream service has closed, and the
    /// facet's own response read fails first on that service.
    ///
    /// Takes the socket's id rather than the response: a response with a
    /// body stream is not `Sync`, so a borrow of it across the proof would
    /// make the op's future not `Send`.
    pub(crate) async fn gate_response(&self, socket: Option<u64>) -> Result<(), String> {
        self.check_host()?;
        let gated = self.gate_reply().await;
        if let (Err(_), Some(id)) = (&gated, socket) {
            ws_release_unanswered(id, &self.scope);
        }
        gated
    }

    /// A facet's reply leaves as workerd's facet output gate lets it: after
    /// every write the facet committed is durable on its own stream. That
    /// covers an earlier call's write this reply can reveal as well as the
    /// call's own, and an error reply as well as a result.
    pub(crate) async fn gate_reply(&self) -> Result<(), String> {
        self.check_host()?;
        prove_facet(self.stream.clone(), self.epoch)
            .await
            .map_err(|refusal| format!("facet reply: {refusal}"))
    }
}

/// The loader id of a class from the script's own `ctx.exports`. Loaded
/// Workers' ids start at 1.
#[allow(dead_code)]
pub(crate) const OWN_SCRIPT_LOADER: u64 = 0;

/// The isolate a facet's class runs in: a loaded Worker's, or, for a class
/// of the root's own script, the root's own, as workerd runs it.
pub(crate) enum FacetHost {
    Loaded(tokio::sync::watch::Receiver<LoaderState>),
    Own(Arc<crate::pool::Slot>),
    /// A class handle borrowed from another native worker. Its root must
    /// record and release it even when that worker belongs to a normal pool.
    Referenced(Arc<crate::pool::Slot>),
    NativeLoaded(crate::application::loader::Worker),
}

impl FacetHost {
    /// Resolved in the op, while the root's turn names its slot.
    #[allow(dead_code)]
    pub(crate) fn of(loader: u64) -> Result<Self, String> {
        if loader == OWN_SCRIPT_LOADER {
            return crate::pool::current_slot()
                .map(Self::Own)
                .ok_or_else(|| "facet: no isolate runs this turn".to_string());
        }
        loader_registry()
            .lock()
            .unwrap()
            .get(&loader)
            .map(|entry| Self::Loaded(entry.state.clone()))
            .ok_or_else(|| "worker loader: unknown worker".to_string())
    }

    pub(crate) async fn slot(self) -> Result<Arc<crate::pool::Slot>, String> {
        match self {
            Self::Loaded(loaded) => loaded_worker_slot(loaded).await,
            Self::Own(slot) | Self::Referenced(slot) => Ok(slot),
            Self::NativeLoaded(worker) => worker.slot().await.map_err(|error| error.to_string()),
        }
    }

    /// Whether the facet runs in a loaded Worker's isolate, which the root's
    /// give-back must close and which its host records. A facet of a
    /// loaded Worker's own class runs in that Worker's isolate too.
    pub(crate) fn is_loaded_worker(&self) -> bool {
        match self {
            Self::Loaded(_) | Self::Referenced(_) | Self::NativeLoaded(_) => true,
            Self::Own(slot) => slot.is_loaded_worker(),
        }
    }
}

/// The facets that each root runs in loaded Workers' executors, by the root's
/// cell and epoch. A facet of a loaded Worker's own class counts, because it
/// runs in that Worker's isolate.
///
/// A loaded Worker can outlive a root through another stub or until garbage
/// collection. Neither lifetime tells the loaded isolate when a root stops,
/// so connection closure cannot depend on collection. The root's own
/// retirement invalidates its stubs synchronously: the give-back turn cannot
/// drive an asynchronous abort. The adoption turn records each facet here,
/// and the root's give-back closes them.
///
/// A facet that runs in the root's own isolate is not recorded:
/// `finish_cell_adoption` closes it in the root's give-back turn, and its
/// slot can be freed while a reference is held, so a later turn on it would
/// panic.
type FacetRoots = HashMap<(String, u64), RootFacets>;

/// Erase the recursive slot/worker graph at the host-services boundary.
pub(crate) trait FacetService: Send + Sync {
    fn bind_domain(&self, domain: crate::asyncrt::DomainToken);

    fn accepts_work(&self) -> bool;

    fn close(&self);

    fn registry(&self) -> Arc<Mutex<FacetRoots>>;

    fn next_manager(&self) -> u64;
}

pub(crate) struct FacetRegistry {
    roots: Arc<Mutex<FacetRoots>>,
    closed: AtomicBool,
    domain: OnceLock<crate::asyncrt::DomainToken>,
    next_manager: AtomicU64,
}

impl Default for FacetRegistry {
    fn default() -> Self {
        Self {
            roots: Arc::new(Mutex::new(HashMap::new())),
            closed: AtomicBool::new(false),
            domain: OnceLock::new(),
            next_manager: AtomicU64::new(1),
        }
    }
}

impl FacetService for FacetRegistry {
    fn bind_domain(&self, domain: crate::asyncrt::DomainToken) {
        if let Err(candidate) = self.domain.set(domain) {
            assert!(self
                .domain
                .get()
                .is_some_and(|bound| bound.same_owner(&candidate)));
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
        let roots = {
            let mut roots = self.roots.lock().expect("root facets poisoned");
            self.closed.store(true, Ordering::Release);
            std::mem::take(&mut *roots)
        };
        // Dropping a final worker or pin can reenter host resource cleanup.
        drop(roots);
    }

    fn registry(&self) -> Arc<Mutex<FacetRoots>> {
        self.roots.clone()
    }

    fn next_manager(&self) -> u64 {
        self.next_manager.fetch_add(1, Ordering::Relaxed)
    }
}

pub(crate) fn check_facet_service(service: &Arc<dyn FacetService>) -> Result<(), String> {
    if !Arc::ptr_eq(service, &crate::asyncrt::services().facets()) {
        return Err("facet belongs to another execution domain".into());
    }
    if !service.accepts_work() {
        return Err("facet execution domain is closed".into());
    }
    Ok(())
}

#[cfg(any(test, cellar_internal_tests))]
pub(crate) fn root_facets() -> Arc<Mutex<FacetRoots>> {
    crate::asyncrt::services().facets().registry()
}

#[derive(Default)]
pub(crate) struct RootFacets {
    /// Facet calls between their start and their adoption turn. The entry
    /// lives while one runs, so a call that adopts after the root stopped
    /// finds `stopped` rather than a fresh entry.
    pub(crate) pending: usize,
    /// The root gave its residency back and does not run again at this
    /// epoch, so no facet of it can be adopted.
    pub(crate) stopped: bool,
    /// Each slot keeps its loaded isolate alive until the release closes
    /// the facet in it.
    pub(crate) open: Vec<RecordedFacet>,
}

/// A facet that a root runs in a loaded Worker's isolate.
pub(crate) struct RecordedFacet {
    pub(crate) slot: Arc<crate::pool::Slot>,
    pub(crate) scope: String,
    /// The facet names from the root down to this facet, which find the
    /// facets below an aborted one.
    pub(crate) path: Vec<String>,
    /// A reference to a pooled slot does not keep its worker installed.
    /// Retain an executor pin through closure when a native class is borrowed
    /// from another worker's pool, including a retiring generation.
    pub(crate) pin: crate::pool::WorkerPin,
}

/// One facet call of a root, from its start until its adoption turn.
pub(crate) struct PendingFacet {
    pub(crate) key: (String, u64),
    service: Arc<dyn FacetService>,
}

impl PendingFacet {
    pub(crate) fn begin(root: &str, epoch: u64) -> Result<Self, String> {
        let key = (root.to_string(), epoch);
        let service = crate::asyncrt::services().facets();
        let registry = service.registry();
        let mut roots = registry.lock().expect("root facets poisoned");
        if !service.accepts_work() {
            return Err("facet execution domain is closed".into());
        }
        let entry = roots.entry(key.clone()).or_default();
        if entry.stopped {
            return Err(format!("{root} epoch {epoch} stopped"));
        }
        entry.pending += 1;
        Ok(Self { key, service })
    }

    /// Record the facet before its adoption. Call inside the adoption turn:
    /// a release that takes the record queues its close behind this turn on
    /// the same slot, and a release that came first makes this refuse.
    pub(crate) fn record(
        &self,
        slot: &Arc<crate::pool::Slot>,
        scope: &str,
        path: &[String],
    ) -> Result<(), String> {
        check_facet_service(&self.service)?;
        let registry = self.service.registry();
        let mut roots = registry.lock().expect("root facets poisoned");
        if !self.service.accepts_work() {
            return Err("facet execution domain is closed".into());
        }
        let entry = roots
            .get_mut(&self.key)
            .expect("a pending facet keeps its entry");
        if entry.stopped {
            return Err(format!("{} epoch {} stopped", self.key.0, self.key.1));
        }
        if !entry
            .open
            .iter()
            .any(|open| Arc::ptr_eq(&open.slot, slot) && open.scope == scope)
        {
            entry.open.push(RecordedFacet {
                slot: slot.clone(),
                scope: scope.to_string(),
                path: path.to_vec(),
                pin: slot.retain_worker(),
            });
        }
        Ok(())
    }
}

impl Drop for PendingFacet {
    fn drop(&mut self) {
        let registry = self.service.registry();
        let mut roots = registry.lock().expect("root facets poisoned");
        if let Some(entry) = roots.get_mut(&self.key) {
            entry.pending -= 1;
            if entry.pending == 0 && entry.open.is_empty() {
                roots.remove(&self.key);
            }
        }
    }
}

/// How a root leaves the isolate that runs it.
#[derive(Clone, Copy)]
pub(crate) enum RootRelease {
    /// The root stops at this epoch: its facets close, and none opens again.
    Stop,
    /// The root restarts at the same epoch on another generation. It builds
    /// a new facet manager there, so the facets of this one close, but the
    /// new instance can open its own.
    ///
    /// A call of the old instance can still adopt after this release, and
    /// that is bounded. The facet ops read the root's storage identity
    /// synchronously, and the give-back closed that storage, so only a call
    /// that started before the swap turn gets here. It records under the
    /// same root and epoch, so the root's next release closes it. Marking
    /// the epoch stopped instead would refuse the new instance's facets.
    Swap,
}

/// Close each facet that `root` runs in a loaded Worker's isolate. The
/// caller gives the root back first, so the root cannot start a facet call
/// after the facets are taken.
pub(crate) fn release_root_facets(
    root: &str,
    epoch: u64,
    release: RootRelease,
) -> impl std::future::Future<Output = Result<(), String>> + Send {
    release_root_facets_in(
        crate::asyncrt::services().facets(),
        root.to_string(),
        epoch,
        release,
    )
}

async fn release_root_facets_in(
    service: Arc<dyn FacetService>,
    root: String,
    epoch: u64,
    release: RootRelease,
) -> Result<(), String> {
    check_facet_service(&service)?;
    let key = (root.clone(), epoch);
    let open = {
        let registry = service.registry();
        let mut roots = registry.lock().expect("root facets poisoned");
        let Some(entry) = roots.get_mut(&key) else {
            return Ok(());
        };
        if matches!(release, RootRelease::Stop) {
            entry.stopped = true;
        }
        let open = std::mem::take(&mut entry.open);
        if entry.pending == 0 {
            roots.remove(&key);
        }
        open
    };
    // A nested facet records after its parent, so it closes first.
    for RecordedFacet {
        slot, scope, pin, ..
    } in open.into_iter().rev()
    {
        if let Err(error) = slot.turn(|worker| worker.own_cell(&scope, None)).await {
            tracing::warn!(root, epoch, facet = %scope, %error, "a facet did not close with its root");
        }
        ws_close_facet(&scope, "the parent Durable Object was released");
        drop(pin);
    }
    Ok(())
}

/// Take the records of the facets below the facet at `path`, deepest first.
/// Their isolates can differ from the aborted facet's: a child of a loaded
/// class runs in its own loaded Worker.
pub(crate) fn take_recorded_facets_below(
    root: &str,
    epoch: u64,
    path: &[String],
) -> Vec<RecordedFacet> {
    let key = (root.to_string(), epoch);
    let registry = crate::asyncrt::services().facets().registry();
    let mut roots = registry.lock().expect("root facets poisoned");
    let Some(entry) = roots.get_mut(&key) else {
        return Vec::new();
    };
    let (mut below, kept) = std::mem::take(&mut entry.open)
        .into_iter()
        .partition::<Vec<_>, _>(|open| open.path.len() > path.len() && open.path.starts_with(path));
    entry.open = kept;
    if entry.pending == 0 && entry.open.is_empty() {
        roots.remove(&key);
    }
    below.sort_by_key(|open| std::cmp::Reverse(open.path.len()));
    below
}

/// The loaded isolate that runs the facet `scope` of `root` at `epoch`, while
/// the root's give-back has not closed it. `None` also answers for a facet in
/// the root's own isolate, which the root's residency pins instead.
pub(crate) fn recorded_facet_slot(
    root: &str,
    epoch: u64,
    scope: &str,
) -> Option<Arc<crate::pool::Slot>> {
    crate::asyncrt::services()
        .facets()
        .registry()
        .lock()
        .expect("root facets poisoned")
        .get(&(root.to_string(), epoch))?
        .open
        .iter()
        .find(|open| open.scope == scope)
        .map(|open| open.slot.clone())
}

/// Prove every write that a facet committed on its own stream.
pub(crate) async fn prove_facet_stream(stream: String, epoch: u64) -> anyhow::Result<()> {
    prove_facet(stream, epoch)
        .await
        .map_err(|refusal| anyhow!("facet output: {refusal}"))
}

/// Drop an aborted facet's record, which its abort closed already.
pub(crate) fn forget_root_facet(
    root: &str,
    epoch: u64,
    slot: &Arc<crate::pool::Slot>,
    scope: &str,
) {
    let key = (root.to_string(), epoch);
    let forgotten = {
        let registry = crate::asyncrt::services().facets().registry();
        let mut roots = registry.lock().expect("root facets poisoned");
        if let Some(entry) = roots.get_mut(&key) {
            let (forgotten, kept) = std::mem::take(&mut entry.open)
                .into_iter()
                .partition::<Vec<_>, _>(|open| {
                    Arc::ptr_eq(&open.slot, slot) && open.scope == scope
                });
            entry.open = kept;
            if entry.pending == 0 && entry.open.is_empty() {
                roots.remove(&key);
            }
            forgotten
        } else {
            Vec::new()
        }
    };
    drop(forgotten);
}

#[allow(dead_code)]
pub(crate) fn prepare_facet(
    host: FacetHost,
    start: FacetStart,
) -> impl std::future::Future<Output = Result<LoadedFacet, String>> + Send {
    let pending = if host.is_loaded_worker() {
        PendingFacet::begin(&start.parent.root_scope, start.parent.epoch).map(Some)
    } else {
        Ok(None)
    };
    prepare_facet_in(
        crate::asyncrt::services().facets(),
        host,
        start,
        pending,
        None,
    )
}

/// Take the pending claim while the parent is still executing. A host task
/// started later cannot adopt storage after root retirement removed its map.
pub(crate) fn prepare_native_facet(
    host: FacetHost,
    start: FacetStart,
    authority: Arc<crate::application::Activation>,
) -> impl std::future::Future<Output = Result<LoadedFacet, String>> + Send {
    let pending = PendingFacet::begin(&start.parent.root_scope, start.parent.epoch).map(Some);
    prepare_facet_in(
        crate::asyncrt::services().facets(),
        host,
        start,
        pending,
        Some(authority),
    )
}

async fn prepare_facet_in(
    service: Arc<dyn FacetService>,
    host: FacetHost,
    start: FacetStart,
    pending: Result<Option<PendingFacet>, String>,
    authority: Option<Arc<crate::application::Activation>>,
) -> Result<LoadedFacet, String> {
    let pending = pending?;
    check_facet_service(&service)?;
    if start.parent.facet_path.len() >= 3 {
        return Err("Facet nesting depth limit exceeded. The maximum depth including the root Durable Object is 4.".into());
    }
    let recorded = host.is_loaded_worker();
    let slot = host.slot().await?;
    check_facet_service(&service)?;
    let file = open_facet_file(&start.parent, &start.name).await?;
    let mut names = start.parent.facet_path.clone();
    names.push(start.name.clone());
    let stream = crate::engine_api::facet_cell(&start.parent.root_scope, &names);
    let epoch = start.parent.epoch;
    let scope = facet_scope(
        &start.class_name,
        &start.parent_scope,
        &start.owner,
        &start.name,
    );
    let opening = slot
        .turn(|worker| {
            if let Some(authority) = &authority {
                if authority
                    .resources
                    .retired
                    .load(std::sync::atomic::Ordering::Acquire)
                    || authority.resources.failure.lock().unwrap().is_some()
                {
                    return Err("facet parent activation has retired".into());
                }
            }
            if let Some(pending) = pending.as_ref().filter(|_| recorded) {
                pending.record(&slot, &scope, &names)?;
            }
            worker
                .own_embedded_cell(
                    &scope,
                    &start.parent,
                    &start.name,
                    start.id_sc,
                    start.props_sc,
                    file,
                )
                .map_err(|error| format!("{error}"))
        })
        .await
        .map_err(|error| format!("worker loader facet: {error}"))?;
    Ok(LoadedFacet {
        service: Arc::downgrade(&service),
        slot,
        scope,
        root: start.parent.root_scope,
        stream,
        epoch,
        opening,
    })
}
