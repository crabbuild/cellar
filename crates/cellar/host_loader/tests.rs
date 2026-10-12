use super::*;
use crate::application::{
    Application, Cell, CellIdentity, Context, HandlerFuture, Invocation, Output, Registry,
};
use crate::engine_api::{Compat, WorkerConfigOptions};
use crate::host_facets::*;
use std::collections::HashSet;
use std::sync::atomic::AtomicUsize;

static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[test]
fn concurrent_admission_shares_principal_limits_and_reserves_process_capacity() {
    let _serial = SERIAL.blocking_lock();
    let first = LoaderOwner::fresh("host-quota", 1);
    let second = LoaderOwner::fresh("host-quota", 1);
    let threads = (0..32)
        .map(|index| {
            let owner = if index % 2 == 0 {
                first.clone()
            } else {
                second.clone()
            };
            std::thread::spawn(move || {
                (0..8)
                    .map(|_| {
                        let (_, state) = tokio::sync::watch::channel(LoaderState::Loading);
                        admit_loaded_worker(owner.clone(), state)
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    let results = threads
        .into_iter()
        .flat_map(|thread| thread.join().unwrap())
        .collect::<Vec<_>>();
    let admitted = results
        .iter()
        .filter_map(|result| result.as_ref().ok().copied())
        .collect::<HashSet<_>>();
    assert_eq!(admitted.len(), MAX_LOADED_WORKERS_PER_PRINCIPAL);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
    // The generation is part of the principal, even for the same script.
    let competing = LoaderOwner::fresh("host-quota", 2);
    let (_, state) = tokio::sync::watch::channel(LoaderState::Loading);
    let final_id = admit_loaded_worker(competing.clone(), state).unwrap();
    let (_, state) = tokio::sync::watch::channel(LoaderState::Loading);
    assert!(admit_loaded_worker(competing.clone(), state)
        .err()
        .unwrap()
        .contains("too many loaded workers (limit"));
    let taken = take_loader_owner(&first);
    assert!(admitted
        .iter()
        .any(|id| !loader_registry().lock().unwrap().contains_key(id)));
    assert!(loader_registry().lock().unwrap().contains_key(&final_id));
    let remaining = take_loader_owner(&second);
    assert_eq!(
        taken.len() + remaining.len(),
        MAX_LOADED_WORKERS_PER_PRINCIPAL
    );
    drop(taken);
    drop(remaining);
    drop(take_loader_owner(&competing));
    assert!(admitted
        .iter()
        .all(|id| !loader_registry().lock().unwrap().contains_key(id)));
}

struct DropProbe(Arc<AtomicUsize>);
impl Drop for DropProbe {
    fn drop(&mut self) {
        assert!(
            loader_registry().try_lock().is_ok(),
            "worker dropped under loader registry lock"
        );
        assert!(
            ROOT_FACETS
                .get()
                .is_none_or(|roots| roots.try_lock().is_ok()),
            "worker dropped under root facet lock"
        );
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn config(application: Arc<dyn Application>) -> Arc<WorkerConfig> {
    Arc::new(
        WorkerConfig::new(WorkerConfigOptions {
            src: "not a JavaScript module!".into(),
            script_name: "loaded-native".into(),
            do_classes: Vec::new(),
            bindings: Vec::new(),
            blob_bindings: Vec::new(),
            sql_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: Vec::new(),
            node: "test".into(),
            modules: Vec::new(),
            compat: Compat::default(),
        })
        .with_native_application(application)
        .into_dynamic_worker(),
    )
}

fn probed_config(drops: Arc<AtomicUsize>) -> Arc<WorkerConfig> {
    let probe = DropProbe(drops);
    config(Arc::new(Registry::new(move |_, _| {
        let _ = &probe;
        Box::pin(async { Ok(Output::Done) })
    })))
}
fn slot(drops: Arc<AtomicUsize>) -> Arc<crate::pool::Slot> {
    crate::pool::Slot::loaded_worker(Worker::load_config(probed_config(drops)).unwrap())
}

struct PanicConstructor;
impl Application for PanicConstructor {
    fn invoke(self: Arc<Self>, _: Context, _: Invocation) -> HandlerFuture<Output> {
        Box::pin(async { Ok(Output::Done) })
    }
    fn supports_cell(&self, _: &str) -> bool {
        panic!("native startup panic");
    }
    fn create_cell(&self, _: Context, _: &CellIdentity) -> anyhow::Result<Arc<dyn Cell>> {
        unreachable!()
    }
}

// Called by the native driver's single process-domain test, after it has
// installed the host handle. Facet release uses real host wake/socket state.
pub(crate) async fn check_host_lifecycles() -> anyhow::Result<()> {
    let _serial = SERIAL.lock().await;
    let owner = LoaderOwner::fresh("host-lifetime", 3);
    let drops = Arc::new(AtomicUsize::new(0));
    let worker = slot(drops.clone());
    let (send, state) = tokio::sync::watch::channel(LoaderState::Ready(worker));
    let caller = state.clone();
    let id = admit_loaded_worker(owner.clone(), state).unwrap();
    drop(send);
    let removed = take_loader_owner(&owner);
    assert!(!loader_registry().lock().unwrap().contains_key(&id));
    drop(removed);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "an in-flight caller retains its loaded worker"
    );
    let ready = loaded_worker_slot(caller).await.unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(ready);
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    let drops = Arc::new(AtomicUsize::new(0));
    let (send, state) = tokio::sync::watch::channel(LoaderState::Ready(slot(drops.clone())));
    let id = admit_loaded_worker(owner.clone(), state).unwrap();
    drop(send);
    drop(take_loaded_worker(id));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(take_loaded_worker(id).is_none());

    let drops = Arc::new(AtomicUsize::new(0));
    let (send, mut state) = tokio::sync::watch::channel(LoaderState::Loading);
    let id = admit_loaded_worker(owner.clone(), state.clone()).unwrap();
    spawn_loaded_worker(
        tokio::runtime::Handle::current(),
        send,
        probed_config(drops.clone()),
    );
    let ready = loaded_worker_slot(state.clone()).await.unwrap();
    assert!(ready.is_loaded_worker());
    // Observe the load producer's terminal lifetime before releasing every
    // receiver; readiness can notify a caller just before the producer exits.
    while state.changed().await.is_ok() {}
    drop(take_loaded_worker(id));
    drop(state);
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(ready);
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    let mut bad = config(Arc::new(PanicConstructor));
    Arc::get_mut(&mut bad)
        .unwrap()
        .do_classes
        .push("Panic".into());
    let (send, state) = tokio::sync::watch::channel(LoaderState::Loading);
    spawn_loaded_worker(tokio::runtime::Handle::current(), send, bad);
    let failure = loaded_worker_slot(state).await.err().unwrap();
    assert!(failure.contains("worker loader: load task failed:"));
    assert!(failure.contains("native startup panic"));
    let (send, state) = tokio::sync::watch::channel(LoaderState::Loading);
    drop(send);
    assert_eq!(
        loaded_worker_slot(state).await.err().unwrap(),
        "worker loader: load task dropped"
    );

    let drops = Arc::new(AtomicUsize::new(0));
    let parent = slot(drops.clone());
    let child = slot(drops.clone());
    let root = "host-facet-release";
    let pending = PendingFacet::begin(root, 7).unwrap();
    pending
        .record(&parent, "Facet:parent", &["p".into()])
        .unwrap();
    pending
        .record(&parent, "Facet:parent", &["p".into()])
        .unwrap();
    pending
        .record(&child, "Facet:child", &["p".into(), "child".into()])
        .unwrap();
    pending
        .record(
            &child,
            "Facet:deep",
            &["p".into(), "child".into(), "deep".into()],
        )
        .unwrap();
    assert_eq!(root_facets().get(&(root.into(), 7)).unwrap().open.len(), 3);
    assert!(Arc::ptr_eq(
        &recorded_facet_slot(root, 7, "Facet:child").unwrap(),
        &child
    ));
    let below = take_recorded_facets_below(root, 7, &["p".into()]);
    assert_eq!(
        below
            .iter()
            .map(|facet| facet.scope.as_str())
            .collect::<Vec<_>>(),
        ["Facet:deep", "Facet:child"]
    );
    drop(below);
    release_root_facets(root, 7, RootRelease::Stop).await;
    assert!(pending.record(&child, "Facet:late", &["p".into()]).is_err());
    assert!(PendingFacet::begin(root, 7).is_err());
    assert!(recorded_facet_slot(root, 7, "Facet:parent").is_none());
    drop(pending);
    assert!(!root_facets().contains_key(&(root.into(), 7)));

    let root = "host-facet-swap";
    let pending = PendingFacet::begin(root, 8).unwrap();
    pending.record(&parent, "Facet:old", &["p".into()]).unwrap();
    release_root_facets(root, 8, RootRelease::Swap).await;
    pending.record(&child, "Facet:new", &["p".into()]).unwrap();
    let next = PendingFacet::begin(root, 8).unwrap();
    assert!(recorded_facet_slot(root, 8, "Facet:old").is_none());
    assert!(recorded_facet_slot(root, 8, "Facet:new").is_some());
    forget_root_facet(root, 8, &child, "Facet:new");
    assert!(recorded_facet_slot(root, 8, "Facet:new").is_none());
    drop(next);
    drop(pending);
    drop(parent);
    drop(child);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert!(!root_facets().contains_key(&(root.into(), 8)));
    Ok(())
}
