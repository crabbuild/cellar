use super::*;
use crate::application::Registry;
use crate::engine_api::{Compat, WorkerConfigOptions};
use crate::wake_entry::ArmGateRx;
use futures_util::FutureExt;
use std::sync::atomic::AtomicUsize;
use std::sync::Weak;
use std::task::{Wake, Waker};
use std::time::Duration;
use tokio::sync::{oneshot, Notify};

struct ReceiverWake {
    sender: Arc<WakeTurn>,
    calls: AtomicUsize,
}

impl Wake for ReceiverWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        assert!(
            self.sender.0.try_lock().is_ok(),
            "receiver notified under sender lock"
        );
        assert!(!self.sender.pending());
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn native_wake_releases_its_sender_lock_before_notifying_the_receiver() {
    let (send, mut receive) = oneshot::channel();
    let sender = Arc::new(WakeTurn(Mutex::new(Some(send))));
    let observed = Arc::new(ReceiverWake {
        sender: sender.clone(),
        calls: AtomicUsize::new(0),
    });
    let waker = Waker::from(observed.clone());
    let mut cx = TaskContext::from_waker(&waker);
    assert!(Pin::new(&mut receive).poll(&mut cx).is_pending());
    futures_util::task::ArcWake::wake_by_ref(&sender);
    assert_eq!(observed.calls.load(Ordering::SeqCst), 1);
    assert_eq!(Pin::new(&mut receive).poll(&mut cx), Poll::Ready(Ok(())));
    futures_util::task::ArcWake::wake_by_ref(&sender);
    assert_eq!(observed.calls.load(Ordering::SeqCst), 1);
}

#[derive(Default)]
struct Signals {
    handler: Notify,
    background: Notify,
    background_done: AtomicUsize,
    handler_drops: Arc<AtomicUsize>,
    gate: Mutex<Option<ArmGateRx>>,
}

struct DropCount(Arc<AtomicUsize>);

impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct HandedOperation {
    owner: Weak<IoContext>,
    drops: Arc<AtomicUsize>,
}

impl Future for HandedOperation {
    type Output = std::result::Result<asyncrt::OpOut, String>;

    fn poll(self: Pin<&mut Self>, _: &mut TaskContext<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for HandedOperation {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            // Reentrant mailbox access must be possible on both refusal and retirement.
            drop(owner.take_handed_ops());
        }
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

fn config(signals: Arc<Signals>) -> Arc<WorkerConfig> {
    let application = Registry::new(move |context, invocation| {
        let signals = signals.clone();
        Box::pin(async move {
            context.check_turn()?;
            assert_eq!(context.var("GREETING"), Some("native"));
            let Invocation::Fetch(request) = invocation else {
                unreachable!()
            };
            if request.url.ends_with("/gated") {
                let gate = signals.gate.lock().unwrap().take().unwrap();
                assert!(context.io.register_arm_gate(gate).is_ok());
            } else {
                let background_context = context.clone();
                let background_signals = signals.clone();
                context.wait_until(async move {
                    background_signals.background.notified().await;
                    background_context.check_turn()?;
                    background_signals
                        .background_done
                        .fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })?;
                let _drop = DropCount(signals.handler_drops.clone());
                signals.handler.notified().await;
                context.check_turn()?;
            }
            Ok(Output::Http(HttpResponse {
                status: 200,
                body: b"native".to_vec(),
                stream: None,
                headers: Vec::new(),
                websocket: None,
                write_position: None,
                observed_position: None,
            }))
        })
    });
    Arc::new(
        WorkerConfig::new(WorkerConfigOptions {
            src: "not a JavaScript module!".into(),
            script_name: "native-direct".into(),
            do_classes: Vec::new(),
            bindings: Vec::new(),
            blob_bindings: Vec::new(),
            sql_bindings: Vec::new(),
            kv_bindings: Vec::new(),
            queue_bindings: Vec::new(),
            queue_consumers: Vec::new(),
            workflow_bindings: Vec::new(),
            vars: vec![("GREETING".into(), "native".into())],
            node: "test".into(),
            modules: Vec::new(),
            compat: Compat::default(),
        })
        .with_native_application(Arc::new(application)),
    )
}

// A response deadline is host elapsed time, including in this direct-driver test.
#[allow(clippy::disallowed_methods)]
fn job(path: &str) -> (crate::WorkerJob, oneshot::Receiver<Result<HttpResponse>>) {
    let (reply, receive) = oneshot::channel();
    (
        crate::WorkerJob::Fetch {
            queued_at: Instant::now(),
            entrypoint: None,
            invocation_limits: None,
            url: format!("http://native/{path}"),
            method: "GET".into(),
            body: Vec::new().into(),
            headers: Vec::new(),
            request_id: Some(crate::engine_api::next_request_id()),
            tail_report: None,
            reply,
        },
        receive,
    )
}

async fn resume(worker: &mut NativeWorker, entry: &mut InFlight, ops: &mut Vec<Op>) {
    assert_eq!(ops.len(), 1, "one native wake owns this suspended turn");
    let (id, future) = ops.pop().unwrap();
    asyncrt::timeout(Duration::from_secs(2), future)
        .await
        .unwrap()
        .unwrap();
    assert!(entry.ops.remove(&id));
    ops.extend(worker.poll(entry));
    assert!(installed_context().is_none());
}

// One runtime owns the unit-test process domain. No Engine::init, JS Worker,
// isolate factory, or ActorRuntimeState participates in this bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn direct_native_bootstrap_preserves_turns_background_cancellation_gates_and_handoff(
) -> Result<()> {
    asyncrt::set_host_handle(tokio::runtime::Handle::current());
    let signals = Arc::new(Signals::default());
    let mut worker = NativeWorker::new(config(signals.clone()))?;
    let (request, mut receive) = job("respond");
    let (entry, mut ops) = worker.begin(request, None);
    let mut entry = entry.unwrap();
    assert!(entry.engine.is_none());
    assert!(installed_context().is_none());
    let id = entry.context.continuation_id().unwrap();
    assert!(Arc::ptr_eq(
        &worker.registry.lookup(id).unwrap(),
        &entry.context
    ));
    assert!(receive.try_recv().is_err());
    signals.handler.notify_one();
    resume(&mut worker, &mut entry, &mut ops).await;
    assert_eq!(receive.await??.body, b"native");
    assert!(entry.answered());
    assert!(
        !entry.finished(),
        "waitUntil retains the event after its answer"
    );
    signals.background.notify_one();
    resume(&mut worker, &mut entry, &mut ops).await;
    assert_eq!(signals.background_done.load(Ordering::SeqCst), 1);
    assert!(entry.finished());
    worker.retire(&entry);
    entry.abandon();
    drop(entry);
    assert!(worker.registry.lookup(id).is_none());

    let (request, receive) = job("cancel");
    let (entry, mut ops) = worker.begin(request, None);
    let mut entry = entry.unwrap();
    let extra = worker.cancel(&mut entry, false);
    ops.extend(extra);
    assert_eq!(
        receive.await?.err().unwrap().to_string(),
        "The client has disconnected"
    );
    assert_eq!(signals.handler_drops.load(Ordering::SeqCst), 2);
    assert!(
        !entry.finished(),
        "normal disconnect retains background work"
    );
    signals.background.notify_one();
    resume(&mut worker, &mut entry, &mut ops).await;
    assert!(entry.finished());
    worker.retire(&entry);
    entry.abandon();
    drop(entry);

    let (gate, wait) = oneshot::channel();
    *signals.gate.lock().unwrap() = Some(wait);
    let (request, mut receive) = job("gated");
    let (entry, ops) = worker.begin(request, None);
    let mut entry = entry.unwrap();
    assert!(ops.is_empty());
    assert!(!entry.answered());
    assert!(receive.try_recv().is_err());
    entry.cancel_gated_reply();
    let completion =
        asyncrt::timeout(Duration::from_secs(2), entry.gated_reply.as_mut().unwrap()).await?;
    entry.finish_gated_reply(completion);
    assert!(
        gate.is_closed(),
        "all event gates are released before completion"
    );
    assert_eq!(
        receive.await?.err().unwrap().to_string(),
        "The client has disconnected"
    );
    assert!(entry.finished());
    worker.retire(&entry);
    entry.abandon();
    drop(entry);

    let (request, _owner_reply) = job("owner");
    let (owner, owner_ops) = worker.begin(request, None);
    let mut owner = owner.unwrap();
    let (request, _source_reply) = job("source");
    let (source, source_ops) = worker.begin(request, None);
    let mut source = source.unwrap();
    let dropped = Arc::new(AtomicUsize::new(0));
    let owned = Box::pin(HandedOperation {
        owner: Arc::downgrade(&owner.context),
        drops: dropped.clone(),
    });
    source.hand_op(
        owner.context.continuation_id().unwrap(),
        123_456,
        owned,
        asyncrt::OpLifetime::IoContext,
    );
    source.abandon();
    drop(source_ops);
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        0,
        "source retirement cannot cancel a foreign owner's op"
    );
    let transferred = owner.take_handed_ops();
    assert_eq!(transferred.len(), 1);
    assert!(owner.io_context_ops.contains(&123_456));
    owner.abandon();
    drop(transferred);
    drop(owner_ops);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    let refused = Box::pin(HandedOperation {
        owner: Arc::downgrade(&owner.context),
        drops: dropped.clone(),
    });
    source.hand_op(
        owner.context.continuation_id().unwrap(),
        123_457,
        refused,
        asyncrt::OpLifetime::Handler,
    );
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        2,
        "closed owners refuse and release new operations"
    );
    drop(source);
    drop(owner);

    // Already-completed gates win simultaneous cancellation; caller-close
    // releases both the response value and every still-pending gate first.
    let (gate, wait) = oneshot::channel();
    gate.send(Ok(())).unwrap();
    let (reply, receive) = oneshot::channel();
    let mut completion = send_answer_after_arm_gates(reply, Ok(7u8), vec![wait]).unwrap();
    completion.cancel();
    completion.await?;
    assert_eq!(receive.await??, 7);
    let dropped = Arc::new(AtomicUsize::new(0));
    let (gate, wait) = oneshot::channel();
    let (reply, receive) = oneshot::channel();
    drop(receive);
    let completion =
        send_answer_after_arm_gates(reply, Ok(DropCount(dropped.clone())), vec![wait]).unwrap();
    completion.await?;
    assert!(gate.is_closed());
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    check_shared_worker_pool().await?;
    #[cfg(not(cellar_internal_tests))]
    crate::host_loader::tests::check_host_lifecycles().await?;
    super::facets::tests::check_native_facet_lifecycles().await?;
    assert!(asyncrt::drain_spawns().is_empty());
    assert!(installed_context().is_none());
    Ok(())
}

async fn check_shared_worker_pool() -> Result<()> {
    let signals = Arc::new(Signals::default());
    let config = config(signals.clone());
    // A compiled registration is sufficient; no JS parser or initializer can
    // be consulted when this configuration enters the shared worker factory.
    let pool = crate::pool::Pool::new(
        cellar_logic::isolate::PoolLimits {
            grow_at: 2,
            shrink_under: 1,
            max_stateless: 1,
            max_requests: Some(2),
            max_cells: 4,
        },
        Duration::from_secs(1),
        Box::new(move || crate::worker::Worker::load_config(config.clone())),
    );
    pool.warm()?;
    let affiliation = pool.admit(false).map_err(|error| anyhow!("{error}"))?;
    let slot = affiliation.slot().clone();
    let pending_facet = crate::host_facets::PendingFacet::begin("native-pool-facet", 1)
        .map_err(anyhow::Error::msg)?;
    pending_facet
        .record(&slot, "Facet:retained", &["retained".into()])
        .map_err(anyhow::Error::msg)?;
    drop(pending_facet);
    assert!(crate::pool::current_slot().is_none());
    let panic = std::panic::AssertUnwindSafe(slot.turn(|_| panic!("failed host turn")))
        .catch_unwind()
        .await;
    assert!(panic.is_err());
    assert!(
        crate::pool::current_slot().is_none(),
        "panic leaked its current pool slot"
    );
    assert!(installed_context().is_none());
    let (request, receive) = job("respond");
    let (entry, mut ops) = slot
        .turn(|worker| {
            assert!(worker.backend_mut::<NativeWorker>().is_some());
            assert!(worker.heap_bytes().is_none());
            assert!(Arc::ptr_eq(&crate::pool::current_slot().unwrap(), &slot));
            worker.turn_begin(request, None)
        })
        .await;
    let mut entry = entry.unwrap();
    assert!(entry.engine.is_none());
    pool.retire_all();
    assert!(
        !pool.is_drained(),
        "a suspended affiliation pins its original backend"
    );
    // A request that already snapshotted a draining generation may still
    // land. Any newly built backend is born retiring and cannot stay warm.
    let late = pool.admit(false).map_err(|error| anyhow!("{error}"))?;
    assert!(late.slot().is_retiring());
    drop(late);
    pool.reap();
    assert!(
        !pool.is_drained(),
        "the original request still owns its worker"
    );
    signals.handler.notify_one();
    let (id, future) = ops.pop().unwrap();
    let result = asyncrt::timeout(Duration::from_secs(2), future).await?;
    ops.extend(
        slot.turn(|worker| worker.turn_deliver(&mut entry, id, result))
            .await,
    );
    assert_eq!(receive.await??.body, b"native");
    assert!(!entry.finished());
    assert!(
        !pool.is_drained(),
        "background work keeps the affiliation live"
    );
    signals.background.notify_one();
    let (id, future) = ops.pop().unwrap();
    let result = asyncrt::timeout(Duration::from_secs(2), future).await?;
    ops.extend(
        slot.turn(|worker| worker.turn_deliver(&mut entry, id, result))
            .await,
    );
    assert!(entry.finished());
    assert!(ops.is_empty());
    assert_eq!(signals.background_done.load(Ordering::SeqCst), 1);
    slot.turn(|worker| worker.turn_abandon_input_gates(&entry))
        .await;
    entry.abandon();
    drop(entry);
    drop(affiliation);
    pool.reap();
    assert!(
        !pool.is_drained(),
        "a borrowed facet class must pin its retiring backend"
    );
    crate::host_facets::release_root_facets(
        "native-pool-facet",
        1,
        crate::host_facets::RootRelease::Stop,
    )
    .await
    .map_err(anyhow::Error::msg)?;
    pool.reap();
    assert!(pool.is_drained());
    assert!(crate::pool::current_slot().is_none());
    assert!(installed_context().is_none());
    crate::host_tcp::tests::native_tls_application(|application| {
        let mut configuration = self::config(signals.clone());
        Arc::get_mut(&mut configuration).unwrap().native_application = Some(Arc::new(application));
        async move {
            let mut worker = NativeWorker::new(configuration)?;
            let (request, receive) = job("tls");
            let (entry, mut ops) = worker.begin(request, None);
            let mut entry = entry.unwrap();
            while !entry.finished() {
                resume(&mut worker, &mut entry, &mut ops).await;
            }
            assert_eq!(receive.await??.body, b"yes");
            worker.retire(&entry);
            Ok(())
        }
    })
    .await?;
    Ok(())
}

#[cfg(not(cellar_internal_tests))]
#[tokio::test]
async fn sdk_context_refuses_another_domain_and_effects_after_its_owner_closes() -> Result<()> {
    let first = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let second = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    first
        .run(async {
            let worker = NativeWorker::new(config(Arc::new(Signals::default())))?;
            let context = worker.context(None, None, None, false)?;
            let _turn = crate::host_io::CurrentGuard::enter(context.io.clone());
            context.check_turn()?;
            second
                .run(async {
                    assert!(context
                        .wait_until(async { Ok(()) })
                        .unwrap_err()
                        .to_string()
                        .contains("another execution domain"));
                })
                .await?;
            context.check_turn()?;
            first.close();
            assert!(context
                .wait_until(async { Ok(()) })
                .unwrap_err()
                .to_string()
                .contains("execution domain is closed"));
            Ok::<_, anyhow::Error>(())
        })
        .await??;
    first.shutdown().await;
    second.shutdown().await;
    Ok(())
}
