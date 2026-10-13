use super::*;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Weak;
use std::task::{Context, Poll, Wake, Waker};

#[test]
fn guards_restore_nested_context_before_reentrant_cleanup() {
    assert!(installed_context().is_none());
    let outer = IoContext::new();
    let outer_guard = CurrentGuard::enter(outer.clone());
    let observed = Arc::new(AtomicUsize::new(0));
    let count = observed.clone();
    let expected = Arc::downgrade(&outer);
    let mut inner = IoContext::new();
    Arc::get_mut(&mut inner)
        .unwrap()
        .set_registration(17, move |id| {
            assert_eq!(id, 17);
            assert!(Arc::ptr_eq(
                &installed_context().unwrap(),
                &expected.upgrade().unwrap()
            ));
            // Reentrant nested entry must also restore the original outer owner.
            let guard = CurrentGuard::enter(IoContext::new());
            drop(guard);
            assert!(Arc::ptr_eq(
                &installed_context().unwrap(),
                &expected.upgrade().unwrap()
            ));
            count.fetch_add(1, Ordering::Relaxed);
        });
    let weak = Arc::downgrade(&inner);
    let inner_guard = CurrentGuard::enter(inner);
    assert!(Arc::ptr_eq(
        &installed_context().unwrap(),
        &weak.upgrade().unwrap()
    ));
    drop(inner_guard);
    assert!(weak.upgrade().is_none());
    assert_eq!(observed.load(Ordering::Relaxed), 1);
    assert!(Arc::ptr_eq(&installed_context().unwrap(), &outer));
    drop(outer_guard);
    assert!(installed_context().is_none());
    assert!(outer.engine.get().is_none());
}

#[test]
fn panic_unwinds_to_the_original_host_turn() {
    let outer = IoContext::new();
    let guard = CurrentGuard::enter(outer.clone());
    let result = std::panic::catch_unwind(|| {
        let _inner = CurrentGuard::enter(IoContext::new());
        panic!("handler failure");
    });
    assert!(result.is_err());
    assert!(Arc::ptr_eq(&installed_context().unwrap(), &outer));
    drop(guard);
    assert!(installed_context().is_none());
}

#[test]
fn weak_registration_does_not_retain_the_invocation() {
    let registry = Arc::new(ContextRegistry::default());
    let mut context = IoContext::new();
    let id = registry.register(&mut context);
    assert_eq!(context.continuation_id(), Some(id));
    assert!(Arc::ptr_eq(&registry.lookup(id).unwrap(), &context));
    assert!(context.engine.get().is_none());
    drop(context);
    assert!(registry.lookup(id).is_none());
    assert!(registry.0.lock().unwrap().is_empty());
    // Dropping a worker first does not leave a strong callback cycle.
    let mut surviving = IoContext::new();
    registry.register(&mut surviving);
    let weak_registry = Arc::downgrade(&registry);
    drop(registry);
    assert!(weak_registry.upgrade().is_none());
    drop(surviving);
    let ids = (0..8)
        .map(|_| {
            std::thread::spawn(|| {
                (0..1000)
                    .map(|_| allocate_io_context_id())
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    let unique = ids
        .into_iter()
        .flat_map(|ids| ids.join().unwrap())
        .collect::<HashSet<_>>();
    assert_eq!(unique.len(), 8000);
    assert!(!unique.contains(&id));
}

struct PendingOperation {
    context: Weak<IoContext>,
    drops: Arc<AtomicUsize>,
}

impl Future for PendingOperation {
    type Output = Result<asyncrt::OpOut, String>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for PendingOperation {
    fn drop(&mut self) {
        if let Some(context) = self.context.upgrade() {
            assert!(
                context.handed.try_lock().is_ok(),
                "cleanup ran under the mailbox lock"
            );
        }
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn mailbox_wakes_its_driver_and_terminal_retirement_refuses_new_work() {
    let context = IoContext::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let make_op = || {
        Box::pin(PendingOperation {
            context: Arc::downgrade(&context),
            drops: drops.clone(),
        }) as asyncrt::OpFuture
    };
    let mut ready = Box::pin(context.handed_ready());
    let mut task = Context::from_waker(Waker::noop());
    assert!(ready.as_mut().poll(&mut task).is_pending());
    assert!(context.hand_op(1, make_op(), asyncrt::OpLifetime::IoContext));
    assert!(ready.as_mut().poll(&mut task).is_ready());
    assert!(context.has_handed_ops());
    let taken = context.take_handed_ops();
    assert_eq!(taken[0].0, 1);
    assert!(taken[0].2 == asyncrt::OpLifetime::IoContext);
    assert!(!context.has_handed_ops());
    drop(taken);
    assert!(context.hand_op(2, make_op(), asyncrt::OpLifetime::Handler));
    let abandoned = context.close_handed_ops();
    assert_eq!(abandoned[0].0, 2);
    assert!(!context.accepts_handed_ops());
    assert!(!context.has_handed_ops());
    drop(abandoned);
    assert!(!context.hand_op(3, make_op(), asyncrt::OpLifetime::Unrefed));
    assert_eq!(drops.load(Ordering::Relaxed), 3);
    context.register_pending_event(5);
    context.register_pending_event(3);
    context.register_pending_event(4);
    context.finish_pending_event(4);
    assert!(context.has_pending_events());
    assert_eq!(context.take_pending_events(), [3, 5]);
    assert!(!context.has_pending_events());
}

#[test]
fn budget_is_atomic_invocation_local_and_counts_failed_attempts() {
    let context = IoContext::with_resource_limits(Some(ResourceLimits {
        cpu_ms: None,
        sub_requests: Some(16),
    }));
    let callers = (0..8)
        .map(|_| {
            let context = context.clone();
            std::thread::spawn(move || {
                (0..40)
                    .filter(|_| context.charge_subrequest().is_ok())
                    .count()
            })
        })
        .collect::<Vec<_>>();
    let allowed: usize = callers
        .into_iter()
        .map(|caller| caller.join().unwrap())
        .sum();
    assert_eq!(allowed, 16);
    assert_eq!(context.subrequests.load(Ordering::Relaxed), 320);
    assert_eq!(
        context.charge_subrequest(),
        Err("Worker exceeded subrequest limit of 16".into())
    );
    assert_eq!(context.subrequests.load(Ordering::Relaxed), 321);
    let separate = IoContext::with_resource_limits(Some(ResourceLimits {
        cpu_ms: None,
        sub_requests: Some(1),
    }));
    assert!(separate.charge_subrequest().is_ok());
    assert!(separate.charge_subrequest().is_err());
}

thread_local! {
    static CPU_SAMPLES: RefCell<VecDeque<Option<u64>>> = const { RefCell::new(VecDeque::new()) };
}

fn sample_cpu() -> Option<u64> {
    CPU_SAMPLES.with(|samples| samples.borrow_mut().pop_front().expect("CPU sample"))
}

#[test]
fn completed_turns_share_one_budget_without_charging_suspension() {
    let context = IoContext::with_resource_limits(Some(ResourceLimits {
        cpu_ms: Some(1),
        sub_requests: None,
    }));
    context.install_cpu_clock(sample_cpu);
    CPU_SAMPLES.with(|samples| {
        *samples.borrow_mut() = [Some(100), Some(900100), Some(1000000), Some(1100001)].into();
    });
    context.begin_cpu_turn();
    assert!(context.finish_cpu_turn().is_ok());
    assert_eq!(
        context.remaining_cpu_budget(),
        Some(Duration::from_nanos(100000))
    );
    assert!(context.finish_cpu_turn().is_ok());
    context.begin_cpu_turn();
    assert_eq!(
        context.finish_cpu_turn(),
        Err("Worker exceeded CPU limit of 1 ms".into())
    );
    assert_eq!(context.remaining_cpu_budget(), Some(Duration::ZERO));
    CPU_SAMPLES.with(|samples| assert!(samples.borrow().is_empty()));
}

#[test]
fn temporary_directories_are_lazy_and_do_not_cross_invocations() {
    let first = IoContext::new();
    let second = IoContext::new();
    assert!(first.vfs.get().is_none());
    let mut vfs = first.vfs.get_or_init(Default::default).lock().unwrap();
    assert!(matches!(
        vfs.mkdir("/bundle/new", true),
        VfsMkdirResult::Error("EPERM")
    ));
    assert!(matches!(
        vfs.mkdir("/tmp/missing/child", false),
        VfsMkdirResult::Error("ENOENT")
    ));
    assert!(
        matches!(vfs.mkdir("/tmp/parent/child", true), VfsMkdirResult::Created(Some(path)) if path == "/tmp/parent")
    );
    assert_eq!(vfs.stat("/tmp/parent/child"), Some(true));
    assert_eq!(vfs.stat("/bundle"), Some(false));
    assert!(matches!(
        vfs.mkdir("/tmp/parent/child", true),
        VfsMkdirResult::Created(None)
    ));
    drop(vfs);
    assert!(second.vfs.get().is_none());
    assert_eq!(
        second
            .vfs
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .stat("/tmp/parent"),
        None
    );
}

#[test]
fn tail_capture_uses_serialized_bytes_and_stops_at_first_oversized_record() {
    let context = IoContext::with_options(None, true);
    context.record_tail_log("log", "\"quoted\"\\line\n");
    let capture = context.tail_logs.lock().unwrap();
    assert_eq!(
        capture.serialized_bytes,
        serialized_json_bytes(&capture.records[0])
    );
    drop(capture);
    context.record_tail_log("log", &"x".repeat(TAIL_LOG_LIMIT_BYTES));
    context.record_tail_log("log", "later small record");
    let records = context.take_tail_logs();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].message, ["\"quoted\"\\line\n"]);
    assert!(serde_json::to_vec(&records).unwrap().len() <= TAIL_LOG_LIMIT_BYTES);
    assert!(context.take_tail_logs().is_empty());
    let disabled = IoContext::new();
    disabled.record_tail_log("log", "not captured");
    assert!(disabled.take_tail_logs().is_empty());
}

struct RetirementWake {
    context: Weak<IoContext>,
    calls: AtomicUsize,
}

impl Wake for RetirementWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let context = self.context.upgrade().unwrap();
        assert!(
            context.input_gates.try_lock().is_ok(),
            "retirement woke under the ownership lock"
        );
        self.calls.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn input_gate_retirement_drains_exact_claims_and_notifies_outside_ownership_lock() {
    let context = IoContext::new();
    let mut retirement = {
        let mut claims = context.input_gates.lock().unwrap();
        claims.held.insert(3, "cell-c".into());
        claims.held.insert(1, "cell-a".into());
        claims.retirement.subscribe()
    };
    assert!(!context.release_input_gate("wrong", 3));
    let wake = Arc::new(RetirementWake {
        context: Arc::downgrade(&context),
        calls: AtomicUsize::new(0),
    });
    let waker = Waker::from(wake.clone());
    let mut task = Context::from_waker(&waker);
    let mut changed = Box::pin(retirement.changed());
    assert!(changed.as_mut().poll(&mut task).is_pending());
    assert_eq!(
        context.retire_input_gates(),
        [("cell-a".into(), 1), ("cell-c".into(), 3)]
    );
    assert!(changed.as_mut().poll(&mut task).is_ready());
    assert_eq!(wake.calls.load(Ordering::Relaxed), 1);
    assert!(context.retire_input_gates().is_empty());
    assert!(context.input_gates.lock().unwrap().retired);
}

struct ReentrantBackground(Weak<IoContext>, Arc<AtomicUsize>);

impl Drop for ReentrantBackground {
    fn drop(&mut self) {
        let context = self.0.upgrade().unwrap();
        assert!(
            context.events.try_lock().is_ok(),
            "background cleanup ran under the event lock"
        );
        self.1.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn refused_and_abandoned_background_work_drops_after_unlocking_event_state() {
    let context = IoContext::new();
    let drops = Arc::new(AtomicUsize::new(0));
    let make_work = || {
        Box::new(ReentrantBackground(Arc::downgrade(&context), drops.clone()))
            as BackgroundRegistration
    };
    context.register_background(make_work());
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    context.begin_event();
    context.register_background(make_work());
    let initial = context.end_event_registrations().unwrap();
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    drop(initial);
    assert_eq!(drops.load(Ordering::Relaxed), 2);
    context.register_background(make_work());
    context.seal_wait_until();
    assert_eq!(drops.load(Ordering::Relaxed), 3);
    assert!(!context.accepts_wait_until());
}

thread_local! {
    static CLOSED_SOCKETS: RefCell<Vec<u64>> = const { RefCell::new(Vec::new()) };
}

fn close_owned_websockets(owner: &IoContext, sockets: Vec<u64>) {
    if sockets.is_empty() {
        return;
    }
    assert_eq!(owner.continuation_id(), Some(2));
    assert_eq!(installed_context().unwrap().continuation_id(), Some(1));
    assert!(owner.sockets.try_lock().is_ok());
    CLOSED_SOCKETS.with(|closed| closed.borrow_mut().extend(sockets));
}

fn close_owned_tcp(_: &IoContext, socket: u64) {
    CLOSED_SOCKETS.with(|closed| closed.borrow_mut().push(socket));
}

#[test]
fn socket_cleanup_receives_the_retiring_owner_after_its_guard_restores_the_predecessor() {
    CLOSED_SOCKETS.with(|closed| closed.borrow_mut().clear());
    let mut outer = IoContext::new();
    Arc::get_mut(&mut outer)
        .unwrap()
        .set_registration(1, |_| {});
    let guard = CurrentGuard::enter(outer);
    let mut inner = IoContext::new();
    Arc::get_mut(&mut inner)
        .unwrap()
        .set_registration(2, |_| {});
    Arc::get_mut(&mut inner).unwrap().cleanup = OnceLock::from(ContextCleanup {
        retire_input_gates: |_| {},
        close_websockets: close_owned_websockets,
        close_tcp: close_owned_tcp,
    });
    inner.sockets.lock().unwrap().extend([11, 12]);
    inner.tcp_sockets.lock().unwrap().push(13);
    let inner_guard = CurrentGuard::enter(inner);
    drop(inner_guard);
    CLOSED_SOCKETS.with(|closed| assert_eq!(*closed.borrow(), [11, 12, 13]));
    assert_eq!(installed_context().unwrap().continuation_id(), Some(1));
    drop(guard);
    assert!(installed_context().is_none());
}
