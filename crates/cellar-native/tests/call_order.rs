// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use cellar_native::call_order::*;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
};

#[derive(Default)]
struct Count(AtomicUsize);

impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn poll<F: Future + Unpin>(f: &mut F) -> Poll<F::Output> {
    Pin::new(f).poll(&mut Context::from_waker(Waker::noop()))
}

#[test]
fn delivered_calls_release_in_order_and_callers_and_cells_are_independent() {
    let chains = CallChains::default();
    let other = CallChains::default();
    let mut a = chains.enter("Cell:a");
    let mut b = chains.enter("Cell:a");
    let mut unrelated = chains.enter("Cell:b");
    let mut caller = other.enter("Cell:a");
    assert!(poll(&mut a.wait()).is_ready());
    assert!(poll(&mut b.wait()).is_pending());
    assert!(poll(&mut unrelated.wait()).is_ready());
    assert!(poll(&mut caller.wait()).is_ready());
    assert_eq!(chains.pending_cells(), 2);
    a.delivered();
    drop(a);
    assert!(poll(&mut b.wait()).is_ready());
    drop(b);
    drop(unrelated);
    drop(caller);
    assert_eq!(chains.pending_cells(), 0);
    assert_eq!(other.pending_cells(), 0);
}

#[test]
fn cancelled_wait_preserves_predecessor_for_the_successor_and_for_a_retry() {
    let chains = CallChains::default();
    let mut a = chains.enter("a");
    let mut b = chains.enter("a");
    let mut c = chains.enter("a");
    let mut wait = b.wait();
    assert!(poll(&mut wait).is_pending());
    drop(wait);
    assert!(poll(&mut b.wait()).is_pending());
    drop(b);
    assert!(poll(&mut c.wait()).is_pending());
    a.delivered();
    drop(a);
    assert!(poll(&mut c.wait()).is_ready());
    c.delivered();
    drop(c);
    assert_eq!(chains.pending_cells(), 0);
    let mut first = chains.enter("a");
    let cancelled_tail = chains.enter("a");
    drop(cancelled_tail);
    let mut later = chains.enter("a");
    assert!(
        poll(&mut later.wait()).is_pending(),
        "dropping the tail must not erase its still-live predecessor"
    );
    first.delivered();
    drop(first);
    assert!(poll(&mut later.wait()).is_ready());
    drop(later);
    assert_eq!(chains.pending_cells(), 0);
}

#[test]
fn waits_replace_and_release_wakers_instead_of_retaining_cancelled_tasks() {
    let chains = CallChains::default();
    let a = chains.enter("a");
    let mut b = chains.enter("a");
    let counter = Arc::new(Count::default());
    let weak = Arc::downgrade(&counter);
    let waker = Waker::from(counter.clone());
    let mut cx = Context::from_waker(&waker);
    let mut wait = b.wait();
    for _ in 0..100 {
        assert!(Pin::new(&mut wait).poll(&mut cx).is_pending());
    }
    assert_eq!(Arc::strong_count(&counter), 3);
    drop(wait);
    assert_eq!(Arc::strong_count(&counter), 2);
    drop(waker);
    drop(counter);
    assert!(weak.upgrade().is_none());
    drop(a);
    assert!(poll(&mut b.wait()).is_ready());
}

#[test]
fn opaque_event_owners_survive_until_token_drop_and_reentry_runs_outside_chain_locks() {
    struct Owner(CallChains, Arc<AtomicUsize>);

    impl Drop for Owner {
        fn drop(&mut self) {
            let _nested = self.0.enter("nested");
            self.1.fetch_add(1, Ordering::SeqCst);
        }
    }
    let chains = CallChains::default();
    let drops = Arc::new(AtomicUsize::new(0));
    let owner = Arc::new(Owner(chains.clone(), drops.clone()));
    let weak = Arc::downgrade(&owner);
    let order = chains.enter_with_owner("a", owner.clone());
    drop(owner);
    assert!(weak.upgrade().is_some());
    drop(order);
    assert!(weak.upgrade().is_none());
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(chains.pending_cells(), 0);

    struct Reentrant(CallChains, Arc<AtomicUsize>);

    impl Wake for Reentrant {
        fn wake(self: Arc<Self>) {
            let _ = self.0.pending_cells();
            self.1.fetch_add(1, Ordering::SeqCst);
        }
    }
    let a = chains.enter("a");
    let mut b = chains.enter("a");
    let calls = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(Reentrant(chains, calls.clone())));
    let mut wait = b.wait();
    assert!(Pin::new(&mut wait)
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    drop(a);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(poll(&mut wait).is_ready());
}

#[test]
fn long_failed_prefixes_yield_bounded_work_then_park_without_spinning() {
    let chains = CallChains::default();
    let mut first = chains.enter("a");
    let failed: Vec<_> = (0..512).map(|_| chains.enter("a")).collect();
    let mut last = chains.enter("a");
    for order in failed {
        drop(order);
    }
    let calls = Arc::new(Count::default());
    let waker = Waker::from(calls.clone());
    let mut cx = Context::from_waker(&waker);
    let mut wait = last.wait();
    for _ in 0..9 {
        assert!(Pin::new(&mut wait).poll(&mut cx).is_pending());
    }
    let yielded = calls.0.load(Ordering::SeqCst);
    assert_eq!(yielded, 8);
    for _ in 0..20 {
        assert!(Pin::new(&mut wait).poll(&mut cx).is_pending());
    }
    assert_eq!(calls.0.load(Ordering::SeqCst), yielded);
    first.delivered();
    drop(first);
    assert!(poll(&mut wait).is_ready());
    drop(wait);
    drop(last);
    assert_eq!(chains.pending_cells(), 0);
}

#[test]
fn abandoned_chains_reclaim_iteratively_on_small_host_stacks() {
    std::thread::Builder::new()
        .stack_size(128 * 1024)
        .spawn(|| {
            let chains = CallChains::default();
            let calls: Vec<_> = (0..100_000).map(|_| chains.enter("a")).collect();
            for call in calls {
                drop(call);
            }
            assert_eq!(chains.pending_cells(), 0);
        })
        .unwrap()
        .join()
        .unwrap();
}
