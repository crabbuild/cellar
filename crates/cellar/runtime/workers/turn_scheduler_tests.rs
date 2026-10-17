use super::{TurnLane, TurnScheduler};
use futures_util::FutureExt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::task::{Context, Wake, Waker};

fn cell(name: &str) -> TurnLane {
    TurnLane::Cell(name.to_owned())
}

#[test]
fn granting_a_turn_wakes_its_waiter_outside_the_scheduler_lock() {
    type Waiting = Pin<Box<dyn Future<Output = ()> + Send>>;

    struct Probe {
        scheduler: Arc<TurnScheduler>,
        locked: AtomicBool,
        cancelled: AtomicBool,
        waiting: Mutex<Option<Waiting>>,
    }

    impl Wake for Probe {
        fn wake(self: Arc<Self>) {
            if self.scheduler.schedule.try_lock().is_err() {
                // Report the regression instead of deadlocking this test.
                self.locked.store(true, Ordering::SeqCst);
                return;
            }
            let waiting = self.waiting.lock().unwrap().take();
            drop(waiting);
            self.cancelled.store(true, Ordering::SeqCst);
        }
    }

    let scheduler = Arc::new(TurnScheduler::default());
    let owner = scheduler.acquire(cell("owner")).now_or_never().unwrap();
    let queued = scheduler.clone();
    let mut waiting: Waiting = Box::pin(async move {
        drop(queued.acquire(cell("waiting")).await);
    });
    let probe = Arc::new(Probe {
        scheduler: scheduler.clone(),
        locked: AtomicBool::new(false),
        cancelled: AtomicBool::new(false),
        waiting: Mutex::new(None),
    });
    let waker = Waker::from(probe.clone());
    assert!(waiting
        .as_mut()
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    *probe.waiting.lock().unwrap() = Some(waiting);
    let mut successor = Box::pin(scheduler.acquire(TurnLane::Stateless));
    assert!(successor.as_mut().now_or_never().is_none());
    drop(owner);
    // Break the retained future/waker cycle even when the lock probe failed.
    let waiting = probe.waiting.lock().unwrap().take();
    drop(waiting);
    assert!(
        !probe.locked.load(Ordering::SeqCst),
        "grant notification held the scheduler lock"
    );
    assert!(probe.cancelled.load(Ordering::SeqCst));
    drop(successor.as_mut().now_or_never().unwrap());
    assert!(scheduler.acquire(cell("after")).now_or_never().is_some());
}

#[test]
fn cancelling_a_granted_turn_before_its_next_poll_releases_the_lane() {
    for lane in [TurnLane::Stateless, cell("first"), cell("second")] {
        let scheduler = TurnScheduler::default();
        let owner = scheduler.acquire(cell("first")).now_or_never().unwrap();
        let mut cancelled = Box::pin(scheduler.acquire(lane.clone()));
        let mut successor = Box::pin(scheduler.acquire(cell("third")));
        assert!(cancelled.as_mut().now_or_never().is_none());
        assert!(successor.as_mut().now_or_never().is_none());

        // The notification has been sent successfully, but acquire has not
        // polled it yet. Cancelling here must pass on the granted turn.
        drop(owner);
        drop(cancelled);
        let next = successor
            .as_mut()
            .now_or_never()
            .expect("cancellation stranded the granted turn");
        drop(next);
        let schedule = scheduler.schedule.lock().unwrap();
        assert!(schedule.current.is_none());
        assert!(schedule.ready.is_empty());
        assert!(schedule.waiting.is_empty());
    }
}

#[test]
fn cancelling_the_only_granted_waiter_allows_new_turns() {
    let scheduler = TurnScheduler::default();
    let owner = scheduler
        .acquire(TurnLane::Stateless)
        .now_or_never()
        .unwrap();
    let mut cancelled = Box::pin(scheduler.acquire(cell("waiting")));
    assert!(cancelled.as_mut().now_or_never().is_none());
    drop(owner);
    drop(cancelled);
    assert!(scheduler.acquire(cell("new")).now_or_never().is_some());
}

#[test]
fn receiving_a_grant_retains_exclusive_ownership_until_permit_drop() {
    let scheduler = TurnScheduler::default();
    let owner = scheduler.acquire(cell("owner")).now_or_never().unwrap();
    let mut first = Box::pin(scheduler.acquire(cell("first")));
    let mut second = Box::pin(scheduler.acquire(cell("second")));
    assert!(first.as_mut().now_or_never().is_none());
    assert!(second.as_mut().now_or_never().is_none());
    drop(owner);
    let permit = first.as_mut().now_or_never().unwrap();
    drop(first);
    assert!(second.as_mut().now_or_never().is_none());
    drop(permit);
    drop(second.as_mut().now_or_never().unwrap());
    assert!(scheduler
        .acquire(TurnLane::Stateless)
        .now_or_never()
        .is_some());
}

#[test]
fn cancelled_waiters_preserve_round_robin_and_same_lane_fifo() {
    let scheduler = TurnScheduler::default();
    let owner = scheduler.acquire(cell("hot")).now_or_never().unwrap();
    let mut cancelled = Box::pin(scheduler.acquire(cell("hot")));
    let mut hot_first = Box::pin(scheduler.acquire(cell("hot")));
    let mut hot_second = Box::pin(scheduler.acquire(cell("hot")));
    let mut cold = Box::pin(scheduler.acquire(cell("cold")));
    for waiter in [&mut cancelled, &mut hot_first, &mut hot_second, &mut cold] {
        assert!(waiter.as_mut().now_or_never().is_none());
    }
    // Skip a cancelled queued waiter, give the other cell its turn, and then
    // keep this cell's FIFO even when its first granted waiter is cancelled.
    drop(cancelled);
    drop(owner);
    assert!(hot_first.as_mut().now_or_never().is_none());
    assert!(hot_second.as_mut().now_or_never().is_none());
    drop(cold.as_mut().now_or_never().unwrap());
    assert!(hot_second.as_mut().now_or_never().is_none());
    drop(hot_first);
    drop(hot_second.as_mut().now_or_never().unwrap());
    assert!(scheduler
        .acquire(TurnLane::Stateless)
        .now_or_never()
        .is_some());
}

#[test]
fn cancellation_racing_grant_keeps_the_next_waiter_runnable() {
    for _ in 0..256 {
        let scheduler = TurnScheduler::default();
        let owner = scheduler.acquire(cell("owner")).now_or_never().unwrap();
        let mut cancelled = Box::pin(scheduler.acquire(cell("cancelled")));
        let mut successor = Box::pin(scheduler.acquire(TurnLane::Stateless));
        assert!(cancelled.as_mut().now_or_never().is_none());
        assert!(successor.as_mut().now_or_never().is_none());
        std::thread::scope(|scope| {
            let grant = scope.spawn(|| drop(owner));
            drop(cancelled);
            grant.join().unwrap();
        });
        drop(
            successor
                .as_mut()
                .now_or_never()
                .expect("lost racing grant"),
        );
        assert!(scheduler.acquire(cell("after")).now_or_never().is_some());
    }
}
