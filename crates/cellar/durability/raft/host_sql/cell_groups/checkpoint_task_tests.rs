// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use std::sync::atomic::AtomicUsize;

struct Custody(Arc<AtomicUsize>);

impl Drop for Custody {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn cancellation_before_first_poll_returns_custody_without_dropping_it() {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let drops = Arc::new(AtomicUsize::new(0));
    let slot = Arc::new(Mutex::new(None));
    let guard = ReturnToOwner {
        work: Some(Custody(drops.clone())),
        slot: slot.clone(),
    };
    let mut task = domain
        .spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        })
        .unwrap();
    task.abort();
    assert!((&mut task).await.is_err());
    assert!(task.was_cancelled());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    let returned = slot
        .lock()
        .unwrap()
        .take()
        .expect("returned unpolled custody");
    drop(returned);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    domain.shutdown().await;
}

#[tokio::test]
async fn cancelling_driver_returns_entered_io_without_blocking_async_executor() {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let (entered, started) = oneshot::channel();
    let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
    let held = gate.clone();
    let drops = Arc::new(AtomicUsize::new(0));
    let physical_custody = Custody(drops.clone());
    let physical = domain
        .blocking(move || {
            let _custody = physical_custody;
            let _ = entered.send(());
            let (lock, wake) = &*held;
            let mut released = lock.lock().unwrap();
            while !*released {
                released = wake.wait(released).unwrap();
            }
        })
        .unwrap();
    started.await.unwrap();
    let slot = Arc::new(Mutex::new(None));
    let mut guard = ReturnToOwner {
        work: Some(physical),
        slot: slot.clone(),
    };
    let (polling, polled) = oneshot::channel();
    let mut driver = domain
        .spawn(async move {
            let task = guard.work.as_mut().unwrap();
            let _ = polling.send(());
            task.await
        })
        .unwrap();
    polled.await.unwrap();
    driver.abort();
    // This test uses one async thread: a blocking destructor would prevent
    // this join and the gate release below from ever running.
    assert!((&mut driver).await.is_err());
    assert!(driver.was_cancelled());
    assert_eq!(drops.load(Ordering::SeqCst), 0, "entered I/O lost custody");
    let mut physical = slot.lock().unwrap().take().expect("returned entered I/O");
    assert!(Pin::new(&mut physical)
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    physical.await.unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    domain.shutdown().await;
}

#[tokio::test]
async fn panicking_driver_returns_custody_and_preserves_panic_classification() {
    let domain = crate::asyncrt::ExecutionDomain::new(tokio::runtime::Handle::current());
    let drops = Arc::new(AtomicUsize::new(0));
    let slot = Arc::new(Mutex::new(None));
    let guard = ReturnToOwner {
        work: Some(Custody(drops.clone())),
        slot: slot.clone(),
    };
    let mut driver = domain
        .spawn(async move {
            let _guard = guard;
            panic!("checkpoint driver fixture");
        })
        .unwrap();
    assert!((&mut driver).await.is_err());
    assert!(!driver.was_cancelled());
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    drop(slot.lock().unwrap().take().expect("returned panic custody"));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    domain.shutdown().await;
}
