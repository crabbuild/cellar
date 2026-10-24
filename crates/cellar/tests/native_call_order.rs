// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use anyhow::Result;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Waker},
};

#[tokio::test]
async fn cancelled_wait_hands_its_predecessor_to_the_next_call() -> Result<()> {
    cellar::asyncrt::set_host_handle(tokio::runtime::Handle::current());
    let caller = cellar::host_io::IoContext::new();
    let mut first = cellar::host_channels::enter_call_order(caller.clone(), "Cell:one");
    let mut middle = cellar::host_channels::enter_call_order(caller.clone(), "Cell:one");
    let mut last = cellar::host_channels::enter_call_order(caller, "Cell:one");
    let mut cx = Context::from_waker(Waker::noop());
    let mut wait = Box::pin(middle.wait());
    assert!(Pin::new(&mut wait).poll(&mut cx).is_pending());
    drop(wait);
    drop(middle);
    let mut wait = Box::pin(last.wait());
    assert!(
        Pin::new(&mut wait).poll(&mut cx).is_pending(),
        "a cancelled middle call must not let its successor overtake the first"
    );
    first.delivered();
    drop(first);
    assert!(Pin::new(&mut wait).poll(&mut cx).is_ready());
    drop(wait);
    last.delivered();
    Ok(())
}

#[test]
fn host_cancellation_tokens_retain_encoding_disarm_and_drop_cleanup() {
    use cellar::host_channels::{begin_call_cancellation, cancel_call};
    let mut identities = std::collections::HashSet::new();
    for _ in 0..100 {
        let (id, mut receive, mut guard) = begin_call_cancellation();
        assert!(identities.insert(id));
        assert_eq!(
            cellar::engine_api::parse_request_id(&cellar::engine_api::request_id_string(id)),
            Some(id)
        );
        assert!(Pin::new(&mut receive)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        assert!(cancel_call(id));
        assert!(!cancel_call(id));
        assert!(Pin::new(&mut receive)
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready());
        guard.disarm();
        let (id, mut receive, mut guard) = begin_call_cancellation();
        guard.disarm();
        assert!(!cancel_call(id));
        assert!(matches!(
            Pin::new(&mut receive).poll(&mut Context::from_waker(Waker::noop())),
            std::task::Poll::Ready(Err(_))
        ));
        let (id, mut receive, guard) = begin_call_cancellation();
        drop(guard);
        assert!(!cancel_call(id));
        assert!(matches!(
            Pin::new(&mut receive).poll(&mut Context::from_waker(Waker::noop())),
            std::task::Poll::Ready(Ok(()))
        ));
    }
}

#[test]
fn cancel_and_disarm_notify_reentrant_receivers_outside_the_registry_lock() {
    use cellar::host_channels::{begin_call_cancellation, cancel_call};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    struct Reentrant {
        missing: cellar::engine_api::RequestId,
        calls: Arc<AtomicUsize>,
    }

    impl std::task::Wake for Reentrant {
        fn wake(self: Arc<Self>) {
            assert!(!cancel_call(self.missing));
            self.calls.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (missing, _, mut finished) = begin_call_cancellation();
    finished.disarm();
    for mode in 0..3 {
        let (id, mut receive, mut guard) = begin_call_cancellation();
        let calls = Arc::new(AtomicUsize::new(0));
        let waker = Waker::from(Arc::new(Reentrant {
            missing,
            calls: calls.clone(),
        }));
        assert!(Pin::new(&mut receive)
            .poll(&mut Context::from_waker(&waker))
            .is_pending());
        match mode {
            0 => {
                assert!(cancel_call(id));
            }
            1 => guard.disarm(),
            _ => drop(guard),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!cancel_call(id));
    }
}
