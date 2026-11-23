// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use std::{future::Future as _, sync::atomic::AtomicUsize};

#[test]
fn primed_checkpoint_submission_retains_ready_outcomes_for_normal_turn() {
    for failed in [false, true] {
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = polls.clone();
        let mut result = Some(if failed {
            Err(unavailable())
        } else {
            Ok(CheckpointProof::Rejected)
        });
        let mut future: BoxFuture<'static, Result<CheckpointProof, ExecutionError>> =
            Box::pin(futures_util::future::poll_fn(move |_| {
                observed.fetch_add(1, Ordering::Relaxed);
                Poll::Ready(result.take().expect("completed submission re-polled"))
            }));
        let mut cx = Context::from_waker(Waker::noop());
        prime_submission(&mut future, &mut cx);
        assert_eq!(polls.load(Ordering::Relaxed), 1);
        let outcome = future.as_mut().poll(&mut cx);
        if failed {
            assert!(matches!(
                outcome,
                Poll::Ready(Err(ExecutionError::Refused(CommandFailure::Unavailable)))
            ));
        } else {
            assert!(matches!(
                outcome,
                Poll::Ready(Ok(CheckpointProof::Rejected))
            ));
        }
        assert_eq!(
            polls.load(Ordering::Relaxed),
            1,
            "normal turn re-polled completed owner instead of retained outcome"
        );
    }
}

#[test]
fn primed_checkpoint_submission_keeps_pending_owner_and_its_wake() {
    #[derive(Default)]
    struct Notified(AtomicUsize);

    impl Wake for Notified {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }
    let (sender, receiver) = oneshot::channel::<Result<CheckpointProof, ExecutionError>>();
    let mut receiving = Box::pin(async move { receiver.await.map_err(|_| unavailable())? });
    let polls = Arc::new(AtomicUsize::new(0));
    let observed = polls.clone();
    let mut future: BoxFuture<'static, Result<CheckpointProof, ExecutionError>> =
        Box::pin(futures_util::future::poll_fn(move |cx| {
            observed.fetch_add(1, Ordering::Relaxed);
            receiving.as_mut().poll(cx)
        }));
    let notified = Arc::new(Notified::default());
    let waker = Waker::from(notified.clone());
    let mut cx = Context::from_waker(&waker);
    prime_submission(&mut future, &mut cx);
    assert_eq!(polls.load(Ordering::Relaxed), 1);
    assert!(sender.send(Ok(CheckpointProof::Rejected)).is_ok());
    assert!(
        notified.0.load(Ordering::Relaxed) > 0,
        "pending owner lost SQL worker wake"
    );
    assert!(matches!(
        future.as_mut().poll(&mut cx),
        Poll::Ready(Ok(CheckpointProof::Rejected))
    ));
    assert_eq!(polls.load(Ordering::Relaxed), 2);
}
