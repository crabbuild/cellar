//! Real channel reservations exercise the send/teardown ownership boundary.
#![allow(clippy::disallowed_methods)]
use super::*;
use crate::raft::budget::Budget;
use std::sync::mpsc as sync;
use std::time::Duration;
use tokio::sync::oneshot;

#[test]
fn receiver_teardown_joins_an_admitted_late_sender() {
    let (sender, receiver) = channel(2);
    let budget = Budget::new(1024, 1);
    let (reply, mut response) = oneshot::channel::<()>();
    let payload = (reply, budget.acquire(1024).unwrap());
    let (entered, admitted) = sync::sync_channel(0);
    let (release, resume) = sync::sync_channel(0);
    let outbound = sender.clone();
    let producer = std::thread::spawn(move || {
        let sender = outbound.state.sender.lock().unwrap();
        // Pause inside the synchronous reserve/publish operation. Raw Tokio
        // reservations remain sendable after receiver destruction begins.
        let reservation = sender.as_ref().unwrap().try_reserve().unwrap();
        entered.send(()).unwrap();
        resume.recv().unwrap();
        reservation.send(payload);
    });
    admitted.recv_timeout(Duration::from_secs(5)).unwrap();
    let (finished, joined) = sync::sync_channel(1);
    let consumer = std::thread::spawn(move || {
        drop(receiver);
        finished.send(()).unwrap();
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !sender.state.closed.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    let observed_close = sender.state.closed.load(Ordering::Acquire);
    let early = joined.recv_timeout(Duration::from_millis(30));
    release.send(()).unwrap();
    producer.join().unwrap();
    consumer.join().unwrap();
    assert!(observed_close, "receiver must close admission");
    assert!(
        matches!(early, Err(sync::RecvTimeoutError::Timeout)),
        "teardown must join the in-flight send"
    );
    joined.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        matches!(
            response.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ),
        "a retained sender must not retain the late reply"
    );
    assert_eq!(budget.usage(), (0, 0));
    assert!(matches!(
        sender.try_send((oneshot::channel().0, budget.acquire(1024).unwrap())),
        Err(mpsc::error::TrySendError::Closed(_))
    ));
    assert_eq!(budget.usage(), (0, 0));
}

#[tokio::test]
async fn receiver_does_not_retain_its_producers() {
    let (sender, mut receiver) = channel::<u64>(1);
    let clone = sender.clone();
    sender.try_send(7).unwrap();
    drop(sender);
    assert_eq!(receiver.recv().await, Some(7));
    drop(clone);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), receiver.recv())
            .await
            .unwrap(),
        None
    );
}
