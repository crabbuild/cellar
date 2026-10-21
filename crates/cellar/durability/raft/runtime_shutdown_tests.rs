//! A synchronous send racing teardown must relinquish its reply and permit.
#![allow(clippy::disallowed_methods)]
use super::*;
use std::sync::mpsc as sync;
use std::time::Duration;

#[test]
fn receiver_teardown_joins_an_admitted_late_sender() {
    let (sender, receiver) = mpsc::channel(2);
    let sender = Arc::new(Mutex::new(Some(sender)));
    let closing = Arc::new(AtomicBool::new(false));
    let requests = Requests {
        receiver,
        sender: sender.clone(),
        closing: closing.clone(),
    };
    let budget = Budget::new(1024, 1);
    let (reply, mut response) = oneshot::channel();
    let request = Request {
        command: Command::Tick,
        reply,
        _permit: Some(budget.acquire(1024).unwrap()),
        _inbound: None,
    };
    let (entered, admitted) = sync::sync_channel(0);
    let (release, resume) = sync::sync_channel(0);
    let outbound = sender.clone();
    let producer = std::thread::spawn(move || {
        // Model a producer descheduled after admission and channel reservation,
        // before its synchronous send publishes the reply-bearing request.
        let sender = outbound.lock().unwrap();
        entered.send(()).unwrap();
        resume.recv().unwrap();
        assert!(sender.as_ref().unwrap().try_send(request).is_ok());
    });
    admitted.recv_timeout(Duration::from_secs(5)).unwrap();
    let (finished, joined) = sync::sync_channel(1);
    let consumer = std::thread::spawn(move || {
        drop(requests);
        finished.send(()).unwrap();
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !closing.load(Ordering::Acquire) && std::time::Instant::now() < deadline {
        std::thread::yield_now();
    }
    let observed_close = closing.load(Ordering::Acquire);
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
        "shutdown must drop the late reply even with retained runtime handles"
    );
    assert_eq!(budget.usage(), (0, 0));
    assert!(sender.lock().unwrap().is_none());
}
