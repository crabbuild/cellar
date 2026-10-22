use super::*;
use std::{
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

#[test]
fn coalesces_without_blocking_and_rearms_after_drain() {
    let wakeup = Wakeup::new().unwrap();
    for _ in 0..100_000 {
        wakeup.notify();
    }
    let mut bytes = [0; 256];
    assert_eq!((&wakeup.reader).read(&mut bytes).unwrap(), 1);
    wakeup.drain().unwrap();
    wakeup.notify();
    assert_eq!((&wakeup.reader).read(&mut bytes).unwrap(), 1);
}

#[test]
fn producers_racing_drain_cannot_strand_work() {
    producers_racing_drain(false);
}

#[test]
fn retirements_racing_drain_cannot_strand_work() {
    producers_racing_drain(true);
}

fn producers_racing_drain(retirement: bool) {
    let wakeup = Wakeup::new().unwrap();
    let (sender, receiver) = mpsc::channel();
    thread::scope(|scope| {
        for _ in 0..4 {
            let sender = sender.clone();
            let wakeup = &wakeup;
            scope.spawn(move || {
                for _ in 0..10_000 {
                    sender.send(()).unwrap();
                    if retirement {
                        wakeup.retire();
                    } else {
                        wakeup.notify();
                    }
                    thread::yield_now();
                }
            });
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut received = 0;
        while received < 40_000 {
            // Scan the work queue ONLY after a real readiness byte. Polling
            // the queue on a timer would mask a lost notification.
            let mut byte = [0];
            match (&wakeup.reader).read(&mut byte) {
                Ok(1) => {
                    wakeup.drain().unwrap();
                    if !retirement || wakeup.take_retirements() {
                        received += receiver.try_iter().count();
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < deadline, "stranded work: {received}/40000");
                    thread::sleep(Duration::from_micros(50));
                }
                other => panic!("unexpected notification read: {other:?}"),
            }
        }
    });
}

#[test]
fn ordinary_wakeups_do_not_require_owner_scans() {
    let wakeup = Wakeup::new().unwrap();
    wakeup.notify();
    wakeup.drain().unwrap();
    assert!(!wakeup.take_retirements());
    for _ in 0..100_000 {
        wakeup.retire();
    }
    assert!(wakeup.take_retirements());
    assert!(!wakeup.take_retirements());
    // This retirement coalesces with the previous unread byte, but must still
    // be visible after the consumer has taken the earlier scan's hint.
    wakeup.retire();
    wakeup.drain().unwrap();
    assert!(wakeup.take_retirements());
    assert!(!wakeup.take_retirements());
}

#[test]
fn notification_failure_is_observable() {
    let wakeup = Wakeup::new().unwrap();
    wakeup.writer.shutdown(std::net::Shutdown::Write).unwrap();
    wakeup.notify();
    assert!(wakeup.check().is_err());
    assert!(wakeup.drain().is_err());
}
