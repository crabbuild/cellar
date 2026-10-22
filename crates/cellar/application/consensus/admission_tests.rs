#![allow(clippy::disallowed_methods)]
use super::*;
use std::{
    sync::{mpsc, Arc},
    time::Duration,
};

#[test]
fn seal_waits_for_prior_work_registration() {
    let admission = Arc::new(Admission::default());
    let work = Arc::new(crate::host_services::ApplicationWork::default());
    // Model a real caller preempted immediately before entering ApplicationWork,
    // after it has acquired the final request-registration barrier.
    let registration = admission.registration(false).unwrap();
    let (done, completed) = mpsc::channel();
    let sealer = admission.clone();
    let thread = std::thread::spawn(move || done.send(sealer.seal()).unwrap());
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !admission.is_draining() {
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert!(matches!(
        completed.try_recv(),
        Err(mpsc::TryRecvError::Empty)
    ));
    assert_eq!(work.snapshot().pending_events, 0);
    let prior = work.enter();
    drop(registration);
    completed
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .unwrap();
    thread.join().unwrap();
    assert_eq!(work.snapshot().pending_events, 1);
    assert!(matches!(
        admission.registration(false),
        Err(CommandFailure::Unavailable)
    ));
    let cleanup = admission.registration(true).unwrap();
    let cleanup_work = work.enter();
    drop(cleanup);
    assert_eq!(work.snapshot().pending_events, 2);
    drop(prior);
    drop(cleanup_work);
    assert_eq!(work.snapshot().pending_events, 0);
}

#[test]
fn poisoned_registration_never_grants_new_work() {
    let admission = Arc::new(Admission::default());
    let owner = admission.clone();
    assert!(std::thread::spawn(move || {
        let _registration = owner.registration(false).unwrap();
        panic!("injected registration panic");
    })
    .join()
    .is_err());
    assert!(admission.is_poisoned());
    assert!(matches!(admission.seal(), Err(CommandFailure::Unavailable)));
    assert!(matches!(
        admission.registration(false),
        Err(CommandFailure::Unavailable)
    ));
    assert!(matches!(
        admission.registration(true),
        Err(CommandFailure::Unavailable)
    ));
}
