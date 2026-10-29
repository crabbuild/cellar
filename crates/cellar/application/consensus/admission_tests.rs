// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

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
    let registration = admission.registration(Access::Application).unwrap();
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
        admission.registration(Access::Application),
        Err(CommandFailure::Unavailable)
    ));
    let cleanup = admission.registration(Access::Cleanup).unwrap();
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
        let _registration = owner.registration(Access::Application).unwrap();
        panic!("injected registration panic");
    })
    .join()
    .is_err());
    assert!(admission.is_poisoned());
    assert!(matches!(admission.seal(), Err(CommandFailure::Unavailable)));
    assert!(matches!(
        admission.registration(Access::Application),
        Err(CommandFailure::Unavailable)
    ));
    assert!(matches!(
        admission.registration(Access::Cleanup),
        Err(CommandFailure::Unavailable)
    ));
}

#[test]
fn maintenance_reopens_application_without_resetting_permanent_shutdown() {
    use crate::raft::fleet_stop::{
        BootBinding, LifecyclePurpose, MaintenanceAdmission, StopSelector,
    };
    let gate = Arc::new(MaintenanceAdmission::default());
    let admission = Admission::default();
    admission.bind_maintenance(gate.clone()).unwrap();
    let selector = StopSelector {
        purpose: LifecyclePurpose::Maintenance,
        boot: BootBinding {
            catalog: [1; 32],
            fleet: [2; 16],
            node: [3; 16],
            disk: [4; 16],
            engine: [5; 16],
            boot: [6; 16],
            process: [7; 16],
            epoch: 1,
            join_key: [8; 32],
        },
        epoch: 2,
        operation: [9; 16],
        finalization: [10; 16],
        request: [11; 16],
    };
    gate.begin(&selector).unwrap();
    assert!(admission.application_paused());
    // An accepted HTTP handler can still register its final SDK write.
    drop(admission.registration(Access::Application).unwrap());
    gate.seal_raft(&selector).unwrap();
    admission.barrier().unwrap();
    assert!(matches!(
        admission.registration(Access::Application),
        Err(CommandFailure::Unavailable)
    ));
    drop(admission.registration(Access::Control).unwrap());
    gate.fence(&selector).unwrap();
    assert!(!admission.application_paused());
    drop(admission.registration(Access::Application).unwrap());
    assert!(gate.begin(&selector).is_err());
    let mut next = selector.clone();
    next.epoch += 1;
    gate.begin(&next).unwrap();
    gate.seal_raft(&next).unwrap();
    admission.seal().unwrap();
    gate.fence(&next).unwrap();
    assert!(admission.is_draining());
    assert!(admission.application_paused());
    assert!(matches!(
        admission.registration(Access::Application),
        Err(CommandFailure::Unavailable)
    ));
    assert!(matches!(
        admission.registration(Access::Control),
        Err(CommandFailure::Unavailable)
    ));
    drop(admission.registration(Access::Cleanup).unwrap());
}
