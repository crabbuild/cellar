// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use futures_util::FutureExt;

#[tokio::test]
async fn tail_refusal_keeps_real_credit_with_its_last_owner() {
    let pool = super::super::CellGroupArchiveTailPool::new(Default::default()).unwrap();
    let held = pool.admit().unwrap();
    let capture = Capture::new();
    let mut observed = pool.clone();
    observed.capture_capacity(capture.clone());
    assert!(matches!(
        observed.admit(),
        Err(super::super::CellGroupArchiveError::Busy)
    ));
    let mut released = Box::pin(capture.take().unwrap().released());
    assert!(released.as_mut().now_or_never().is_none());
    assert_eq!(pool.usage().operations, 1);
    let retained = held.clone();
    drop(held);
    assert!(released.as_mut().now_or_never().is_none());
    assert_eq!(pool.usage().operations, 1);
    drop(retained);
    released.await;
    assert_eq!(pool.usage().operations, 0);
    // A wake observes credit; it does not reserve or manufacture a slot.
    let competing = pool.admit().unwrap();
    assert!(matches!(
        observed.admit(),
        Err(super::super::CellGroupArchiveError::Busy)
    ));
    drop(competing);
}

#[tokio::test]
async fn partial_byte_release_does_not_wake_an_unfitting_request() {
    let bank = Budget::new(10, 3);
    let capture = Capture::new();
    let mut held = bank.acquire(8).unwrap();
    assert!(capture.acquire(&bank, 4).is_none());
    let mut released = Box::pin(capture.take().unwrap().released());
    assert!(released.as_mut().now_or_never().is_none());
    assert!(held.shrink_to(7));
    assert!(released.as_mut().now_or_never().is_none());
    assert!(held.shrink_to(6));
    released.await;
    assert_eq!(bank.usage(), (6, 1));
}

#[tokio::test]
async fn release_before_registration_survives_a_consumed_signal() {
    let bank = Budget::new(10, 1);
    let capture = Capture::new();
    let held = bank.acquire(1).unwrap();
    assert!(capture.acquire(&bank, 1).is_none());
    let wait = capture.take().unwrap();
    drop(held);
    assert!(bank.wake.notified().now_or_never().is_some());
    assert!(wait.released().now_or_never().is_some());
    assert_eq!(bank.usage(), (0, 0));
}

#[tokio::test]
async fn a_wait_hint_does_not_keep_a_closed_bank_alive() {
    let bank = Budget::new(10, 1);
    let capture = Capture::new();
    let held = bank.acquire(1).unwrap();
    assert!(capture.acquire(&bank, 1).is_none());
    let wait = capture.take().unwrap();
    let weak = Arc::downgrade(&bank);
    drop((held, bank));
    assert!(weak.upgrade().is_none());
    assert!(wait.released().now_or_never().is_some());
}
