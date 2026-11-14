// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::budget::Budget;
use cellar_logic::consensus::CommandFailure;
use futures_util::poll;
use std::task::Poll;

fn ordinary_slot(slots: &Arc<Semaphore>) -> Result<OwnedSemaphorePermit, ExecutionError> {
    slots
        .clone()
        .try_acquire_owned()
        .map_err(|error| match error {
            TryAcquireError::Closed => unavailable(),
            TryAcquireError::NoPermits => super::super::overloaded(),
        })
}

#[tokio::test]
async fn apply_then_completion_keeps_fifo_and_existing_capacity() {
    let reserved = Arc::new(Semaphore::new(1));
    let ordinary = Arc::new(Semaphore::new(2));
    let active = reserved.clone().try_acquire_owned().unwrap();
    let budget = Budget::new(64, 1);
    let admission = Arc::new(budget.acquire(8).unwrap());
    let mut apply = Box::pin(reserve(&reserved, || Ok(admission.clone())));
    assert!(poll!(apply.as_mut()).is_pending());
    let mut completion = Box::pin(reserve(&reserved, || ordinary_slot(&ordinary)));
    assert!(poll!(completion.as_mut()).is_pending());
    assert_eq!(ordinary.available_permits(), 1);
    drop(active);
    let applied = apply.await.unwrap();
    assert!(poll!(completion.as_mut()).is_pending());
    assert!(matches!(
        reserved.clone().try_acquire_owned(),
        Err(TryAcquireError::NoPermits)
    ));
    drop(applied);
    let completed = completion.await.unwrap();
    assert_eq!(ordinary.available_permits(), 2);
    assert_eq!(reserved.available_permits(), 0);
    drop(completed);
    assert_eq!(reserved.available_permits(), 1);
    drop(admission);
    assert_eq!(budget.usage(), (0, 0));
}

#[tokio::test]
async fn completion_then_apply_keeps_fifo() {
    let reserved = Arc::new(Semaphore::new(1));
    let ordinary = Arc::new(Semaphore::new(1));
    let active = reserved.clone().try_acquire_owned().unwrap();
    let mut completion = Box::pin(reserve(&reserved, || ordinary_slot(&ordinary)));
    assert!(poll!(completion.as_mut()).is_pending());
    let budget = Budget::new(64, 1);
    let admission = Arc::new(budget.acquire(8).unwrap());
    let mut apply = Box::pin(reserve(&reserved, || Ok(admission.clone())));
    assert!(poll!(apply.as_mut()).is_pending());
    drop(active);
    let completed = completion.await.unwrap();
    assert!(poll!(apply.as_mut()).is_pending());
    drop(completed);
    drop(apply.await.unwrap());
    assert_eq!(reserved.available_permits(), 1);
    assert_eq!(ordinary.available_permits(), 1);
}

#[tokio::test]
async fn cancelled_waiter_releases_ordinary_capacity() {
    let reserved = Arc::new(Semaphore::new(1));
    let ordinary = Arc::new(Semaphore::new(1));
    let active = reserved.clone().try_acquire_owned().unwrap();
    let mut waiting = Box::pin(reserve(&reserved, || ordinary_slot(&ordinary)));
    assert!(poll!(waiting.as_mut()).is_pending());
    assert_eq!(ordinary.available_permits(), 0);
    drop(waiting);
    assert_eq!(ordinary.available_permits(), 1);
    drop(active);
    drop(
        reserve(&reserved, || ordinary_slot(&ordinary))
            .await
            .unwrap(),
    );
    assert_eq!(reserved.available_permits(), 1);
}

#[tokio::test]
async fn waiter_limit_refuses_without_growing_capacity() {
    let reserved = Arc::new(Semaphore::new(1));
    let ordinary = Arc::new(Semaphore::new(1));
    let _active = reserved.clone().try_acquire_owned().unwrap();
    let mut first = Box::pin(reserve(&reserved, || ordinary_slot(&ordinary)));
    assert!(poll!(first.as_mut()).is_pending());
    let mut second = Box::pin(reserve(&reserved, || ordinary_slot(&ordinary)));
    assert!(matches!(
        poll!(second.as_mut()),
        Poll::Ready(Err(ExecutionError::Refused(CommandFailure::Overloaded)))
    ));
    assert_eq!(ordinary.available_permits(), 0);
    assert_eq!(reserved.available_permits(), 0);
    drop(first);
    assert_eq!(ordinary.available_permits(), 1);
}

#[tokio::test]
async fn idle_progress_slot_stays_available_with_a_full_ordinary_queue() {
    let reserved = Arc::new(Semaphore::new(1));
    let ordinary = Arc::new(Semaphore::new(1));
    let _ordinary = ordinary.clone().try_acquire_owned().unwrap();
    let slot = reserve(&reserved, || ordinary_slot(&ordinary))
        .await
        .unwrap();
    assert_eq!(reserved.available_permits(), 0);
    assert_eq!(ordinary.available_permits(), 0);
    drop(slot);
    assert_eq!(reserved.available_permits(), 1);
}

#[tokio::test]
async fn closed_progress_lane_releases_waiter_admission() {
    let reserved = Arc::new(Semaphore::new(1));
    let ordinary = Arc::new(Semaphore::new(1));
    let _active = reserved.clone().try_acquire_owned().unwrap();
    let mut waiting = Box::pin(reserve(&reserved, || ordinary_slot(&ordinary)));
    assert!(poll!(waiting.as_mut()).is_pending());
    reserved.close();
    assert!(matches!(
        waiting.await,
        Err(ExecutionError::Refused(CommandFailure::Unavailable))
    ));
    assert_eq!(ordinary.available_permits(), 1);
}
