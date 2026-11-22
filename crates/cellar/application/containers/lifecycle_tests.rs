// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use std::{future::poll_fn, task::Poll};
use tokio::sync::Notify;

#[tokio::test]
async fn background_cannot_overtake_a_foreground_proof() {
    let locks = LifecycleLocks::default();
    let name: Arc<str> = "TOOL".into();
    let held = locks.get(&name, 2).unwrap().lock().await;
    let proof = Notify::new();
    let mut waiting = Box::pin(locks.get(&name, 2).unwrap().lock_shared(|mutex| async {
        proof.notified().await;
        Ok(mutex.lock_owned().await)
    }));
    poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(held);
    assert!(
        locks
            .get(&name, 2)
            .unwrap()
            .try_background(BackgroundAdmission::Routine)
            .is_none(),
        "background bypassed foreground admission while its proof was pending"
    );
    proof.notify_one();
    let admitted = waiting.await.unwrap();
    assert!(locks
        .get(&name, 2)
        .unwrap()
        .try_background(BackgroundAdmission::Routine)
        .is_none());
    drop(admitted);
    assert!(locks
        .get(&name, 2)
        .unwrap()
        .try_background(BackgroundAdmission::Routine)
        .is_some());
}

#[tokio::test]
async fn cancelling_a_foreground_proof_releases_only_its_binding() {
    let locks = LifecycleLocks::default();
    let name: Arc<str> = "TOOL".into();
    let other: Arc<str> = "OTHER".into();
    let held = locks.get(&name, 2).unwrap().lock().await;
    let mut waiting = Box::pin(locks.get(&name, 2).unwrap().lock_shared(|mutex| async {
        std::future::pending::<()>().await;
        Ok(mutex.lock_owned().await)
    }));
    poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(held);
    assert!(locks
        .get(&other, 2)
        .unwrap()
        .try_background(BackgroundAdmission::Routine)
        .is_some());
    assert!(locks
        .get(&name, 2)
        .unwrap()
        .try_background(BackgroundAdmission::Routine)
        .is_none());
    drop(waiting);
    assert!(locks
        .get(&name, 2)
        .unwrap()
        .try_background(BackgroundAdmission::Routine)
        .is_some());
}

#[tokio::test]
async fn failed_proof_and_completed_guards_release_bounded_inventory() {
    let locks = LifecycleLocks::default();
    let name: Arc<str> = "TOOL".into();
    let held = locks.get(&name, 1).unwrap().lock().await;
    let other: Arc<str> = "OTHER".into();
    assert!(locks.get(&other, 1).is_err());
    let result = locks
        .get(&name, 1)
        .unwrap()
        .lock_shared(|_| async { Err(anyhow::anyhow!("original proof failed")) })
        .await;
    assert!(result.is_err());
    drop(held);
    assert!(locks
        .get(&other, 1)
        .unwrap()
        .try_background(BackgroundAdmission::Routine)
        .is_some());
}

#[tokio::test]
async fn one_cancelled_waiter_does_not_release_another_reservation() {
    let locks = LifecycleLocks::default();
    let name: Arc<str> = "TOOL".into();
    let held = locks.get(&name, 1).unwrap().lock().await;
    let park = |mutex: Arc<AsyncMutex<()>>| async {
        std::future::pending::<()>().await;
        Ok(mutex.lock_owned().await)
    };
    let mut first = Box::pin(locks.get(&name, 1).unwrap().lock_shared(park));
    let mut second = Box::pin(locks.get(&name, 1).unwrap().lock_shared(park));
    poll_fn(|cx| {
        assert!(first.as_mut().poll(cx).is_pending());
        assert!(second.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(held);
    drop(first);
    assert!(locks
        .get(&name, 1)
        .unwrap()
        .try_background(BackgroundAdmission::Routine)
        .is_none());
    drop(second);
    assert!(locks
        .get(&name, 1)
        .unwrap()
        .try_background(BackgroundAdmission::Routine)
        .is_some());
}

#[tokio::test]
async fn required_cleanup_obeys_live_guards_but_not_a_pending_proof() {
    let locks = LifecycleLocks::default();
    let name: Arc<str> = "TOOL".into();
    let held = locks.get(&name, 1).unwrap().lock().await;
    let proof = Notify::new();
    let mut waiting = Box::pin(locks.get(&name, 1).unwrap().lock_shared(|mutex| async {
        proof.notified().await;
        Ok(mutex.lock_owned().await)
    }));
    poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(locks
        .get(&name, 1)
        .unwrap()
        .try_background(BackgroundAdmission::Required)
        .is_none());
    drop(held);
    assert!(locks
        .get(&name, 1)
        .unwrap()
        .try_background(BackgroundAdmission::Routine)
        .is_none());
    let cleanup = locks
        .get(&name, 1)
        .unwrap()
        .try_background(BackgroundAdmission::Required)
        .expect("required cleanup must progress while only a foreground proof is pending");
    proof.notify_one();
    poll_fn(|cx| {
        assert!(waiting.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(cleanup);
    let foreground = waiting.await.unwrap();
    assert!(locks
        .get(&name, 1)
        .unwrap()
        .try_background(BackgroundAdmission::Required)
        .is_none());
    drop(foreground);
    assert!(locks
        .get(&name, 1)
        .unwrap()
        .try_background(BackgroundAdmission::Routine)
        .is_some());
}

#[test]
fn maintenance_priority_preserves_expiry_when_wall_time_moves_backwards() {
    assert_eq!(
        BackgroundAdmission::for_maintenance(true, 20, 10, 15),
        BackgroundAdmission::Routine
    );
    assert_eq!(
        BackgroundAdmission::for_maintenance(true, 20, 20, 15),
        BackgroundAdmission::Required
    );
    assert_eq!(
        BackgroundAdmission::for_maintenance(true, 20, 5, 20),
        BackgroundAdmission::Required
    );
    assert_eq!(
        BackgroundAdmission::for_maintenance(false, 20, 10, 15),
        BackgroundAdmission::Required
    );
}
