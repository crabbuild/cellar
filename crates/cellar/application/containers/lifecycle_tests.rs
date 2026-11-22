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

#[tokio::test]
async fn inspection_yields_after_its_proof_to_a_late_foreground_waiter() {
    let locks = LifecycleLocks::default();
    let name: Arc<str> = "TOOL".into();
    let background = locks.get(&name, 1).unwrap();
    let admission = InspectionAdmission::new(background.foreground_probe(), 100, 10);
    let held = background
        .try_background(BackgroundAdmission::Routine)
        .unwrap();
    let inspection_proof = Notify::new();
    let constructed = AtomicUsize::new(0);
    let mut inspection = Box::pin(async {
        inspection_proof.notified().await;
        InspectionAdmission::run(Some(&admission), 20, || {
            constructed.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        })
        .await
    });
    poll_fn(|cx| {
        assert!(inspection.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let foreground_proof = Notify::new();
    let mut foreground = Box::pin(locks.get(&name, 1).unwrap().lock_shared(|mutex| async {
        foreground_proof.notified().await;
        Ok(mutex.lock_owned().await)
    }));
    poll_fn(|cx| {
        assert!(foreground.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    inspection_proof.notify_one();
    assert_eq!(inspection.await.unwrap(), None);
    assert_eq!(
        constructed.load(Ordering::SeqCst),
        0,
        "deferred I/O future was constructed"
    );
    foreground_proof.notify_one();
    poll_fn(|cx| {
        assert!(foreground.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(held);
    let foreground = foreground.await.unwrap();
    assert!(locks
        .get(&name, 1)
        .unwrap()
        .try_background(BackgroundAdmission::Routine)
        .is_none());
    drop(foreground);
    assert!(locks
        .get(&name, 1)
        .unwrap()
        .try_background(BackgroundAdmission::Routine)
        .is_some());
}

#[tokio::test]
async fn inspection_handoff_never_defers_expired_or_required_work() {
    let locks = LifecycleLocks::default();
    let name: Arc<str> = "TOOL".into();
    let background = locks.get(&name, 1).unwrap();
    let probe = background.foreground_probe();
    let admission = InspectionAdmission::new(probe.clone(), 100, 10);
    let held = background
        .try_background(BackgroundAdmission::Routine)
        .unwrap();
    let mut foreground = Box::pin(locks.get(&name, 1).unwrap().lock_shared(|mutex| async {
        std::future::pending::<()>().await;
        Ok(mutex.lock_owned().await)
    }));
    poll_fn(|cx| {
        assert!(foreground.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let constructed = AtomicUsize::new(0);
    let operation = || {
        constructed.fetch_add(1, Ordering::SeqCst);
        async { Ok(()) }
    };
    assert_eq!(
        InspectionAdmission::run(Some(&admission), 99, operation)
            .await
            .unwrap(),
        None
    );
    assert_eq!(constructed.load(Ordering::SeqCst), 0);
    assert_eq!(
        InspectionAdmission::run(Some(&admission), 100, operation)
            .await
            .unwrap(),
        Some(())
    );
    let logically_expired = InspectionAdmission::new(probe, 100, 100);
    assert_eq!(
        InspectionAdmission::run(Some(&logically_expired), 5, operation)
            .await
            .unwrap(),
        Some(())
    );
    assert_eq!(
        InspectionAdmission::run(None, 20, operation).await.unwrap(),
        Some(())
    );
    assert_eq!(constructed.load(Ordering::SeqCst), 3);
    drop(foreground);
    drop(held);
}

#[tokio::test]
async fn late_waiter_does_not_cancel_entered_io_and_cancellation_restores_inspection() {
    let locks = LifecycleLocks::default();
    let name: Arc<str> = "TOOL".into();
    let background = locks.get(&name, 1).unwrap();
    let admission = InspectionAdmission::new(background.foreground_probe(), 100, 10);
    let held = background
        .try_background(BackgroundAdmission::Routine)
        .unwrap();
    let completion = Notify::new();
    let entered = AtomicUsize::new(0);
    let mut inspection = Box::pin(InspectionAdmission::run(Some(&admission), 20, || {
        entered.fetch_add(1, Ordering::SeqCst);
        async {
            completion.notified().await;
            anyhow::bail!("original inspection outcome is unknown")
        }
    }));
    poll_fn(|cx| {
        assert!(inspection.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let mut foreground = Box::pin(locks.get(&name, 1).unwrap().lock_shared(|mutex| async {
        std::future::pending::<()>().await;
        Ok(mutex.lock_owned().await)
    }));
    poll_fn(|cx| {
        assert!(foreground.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    completion.notify_one();
    let result: Result<Option<()>> = inspection.await;
    assert_eq!(
        result.unwrap_err().to_string(),
        "original inspection outcome is unknown"
    );
    assert_eq!(entered.load(Ordering::SeqCst), 1);
    assert_eq!(
        InspectionAdmission::run(Some(&admission), 20, || async { Ok(()) })
            .await
            .unwrap(),
        None
    );
    drop(foreground);
    assert_eq!(
        InspectionAdmission::run(Some(&admission), 20, || async { Ok(()) })
            .await
            .unwrap(),
        Some(())
    );
    drop(held);
}
