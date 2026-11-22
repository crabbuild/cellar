// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Activation-local lifecycle admission; remote execution has separate fencing.
use anyhow::{ensure, Result};
use std::{
    collections::HashMap,
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

#[derive(Default)]
pub(crate) struct LifecycleLocks {
    locks: Mutex<HashMap<Arc<str>, Entry>>,
}

struct Entry {
    mutex: Weak<AsyncMutex<()>>,
    waiting: Arc<AtomicUsize>,
}

pub(crate) struct LifecycleLock {
    mutex: Arc<AsyncMutex<()>>,
    waiting: Arc<AtomicUsize>,
}

/// Reserve admission before the caller's proof can suspend it. This owns no
/// SQL writer, serving authority, remote claim, task or timer.
struct ForegroundWait(Arc<AtomicUsize>);

impl ForegroundWait {
    fn new(waiting: Arc<AtomicUsize>) -> Result<Self> {
        waiting
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .map_err(|_| anyhow::anyhow!("provider lifecycle waiter limit exceeded"))?;
        Ok(Self(waiting))
    }
}

impl Drop for ForegroundWait {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl LifecycleLocks {
    pub(crate) fn get(&self, name: &Arc<str>, limit: usize) -> Result<LifecycleLock> {
        let mut locks = self.locks.lock().unwrap();
        locks.retain(|_, entry| entry.mutex.strong_count() > 0);
        if let Some(entry) = locks.get(name) {
            if let Some(mutex) = entry.mutex.upgrade() {
                return Ok(LifecycleLock {
                    mutex,
                    waiting: entry.waiting.clone(),
                });
            }
        }
        ensure!(locks.len() < limit, "provider lifecycle limit exceeded");
        let mutex = Arc::new(AsyncMutex::new(()));
        let waiting = Arc::new(AtomicUsize::new(0));
        locks.insert(
            name.clone(),
            Entry {
                mutex: Arc::downgrade(&mutex),
                waiting: waiting.clone(),
            },
        );
        Ok(LifecycleLock { mutex, waiting })
    }
}

impl LifecycleLock {
    pub(crate) async fn lock(self) -> OwnedMutexGuard<()> {
        self.mutex.lock_owned().await
    }

    /// The caller parks its private SQL writer through its original proof gate.
    pub(crate) async fn lock_shared<F, Fut>(self, park: F) -> Result<OwnedMutexGuard<()>>
    where
        F: FnOnce(Arc<AsyncMutex<()>>) -> Fut,
        Fut: Future<Output = Result<OwnedMutexGuard<()>>>,
    {
        match self.mutex.clone().try_lock_owned() {
            Ok(guard) => Ok(guard),
            Err(_) => {
                // The real mutex waiter is constructed only after the source
                // proof. Background inspection must not reacquire in that gap.
                // Cancellation and proof failure drop this reservation too.
                let waiting = ForegroundWait::new(self.waiting)?;
                let result = park(self.mutex).await;
                drop(waiting);
                result
            }
        }
    }

    pub(crate) fn try_background(self) -> Option<OwnedMutexGuard<()>> {
        if self.waiting.load(Ordering::Acquire) != 0 {
            return None;
        }
        let guard = self.mutex.try_lock_owned().ok()?;
        // A foreground caller can register between the first check and mutex
        // acquisition. Refuse this background turn without publishing work.
        (self.waiting.load(Ordering::Acquire) == 0).then_some(guard)
    }
}

#[cfg(test)]
mod tests {
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
            locks.get(&name, 2).unwrap().try_background().is_none(),
            "background bypassed foreground admission while its proof was pending"
        );
        proof.notify_one();
        let admitted = waiting.await.unwrap();
        assert!(locks.get(&name, 2).unwrap().try_background().is_none());
        drop(admitted);
        assert!(locks.get(&name, 2).unwrap().try_background().is_some());
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
        assert!(locks.get(&other, 2).unwrap().try_background().is_some());
        assert!(locks.get(&name, 2).unwrap().try_background().is_none());
        drop(waiting);
        assert!(locks.get(&name, 2).unwrap().try_background().is_some());
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
        assert!(locks.get(&other, 1).unwrap().try_background().is_some());
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
        assert!(locks.get(&name, 1).unwrap().try_background().is_none());
        drop(second);
        assert!(locks.get(&name, 1).unwrap().try_background().is_some());
    }
}
