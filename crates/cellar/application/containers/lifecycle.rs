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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BackgroundAdmission {
    Routine,
    Required,
}

impl BackgroundAdmission {
    pub(crate) fn for_maintenance(
        stable_running: bool,
        deadline_ms: i64,
        now_ms: i64,
        logical_ms: i64,
    ) -> Self {
        if stable_running && now_ms.max(logical_ms) < deadline_ms {
            Self::Routine
        } else {
            Self::Required
        }
    }
}

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
                // proof. Routine inspection must not reacquire in that gap.
                // Cancellation and proof failure drop this reservation too.
                let waiting = ForegroundWait::new(self.waiting)?;
                let result = park(self.mutex).await;
                drop(waiting);
                result
            }
        }
    }

    pub(crate) fn try_background(
        self,
        admission: BackgroundAdmission,
    ) -> Option<OwnedMutexGuard<()>> {
        if admission == BackgroundAdmission::Routine && self.waiting.load(Ordering::Acquire) != 0 {
            return None;
        }
        let guard = self.mutex.try_lock_owned().ok()?;
        // A foreground caller can register between the first check and mutex
        // acquisition. Refuse routine inspection without publishing work;
        // required cleanup still uses the same exclusive mutex and proof gates.
        (admission == BackgroundAdmission::Required || self.waiting.load(Ordering::Acquire) == 0)
            .then_some(guard)
    }
}

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod tests;
