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

#[derive(Clone)]
pub(crate) struct ForegroundProbe(Arc<AtomicUsize>);

/// Clones share one admission. A known inspection handoff revokes every old
/// local handle together; retaining a clone cannot retain or revive the lock.
#[derive(Clone)]
pub(crate) struct LifecycleHold(Arc<Mutex<Option<OwnedMutexGuard<()>>>>);

impl LifecycleHold {
    pub(crate) fn new(guard: OwnedMutexGuard<()>) -> Self {
        Self(Arc::new(Mutex::new(Some(guard))))
    }

    pub(crate) fn held(&self) -> bool {
        self.0.lock().unwrap().is_some()
    }

    fn release(&self) {
        let guard = self.0.lock().unwrap().take();
        drop(guard);
    }
}

/// A scheduling hint for a healthy inspection, never permission for I/O.
#[derive(Clone)]
pub(crate) struct InspectionAdmission {
    foreground: ForegroundProbe,
    deadline_ms: i64,
    logical_ms: i64,
}

impl InspectionAdmission {
    pub(crate) fn new(foreground: ForegroundProbe, deadline_ms: i64, logical_ms: i64) -> Self {
        Self {
            foreground,
            deadline_ms,
            logical_ms,
        }
    }

    /// Transfer lifecycle custody through the owned fence I/O. A known healthy
    /// deferral can release it before the SQL continuation reacquires a writer;
    /// an error retains admission and its original failure. Only a completed
    /// fence (or a refusal before entry) can make this local handoff.
    pub(crate) async fn fence<F, Fut>(
        admission: Option<&Self>,
        lifecycle: &LifecycleHold,
        now_ms: impl Fn() -> i64,
        operation: F,
    ) -> Result<Option<()>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let result = Self::run(admission, now_ms(), operation).await;
        if result
            .as_ref()
            .is_ok_and(|activated| activated.is_none() || Self::defers(admission, now_ms()))
        {
            lifecycle.release();
            return Ok(None);
        }
        result
    }

    fn defers(admission: Option<&Self>, now_ms: i64) -> bool {
        admission.is_some_and(|admission| {
            BackgroundAdmission::for_maintenance(
                true,
                admission.deadline_ms,
                now_ms,
                admission.logical_ms,
            ) == BackgroundAdmission::Routine
                && admission.foreground.0.load(Ordering::Acquire) != 0
        })
    }

    /// Call only after the original source proof. Deferred work constructs no
    /// operation future. Expiry and required cleanup do not yield this way.
    pub(crate) async fn run<T, F, Fut>(
        admission: Option<&Self>,
        now_ms: i64,
        operation: F,
    ) -> Result<Option<T>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        if Self::defers(admission, now_ms) {
            return Ok(None);
        }
        operation().await.map(Some)
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
    pub(crate) fn foreground_probe(&self) -> ForegroundProbe {
        ForegroundProbe(self.waiting.clone())
    }

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
