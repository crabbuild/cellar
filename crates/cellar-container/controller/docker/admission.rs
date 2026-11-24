// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Isolate binding fences; shutdown and history revocation exclude every scope.
use super::ExecutionControllerFailure;
use anyhow::Result;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{
    Mutex as AsyncMutex, OwnedMutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard,
};

#[derive(Default)]
pub(super) struct Admission {
    global: RwLock<()>,
    resources: Mutex<BTreeMap<String, Weak<AsyncMutex<()>>>>,
    #[cfg(test)]
    attempts: std::sync::atomic::AtomicUsize,
}

pub(super) struct Resource<'a> {
    _resource: OwnedMutexGuard<()>,
    _global: RwLockReadGuard<'a, ()>,
}

impl Admission {
    pub(super) async fn exclusive(&self) -> RwLockWriteGuard<'_, ()> {
        self.global.write().await
    }

    pub(super) async fn resource(&self, key: &str) -> Result<Resource<'_>> {
        #[cfg(test)]
        self.attempts
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        let global = self.global.read().await;
        let resource = {
            let mut resources = self
                .resources
                .lock()
                .map_err(|_| ExecutionControllerFailure::Unavailable)?;
            // Only entered/waiting scopes keep a strong reference. Idle cells
            // do not retain a mutex, and later acquisition removes dead keys.
            resources.retain(|_, lock| lock.strong_count() != 0);
            match resources.get(key).and_then(Weak::upgrade) {
                Some(lock) => lock,
                None => {
                    let lock = Arc::new(AsyncMutex::new(()));
                    resources.insert(key.to_owned(), Arc::downgrade(&lock));
                    lock
                }
            }
        };
        Ok(Resource {
            _resource: resource.lock_owned().await,
            _global: global,
        })
    }

    #[cfg(test)]
    pub(super) fn attempts(&self) -> usize {
        self.attempts.load(std::sync::atomic::Ordering::Acquire)
    }
}
