// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exclude physical reservation from revocation and absent-operation sealing.
use anyhow::Result;
use tokio::sync::{Mutex, MutexGuard};

#[derive(Default)]
pub(super) struct Admission {
    global: Mutex<()>,
    #[cfg(test)]
    attempts: std::sync::atomic::AtomicUsize,
}

pub(super) struct Resource<'a> {
    _global: MutexGuard<'a, ()>,
}

impl Admission {
    pub(super) async fn exclusive(&self) -> MutexGuard<'_, ()> {
        self.global.lock().await
    }

    pub(super) async fn resource(&self, _key: &str) -> Result<Resource<'_>> {
        #[cfg(test)]
        self.attempts
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        Ok(Resource {
            _global: self.global.lock().await,
        })
    }

    #[cfg(test)]
    pub(super) fn attempts(&self) -> usize {
        self.attempts.load(std::sync::atomic::Ordering::Acquire)
    }
}
