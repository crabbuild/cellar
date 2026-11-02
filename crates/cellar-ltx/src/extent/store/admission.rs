// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded blocking admission, same-name exclusion, and fair GC exclusion.
use super::*;
use timing::Phase;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Key {
    Object(ExtentId),
    Root(ImageRoot),
}

pub(super) struct KeyGuard {
    inner: Arc<Inner>,
    key: Key,
}

impl Drop for KeyGuard {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
        state.keys.remove(&self.key);
        if std::thread::panicking() {
            state.quarantined = true;
        }
        drop(state);
        self.inner.idle.notify_all();
    }
}

impl Inner {
    pub(super) fn key(
        self: &Arc<Self>,
        key: Key,
        allow_quarantine: bool,
    ) -> Result<KeyGuard, StoreError> {
        let _wait = timing::span(Phase::InventoryWait);
        let mut state = self.state()?;
        loop {
            if state.quarantined && !allow_quarantine {
                return Err(StoreError::Quarantined);
            }
            if state.keys.insert(key) {
                return Ok(KeyGuard {
                    inner: self.clone(),
                    key,
                });
            }
            state = self.idle.wait(state).map_err(|_| StoreError::Quarantined)?;
        }
    }

    pub(super) fn try_key(self: &Arc<Self>, key: Key) -> Result<Option<KeyGuard>, StoreError> {
        let mut state = self.state()?;
        if state.quarantined {
            return Err(StoreError::Quarantined);
        }
        Ok(state.keys.insert(key).then(|| KeyGuard {
            inner: self.clone(),
            key,
        }))
    }
}

pub(super) struct Operation {
    inner: Arc<Inner>,
    exclusive: bool,
    allow_quarantine: bool,
    complete: bool,
}

impl Operation {
    pub(super) fn finish<T>(mut self, result: Result<T, StoreError>) -> Result<T, StoreError> {
        let mut state = self.inner.state()?;
        state.active = state.active.checked_sub(1).ok_or(StoreError::Quarantined)?;
        if self.exclusive {
            state.exclusive = false;
        }
        let quarantined = state.quarantined;
        self.complete = true;
        drop(state);
        self.inner.idle.notify_all();
        if result.is_ok() && quarantined && !self.allow_quarantine {
            Err(StoreError::Quarantined)
        } else {
            result
        }
    }
}

impl Drop for Operation {
    fn drop(&mut self) {
        if !self.complete {
            let mut state = self.inner.state.lock().unwrap_or_else(|e| e.into_inner());
            state.quarantined = true;
            state.active = state.active.saturating_sub(1);
            if self.exclusive {
                state.exclusive = false;
            }
            drop(state);
            self.inner.idle.notify_all();
        }
    }
}

impl NodeExtentStore {
    pub(super) fn begin(&self) -> Result<Operation, StoreError> {
        self.start(false, false)
    }

    pub(super) fn begin_read(&self) -> Result<Operation, StoreError> {
        self.start(true, false)
    }

    pub(super) fn begin_exclusive(&self) -> Result<Operation, StoreError> {
        self.start(false, true)
    }

    fn start(&self, allow_quarantine: bool, exclusive: bool) -> Result<Operation, StoreError> {
        let _wait = timing::span(Phase::InventoryWait);
        let mut state = self.inner.state()?;
        let waiting = exclusive && self.inner.serialized;
        if waiting {
            state.exclusive_waiters += 1;
        }
        loop {
            if state.quarantined && !allow_quarantine {
                if waiting {
                    state.exclusive_waiters -= 1;
                }
                drop(state);
                self.inner.idle.notify_all();
                return Err(StoreError::Quarantined);
            }
            let admitted = if exclusive {
                state.active == 0
            } else {
                !state.exclusive
                    && state.exclusive_waiters == 0
                    && state.active < self.inner.publications
            };
            if admitted {
                if waiting {
                    state.exclusive_waiters -= 1;
                }
                state.active += 1;
                state.exclusive = exclusive;
                return Ok(Operation {
                    inner: self.inner.clone(),
                    exclusive,
                    allow_quarantine,
                    complete: false,
                });
            }
            if !self.inner.serialized {
                return Err(StoreError::Busy);
            }
            state = self
                .inner
                .idle
                .wait(state)
                .map_err(|_| StoreError::Quarantined)?;
        }
    }
}
