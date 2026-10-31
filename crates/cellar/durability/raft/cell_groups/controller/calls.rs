// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Response deadlines do not own the lifetime of admitted mutations.
use super::*;
use crate::raft::budget::Permit;
use tokio::sync::oneshot;

pub(super) struct Delivery<T, E> {
    pub result: Result<T, Arc<E>>,
    pub _permit: Arc<Permit>,
}

pub(super) struct Call<I, T, E> {
    pub receiver: oneshot::Receiver<Delivery<T, E>>,
    pub identity: I,
    pub deadline: u64,
    pub domain: crate::asyncrt::DomainToken,
    pub failure: fn(I, RuntimeError) -> E,
}

impl<I: Copy, T, E> Call<I, T, E> {
    pub async fn response(self) -> Result<T, Arc<E>> {
        let failure = |reason| Arc::new((self.failure)(self.identity, reason));
        let now = self
            .domain
            .mono_ms()
            .map_err(|_| failure(RuntimeError::Closed))?;
        if now >= self.deadline {
            return Err(failure(RuntimeError::DeadlineExceeded));
        }
        let timer = self
            .domain
            .sleep_until(self.deadline)
            .map_err(|_| failure(RuntimeError::Closed))?;
        let delivery = crate::asyncrt::select! {
            result = self.receiver => result.map_err(|_| failure(RuntimeError::Closed))?,
            _ = timer => return Err(failure(RuntimeError::DeadlineExceeded)),
        };
        if self
            .domain
            .mono_ms()
            .map_err(|_| failure(RuntimeError::Closed))?
            >= self.deadline
        {
            return Err(failure(RuntimeError::DeadlineExceeded));
        }
        delivery.result
    }
}
