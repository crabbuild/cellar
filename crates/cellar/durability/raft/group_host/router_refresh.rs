// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One bounded request lane into the existing joined directory/cache owner.
use crate::asyncrt::DomainToken;
use crate::raft::{
    budget::{Budget, Permit},
    completion::RuntimeError,
};
use cellar_logic::cell_group::GroupHistory;
use std::sync::Arc;
use tokio::sync::{mpsc, oneshot};

pub(crate) struct RouterRefreshRequest {
    pub(crate) history: GroupHistory,
    pub(crate) deadline: u64,
    pub(crate) reply: oneshot::Sender<Result<(), RuntimeError>>,
    pub(crate) _memory: Permit,
}

#[derive(Clone)]
pub(crate) struct RouterRefreshClient {
    sender: mpsc::Sender<RouterRefreshRequest>,
    memory: Arc<Budget>,
    domain: DomainToken,
}

impl RouterRefreshClient {
    pub(crate) fn channel(
        memory: Arc<Budget>,
        domain: DomainToken,
    ) -> Result<(Self, mpsc::Receiver<RouterRefreshRequest>), RuntimeError> {
        let (_, capacity) = memory.capacity();
        if capacity == 0 {
            return Err(RuntimeError::Invalid("router refresh capacity".into()));
        }
        let (sender, receiver) = mpsc::channel(capacity);
        Ok((
            Self {
                sender,
                memory,
                domain,
            },
            receiver,
        ))
    }

    pub(crate) async fn request(
        &self,
        history: GroupHistory,
        deadline: u64,
    ) -> Result<(), RuntimeError> {
        if self.domain.mono_ms().map_err(|_| RuntimeError::Closed)? >= deadline {
            return Err(RuntimeError::DeadlineExceeded);
        }
        let memory = self
            .memory
            .acquire(32 * 1024)
            .ok_or(RuntimeError::Overloaded)?;
        let (reply, response) = oneshot::channel();
        self.sender
            .try_send(RouterRefreshRequest {
                history,
                deadline,
                reply,
                _memory: memory,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => RuntimeError::Overloaded,
                mpsc::error::TrySendError::Closed(_) => RuntimeError::Closed,
            })?;
        // The receiver retains the original memory until the owned lookup exits.
        self.domain
            .run(crate::asyncrt::timeout_at(deadline, response))
            .await
            .map_err(|_| RuntimeError::Closed)?
            .map_err(|_| RuntimeError::DeadlineExceeded)?
            .map_err(|_| RuntimeError::Closed)?
    }
}
