// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Read callbacks share the node queue and SQL owner, independently of waiters.
use super::calls::Delivery;
use super::*;
use crate::raft::budget::Permit;
use crate::raft::cell_groups::{
    CellDispatchError, CellQueryInvocation, CellQueryResponse, CellSqlOptions,
};
use cellar_logic::consensus::ProposalId;
use tokio::sync::oneshot;

pub(super) struct Request {
    invocation: CellQueryInvocation,
    options: CellSqlOptions,
    reply: oneshot::Sender<Delivery<CellQueryResponse, CellDispatchError>>,
    permit: Arc<Permit>,
}

/// Dropping or expiring the response does not cancel an admitted query callback.
/// Its physical connection and reservation remain owned until the callback exits.
pub struct CellQueryCall {
    inner: calls::Call<ProposalId, CellQueryResponse, CellDispatchError>,
}

impl CellQueryCall {
    pub async fn response(self) -> Result<CellQueryResponse, Arc<CellDispatchError>> {
        self.inner.response().await
    }
}

impl CellGroupController {
    /// Queue one fresh registered query synchronously. It retains its original
    /// consistency, token and deadline through discovery, read barrier and SQL.
    pub fn query(
        &self,
        invocation: CellQueryInvocation,
        mut options: CellSqlOptions,
    ) -> Result<CellQueryCall, CellDispatchError> {
        let shared = &self.handle.0;
        let (identity, deadline, bytes) = invocation.controller_admission(&shared.domain)?;
        if shared.closing.load(Ordering::Acquire) {
            return Err(CellDispatchError::Runtime(RuntimeError::Closed));
        }
        let bytes = bytes
            .checked_add(commands::option_bytes(&mut options).map_err(CellDispatchError::Runtime)?)
            .ok_or(CellDispatchError::Runtime(RuntimeError::Overloaded))?;
        let permit = shared
            .commands
            .acquire(bytes)
            .ok_or(CellDispatchError::Runtime(RuntimeError::Overloaded))?;
        let (reply, receiver) = oneshot::channel();
        shared
            .requests
            .try_send(requests::Request::Query(Box::new(Request {
                invocation,
                options,
                reply,
                permit: Arc::new(permit),
            })))
            .map_err(|error| {
                CellDispatchError::Runtime(match error {
                    tokio::sync::mpsc::error::TrySendError::Closed(_) => RuntimeError::Closed,
                    tokio::sync::mpsc::error::TrySendError::Full(_) => RuntimeError::Overloaded,
                })
            })?;
        Ok(CellQueryCall {
            inner: calls::Call {
                receiver,
                identity,
                deadline,
                domain: shared.domain.clone(),
                failure: |_, reason| CellDispatchError::Runtime(reason),
            },
        })
    }
}

pub(super) async fn drive(
    request: Request,
    runtime: CellGroupRuntime,
    sql: CellGroupSqlPool,
    shared: Arc<Shared>,
) {
    let Request {
        mut invocation,
        options,
        reply,
        permit,
    } = request;
    let result = async {
        invocation.begin().await?;
        let attempt = invocation
            .attempt()
            .ok_or(CellDispatchError::Runtime(RuntimeError::NotFound))?;
        if !shared
            .groups
            .contains(&attempt.route().history)
            .map_err(CellDispatchError::Runtime)?
        {
            return Err(CellDispatchError::Runtime(RuntimeError::NotFound));
        }
        invocation.execute(&runtime, &sql, options).await
    }
    .await
    .map_err(Arc::new);
    let _ = reply.send(Delivery {
        result,
        _permit: permit,
    });
}
