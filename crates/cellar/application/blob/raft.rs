// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Provider commits remain separate from the source's proven transaction.
//! Unknown external outcomes never reexecute an application command.

use crate::application::{consensus::CommandFailure, Context};
use anyhow::{ensure, Result};
use std::future::Future;

pub(super) fn check(context: &Context, binding: &str, bucket: &str) -> Result<()> {
    context.check_binding_turn()?;
    if context.raft_storage.is_some() {
        ensure!(
            context
                .blob_bindings
                .get(binding)
                .is_some_and(|declared| declared.name() == bucket),
            CommandFailure::UnsupportedCapability
        );
    }
    Ok(())
}

pub(super) async fn io<T: Send + 'static>(
    context: &Context,
    future: impl Future<Output = Result<T>> + Send + 'static,
) -> Result<T> {
    run(context, future, false).await
}

/// Provider HEAD/GET/LIST and consuming a GET body do not mutate the source
/// image. Reuse only an unchanged source proof; mutations take an ordered step.
pub(super) async fn read<T: Send + 'static>(
    context: &Context,
    future: impl Future<Output = Result<T>> + Send + 'static,
) -> Result<T> {
    run(context, future, true).await
}

async fn run<T: Send + 'static>(
    context: &Context,
    future: impl Future<Output = Result<T>> + Send + 'static,
    read_only: bool,
) -> Result<T> {
    context.check_binding_turn()?;
    let result = if context.raft_storage.is_some() {
        let dispatch = crate::storage::capture_raft_dispatch()?
            .ok_or(CommandFailure::UnsupportedCapability)?;
        let action = move |timeout| async move {
            crate::asyncrt::timeout(timeout, future)
                .await
                .map_err(|_| crate::application::consensus::RuntimeError::DeadlineExceeded)?
        };
        let call = if read_only {
            dispatch.schedule_read(action)
        } else {
            dispatch.schedule_io(action)
        };
        call.future.await??
    } else {
        future.await?
    };
    context.check_binding_turn()?;
    Ok(result)
}
