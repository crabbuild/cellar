// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Deliver a queued SDK response only under its original source permit.

use anyhow::Result;
use std::future::Future;

pub(super) async fn receive<T>(
    response: impl Future<Output = Result<T>>,
    check: impl FnOnce() -> Result<()>,
) -> Result<T> {
    let result = response.await;
    // A valid I/O completion may wait in the mailbox while the application is
    // parked. Delivery still requires the original invocation, fence and deadline.
    // A refusal changes no entered target operation or retained target receipt.
    check()?;
    result
}

#[cfg(test)]
#[path = "dispatch_reply_tests.rs"]
mod tests;
