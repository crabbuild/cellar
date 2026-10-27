// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Ordinary RPC budgets cross a peer as durations, never domain timestamps.
//! Expiry proves no rollback and never makes an admitted mutation retryable.
use crate::application::rpc::{Code, Failure};
use anyhow::{ensure, Result};
use hyper::{HeaderMap, Request, StatusCode};
use std::{future::Future, time::Duration};

pub(super) const BUDGET_HEADER: &str = "x-cells-rpc-budget-ms";
const FAILURE_HEADER: &str = "x-cellar-rpc-failure";
const DEADLINE: &str = "DeadlineExceeded";

/// Parse before reading the body, so upload and admission share one receiver
/// deadline. The receiver's policy caps any duration supplied by its peer.
pub(super) fn from_headers(headers: &HeaderMap, now: u64, host_ms: u64) -> Result<Option<u64>> {
    let mut values = headers.get_all(BUDGET_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    ensure!(values.next().is_none(), "duplicate RPC budget header");
    let bytes = value.as_bytes();
    ensure!(
        !bytes.is_empty()
            && bytes.len() <= 20
            && bytes.iter().all(u8::is_ascii_digit)
            && (bytes.len() == 1 || bytes[0] != b'0'),
        "malformed RPC budget header"
    );
    let duration = value.to_str()?.parse::<u64>()?;
    Ok(Some(now.saturating_add(duration.min(host_ms))))
}

/// Refresh immediately before each send, including a provably unsent pooled
/// retry. Time spent establishing a tunnel cannot buy another receiver window.
pub(super) fn write<T>(request: &mut Request<T>, deadline_ms: Option<u64>) -> Result<()> {
    request.headers_mut().remove(BUDGET_HEADER);
    if let Some(deadline) = deadline_ms {
        let remaining = deadline.saturating_sub(crate::asyncrt::mono_ms());
        if remaining == 0 {
            return Err(expired());
        }
        request
            .headers_mut()
            .insert(BUDGET_HEADER, remaining.to_string().parse()?);
    }
    Ok(())
}

fn expired() -> anyhow::Error {
    Failure::new(Code::DeadlineExceeded).into()
}

/// The execution-domain timer is independent of an application/guest event
/// loop. Dropping `operation` retains the existing route/RPC cancellation guards.
pub(super) async fn receive<T>(
    deadline_ms: Option<u64>,
    operation: impl Future<Output = Result<T>>,
) -> Result<T> {
    let Some(deadline) = deadline_ms else {
        return operation.await;
    };
    let remaining = deadline.saturating_sub(crate::asyncrt::mono_ms());
    if remaining == 0 {
        return Err(expired());
    }
    crate::asyncrt::select_biased! { "an RPC deadline wins an undelivered result";
        _ = crate::asyncrt::sleep(Duration::from_millis(remaining)) => Err(expired()),
        result = operation => {
            // Synchronous work can overrun between timer polls.
            if crate::asyncrt::mono_ms() >= deadline { Err(expired()) } else { result }
        },
    }
}

pub(super) fn response() -> super::HttpReply {
    let mut reply = super::response(StatusCode::GATEWAY_TIMEOUT, "RPC deadline exceeded");
    reply.headers_mut().insert(
        FAILURE_HEADER,
        hyper::header::HeaderValue::from_static(DEADLINE),
    );
    super::peer_response(reply)
}

/// Only a paired host status/header carries a fixed infrastructure code. It
/// carries no admission/rollback evidence and never claims `not_committed`.
pub(super) fn remote_failure(status: StatusCode, headers: &HeaderMap) -> Option<Failure> {
    super::rpc_failure::remote(status, headers)
}

#[cfg(test)]
#[path = "rpc_deadline_tests.rs"]
mod tests;
