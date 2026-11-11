// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Read-only snapshots after a failed process boundary, without retrying work.
use super::*;

const DIAGNOSTICS: &str = "/fixture/raft-diagnostics";

/// Failure diagnostics are advisory snapshots, never receipt or quorum proof.
pub(super) fn with_diagnostics(
    router: axum::Router,
    group: RaftGroupHandle,
    cells: RaftCells,
) -> axum::Router {
    router.route(
        DIAGNOSTICS,
        axum::routing::get(move |headers: axum::http::HeaderMap| {
            let group = group.clone();
            let cells = cells.clone();
            async move {
                use axum::http::StatusCode;
                if headers
                    .get(axum::http::header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.strip_prefix("Bearer "))
                    != Some(SECRET)
                {
                    return Err(StatusCode::UNAUTHORIZED);
                }
                let usage = cells.resource_usage();
                let performance = cells.performance();
                let observed = match group.observe(Duration::from_millis(500)).await {
                    Ok(sample) => serde_json::json!({
                        "member": sample.replica.member.get(),
                        "term": sample.status.term.get(),
                        "leader": sample.status.leader,
                        "ready": sample.status.ready,
                        "applied": sample.status.applied.index.get(),
                        "committed": sample.status.committed.get(),
                        "cells": sample.cells,
                    }),
                    Err(error) => serde_json::json!({"error": error.to_string()}),
                };
                let archive = match group.archive_status() {
                    Ok(Some(status)) => serde_json::json!({
                        "installed": true,
                        "attempts": status.attempts,
                        "stopped": status.stopped,
                        "error": status.error.as_ref().map(|error| error.to_string()),
                        "published": status.published.as_ref().map(|manifest| {
                            serde_json::json!({
                                "index": manifest.position().index.get(),
                                "term": manifest.position().term.get(),
                                "active": manifest.is_active(),
                            })
                        }),
                    }),
                    Ok(None) => serde_json::json!({"installed": false}),
                    Err(error) => serde_json::json!({"error": error.to_string()}),
                };
                Ok(axum::Json(serde_json::json!({
                    "closed": cells.is_closed(),
                    "observed": observed,
                    "archive": archive,
                    "resources": usage,
                    "performance_available": performance.available,
                    "performance_saturated": performance.saturated,
                    "counters": performance.counters,
                })))
            }
        }),
    )
}

/// Called only after the original assertion has already failed. These
/// observations neither retry the mutation nor extend its reconciliation window.
pub(super) async fn record_failure(
    client: &reqwest::Client,
    addresses: &[ManagedIngress],
    ledger: &mut recovery::Ledger,
    phase: &str,
) -> Result<()> {
    for (index, address) in addresses.iter().enumerate() {
        let response = client
            .get(format!("{}{DIAGNOSTICS}", address.axum))
            .bearer_auth(SECRET)
            .timeout(Duration::from_secs(2))
            .send()
            .await;
        let snapshot = match response {
            Ok(response) => {
                let status = response.status().as_u16();
                match response.json::<Value>().await {
                    Ok(body) => serde_json::json!({"status": status, "body": body}),
                    Err(error) => serde_json::json!({"status": status, "error": error.to_string()}),
                }
            }
            Err(error) => serde_json::json!({"error": error.to_string()}),
        };
        ledger.record(serde_json::json!({
            "phase": phase,
            "member": index + 1,
            "snapshot": snapshot,
        }))?;
    }
    Ok(())
}
