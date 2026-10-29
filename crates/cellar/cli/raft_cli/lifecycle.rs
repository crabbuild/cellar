// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Advance acknowledged verification steps; never retry an unknown mutation.
use super::*;

pub(super) async fn status(
    client: &http::Client,
    catalog: Option<&str>,
    operation: &str,
    timeout: Duration,
) -> Result<Value> {
    let view = client
        .get::<RaftFleetEvacuationStatus>(&format!("evacuations/{operation}"), None, None, timeout)
        .await?;
    view.validate().map_err(|_| unavailable())?;
    check_catalog(catalog, &view.catalog_id)?;
    if view.operation_id != operation {
        return Err(unavailable());
    }
    serde_json::to_value(view).map_err(|_| unavailable())
}

pub(super) struct Scope<'a> {
    pub node: &'a str,
    pub catalog: &'a str,
    pub request: &'a str,
    pub operation: &'a str,
    pub revision: u64,
    pub verb: &'a str,
    pub process: Option<&'a str>,
    pub stop: Option<&'a str>,
    pub fence: Option<&'a str>,
}

pub(super) async fn execute(
    client: &http::Client,
    scope: Scope<'_>,
    body: &impl serde::Serialize,
    wait: bool,
    timeout: Duration,
) -> Result<Value> {
    let deadline = crate::asyncrt::mono_ms()
        .checked_add(timeout.as_millis() as u64)
        .ok_or_else(|| invalid("verification deadline"))?;
    loop {
        if crate::asyncrt::mono_ms() >= deadline {
            return Err(RaftFleetFailure { code: RaftFleetFailureCode::WaitTimeout, outcome: RaftFleetOutcome::Unknown,
                message: "Verification observation timed out. Retain the exact request; no stop or removal follows from partial progress.".into() });
        }
        let remaining = deadline.saturating_sub(crate::asyncrt::mono_ms());
        let value = client
            .post_timeout::<RaftFleetEvacuationAcceptance>(
                &format!("nodes/{}/{}", scope.node, scope.verb),
                body,
                Duration::from_millis(remaining.min(30_000)),
            )
            .await?;
        let progress = &value.progress;
        progress.validate().map_err(|_| unavailable())?;
        check_catalog(Some(scope.catalog), &progress.catalog_id)?;
        if progress.operation_id != scope.operation
            || progress.request_id != scope.request
            || progress.node_id != scope.node
            || progress.expected_revision != scope.revision
            || scope
                .process
                .is_some_and(|p| progress.process.as_deref() != Some(p))
            || progress.stop_id.as_deref() != scope.stop
            || progress.fence_id.as_deref() != scope.fence
        {
            return Err(unavailable());
        }
        let valid = match scope.verb {
            "cancel-evacuation" => progress.phase == RaftFleetEvacuationPhase::Cancelled,
            "evacuate" => progress.phase == RaftFleetEvacuationPhase::Excluded,
            "prepare-removal" => matches!(
                progress.phase,
                RaftFleetEvacuationPhase::VerifyingBeforeStop
                    | RaftFleetEvacuationPhase::Finalizing
            ),
            "stop" => matches!(
                progress.phase,
                RaftFleetEvacuationPhase::StopRequested | RaftFleetEvacuationPhase::Joined
            ),
            "remove" => matches!(
                progress.phase,
                RaftFleetEvacuationPhase::VerifyingRemoval | RaftFleetEvacuationPhase::Removed
            ),
            _ => false,
        };
        if !valid {
            return Err(unavailable());
        }
        if !wait || !progress.verifying() {
            return serde_json::to_value(value).map_err(|_| unavailable());
        }
        // Only a confirmed nonterminal verification step advances the loop.
        // Errors (including unknown fence/removal outcomes) return immediately.
        crate::asyncrt::sleep(Duration::from_millis(10)).await;
    }
}
