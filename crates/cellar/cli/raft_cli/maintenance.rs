// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Submit once, validate the exact scope, then poll only read-only status.
use super::*;

pub(super) async fn status(
    client: &http::Client,
    catalog: &str,
    operation: &str,
    wait: bool,
    timeout: Duration,
) -> Result<Value> {
    let value = observe(client, catalog, operation, None, wait, false, timeout).await?;
    serde_json::to_value(value).map_err(|_| unavailable())
}

pub(super) async fn submit(
    client: &http::Client,
    node: &str,
    operation: &RaftFleetNodeOperation,
    body: &impl serde::Serialize,
    cancel: bool,
    wait: bool,
    timeout: Duration,
) -> Result<Value> {
    let started = crate::asyncrt::mono_ms();
    let verb = if cancel {
        "cancel-maintenance"
    } else {
        "prepare-maintenance"
    };
    let accepted = client
        .post_timeout::<RaftFleetMaintenanceAcceptance>(
            &format!("nodes/{node}/{verb}"),
            body,
            timeout.min(Duration::from_secs(30)),
        )
        .await?;
    accepted.validate().map_err(|_| unavailable())?;
    check_catalog(Some(&operation.catalog_id), &accepted.catalog_id)?;
    if accepted.node_id != node
        || accepted.operation_id != operation.operation_id
        || accepted.request_id != operation.request_id
        || accepted.expected_revision != operation.expected_revision
    {
        return Err(unavailable());
    }
    if wait {
        let remaining = timeout.as_millis() as u64;
        let remaining = remaining.saturating_sub(crate::asyncrt::mono_ms().saturating_sub(started));
        let value = observe(
            client,
            &operation.catalog_id,
            &operation.operation_id,
            Some(&accepted),
            true,
            cancel,
            Duration::from_millis(remaining),
        )
        .await?;
        serde_json::to_value(value).map_err(|_| unavailable())
    } else {
        serde_json::to_value(accepted).map_err(|_| unavailable())
    }
}

async fn observe(
    client: &http::Client,
    catalog: &str,
    operation: &str,
    accepted: Option<&RaftFleetMaintenanceAcceptance>,
    wait: bool,
    cancel: bool,
    timeout: Duration,
) -> Result<RaftFleetMaintenanceStatus> {
    let deadline = crate::asyncrt::mono_ms().saturating_add(timeout.as_millis() as u64);
    loop {
        let remaining = deadline.saturating_sub(crate::asyncrt::mono_ms());
        if remaining == 0 {
            return Err(RaftFleetFailure {
                code: RaftFleetFailureCode::WaitTimeout,
                outcome: RaftFleetOutcome::Unknown,
                message:
                    "Maintenance observation timed out; retain the request and inspect status."
                        .into(),
            });
        }
        let result = client
            .get::<RaftFleetMaintenanceStatus>(
                &format!("maintenance/{operation}"),
                None,
                None,
                Duration::from_millis(remaining.min(30_000)),
            )
            .await;
        match result {
            Ok(value) => {
                value.validate().map_err(|_| unavailable())?;
                check_catalog(Some(catalog), &value.catalog_id)?;
                if value.operation_id != operation
                    || accepted.is_some_and(|a| {
                        value.node_id != a.node_id
                            || value.observed_revision < a.applied_revision
                            || if cancel {
                                value.cancellation_request.as_deref() != Some(&a.request_id)
                            } else {
                                value.request_id != a.request_id
                                    || value.expected_revision != a.expected_revision
                            }
                    })
                {
                    return Err(unavailable());
                }
                if wait
                    && !cancel
                    && accepted.is_some()
                    && value.phase == RaftFleetMaintenancePhase::Cancelled
                {
                    return Err(RaftFleetFailure {
                        code: RaftFleetFailureCode::Blocked,
                        outcome: RaftFleetOutcome::Unknown,
                        message: "Preparation was cancelled before the requested Ready result; inspect maintenance status.".into(),
                    });
                }
                if !wait
                    || value.phase == RaftFleetMaintenancePhase::Cancelled
                    || !cancel && value.phase == RaftFleetMaintenancePhase::Ready
                {
                    return Ok(value);
                }
            }
            Err(error)
                if wait
                    && matches!(
                        error.code,
                        RaftFleetFailureCode::Busy | RaftFleetFailureCode::NotFound
                    ) => {}
            Err(error) => return Err(error),
        }
        let remaining = deadline.saturating_sub(crate::asyncrt::mono_ms());
        crate::asyncrt::sleep(Duration::from_millis(remaining.min(500))).await;
    }
}
