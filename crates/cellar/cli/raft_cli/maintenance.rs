// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Submit once, validate the exact human scope, then poll only read-only status.
use super::*;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Target {
    Status,
    Prepare,
    Cancel,
    Stop,
    Return,
    Resume,
}

pub(super) async fn status(
    client: &http::Client,
    catalog: &str,
    operation: &str,
    wait: bool,
    timeout: Duration,
) -> Result<Value> {
    let value = observe(
        client,
        catalog,
        operation,
        None,
        wait,
        Target::Status,
        timeout,
    )
    .await?;
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
    let target = if cancel {
        Target::Cancel
    } else {
        Target::Prepare
    };
    let verb = if cancel {
        "cancel-maintenance"
    } else {
        "prepare-maintenance"
    };
    send(client, node, operation, body, (verb, target, wait, timeout)).await
}

pub(super) async fn submit_action(
    client: &http::Client,
    node: &str,
    request: &RaftFleetMaintenanceAction,
    wait: bool,
    timeout: Duration,
) -> Result<Value> {
    let target = match request {
        RaftFleetMaintenanceAction::Stop { .. } => Target::Stop,
        RaftFleetMaintenanceAction::Return { .. } => Target::Return,
        RaftFleetMaintenanceAction::Resume { .. } => Target::Resume,
    };
    send(
        client,
        node,
        request.operation(),
        request,
        ("maintenance-actions", target, wait, timeout),
    )
    .await
}

async fn send(
    client: &http::Client,
    node: &str,
    operation: &RaftFleetNodeOperation,
    body: &impl serde::Serialize,
    options: (&str, Target, bool, Duration),
) -> Result<Value> {
    let (verb, target, wait, timeout) = options;
    let started = crate::asyncrt::mono_ms();
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
        let remaining = (timeout.as_millis() as u64)
            .saturating_sub(crate::asyncrt::mono_ms().saturating_sub(started));
        let value = observe(
            client,
            &operation.catalog_id,
            &operation.operation_id,
            Some(&accepted),
            true,
            target,
            Duration::from_millis(remaining),
        )
        .await?;
        serde_json::to_value(value).map_err(|_| unavailable())
    } else {
        serde_json::to_value(accepted).map_err(|_| unavailable())
    }
}

fn request(value: &RaftFleetMaintenanceStatus, target: Target) -> Option<&str> {
    match target {
        Target::Prepare => Some(&value.request_id),
        Target::Cancel => value.cancellation_request.as_deref(),
        Target::Stop => value.stop_request.as_deref(),
        Target::Return => value.return_request.as_deref(),
        Target::Resume => value.resume_request.as_deref(),
        Target::Status => None,
    }
}

fn reached(value: &RaftFleetMaintenanceStatus, target: Target) -> bool {
    use RaftFleetMaintenancePhase as P;
    match target {
        Target::Prepare => value.readiness_request.is_some(),
        Target::Cancel => value.phase == P::Cancelled,
        Target::Stop => value.offline.is_some(),
        Target::Return => value.returning.is_some(),
        Target::Resume => value.phase == P::Complete && value.completed_revision.is_some(),
        Target::Status => matches!(
            value.phase,
            P::Ready | P::Offline | P::AwaitReturned | P::Cancelled | P::Complete
        ),
    }
}

async fn observe(
    client: &http::Client,
    catalog: &str,
    operation: &str,
    accepted: Option<&RaftFleetMaintenanceAcceptance>,
    wait: bool,
    target: Target,
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
                            || request(&value, target) != Some(a.request_id.as_str())
                            || target == Target::Prepare
                                && value.expected_revision != a.expected_revision
                    })
                {
                    return Err(unavailable());
                }
                if wait
                    && accepted.is_some()
                    && (target == Target::Cancel && value.cancellation_refused
                        || target != Target::Cancel
                            && value.phase == RaftFleetMaintenancePhase::Cancelled)
                {
                    return Err(RaftFleetFailure {code:RaftFleetFailureCode::Blocked,outcome:RaftFleetOutcome::Unknown,
                        message:"The requested maintenance result was refused or cancelled; inspect the durable operation status.".into()});
                }
                if !wait || reached(&value, target) {
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
        // Leave room for a final read instead of sleeping through the last
        // observation window. Only GET polling changes; mutation stays once.
        let delay = (remaining / 2).clamp(25, 500).min(remaining);
        crate::asyncrt::sleep(Duration::from_millis(delay)).await;
    }
}
