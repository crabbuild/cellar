// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Operator receipts must match the exact first-group request.
use super::*;

pub(super) async fn standby(
    client: &http::Client,
    catalog: &str,
    namespace: &str,
    timeout: Duration,
) -> Result<Value> {
    let observed = client
        .get::<RaftFleetFirstGroupStandby>(
            &format!("namespaces/{namespace}/first-group/standby"),
            None,
            None,
            timeout,
        )
        .await?;
    observed.validate().map_err(|_| unavailable())?;
    if observed.catalog_id != catalog || observed.namespace_id != namespace {
        return Err(unavailable());
    }
    serde_json::to_value(observed).map_err(|_| unavailable())
}

pub(super) async fn reserve(
    client: &http::Client,
    plan: &RaftFleetFirstGroupPlan,
) -> Result<Value> {
    let response = client
        .post::<RaftFleetFirstGroupAcceptance>(
            &format!("namespaces/{}/first-group/reserve", plan.namespace_id),
            plan,
        )
        .await?;
    let expected = RaftFleetFirstGroupAcceptance::from_plan(
        plan,
        &plan.request_id,
        plan.expected_revision,
        RaftFleetFirstGroupStage::Reserved,
    );
    checked(response, expected)
}

pub(super) async fn publish(
    client: &http::Client,
    request: &RaftFleetFirstGroupPublish,
) -> Result<Value> {
    let response = client
        .post::<RaftFleetFirstGroupAcceptance>(
            &format!(
                "namespaces/{}/first-group/publish",
                request.plan.namespace_id
            ),
            request,
        )
        .await?;
    let expected = RaftFleetFirstGroupAcceptance::from_plan(
        &request.plan,
        &request.request_id,
        request.expected_revision,
        RaftFleetFirstGroupStage::Published,
    );
    checked(response, expected)
}

fn checked(
    response: RaftFleetFirstGroupAcceptance,
    expected: RaftFleetFirstGroupAcceptance,
) -> Result<Value> {
    if response != expected {
        return Err(unavailable());
    }
    serde_json::to_value(response).map_err(|_| unavailable())
}
