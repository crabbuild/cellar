// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Node receipts remain historical; validate their exact request before display.
use super::*;

pub(super) async fn status(
    client: &http::Client,
    catalog: Option<&str>,
    node: &str,
    timeout: Duration,
) -> Result<Value> {
    let status = client
        .get::<RaftFleetNodeStatus>(&format!("nodes/{node}"), None, None, timeout)
        .await?;
    status.validate().map_err(|_| unavailable())?;
    check_catalog(catalog, &status.catalog_id)?;
    if status.descriptor.node_id != node {
        return Err(unavailable());
    }
    serde_json::to_value(status).map_err(|_| unavailable())
}

pub(super) async fn register(
    client: &http::Client,
    request: &RaftFleetRegisterNode,
) -> Result<Value> {
    let accepted = client
        .post::<RaftFleetNodeAcceptance>("nodes", request)
        .await?;
    checked(
        accepted,
        &request.catalog_id,
        &request.request_id,
        request.expected_revision,
        &request.descriptor.node_id,
        RaftFleetNodeAction::Register,
    )
}

pub(super) async fn capacity(
    client: &http::Client,
    node: &str,
    request: &RaftFleetNodeCapacity,
    activate: bool,
) -> Result<Value> {
    let verb = if activate { "activate" } else { "qualify" };
    let accepted = client
        .post::<RaftFleetNodeAcceptance>(&format!("nodes/{node}/{verb}"), request)
        .await?;
    checked(
        accepted,
        &request.catalog_id,
        &request.request_id,
        request.expected_revision,
        node,
        if activate {
            RaftFleetNodeAction::Activate
        } else {
            RaftFleetNodeAction::Qualify
        },
    )
}

fn checked(
    accepted: RaftFleetNodeAcceptance,
    catalog: &str,
    request: &str,
    revision: u64,
    node: &str,
    action: RaftFleetNodeAction,
) -> Result<Value> {
    let admission = &accepted.admission;
    admission.validate().map_err(|_| unavailable())?;
    check_catalog(Some(catalog), &admission.catalog_id)?;
    if admission.request_id != request
        || admission.expected_revision != revision
        || admission.node_id != node
        || admission.action != action
    {
        return Err(unavailable());
    }
    serde_json::to_value(accepted).map_err(|_| unavailable())
}
