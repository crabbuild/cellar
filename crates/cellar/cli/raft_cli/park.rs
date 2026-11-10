// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{options::ParkAction, *};

pub(super) async fn execute(
    client: &http::Client,
    action: &ParkAction,
    request: &RaftFleetParkRequest,
) -> Result<Value> {
    let verb = match action {
        ParkAction::Park => "park",
        ParkAction::Status => "status",
        ParkAction::Wake => "wake",
    };
    let path = format!(
        "namespaces/{}/parks/{}/{verb}",
        request.namespace_id, request.operation_id
    );
    match action {
        ParkAction::Park => {
            let status = client.post::<RaftFleetParkStatus>(&path, request).await?;
            verify(request, &status)?;
            if status.phase == RaftFleetParkPhase::Negotiating {
                return Err(unavailable());
            }
            serde_json::to_value(status).map_err(|_| unavailable())
        }
        ParkAction::Status => {
            let status = client
                .post::<Option<RaftFleetParkStatus>>(&path, request)
                .await?;
            if let Some(status) = &status {
                verify(request, status)?;
            }
            serde_json::to_value(status).map_err(|_| unavailable())
        }
        ParkAction::Wake => {
            let status = client.post::<RaftFleetWakeStatus>(&path, request).await?;
            verify(request, &status.park)?;
            if status.park.phase == RaftFleetParkPhase::Negotiating
                || status.applied_index < status.park.generation
                || status.applied_term == 0
            {
                return Err(unavailable());
            }
            serde_json::to_value(status).map_err(|_| unavailable())
        }
    }
}

fn verify(request: &RaftFleetParkRequest, status: &RaftFleetParkStatus) -> Result<()> {
    check_catalog(Some(&request.catalog_id), &status.catalog_id)?;
    if status.namespace_id != request.namespace_id
        || status.group != request.group
        || status.operation_id != request.operation_id
        || status.generation == 0
        || status.observed_revision <= request.expected_revision
    {
        return Err(unavailable());
    }
    Ok(())
}
