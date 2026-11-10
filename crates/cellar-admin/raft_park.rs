// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use crate::{api::ApiError, raft::*, Console, Permission, Principal};
use axum::{
    extract::{rejection::JsonRejection, Path, State},
    response::{IntoResponse, Response},
    Extension, Json,
};
use cellar::sdk::fleet_operations::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Action {
    Park,
    Status,
    Wake,
}

#[derive(Serialize)]
#[serde(untagged)]
enum Outcome {
    Park(RaftFleetParkStatus),
    Wake(RaftFleetWakeStatus),
}

pub(crate) async fn dispatch(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, namespace, operation, action)): Path<(String, String, String, Action)>,
    body: Result<Json<RaftFleetParkRequest>, JsonRejection>,
) -> Result<Response, ApiError> {
    let permission = match action {
        Action::Status => Permission::MetadataRead,
        Action::Park | Action::Wake => Permission::FleetTopology,
    };
    principal.require(&fleet, permission)?;
    let Ok(Json(request)) = body else {
        return Ok(failure(RaftFleetFailure::invalid(
            "invalid group park request",
        )));
    };
    if let Err(error) = request.validate(&namespace, &operation) {
        return Ok(failure(error));
    }
    let control = match backend(&console, &fleet) {
        Ok(control) => control,
        Err(error) => return Ok(failure(error)),
    };
    let (audit_id, verb) = match action {
        Action::Status => {
            return Ok(
                match control
                    .park_group_status(&principal.id, &request, TIMEOUT)
                    .await
                {
                    Ok(value) => Json(value).into_response(),
                    Err(error) => failure(error),
                },
            );
        }
        Action::Park => (
            format!("raft-{}-{}", request.catalog_id, request.request_id),
            "park",
        ),
        Action::Wake => (
            format!(
                "raft-wake-{}-{}",
                request.catalog_id, request.settlement_request_id
            ),
            "wake",
        ),
    };
    let fingerprint = match serde_json::to_vec(&request) {
        Ok(bytes) => hex_digest(&bytes),
        Err(_) => {
            return Ok(failure(RaftFleetFailure::invalid(
                "invalid group park request",
            )))
        }
    };
    let owner = console.clone();
    let actor = principal.id.clone();
    let begin = console.domain.blocking(move || {
        owner.audit.begin(
            &audit_id,
            &fleet,
            &actor,
            &format!("raft/namespaces/{namespace}/parks/{operation}/{verb}"),
            &format!("raft_group_{verb}"),
            &fingerprint,
        )
    });
    let mut audit = match begin {
        Ok(task) => match task.await {
            Ok(Ok(audit)) => audit,
            Ok(Err(error)) => return Ok(audit_failure(error)),
            Err(error) => return Ok(audit_failure(error.into())),
        },
        Err(error) => return Ok(audit_failure(error.into())),
    };
    let result = match action {
        Action::Park => control
            .park_group(&principal.id, &request, TIMEOUT)
            .await
            .map(Outcome::Park),
        Action::Wake => control
            .wake_group(&principal.id, &request, TIMEOUT)
            .await
            .map(Outcome::Wake),
        Action::Status => unreachable!("status returns before audit"),
    };
    let owner = console.clone();
    let completed = console
        .domain
        .blocking(move || finish(&owner, &mut audit, result));
    match completed {
        Ok(task) => match task.await {
            Ok(result) => result,
            Err(error) => Ok(audit_failure(error.into())),
        },
        Err(error) => Ok(audit_failure(error.into())),
    }
}
