// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Durable human commands; GET never drives the maintenance worker.
//! Audit disk work stays on the console owner and precedes backend dispatch.
use crate::{api::ApiError, audit::Begin, raft::*, Console, Permission, Principal};
use axum::{
    extract::{rejection::JsonRejection, Path, State},
    response::{IntoResponse, Response},
    Extension, Json,
};
use cellar::sdk::fleet_operations::*;
use std::sync::Arc;

pub(crate) async fn status(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, operation)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    principal.require(&fleet, Permission::MetadataRead)?;
    Ok(match backend(&console, &fleet) {
        Ok(control) => match control.maintenance_status(&operation, TIMEOUT).await {
            Ok(value) => Json(value).into_response(),
            Err(error) => failure(error),
        },
        Err(error) => failure(error),
    })
}

pub(crate) async fn prepare(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, node)): Path<(String, String)>,
    body: Result<Json<RaftFleetMaintenancePrepare>, JsonRejection>,
) -> Result<Response, ApiError> {
    principal.require(&fleet, Permission::FleetMaintenance)?;
    let request = match body {
        Ok(Json(request)) => request,
        Err(_) => {
            return Ok(failure(RaftFleetFailure::invalid(
                "invalid maintenance preparation",
            )))
        }
    };
    if let Err(error) = request.validate() {
        return Ok(failure(error));
    }
    let audit = match begin(
        &console,
        &fleet,
        &principal.id,
        &node,
        &request.operation,
        "raft_maintenance_prepare",
        &request,
    )
    .await
    {
        Ok(audit) => audit,
        Err(error) => return Ok(audit_failure(error)),
    };
    let result = match backend(&console, &fleet) {
        Ok(control) => {
            control
                .prepare_node_maintenance(&principal.id, &node, request, TIMEOUT)
                .await
        }
        Err(error) => Err(error),
    };
    complete(&console, audit, result).await
}

pub(crate) async fn cancel(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, node)): Path<(String, String)>,
    body: Result<Json<RaftFleetMaintenanceCancel>, JsonRejection>,
) -> Result<Response, ApiError> {
    principal.require(&fleet, Permission::FleetCancel)?;
    let request = match body {
        Ok(Json(request)) => request,
        Err(_) => {
            return Ok(failure(RaftFleetFailure::invalid(
                "invalid maintenance cancellation",
            )))
        }
    };
    if let Err(error) = request.validate() {
        return Ok(failure(error));
    }
    let audit = match begin(
        &console,
        &fleet,
        &principal.id,
        &node,
        &request.operation,
        "raft_maintenance_cancel",
        &request,
    )
    .await
    {
        Ok(audit) => audit,
        Err(error) => return Ok(audit_failure(error)),
    };
    let result = match backend(&console, &fleet) {
        Ok(control) => {
            control
                .cancel_node_maintenance(&principal.id, &node, request, TIMEOUT)
                .await
        }
        Err(error) => Err(error),
    };
    complete(&console, audit, result).await
}

pub(crate) async fn action(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, node)): Path<(String, String)>,
    body: Result<Json<RaftFleetMaintenanceAction>, JsonRejection>,
) -> Result<Response, ApiError> {
    let request = match body {
        Ok(Json(request)) => request,
        Err(_) => {
            return Ok(failure(RaftFleetFailure::invalid(
                "invalid maintenance action",
            )))
        }
    };
    let name = match &request {
        RaftFleetMaintenanceAction::Stop { .. } => {
            principal.require(&fleet, Permission::FleetStop)?;
            "stop"
        }
        RaftFleetMaintenanceAction::Return { .. } => {
            principal.require(&fleet, Permission::FleetMaintenance)?;
            "return"
        }
        RaftFleetMaintenanceAction::Resume { .. } => {
            principal.require(&fleet, Permission::FleetMaintenance)?;
            principal.require(&fleet, Permission::FleetCapacity)?;
            "resume"
        }
    };
    if let Err(error) = request.validate() {
        return Ok(failure(error));
    }
    let audit = match begin(
        &console,
        &fleet,
        &principal.id,
        &node,
        request.operation(),
        &format!("raft_maintenance_{name}"),
        &request,
    )
    .await
    {
        Ok(audit) => audit,
        Err(error) => return Ok(audit_failure(error)),
    };
    let result = match backend(&console, &fleet) {
        Ok(control) => {
            control
                .admit_node_maintenance_action(&principal.id, &node, request, TIMEOUT)
                .await
        }
        Err(error) => Err(error),
    };
    complete(&console, audit, result).await
}

async fn begin(
    console: &Arc<Console>,
    fleet: &str,
    principal: &str,
    node: &str,
    operation: &RaftFleetNodeOperation,
    action: &str,
    body: &impl serde::Serialize,
) -> anyhow::Result<Begin> {
    let id = format!("raft-{}-{}", operation.catalog_id, operation.request_id);
    let target = format!("raft/nodes/{node}/maintenance");
    let fingerprint = hex_digest(&serde_json::to_vec(body)?);
    let fleet = fleet.to_owned();
    let principal = principal.to_owned();
    let action = action.to_owned();
    let owner = console.clone();
    // Entered SQLite work survives caller loss and is joined by the console.
    let task = console
        .domain
        .blocking(move || {
            owner
                .audit
                .begin(&id, &fleet, &principal, &target, &action, &fingerprint)
        })
        .map_err(anyhow::Error::from)?;
    task.await.map_err(anyhow::Error::from)?
}

async fn complete(
    console: &Arc<Console>,
    mut audit: Begin,
    result: Result<RaftFleetMaintenanceAcceptance, RaftFleetFailure>,
) -> Result<Response, ApiError> {
    let owner = console.clone();
    let task = match console.domain.blocking(move || {
        let reconciled = matches!(audit, Begin::Existing(_));
        finish(
            &owner,
            &mut audit,
            result.map(|mut value| {
                value.reconciled = reconciled;
                value
            }),
        )
    }) {
        Ok(task) => task,
        Err(error) => return Ok(audit_failure(anyhow::Error::from(error))),
    };
    match task.await {
        Ok(result) => result,
        Err(error) => Ok(audit_failure(anyhow::Error::from(error))),
    }
}
