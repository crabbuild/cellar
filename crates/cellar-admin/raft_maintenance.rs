// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Durable human commands; GET never drives the maintenance worker.
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
    let mut audit = match begin(
        &console,
        &fleet,
        &principal.id,
        &node,
        &request.operation,
        "raft_maintenance_prepare",
        &request,
    ) {
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
    complete(&console, &mut audit, result)
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
    let mut audit = match begin(
        &console,
        &fleet,
        &principal.id,
        &node,
        &request.operation,
        "raft_maintenance_cancel",
        &request,
    ) {
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
    complete(&console, &mut audit, result)
}

fn begin(
    console: &Console,
    fleet: &str,
    principal: &str,
    node: &str,
    operation: &RaftFleetNodeOperation,
    action: &str,
    body: &impl serde::Serialize,
) -> anyhow::Result<Begin> {
    console.audit.begin(
        &format!("raft-{}-{}", operation.catalog_id, operation.request_id),
        fleet,
        principal,
        &format!("raft/nodes/{node}/maintenance"),
        action,
        &hex_digest(&serde_json::to_vec(body)?),
    )
}

fn complete(
    console: &Console,
    audit: &mut Begin,
    result: Result<RaftFleetMaintenanceAcceptance, RaftFleetFailure>,
) -> Result<Response, ApiError> {
    let reconciled = matches!(audit, Begin::Existing(_));
    finish(
        console,
        audit,
        result.map(|mut value| {
            value.reconciled = reconciled;
            value
        }),
    )
}
