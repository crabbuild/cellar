// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Lifecycle grants are independent; verification never implies stop consent.
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
        Ok(control) => match control.evacuation_status(&operation, TIMEOUT).await {
            Ok(value) => Json(value).into_response(),
            Err(error) => failure(error),
        },
        Err(error) => failure(error),
    })
}

pub(crate) async fn evacuate(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, node)): Path<(String, String)>,
    body: Result<Json<RaftFleetNodeOperation>, JsonRejection>,
) -> Result<Response, ApiError> {
    principal.require(&fleet, Permission::FleetEvacuate)?;
    let request = match body {
        Ok(Json(request)) => request,
        Err(_) => {
            return Ok(failure(RaftFleetFailure::invalid(
                "invalid evacuation request",
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
        &request,
        "raft_node_evacuate",
        &request,
    ) {
        Ok(a) => a,
        Err(e) => return Ok(audit_failure(e)),
    };
    let result = match backend(&console, &fleet) {
        Ok(c) => {
            c.evacuate_node(&principal.id, &node, request, TIMEOUT)
                .await
        }
        Err(e) => Err(e),
    };
    finish_progress(&console, &mut audit, result)
}

pub(crate) async fn prepare(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, node)): Path<(String, String)>,
    body: Result<Json<RaftFleetNodeOperation>, JsonRejection>,
) -> Result<Response, ApiError> {
    principal.require(&fleet, Permission::FleetRemove)?;
    let request = match body {
        Ok(Json(request)) => request,
        Err(_) => {
            return Ok(failure(RaftFleetFailure::invalid(
                "invalid removal preparation",
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
        &request,
        "raft_node_prepare_removal",
        &request,
    ) {
        Ok(a) => a,
        Err(e) => return Ok(audit_failure(e)),
    };
    let result = match backend(&console, &fleet) {
        Ok(c) => {
            c.prepare_node_removal(&principal.id, &node, request, TIMEOUT)
                .await
        }
        Err(e) => Err(e),
    };
    finish_progress(&console, &mut audit, result)
}

pub(crate) async fn stop(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, node)): Path<(String, String)>,
    body: Result<Json<RaftFleetNodeStop>, JsonRejection>,
) -> Result<Response, ApiError> {
    principal.require(&fleet, Permission::FleetStop)?;
    let request = match body {
        Ok(Json(request)) => request,
        Err(_) => {
            return Ok(failure(RaftFleetFailure::invalid(
                "invalid guarded stop request",
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
        "raft_node_stop",
        &request,
    ) {
        Ok(a) => a,
        Err(e) => return Ok(audit_failure(e)),
    };
    let result = match backend(&console, &fleet) {
        Ok(c) => {
            c.stop_evacuated_node(&principal.id, &node, request, TIMEOUT)
                .await
        }
        Err(e) => Err(e),
    };
    finish_progress(&console, &mut audit, result)
}

pub(crate) async fn remove(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, node)): Path<(String, String)>,
    body: Result<Json<RaftFleetNodeRemove>, JsonRejection>,
) -> Result<Response, ApiError> {
    principal.require(&fleet, Permission::FleetRemove)?;
    let request = match body {
        Ok(Json(request)) => request,
        Err(_) => {
            return Ok(failure(RaftFleetFailure::invalid(
                "invalid removal request",
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
        "raft_node_remove",
        &request,
    ) {
        Ok(a) => a,
        Err(e) => return Ok(audit_failure(e)),
    };
    let result = match backend(&console, &fleet) {
        Ok(c) => c.remove_node(&principal.id, &node, request, TIMEOUT).await,
        Err(e) => Err(e),
    };
    finish_progress(&console, &mut audit, result)
}

fn begin(
    console: &Console,
    fleet: &str,
    principal: &str,
    node: &str,
    request: &RaftFleetNodeOperation,
    action: &str,
    body: &impl serde::Serialize,
) -> anyhow::Result<Begin> {
    console.audit.begin(
        &format!("raft-{}-{}", request.catalog_id, request.request_id),
        fleet,
        principal,
        &format!("raft/nodes/{node}"),
        action,
        &hex_digest(&serde_json::to_vec(body)?),
    )
}

fn finish_progress(
    console: &Console,
    audit: &mut Begin,
    result: Result<RaftFleetEvacuationProgress, RaftFleetFailure>,
) -> Result<Response, ApiError> {
    let reconciled = matches!(audit, Begin::Existing(_));
    let status = if result.as_ref().is_ok_and(|p| p.verifying()) {
        "verifying"
    } else {
        "accepted"
    };
    finish_with_status(
        console,
        audit,
        result.map(|progress| RaftFleetEvacuationAcceptance {
            progress,
            reconciled,
        }),
        status,
    )
}
