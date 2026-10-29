// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit node grants and durable audit precede the guarded SDK adapters.
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
    Path((fleet, node)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    principal.require(&fleet, Permission::MetadataRead)?;
    Ok(match backend(&console, &fleet) {
        Ok(control) => match control.node(&node, TIMEOUT).await {
            Ok(value) => Json(value).into_response(),
            Err(error) => failure(error),
        },
        Err(error) => failure(error),
    })
}

pub(crate) async fn register(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path(fleet): Path<String>,
    body: Result<Json<RaftFleetRegisterNode>, JsonRejection>,
) -> Result<Response, ApiError> {
    principal.require(&fleet, Permission::FleetRegister)?;
    let request = match body {
        Ok(Json(request)) => request,
        Err(_) => {
            return Ok(failure(RaftFleetFailure::invalid(
                "invalid node registration request",
            )))
        }
    };
    if let Err(error) = request.validate() {
        return Ok(failure(error));
    }
    let mut audit = match begin_audit(
        &console,
        &fleet,
        &principal.id,
        &format!("raft-{}-{}", request.catalog_id, request.request_id),
        &request.descriptor.node_id,
        "raft_node_register",
        &request,
    ) {
        Ok(value) => value,
        Err(error) => return Ok(audit_failure(error)),
    };
    let reconciled = matches!(audit, Begin::Existing(_));
    let result = match backend(&console, &fleet) {
        Ok(control) => control.register_node(&principal.id, request, TIMEOUT).await,
        Err(error) => Err(error),
    };
    finish(
        &console,
        &mut audit,
        result.map(|admission| RaftFleetNodeAcceptance {
            admission,
            reconciled,
        }),
    )
}

pub(crate) async fn qualify(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, node)): Path<(String, String)>,
    body: Result<Json<RaftFleetNodeCapacity>, JsonRejection>,
) -> Result<Response, ApiError> {
    capacity(console, principal, fleet, node, body, false).await
}

pub(crate) async fn activate(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, node)): Path<(String, String)>,
    body: Result<Json<RaftFleetNodeCapacity>, JsonRejection>,
) -> Result<Response, ApiError> {
    capacity(console, principal, fleet, node, body, true).await
}

async fn capacity(
    console: Arc<Console>,
    principal: Principal,
    fleet: String,
    node: String,
    body: Result<Json<RaftFleetNodeCapacity>, JsonRejection>,
    activate: bool,
) -> Result<Response, ApiError> {
    principal.require(&fleet, Permission::FleetCapacity)?;
    let request = match body {
        Ok(Json(request)) => request,
        Err(_) => {
            return Ok(failure(RaftFleetFailure::invalid(
                "invalid node capacity request",
            )))
        }
    };
    if let Err(error) = request.validate(&node) {
        return Ok(failure(error));
    }
    let action = if activate {
        "raft_node_activate"
    } else {
        "raft_node_qualify"
    };
    let mut audit = match begin_audit(
        &console,
        &fleet,
        &principal.id,
        &format!("raft-{}-{}", request.catalog_id, request.request_id),
        &node,
        action,
        &request,
    ) {
        Ok(value) => value,
        Err(error) => return Ok(audit_failure(error)),
    };
    let reconciled = matches!(audit, Begin::Existing(_));
    let result = match backend(&console, &fleet) {
        Ok(control) if activate => {
            control
                .activate_node(&principal.id, &node, request, TIMEOUT)
                .await
        }
        Ok(control) => {
            control
                .qualify_node(&principal.id, &node, request, TIMEOUT)
                .await
        }
        Err(error) => Err(error),
    };
    finish(
        &console,
        &mut audit,
        result.map(|admission| RaftFleetNodeAcceptance {
            admission,
            reconciled,
        }),
    )
}

fn begin_audit(
    console: &Console,
    fleet: &str,
    principal: &str,
    id: &str,
    node: &str,
    action: &str,
    body: &impl serde::Serialize,
) -> anyhow::Result<Begin> {
    let fingerprint = hex_digest(&serde_json::to_vec(body)?);
    console.audit.begin(
        id,
        fleet,
        principal,
        &format!("raft/nodes/{node}"),
        action,
        &fingerprint,
    )
}
