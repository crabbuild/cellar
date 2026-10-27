// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Human authorization and durable audit precede rooted fleet dispatch.
use crate::{
    api::ApiError,
    audit::{AuditPayloadConflict, Begin},
    Console, Permission, Principal,
};
use axum::{
    extract::{rejection::JsonRejection, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use cellar::sdk::fleet_operations::*;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};

pub(super) const TIMEOUT: Duration = Duration::from_secs(25);

pub(super) fn failure(value: RaftFleetFailure) -> Response {
    use RaftFleetFailureCode as C;
    let status = match value.code {
        C::InvalidInput => StatusCode::BAD_REQUEST,
        C::Conflict | C::PayloadConflict => StatusCode::CONFLICT,
        C::Blocked => StatusCode::LOCKED,
        C::Limit | C::Busy => StatusCode::TOO_MANY_REQUESTS,
        C::NotFound => StatusCode::NOT_FOUND,
        C::Unsupported => StatusCode::NOT_IMPLEMENTED,
        C::WaitTimeout => StatusCode::REQUEST_TIMEOUT,
        C::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        C::AuthRequired => StatusCode::UNAUTHORIZED,
        C::Forbidden => StatusCode::FORBIDDEN,
    };
    (
        status,
        Json(json!({"error":value,"phase":null,"terminal":null})),
    )
        .into_response()
}

pub(super) fn backend<'a>(
    console: &'a Console,
    fleet: &str,
) -> Result<&'a RaftFleetControl, RaftFleetFailure> {
    console
        .fleets
        .get(fleet)
        .and_then(|f| f.client.raft_fleet())
        .ok_or_else(RaftFleetFailure::unsupported)
}

pub(crate) async fn status(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path(fleet): Path<String>,
) -> Result<Response, ApiError> {
    principal.require(&fleet, Permission::MetadataRead)?;
    Ok(match backend(&console, &fleet) {
        Ok(control) => match control.status(TIMEOUT).await {
            Ok(value) => Json(value).into_response(),
            Err(error) => failure(error),
        },
        Err(error) => failure(error),
    })
}

pub(crate) async fn operation(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, operation)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    principal.require(&fleet, Permission::MetadataRead)?;
    Ok(match backend(&console, &fleet) {
        Ok(control) => match control.operation(&operation, TIMEOUT).await {
            Ok(value) => Json(value).into_response(),
            Err(error) => failure(error),
        },
        Err(error) => failure(error),
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Page {
    cursor: Option<String>,
    #[serde(default = "page_limit")]
    limit: usize,
}

fn page_limit() -> usize {
    64
}

pub(crate) async fn movements(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, operation)): Path<(String, String)>,
    Query(page): Query<Page>,
) -> Result<Response, ApiError> {
    principal.require(&fleet, Permission::MetadataRead)?;
    Ok(match backend(&console, &fleet) {
        Ok(control) => match control
            .movements(&operation, page.cursor.as_deref(), page.limit, TIMEOUT)
            .await
        {
            Ok(value) => Json(value).into_response(),
            Err(error) => failure(error),
        },
        Err(error) => failure(error),
    })
}

pub(super) fn audit_failure(error: anyhow::Error) -> Response {
    if error.downcast_ref::<AuditPayloadConflict>().is_some() {
        failure(RaftFleetFailure {
            code: RaftFleetFailureCode::PayloadConflict,
            outcome: RaftFleetOutcome::NotSubmitted,
            message: "The audit request ID already has a different canonical payload.".into(),
        })
    } else {
        failure(RaftFleetFailure {
            code: RaftFleetFailureCode::Unavailable,
            outcome: RaftFleetOutcome::Unknown,
            message: "The operation audit could not be verified; reconcile the exact request."
                .into(),
        })
    }
}

pub(crate) async fn begin(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path(fleet): Path<String>,
    body: Result<Json<RaftFleetBegin>, JsonRejection>,
) -> Result<Response, ApiError> {
    principal.require(&fleet, Permission::FleetTopology)?;
    let request = match body {
        Ok(Json(request)) => match request.canonicalize() {
            Ok(request) => request,
            Err(error) => return Ok(failure(error)),
        },
        Err(_) => {
            return Ok(failure(RaftFleetFailure::invalid(
                "invalid topology request",
            )))
        }
    };
    let fingerprint = match serde_json::to_vec(&request) {
        Ok(bytes) => hex_digest(&bytes),
        Err(_) => {
            return Ok(failure(RaftFleetFailure::invalid(
                "invalid topology request",
            )))
        }
    };
    let mut audit = match console.audit.begin(
        &format!("raft-{}-{}", request.catalog_id, request.operation_id),
        &fleet,
        &principal.id,
        "raft/topology",
        "raft_topology",
        &fingerprint,
    ) {
        Ok(value) => value,
        Err(error) => return Ok(audit_failure(error)),
    };
    let reconciled = matches!(audit, Begin::Existing(_));
    let result = match backend(&console, &fleet) {
        Ok(control) => control.begin(&principal.id, request.clone(), TIMEOUT).await,
        Err(error) => Err(error),
    };
    finish(
        &console,
        &mut audit,
        result.map(|operation| RaftFleetAcceptance {
            request_id: request.operation_id,
            operation,
            reconciled,
        }),
    )
}

pub(crate) async fn cancel(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, operation)): Path<(String, String)>,
    body: Result<Json<RaftFleetCancel>, JsonRejection>,
) -> Result<Response, ApiError> {
    principal.require(&fleet, Permission::FleetCancel)?;
    let request = match body {
        Ok(Json(request)) => request,
        Err(_) => {
            return Ok(failure(RaftFleetFailure::invalid(
                "invalid cancellation request",
            )))
        }
    };
    if let Err(error) = request.validate(&operation) {
        return Ok(failure(error));
    }
    let fingerprint = match serde_json::to_vec(&(operation.clone(), &request)) {
        Ok(bytes) => hex_digest(&bytes),
        Err(_) => {
            return Ok(failure(RaftFleetFailure::invalid(
                "invalid cancellation request",
            )))
        }
    };
    let mut audit = match console.audit.begin(
        &format!("raft-{}-{}", request.catalog_id, request.request_id),
        &fleet,
        &principal.id,
        &format!("raft/topology/{operation}"),
        "raft_topology_cancel",
        &fingerprint,
    ) {
        Ok(value) => value,
        Err(error) => return Ok(audit_failure(error)),
    };
    let reconciled = matches!(audit, Begin::Existing(_));
    let result = match backend(&console, &fleet) {
        Ok(control) => {
            control
                .cancel(&principal.id, &operation, request.clone(), TIMEOUT)
                .await
        }
        Err(error) => Err(error),
    };
    finish(
        &console,
        &mut audit,
        result.map(|operation| RaftFleetAcceptance {
            request_id: request.request_id,
            operation,
            reconciled,
        }),
    )
}

pub(super) fn hex_digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(super) fn finish<T: serde::Serialize>(
    console: &Console,
    audit: &mut Begin,
    result: Result<T, RaftFleetFailure>,
) -> Result<Response, ApiError> {
    let (Begin::Started(operation) | Begin::Existing(operation)) = audit;
    // The audit describes dispatch, not convergence. Even a historical success
    // is rechecked by the backend rather than replayed as an empty audit handle.
    let status = match &result {
        Ok(_) => "accepted",
        Err(error) if error.outcome == RaftFleetOutcome::NotSubmitted => "not_submitted",
        Err(_) => "outcome_unknown",
    };
    if let Err(error) = console.audit.finish(operation, status) {
        return Ok(audit_failure(error));
    }
    Ok(match result {
        Ok(value) => (StatusCode::ACCEPTED, Json(value)).into_response(),
        Err(error) => failure(error),
    })
}
