// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bind first-group operations to the authenticated fleet and namespace.
use crate::{
    api::ApiError,
    raft::{audit_failure, backend, failure, finish, hex_digest, TIMEOUT},
    Console, Permission, Principal,
};
use axum::{
    extract::{rejection::JsonRejection, Path, State},
    response::{IntoResponse, Response},
    Extension, Json,
};
use cellar::sdk::fleet_operations::*;
use serde::Serialize;
use std::sync::Arc;

pub(crate) async fn standby(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, namespace)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    principal.require(&fleet, Permission::MetadataRead)?;
    Ok(match backend(&console, &fleet) {
        Ok(control) => match control.first_group_standby(&namespace, TIMEOUT).await {
            Ok(value) => Json(value).into_response(),
            Err(error) => failure(error),
        },
        Err(error) => failure(error),
    })
}

pub(crate) async fn reserve(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, namespace)): Path<(String, String)>,
    body: Result<Json<RaftFleetFirstGroupPlan>, JsonRejection>,
) -> Result<Response, ApiError> {
    principal.require(&fleet, Permission::FleetTopology)?;
    let Ok(Json(plan)) = body else {
        return Ok(failure(RaftFleetFailure::invalid(
            "invalid first group plan",
        )));
    };
    submit(
        &console,
        &principal,
        &fleet,
        &namespace,
        Mutation::Reserve(plan),
    )
    .await
}

pub(crate) async fn publish(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, namespace)): Path<(String, String)>,
    body: Result<Json<RaftFleetFirstGroupPublish>, JsonRejection>,
) -> Result<Response, ApiError> {
    principal.require(&fleet, Permission::FleetTopology)?;
    let Ok(Json(request)) = body else {
        return Ok(failure(RaftFleetFailure::invalid(
            "invalid first group confirmation",
        )));
    };
    submit(
        &console,
        &principal,
        &fleet,
        &namespace,
        Mutation::Publish(request),
    )
    .await
}

pub(crate) async fn archive(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, namespace)): Path<(String, String)>,
    body: Result<Json<RaftFleetFirstGroupMutation>, JsonRejection>,
) -> Result<Response, ApiError> {
    principal.require(&fleet, Permission::FleetTopology)?;
    let Ok(Json(request)) = body else {
        return Ok(failure(RaftFleetFailure::invalid(
            "invalid first group archive request",
        )));
    };
    submit(
        &console,
        &principal,
        &fleet,
        &namespace,
        Mutation::Archive(request),
    )
    .await
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
enum Mutation {
    Reserve(RaftFleetFirstGroupPlan),
    Publish(RaftFleetFirstGroupPublish),
    Archive(RaftFleetFirstGroupMutation),
}

async fn submit(
    console: &Console,
    principal: &Principal,
    fleet: &str,
    namespace: &str,
    mutation: Mutation,
) -> Result<Response, ApiError> {
    let (plan, request_id, action, valid) = match &mutation {
        Mutation::Reserve(plan) => (
            plan,
            &plan.request_id,
            "raft_first_group_reserve",
            plan.validate(namespace),
        ),
        Mutation::Publish(request) => (
            &request.plan,
            &request.request_id,
            "raft_first_group_publish",
            request.validate(namespace),
        ),
        Mutation::Archive(request) => (
            &request.plan,
            &request.request_id,
            "raft_first_group_archive",
            request.validate(namespace),
        ),
    };
    if let Err(error) = valid {
        return Ok(failure(error));
    }
    let fingerprint = match serde_json::to_vec(&mutation) {
        Ok(bytes) => hex_digest(&bytes),
        Err(_) => {
            return Ok(failure(RaftFleetFailure::invalid(
                "invalid first group request",
            )))
        }
    };
    let mut audit = match console.audit.begin(
        &format!("raft-{}-{request_id}", plan.catalog_id),
        fleet,
        &principal.id,
        &format!("raft/namespaces/{namespace}/first-group"),
        action,
        &fingerprint,
    ) {
        Ok(value) => value,
        Err(error) => return Ok(audit_failure(error)),
    };
    let result = match backend(console, fleet) {
        Ok(control) => match &mutation {
            Mutation::Reserve(plan) => {
                control
                    .reserve_first_group(&principal.id, namespace, plan, TIMEOUT)
                    .await
            }
            Mutation::Publish(request) => {
                control
                    .publish_first_group(&principal.id, namespace, request, TIMEOUT)
                    .await
            }
            Mutation::Archive(request) => {
                control
                    .initialize_first_group_archive(&principal.id, namespace, request, TIMEOUT)
                    .await
            }
        },
        Err(error) => Err(error),
    };
    finish(console, &mut audit, result)
}
