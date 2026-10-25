// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use crate::{api::ApiError, audit::Begin, Console, Permission, Principal};
use axum::{
    extract::{Path, State},
    Extension, Json,
};
use cellar::management::NodeAction;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
    generation: String,
    action: NodeAction,
    operation_id: String,
}

pub(crate) async fn action(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, node)): Path<(String, String)>,
    Json(mut request): Json<Request>,
) -> Result<Json<Value>, ApiError> {
    let readonly = matches!(request.action, NodeAction::State | NodeAction::Cells { .. });
    principal.require(
        &fleet,
        if readonly {
            Permission::MetadataRead
        } else {
            Permission::Operate
        },
    )?;
    let client = &console
        .fleets
        .get(&fleet)
        .ok_or_else(ApiError::forbidden)?
        .client;
    if let NodeAction::Stop { operation_id, .. } = &request.action {
        if operation_id != &request.operation_id {
            return Err(anyhow::anyhow!("stop operation IDs must match").into());
        }
    }
    if let NodeAction::FleetStop { request: stop } = &request.action {
        if stop.request_id() != request.operation_id {
            return Err(
                anyhow::anyhow!("fleet stop guard and audit request IDs must match").into(),
            );
        }
    }
    let mut operation = if readonly {
        None
    } else {
        let fingerprint = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&request).map_err(anyhow::Error::from)?)
        );
        let label = serde_json::to_value(&request.action).map_err(anyhow::Error::from)?;
        let target = match &request.action {
            NodeAction::Evict { scope, epoch } => {
                format!("cell/{scope}@{epoch} on node/{node}@{}", request.generation)
            }
            _ => format!("node/{node}@{}", request.generation),
        };
        match console.audit.begin(
            &request.operation_id,
            &fleet,
            &principal.id,
            &target,
            match &request.action {
                NodeAction::Stop { .. } => "stop",
                NodeAction::FleetStop { request } => match request.action() {
                    cellar::sdk::consensus::FleetStopAction::Stop => "fleet_stop",
                    cellar::sdk::consensus::FleetStopAction::Fence => "fleet_stop_fence",
                },
                _ => label.as_str().unwrap_or("evict"),
            },
            &fingerprint,
        )? {
            Begin::Started(operation) => Some(operation),
            Begin::Existing(operation) => {
                return Ok(Json(json!({"operation":operation,"replayed":true})))
            }
        }
    };
    if let NodeAction::Stop { operation_id, .. } = &mut request.action {
        *operation_id = stop_id(&fleet, &principal.id, &request.operation_id);
    }
    let result = client
        .node_action(&node, &request.generation, request.action)
        .await;
    if let Some(operation) = &mut operation {
        console.audit.finish(
            operation,
            if result
                .as_ref()
                .is_ok_and(|value| value.get("propagation").is_some())
            {
                "accepted"
            } else if result.is_ok() {
                "succeeded"
            } else {
                "outcome_unknown"
            },
        )?;
    }
    match result {
        Ok(result) => Ok(Json(json!({"result":result,"operation":operation}))),
        Err(error) => Ok(Json(
            json!({"error":error.to_string(),"operation":operation}),
        )),
    }
}

fn stop_id(fleet: &str, actor: &str, operation: &str) -> String {
    format!(
        "{:x}",
        Sha256::digest(format!("{fleet}\0{actor}\0{operation}").as_bytes())
    )
}

pub(crate) async fn receipt(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, node, generation, actor, operation)): Path<(
        String,
        String,
        String,
        String,
        String,
    )>,
) -> Result<Json<Value>, ApiError> {
    principal.require(&fleet, Permission::MetadataRead)?;
    let client = &console
        .fleets
        .get(&fleet)
        .ok_or_else(ApiError::forbidden)?
        .client;
    let receipt = client
        .stop_receipt(&node, &generation, &stop_id(&fleet, &actor, &operation))
        .await?;
    if let Some(receipt) = &receipt {
        console
            .audit
            .reconcile_stop(&fleet, &actor, &operation, receipt)?;
    }
    Ok(Json(json!({"receipt":receipt})))
}
