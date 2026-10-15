//! Browser event workspaces reuse owner-routed native operator protocols.

use crate::{api::ApiError, audit::Begin, Console, Permission, Principal};
use axum::{
    extract::{Path, Query, State},
    Extension, Json,
};
use cellar::management::{EventStreamAction, OutboxAction};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OutboxRequest {
    operation_id: String,
    action: OutboxAction,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StreamRequest {
    operation_id: String,
    resource_id: String,
    action: EventStreamAction,
}

fn operator(fleet: &str, principal: &Principal) -> String {
    // The signed native receipt identity is stable and cannot be supplied by a browser.
    format!(
        "dashboard-{:x}",
        Sha256::digest(format!("{fleet}:{}", principal.id).as_bytes())
    )
}

fn operation_id(id: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        id.len() == 32
            && id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        "invalid event operation ID"
    );
    Ok(())
}

fn intent(
    console: &Console,
    principal: &Principal,
    fleet: &str,
    id: &str,
    target: &str,
    action: &str,
    body: &impl Serialize,
) -> anyhow::Result<Begin> {
    operation_id(id)?;
    let fingerprint = format!("{:x}", Sha256::digest(serde_json::to_vec(body)?));
    console
        .audit
        .begin(id, fleet, &principal.id, target, action, &fingerprint)
}

pub(crate) async fn outbox(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, scope)): Path<(String, String)>,
    Json(body): Json<OutboxRequest>,
) -> Result<Json<Value>, ApiError> {
    principal.require(&fleet, Permission::DataRead)?;
    if body.action.is_write() {
        principal.require(&fleet, Permission::Operate)?;
    }
    let client = &console
        .fleets
        .get(&fleet)
        .ok_or_else(ApiError::forbidden)?
        .client;
    operation_id(&body.operation_id)?;
    body.action.validate(&scope, &body.operation_id)?;
    let target = format!("outbox/{scope}");
    let mut operation = if body.action.is_write() {
        match intent(
            &console,
            &principal,
            &fleet,
            &body.operation_id,
            &target,
            "outbox_retry",
            &body,
        )? {
            Begin::Started(operation) => Some(operation),
            Begin::Existing(operation) => {
                return Ok(Json(json!({"operation":operation,"replayed":true})))
            }
        }
    } else {
        None
    };
    let status_id = if let OutboxAction::Status { operation } = &body.action {
        Some(operation.clone())
    } else {
        None
    };
    let result = client
        .outbox(
            &scope,
            body.action,
            &operator(&fleet, &principal),
            &body.operation_id,
        )
        .await;
    if let Some(operation) = operation.as_mut() {
        console.audit.finish(
            operation,
            if result.is_ok() {
                "succeeded"
            } else {
                "outcome_unknown"
            },
        )?;
    }
    if let (Some(id), Ok(receipt)) = (status_id, &result) {
        if receipt["operation"] == id
            && receipt["operator"] == operator(&fleet, &principal)
            && receipt["expected"]["source"] == scope
            && receipt["rearmed"].is_boolean()
        {
            console
                .audit
                .reconcile_event(&fleet, &principal.id, &id, &target, "outbox_retry")?;
        }
    }
    event_reply(result, operation)
}

pub(crate) async fn stream(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, application, binding)): Path<(String, String, String)>,
    Json(body): Json<StreamRequest>,
) -> Result<Json<Value>, ApiError> {
    principal.require(&fleet, Permission::DataRead)?;
    if body.action.is_write() {
        principal.require(&fleet, Permission::Operate)?;
    }
    if body.action.requires_data_write() {
        principal.require(&fleet, Permission::DataWrite)?;
    }
    let client = &console
        .fleets
        .get(&fleet)
        .ok_or_else(ApiError::forbidden)?
        .client;
    operation_id(&body.operation_id)?;
    body.action
        .validate(&body.resource_id, &body.operation_id)?;
    let target = format!("stream/{application}/{binding}/{}", body.resource_id);
    let mut operation = if body.action.is_write() {
        match intent(
            &console,
            &principal,
            &fleet,
            &body.operation_id,
            &target,
            "subscription_change",
            &body,
        )? {
            Begin::Started(operation) => Some(operation),
            Begin::Existing(operation) => {
                return Ok(Json(json!({"operation":operation,"replayed":true})))
            }
        }
    } else {
        None
    };
    let status_id = if let EventStreamAction::Status { operation } = &body.action {
        Some(operation.clone())
    } else {
        None
    };
    let result = client
        .event_stream(
            &application,
            &binding,
            &body.resource_id,
            body.action,
            &operator(&fleet, &principal),
            &body.operation_id,
        )
        .await;
    if let Some(operation) = operation.as_mut() {
        console.audit.finish(
            operation,
            if result.is_ok() {
                "succeeded"
            } else {
                "outcome_unknown"
            },
        )?;
    }
    if let (Some(id), Ok(receipt)) = (status_id, &result) {
        if receipt["operation"] == id
            && receipt["operator"] == operator(&fleet, &principal)
            && receipt["previous"]["resource"] == body.resource_id
            && receipt["current"]["resource"] == body.resource_id
        {
            console.audit.reconcile_event(
                &fleet,
                &principal.id,
                &id,
                &target,
                "subscription_change",
            )?;
        }
    }
    event_reply(result, operation)
}

fn event_reply(
    result: anyhow::Result<Value>,
    operation: Option<crate::Operation>,
) -> Result<Json<Value>, ApiError> {
    match result {
        Ok(result) => Ok(Json(json!({"result":result,"operation":operation}))),
        Err(error) if operation.is_some() => Ok(Json(
            json!({"failure":error.to_string(),"operation":operation}),
        )),
        Err(error) => Err(error.into()),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CatalogQuery {
    cursor: Option<String>,
    #[serde(default = "page_size")]
    limit: usize,
}

fn page_size() -> usize {
    50
}

pub(crate) async fn resources(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, application, binding)): Path<(String, String, String)>,
    Query(page): Query<CatalogQuery>,
) -> Result<Json<Value>, ApiError> {
    principal.require(&fleet, Permission::DataRead)?;
    let client = &console
        .fleets
        .get(&fleet)
        .ok_or_else(ApiError::forbidden)?
        .client;
    Ok(Json(
        serde_json::to_value(
            client
                .event_resources(&application, &binding, page.cursor, page.limit)
                .await?,
        )
        .map_err(anyhow::Error::from)?,
    ))
}

pub(crate) async fn state(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, scope)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    principal.require(&fleet, Permission::DataRead)?;
    let client = &console
        .fleets
        .get(&fleet)
        .ok_or_else(ApiError::forbidden)?
        .client;
    let sample = client.cell_event_state(&scope).await?;
    Ok(Json(
        serde_json::to_value(sample).map_err(anyhow::Error::from)?,
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MetricsQuery {
    generation: String,
}

pub(crate) async fn metrics(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, node)): Path<(String, String)>,
    Query(query): Query<MetricsQuery>,
) -> Result<Json<Value>, ApiError> {
    principal.require(&fleet, Permission::DataRead)?;
    let client = &console
        .fleets
        .get(&fleet)
        .ok_or_else(ApiError::forbidden)?
        .client;
    Ok(Json(
        client.node_event_metrics(&node, &query.generation).await?,
    ))
}
