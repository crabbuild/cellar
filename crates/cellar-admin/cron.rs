// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use crate::{api::ApiError, Console, Permission, Principal};
use axum::{
    extract::{Path, State},
    Extension, Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Preview {
    expression: String,
}

pub(crate) async fn preview(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path(fleet): Path<String>,
    Json(request): Json<Preview>,
) -> Result<Json<Value>, ApiError> {
    principal.require(&fleet, Permission::MetadataRead)?;
    if !console.fleets.contains_key(&fleet) {
        return Err(ApiError::forbidden());
    }
    if request.expression.len() > 256 {
        return Err(anyhow::anyhow!("cron expression is too long").into());
    }
    let observed = cellar::ownership_store::now_ms();
    // Unsatisfiable schedules can search a full calendar cycle; keep that work
    // off the request executor while retaining shutdown join coverage.
    let task = console
        .domain
        .blocking(move || -> anyhow::Result<Value> {
            let cron = cellar_logic::cron::parse(&request.expression)
                .map_err(|error| anyhow::anyhow!("{error:?}"))?;
            let mut cursor = i64::try_from(observed)
                .map_err(|_| anyhow::anyhow!("clock exceeds cron timestamp range"))?;
            let mut occurrences = Vec::new();
            for _ in 0..5 {
                let Some(next) = cellar_logic::cron::next_after(&cron, cursor) else {
                    break;
                };
                occurrences.push(next);
                cursor = next;
            }
            Ok(json!({
                "expression": request.expression,
                "timezone": "UTC",
                "after": observed,
                "occurrences": occurrences,
                "satisfiable": !occurrences.is_empty(),
            }))
        })
        .map_err(anyhow::Error::from)?;
    Ok(Json(task.await.map_err(anyhow::Error::from)??))
}

pub(crate) async fn configuration(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, application)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    principal.require(&fleet, Permission::MetadataRead)?;
    let client = &console
        .fleets
        .get(&fleet)
        .ok_or_else(ApiError::forbidden)?
        .client;
    Ok(Json(
        serde_json::to_value(client.cron_configuration(&application).await?)
            .map_err(anyhow::Error::from)?,
    ))
}

#[derive(Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Update {
    operation_id: String,
    deployment_revision: String,
    expected_revision: Option<String>,
    expressions: Vec<String>,
}

pub(crate) async fn update(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, application)): Path<(String, String)>,
    Json(request): Json<Update>,
) -> Result<Json<Value>, ApiError> {
    use crate::audit::Begin;
    use sha2::{Digest, Sha256};
    principal.require(&fleet, Permission::Configure)?;
    let client = &console
        .fleets
        .get(&fleet)
        .ok_or_else(ApiError::forbidden)?
        .client;
    let fingerprint = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(&request).map_err(anyhow::Error::from)?)
    );
    let mut operation = match console.audit.begin(
        &request.operation_id,
        &fleet,
        &principal.id,
        &format!("application/{application}"),
        "cron_update",
        &fingerprint,
    )? {
        Begin::Started(operation) => operation,
        Begin::Existing(operation) => {
            return Ok(Json(json!({"operation":operation,"replayed":true})))
        }
    };
    let result = client
        .update_crons(
            &application,
            &request.deployment_revision,
            request.expected_revision.as_deref(),
            request.expressions,
        )
        .await;
    console.audit.finish(
        &mut operation,
        if result.is_ok() {
            "accepted"
        } else {
            "outcome_unknown"
        },
    )?;
    match result {
        Ok(configuration) => Ok(Json(
            json!({"configuration":configuration,"operation":operation,"propagation":"pending"}),
        )),
        Err(error) => Ok(Json(
            json!({"error":error.to_string(),"operation":operation}),
        )),
    }
}

pub(crate) async fn propagation(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, application)): Path<(String, String)>,
    axum::extract::Query(page): axum::extract::Query<cellar::management::PageRequest>,
) -> Result<Json<Value>, ApiError> {
    principal.require(&fleet, Permission::MetadataRead)?;
    let client = &console
        .fleets
        .get(&fleet)
        .ok_or_else(ApiError::forbidden)?
        .client;
    Ok(Json(
        serde_json::to_value(client.cron_propagation(&application, page).await?)
            .map_err(anyhow::Error::from)?,
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExecutionQuery {
    before: Option<u64>,
    #[serde(default = "execution_page_size")]
    limit: u32,
    expression: Option<String>,
}

fn execution_page_size() -> u32 {
    50
}

pub(crate) async fn executions(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((fleet, application)): Path<(String, String)>,
    axum::extract::Query(query): axum::extract::Query<ExecutionQuery>,
) -> Result<Json<Value>, ApiError> {
    principal.require(&fleet, Permission::DataRead)?;
    let client = &console
        .fleets
        .get(&fleet)
        .ok_or_else(ApiError::forbidden)?
        .client;
    Ok(Json(
        client
            .cron_history(
                &application,
                query.before,
                query.limit,
                query.expression.as_deref(),
            )
            .await?,
    ))
}
