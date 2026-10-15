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
    let fingerprint = fingerprint(body)?;
    console
        .audit
        .begin(id, fleet, &principal.id, target, action, &fingerprint)
}

fn fingerprint(body: &impl Serialize) -> anyhow::Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(body)?)))
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
            let original: OutboxRequest = serde_json::from_value(json!({
                "operation_id": id,
                "action": {"kind": "retry", "expected": receipt["expected"], "reason": receipt["reason"]},
            })).map_err(anyhow::Error::from)?;
            if !console.audit.reconcile_event(
                &fleet,
                &principal.id,
                &id,
                &target,
                "outbox_retry",
                &fingerprint(&original)?,
            )? {
                return Err(anyhow::anyhow!(
                    "Receipt does not match the recorded request; outcome remains unknown"
                )
                .into());
            }
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
    let read_payloads = match &body.action {
        EventStreamAction::Read { payload, .. } => Some(*payload),
        _ => None,
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
        .await
        .and_then(|reply| stream_read_view(reply, read_payloads));
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
            let original: StreamRequest = serde_json::from_value(json!({
                "operation_id": id,
                "resource_id": body.resource_id,
                "action": {"kind": "change", "expected": receipt["previous"], "action": receipt["action"], "reason": receipt["reason"]},
            })).map_err(anyhow::Error::from)?;
            if !console.audit.reconcile_event(
                &fleet,
                &principal.id,
                &id,
                &target,
                "subscription_change",
                &fingerprint(&original)?,
            )? {
                return Err(anyhow::anyhow!(
                    "Receipt does not match the recorded request; outcome remains unknown"
                )
                .into());
            }
        }
    }
    event_reply(result, operation)
}

// JSON text preserves application integer values when the browser parses the
// enclosing API reply. Metadata and all durable offsets keep their native shape.
fn stream_read_view(mut reply: Value, payloads: Option<bool>) -> anyhow::Result<Value> {
    let Some(include) = payloads else {
        return Ok(reply);
    };
    let records = reply
        .get_mut("records")
        .and_then(Value::as_array_mut)
        .ok_or_else(|| anyhow::anyhow!("invalid event read page"))?;
    for record in records {
        let record = record
            .as_object_mut()
            .ok_or_else(|| anyhow::anyhow!("invalid event record"))?;
        let payload = record
            .get_mut("event")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| anyhow::anyhow!("invalid event metadata"))?
            .remove("payload");
        if include {
            let payload =
                payload.ok_or_else(|| anyhow::anyhow!("requested event payload missing"))?;
            record.insert(
                "payload_json".into(),
                Value::String(serde_json::to_string(&payload)?),
            );
        }
    }
    Ok(reply)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_payload_json_preserves_large_integers_and_explicit_opt_in() -> anyhow::Result<()> {
        let reply: Value = serde_json::from_str(
            r#"{"records":[{"offset":"1","event":{"id":"event-one","payload":{"id":9007199254740993,"max":18446744073709551615,"min":-9223372036854775808}}}]}"#,
        )?;
        let view = stream_read_view(reply.clone(), Some(true))?;
        assert!(view["records"][0]["event"].get("payload").is_none());
        let encoded = view["records"][0]["payload_json"].as_str().unwrap();
        assert!(encoded.contains("9007199254740993"));
        assert_eq!(
            serde_json::from_str::<Value>(encoded)?,
            reply["records"][0]["event"]["payload"]
        );
        let metadata = stream_read_view(reply, Some(false))?;
        assert!(metadata["records"][0]["event"].get("payload").is_none());
        assert!(metadata["records"][0].get("payload_json").is_none());
        assert!(stream_read_view(json!(false), Some(true)).is_err());
        assert!(stream_read_view(json!({"records":[[]]}), Some(true)).is_err());
        Ok(())
    }
}
