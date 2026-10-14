//! Authenticated operator fetch protocol for reserved workflow cells.
use super::{cell::*, *};
use crate::engine_api::{HttpResponse, RequestBody};
use futures_util::TryStreamExt;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum OperatorAction {
    Inspect,
    Status,
    History {
        cursor: u64,
    },
    Start {
        params: serde_json::Value,
    },
    Pause,
    Resume,
    Restart,
    Terminate {
        #[serde(default)]
        rollback: bool,
    },
    SendEvent {
        event_type: String,
        payload: serde_json::Value,
    },
}

impl OperatorAction {
    pub fn is_read(&self) -> bool {
        matches!(self, Self::Inspect | Self::Status | Self::History { .. })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    binding: String,
    instance: Option<String>,
    command: OperatorAction,
    expected_generation: Option<String>,
}

impl InstanceCell {
    pub(super) async fn operator(&self, context: &Context, body: RequestBody) -> HttpResponse {
        let result = async {
            let mut stream = context.take_body(body)?;
            let mut bytes = Vec::new();
            while let Some(chunk) = stream.try_next().await.map_err(anyhow::Error::msg)? {
                ensure!(
                    bytes.len().saturating_add(chunk.len()) <= 2 * VALUE_LIMIT,
                    "workflow operator body is too large"
                );
                bytes.extend(chunk);
            }
            let request: Request = serde_json::from_slice(&bytes)?;
            let workflow = context.workflow(&request.binding)?;
            let existing = read(&self.storage, META)?;
            // Inventory may visit another binding's instance in the same application.
            if matches!(request.command, OperatorAction::Inspect)
                && existing.as_ref().is_none_or(|meta| {
                    text(meta, "workflowName").ok().as_deref() != Some(workflow.name())
                })
            {
                return Ok(serde_json::Value::Null);
            }
            let id = request
                .instance
                .or_else(|| {
                    existing
                        .as_ref()
                        .and_then(|meta| text(meta, "instanceId").ok())
                })
                .ok_or_else(|| error("instance does not exist"))?;
            check_id(&id)?;
            ensure!(
                    context.identity().is_some_and(
                        |identity| identity.scope == workflow.instance(id.clone()).scope()
                    ),
                    "workflow instance identity mismatch"
                );
            if let Some(meta) = &existing {
                ensure!(
                    text(meta, "workflowName")? == workflow.name()
                        && text(meta, "instanceId")? == id,
                    "workflow metadata identity mismatch"
                );
            }
            match request.command {
                OperatorAction::Inspect | OperatorAction::Status => {
                    let meta = existing.ok_or_else(|| error("instance does not exist"))?;
                    let mut result = serde_json::Map::new();
                    let fields: &[&str] = if matches!(request.command, OperatorAction::Inspect) {
                        &[
                            "workflowName",
                            "instanceId",
                            "generation",
                            "createdMs",
                            "endedMs",
                            "expiresMs",
                            "status",
                        ]
                    } else {
                        &[
                            "workflowName",
                            "instanceId",
                            "generation",
                            "createdMs",
                            "endedMs",
                            "expiresMs",
                            "status",
                            "error",
                            "output",
                            "rollback",
                            "params",
                            "definitionSnapshot",
                        ]
                    };
                    for field in fields {
                        if let Some(value) = property(&meta, field)? {
                            result.insert((*field).into(), operator_json(&value)?);
                        }
                    }
                    result.insert(
                        "retention_expired".into(),
                        json!(
                            terminal(&meta)?
                                && property(&meta, "expiresMs")?.is_some()
                                && number(&meta, "expiresMs")? <= now() as f64
                        ),
                    );
                    Ok(serde_json::Value::Object(result))
                }
                OperatorAction::History { cursor } => operator_history(&self.storage, &id, cursor),
                action => {
                    if !matches!(action,OperatorAction::Start {..}) {
                        let meta = existing.as_ref().ok_or_else(|| error("instance does not exist"))?;
                        ensure!(request.expected_generation.as_deref() == Some(text(meta,"generation")?.as_str()), "workflow generation changed or was not provided; inspect the instance before acting");
                    }
                    let (method, argument) = match action {
                        OperatorAction::Start { params } => {
                            let (_, argument) = workflow.options(CreateOptions {
                                id: Some(id),
                                params: Some(data(params)?),
                                ..Default::default()
                            })?;
                            ("__wfCreate", argument)
                        }
                        OperatorAction::Pause => ("__wfPause", data(json!({}))?),
                        OperatorAction::Resume => ("__wfResume", data(json!({}))?),
                        OperatorAction::Restart => ("__wfRestart", data(json!({}))?),
                        OperatorAction::Terminate { rollback } => {
                            ("__wfTerminate", data(json!({"rollback":rollback}))?)
                        }
                        OperatorAction::SendEvent {
                            event_type,
                            payload,
                        } => (
                            "__wfSendEvent",
                            data(json!({"type":event_type,"payload":payload}))?,
                        ),
                        _ => unreachable!("read actions handled above"),
                    };
                    let value = self.control(context, method, argument)?;
                    self.changed.notify_waiters();
                    operator_json(&value)
                }
            }
        }
        .await;
        let (status, body) = match result {
            Ok(value) => (200, json!({"result":value})),
            Err(error) => (400, json!({"error":error.to_string()})),
        };
        HttpResponse {
            status,
            body: serde_json::to_vec(&body).expect("JSON operator response"),
            stream: None,
            headers: vec![("content-type".into(), "application/json".into())],
            websocket: None,
            write_position: None,
            observed_position: None,
        }
    }
}

fn operator_history(
    storage: &crate::application::Storage,
    instance: &str,
    cursor: u64,
) -> Result<serde_json::Value> {
    let page = super::history::page(storage, &data(json!({ "cursor": cursor }))?)?;
    // Convert fields individually: a structured output must not hide the timeline.
    let mut event = property(&page, "events")?.ok_or_else(|| error("missing events"))?;
    let roots = match event.root {
        CloneValue::Reference(index) => match event.objects.get(index as usize) {
            Some(cellar_native::clone::Object::DenseArray { elements, .. }) => elements.clone(),
            _ => return Err(error("invalid history events")),
        },
        _ => return Err(error("invalid history events")),
    };
    let mut output = Vec::new();
    let mut definitions = serde_json::Map::new();
    for root in roots {
        event.root = root;
        let mut value = serde_json::Map::new();
        for field in [
            "type",
            "instanceId",
            "eventId",
            "timestamp",
            "generation",
            "stepName",
            "count",
            "attempt",
            "config",
            "input",
            "output",
            "error",
            "params",
            "durationMs",
            "eventType",
        ] {
            if let Some(field_value) = property(&event, field)? {
                value.insert(field.into(), operator_json(&field_value)?);
            }
        }
        if let Some(generation) = value.get("generation").and_then(serde_json::Value::as_str) {
            if !definitions.contains_key(generation) {
                if let Some(snapshot) = read(storage, &format!("__wf.definition.{generation}"))? {
                    definitions.insert(generation.into(), operator_json(&snapshot)?);
                }
            }
        }
        output.push(serde_json::Value::Object(value));
    }
    let done = property(&page, "done")?.ok_or_else(|| error("missing history completion"))?;
    Ok(json!({
        "events": output,
        "definitions": definitions,
        "instanceId": instance,
        "cursor": cursor,
        "done": operator_json(&done)?,
        "count": output.len(),
    }))
}

fn operator_json(value: &StructuredValue) -> Result<serde_json::Value> {
    use base64::Engine;
    if value.root == CloneValue::Undefined {
        return Ok(serde_json::Value::Null);
    }
    match value.to_json() {
        Ok(value) => Ok(value),
        Err(_) => Ok(
            json!({"encoding":"cellar_structured_clone","base64":base64::engine::general_purpose::STANDARD.encode(cellar_native::clone::encode(value)?)}),
        ),
    }
}
