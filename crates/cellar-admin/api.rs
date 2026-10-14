// Management-server static assets are outside every node execution domain.
#![allow(clippy::disallowed_methods)]

use crate::{audit::Begin, Console, Fleet, Permission, Principal};
use axum::{
    extract::{Path, Query, State},
    http::{header, StatusCode, Uri},
    response::{IntoResponse, Response},
    Extension, Json,
};
use base64::Engine;
use cellar::management::{PageRequest, QueueAction, WorkflowAction};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;

pub(crate) struct ApiError(StatusCode, String);

impl ApiError {
    pub(crate) fn forbidden() -> Self {
        Self(
            StatusCode::FORBIDDEN,
            "You do not have access to this operation.".into(),
        )
    }

    pub(crate) fn unauthorized() -> Self {
        Self(
            StatusCode::UNAUTHORIZED,
            "Sign in with a dashboard access token.".into(),
        )
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(value: anyhow::Error) -> Self {
        Self(StatusCode::BAD_REQUEST, value.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error": self.1}))).into_response()
    }
}

fn fleet<'a>(
    console: &'a Console,
    principal: &Principal,
    id: &str,
    permission: Permission,
) -> Result<&'a Fleet, ApiError> {
    principal.require(id, permission)?;
    console.fleets.get(id).ok_or_else(ApiError::forbidden)
}

pub(crate) async fn session(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
) -> Json<Value> {
    Json(
        json!({"user": principal.id, "fleets": console.fleets.values().filter(|fleet| principal.allows(&fleet.info.id, Permission::MetadataRead)).map(|fleet| &fleet.info).collect::<Vec<_>>(), "grants": principal.grants}),
    )
}

pub(crate) async fn nodes(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
    Query(page): Query<PageRequest>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        serde_json::to_value(
            fleet(&console, &principal, &id, Permission::MetadataRead)?
                .client
                .nodes(page)
                .await?,
        )
        .map_err(anyhow::Error::from)?,
    ))
}

pub(crate) async fn node(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((id, node)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        serde_json::to_value(
            fleet(&console, &principal, &id, Permission::MetadataRead)?
                .client
                .node(&node)
                .await?,
        )
        .map_err(anyhow::Error::from)?,
    ))
}

pub(crate) async fn probe(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((id, node)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        fleet(&console, &principal, &id, Permission::MetadataRead)?
            .client
            .probe(&node)
            .await?,
    ))
}

pub(crate) async fn cells(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
    Query(page): Query<PageRequest>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        serde_json::to_value(
            fleet(&console, &principal, &id, Permission::MetadataRead)?
                .client
                .cells(page)
                .await?,
        )
        .map_err(anyhow::Error::from)?,
    ))
}

pub(crate) async fn cell(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((id, scope)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        serde_json::to_value(
            fleet(&console, &principal, &id, Permission::MetadataRead)?
                .client
                .cell(&scope)
                .await?,
        )
        .map_err(anyhow::Error::from)?,
    ))
}

pub(crate) async fn applications(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
    Query(page): Query<PageRequest>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        serde_json::to_value(
            fleet(&console, &principal, &id, Permission::MetadataRead)?
                .client
                .applications(page)
                .await?,
        )
        .map_err(anyhow::Error::from)?,
    ))
}

pub(crate) async fn application(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path((id, name)): Path<(String, String)>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        serde_json::to_value(
            fleet(&console, &principal, &id, Permission::MetadataRead)?
                .client
                .application(&name)
                .await?,
        )
        .map_err(anyhow::Error::from)?,
    ))
}

pub(crate) async fn operations(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    fleet(&console, &principal, &id, Permission::MetadataRead)?;
    Ok(Json(
        json!({"items": console.audit.list(&id)?, "coverage": "latest_100"}),
    ))
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ResourceRequest {
    application: String,
    binding: String,
    operation_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expected_identity: Option<String>,
    command: ResourceAction,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
enum ResourceAction {
    Workflow {
        instance: String,
        command: WorkflowAction,
        expected_generation: Option<String>,
    },
    WorkflowList {
        cursor: Option<String>,
    },
    SqlMigrations {
        table: String,
        after: u64,
    },
    Sql {
        sql: String,
        #[serde(default = "read_only")]
        readonly: bool,
    },
    KvList {
        #[serde(default)]
        prefix: String,
        #[serde(default)]
        after: String,
    },
    KvGet {
        key: String,
    },
    KvPut {
        key: String,
        value_base64: String,
        metadata: Option<Value>,
        expiration_ttl: Option<u64>,
    },
    KvDelete {
        key: String,
    },
    KvEdit {
        key: String,
        edit: cellar::management::KvEdit,
    },
    KvDeleteIf {
        key: String,
        condition: cellar::management::RevisionCondition,
    },
    Queue {
        operation: QueueAction,
        limit: u32,
    },
}

fn read_only() -> bool {
    true
}

impl ResourceAction {
    fn permission(&self) -> Permission {
        match self {
            Self::Workflow { command, .. } if command.is_read() => Permission::DataRead,
            Self::WorkflowList { .. } => Permission::DataRead,
            Self::SqlMigrations { .. }
            | Self::Sql { readonly: true, .. }
            | Self::KvList { .. }
            | Self::KvGet { .. }
            | Self::Queue {
                operation: QueueAction::Info | QueueAction::Peek,
                ..
            } => Permission::DataRead,
            _ => Permission::DataWrite,
        }
    }

    fn kind(&self) -> &str {
        match self {
            Self::Sql { .. } | Self::SqlMigrations { .. } => "sql",
            Self::Workflow { .. } | Self::WorkflowList { .. } => "workflow",
            Self::Queue { .. } => "queue",
            _ => "kv",
        }
    }
}

pub(crate) async fn resource_action(
    State(console): State<Arc<Console>>,
    Extension(principal): Extension<Principal>,
    Path(id): Path<String>,
    Json(request): Json<ResourceRequest>,
) -> Result<Json<Value>, ApiError> {
    let fleet = fleet(&console, &principal, &id, request.command.permission())?;
    if let ResourceAction::KvEdit { key, .. } | ResourceAction::KvDeleteIf { key, .. } =
        &request.command
    {
        principal.require(&id, Permission::DataRead)?;
        if request.expected_identity.is_none() {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "Conditional KV edits require the inspected namespace identity.".into(),
            ));
        }
        if key.is_empty() || key.len() > 512 {
            return Err(ApiError(
                StatusCode::BAD_REQUEST,
                "key must contain 1–512 bytes".into(),
            ));
        }
    }
    match &request.command {
        ResourceAction::KvEdit { edit, .. } => {
            edit.validate()
                .map_err(|error| ApiError(StatusCode::BAD_REQUEST, error.to_string()))?;
        }
        ResourceAction::KvDeleteIf { condition, .. } => {
            condition
                .validate()
                .map_err(|error| ApiError(StatusCode::BAD_REQUEST, error.to_string()))?;
        }
        _ => {}
    }
    // Writable SQL can return arbitrary rows as well as changing data.
    if matches!(
        request.command,
        ResourceAction::Sql {
            readonly: false,
            ..
        }
    ) {
        principal.require(&id, Permission::DataRead)?;
    }
    let app = fleet.client.application(&request.application).await?;
    let binding = app
        .bindings
        .iter()
        .find(|binding| binding.name == request.binding && binding.kind == request.command.kind())
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, "Resource binding not found.".into()))?;
    if request
        .expected_identity
        .as_ref()
        .is_some_and(|identity| *identity != binding.identity)
    {
        return Err(ApiError(StatusCode::CONFLICT, "This binding now targets another resource. Copy your draft before reopening the workspace.".into()));
    }
    let mut operation = if request.command.permission() == Permission::DataWrite {
        let fingerprint = format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&request).map_err(anyhow::Error::from)?)
        );
        let action = serde_json::to_value(&request.command).map_err(anyhow::Error::from)?;
        match console.audit.begin(
            &request.operation_id,
            &id,
            &principal.id,
            &format!("{}/{}", app.name, binding.name),
            action["action"].as_str().unwrap_or("operation"),
            &fingerprint,
        )? {
            Begin::Started(operation) => Some(operation),
            Begin::Existing(operation) => {
                return Ok(Json(json!({"operation": operation, "replayed": true})))
            }
        }
    } else {
        None
    };
    let result = match request.command {
        ResourceAction::Workflow {
            instance,
            command,
            expected_generation,
        } => {
            fleet
                .client
                .workflow(
                    &app.name,
                    binding,
                    &instance,
                    command,
                    expected_generation.as_deref(),
                )
                .await
        }
        ResourceAction::WorkflowList { cursor } => fleet
            .client
            .workflow_instances(
                &app.name,
                &binding.name,
                PageRequest {
                    cursor,
                    limit: Some(20),
                    prefix: None,
                },
            )
            .await
            .and_then(|page| serde_json::to_value(page).map_err(Into::into)),
        ResourceAction::SqlMigrations { table, after } => {
            fleet
                .client
                .sql_migrations(&binding.identity, &table, after)
                .await
        }
        ResourceAction::Sql { sql, readonly } => {
            fleet.client.sql(&binding.identity, &sql, readonly).await
        }
        ResourceAction::KvList { prefix, after } => {
            fleet
                .client
                .kv_list(&binding.identity, &prefix, &after)
                .await
        }
        ResourceAction::KvGet { key } => fleet.client.kv_inspect(&binding.identity, &key).await,
        ResourceAction::KvPut {
            key,
            value_base64,
            metadata,
            expiration_ttl,
        } => match base64::engine::general_purpose::STANDARD.decode(value_base64) {
            Ok(bytes) => {
                fleet
                    .client
                    .kv_put(&binding.identity, &key, bytes, metadata, expiration_ttl)
                    .await
            }
            Err(_) => Err(anyhow::anyhow!("Value must use valid base64 encoding")),
        },
        ResourceAction::KvDelete { key } => fleet.client.kv_delete(&binding.identity, &key).await,
        ResourceAction::KvEdit { key, edit } => {
            fleet.client.kv_edit(&binding.identity, &key, edit).await
        }
        ResourceAction::KvDeleteIf { key, condition } => {
            fleet
                .client
                .kv_delete_if(&binding.identity, &key, condition)
                .await
        }
        ResourceAction::Queue { operation, limit } => {
            fleet
                .client
                .queue(&binding.identity, operation, limit)
                .await
        }
    };
    if let Some(operation) = &mut operation {
        // Transport errors can follow committed writes. Until the protocol
        // supplies a definite rejection, preserve ambiguity rather than retry.
        console.audit.finish(
            operation,
            if result.as_ref().is_ok_and(|value| value["conflict"] == true) {
                "rejected"
            } else if result.is_ok() {
                "succeeded"
            } else {
                "outcome_unknown"
            },
        )?;
    }
    match result {
        Ok(value) if value["conflict"] == true => Err(ApiError(StatusCode::CONFLICT, "This key changed or expired after it was inspected. Reload the latest value before saving or deleting.".into())),
        Ok(value) => Ok(Json(json!({"result": value, "operation": operation}))),
        Err(error) => Ok(Json(
            json!({"error": error.to_string(), "operation": operation}),
        )),
    }
}

pub(crate) async fn asset(State(console): State<Arc<Console>>, uri: Uri) -> Response {
    if uri.path().starts_with("/api/") {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error":"API route not found"})),
        )
            .into_response();
    }
    let relative = uri.path().trim_start_matches('/');
    if relative.split('/').any(|part| part == "..") {
        return StatusCode::NOT_FOUND.into_response();
    }
    let root = match tokio::fs::canonicalize(&console.assets).await {
        Ok(root) => root,
        Err(_) => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Dashboard assets unavailable. Run npm ci && npm run build in dashboard/.",
            )
                .into_response()
        }
    };
    let candidate = root.join(if relative.is_empty() {
        "index.html"
    } else {
        relative
    });
    let path = match tokio::fs::canonicalize(candidate).await {
        Ok(path) if path.starts_with(&root) => path,
        Ok(_) => return StatusCode::NOT_FOUND.into_response(),
        Err(_) if !relative.contains('.') => root.join("index.html"),
        Err(_) => return StatusCode::NOT_FOUND.into_response(),
    };
    let content_type = match path.extension().and_then(|v| v.to_str()) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "text/javascript",
        Some("css") => "text/css",
        Some("svg") => "image/svg+xml",
        Some("woff2") => "font/woff2",
        Some("png") => "image/png",
        _ => "application/octet-stream",
    };
    match tokio::fs::read(path).await {
        Ok(bytes) => ([(header::CONTENT_TYPE, content_type), (header::CACHE_CONTROL, "no-cache"), (header::CONTENT_SECURITY_POLICY, "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; frame-ancestors 'none'; base-uri 'none'; form-action 'self'"), (header::X_CONTENT_TYPE_OPTIONS, "nosniff")], bytes).into_response(),
        Err(_) => StatusCode::NOT_FOUND.into_response(),
    }
}
