// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::FleetClient;
use anyhow::{ensure, Context, Result};
use futures_util::{stream, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PageRequest {
    pub cursor: Option<String>,
    pub prefix: Option<String>,
    pub limit: Option<usize>,
}

impl PageRequest {
    pub fn limit(&self) -> Result<usize> {
        let limit = self.limit.unwrap_or(50);
        ensure!(
            (1..=100).contains(&limit),
            "limit must be between 1 and 100"
        );
        ensure!(
            self.cursor.as_ref().is_none_or(|v| v.len() <= 16384),
            "cursor is too long"
        );
        Ok(limit)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_cursor: Option<String>,
    pub observed_at: u64,
    pub coverage: &'static str,
    pub warnings: Vec<String>,
}

impl<T> Page<T> {
    pub(super) fn new(items: Vec<T>, next_cursor: Option<String>, warnings: Vec<String>) -> Self {
        Self {
            coverage: if next_cursor.is_some() || !warnings.is_empty() {
                "partial"
            } else {
                "complete"
            },
            items,
            next_cursor,
            observed_at: crate::ownership_store::now_ms(),
            warnings,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct NodeView {
    pub id: String,
    pub generation: String,
    pub address: String,
    pub lease_expires_at: u64,
    pub lease_current: bool,
    pub protocol: u16,
    pub load: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CellView {
    pub scope: String,
    pub class: Option<String>,
    pub kind: String,
    pub owner: Option<String>,
    pub epoch: Option<u64>,
    pub observed_at: u64,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BindingView {
    pub name: String,
    pub kind: String,
    pub identity: String,
    pub scope: Option<String>,
    pub schedules: Vec<String>,
    pub definition: Option<crate::application::workflow::Definition>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<EventBindingView>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EventBindingView {
    pub resource: crate::application::event_stream::ResourceConfig,
    pub publish: bool,
    pub read: bool,
    pub administer: bool,
    pub subscriptions: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApplicationView {
    pub name: String,
    pub version: String,
    pub revision: String,
    pub primary: bool,
    pub classes: Vec<String>,
    pub bindings: Vec<BindingView>,
    pub crons: Vec<String>,
}

impl FleetClient {
    pub(super) async fn application_manifest(
        &self,
        name: &str,
    ) -> Result<(crate::protocol::Manifest, String, String)> {
        ensure!(
            !name.is_empty()
                && name.len() <= 128
                && name
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"-_".contains(&c)),
            "invalid application name"
        );
        let (bytes, revision) = self
            .bucket
            .get(&format!("deploy/{name}/current.json"))
            .await?
            .context("application not found")?;
        let pointer: crate::protocol::DeployPointer = serde_json::from_slice(&bytes)?;
        let (bytes, _) = self
            .bucket
            .get(&format!("{}/manifest.json", pointer.prefix))
            .await?
            .context("manifest unavailable")?;
        let manifest: crate::protocol::Manifest = serde_json::from_slice(&bytes)?;
        ensure!(
            manifest.script_name == name && manifest.version == pointer.version,
            "application identity or version mismatch"
        );
        Ok((manifest, revision, pointer.prefix))
    }

    pub async fn application(&self, name: &str) -> Result<ApplicationView> {
        let (manifest, revision, prefix) = self.application_manifest(name).await?;
        let primary = self
            .bucket
            .get("deploy/current.json")
            .await?
            .map(|(bytes, _)| serde_json::from_slice::<crate::protocol::DeployPointer>(&bytes))
            .transpose()?;
        Ok(application_view(
            &manifest,
            revision,
            primary.is_some_and(|current| current.prefix == prefix),
        ))
    }

    pub async fn nodes(&self, request: PageRequest) -> Result<Page<NodeView>> {
        let limit = request.limit()?;
        let page = self
            .bucket
            .objects_page("nodes", request.cursor, limit)
            .await?;
        let mut items = Vec::new();
        let mut warnings = Vec::new();
        // Bound both storage work and simultaneously retained replies.
        let mut results = stream::iter(
            page.objects
                .into_iter()
                .filter_map(|object| {
                    object
                        .location
                        .as_ref()
                        .strip_prefix("nodes/")?
                        .strip_suffix(".json")
                        .map(str::to_owned)
                })
                .map(|id| async move {
                    let result = self.node(&id).await;
                    (id, result)
                }),
        )
        .buffered(8);
        while let Some((id, result)) = results.next().await {
            match result {
                Ok(node) => items.push(node),
                Err(_) => warnings.push(format!("Could not observe node {id}")),
            }
        }
        Ok(Page::new(items, page.page_token, warnings))
    }

    pub async fn node(&self, id: &str) -> Result<NodeView> {
        ensure!(super::valid_node_id(id), "invalid node ID");
        let (bytes, _) = self
            .bucket
            .get(&format!("nodes/{id}.json"))
            .await?
            .context("node not found")?;
        let lease: crate::ownership_store::NodeLeaseWire = serde_json::from_slice(&bytes)?;
        ensure!(lease.node == id, "node lease identity mismatch");
        let rss = lease.load.rss_bytes;
        let mut load = serde_json::to_value(lease.load)?;
        // RSS=1 is the platform's unavailable sentinel, not one measured byte.
        if rss <= 1 {
            load["rss_bytes"] = Value::Null;
        }
        Ok(NodeView {
            id: id.into(),
            generation: lease.generation,
            address: lease.addr,
            lease_expires_at: lease.expires_ms,
            lease_current: lease.expires_ms > crate::ownership_store::now_ms(),
            protocol: lease.peer_protocol,
            load,
        })
    }

    pub async fn cells(&self, request: PageRequest) -> Result<Page<CellView>> {
        let limit = request.limit()?;
        let prefix = request.prefix.unwrap_or_default();
        ensure!(
            prefix.is_empty() || cellar_logic::cell::valid_cell_scope(&prefix),
            "invalid cell prefix"
        );
        let page = self
            .bucket
            .common_prefixes_page(&format!("cells/{prefix}"), None, request.cursor, limit)
            .await?;
        let mut items = Vec::new();
        let mut warnings = Vec::new();
        let mut results = stream::iter(
            page.prefixes
                .into_iter()
                .filter_map(|prefix| {
                    prefix
                        .strip_prefix("cells/")
                        .filter(|scope| cellar_logic::cell::valid_cell_scope(scope))
                        .map(str::to_owned)
                })
                .map(|scope| async move {
                    let result = self.cell(&scope).await;
                    (scope, result)
                }),
        )
        .buffered(8);
        while let Some((scope, result)) = results.next().await {
            match result {
                Ok(cell) => {
                    if let Some(error) = &cell.error {
                        warnings.push(format!("{scope}: {error}"));
                    }
                    items.push(cell);
                }
                Err(_) => {
                    warnings.push(format!("Could not observe owner of {scope}"));
                    let mut cell = cell_view(&scope);
                    cell.error = Some("Ownership observation unavailable".into());
                    items.push(cell);
                }
            }
        }
        Ok(Page::new(items, page.page_token, warnings))
    }

    pub async fn cell(&self, scope: &str) -> Result<CellView> {
        ensure!(
            cellar_logic::cell::valid_cell_scope(scope),
            "invalid cell scope"
        );
        let mut cell = cell_view(scope);
        if let Some((bytes, _)) = self.bucket.get(&format!("cells/{scope}/own.json")).await? {
            #[derive(Deserialize)]
            struct Owner {
                node: String,
                epoch: u64,
            }
            let owner: Owner = serde_json::from_slice(&bytes)?;
            cell.owner = (!owner.node.is_empty()).then_some(owner.node);
            cell.epoch = Some(owner.epoch);
        } else {
            cell.error = Some("No ownership record observed; the cell may not exist or may not have been activated".into());
        }
        Ok(cell)
    }

    pub async fn applications(&self, request: PageRequest) -> Result<Page<ApplicationView>> {
        let limit = request.limit()?;
        let page = self
            .bucket
            .common_prefixes_page("deploy/", None, request.cursor, limit)
            .await?;
        let primary = self
            .bucket
            .get("deploy/current.json")
            .await?
            .map(|(bytes, _)| serde_json::from_slice::<crate::protocol::DeployPointer>(&bytes))
            .transpose()?;
        let mut items = Vec::new();
        let mut warnings = Vec::new();
        for prefix in page.prefixes {
            let result = async {
                let Some((bytes, revision)) =
                    self.bucket.get(&format!("{prefix}/current.json")).await?
                else {
                    return Ok(None);
                };
                let pointer: crate::protocol::DeployPointer = serde_json::from_slice(&bytes)?;
                let (bytes, _) = self
                    .bucket
                    .get(&format!("{}/manifest.json", pointer.prefix))
                    .await?
                    .context("manifest unavailable")?;
                let manifest: crate::protocol::Manifest = serde_json::from_slice(&bytes)?;
                Ok::<_, anyhow::Error>(Some(application_view(
                    &manifest,
                    revision,
                    primary.as_ref().is_some_and(|p| p.prefix == pointer.prefix),
                )))
            }
            .await;
            match result {
                Ok(Some(view)) => items.push(view),
                Ok(None) => {}
                Err(_) => warnings.push(format!("Could not observe application at {prefix}")),
            }
        }
        Ok(Page::new(items, page.page_token, warnings))
    }
}

fn cell_view(scope: &str) -> CellView {
    let class = scope.split_once(':').map(|(class, _)| class.to_string());
    let kind = match class.as_deref() {
        Some(crate::deploy::SQL_CLASS) => "sql",
        Some(crate::deploy::KV_CLASS) => "kv",
        Some(crate::deploy::QUEUE_CLASS) => "queue",
        Some(crate::deploy::EVENT_STREAM_CLASS) => "event_stream",
        Some(crate::deploy::EVENT_CATALOG_CLASS) => "event_catalog",
        Some(cellar_logic::cron::RESERVED_CLASS) => "cron",
        Some(class) if crate::deploy::is_workflow_class(class) => "workflow",
        _ => "application",
    };
    CellView {
        scope: scope.into(),
        class,
        kind: kind.into(),
        owner: None,
        epoch: None,
        observed_at: crate::ownership_store::now_ms(),
        error: None,
    }
}

fn application_view(
    manifest: &crate::protocol::Manifest,
    revision: String,
    primary: bool,
) -> ApplicationView {
    // Never serialize raw_metadata: it also holds plain-text secret bindings.
    let mut bindings: Vec<BindingView> = manifest.raw_metadata["bindings"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(binding_view)
        .collect();
    for value in manifest.raw_metadata["stream_bindings"]
        .as_array()
        .into_iter()
        .flatten()
        .take(128)
    {
        let Ok(binding) = serde_json::from_value::<crate::application::event_stream::BindingConfig>(
            value.clone(),
        ) else {
            continue;
        };
        if binding.validate().is_err() || binding.principal != manifest.script_name {
            continue;
        }
        let identity = binding.resource.id.clone().unwrap_or_default();
        let scope = binding.explicit().ok().and_then(|binding| {
            crate::application::event_stream::cell_scope(&binding.resource).ok()
        });
        bindings.push(BindingView {
            name: binding.environment,
            kind: "event_stream".into(),
            identity,
            scope,
            schedules: Vec::new(),
            definition: None,
            event: Some(EventBindingView {
                resource: binding.resource,
                publish: binding.publish,
                read: binding.read,
                administer: binding.administer,
                subscriptions: binding.subscriptions,
            }),
        });
    }
    ApplicationView {
        name: manifest.script_name.clone(),
        version: manifest.version.clone(),
        revision,
        primary,
        classes: manifest.do_classes.clone(),
        bindings,
        crons: manifest.crons.clone(),
    }
}

fn binding_view(value: &Value) -> Option<BindingView> {
    let kind = value["type"].as_str()?;
    let identity = value[match kind {
        "sql" => "database_id",
        "kv" => "id",
        "queue" => "queue",
        "workflow" => "workflow_name",
        "durable_object_namespace" => "class_name",
        "service" => "service",
        "blob_bucket" => "bucket_name",
        _ => return None,
    }]
    .as_str()?
    .to_string();
    let scope = match kind {
        "sql" => Some(crate::engine_api::sql_cell_scope(&identity)),
        "kv" => Some(crate::engine_api::kv_cell_scope(&identity, 0)),
        "queue" => Some(crate::engine_api::queue_cell_scope(&identity)),
        _ => None,
    };
    Some(BindingView {
        name: value["name"].as_str()?.into(),
        kind: kind.into(),
        identity,
        scope,
        definition: if kind == "workflow" {
            value
                .get("definition")
                .filter(|v| !v.is_null())
                .and_then(|v| {
                    serde_json::from_value::<crate::application::workflow::Definition>(v.clone())
                        .ok()
                })
                .filter(|v| v.validate().is_ok())
        } else {
            None
        },
        schedules: value["schedules"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect(),
        event: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn stream_metadata_excludes_grants_and_keeps_named_selectors() -> Result<()> {
        let manifest: crate::protocol::Manifest = serde_json::from_value(json!({
            "schema_version":1,"version":"v1","script_name":"app","main_module":null,
            "do_classes":[],"sqlite_classes":[],"modules":[],
            "raw_metadata":{"stream_bindings":[{"environment":"EVENTS","resource":{"tenant":"team","application":"app","name":"events","family":"changed"},"principal":"app","grant":"private-grant-marker","publish":true,"read":true,"administer":true,"subscriptions":[]}]}
        }))?;
        let view = application_view(&manifest, "revision".into(), true);
        assert_eq!(view.bindings.len(), 1);
        assert_eq!(view.bindings[0].kind, "event_stream");
        assert!(view.bindings[0].scope.is_none());
        assert!(view.bindings[0]
            .event
            .as_ref()
            .unwrap()
            .resource
            .id
            .is_none());
        assert!(!serde_json::to_string(&view)?.contains("private-grant-marker"));
        Ok(())
    }

    #[test]
    fn metadata_excludes_secrets_and_keeps_shared_identity() {
        assert!(
            binding_view(&json!({"type":"plain_text", "name":"TOKEN", "text":"secret"})).is_none()
        );
        let one =
            binding_view(&json!({"type":"sql", "name":"DB", "database_id":"shared"})).unwrap();
        let two =
            binding_view(&json!({"type":"sql", "name":"OTHER", "database_id":"shared"})).unwrap();
        assert_eq!(one.scope, two.scope);
        assert_eq!(cell_view(".cron:app").kind, "cron");
    }

    #[test]
    fn page_limits_cannot_request_a_fleet_scan() {
        assert!(PageRequest {
            limit: Some(0),
            ..Default::default()
        }
        .limit()
        .is_err());
        assert!(PageRequest {
            limit: Some(101),
            ..Default::default()
        }
        .limit()
        .is_err());
    }
}
