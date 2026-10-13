//! Validate declarations before assembling runtime routes or exporting documents.
use anyhow::{ensure, Result};
use serde_json::{json, Value};
use std::collections::HashSet;
use utoipa::openapi::OpenApi;

const METHODS: &[&str] = &[
    "get", "put", "post", "delete", "options", "head", "patch", "trace",
];

pub(crate) fn validate(api: &OpenApi) -> Result<()> {
    let value = serde_json::to_value(api)?;
    ensure!(
        value["openapi"] == "3.1.0",
        "this adapter currently accepts OpenAPI 3.1.0 documents"
    );
    let mut ids = HashSet::new();
    let mut route_patterns = HashSet::new();
    if let Some(paths) = value["paths"].as_object() {
        for (path, item) in paths {
            ensure!(
                path.starts_with('/') && !path.contains(['?', '#']),
                "invalid OpenAPI path {path}"
            );
            ensure!(
                route_patterns.insert(crate::builder::normalized_path(path)),
                "ambiguous OpenAPI path {path}"
            );
            ensure!(
                item.get("query").is_none() && item.get("additionalOperations").is_none(),
                "OpenAPI QUERY and custom operations are not mapped by this adapter"
            );
            for method in METHODS {
                if let Some(operation) = item.get(*method) {
                    let id = operation["operationId"].as_str();
                    ensure!(
                        id.is_some_and(|id| !id.is_empty()),
                        "operation {method} {path} requires a stable operationId"
                    );
                    ensure!(
                        ids.insert(id),
                        "duplicate operationId {}",
                        id.unwrap_or_default()
                    );
                }
            }
        }
    }
    validate_references(&value, &value)?;
    Ok(())
}

fn validate_references(root: &Value, value: &Value) -> Result<()> {
    match value {
        Value::Object(fields) => {
            if let Some(reference) = fields.get("$ref").and_then(Value::as_str) {
                ensure!(
                    reference.starts_with('#') && root.pointer(&reference[1..]).is_some(),
                    "unresolved OpenAPI reference {reference}"
                );
            }
            for value in fields.values() {
                validate_references(root, value)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                validate_references(root, value)?;
            }
        }
        _ => {}
    }
    Ok(())
}

pub(crate) fn merge(existing: &mut OpenApi, incoming: OpenApi) -> Result<()> {
    validate(&incoming)?;
    for path in incoming.paths.paths.keys() {
        ensure!(
            !existing
                .paths
                .paths
                .keys()
                .any(|existing| crate::builder::normalized_path(existing)
                    == crate::builder::normalized_path(path)),
            "public and cell routes overlap at {path}"
        );
    }
    let left = serde_json::to_value(&*existing)?;
    let right = serde_json::to_value(&incoming)?;
    if let (Some(left), Some(right)) = (
        left["components"].as_object(),
        right["components"].as_object(),
    ) {
        for (category, entries) in right {
            if let (Some(existing), Some(incoming)) = (
                left.get(category).and_then(Value::as_object),
                entries.as_object(),
            ) {
                for (name, value) in incoming {
                    ensure!(
                        existing.get(name).is_none_or(|existing| existing == value),
                        "conflicting OpenAPI component {category}.{name}"
                    );
                }
            }
        }
    }
    let mut combined = existing.clone();
    combined.merge(incoming);
    validate(&combined)?;
    *existing = combined;
    Ok(())
}

pub(crate) fn cell_parameters(api: &mut OpenApi, parameter: &str, identity: &str) -> Result<()> {
    let mut value = serde_json::to_value(&*api)?;
    if let Some(paths) = value["paths"].as_object_mut() {
        for (path, item) in paths {
            if let Some(item) = item.as_object_mut() {
                ensure!(
                    item.get("trace").is_none(),
                    "TRACE is unsupported by Cellar cell fetch at {path}"
                );
                let parameters = item.entry("parameters").or_insert_with(|| json!([]));
                if let Some(parameters) = parameters.as_array_mut() {
                    if !parameters
                        .iter()
                        .any(|existing| existing["name"] == parameter && existing["in"] == "path")
                    {
                        parameters.push(json!({"name":parameter,"in":"path","required":true,"schema":{"type":"string"}}));
                    }
                }
                for method in METHODS {
                    if let Some(operation) = item.get_mut(*method).and_then(Value::as_object_mut) {
                        let id = operation
                            .get("operationId")
                            .and_then(Value::as_str)
                            .map(str::to_owned)
                            .unwrap_or_else(|| format!("{method}.{path}"));
                        operation.insert(
                            "operationId".into(),
                            Value::String(format!("{identity}.{id}")),
                        );
                    }
                }
            }
        }
    }
    *api = serde_json::from_value(value)?;
    validate(api)
}
