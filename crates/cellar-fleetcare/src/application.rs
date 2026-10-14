use crate::config::*;
use crate::contracts::Command;
use crate::{asynchronous, state};
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{
    http::Response, Application, Cell, Context, HandlerFuture, Invocation, Output, Registry,
};
use serde_json::{json, Value};
use std::sync::Arc;

pub fn response(context: &Context, value: Value) -> Result<Output> {
    Ok(Output::Http(
        Response::from_json(&value)?.into_http(context)?,
    ))
}

pub fn class(kind: &str) -> Result<&'static str> {
    match kind {
        "devices" => Ok("Device"),
        "books" => Ok("WorkOrderBook"),
        "inventory" => Ok("Inventory"),
        _ => anyhow::bail!("unknown entity kind"),
    }
}

pub fn entity_name(tenant: usize, id: usize) -> String {
    format!("tenant-{tenant}/entity-{id}")
}

pub fn stub(
    context: &Context,
    tenant: usize,
    kind: &str,
    id: usize,
) -> Result<cellar::sdk::cells::Stub> {
    context.exported_namespace(class(kind)?)?.get(
        &context
            .exported_namespace(class(kind)?)?
            .id_from_name(&entity_name(tenant, id)),
    )
}

struct Entity {
    config: Arc<AppConfig>,
    class: String,
    tenant: usize,
    entity: usize,
    ordinary_subscriptions: std::sync::Mutex<std::collections::BTreeSet<u64>>,
}

impl Cell for Entity {
    fn invoke(self: Arc<Self>, context: Context, event: Invocation) -> HandlerFuture<Output> {
        Box::pin(async move {
            let storage = context.storage()?;
            let identity = context.identity().context("missing cell identity")?;
            let scope = identity.scope.clone();
            let key = state::EntityKey {
                class: &self.class,
                tenant: self.tenant,
                entity: self.entity,
                scope: &scope,
            };
            match event {
                Invocation::Alarm { .. } => {
                    state::process_deadlines(
                        &storage,
                        &self.config,
                        &self.class,
                        self.tenant,
                        &scope,
                        cellar::asyncrt::wall_ms(),
                    )?;
                    crate::subscriptions::broadcast(&context, &self.ordinary_subscriptions)?;
                    asynchronous::flush(&context, &self.config, self.tenant).await?;
                    state::schedule(
                        &storage,
                        &self.config,
                        &self.class,
                        cellar::asyncrt::wall_ms(),
                    )?;
                    Ok(Output::Done)
                }
                Invocation::CellRpc { method, args } => {
                    let value = cellar::sdk::decode_rpc(&args)?.to_json()?;
                    if method == "command" {
                        let command: Command = serde_json::from_value(value)?;
                        let (receipt, replay) = state::apply(
                            &storage,
                            &self.config,
                            key,
                            &command,
                            cellar::asyncrt::wall_ms(),
                        )?;
                        if receipt.accepted && !replay {
                            crate::subscriptions::broadcast(
                                &context,
                                &self.ordinary_subscriptions,
                            )?;
                        }
                        Ok(Output::Rpc(cellar::sdk::encode_rpc(
                            &cellar::sdk::clone::StructuredValue::from_json(
                                json!({"receipt": receipt, "replay": replay}),
                            )?,
                        )?))
                    } else {
                        ensure!(method == "read", "unknown RPC method");
                        Ok(Output::Rpc(cellar::sdk::encode_rpc(
                            &cellar::sdk::clone::StructuredValue::from_json(state::audit(
                                &storage,
                            )?)?,
                        )?))
                    }
                }
                Invocation::Fetch(request) => {
                    let url = url::Url::parse(&request.url)?;
                    let path = url.path();
                    if path.ends_with("/ws") {
                        ensure!(
                            self.class == "Device" && request.method == "GET",
                            "only device GET supports subscriptions"
                        );
                        return crate::subscriptions::upgrade(
                            &context,
                            self.tenant,
                            &request,
                            &self.ordinary_subscriptions,
                        );
                    }
                    if let Some((_, case)) = path.rsplit_once("/diagnostics/") {
                        ensure!(
                            self.class == "Device",
                            "only devices expose technician diagnostics"
                        );
                        return crate::diagnostics::cell(&context, self.tenant, case).await;
                    }
                    let mut value = if request.method == "POST" {
                        let bytes = request.into_body_reader(&context)?.bytes(&context).await?;
                        ensure!(bytes.len() <= 66_560, "command body exceeds limit");
                        let command: Command = serde_json::from_slice(&bytes)?;
                        let (receipt, replay) = state::apply(
                            &storage,
                            &self.config,
                            key,
                            &command,
                            cellar::asyncrt::wall_ms(),
                        )?;
                        if receipt.accepted && !replay {
                            crate::subscriptions::broadcast(
                                &context,
                                &self.ordinary_subscriptions,
                            )?;
                        }
                        if self.config.asynchronous {
                            let config = self.config.clone();
                            let child = context.clone();
                            let tenant = self.tenant;
                            context.wait_until(async move {
                                // Alarm/outbox is authoritative. An acceleration
                                // failure is retained for the alarm's next retry.
                                let _ = asynchronous::flush(&child, &config, tenant).await;
                                Ok(())
                            })?;
                        }
                        json!({"receipt": receipt, "replay": replay})
                    } else {
                        ensure!(request.method == "GET", "use GET or POST");
                        if path.contains("/receipts/") {
                            let id = path.rsplit('/').next().context("missing receipt ID")?;
                            json!({"receipt": state::receipt(&storage, id)?})
                        } else if path.ends_with("/history") {
                            let after = url
                                .query_pairs()
                                .find(|(k, _)| k == "after")
                                .map(|(_, v)| v.parse::<u64>())
                                .transpose()?
                                .unwrap_or(0);
                            let (_, rows, _) = storage.sql(
                                "SELECT version,command_id,snapshot_json FROM changes WHERE version>?1 ORDER BY version LIMIT 100",
                                &[json!(after)])?;
                            json!({"rows": rows, "version": state::state(&storage)?["version"]})
                        } else if path.contains("/orders/") {
                            json!({"order": state::order(&storage, path.rsplit('/').next().unwrap_or(""))?})
                        } else if path.contains("/reservations/") {
                            json!({"reservation": state::reservation(&storage, path.rsplit('/').next().unwrap_or(""))?})
                        } else if path.ends_with("/audit") {
                            state::audit(&storage)?
                        } else {
                            json!({"state": state::state(&storage)?})
                        }
                    };
                    value["owner"] = json!(self.config.node.as_ref());
                    value["scope"] = json!(scope);
                    value["epoch"] = json!(identity.epoch);
                    response(&context, value)
                }
                Invocation::WebSocketOpen { .. } => Ok(Output::Done),
                Invocation::WebSocketClose { id, .. } => {
                    self.ordinary_subscriptions.lock().unwrap().remove(&id);
                    Ok(Output::Done)
                }
                Invocation::WebSocketMessage { id, data } => {
                    crate::subscriptions::message(&context, id, data)
                }
                _ => anyhow::bail!("unsupported entity event"),
            }
        })
    }
}

pub fn application(config: Arc<AppConfig>) -> Result<Arc<dyn Application>> {
    let root = config.clone();
    let mut registry = Registry::new(move |context, invocation| {
        let config = root.clone();
        Box::pin(async move {
            match invocation {
                Invocation::Queue(batch) => asynchronous::consume(context, config, batch).await,
                Invocation::Scheduled(_) => {
                    asynchronous::reconcile(&context, &config).await?;
                    Ok(Output::Done)
                }
                Invocation::Fetch(request) => {
                    let allowed = request.headers.iter().any(|(key, value)| {
                        key.eq_ignore_ascii_case("authorization")
                            && value == &format!("Bearer {}", config.token)
                    });
                    if !allowed {
                        return Ok(Output::Http(
                            Response::new(401, "application authentication required", Vec::new())
                                .into_http(&context)?,
                        ));
                    }
                    let url = url::Url::parse(&request.url)?;
                    if url.path() == "/metadata" {
                        return response(
                            &context,
                            json!({
                                "application": SCRIPT, "version": VERSION, "profile": config.profile,
                                "counts": config.profile.counts(), "node": config.node.as_ref(), "asynchronous": config.asynchronous
                            }),
                        );
                    }
                    if url.path() == "/manifest" {
                        return response(&context, manifest(&context, &config)?);
                    }
                    if url.path().starts_with("/system/") {
                        return asynchronous::system(&context, &config, request).await;
                    }
                    if url.path().starts_with("/attachments/") {
                        return crate::attachments::handle(&context, &config, request).await;
                    }
                    if let Some(case) = url.path().strip_prefix("/diagnostics/") {
                        return crate::diagnostics::root(&context, &config, case).await;
                    }
                    let parts: Vec<_> = url.path().trim_matches('/').split('/').collect();
                    ensure!(
                        parts.len() >= 4 && parts[0] == "api",
                        "use /api/TENANT/KIND/ID"
                    );
                    let tenant = parts[1].parse::<usize>()?;
                    let id = parts[3].parse::<usize>()?;
                    config.validate_target(tenant, parts[2], id)?;
                    let mut result = stub(&context, tenant, parts[2], id)?
                        .fetch(&context, request)
                        .await?;
                    result
                        .headers
                        .push(("x-fleetcare-ingress".into(), config.node.to_string()));
                    Ok(Output::Http(result))
                }
                _ => anyhow::bail!("unsupported application event"),
            }
        })
    });
    registry = registry.entrypoint("Reports", |context, invocation| Box::pin(async move {
        match invocation {
            Invocation::Fetch(request) => response(&context, json!({"service":"Reports", "props":cellar::sdk::clone::decode(&request.entrypoint.context("missing report entrypoint")?.props)?.to_json()?})),
            Invocation::Rpc { .. } => Ok(Output::Bytes(cellar::sdk::clone::encode(
                &cellar::sdk::clone::StructuredValue::from_json(json!({"service":"Reports"}))?)?)),
            _ => anyhow::bail!("unsupported report service event"),
        }
    }))?;
    for kind in ["Device", "WorkOrderBook", "Inventory"] {
        let config = config.clone();
        registry = registry.cell(kind, move |context, identity| {
            state::initialize(&context.storage()?, kind)?;
            let name = identity
                .name
                .as_deref()
                .context("entity requires a stable business name")?;
            let tenant = name
                .split('/')
                .next()
                .context("missing tenant")?
                .strip_prefix("tenant-")
                .context("invalid tenant identity")?
                .parse()?;
            let entity = name
                .split('/')
                .nth(1)
                .context("missing entity")?
                .strip_prefix("entity-")
                .context("invalid entity identity")?
                .parse()?;
            Ok(Arc::new(Entity {
                config: config.clone(),
                class: kind.into(),
                tenant,
                entity,
                ordinary_subscriptions: Default::default(),
            }))
        })?;
    }
    registry = registry.workflow("Dispatch", move |context, event, steps| {
        asynchronous::dispatch(context, config.clone(), event, steps)
    })?;
    registry = registry.cell("Diagnostic", |_, _| {
        Ok(Arc::new(crate::diagnostics::Diagnostic))
    })?;
    Ok(Arc::new(registry))
}

fn manifest(context: &Context, config: &AppConfig) -> Result<Value> {
    let counts = config.profile.counts();
    let mut entries = Vec::with_capacity(counts.total());
    for tenant in 0..counts.tenants {
        for (kind, n) in [
            ("devices", counts.devices),
            ("books", counts.books),
            ("inventory", counts.inventory),
        ] {
            for id in 0..counts.in_tenant(n, tenant) {
                entries.push(json!({"kind": kind, "tenant": tenant, "id": id,
                    "scope": stub(context, tenant, kind, id)?.id().scope(),
                    "path": format!("/api/{tenant}/{kind}/{id}")}));
            }
        }
        for partition in 0..3 {
            entries.push(json!({"kind": "sql", "tenant": tenant, "id": partition,
                "scope": cellar::engine_api::sql_cell_scope(&sql_identity(tenant, partition)),
                "path": format!("/system/sql/{tenant}/{partition}")}));
        }
        entries.push(json!({"kind": "kv", "tenant": tenant, "id": 0,
            "scope": cellar::engine_api::kv_cell_scope(&kv_identity(tenant), 0),
            "path": format!("/system/kv/{tenant}/0")}));
        for slot in 0..counts.workflows / counts.tenants {
            let id = workflow_id(tenant, slot);
            entries.push(json!({"kind": "workflow", "tenant": tenant, "id": slot,
                "scope": cellar::engine_api::workflow_cell_scope(SCRIPT, "dispatch", &id),
                "path": format!("/system/workflow/{tenant}/{slot}")}));
        }
    }
    for pair in 0..counts.queues / 2 {
        for dead in [false, true] {
            entries.push(
                json!({"kind": if dead { "dlq" } else { "queue" }, "tenant": pair * 2, "id": pair,
                "scope": cellar::engine_api::queue_cell_scope(&queue_identity(pair, dead)),
                "path": format!("/system/{}/{pair}/0", if dead { "dlq" } else { "queue" })}),
            );
        }
    }
    entries.push(json!({"kind": "cron", "tenant": 0, "id": 0,
        "scope": cellar_logic::cron::reserved_cell(SCRIPT), "path": "/system/cron/0/0"}));
    ensure!(entries.len() == counts.total(), "manifest count mismatch");
    Ok(json!({"profile": config.profile, "count": entries.len(), "cells": entries}))
}
