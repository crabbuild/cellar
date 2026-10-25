// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Optional Workers/DO operations. Every resource belongs to one native event.
use crate::Limits;
use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{Context, InvocationAuthority};
use serde_json::Value;
use std::sync::OnceLock;
mod kv;
mod namespace;
pub mod rpc;
mod sql;

#[derive(Default)]
pub struct State {
    authority: OnceLock<InvocationAuthority>,
    sql: sql::Cursors,
}

pub fn is_read(name: &str) -> bool {
    matches!(
        name,
        "workers.lifetime.response"
            | "workers.container.running"
            | "workers.container.admit"
            | "workers.container.discard"
            | "workers.crypto.random"
            | "workers.crypto.uuid"
            | "workers.identity"
            | "workers.namespace"
            | "workers.callback.begin"
            | "workers.sql.size"
            | "workers.kv.get"
            | "workers.kv.list"
    )
}

pub fn supports_synchronous(name: &str) -> bool {
    matches!(
        name,
        "workers.callback.rollback"
            | "workers.container.running"
            | "workers.container.admit"
            | "workers.container.discard"
            | "workers.crypto.random"
            | "workers.crypto.uuid"
            | "workers.identity"
            | "workers.namespace"
            | "workers.sql.exec"
            | "workers.sql.next"
            | "workers.sql.close"
            | "workers.sql.size"
            | "workers.kv.get"
            | "workers.kv.put"
            | "workers.kv.delete"
            | "workers.kv.list"
            | "workers.kv.delete_all"
    )
}

impl State {
    pub async fn execute(
        &self,
        context: &Context,
        name: &str,
        input: Value,
        options: &Limits,
    ) -> Result<Value> {
        (|| -> Result<()> {
            crate::values::check_depth(&input)?;
            ensure!(input.is_object(), "Workers operation requires an object");
            ensure!(
                supports_synchronous(name) || name == "workers.rpc",
                "unsupported Workers operation"
            );
            let fields: Option<&[&str]> = match name {
                "workers.crypto.random" => Some(&["length"]),
                "workers.crypto.uuid" | "workers.identity" | "workers.sql.size" => Some(&[]),
                "workers.sql.exec" => Some(&["sql", "params"]),
                "workers.sql.next" | "workers.sql.close" => Some(&["id"]),
                _ => None,
            };
            if let Some(fields) = fields {
                ensure!(
                    input
                        .as_object()
                        .unwrap()
                        .keys()
                        .all(|key| fields.contains(&key.as_str())),
                    "unknown Workers field"
                );
            }
            Ok(())
        })()
        .context(crate::failure::Validation)?;
        let candidate = context.invocation_authority()?;
        self.authority.get_or_init(|| candidate).check(context)?;
        if name == "workers.namespace" {
            return namespace::identity(context, input);
        }
        if name == "workers.rpc" {
            return namespace::call(context, input, options).await;
        }
        if name == "workers.crypto.random" {
            let length = input["length"]
                .as_u64()
                .filter(|length| *length <= 65_536 && *length <= options.max_body_bytes as u64)
                .context("random-values quota exceeded")
                .context(crate::failure::Validation)?;
            let mut bytes = vec![0; length as usize];
            cellar::native::crypto::random_values(&mut bytes)?;
            use base64::Engine as _;
            return Ok(serde_json::json!(
                base64::engine::general_purpose::STANDARD.encode(bytes)
            ));
        }
        if name == "workers.crypto.uuid" {
            return Ok(serde_json::json!(cellar::native::crypto::random_uuid()?));
        }
        if name == "workers.identity" {
            let identity = context.identity().context("not a Durable Object")?;
            let (_, id) = identity
                .scope
                .split_once(':')
                .context("invalid native cell identity")?;
            let id = context
                .exported_namespace(&identity.class)?
                .id_from_string(id)?;
            return Ok(
                serde_json::json!({"id":id.to_string(), "name":identity.name.as_deref().map(|name| name.encode_utf16().collect::<Vec<_>>())}),
            );
        }
        if name.starts_with("workers.sql.") {
            self.sql.execute(context, name, input, options)
        } else {
            kv::execute(context, name, input, options)
        }
    }
}
