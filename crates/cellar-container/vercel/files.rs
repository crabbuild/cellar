// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::Config;
use crate::{remote_http, Limits, Operation};
use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::sdk::Context;
use serde_json::{json, Value};

impl Config {
    pub(super) async fn file(
        &self,
        context: &Context,
        operation: Operation,
        input: Value,
        _limits: &Limits,
    ) -> Result<Value> {
        let resource = self.connected(context).await?;
        if operation == Operation::ReadFile {
            let response = remote_http::request(
                context,
                self.url(&format!(
                    "v2/sandboxes/sessions/{}/fs/read",
                    resource.session
                ))?,
                "POST",
                self.headers("application/json"),
                Some(serde_json::to_vec(&json!({"path":input["path"]}))?),
                input["max_bytes"].as_u64().unwrap() as usize,
                self.timeout_ms,
            )
            .await?;
            ensure!(
                response.status == 200,
                "Vercel file read failed: HTTP {}",
                response.status
            );
            ensure!(
                response
                    .headers
                    .iter()
                    .any(|(key, value)| key.eq_ignore_ascii_case("content-type")
                        && value.split(';').next() == Some("application/octet-stream")),
                "Vercel file endpoint did not return binary data"
            );
            return Ok(json!(STANDARD.encode(response.body)));
        }
        let (operation, maximum) = match operation {
            Operation::WriteFile => ("write", 4096),
            Operation::StatFile => ("stat", remote_http::CONTROL_BYTES),
            Operation::ListFiles => ("list", input["max_bytes"].as_u64().unwrap() as usize),
            _ => anyhow::bail!("unsupported Vercel file operation"),
        };
        let mut request = input;
        request["format"] = json!(1);
        request["operation"] = json!(operation);
        let reply = self
            .helper(context, &resource, request.clone(), maximum, false)
            .await?;
        ensure!(
            reply.code == 0 && reply.stdout.is_empty() && reply.stderr.is_empty(),
            "Vercel file helper returned unexpected command data"
        );
        let result = reply
            .result
            .context("Vercel file helper returned no result")?;
        if operation == "list" {
            crate::files::validate_listing(
                &result,
                maximum,
                request["max_entries"].as_u64().unwrap() as usize,
            )?;
        }
        if operation == "stat" {
            crate::files::validate_metadata(&result, maximum)?;
        }
        Ok(result)
    }
}
