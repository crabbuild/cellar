// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{lifecycle::Sandbox, transport, Config};
use crate::{Limits, Operation};
use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::sdk::Context;
use serde_json::{json, Value};

impl Config {
    async fn stat(&self, context: &Context, sandbox: &Sandbox, path: &str) -> Result<Value> {
        let value = self
            .rpc(
                context,
                sandbox,
                "filesystem.Filesystem/Stat",
                json!({"path":path}),
                None,
                transport::CONTROL_BYTES,
            )
            .await?;
        metadata(&value["entry"])
    }

    pub(super) async fn file(
        &self,
        context: &Context,
        operation: Operation,
        input: Value,
        _limits: &Limits,
    ) -> Result<Value> {
        let sandbox = self.connected(context).await?;
        let path = input["path"].as_str().unwrap();
        match operation {
            Operation::ReadFile => {
                let entry = self.stat(context, &sandbox, path).await?;
                ensure!(
                    matches!(entry["kind"].as_str(), Some("file" | "symlink")),
                    "E2B read requires a regular file"
                );
                let query = url::form_urlencoded::Serializer::new(String::new())
                    .append_pair("path", path)
                    .append_pair("user", "root")
                    .finish();
                let reply = transport::request(
                    context,
                    self.envd_url(&sandbox.id, &format!("files?{query}"))?,
                    "GET",
                    self.headers(&sandbox, None, "application/octet-stream")?,
                    None,
                    input["max_bytes"].as_u64().unwrap() as usize,
                    self.timeout_ms,
                )
                .await?;
                ensure!(
                    reply.status == 200,
                    "E2B file read failed: HTTP {}",
                    reply.status
                );
                Ok(json!(STANDARD.encode(reply.body)))
            }
            Operation::WriteFile => {
                // tee uses open/truncate on an existing parent, without the envd
                // upload endpoint's implicit mkdir. No shell command is constructed.
                let bytes = STANDARD.decode(input["data"].as_str().unwrap())?;
                let written = self
                    .command(
                        context,
                        &sandbox,
                        &json!({"cmd":["/usr/bin/tee","--",path]}),
                        bytes.len().saturating_add(4096),
                        Some(&bytes),
                    )
                    .await?;
                ensure!(
                    written["exit_code"] == 0,
                    "E2B file writer failed after submission"
                );
                let echoed = STANDARD.decode(written["stdout"].as_str().unwrap())?;
                ensure!(
                    echoed == bytes,
                    "E2B file writer did not consume the exact input"
                );
                let mode = input["mode"].as_u64().unwrap();
                let changed = self
                    .command(
                        context,
                        &sandbox,
                        &json!({"cmd":["/bin/chmod","--",format!("{mode:o}"),path]}),
                        4096,
                        None,
                    )
                    .await?;
                ensure!(
                    changed["exit_code"] == 0,
                    "E2B permission update failed after file write"
                );
                Ok(Value::Null)
            }
            Operation::StatFile => self.stat(context, &sandbox, path).await,
            Operation::ListFiles => {
                let maximum = input["max_bytes"].as_u64().unwrap() as usize;
                let value = self
                    .rpc(
                        context,
                        &sandbox,
                        "filesystem.Filesystem/ListDir",
                        json!({"path":path,"depth":1}),
                        None,
                        maximum.saturating_add(16 * 1024),
                    )
                    .await?;
                let mut entries = value.get("entries").map_or(Ok(Vec::new()), |value| {
                    value
                        .as_array()
                        .context("invalid E2B directory entries")
                        .and_then(|values| values.iter().map(metadata).collect())
                })?;
                entries.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
                let result = Value::Array(entries);
                crate::files::validate_listing(
                    &result,
                    maximum,
                    input["max_entries"].as_u64().unwrap() as usize,
                )?;
                Ok(result)
            }
            _ => anyhow::bail!("unsupported E2B file operation"),
        }
    }
}

fn metadata(value: &Value) -> Result<Value> {
    let name = value["name"].as_str().context("E2B file name missing")?;
    ensure!(
        name == "/" || (!matches!(name, "." | "..") && !name.contains('/')),
        "invalid E2B file basename"
    );
    ensure!(
        value
            .get("isSymlink")
            .is_none_or(|value| value.is_null() || value.is_boolean()),
        "invalid E2B symlink flag"
    );
    let kind = if value["isSymlink"] == true {
        "symlink"
    } else {
        match &value["type"] {
            Value::String(v) if v == "FILE_TYPE_FILE" => "file",
            Value::String(v) if v == "FILE_TYPE_DIRECTORY" => "directory",
            Value::String(v) if v == "FILE_TYPE_SYMLINK" => "symlink",
            Value::String(v) if v == "FILE_TYPE_UNSPECIFIED" => "other",
            Value::Number(v) => match v.as_u64() {
                Some(0) => "other",
                Some(1) => "file",
                Some(2) => "directory",
                Some(3) => "symlink",
                _ => anyhow::bail!("unknown E2B file type"),
            },
            Value::Null => "other",
            _ => anyhow::bail!("unknown E2B file type"),
        }
    };
    let size = match &value["size"] {
        Value::Null => 0,
        Value::String(v) => v.parse::<u64>()?,
        v => v.as_u64().context("invalid E2B file size")?,
    };
    let mode = value
        .get("mode")
        .map_or(Some(0), Value::as_u64)
        .context("invalid E2B file mode")?;
    ensure!(
        u32::try_from(mode).is_ok(),
        "invalid E2B file permission word"
    );
    let modified = match &value["modifiedTime"] {
        Value::String(time) => Some(chrono::DateTime::parse_from_rfc3339(time)?.timestamp_millis()),
        Value::Null => None,
        _ => anyhow::bail!("invalid E2B file modification timestamp"),
    };
    ensure!(
        value["symlinkTarget"].is_null() || value["symlinkTarget"].is_string(),
        "invalid E2B symlink target"
    );
    let result = json!({"name":name,"kind":kind,"size":size,"mode":mode & 0o777,"modified_ms":modified,"link_target":value["symlinkTarget"].as_str()});
    crate::files::validate_metadata(&result, transport::CONTROL_BYTES)?;
    Ok(result)
}

#[cfg(test)]
#[path = "files_tests.rs"]
mod tests;
