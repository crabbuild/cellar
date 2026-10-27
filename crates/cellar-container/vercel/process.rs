// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{
    config::identifier,
    lifecycle::{pairs, Resource},
    wire::Decoder,
    Config,
};
use crate::remote_http::{self, CONTROL_BYTES};
use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::sdk::Context;
use futures_util::StreamExt;
use serde_json::{json, Value};

pub(super) const HELPER: &str = include_str!("helper.py");

pub(super) struct HelperOutput {
    pub code: i64,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub result: Option<Value>,
}

impl Config {
    pub(super) fn validate_command(&self, options: &Value) -> Result<()> {
        ensure!(
            options.get("pty").is_none_or(Value::is_null),
            "Vercel terminal execution is not qualified"
        );
        ensure!(
            options.get("user").is_none_or(Value::is_null),
            "Vercel executes as the image's configured user; arbitrary users are unsupported"
        );
        pairs(&options["env"])?;
        Ok(())
    }

    pub(super) async fn command(
        &self,
        context: &Context,
        resource: &Resource,
        options: &Value,
        maximum: usize,
    ) -> Result<Value> {
        let mut opts = json!({"cmd":options["cmd"],"env":pairs(&options["env"])?});
        if let Some(cwd) = options["cwd"].as_str() {
            opts["cwd"] = json!(cwd);
        }
        let request = json!({"format":1,"operation":"exec","options":opts,"max_bytes":maximum,"deadline_ms":self.timeout_ms});
        let output = self
            .helper(
                context,
                resource,
                request,
                maximum,
                options["combined"] == true,
            )
            .await?;
        ensure!(
            output.result.is_none(),
            "Vercel command returned a file result"
        );
        Ok(
            json!({"exit_code":output.code,"stdout":STANDARD.encode(output.stdout),"stderr":STANDARD.encode(output.stderr)}),
        )
    }

    pub(super) async fn helper(
        &self,
        context: &Context,
        resource: &Resource,
        input: Value,
        maximum: usize,
        combined: bool,
    ) -> Result<HelperOutput> {
        let data = serde_json::to_vec(&input)?;
        ensure!(
            data.len() <= 64 * 1024 * 1024,
            "Vercel helper input exceeds limit before command submission"
        );
        let request = json!({"command":self.python,"args":["-I","-c",HELPER],"env":{},"sudo":false,"wait":true,"logs":true,"attachStdin":true,"timeout":self.timeout_ms});
        let mut response = remote_http::open(
            context,
            self.url(&format!("v2/sandboxes/sessions/{}/cmd", resource.session))?,
            "POST",
            self.headers("application/json"),
            Some(serde_json::to_vec(&request)?),
            self.timeout_ms,
        )
        .await?;
        ensure!(
            response.status == 200,
            "Vercel command start failed: HTTP {}",
            response.status
        );
        ensure!(
            response
                .headers
                .iter()
                .any(|(key, value)| key.eq_ignore_ascii_case("content-type")
                    && value.split(';').next() == Some("application/x-ndjson")),
            "Vercel command response is not NDJSON"
        );
        let frame_limit = maximum
            .div_ceil(3)
            .saturating_mul(4)
            .saturating_mul(2)
            .saturating_add(CONTROL_BYTES);
        let mut outer = Decoder::new(frame_limit);
        let mut inner = Decoder::new(
            maximum
                .div_ceil(3)
                .saturating_mul(4)
                .saturating_add(CONTROL_BYTES),
        );
        let authority = context.invocation_authority()?;
        let mut command: Option<String> = None;
        let mut completed = false;
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut exit = None;
        let mut result = None;
        let (send, receive) = tokio::sync::oneshot::channel();
        let mut send = Some(send);
        let write = async {
            let id = receive
                .await
                .context("Vercel command closed before stdin admission")?;
            let reply = self
                .control(
                    context,
                    "POST",
                    &format!("v2/sandboxes/sessions/{}/cmd/{id}/stdin", resource.session),
                    Some(json!({"data":STANDARD.encode(&data),"offset":0,"close":true})),
                )
                .await?;
            ensure!(
                reply.status == 200,
                "Vercel stdin submission failed: HTTP {}",
                reply.status
            );
            let value: Value = serde_json::from_slice(&reply.body)?;
            if let Some(count) = value.get("bytesWritten") {
                ensure!(
                    count.as_u64() == Some(data.len() as u64),
                    "Vercel stdin acknowledgement is incomplete"
                );
            }
            Ok::<_, anyhow::Error>(())
        };
        let read = async {
            loop {
                authority.check(context)?;
                let chunk = if !response.body.is_empty() {
                    Some(std::mem::take(&mut response.body))
                } else if let Some(stream) = response.stream.as_mut() {
                    stream
                        .next()
                        .await
                        .transpose()
                        .map_err(anyhow::Error::msg)?
                } else {
                    None
                };
                let Some(chunk) = chunk else {
                    break;
                };
                for value in outer.push(&chunk)? {
                    ensure!(!completed, "Vercel data after command completion");
                    if let Some(info) = value.get("command") {
                        let id = info["id"].as_str().context("Vercel command ID missing")?;
                        identifier(id)?;
                        ensure!(
                            info["sessionId"] == resource.session,
                            "Vercel command session changed"
                        );
                        ensure!(
                            info["name"] == self.python
                                && info["args"] == json!(["-I", "-c", HELPER]),
                            "Vercel command does not match the submitted helper"
                        );
                        if let Some(known) = &command {
                            ensure!(known == id, "Vercel command identity changed");
                            ensure!(info["exitCode"] == 0, "Vercel helper command failed");
                            completed = true;
                        } else {
                            ensure!(
                                info["exitCode"].is_null(),
                                "Vercel initial command was already complete"
                            );
                            command = Some(id.into());
                            if let Some(send) = send.take() {
                                let _ = send.send(id.to_owned());
                            }
                        }
                        continue;
                    }
                    ensure!(command.is_some(), "Vercel log before command start");
                    ensure!(
                        value["stream"] == "stdout",
                        "Vercel helper transport returned stderr/error"
                    );
                    let text = value["data"].as_str().context("Vercel text log missing")?;
                    for frame in inner.push(text.as_bytes())? {
                        ensure!(exit.is_none(), "Vercel helper data after exit");
                        ensure!(
                            frame.as_object().unwrap().len()
                                == if frame.get("stream").is_some() { 2 } else { 1 },
                            "invalid Vercel helper frame union"
                        );
                        ensure!(
                            frame.get("error").is_none(),
                            "Vercel helper reported an error; outcome may be unknown"
                        );
                        if let Some(code) = frame.get("end") {
                            exit = Some(
                                code.as_i64()
                                    .filter(|code| i32::try_from(*code).is_ok())
                                    .context("invalid Vercel child exit code")?,
                            );
                        } else if let Some(value) = frame.get("result") {
                            ensure!(result.is_none(), "duplicate Vercel helper result");
                            ensure!(
                                serde_json::to_vec(value)?.len() <= maximum,
                                "Vercel helper result exceeds limit"
                            );
                            result = Some(value.clone());
                        } else {
                            let channel = frame["stream"]
                                .as_str()
                                .context("Vercel helper channel missing")?;
                            ensure!(
                                matches!(channel, "stdout" | "stderr"),
                                "unknown Vercel helper channel"
                            );
                            let encoded = frame["data"]
                                .as_str()
                                .context("Vercel helper bytes missing")?;
                            let remaining = maximum.saturating_sub(stdout.len() + stderr.len());
                            ensure!(
                                encoded.len() <= remaining.div_ceil(3).saturating_mul(4),
                                "Vercel combined output exceeds limit"
                            );
                            let bytes = STANDARD.decode(encoded)?;
                            ensure!(
                                bytes.len() <= remaining,
                                "Vercel combined output exceeds limit"
                            );
                            if channel == "stdout" || combined {
                                stdout.extend(bytes);
                            } else {
                                stderr.extend(bytes);
                            }
                        }
                    }
                }
            }
            outer.finish()?;
            inner.finish()?;
            authority.check(context)?;
            ensure!(completed, "Vercel command ended without final status");
            let code = exit.context("Vercel helper ended without child status")?;
            Ok(HelperOutput {
                code,
                stdout,
                stderr,
                result,
            })
        };
        let (output, ()) = futures_util::future::try_join(read, write).await?;
        Ok(output)
    }
}
