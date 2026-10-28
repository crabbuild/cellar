// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{lifecycle::Sandbox, transport, wire, Config};
use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::sdk::Context;
use futures_util::StreamExt;
use serde_json::{json, Value};

impl Config {
    pub(super) fn validate_command(&self, options: &Value) -> Result<()> {
        ensure!(
            options.get("pty").is_none_or(Value::is_null),
            "E2B terminal execution is not qualified"
        );
        if let Some(user) = options["user"].as_str() {
            ensure!(
                !user.is_empty()
                    && user.len() <= 256
                    && !user.chars().any(char::is_control)
                    && !user.contains(':'),
                "E2B expects a Unix username, not a UID:GID pair"
            );
        }
        super::lifecycle::pairs(&options["env"])?;
        Ok(())
    }

    pub(super) fn headers(
        &self,
        sandbox: &Sandbox,
        user: Option<&str>,
        content_type: &str,
    ) -> Result<Vec<(String, String)>> {
        let mut headers = vec![
            ("X-Access-Token".into(), sandbox.token.clone()),
            ("E2b-Sandbox-Id".into(), sandbox.id.clone()),
            ("E2b-Sandbox-Port".into(), "49983".into()),
            ("Content-Type".into(), content_type.into()),
            ("Connect-Protocol-Version".into(), "1".into()),
        ];
        let user = user.unwrap_or("root");
        ensure!(
            !user.is_empty()
                && user.len() <= 256
                && !user.chars().any(char::is_control)
                && !user.contains(':'),
            "invalid E2B process user"
        );
        headers.push((
            "Authorization".into(),
            format!("Basic {}", STANDARD.encode(format!("{user}:"))),
        ));
        Ok(headers)
    }

    pub(super) async fn rpc(
        &self,
        context: &Context,
        sandbox: &Sandbox,
        path: &str,
        input: Value,
        user: Option<&str>,
        maximum: usize,
    ) -> Result<Value> {
        let reply = transport::request(
            context,
            self.envd_url(&sandbox.id, path)?,
            "POST",
            self.headers(sandbox, user, "application/json")?,
            Some(serde_json::to_vec(&input)?),
            maximum,
            self.timeout_ms,
        )
        .await?;
        ensure!(
            reply.status == 200,
            "E2B envd RPC failed: HTTP {}",
            reply.status
        );
        Ok(serde_json::from_slice(&reply.body)?)
    }

    pub(super) async fn command(
        &self,
        context: &Context,
        sandbox: &Sandbox,
        options: &Value,
        maximum: usize,
        stdin: Option<&[u8]>,
    ) -> Result<Value> {
        let cmd = options["cmd"]
            .as_array()
            .context("E2B command argv missing")?;
        let user = options["user"].as_str();
        let env = super::lifecycle::pairs(&options["env"])?;
        let mut process = json!({"cmd":cmd[0],"args":&cmd[1..],"envs":env});
        if let Some(cwd) = options["cwd"].as_str() {
            process["cwd"] = json!(cwd);
        }
        let frame = wire::frame(&json!({"process":process,"stdin":stdin.is_some()}))?;
        let mut response = transport::open(
            context,
            self.envd_url(&sandbox.id, "process.Process/Start")?,
            "POST",
            self.headers(sandbox, user, "application/connect+json")?,
            Some(frame),
            self.timeout_ms,
        )
        .await?;
        ensure!(
            response.status == 200,
            "E2B command start failed: HTTP {}",
            response.status
        );
        ensure!(
            response
                .headers
                .iter()
                .any(|(key, value)| key.eq_ignore_ascii_case("content-type")
                    && value.split(';').next() == Some("application/connect+json")),
            "E2B command response is not Connect JSON"
        );
        let frame_limit = maximum
            .div_ceil(3)
            .saturating_mul(4)
            .saturating_add(16 * 1024);
        let mut decoder = wire::Decoder::new(frame_limit);
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut started = false;
        let mut exit = None;
        let authority = context.invocation_authority()?;
        let (pid_send, pid_receive) = tokio::sync::oneshot::channel();
        let mut pid_send = Some(pid_send);
        // Drive stdin and stdout together. tee can fill stdout before a large
        // SendInput completes; awaiting that RPC inside the read loop deadlocks.
        let write = async {
            if let Some(bytes) = stdin {
                let pid = pid_receive
                    .await
                    .context("E2B process closed before stdin admission")?;
                if !bytes.is_empty() {
                    self.rpc(
                        context,
                        sandbox,
                        "process.Process/SendInput",
                        json!({"process":{"pid":pid},"input":{"stdin":STANDARD.encode(bytes)}}),
                        user,
                        16 * 1024,
                    )
                    .await?;
                }
                self.rpc(
                    context,
                    sandbox,
                    "process.Process/CloseStdin",
                    json!({"process":{"pid":pid}}),
                    user,
                    16 * 1024,
                )
                .await?;
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
                for value in decoder.push(&chunk)? {
                    let event = value
                        .get("event")
                        .and_then(Value::as_object)
                        .context("E2B process event missing")?;
                    ensure!(exit.is_none(), "E2B data after process end");
                    ensure!(event.len() == 1, "invalid E2B event union");
                    if let Some(start) = event.get("start") {
                        ensure!(!started, "E2B process started twice");
                        started = true;
                        let pid = start["pid"]
                            .as_u64()
                            .filter(|pid| *pid > 0 && *pid <= u32::MAX as u64)
                            .context("invalid E2B process PID")?;
                        if let Some(send) = pid_send.take() {
                            let _ = send.send(pid);
                        }
                    } else if let Some(data) = event.get("data") {
                        ensure!(started, "E2B data before process start");
                        let output = data.as_object().context("invalid E2B process data")?;
                        ensure!(output.len() == 1, "invalid E2B output union");
                        let (channel, encoded) = output.iter().next().unwrap();
                        ensure!(
                            matches!(channel.as_str(), "stdout" | "stderr"),
                            "E2B PTY output was not requested"
                        );
                        let remaining = maximum.saturating_sub(stdout.len() + stderr.len());
                        let encoded = encoded.as_str().context("invalid E2B binary output")?;
                        ensure!(
                            encoded.len() <= remaining.div_ceil(3).saturating_mul(4),
                            "E2B command output exceeds byte budget"
                        );
                        let bytes = STANDARD.decode(encoded)?;
                        ensure!(
                            bytes.len() <= remaining,
                            "E2B command output exceeds byte budget"
                        );
                        if channel == "stdout" || options["combined"] == true {
                            stdout.extend(bytes);
                        } else {
                            stderr.extend(bytes);
                        }
                    } else if let Some(end) = event.get("end") {
                        ensure!(
                            started && end["exited"] == true,
                            "E2B command did not report a normal child exit"
                        );
                        let code = end
                            .get("exitCode")
                            .map_or(Some(0), Value::as_i64)
                            .filter(|c| i32::try_from(*c).is_ok())
                            .context("invalid E2B exit code")?;
                        exit = Some(code);
                    } else {
                        ensure!(event.contains_key("keepalive"), "unknown E2B process event");
                    }
                }
            }
            decoder.finish()?;
            authority.check(context)?;
            Ok(
                json!({"exit_code":exit.context("E2B process stream has no exit status")?,"stdout":STANDARD.encode(stdout),"stderr":STANDARD.encode(stderr)}),
            )
        };
        let (output, ()) = futures_util::future::try_join(read, write).await?;
        Ok(output)
    }
}
