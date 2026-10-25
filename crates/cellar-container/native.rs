// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Invocation-owned container capabilities. Native handles never cross the bridge.
use crate::{values, Limits, Validation};
use anyhow::{bail, ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::sdk::{containers, http, sockets, websocket, Context};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

pub(crate) const MAX_RESOURCES: usize = 64;

#[derive(Default)]
pub(crate) struct Resources(Mutex<Table>);

#[derive(Default)]
struct Table {
    next: u64,
    handles: HashMap<String, Resource>,
}

#[derive(Clone)]
enum Resource {
    Exec(Arc<containers::Exec>),
    Socket(Arc<sockets::Socket>),
    WebSocket(websocket::WebSocket),
}

impl Resources {
    fn available(&self) -> Result<()> {
        ensure!(
            self.0.lock().unwrap().handles.len() < MAX_RESOURCES,
            "container resource limit"
        );
        Ok(())
    }

    fn insert(&self, resource: Resource) -> String {
        let mut table = self.0.lock().unwrap();
        table.next += 1;
        let id = table.next.to_string();
        table.handles.insert(id.clone(), resource);
        id
    }

    fn get(&self, value: &Value) -> Result<Resource> {
        self.0
            .lock()
            .unwrap()
            .handles
            .get(values::field(value, "id")?)
            .cloned()
            .context("unknown invocation resource")
    }

    fn exec(&self, value: &Value) -> Result<Arc<containers::Exec>> {
        match self.get(value)? {
            Resource::Exec(exec) => Ok(exec),
            _ => bail!("resource is not an exec process"),
        }
    }

    fn websocket(&self, value: &Value) -> Result<websocket::WebSocket> {
        match self.get(value)? {
            Resource::WebSocket(socket) => Ok(socket),
            _ => bail!("resource is not a WebSocket"),
        }
    }

    fn socket(&self, value: &Value) -> Result<Arc<sockets::Socket>> {
        match self.get(value)? {
            Resource::Socket(socket) => Ok(socket),
            _ => bail!("resource is not a TCP socket"),
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Start {
    entrypoint: Option<Vec<String>>,
    env: Vec<(String, String)>,
    enable_internet: bool,
    labels: Vec<(String, String)>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Exec {
    cmd: Vec<String>,
    env: Vec<(String, String)>,
    cwd: Option<String>,
    user: Option<String>,
    combined: bool,
}

fn start(value: &Value) -> Result<containers::StartOptions> {
    let options: Start = serde_json::from_value(value["options"].clone())?;
    Ok(containers::StartOptions {
        entrypoint: options.entrypoint,
        env: options.env,
        enable_internet: options.enable_internet,
        labels: options.labels,
    })
}

fn exec(value: &Value) -> Result<containers::ExecOptions> {
    let options: Exec = serde_json::from_value(value["options"].clone())?;
    ensure!(
        options
            .cmd
            .first()
            .is_some_and(|command| !command.is_empty())
            && options.cmd.iter().all(|argument| !argument.contains('\0')),
        "invalid exec command"
    );
    Ok(containers::ExecOptions {
        cmd: options.cmd,
        env: options.env,
        cwd: options.cwd,
        user: options.user,
        combined: options.combined,
    })
}

fn integer(value: &Value, field: &str, max: u64) -> Result<u64> {
    let n = value[field]
        .as_u64()
        .context("expected nonnegative integer")?;
    ensure!(n <= max, "integer exceeds limit");
    Ok(n)
}

fn signal(value: &Value) -> Result<u32> {
    let n = integer(value, "signal", 64)?;
    ensure!(n > 0, "signal must be nonzero");
    Ok(n as u32)
}

fn timeout(value: &Value) -> Result<Duration> {
    // Cap waits and inactivity configuration at one day, using integral millis.
    Ok(Duration::from_millis(integer(
        value,
        "timeout_ms",
        86_400_000,
    )?))
}

fn port(value: &Value) -> Result<u16> {
    let n = integer(value, "port", u16::MAX.into())?;
    ensure!(n > 0, "port must be nonzero");
    Ok(n as u16)
}

fn bytes(value: &Value, limit: usize) -> Result<Vec<u8>> {
    let data = STANDARD.decode(values::field(value, "data")?)?;
    ensure!(data.len() <= limit, "container bytes exceed body limit");
    Ok(data)
}

pub(crate) fn validate(name: &str, value: &Value, limits: &Limits) -> Result<()> {
    match name {
        "container.capabilities"
        | "container.running"
        | "container.monitor"
        | "container.destroy" => {}
        "container.start" => {
            start(value)?;
        }
        "container.signal" | "container.exec.kill" => {
            signal(value)?;
        }
        "container.set_inactivity_timeout" => {
            timeout(value)?;
        }
        "container.exec" | "container.exec_output" => {
            exec(value)?;
        }
        "container.port.wait_ready" => {
            port(value)?;
            timeout(value)?;
        }
        "container.port.fetch" => {
            port(value)?;
            values::input(&value["request"], limits.max_body_bytes)?;
        }
        "container.port.connect" => {
            port(value)?;
        }
        "container.port.connect_websocket" => {
            port(value)?;
            values::field(value, "url")?;
            let headers: Vec<(String, String)> = serde_json::from_value(value["headers"].clone())?;
            values::headers(&headers)?;
        }
        "container.websocket.receive" => {}
        "container.websocket.send_text" => {
            ensure!(
                values::field(value, "text")?.len() <= limits.max_body_bytes,
                "WebSocket text exceeds limit"
            );
        }
        "container.websocket.send_binary" => {
            bytes(value, limits.max_body_bytes)?;
        }
        "container.websocket.close" => {
            let code = integer(value, "code", 4999)?;
            ensure!(code == 1000 || code >= 3000, "invalid WebSocket close code");
            ensure!(
                values::field(value, "reason")?.len() <= 123,
                "WebSocket close reason too long"
            );
        }
        "container.exec.write" | "container.socket.write" => {
            bytes(value, limits.max_body_bytes)?;
        }
        "container.exec.read_stdout"
        | "container.exec.read_stderr"
        | "container.exec.close_stdin"
        | "container.exec.wait"
        | "container.socket.read"
        | "container.socket.close_write"
        | "container.socket.close"
        | "container.release" => {}
        _ => bail!("unsupported container operation"),
    }
    if name == "container.exec_output" {
        integer(value, "max_bytes", limits.max_body_bytes as u64)?;
    }
    if name.starts_with("container.exec.")
        || name.starts_with("container.socket.")
        || name.starts_with("container.websocket.")
        || name == "container.release"
    {
        values::field(value, "id")?;
    }
    Ok(())
}

pub(crate) async fn execute(
    context: &Context,
    resources: &Resources,
    name: &str,
    value: Value,
    limits: &Limits,
) -> Result<Value> {
    validate(name, &value, limits).context(Validation)?;
    // Root-cell, class declaration, current turn and activation authority are
    // resolved afresh on every call, including reads of invocation resources.
    let container = context.container()?;
    match name {
        "container.running" => Ok(json!(container.running(context)?)),
        "container.start" => Ok(json!(container
            .start(context, start(&value)?)
            .await?
            .to_string())),
        "container.monitor" => Ok(json!(container.monitor(context).await?)),
        "container.destroy" => {
            container.destroy(context).await?;
            Ok(Value::Null)
        }
        "container.signal" => {
            container.signal(context, signal(&value)?).await?;
            Ok(Value::Null)
        }
        "container.set_inactivity_timeout" => {
            container
                .set_inactivity_timeout(context, timeout(&value)?)
                .await?;
            Ok(Value::Null)
        }
        "container.exec_output" => {
            let process = container.exec(context, exec(&value)?).await?;
            process.close_stdin(context).await?;
            let output = process
                .output(
                    context,
                    integer(&value, "max_bytes", limits.max_body_bytes as u64)? as usize,
                )
                .await?;
            Ok(
                json!({"exit_code":output.exit_code,"stdout":STANDARD.encode(output.stdout),"stderr":STANDARD.encode(output.stderr)}),
            )
        }
        "container.exec" => {
            resources.available().context(Validation)?;
            let process = container.exec(context, exec(&value)?).await?;
            Ok(json!(resources.insert(Resource::Exec(Arc::new(process)))))
        }
        "container.exec.read_stdout" | "container.exec.read_stderr" => {
            let process = resources.exec(&value).context(Validation)?;
            let data = if name.ends_with("read_stdout") {
                process.read_stdout(context).await?
            } else {
                process.read_stderr(context).await?
            };
            ensure!(
                data.len() <= limits.max_body_bytes,
                "exec chunk exceeds body limit"
            );
            Ok(json!(STANDARD.encode(data)))
        }
        "container.exec.write" => {
            resources
                .exec(&value)
                .context(Validation)?
                .write(context, bytes(&value, limits.max_body_bytes)?)
                .await?;
            Ok(Value::Null)
        }
        "container.exec.close_stdin" => {
            resources
                .exec(&value)
                .context(Validation)?
                .close_stdin(context)
                .await?;
            Ok(Value::Null)
        }
        "container.exec.wait" => Ok(json!(
            resources
                .exec(&value)
                .context(Validation)?
                .wait(context)
                .await?
        )),
        "container.exec.kill" => {
            resources
                .exec(&value)
                .context(Validation)?
                .kill(context, signal(&value)?)
                .await?;
            Ok(Value::Null)
        }
        "container.port.wait_ready" => {
            container
                .port(context, port(&value)?)?
                .wait_ready(context, timeout(&value)?)
                .await?;
            Ok(Value::Null)
        }
        "container.port.fetch" => {
            let request = values::input(&value["request"], limits.max_body_bytes)?;
            let response = container
                .port(context, port(&value)?)?
                .fetch(
                    context,
                    http::OutboundRequest {
                        url: request.url,
                        method: request.method,
                        headers: request.headers,
                        body: Some(request.body),
                        redirect: http::Redirect::Manual,
                    },
                )
                .await?;
            values::response(response, limits.max_body_bytes).await
        }
        "container.port.connect_websocket" => {
            resources.available().context(Validation)?;
            let entries: Vec<(String, String)> = serde_json::from_value(value["headers"].clone())?;
            let headers = http::Headers::new();
            for (name, value) in entries {
                headers.append(&name, &value)?;
            }
            match container
                .port(context, port(&value)?)?
                .connect_websocket(context, values::field(&value, "url")?, headers)
                .await?
            {
                websocket::UpgradeResponse::Upgraded(socket) => {
                    let protocol = socket.protocol();
                    Ok(
                        json!({"upgraded":true,"id":resources.insert(Resource::WebSocket(socket)),"protocol":protocol}),
                    )
                }
                websocket::UpgradeResponse::Declined(response) => Ok(
                    json!({"upgraded":false,"response":values::response(response, limits.max_body_bytes).await?}),
                ),
            }
        }
        "container.websocket.receive" => {
            use websocket::SocketEvent;
            let event = resources
                .websocket(&value)
                .context(Validation)?
                .next(context)
                .await?;
            Ok(match event {
                SocketEvent::Open(protocol) => json!({"type":"open","protocol":protocol}),
                SocketEvent::Text(text) => {
                    ensure!(
                        text.len() <= limits.max_body_bytes,
                        "WebSocket text exceeds limit"
                    );
                    json!({"type":"text","text":text})
                }
                SocketEvent::Binary(data) => {
                    ensure!(
                        data.len() <= limits.max_body_bytes,
                        "WebSocket frame exceeds limit"
                    );
                    json!({"type":"binary","data":STANDARD.encode(data)})
                }
                SocketEvent::Close(code, reason, clean) => {
                    json!({"type":"close","code":code,"reason":reason,"clean":clean})
                }
            })
        }
        "container.websocket.send_text" => {
            resources
                .websocket(&value)
                .context(Validation)?
                .send_text(context, values::field(&value, "text")?)?;
            Ok(Value::Null)
        }
        "container.websocket.send_binary" => {
            resources
                .websocket(&value)
                .context(Validation)?
                .send_binary(context, bytes(&value, limits.max_body_bytes)?)?;
            Ok(Value::Null)
        }
        "container.websocket.close" => {
            resources.websocket(&value).context(Validation)?.close(
                context,
                integer(&value, "code", 4999)? as u16,
                values::field(&value, "reason")?,
            )?;
            resources
                .0
                .lock()
                .unwrap()
                .handles
                .remove(values::field(&value, "id")?);
            Ok(Value::Null)
        }
        "container.port.connect" => {
            resources.available().context(Validation)?;
            let socket = container.port(context, port(&value)?)?.connect(
                context,
                sockets::SocketOptions {
                    allow_half_open: true,
                    ..Default::default()
                },
            )?;
            socket.opened().await?;
            Ok(json!(resources.insert(Resource::Socket(Arc::new(socket)))))
        }
        "container.socket.read" => {
            let socket = resources.socket(&value).context(Validation)?;
            let data = socket.readable.get_reader()?.read(context).await?;
            ensure!(
                data.len() <= limits.max_body_bytes,
                "socket chunk exceeds body limit"
            );
            Ok(json!(STANDARD.encode(data)))
        }
        "container.socket.write" => {
            resources
                .socket(&value)
                .context(Validation)?
                .writable
                .get_writer()?
                .write(context, bytes(&value, limits.max_body_bytes)?)
                .await?;
            Ok(Value::Null)
        }
        "container.socket.close_write" => {
            resources
                .socket(&value)
                .context(Validation)?
                .writable
                .get_writer()?
                .close(context)
                .await?;
            Ok(Value::Null)
        }
        "container.socket.close" => {
            resources
                .socket(&value)
                .context(Validation)?
                .close(context)
                .await?;
            resources
                .0
                .lock()
                .unwrap()
                .handles
                .remove(values::field(&value, "id")?);
            Ok(Value::Null)
        }
        "container.release" => {
            let resource = resources
                .0
                .lock()
                .unwrap()
                .handles
                .remove(values::field(&value, "id")?)
                .context("unknown invocation resource")
                .context(Validation)?;
            match resource {
                Resource::Socket(socket) => socket.close(context).await?,
                Resource::WebSocket(socket) => socket.close(context, 1000, "")?,
                Resource::Exec(_) => {}
            }
            Ok(Value::Null)
        }
        _ => bail!("unsupported container operation"),
    }
}

#[cfg(test)]
#[path = "native_tests.rs"]
mod tests;
