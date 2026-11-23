// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded Sandbox operations use the ordinary fenced native Container adapter.
use super::execution::{SandboxExecutor, MAX_SANDBOX_MESSAGE_BYTES};
use crate::application::{consensus::CommandFailure, containers::*, Context, HandlerFuture};
use anyhow::{ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::{atomic::Ordering, Arc};

pub(crate) fn executor(context: &Context) -> Result<Option<Arc<dyn SandboxExecutor>>> {
    let Some(storage) = &context.raft_storage else {
        return Ok(None);
    };
    ensure!(
        storage.resources.sandbox_execution.load(Ordering::Acquire),
        CommandFailure::UnsupportedCapability
    );
    let binding = storage
        .resources
        .container_adapter
        .get()
        .ok_or(CommandFailure::UnsupportedCapability)?;
    Ok(Some(Arc::new(NativeExecutor {
        configuration: binding.identity.configuration_digest.clone(),
        provider: binding.identity.provider.clone(),
        max_bytes: binding.max_body_bytes,
    })))
}

struct NativeExecutor {
    configuration: String,
    provider: String,
    max_bytes: usize,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct StartWire {
    entrypoint: Option<Vec<String>>,
    env: Vec<(String, String)>,
    labels: Vec<(String, String)>,
    enable_internet: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Start {
    options: StartWire,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ExecWire {
    cmd: Vec<String>,
    env: Vec<(String, String)>,
    cwd: Option<String>,
    user: Option<String>,
    combined: bool,
    pty: Option<Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Exec {
    options: ExecWire,
    max_bytes: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Read {
    path: String,
    max_bytes: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Write {
    path: String,
    bytes: String,
    mode: u32,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Stat {
    path: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct List {
    path: String,
    max_bytes: usize,
    max_entries: usize,
}

impl SandboxExecutor for NativeExecutor {
    fn configuration_digest(&self) -> &str {
        &self.configuration
    }

    fn execute(
        self: Arc<Self>,
        context: Context,
        operation: String,
        input: Value,
    ) -> HandlerFuture<Value> {
        Box::pin(async move {
            ensure!(
                serde_json::to_vec(&input)?.len() <= MAX_SANDBOX_MESSAGE_BYTES,
                "sandbox input exceeds limit"
            );
            let container = context.container()?;
            let value = match operation.as_str() {
                "container.capabilities" => {
                    json!({"format":1,"provider":self.provider,"environment":"linux","fencing":"provider_enforced","max_resources":1,"synchronous_running":false,"queued_lifecycle":false,"exec_pty":false,"operations":["container.start","container.destroy","container.reconcile","container.running","container.exec_output","container.file.read","container.file.write","container.file.stat","container.file.list"],"max_body_bytes":self.max_bytes,"configuration_digest":self.configuration})
                }
                "container.start" => {
                    let Start { options } = serde_json::from_value(input)?;
                    json!(
                        container
                            .start(
                                &context,
                                StartOptions {
                                    entrypoint: options.entrypoint,
                                    env: options.env,
                                    labels: options.labels,
                                    enable_internet: options.enable_internet
                                }
                            )
                            .await?
                    )
                }
                "container.reconcile" => {
                    json!({"state":if container.running(&context)?{"running"}else{"unknown"}})
                }
                "container.running" => json!(container.running(&context)?),
                "container.destroy" => {
                    container.destroy(&context).await?;
                    Value::Null
                }
                "container.exec_output" => {
                    let Exec { options, max_bytes } = serde_json::from_value(input)?;
                    ensure!(options.pty.is_none(), CommandFailure::UnsupportedCapability);
                    ensure!(max_bytes <= self.max_bytes, "sandbox output exceeds policy");
                    let output = container
                        .exec_output(
                            &context,
                            ExecOptions {
                                cmd: options.cmd,
                                env: options.env,
                                cwd: options.cwd,
                                user: options.user,
                                combined: options.combined,
                                pty: None,
                            },
                            max_bytes,
                        )
                        .await?;
                    json!({"exit_code":output.exit_code,"stdout":STANDARD.encode(output.stdout),"stderr":STANDARD.encode(output.stderr)})
                }
                "container.file.read" => {
                    let Read { path, max_bytes } = serde_json::from_value(input)?;
                    ensure!(max_bytes <= self.max_bytes, "sandbox read exceeds policy");
                    json!(STANDARD.encode(
                        container
                            .read_file(&context, FilePath::new(path)?, max_bytes)
                            .await?
                    ))
                }
                "container.file.write" => {
                    let Write { path, bytes, mode } = serde_json::from_value(input)?;
                    ensure!(
                        bytes.len() <= self.max_bytes.saturating_mul(2),
                        "sandbox write exceeds policy"
                    );
                    let bytes = STANDARD.decode(bytes)?;
                    ensure!(
                        bytes.len() <= self.max_bytes,
                        "sandbox write exceeds policy"
                    );
                    container
                        .write_file(&context, FilePath::new(path)?, bytes, mode)
                        .await?;
                    Value::Null
                }
                "container.file.stat" => {
                    let Stat { path } = serde_json::from_value(input)?;
                    serde_json::to_value(
                        container.stat_file(&context, FilePath::new(path)?).await?,
                    )?
                }
                "container.file.list" => {
                    let List {
                        path,
                        max_bytes,
                        max_entries,
                    } = serde_json::from_value(input)?;
                    ensure!(
                        max_bytes <= self.max_bytes,
                        "sandbox listing exceeds policy"
                    );
                    serde_json::to_value(
                        container
                            .list_files(&context, FilePath::new(path)?, max_bytes, max_entries)
                            .await?,
                    )?
                }
                _ => anyhow::bail!(CommandFailure::UnsupportedCapability),
            };
            ensure!(
                serde_json::to_vec(&value)?.len() <= MAX_SANDBOX_MESSAGE_BYTES,
                "sandbox output exceeds limit"
            );
            Ok(value)
        })
    }
}
