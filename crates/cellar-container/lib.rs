// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Container and sandbox providers shared by native and guest applications.
#![doc = include_str!("README.md")]
#![warn(clippy::disallowed_methods, clippy::disallowed_types)]

mod native;
mod results;
mod values;

use anyhow::{ensure, Context as _, Result};
use cellar::sdk::{containers::ProviderAuthority, Context};
use serde_json::Value;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, OnceLock},
};

/// Bounded binary bodies and combined buffered exec output.
#[derive(Clone, Debug)]
pub struct Limits {
    pub max_body_bytes: usize,
}

/// Attached only to a refusal proven to precede provider effects.
#[derive(Debug)]
pub struct Validation;

impl std::fmt::Display for Validation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("invalid Cellar operation")
    }
}

impl std::error::Error for Validation {}

/// Closed operation vocabulary of the container bridge, version 1.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Operation {
    Running,
    Start,
    Monitor,
    Destroy,
    Signal,
    SetInactivityTimeout,
    Exec,
    ExecOutput,
    ReadStdout,
    ReadStderr,
    WriteStdin,
    CloseStdin,
    WaitExec,
    KillExec,
    WaitPort,
    Fetch,
    Connect,
    ConnectWebSocket,
    ReadSocket,
    WriteSocket,
    CloseWrite,
    CloseSocket,
    ReceiveWebSocket,
    SendText,
    SendBinary,
    CloseWebSocket,
    Release,
}

impl Operation {
    pub const ALL: &'static [Self] = &[
        Self::Running,
        Self::Start,
        Self::Monitor,
        Self::Destroy,
        Self::Signal,
        Self::SetInactivityTimeout,
        Self::Exec,
        Self::ExecOutput,
        Self::ReadStdout,
        Self::ReadStderr,
        Self::WriteStdin,
        Self::CloseStdin,
        Self::WaitExec,
        Self::KillExec,
        Self::WaitPort,
        Self::Fetch,
        Self::Connect,
        Self::ConnectWebSocket,
        Self::ReadSocket,
        Self::WriteSocket,
        Self::CloseWrite,
        Self::CloseSocket,
        Self::ReceiveWebSocket,
        Self::SendText,
        Self::SendBinary,
        Self::CloseWebSocket,
        Self::Release,
    ];

    pub fn parse(name: &str) -> std::result::Result<Self, Validation> {
        match name {
            "container.running" => Ok(Self::Running),
            "container.start" => Ok(Self::Start),
            "container.monitor" => Ok(Self::Monitor),
            "container.destroy" => Ok(Self::Destroy),
            "container.signal" => Ok(Self::Signal),
            "container.set_inactivity_timeout" => Ok(Self::SetInactivityTimeout),
            "container.exec" => Ok(Self::Exec),
            "container.exec_output" => Ok(Self::ExecOutput),
            "container.exec.read_stdout" => Ok(Self::ReadStdout),
            "container.exec.read_stderr" => Ok(Self::ReadStderr),
            "container.exec.write" => Ok(Self::WriteStdin),
            "container.exec.close_stdin" => Ok(Self::CloseStdin),
            "container.exec.wait" => Ok(Self::WaitExec),
            "container.exec.kill" => Ok(Self::KillExec),
            "container.port.wait_ready" => Ok(Self::WaitPort),
            "container.port.fetch" => Ok(Self::Fetch),
            "container.port.connect" => Ok(Self::Connect),
            "container.port.connect_websocket" => Ok(Self::ConnectWebSocket),
            "container.socket.read" => Ok(Self::ReadSocket),
            "container.socket.write" => Ok(Self::WriteSocket),
            "container.socket.close_write" => Ok(Self::CloseWrite),
            "container.socket.close" => Ok(Self::CloseSocket),
            "container.websocket.receive" => Ok(Self::ReceiveWebSocket),
            "container.websocket.send_text" => Ok(Self::SendText),
            "container.websocket.send_binary" => Ok(Self::SendBinary),
            "container.websocket.close" => Ok(Self::CloseWebSocket),
            "container.release" => Ok(Self::Release),
            _ => Err(Validation),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Running => "container.running",
            Self::Start => "container.start",
            Self::Monitor => "container.monitor",
            Self::Destroy => "container.destroy",
            Self::Signal => "container.signal",
            Self::SetInactivityTimeout => "container.set_inactivity_timeout",
            Self::Exec => "container.exec",
            Self::ExecOutput => "container.exec_output",
            Self::ReadStdout => "container.exec.read_stdout",
            Self::ReadStderr => "container.exec.read_stderr",
            Self::WriteStdin => "container.exec.write",
            Self::CloseStdin => "container.exec.close_stdin",
            Self::WaitExec => "container.exec.wait",
            Self::KillExec => "container.exec.kill",
            Self::WaitPort => "container.port.wait_ready",
            Self::Fetch => "container.port.fetch",
            Self::Connect => "container.port.connect",
            Self::ConnectWebSocket => "container.port.connect_websocket",
            Self::ReadSocket => "container.socket.read",
            Self::WriteSocket => "container.socket.write",
            Self::CloseWrite => "container.socket.close_write",
            Self::CloseSocket => "container.socket.close",
            Self::ReceiveWebSocket => "container.websocket.receive",
            Self::SendText => "container.websocket.send_text",
            Self::SendBinary => "container.websocket.send_binary",
            Self::CloseWebSocket => "container.websocket.close",
            Self::Release => "container.release",
        }
    }

    /// Observations may overlap control operations in the language bridge.
    /// This does not establish that a failed observation is safe to retry.
    pub fn is_read(self) -> bool {
        matches!(
            self,
            Self::Running
                | Self::Monitor
                | Self::ReadStdout
                | Self::ReadStderr
                | Self::WaitExec
                | Self::ReadSocket
                | Self::ReceiveWebSocket
        )
    }
}

/// Provider futures execute inside the original native invocation.
pub type ProviderFuture<'a> = Pin<Box<dyn Future<Output = Result<Value>> + Send + 'a>>;

/// Host-selected backend. Credentials and backend selection never come from guests.
/// Implementations declare supported operations and allocate invocation resources.
pub trait Provider: Send + Sync {
    fn name(&self) -> &str;

    fn operations(&self) -> &[Operation];

    fn open(&self) -> Box<dyn ProviderSession>;
}

/// Invocation-local backend resources. Drop must cancel work and release streams.
/// See the README for authority, isolation, response and retry requirements.
pub trait ProviderSession: Send + Sync {
    fn execute<'a>(
        &'a self,
        context: &'a Context,
        operation: Operation,
        input: Value,
        limits: &'a Limits,
    ) -> ProviderFuture<'a>;
}

/// Docker/Podman through Cellar's existing native, cell-owned engine.
#[derive(Default)]
pub struct Native;

impl Provider for Native {
    fn name(&self) -> &str {
        "native"
    }

    fn operations(&self) -> &[Operation] {
        Operation::ALL
    }

    fn open(&self) -> Box<dyn ProviderSession> {
        Box::new(native::Resources::default())
    }
}

impl ProviderSession for native::Resources {
    fn execute<'a>(
        &'a self,
        context: &'a Context,
        operation: Operation,
        input: Value,
        limits: &'a Limits,
    ) -> ProviderFuture<'a> {
        Box::pin(native::execute(
            context,
            self,
            operation.name(),
            input,
            limits,
        ))
    }
}

/// Reusable provider configuration; create a fresh session for every invocation.
#[derive(Clone)]
pub struct Adapter(Arc<dyn Provider>);

impl Default for Adapter {
    fn default() -> Self {
        Self(Arc::new(Native))
    }
}

impl Adapter {
    pub fn new(provider: Arc<dyn Provider>) -> Self {
        Self(provider)
    }

    pub fn provider(&self) -> &dyn Provider {
        self.0.as_ref()
    }

    pub fn session(&self) -> Session {
        Session {
            backend: self.0.open(),
            provider: self.0.clone(),
            authority: OnceLock::new(),
            control: tokio::sync::Mutex::new(()),
        }
    }
}

/// Binds a provider session to exactly one root-cell event, including its epoch.
/// The boundary validates inputs and gates mutations before invoking a backend.
pub struct Session {
    backend: Box<dyn ProviderSession>,
    provider: Arc<dyn Provider>,
    authority: OnceLock<ProviderAuthority>,
    control: tokio::sync::Mutex<()>,
}

impl Default for Session {
    fn default() -> Self {
        Adapter::default().session()
    }
}

impl Session {
    pub async fn execute(
        &self,
        context: &Context,
        operation: Operation,
        input: Value,
        limits: &Limits,
    ) -> Result<Value> {
        (|| -> Result<()> {
            values::check_depth(&input)?;
            ensure!(input.is_object(), "container input must be an object");
            ensure!(
                self.provider.operations().contains(&operation),
                "unsupported provider operation"
            );
            native::validate(operation.name(), &input, limits)
        })()
        .context(Validation)?;
        let candidate = context.container_provider_authority()?;
        let authority = self.authority.get_or_init(|| candidate);
        authority.check(context)?;
        // Observations can drain streams while one control operation is pending.
        let _control = if operation.is_read() {
            None
        } else {
            Some(self.control.lock().await)
        };
        authority.check(context)?;
        if !operation.is_read() {
            authority.before_effect(context).await?;
        }
        let result_limit = if operation == Operation::ExecOutput {
            input["max_bytes"].as_u64().unwrap() as usize // validated before dispatch
        } else {
            limits.max_body_bytes
        };
        let result = self
            .backend
            .execute(context, operation, input, limits)
            .await;
        authority.check(context)?;
        // Result validation follows execution: failure cannot prove rollback.
        if let Ok(value) = &result {
            values::check_depth(value)?;
            results::validate(operation, value, result_limit)?;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_names_are_unique_and_round_trip() {
        let mut names = std::collections::HashSet::new();
        for operation in Operation::ALL {
            assert!(names.insert(operation.name()));
            assert_eq!(Operation::parse(operation.name()).unwrap(), *operation);
        }
        assert!(Operation::parse("container.inspect").is_err());
        assert_eq!(Adapter::default().provider().operations(), Operation::ALL);
    }
}
