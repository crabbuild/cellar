// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Container and sandbox providers shared by native and guest applications.
#![doc = include_str!("README.md")]
#![warn(clippy::disallowed_methods, clippy::disallowed_types)]

mod capabilities;
pub mod e2b;
mod files;
mod native;
mod queued;
mod results;
mod values;

use anyhow::{ensure, Context as _, Result};
pub use capabilities::{Capabilities, Environment, Fencing};
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

/// Cancellation stops waiting; a submitted provider mutation may have committed.
#[derive(Debug)]
pub struct Cancelled {
    before_dispatch: bool,
}

impl Cancelled {
    pub fn before_dispatch(&self) -> bool {
        self.before_dispatch
    }
}

impl std::fmt::Display for Cancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("container operation cancelled")
    }
}

impl std::error::Error for Cancelled {}

/// Closed operation vocabulary of the container bridge, version 1.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Operation {
    Capabilities,
    Running,
    Start,
    Monitor,
    Destroy,
    Signal,
    SetInactivityTimeout,
    Exec,
    ExecOutput,
    ReadFile,
    WriteFile,
    StatFile,
    ListFiles,
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
        Self::Capabilities,
        Self::Running,
        Self::Start,
        Self::Monitor,
        Self::Destroy,
        Self::Signal,
        Self::SetInactivityTimeout,
        Self::Exec,
        Self::ExecOutput,
        Self::ReadFile,
        Self::WriteFile,
        Self::StatFile,
        Self::ListFiles,
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
            "container.capabilities" => Ok(Self::Capabilities),
            "container.running" => Ok(Self::Running),
            "container.start" => Ok(Self::Start),
            "container.monitor" => Ok(Self::Monitor),
            "container.destroy" => Ok(Self::Destroy),
            "container.signal" => Ok(Self::Signal),
            "container.set_inactivity_timeout" => Ok(Self::SetInactivityTimeout),
            "container.exec" => Ok(Self::Exec),
            "container.exec_output" => Ok(Self::ExecOutput),
            "container.file.read" => Ok(Self::ReadFile),
            "container.file.write" => Ok(Self::WriteFile),
            "container.file.stat" => Ok(Self::StatFile),
            "container.file.list" => Ok(Self::ListFiles),
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
            Self::Capabilities => "container.capabilities",
            Self::Running => "container.running",
            Self::Start => "container.start",
            Self::Monitor => "container.monitor",
            Self::Destroy => "container.destroy",
            Self::Signal => "container.signal",
            Self::SetInactivityTimeout => "container.set_inactivity_timeout",
            Self::Exec => "container.exec",
            Self::ExecOutput => "container.exec_output",
            Self::ReadFile => "container.file.read",
            Self::WriteFile => "container.file.write",
            Self::StatFile => "container.file.stat",
            Self::ListFiles => "container.file.list",
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
            Self::Capabilities
                | Self::Running
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

    /// Execution environment supplied by this configured backend. Unknown is
    /// explicit for existing providers until they declare their environment.
    fn environment(&self) -> Environment {
        Environment::Unspecified
    }

    /// Ownership protection supplied by this configured backend. Local
    /// invocation checks alone do not revoke already accepted remote requests.
    fn fencing(&self) -> Fencing {
        Fencing::Unspecified
    }

    /// Maximum live handles enforced by the provider, when declared.
    fn max_resources(&self) -> Option<usize> {
        None
    }

    /// Whether sessions implement an immediate, I/O-free running snapshot.
    /// Workers uses this for its synchronous `container.running` property.
    fn synchronous_running(&self) -> bool {
        false
    }

    /// Support bounded, synchronous admission followed by native-owned control.
    /// Sessions must implement `preflight` without provider I/O or effects.
    fn queued_lifecycle(&self) -> bool {
        false
    }

    fn operations(&self) -> &[Operation];

    fn open(&self) -> Box<dyn ProviderSession>;
}

/// Invocation-local backend resources. Drop must cancel work and release streams.
/// See the README for authority, isolation, response and retry requirements.
pub trait ProviderSession: Send + Sync {
    /// Validate configured backend prerequisites without I/O or effects.
    /// Shared shape, operation and invocation checks precede this hook.
    fn preflight(&self, _context: &Context, _operation: Operation, _input: &Value) -> Result<()> {
        anyhow::bail!(Validation)
    }

    /// Observe authoritative state without I/O, blocking or starting work.
    /// Providers must opt in through `Provider::synchronous_running`.
    fn running_snapshot(&self, _context: &Context) -> Result<bool> {
        anyhow::bail!(Validation)
    }

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
    fn fencing(&self) -> Fencing {
        Fencing::NativeCell
    }

    fn queued_lifecycle(&self) -> bool {
        true
    }

    fn synchronous_running(&self) -> bool {
        true
    }

    fn environment(&self) -> Environment {
        Environment::Linux
    }

    fn max_resources(&self) -> Option<usize> {
        Some(native::MAX_RESOURCES)
    }

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
    fn preflight(&self, context: &Context, operation: Operation, _input: &Value) -> Result<()> {
        let container = context.container()?;
        if operation == Operation::Start {
            ensure!(!container.running(context)?, "container is already running");
        }
        Ok(())
    }

    fn running_snapshot(&self, context: &Context) -> Result<bool> {
        context.container()?.running(context)
    }

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

    /// Describe the configured provider without allocating an invocation session
    /// or contacting a remote service. Limits are those of the invoking host.
    pub fn capabilities(&self, limits: &Limits) -> Capabilities {
        Capabilities::new(self.0.as_ref(), limits)
    }

    pub fn session(&self) -> Session {
        Session {
            backend: self.0.open(),
            provider: self.0.clone(),
            authority: OnceLock::new(),
            control: tokio::sync::Mutex::new(()),
            queued: Default::default(),
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
    queued: queued::Queue,
}

impl Default for Session {
    fn default() -> Self {
        Adapter::default().session()
    }
}

impl Session {
    /// Cancel an invocation-owned operation by dropping its provider future.
    /// After dispatch this proves neither rollback nor termination of remote work.
    pub async fn execute_with_signal(
        &self,
        context: &Context,
        operation: Operation,
        input: Value,
        limits: &Limits,
        signal: Option<&cellar::sdk::web::AbortSignal>,
    ) -> Result<Value> {
        let Some(signal) = signal else {
            return self.execute(context, operation, input, limits).await;
        };
        if signal.aborted() {
            return Err(Cancelled {
                before_dispatch: true,
            }
            .into());
        }
        cellar::asyncrt::select_biased! { "preserve an available provider acknowledgement before cancellation";
            result = self.execute(context, operation, input, limits) => result,
            _ = signal.cancelled() => Err(Cancelled { before_dispatch: false }.into()),
        }
    }

    /// Accept immutable lifecycle inputs without starting provider work.
    /// The returned event-owned ticket can be consumed exactly once.
    pub fn admit_queued(
        &self,
        context: &Context,
        operation: Operation,
        input: Value,
        limits: &Limits,
    ) -> Result<String> {
        (|| -> Result<()> {
            ensure!(
                queued::supported(self.provider.as_ref()),
                "provider does not support queued lifecycle"
            );
            ensure!(
                matches!(
                    operation,
                    Operation::Start | Operation::Signal | Operation::Destroy
                ),
                "not a queued container control"
            );
            values::check_depth(&input)?;
            ensure!(input.is_object(), "container input must be an object");
            native::validate(operation.name(), &input, limits)?;
            queued::validate(operation, &input)
        })()
        .context(Validation)?;
        let candidate = context.container_provider_authority()?;
        let authority = self.authority.get_or_init(|| candidate);
        authority.check(context)?;
        self.backend
            .preflight(context, operation, &input)
            .context(Validation)?;
        authority.check(context)?;
        self.queued
            .admit(operation, input, limits.max_body_bytes)
            .context(Validation)
    }

    /// Run an accepted command under the same event, output gate and provider
    /// control serialization as ordinary operations. Dropping releases quota.
    pub async fn execute_queued(
        &self,
        context: &Context,
        ticket: &str,
        limits: &Limits,
    ) -> Result<Value> {
        let candidate = context.container_provider_authority()?;
        self.authority.get_or_init(|| candidate).check(context)?;
        let command = self.queued.take(ticket)?;
        command.run(self, context, limits).await
    }

    /// Release an unconsumed admission. This cannot cancel or roll back a
    /// command already claimed for execution.
    pub fn discard_queued(&self, context: &Context, ticket: &str) -> Result<()> {
        let candidate = context.container_provider_authority()?;
        self.authority.get_or_init(|| candidate).check(context)?;
        drop(self.queued.take(ticket)?);
        Ok(())
    }

    /// Read the provider's immediate snapshot under this event's authority.
    /// This is separate from `execute(Running)`, which may perform backend I/O.
    pub fn running_snapshot(&self, context: &Context) -> Result<bool> {
        ensure!(
            self.provider.synchronous_running()
                && self.provider.operations().contains(&Operation::Running),
            Validation
        );
        let candidate = context.container_provider_authority()?;
        let authority = self.authority.get_or_init(|| candidate);
        authority.check(context)?;
        let result = self.backend.running_snapshot(context);
        authority.check(context)?;
        result
    }

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
                operation == Operation::Capabilities
                    || self.provider.operations().contains(&operation),
                "unsupported provider operation"
            );
            native::validate(operation.name(), &input, limits)
        })()
        .context(Validation)?;
        let candidate = context.container_provider_authority()?;
        let authority = self.authority.get_or_init(|| candidate);
        authority.check(context)?;
        if operation == Operation::Capabilities {
            return Ok(serde_json::to_value(Capabilities::new(
                self.provider.as_ref(),
                limits,
            ))?);
        }
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
        let result_limit = if matches!(
            operation,
            Operation::ExecOutput | Operation::ReadFile | Operation::ListFiles
        ) {
            input["max_bytes"].as_u64().unwrap() as usize // validated before dispatch
        } else {
            limits.max_body_bytes
        };
        let entry_limit = if operation == Operation::ListFiles {
            Some(input["max_entries"].as_u64().unwrap() as usize)
        } else {
            None
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
            if operation == Operation::ListFiles {
                files::validate_listing(value, result_limit, entry_limit.unwrap())?;
            }
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
