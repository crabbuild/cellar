// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cell-owned containers. Retained handles require the current executing Context.
pub(crate) mod backend;
mod lifecycle;
mod native;
mod values;
pub use crate::application::container_bindings::{
    BindingCheckpoint, BindingIntent, BindingJournal, BindingLifecycleGuard, BindingObservation,
    BindingRecord, MAX_BINDINGS, MAX_BINDING_BYTES,
};
pub use crate::application::container_controller::{
    ExecutionController, ExecutionControllerBackend, ExecutionControllerFailure,
    ExecutionControllerLimits, ExecutionControllerOptions, ExecutionOperation,
    ExecutionOperationKind, ExecutionOutcome, ExecutionPendingOperation,
};
pub use crate::application::container_execution::{
    ExecutionClaim, ExecutionFenceAuthority, ExecutionGrant,
};
pub use crate::application::container_supervision::{
    supervise, BackendIdentity, DeleteSafety, FencedMaintenanceBackend, MaintenanceAction,
    MaintenanceBackend, MaintenancePolicy, MaintenancePreparation, RetentionExpired, Supervision,
};
pub use crate::application::sandbox_catalog::{
    SandboxAllocation, SandboxAllocationPage, SandboxAllocationState, SandboxCatalog,
    SandboxExecutor, SandboxFailure, SandboxFailureKind, SandboxParent, SandboxParentState,
    SandboxReservation, MAX_SANDBOX_MESSAGE_BYTES,
};
use crate::application::{http, sockets, websocket, Context, HandlerFuture, InvocationAuthority};
pub use crate::container::{FileKind, FileMetadata, FilePath, TerminalSize, MAX_FILE_ENTRIES};
use crate::{application::consensus::CommandFailure, host_gates};
use anyhow::{anyhow, Result};
pub use backend::{
    ContainerAdapter, ContainerFuture, ContainerReply, ContainerRequest, PreparedContainerCall,
};
pub(crate) use lifecycle::{BackgroundAdmission, LifecycleLocks};
use std::{sync::Arc, time::Duration};
pub use values::{ExecInfo, ExecOptions, ExecOutput, StartOptions};

/// Exact event authority; the provider must additionally fence accepted execution.
pub struct ProviderAuthority {
    authority: Arc<InvocationAuthority>,
}

impl Context {
    pub fn container_provider_authority(&self) -> Result<ProviderAuthority> {
        let authority = self.invocation_authority()?;
        anyhow::ensure!(
            self.identity().is_some(),
            "stateless handlers have no container"
        );
        anyhow::ensure!(
            self.facet().is_none(),
            "containers must be controlled by their root cell"
        );
        if let Some(storage) = &self.raft_storage {
            anyhow::ensure!(
                storage
                    .resources
                    .provider_bindings
                    .get()
                    .is_some_and(|bindings| !bindings.is_empty()),
                CommandFailure::UnsupportedCapability
            );
        }
        Ok(ProviderAuthority {
            authority: Arc::new(authority),
        })
    }

    /// Obtain the host-selected container without contacting its provider.
    pub fn container(&self) -> Result<Container> {
        if let Some(storage) = &self.raft_storage {
            let binding = storage
                .resources
                .container_adapter
                .get()
                .ok_or(CommandFailure::UnsupportedCapability)?;
            return Ok(Container(Owner::Shared(backend::Handle::new(
                self,
                binding.clone(),
            )?)));
        }
        Ok(Container(Owner::Native(native::Container::open(self)?)))
    }
}

impl ProviderAuthority {
    pub fn check(&self, context: &Context) -> Result<()> {
        self.authority.check(context)
    }

    /// Prove the exact original invocation's required quorum/S3 minimum.
    pub fn before_effect(&self, context: &Context) -> HandlerFuture<()> {
        if let Err(error) = self.check(context) {
            return Box::pin(async move { Err(error) });
        }
        if context.raft_storage.is_some() {
            let flush = match context.storage() {
                Ok(storage) => storage.confirm_checkpoint(),
                Err(error) => return Box::pin(async move { Err(error) }),
            };
            let authority = self.authority.clone();
            let context = context.clone();
            return Box::pin(async move {
                flush.await?;
                authority.check(&context)
            });
        }
        let gate = host_gates::egress_gate_request(&context.io, cellar_logic::Channel::Container);
        let context = context.clone();
        Box::pin(async move {
            host_gates::await_egress_gate(gate)
                .await
                .map_err(anyhow::Error::msg)?;
            context.check_turn()
        })
    }
}

#[derive(Clone)]
enum Owner {
    Native(native::Container),
    Shared(backend::Handle),
}

/// Activation-bound handle; every operation takes its current executing Context.
#[derive(Clone)]
pub struct Container(Owner);

fn unsupported<T: Send + 'static>() -> HandlerFuture<T> {
    Box::pin(async { Err(CommandFailure::UnsupportedCapability.into()) })
}

fn mapped<T: Send + 'static>(
    future: HandlerFuture<ContainerReply>,
    decode: fn(ContainerReply) -> Result<T>,
) -> HandlerFuture<T> {
    Box::pin(async move { decode(future.await?) })
}

fn done(reply: ContainerReply) -> Result<()> {
    match reply {
        ContainerReply::Done => Ok(()),
        _ => Err(anyhow!("invalid container adapter result")),
    }
}

impl Container {
    /// Latest admitted observation in this invocation; Start/Destroy update it.
    /// An uncertain lifecycle operation invalidates the snapshot.
    pub fn running(&self, context: &Context) -> Result<bool> {
        match &self.0 {
            Owner::Native(native) => native.running(context),
            Owner::Shared(shared) => shared.running(context),
        }
    }

    /// Wait for admitted startup and return its run or provider generation.
    pub fn start(&self, context: &Context, options: StartOptions) -> HandlerFuture<u64> {
        match &self.0 {
            Owner::Native(native) => native.start(context, options),
            Owner::Shared(shared) => mapped(
                shared.call(context, ContainerRequest::Start(options)),
                |reply| match reply {
                    ContainerReply::Started(run) => Ok(run),
                    _ => Err(anyhow!("invalid container Start result")),
                },
            ),
        }
    }

    pub fn destroy(&self, context: &Context) -> HandlerFuture<()> {
        match &self.0 {
            Owner::Native(native) => native.destroy(context),
            Owner::Shared(shared) => mapped(shared.call(context, ContainerRequest::Destroy), done),
        }
    }

    /// Wait for the current run to exit. Cancellation stops waiting.
    pub fn monitor(&self, context: &Context) -> HandlerFuture<i64> {
        match &self.0 {
            Owner::Native(native) => native.monitor(context),
            Owner::Shared(shared) => mapped(
                shared.call(context, ContainerRequest::Monitor),
                |reply| match reply {
                    ContainerReply::Exited(code) => Ok(code),
                    _ => Err(anyhow!("invalid container monitor result")),
                },
            ),
        }
    }

    pub fn signal(&self, context: &Context, signal: u32) -> HandlerFuture<()> {
        match &self.0 {
            Owner::Native(native) => native.signal(context, signal),
            Owner::Shared(shared) => {
                mapped(shared.call(context, ContainerRequest::Signal(signal)), done)
            }
        }
    }

    pub fn set_inactivity_timeout(
        &self,
        context: &Context,
        timeout: Duration,
    ) -> HandlerFuture<()> {
        match &self.0 {
            Owner::Native(native) => native.set_inactivity_timeout(context, timeout),
            Owner::Shared(shared) => mapped(
                shared.call(context, ContainerRequest::SetInactivityTimeout(timeout)),
                done,
            ),
        }
    }

    /// A lost or cancelled file mutation remains uncertain.
    pub fn write_file(
        &self,
        context: &Context,
        path: FilePath,
        data: Vec<u8>,
        mode: u32,
    ) -> HandlerFuture<()> {
        match &self.0 {
            Owner::Native(native) => native.write_file(context, path, data, mode),
            Owner::Shared(shared) => mapped(
                shared.call(context, ContainerRequest::WriteFile { path, data, mode }),
                done,
            ),
        }
    }

    /// Read bounded regular-file bytes without extracting them onto the host.
    pub fn read_file(
        &self,
        context: &Context,
        path: FilePath,
        max_bytes: usize,
    ) -> HandlerFuture<Vec<u8>> {
        match &self.0 {
            Owner::Native(native) => native.read_file(context, path, max_bytes),
            Owner::Shared(shared) => mapped(
                shared.call(context, ContainerRequest::ReadFile { path, max_bytes }),
                |reply| match reply {
                    ContainerReply::Bytes(bytes) => Ok(bytes),
                    _ => Err(anyhow!("invalid container file result")),
                },
            ),
        }
    }

    /// Obtain portable metadata under current ownership and the source gate.
    pub fn stat_file(&self, context: &Context, path: FilePath) -> HandlerFuture<FileMetadata> {
        match &self.0 {
            Owner::Native(native) => native.stat_file(context, path),
            Owner::Shared(shared) => mapped(
                shared.call(context, ContainerRequest::StatFile(path)),
                |reply| match reply {
                    ContainerReply::Metadata(metadata) => Ok(metadata),
                    _ => Err(anyhow!("invalid container metadata result")),
                },
            ),
        }
    }

    /// List bounded direct children, sorted by name; partial listings refuse.
    pub fn list_files(
        &self,
        context: &Context,
        path: FilePath,
        max_bytes: usize,
        max_entries: usize,
    ) -> HandlerFuture<Vec<FileMetadata>> {
        match &self.0 {
            Owner::Native(native) => native.list_files(context, path, max_bytes, max_entries),
            Owner::Shared(shared) => mapped(
                shared.call(
                    context,
                    ContainerRequest::ListFiles {
                        path,
                        max_bytes,
                        max_entries,
                    },
                ),
                |reply| match reply {
                    ContainerReply::Entries(entries) => Ok(entries),
                    _ => Err(anyhow!("invalid container directory result")),
                },
            ),
        }
    }

    pub fn exec(&self, context: &Context, options: ExecOptions) -> HandlerFuture<Exec> {
        match &self.0 {
            Owner::Native(native) => {
                let future = native.exec(context, options);
                Box::pin(async move { Ok(Exec(future.await?)) })
            }
            Owner::Shared(shared) => {
                if let Err(error) = shared.check(context) {
                    return Box::pin(async move { Err(error) });
                }
                unsupported()
            }
        }
    }

    pub fn port(&self, context: &Context, port: u16) -> Result<Port> {
        match &self.0 {
            Owner::Native(native) => Ok(Port(native.port(context, port)?)),
            Owner::Shared(shared) => {
                shared.check(context)?;
                Err(CommandFailure::UnsupportedCapability.into())
            }
        }
    }

    pub fn supports_outbound_http(&self, context: &Context) -> Result<bool> {
        match &self.0 {
            Owner::Native(native) => native.supports_outbound_http(context),
            Owner::Shared(shared) => {
                shared.check(context)?;
                Ok(false)
            }
        }
    }

    pub fn supports_outbound_https(&self, context: &Context) -> Result<bool> {
        match &self.0 {
            Owner::Native(native) => native.supports_outbound_https(context),
            Owner::Shared(shared) => {
                shared.check(context)?;
                Ok(false)
            }
        }
    }

    pub fn intercept_outbound_http(
        &self,
        context: &Context,
        target: &str,
        fetcher: &crate::application::service::Fetcher,
    ) -> HandlerFuture<()> {
        match &self.0 {
            Owner::Native(native) => native.intercept_outbound_http(context, target, fetcher),
            Owner::Shared(shared) => {
                if let Err(error) = shared.check(context) {
                    return Box::pin(async move { Err(error) });
                }
                unsupported()
            }
        }
    }

    pub fn intercept_all_outbound_http(
        &self,
        context: &Context,
        fetcher: &crate::application::service::Fetcher,
    ) -> HandlerFuture<()> {
        match &self.0 {
            Owner::Native(native) => native.intercept_all_outbound_http(context, fetcher),
            Owner::Shared(shared) => {
                if let Err(error) = shared.check(context) {
                    return Box::pin(async move { Err(error) });
                }
                unsupported()
            }
        }
    }

    pub fn intercept_outbound_https(
        &self,
        context: &Context,
        target: &str,
        fetcher: &crate::application::service::Fetcher,
    ) -> HandlerFuture<()> {
        match &self.0 {
            Owner::Native(native) => native.intercept_outbound_https(context, target, fetcher),
            Owner::Shared(shared) => {
                if let Err(error) = shared.check(context) {
                    return Box::pin(async move { Err(error) });
                }
                unsupported()
            }
        }
    }
}

/// Native port capability. Shared transport sessions require a separate owner.
#[derive(Clone)]
pub struct Port(native::Port);

impl Port {
    pub fn connect(
        &self,
        context: &Context,
        options: sockets::SocketOptions,
    ) -> Result<sockets::Socket> {
        self.0.connect(context, options)
    }

    pub fn wait_ready(&self, context: &Context, timeout: Duration) -> HandlerFuture<()> {
        self.0.wait_ready(context, timeout)
    }

    pub fn fetch(
        &self,
        context: &Context,
        request: http::OutboundRequest,
    ) -> HandlerFuture<crate::engine_api::HttpResponse> {
        self.0.fetch(context, request)
    }

    pub fn connect_websocket(
        &self,
        context: &Context,
        url: &str,
        headers: http::Headers,
    ) -> HandlerFuture<websocket::UpgradeResponse> {
        self.0.connect_websocket(context, url, headers)
    }

    pub fn upgrade_websocket(
        &self,
        context: &Context,
        url: &str,
        headers: http::Headers,
    ) -> HandlerFuture<websocket::UpgradeResponse> {
        self.0.upgrade_websocket(context, url, headers)
    }
}

/// Native process streams retain the activation and run that allocated them.
pub struct Exec(native::Exec);

impl Exec {
    pub fn info(&self, context: &Context) -> Result<ExecInfo> {
        self.0.info(context)
    }

    pub async fn resize(&self, context: &Context, cols: u16, rows: u16) -> Result<()> {
        self.0.resize(context, cols, rows).await
    }

    pub async fn read_stdout(&self, context: &Context) -> Result<Vec<u8>> {
        self.0.read_stdout(context).await
    }

    pub async fn read_stderr(&self, context: &Context) -> Result<Vec<u8>> {
        self.0.read_stderr(context).await
    }

    pub async fn write(&self, context: &Context, bytes: impl AsRef<[u8]>) -> Result<()> {
        self.0.write(context, bytes).await
    }

    pub async fn close_stdin(&self, context: &Context) -> Result<()> {
        self.0.close_stdin(context).await
    }

    pub async fn wait(&self, context: &Context) -> Result<i64> {
        self.0.wait(context).await
    }

    pub async fn kill(&self, context: &Context, signal: u32) -> Result<()> {
        self.0.kill(context, signal).await
    }

    pub async fn output(self, context: &Context, max_bytes: usize) -> Result<ExecOutput> {
        self.0.output(context, max_bytes).await
    }
}
