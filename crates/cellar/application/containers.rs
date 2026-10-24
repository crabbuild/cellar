// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Cell-owned containers. Handles may be retained by a cell; every operation
//! requires its current executing context. Mutations wait for durability.

use super::{http, sockets, websocket, Context, HandlerFuture};
use crate::{container as host, host_gates};
use anyhow::{anyhow, Result};
use std::{
    future::Future,
    sync::{Arc, Weak},
    time::Duration,
};

/// Options for the container's root process. Internet access defaults to off.
#[derive(Clone, Debug, Default)]
pub struct StartOptions {
    pub entrypoint: Option<Vec<String>>,
    pub env: Vec<(String, String)>,
    pub enable_internet: bool,
    pub labels: Vec<(String, String)>,
}

#[derive(Clone, Debug, Default)]
pub struct ExecOptions {
    pub cmd: Vec<String>,
    pub env: Vec<(String, String)>,
    pub cwd: Option<String>,
    pub user: Option<String>,
    pub combined: bool,
}

/// Authority for the container attached to one root cell activation.
#[derive(Clone)]
pub struct Container {
    scope: Arc<str>,
    epoch: u64,
    engine: Arc<host::ContainerEngine>,
    cell: Arc<host::CellContainer>,
    activation: Weak<super::Activation>,
}

impl Context {
    pub fn container(&self) -> Result<Container> {
        self.check_turn()?;
        let identity = self
            .identity()
            .ok_or_else(|| anyhow!("stateless handlers have no container"))?;
        anyhow::ensure!(
            self.facet().is_none(),
            "containers must be controlled by their root cell"
        );
        anyhow::ensure!(
            self.worker_config.container_class(&identity.class),
            "this cell class has no container"
        );
        let engine =
            host::engine_if_ready().ok_or_else(|| anyhow!("container engine is not ready"))?;
        let cell = engine
            .cell(&identity.scope)
            .ok_or_else(|| anyhow!("this cell has no container"))?;
        Ok(Container {
            scope: identity.scope.clone().into(),
            epoch: identity.epoch,
            engine,
            cell,
            activation: Arc::downgrade(
                self.io
                    .native_activation
                    .as_ref()
                    .expect("cell has a native activation"),
            ),
        })
    }
}

impl Container {
    fn check(&self, context: &Context) -> Result<()> {
        context.check_turn()?;
        self.engine.check_owner()?;
        anyhow::ensure!(
            self.activation.upgrade().is_some_and(|original| context
                .io
                .native_activation
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(&original, current))),
            "container belongs to a retired cell activation"
        );
        anyhow::ensure!(
            context.facet().is_none(),
            "containers must be controlled by their root cell"
        );
        anyhow::ensure!(
            context
                .identity()
                .is_some_and(|id| id.scope == self.scope.as_ref() && id.epoch == self.epoch),
            "container belongs to another or retired cell activation"
        );
        anyhow::ensure!(
            self.engine
                .cell(&self.scope)
                .is_some_and(|cell| Arc::ptr_eq(&cell, &self.cell)),
            "container has been released by its owner"
        );
        Ok(())
    }

    fn effect<T, F, Fut>(&self, context: &Context, effect: F) -> HandlerFuture<T>
    where
        T: Send + 'static,
        F: FnOnce(Arc<host::ContainerEngine>, Arc<host::CellContainer>) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        if let Err(error) = self.check(context) {
            return Box::pin(async move { Err(error) });
        }
        let gate = host_gates::egress_gate_request(&context.io, cellar_logic::Channel::Container);
        let owner = self.clone();
        let context = context.clone();
        Box::pin(async move {
            host_gates::await_egress_gate(gate)
                .await
                .map_err(anyhow::Error::msg)?;
            let _control = owner.cell.sdk_control.lock().await;
            owner.check(&context)?;
            let result = effect(owner.engine.clone(), owner.cell.clone()).await;
            owner.check(&context)?;
            result
        })
    }

    pub fn running(&self, context: &Context) -> Result<bool> {
        self.check(context)?;
        Ok(self.cell.running())
    }

    /// Wait for startup; the returned run identifies the process started.
    pub fn start(&self, context: &Context, options: StartOptions) -> HandlerFuture<u64> {
        self.effect(context, move |engine, cell| async move {
            let run = cell.begin_run();
            engine
                .start(
                    &cell,
                    run,
                    host::StartParams {
                        entrypoint: options.entrypoint,
                        env: options.env,
                        enable_internet: options.enable_internet,
                        labels: options.labels,
                    },
                )
                .await?;
            Ok(run)
        })
    }

    /// Wait for this run to exit. Dropping the future stops waiting; the
    /// container remains subject to its ownership and inactivity policies.
    pub fn monitor(&self, context: &Context) -> HandlerFuture<i64> {
        if let Err(error) = self.check(context) {
            return Box::pin(async move { Err(error) });
        }
        let run = self.cell.current_run();
        if run == 0 {
            return Box::pin(async { Err(anyhow!("container has no current run")) });
        }
        let owner = self.clone();
        let context = context.clone();
        Box::pin(async move {
            let result = owner
                .engine
                .monitor(&owner.cell, run)
                .await
                .map_err(anyhow::Error::msg);
            owner.check(&context)?;
            result
        })
    }

    pub fn destroy(&self, context: &Context) -> HandlerFuture<()> {
        self.effect(context, |engine, cell| async move {
            engine.destroy(&cell).await
        })
    }

    pub fn signal(&self, context: &Context, signal: u32) -> HandlerFuture<()> {
        self.effect(context, move |engine, cell| async move {
            anyhow::ensure!(signal > 0 && signal <= 64, "invalid container signal");
            engine.signal(&cell, signal).await
        })
    }

    pub fn set_inactivity_timeout(
        &self,
        context: &Context,
        timeout: Duration,
    ) -> HandlerFuture<()> {
        self.effect(context, move |_, cell| async move {
            cell.set_inactivity(timeout);
            Ok(())
        })
    }

    pub fn port(&self, context: &Context, port: u16) -> Result<Port> {
        self.check(context)?;
        anyhow::ensure!(port != 0, "container port must be nonzero");
        Ok(Port {
            owner: self.clone(),
            port,
        })
    }

    pub fn exec(&self, context: &Context, options: ExecOptions) -> HandlerFuture<Exec> {
        let owner = self.clone();
        self.effect(context, move |engine, cell| async move {
            anyhow::ensure!(!options.cmd.is_empty(), "container exec requires a command");
            let run = cell.current_run();
            let process = engine
                .exec(
                    &cell,
                    host::ExecParams {
                        cmd: options.cmd,
                        env: options.env,
                        cwd: options.cwd,
                        user: options.user,
                        combined: options.combined,
                    },
                )
                .await?;
            Ok(Exec {
                owner,
                run,
                process,
            })
        })
    }
}

/// Direct access to a cell-owned port, independent of global egress policy.
#[derive(Clone)]
pub struct Port {
    owner: Container,
    port: u16,
}

impl Port {
    fn address(&self, context: &Context) -> Result<String> {
        self.owner.check(context)?;
        self.owner
            .cell
            .address(self.port)
            .map_err(anyhow::Error::msg)
    }

    fn url(&self, context: &Context, original: &str) -> Result<String> {
        let mut url = url::Url::parse(original)?;
        anyhow::ensure!(
            matches!(url.scheme(), "http" | "https" | "ws" | "wss"),
            "invalid container port URL scheme"
        );
        let address: sockets::SocketAddress = self.address(context)?.try_into()?;
        url.set_scheme("http")
            .map_err(|_| anyhow!("invalid container port URL"))?;
        url.set_host(Some(&address.hostname))?;
        url.set_port(Some(address.port))
            .map_err(|_| anyhow!("invalid container port"))?;
        Ok(url.into())
    }

    pub fn connect(
        &self,
        context: &Context,
        options: sockets::SocketOptions,
    ) -> Result<sockets::Socket> {
        context.connect_direct(self.address(context)?.try_into()?, options)
    }

    /// Wait until the port accepts a connection, with a bounded readiness wait.
    pub fn wait_ready(&self, context: &Context, timeout: Duration) -> HandlerFuture<()> {
        if let Err(error) = self.owner.check(context) {
            return Box::pin(async move { Err(error) });
        }
        let port = self.clone();
        let context = context.clone();
        Box::pin(async move {
            crate::asyncrt::timeout(timeout, async {
                loop {
                    port.owner.check(&context)?;
                    let socket = port.connect(&context, sockets::SocketOptions::default())?;
                    if socket.opened().await.is_ok() {
                        socket.close(&context).await?;
                        return Ok(());
                    }
                    drop(socket);
                    crate::asyncrt::sleep(Duration::from_millis(50)).await;
                }
            })
            .await?
        })
    }

    /// Redirects are returned to the caller so a port capability cannot follow
    /// a redirect to an unrelated destination. Bodies retain event ownership.
    pub fn fetch(
        &self,
        context: &Context,
        mut request: http::OutboundRequest,
    ) -> HandlerFuture<crate::engine_api::HttpResponse> {
        let prepared = (|| -> Result<_> {
            let url = self.url(context, &request.url)?;
            request.method = http::normalize_method(&request.method)?;
            context.io.charge_subrequest().map_err(anyhow::Error::msg)?;
            let guard = super::service::body_guard(context, request.body.as_ref())?;
            let gate = host_gates::egress_gate_request(&context.io, cellar_logic::Channel::Fetch);
            Ok((url, guard, gate))
        })();
        let (url, guard, gate) = match prepared {
            Ok(value) => value,
            Err(error) => return Box::pin(async move { Err(error) }),
        };
        let owner = self.owner.clone();
        let context = context.clone();
        Box::pin(async move {
            let (_, cancel, mut cancellation) = crate::host_channels::begin_call_cancellation();
            let head = crate::host_fetch::fetch(
                context.io.clone(),
                request.method,
                url,
                request.body,
                request.headers,
                "manual",
                guard,
                gate,
                Some(cancel),
                context.trace,
            )
            .await
            .map_err(anyhow::Error::msg)?;
            owner.check(&context)?;
            cancellation.disarm();
            head.into_http().map_err(anyhow::Error::msg)
        })
    }

    pub fn upgrade_websocket(
        &self,
        context: &Context,
        url: &str,
        headers: http::Headers,
    ) -> HandlerFuture<websocket::UpgradeResponse> {
        match self.url(context, url) {
            Ok(url) => context.container_websocket(url, headers),
            Err(error) => Box::pin(async move { Err(error) }),
        }
    }
}

/// An exec process belongs to its cell activation and container run. Drop
/// releases its streams and host registry entry without killing the process.
pub struct Exec {
    owner: Container,
    run: u64,
    process: Arc<host::ExecProcess>,
}

impl Drop for Exec {
    fn drop(&mut self) {
        self.owner.cell.forget_process(self.process.id);
    }
}

#[derive(Debug)]
pub struct ExecOutput {
    pub exit_code: i64,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl Exec {
    fn check(&self, context: &Context) -> Result<()> {
        self.owner.check(context)?;
        anyhow::ensure!(
            self.run == self.owner.cell.current_run(),
            "exec process belongs to a previous container run"
        );
        Ok(())
    }

    pub async fn read_stdout(&self, context: &Context) -> Result<Vec<u8>> {
        self.read(context, 1).await
    }

    pub async fn read_stderr(&self, context: &Context) -> Result<Vec<u8>> {
        self.read(context, 2).await
    }

    async fn read(&self, context: &Context, channel: u8) -> Result<Vec<u8>> {
        self.check(context)?;
        let bytes = self.process.read(channel).await;
        self.check(context)?;
        Ok(bytes.to_vec())
    }

    pub async fn write(&self, context: &Context, bytes: impl AsRef<[u8]>) -> Result<()> {
        self.check(context)?;
        let process = self.process.clone();
        let bytes = bytes.as_ref().to_vec();
        let run = self.run;
        self.owner
            .effect(context, move |_, cell| async move {
                anyhow::ensure!(
                    cell.current_run() == run,
                    "exec process belongs to a previous container run"
                );
                process.write(&bytes).await.map_err(anyhow::Error::msg)
            })
            .await
    }

    pub async fn close_stdin(&self, context: &Context) -> Result<()> {
        self.check(context)?;
        let process = self.process.clone();
        let run = self.run;
        self.owner
            .effect(context, move |_, cell| async move {
                anyhow::ensure!(
                    cell.current_run() == run,
                    "exec process belongs to a previous container run"
                );
                process.close_stdin().await;
                Ok(())
            })
            .await
    }

    pub async fn wait(&self, context: &Context) -> Result<i64> {
        self.check(context)?;
        let code = self.process.wait().await?;
        self.check(context)?;
        Ok(code)
    }

    pub async fn kill(&self, context: &Context, signal: u32) -> Result<()> {
        self.check(context)?;
        let process = self.process.clone();
        let run = self.run;
        self.owner
            .effect(context, move |_, cell| async move {
                anyhow::ensure!(
                    cell.current_run() == run,
                    "exec process belongs to a previous container run"
                );
                anyhow::ensure!(signal > 0 && signal <= 64, "invalid exec signal");
                process.kill(signal).await
            })
            .await
    }

    /// Drain stdout and stderr concurrently with a combined byte limit.
    pub async fn output(self, context: &Context, max_bytes: usize) -> Result<ExecOutput> {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let remaining = AtomicUsize::new(max_bytes);
        let collect = |channel| {
            let remaining = &remaining;
            let this = &self;
            async move {
                let mut output = Vec::new();
                loop {
                    let bytes = this.read(context, channel).await?;
                    if bytes.is_empty() {
                        return Ok::<_, anyhow::Error>(output);
                    }
                    anyhow::ensure!(
                        remaining
                            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n
                                .checked_sub(bytes.len()))
                            .is_ok(),
                        "container exec output exceeds byte limit"
                    );
                    output.extend(bytes);
                }
            }
        };
        let result = futures_util::try_join!(self.wait(context), collect(1), collect(2));
        match result {
            Ok((exit_code, stdout, stderr)) => Ok(ExecOutput {
                exit_code,
                stdout,
                stderr,
            }),
            Err(error) => {
                let _ = self.kill(context, 9).await;
                Err(error)
            }
        }
    }
}
