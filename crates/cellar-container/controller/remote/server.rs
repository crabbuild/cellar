// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{
    wire::{self, Action, Request, Response, Value},
    Permission, RemoteOptions, ServerTls,
};
use anyhow::{ensure, Result};
use cellar::{
    asyncrt::{ExecutionDomain, TaskHandle},
    sdk::containers::*,
};
use futures_util::{stream::FuturesUnordered, StreamExt};
use sha2::{Digest, Sha256};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Semaphore,
};
use tokio_util::sync::CancellationToken;

/// An ordinary host listener. Its shutdown joins every connection but does not
/// shut down the separately owned controller, backend or execution domain.
/// Drop cancels admission; call shutdown to wait for completion.
pub struct ControllerServer {
    address: SocketAddr,
    stop: CancellationToken,
    task: Option<TaskHandle<Result<()>>>,
}

impl ControllerServer {
    pub async fn bind(
        domain: Arc<ExecutionDomain>,
        controller: Arc<ExecutionController>,
        options: RemoteOptions,
        tls: ServerTls,
    ) -> Result<Self> {
        options.validate()?;
        ensure!(
            controller.options().limits.max_inflight >= 2,
            "controller needs reserved control capacity"
        );
        let (acceptor, permissions) = tls.build()?;
        let listener = domain.run(TcpListener::bind(options.address)).await??;
        let address = listener.local_addr()?;
        let stop = CancellationToken::new();
        let cancelled = stop.clone();
        let owner = domain.clone();
        let task = domain.spawn(async move {
            let permissions = Arc::new(permissions);
            let capacity = Arc::new(Semaphore::new(options.max_connections));
            let mut connections = FuturesUnordered::new();
            let result = loop {
                cellar::asyncrt::select_biased! { "stop admission before accepting another controller request";
                    _ = cancelled.cancelled() => break Ok(()),
                    _ = connections.next(), if !connections.is_empty() => {},
                    accepted = listener.accept() => {
                        let (stream, _) = match accepted { Ok(value) => value, Err(error) => break Err(error.into()) };
                        let Ok(permit) = capacity.clone().try_acquire_owned() else { continue };
                        let controller = controller.clone();
                        let acceptor = acceptor.clone();
                        let permissions = permissions.clone();
                        let connection = owner.spawn(async move {
                            let _permit = permit;
                            // Handshake, framing, controller work and response share
                            // one transport budget. It never extends effect expiry.
                            let _ = cellar::asyncrt::timeout(Duration::from_millis(options.timeout_ms),
                                serve(stream, controller, acceptor, &permissions, options)).await;
                        });
                        match connection { Ok(task) => connections.push(task), Err(error) => break Err(error.into()) }
                    },
                }
            };
            drop(listener);
            for task in connections.iter() { task.abort(); }
            while connections.next().await.is_some() {}
            result
        })?;
        Ok(Self {
            address,
            stop,
            task: Some(task),
        })
    }

    pub fn address(&self) -> SocketAddr {
        self.address
    }

    pub async fn shutdown(mut self) -> Result<()> {
        self.stop.cancel();
        if let Some(task) = self.task.take() {
            task.await??;
        }
        Ok(())
    }
}

impl Drop for ControllerServer {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

async fn serve(
    stream: TcpStream,
    controller: Arc<ExecutionController>,
    acceptor: tokio_rustls::TlsAcceptor,
    permissions: &[Permission],
    options: RemoteOptions,
) -> Result<()> {
    let mut stream = acceptor.accept(stream).await?;
    let leaf = stream
        .get_ref()
        .1
        .peer_certificates()
        .and_then(|chain| chain.first())
        .ok_or(ExecutionControllerFailure::Unavailable)?;
    let digest: [u8; 32] = Sha256::digest(leaf.as_ref()).into();
    ensure!(
        permissions
            .iter()
            .any(|permission| permission.certificate_sha256 == digest),
        "unauthorized controller certificate"
    );
    let request: Request = wire::read(&mut stream, options.max_frame_bytes).await?;
    let result = dispatch(controller, permissions, digest, request).await;
    wire::write(
        &mut stream,
        &Response {
            format: 1,
            result: result.map_err(wire::Failure::from_error),
        },
        options.max_frame_bytes,
    )
    .await
}

async fn dispatch(
    controller: Arc<ExecutionController>,
    permissions: &[Permission],
    digest: [u8; 32],
    request: Request,
) -> Result<Value> {
    let history = request.claim.history();
    ensure!(
        request.format == 1
            && request.identity == controller.identity()
            && request.limits == controller.options().limits,
        ExecutionControllerFailure::ConflictingRequest
    );
    ensure!(
        request.claim.binding() == controller.identity().binding
            && permissions
                .iter()
                .any(|permission| permission.certificate_sha256 == digest
                    && permission.cluster == history.cluster.bytes()
                    && permission.namespace == history.namespace.bytes()),
        ExecutionControllerFailure::RevokedHistory
    );
    let claim = request.claim;
    Ok(match request.action {
        Action::Activate => {
            controller.activate(claim).await?;
            Value::Activated
        }
        Action::Execute { operation, payload } => Value::Outcome(
            ExecutionController::execute(&controller, claim, operation, payload).await?,
        ),
        Action::Reconcile { original } => {
            Value::Outcome(ExecutionController::reconcile(&controller, claim, original).await?)
        }
        Action::Pending => Value::Pending(ExecutionController::pending(&controller, claim).await?),
        Action::Lifecycle { kind } => {
            Value::Lifecycle(ExecutionController::lifecycle(&controller, claim, kind).await?)
        }
        Action::SealUnsubmitted { operation } => Value::Sealed(
            ExecutionController::seal_unsubmitted(&controller, claim, operation).await?,
        ),
    })
}
