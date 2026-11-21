// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{
    wire::{self, Action, Request, Response, Value},
    ClientTls, RemoteOptions,
};
use crate::controller::ControllerEndpoint;
use anyhow::{ensure, Result};
use cellar::{
    asyncrt::ExecutionDomain,
    sdk::{containers::*, HandlerFuture},
};
use rustls::pki_types::ServerName;
use sha2::{Digest, Sha256};
use std::{sync::Arc, time::Duration};
use tokio::net::TcpStream;

/// Trusted host endpoint for one exact controller. Construction performs no I/O;
/// identities come from deployment configuration, never an unauthenticated
/// handshake. Each RPC authenticates afresh and makes one attempt only.
pub struct RemoteController {
    domain: Arc<ExecutionDomain>,
    options: RemoteOptions,
    identity: BackendIdentity,
    limits: ExecutionControllerLimits,
    connector: tokio_rustls::TlsConnector,
    name: ServerName<'static>,
    pin: [u8; 32],
}

impl RemoteController {
    pub fn new(
        domain: Arc<ExecutionDomain>,
        options: RemoteOptions,
        identity: BackendIdentity,
        limits: ExecutionControllerLimits,
        tls: ClientTls,
    ) -> Result<Arc<Self>> {
        options.validate()?;
        ensure!(
            options.address.port() != 0 && limits.max_inflight >= 2,
            "invalid controller endpoint"
        );
        let (connector, name, pin) = tls.build()?;
        Ok(Arc::new(Self {
            domain,
            options,
            identity,
            limits,
            connector,
            name,
            pin,
        }))
    }

    async fn call(&self, claim: ExecutionClaim, action: Action) -> Result<Value> {
        let result = self
            .domain
            .run(async {
                cellar::asyncrt::timeout(Duration::from_millis(self.options.timeout_ms), async {
                    let stream = TcpStream::connect(self.options.address).await?;
                    let mut stream = self.connector.connect(self.name.clone(), stream).await?;
                    let leaf = stream
                        .get_ref()
                        .1
                        .peer_certificates()
                        .and_then(|chain| chain.first())
                        .ok_or(ExecutionControllerFailure::Unavailable)?;
                    let digest: [u8; 32] = Sha256::digest(leaf.as_ref()).into();
                    ensure!(digest == self.pin, "controller certificate pin differs");
                    wire::write(
                        &mut stream,
                        &Request {
                            format: 1,
                            identity: self.identity.clone(),
                            limits: self.limits,
                            claim,
                            action,
                        },
                        self.options.max_frame_bytes,
                    )
                    .await?;
                    let response: Response =
                        wire::read(&mut stream, self.options.max_frame_bytes).await?;
                    ensure!(
                        response.format == 1,
                        "unsupported controller response format"
                    );
                    response.result.map_err(|error| error.into_error().into())
                })
                .await
            })
            .await;
        // All transport failures, including post-submission disconnects and
        // decode failures, retain uncertainty. No automatic retry or no-effect
        // classification is inferred from a lost response.
        match result {
            Ok(Ok(result)) => result.map_err(|error: anyhow::Error| match error
                .downcast_ref::<ExecutionControllerFailure>(
            ) {
                Some(failure) => (*failure).into(),
                None => ExecutionControllerFailure::UnknownOperation.into(),
            }),
            _ => Err(ExecutionControllerFailure::UnknownOperation.into()),
        }
    }
}

impl ExecutionFenceAuthority for RemoteController {
    fn identity(&self) -> BackendIdentity {
        self.identity.clone()
    }

    fn activate(self: Arc<Self>, claim: ExecutionClaim) -> HandlerFuture<()> {
        Box::pin(async move {
            ensure!(
                matches!(self.call(claim, Action::Activate).await?, Value::Activated),
                ExecutionControllerFailure::UnknownOperation
            );
            Ok(())
        })
    }
}

impl ControllerEndpoint for RemoteController {
    fn limits(&self) -> ExecutionControllerLimits {
        self.limits
    }

    fn execute(
        self: Arc<Self>,
        claim: ExecutionClaim,
        operation: ExecutionOperation,
        payload: Vec<u8>,
    ) -> HandlerFuture<ExecutionOutcome> {
        Box::pin(async move {
            match self
                .call(claim, Action::Execute { operation, payload })
                .await?
            {
                Value::Outcome(value) => Ok(value),
                _ => Err(ExecutionControllerFailure::UnknownOperation.into()),
            }
        })
    }

    fn reconcile(
        self: Arc<Self>,
        claim: ExecutionClaim,
        original: ExecutionPendingOperation,
    ) -> HandlerFuture<ExecutionOutcome> {
        Box::pin(async move {
            match self.call(claim, Action::Reconcile { original }).await? {
                Value::Outcome(value) => Ok(value),
                _ => Err(ExecutionControllerFailure::UnknownOperation.into()),
            }
        })
    }

    fn pending(
        self: Arc<Self>,
        claim: ExecutionClaim,
    ) -> HandlerFuture<Vec<ExecutionPendingOperation>> {
        Box::pin(async move {
            match self.call(claim, Action::Pending).await? {
                Value::Pending(value) => Ok(value),
                _ => Err(ExecutionControllerFailure::UnknownOperation.into()),
            }
        })
    }

    fn lifecycle(
        self: Arc<Self>,
        claim: ExecutionClaim,
        kind: ExecutionOperationKind,
    ) -> HandlerFuture<Option<ExecutionPendingOperation>> {
        Box::pin(async move {
            match self.call(claim, Action::Lifecycle { kind }).await? {
                Value::Lifecycle(value) => Ok(value),
                _ => Err(ExecutionControllerFailure::UnknownOperation.into()),
            }
        })
    }

    fn seal_unsubmitted(
        self: Arc<Self>,
        claim: ExecutionClaim,
        operation: ExecutionOperation,
    ) -> HandlerFuture<ExecutionPendingOperation> {
        Box::pin(async move {
            match self
                .call(claim, Action::SealUnsubmitted { operation })
                .await?
            {
                Value::Sealed(value) => Ok(value),
                _ => Err(ExecutionControllerFailure::UnknownOperation.into()),
            }
        })
    }
}
