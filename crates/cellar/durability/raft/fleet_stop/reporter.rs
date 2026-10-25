// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded Node-owned guard work outlives disconnected request waiters.
use super::*;
use crate::actor::ShutdownMode;
use crate::node_lifecycle::NodeOutcome;
use crate::peer_probe::PeerProbeSigner;
use crate::raft::fleet_catalog::{FleetCatalog, FleetCatalogError, FleetHostAuthorization};
use crate::raft::storage::{LogError, ReplicaLog};
use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};

type Result<T> = std::result::Result<T, FleetCatalogError>;

pub(crate) struct Services {
    pub authorization: FleetHostAuthorization,
    pub log: ReplicaLog,
    pub signer: Arc<PeerProbeSigner>,
}

struct Job {
    request: FleetStopRequest,
    deadline: u64,
    reply: oneshot::Sender<Result<FleetStopAcceptance>>,
}

pub(crate) struct Reporter {
    boot: BootBinding,
    log: ReplicaLog,
    catalog: FleetCatalog,
    signer: Arc<PeerProbeSigner>,
    sender: mpsc::Sender<Job>,
    shutdown: mpsc::UnboundedSender<ShutdownMode>,
    signaled: OnceLock<bool>,
}

impl Reporter {
    pub fn start(
        services: Services,
        shutdown: mpsc::UnboundedSender<ShutdownMode>,
    ) -> Result<Arc<Self>> {
        let boot = services
            .log
            .lifecycle_guard()
            .map_err(log_error)?
            .ok_or(FleetCatalogError::Corrupt("missing synced boot guard"))?
            .boot;
        if boot.node != services.authorization.node().bytes()
            || boot.disk != services.authorization.disk().bytes()
            || boot.fleet != services.authorization.fleet()
            || boot.join_key != services.signer.public_key_bytes()
        {
            return Err(FleetCatalogError::Conflict);
        }
        let (sender, mut jobs) = mpsc::channel::<Job>(1);
        let reporter = Arc::new(Self {
            boot,
            log: services.log,
            catalog: services.authorization.stop_catalog(),
            signer: services.signer,
            sender,
            shutdown,
            signaled: OnceLock::new(),
        });
        let worker = reporter.clone();
        crate::asyncrt::try_spawn(async move {
            while let Some(job) = jobs.recv().await {
                let result = worker.apply(&job.request, job.deadline).await;
                // Dropping a caller never cancels an admitted engine update or
                // the following stop signal. The Node owns this task and join.
                let _ = job.reply.send(result);
            }
        })
        .map_err(|_| FleetCatalogError::Closed)?
        .detach();
        Ok(reporter)
    }

    pub async fn submit(
        &self,
        request: FleetStopRequest,
        timeout: Duration,
    ) -> Result<FleetStopAcceptance> {
        request
            .selector
            .validate()
            .map_err(|_| FleetCatalogError::Invalid("stop request"))?;
        if timeout.as_millis() == 0 || timeout > Duration::from_secs(60) {
            return Err(FleetCatalogError::Invalid("stop timeout"));
        }
        if request.selector.boot != self.boot {
            return Err(FleetCatalogError::Conflict);
        }
        let deadline = self
            .log
            .domain()
            .mono_ms()
            .map_err(|_| FleetCatalogError::Closed)?
            .checked_add(timeout.as_millis() as u64)
            .ok_or(FleetCatalogError::Limit)?;
        let (reply, response) = oneshot::channel();
        self.sender
            .try_send(Job {
                request,
                deadline,
                reply,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => FleetCatalogError::Overloaded,
                mpsc::error::TrySendError::Closed(_) => FleetCatalogError::Closed,
            })?;
        let result = crate::asyncrt::timeout(self.remaining(deadline)?, response)
            .await
            .map_err(|_| FleetCatalogError::Unconfirmed)?
            .map_err(|_| FleetCatalogError::Unconfirmed)?;
        self.remaining(deadline)?;
        result
    }

    fn remaining(&self, deadline: u64) -> Result<Duration> {
        let now = self
            .log
            .domain()
            .mono_ms()
            .map_err(|_| FleetCatalogError::Closed)?;
        deadline
            .checked_sub(now)
            .filter(|ms| *ms > 0)
            .map(Duration::from_millis)
            .ok_or(FleetCatalogError::Unconfirmed)
    }

    async fn apply(
        &self,
        request: &FleetStopRequest,
        deadline: u64,
    ) -> Result<FleetStopAcceptance> {
        if request.selector.boot != self.boot {
            return Err(FleetCatalogError::Conflict);
        }
        let cached = self
            .log
            .lifecycle_guard()
            .map_err(log_error)?
            .ok_or(FleetCatalogError::Corrupt("missing local guard"))?;
        if cached.accepted() == Some(&request.selector) && request.action == FleetStopAction::Stop {
            // Exact already-admitted work retains its original scope even if
            // its caller's response or deadline was lost.
            signal_stop(&self.signaled, &self.shutdown)?;
            return Ok(request.acceptance());
        }
        self.catalog
            .verify_local_stop(&request.selector, request.action, self.remaining(deadline)?)
            .await?;
        self.remaining(deadline)?;
        let update = match request.action {
            FleetStopAction::Stop => LifecycleUpdate::Stop {
                selector: request.selector.clone(),
                deadline,
            },
            FleetStopAction::Fence => LifecycleUpdate::Fence {
                boot: request.selector.boot.clone(),
                epoch: request.selector.epoch,
                operation: request.selector.operation,
                request: request.selector.request,
                deadline,
            },
        };
        self.log.update_lifecycle(update).await.map_err(log_error)?;
        if request.action == FleetStopAction::Stop {
            signal_stop(&self.signaled, &self.shutdown)?;
        }
        Ok(request.acceptance())
    }

    /// Called only by the surrounding host after the entire Node domain joins.
    /// A separate bounded reporting domain owns this final store operation.
    pub async fn joined(
        &self,
        outcome: &anyhow::Result<NodeOutcome>,
        executor: tokio::runtime::Handle,
    ) {
        let selector = match self.log.lifecycle_guard() {
            Ok(Some(guard)) if guard.boot == self.boot => guard.accepted().cloned(),
            _ => None,
        };
        let Some(selector) = selector else {
            return;
        };
        let sink = match self.catalog.closure_sink(&selector) {
            Ok(sink) => sink,
            Err(error) => {
                tracing::warn!(%error, "fleet closure sink refused");
                return;
            }
        };
        let reporting = crate::asyncrt::ExecutionDomain::new(executor);
        let result = reporting
            .run(async {
                crate::asyncrt::timeout(
                    Duration::from_secs(5),
                    sink.joined(selector, &self.signer, outcome),
                )
                .await
            })
            .await;
        if !matches!(result, Ok(Ok(Ok(())))) {
            tracing::warn!(?result, "fleet physical closure was not confirmed");
        }
        reporting.shutdown().await;
    }
}

fn signal_stop(
    signaled: &OnceLock<bool>,
    shutdown: &mpsc::UnboundedSender<ShutdownMode>,
) -> Result<()> {
    // The driver may still be joining physical I/O while accepted retries
    // arrive. Remember the one delivery outcome without growing its queue.
    if *signaled.get_or_init(|| shutdown.send(ShutdownMode::Preserve).is_ok()) {
        Ok(())
    } else {
        Err(FleetCatalogError::Unconfirmed)
    }
}

fn log_error(error: LogError) -> FleetCatalogError {
    match error {
        LogError::WrongIdentity | LogError::AlreadyExists => FleetCatalogError::Conflict,
        LogError::Overloaded => FleetCatalogError::Overloaded,
        LogError::Closed => FleetCatalogError::Closed,
        LogError::Invalid(_) => FleetCatalogError::Unconfirmed,
        other => FleetCatalogError::Storage(anyhow::Error::new(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fleet_stop_signal_retries_are_bounded() {
        let (sender, mut commands) = mpsc::unbounded_channel();
        let signaled = OnceLock::new();
        for _ in 0..10_000 {
            assert!(signal_stop(&signaled, &sender).is_ok());
        }
        assert!(matches!(commands.try_recv(), Ok(ShutdownMode::Preserve)));
        assert!(matches!(
            commands.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        drop(commands);
        assert!(signal_stop(&signaled, &sender).is_ok());

        let (sender, commands) = mpsc::unbounded_channel();
        drop(commands);
        let signaled = OnceLock::new();
        for _ in 0..2 {
            assert!(matches!(
                signal_stop(&signaled, &sender),
                Err(FleetCatalogError::Unconfirmed)
            ));
        }
    }
}
