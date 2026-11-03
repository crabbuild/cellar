// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical-group fleet controls share the installed node's real runtime and
//! authenticated transport. Lost responses do not establish mutation rollback.
use super::*;
use crate::raft::{
    completion::{ReplicationStatus, RestartLeadershipCheck, RuntimeError},
    fleet_catalog::{FleetCatalogError, FleetExecutionCommand, MovementCommand},
    fleet_stop::BootBinding,
    transport::fleet_actions::Delivery,
};
use std::pin::Pin;

const ACTION_BYTES: usize = 128 * 1024;

struct Local {
    command: FleetExecutionCommand,
    origin: BootBinding,
}

enum Dispatch {
    Local(Box<Local>),
    Remote(Delivery),
}

impl RaftCells {
    pub(crate) fn request_peer_movement(
        &self,
        command: MovementCommand,
        minimum_revision: u64,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<(), FleetCatalogError>> + Send>> {
        self.request_peer_fleet_action(
            FleetExecutionCommand::Movement(command),
            minimum_revision,
            timeout,
        )
    }

    pub(crate) fn request_peer_fleet_action(
        &self,
        command: FleetExecutionCommand,
        minimum_revision: u64,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<(), FleetCatalogError>> + Send>> {
        // Admission and the original deadline precede first polling. The
        // remote transport also starts its owned request at invocation.
        let admitted: Result<_, FleetCatalogError> = (|| {
            command.validate()?;
            let (deadline, memory) = self.action_admission(minimum_revision, timeout)?;
            let dispatch = if command.target().node == self.handle.core.node.node.bytes() {
                let origin = self.action_boot(command.target())?;
                Dispatch::Local(Box::new(Local { command, origin }))
            } else {
                Dispatch::Remote(self.handle.core.transport.request_fleet_action(
                    command,
                    minimum_revision,
                    deadline,
                )?)
            };
            Ok((deadline, memory, dispatch))
        })();
        let owner = self.internal_observer();
        Box::pin(async move {
            let (deadline, _memory, dispatch) = admitted?;
            owner.action_live(deadline)?;
            let result = match dispatch {
                Dispatch::Local(local) => {
                    owner
                        .local_action_at(local.command, local.origin, minimum_revision, deadline)
                        .await
                }
                Dispatch::Remote(delivery) => delivery.receive().await,
            };
            // Expiry/close after dispatch is uncertain even when the command
            // may already have reached its serialized owner.
            owner
                .action_live(deadline)
                .map_err(|_| FleetCatalogError::Unconfirmed)?;
            result
        })
    }

    pub(crate) fn execute_local_fleet_action(
        &self,
        command: FleetExecutionCommand,
        origin: BootBinding,
        minimum_revision: u64,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<(), FleetCatalogError>> + Send>> {
        let admitted: Result<_, FleetCatalogError> = (|| {
            command.validate()?;
            origin
                .validate()
                .map_err(|_| FleetCatalogError::Invalid("fleet action origin"))?;
            self.action_boot(command.target())?;
            self.action_admission(minimum_revision, timeout)
        })();
        let owner = self.internal_observer();
        Box::pin(async move {
            let (deadline, _memory) = admitted?;
            owner
                .local_action_at(command, origin, minimum_revision, deadline)
                .await
        })
    }

    fn action_admission(
        &self,
        minimum_revision: u64,
        timeout: Duration,
    ) -> Result<(u64, crate::raft::budget::Permit), FleetCatalogError> {
        if self.is_closed() {
            return Err(FleetCatalogError::Closed);
        }
        if self.handle.core.managed.is_none() {
            return Err(FleetCatalogError::Invalid("no managed fleet action owner"));
        }
        if minimum_revision == 0
            || minimum_revision >= u64::MAX - 2
            || timeout.is_zero()
            || timeout > Duration::from_secs(60)
        {
            return Err(FleetCatalogError::Invalid("fleet action bounds"));
        }
        self.metadata_admission(timeout, ACTION_BYTES)
            .map_err(|error| match error {
                cellar_logic::consensus::CommandFailure::Overloaded => {
                    FleetCatalogError::Overloaded
                }
                cellar_logic::consensus::CommandFailure::UnsupportedCapability => {
                    FleetCatalogError::Invalid("fleet action callback context")
                }
                _ => FleetCatalogError::Unconfirmed,
            })
    }

    fn action_live(&self, deadline: u64) -> Result<(), FleetCatalogError> {
        self.metadata_live(deadline)
            .map_err(|_| FleetCatalogError::Unconfirmed)
    }

    fn action_boot(&self, target: &BootBinding) -> Result<BootBinding, FleetCatalogError> {
        let core = &self.handle.core;
        let guard = core
            .log
            .lifecycle_guard()
            .map_err(|_| FleetCatalogError::Closed)?
            .ok_or(FleetCatalogError::Closed)?;
        let installed = core
            .managed
            .as_ref()
            .ok_or(FleetCatalogError::Invalid("no managed fleet receiver"))?;
        if &guard.boot != target
            || installed.boot_binding()? != guard.boot
            || target.node != core.node.node.bytes()
            || target.disk != core.node.disk.bytes()
            || target.process != core.node.process.bytes()
            || target.engine != core.engine.bytes()
            || !guard.admits_group_start(target) && guard.quiescing().is_none()
        {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(guard.boot)
    }

    async fn local_action_at(
        &self,
        command: FleetExecutionCommand,
        origin: BootBinding,
        minimum_revision: u64,
        deadline: u64,
    ) -> Result<(), FleetCatalogError> {
        self.action_live(deadline)?;
        let target = command.target().clone();
        self.action_boot(&target)?;
        let host = self
            .handle
            .core
            .managed
            .as_ref()
            .ok_or(FleetCatalogError::Closed)?;
        let remaining = deadline
            .checked_sub(
                self.handle
                    .domain
                    .mono_ms()
                    .map_err(|_| FleetCatalogError::Closed)?,
            )
            .filter(|ms| *ms > 0)
            .map(Duration::from_millis)
            .ok_or(FleetCatalogError::Unconfirmed)?;
        let timer = self
            .handle
            .domain
            .sleep_until(deadline)
            .map_err(|_| FleetCatalogError::Closed)?;
        let work = self.handle.domain.run(host.execute_fleet_command(
            command,
            origin,
            minimum_revision,
            self,
            remaining,
        ));
        let result = crate::asyncrt::select! {
            _ = timer => Err(FleetCatalogError::Unconfirmed),
            value = work => value.unwrap_or(Err(FleetCatalogError::Unconfirmed)),
        };
        self.action_live(deadline)
            .map_err(|_| FleetCatalogError::Unconfirmed)?;
        self.action_boot(&target)
            .map_err(|_| FleetCatalogError::Unconfirmed)?;
        result
    }
}

impl RaftGroupHandle {
    /// Leader-local metadata, without a quorum, applied image or health lease.
    pub fn replication_status(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<ReplicationStatus<GroupHistory>, RuntimeError>> + Send + 'static
    {
        self.replication_status_for(timeout)
    }

    pub(crate) fn replication_status_for(
        &self,
        timeout: Duration,
    ) -> impl Future<Output = Result<ReplicationStatus<GroupHistory>, RuntimeError>> + Send + 'static
    {
        let deadline = self.control_deadline(timeout);
        let group = self.clone();
        async move {
            let deadline = deadline?;
            group.control_live(deadline)?;
            let result = super::drain::within(
                &group.owner.handle.domain,
                deadline,
                group
                    .owner
                    .handle
                    .core
                    .runtime
                    .replication_status(group.history),
            )
            .await?;
            group.control_live(deadline)?;
            Ok(result)
        }
    }

    /// An exact restart observation or transfer, rechecked by the real group
    /// owner after queuing. Acceptance never proves election completion.
    pub(crate) fn restart_leadership(
        &self,
        mut check: RestartLeadershipCheck,
        timeout: Duration,
    ) -> impl Future<Output = Result<(), RuntimeError>> + Send + 'static {
        let deadline = self.control_deadline(timeout);
        let group = self.clone();
        async move {
            let deadline = deadline?.min(check.deadline);
            check.deadline = deadline;
            group.control_live(deadline)?;
            super::drain::within(
                &group.owner.handle.domain,
                deadline,
                group
                    .owner
                    .handle
                    .core
                    .runtime
                    .restart_leadership(group.history, check),
            )
            .await?;
            group.control_live(deadline)
        }
    }

    fn control_live(&self, deadline: u64) -> Result<(), RuntimeError> {
        if self.owner.is_closed() {
            return Err(RuntimeError::Closed);
        }
        if self
            .owner
            .handle
            .domain
            .mono_ms()
            .map_err(|_| RuntimeError::Closed)?
            >= deadline
        {
            return Err(RuntimeError::DeadlineExceeded);
        }
        Ok(())
    }
}
