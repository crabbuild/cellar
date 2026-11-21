// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded maintenance on an exact target history, never business redispatch.

use super::*;
use crate::raft::cell_groups::dispatch::mutation_route::MutationSelectionError;
use crate::raft::cell_groups::{
    CellCommandRequest, CellCommandResponse, CellDispatchError, CellGroupDispatcher,
    CellReadConsistency, CellRoutingLimits, CellSqlOptions,
};
use crate::raft::execution::binding_seal;
use cellar_logic::{
    cell_group::{CellHistory, GroupRoute},
    consensus::{CommandFailure, CommandId},
};

impl CellGroupController {
    pub(crate) async fn binding_retire_routed(
        &self,
        dispatcher: &CellGroupDispatcher,
        transport: &RaftTransport,
        options: CellSqlOptions,
        request: (
            CellHistory,
            [u8; 32],
            binding_seal::Request,
            CellRoutingLimits,
        ),
    ) -> Result<(), CellDispatchError> {
        let shared = &self.handle.0;
        let (cell, schema, request, limits) = request;
        let (mut session, _admission, keys) =
            dispatcher.binding_retirement_session(cell.clone(), &request, schema, limits)?;
        let mut attempt = session.begin().await.map_err(CellDispatchError::Routing)?;
        loop {
            let selected = crate::raft::cell_groups::dispatch::mutation_route::mutation_peer(
                &shared.runtime,
                transport,
                &session,
                &attempt,
                &keys,
                &shared.domain,
            )
            .await;
            let result = match selected {
                Ok(Some(member)) => transport
                    .retire_cell_binding(
                        attempt.route(),
                        &cell,
                        member,
                        request.clone(),
                        keys.clone(),
                        session.deadline_ms(),
                    )
                    .await
                    .map_err(CellDispatchError::Transport),
                Ok(None) => self
                    .binding_retire_local(
                        dispatcher,
                        options.clone(),
                        cell.clone(),
                        (attempt.route().clone(), session.deadline_ms()),
                        request.clone(),
                    )
                    .await
                    .map(drop),
                Err(MutationSelectionError::StalePlacement)
                    if usize::from(attempt.ordinal()) < session.maximum_attempts() =>
                {
                    attempt = session
                        .refresh(&attempt)
                        .await
                        .map_err(CellDispatchError::Routing)?;
                    continue;
                }
                Err(MutationSelectionError::Terminal(error)) => return Err(error),
                Err(error) => Err(error.into_dispatch()),
            };
            match result {
                Ok(()) => return Ok(()),
                Err(error)
                    if usize::from(attempt.ordinal()) < session.maximum_attempts()
                        && !matches!(
                            error.refusal(),
                            Some(
                                CommandFailure::WrongIncarnation
                                    | CommandFailure::CommandConflict
                                    | CommandFailure::UnsupportedCapability
                                    | CommandFailure::CorruptState
                            )
                        ) =>
                {
                    crate::raft::cell_groups::dispatch::mutation_route::backoff(
                        &shared.domain,
                        session.deadline_ms(),
                        shared.limits.retry_interval_ms,
                        session.maximum_attempts() - usize::from(attempt.ordinal()),
                    )
                    .await?;
                    attempt = session
                        .retry(&attempt)
                        .await
                        .map_err(CellDispatchError::Routing)?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Read one coherent allocator value. Discovery admission is released before
    /// the command needs the same one-slot node/SQL budget.
    pub(crate) async fn binding_next_system(
        &self,
        dispatcher: &CellGroupDispatcher,
        options: CellSqlOptions,
        cell: CellHistory,
        route: GroupRoute,
        deadline: u64,
    ) -> Result<CommandId, CellDispatchError> {
        next_system(&self.handle.0, dispatcher, options, cell, route, deadline).await
    }

    pub(crate) async fn binding_retire_local(
        &self,
        dispatcher: &CellGroupDispatcher,
        options: CellSqlOptions,
        cell: CellHistory,
        target: (GroupRoute, u64),
        request: binding_seal::Request,
    ) -> Result<CellCommandResponse, CellDispatchError> {
        let shared = &self.handle.0;
        let _work = shared
            .registration()
            .map_err(CellDispatchError::Runtime)?
            .work
            .take();
        let (route, deadline) = target;
        let minimum = crate::storage::raft_binding_recovery::durability(request.durability)
            .map_err(|_| CellDispatchError::Invalid("binding retirement policy"))?;
        let id = self
            .binding_next_system(
                dispatcher,
                options.clone(),
                cell.clone(),
                route.clone(),
                deadline,
            )
            .await?;
        let remaining = deadline
            .checked_sub(
                shared
                    .domain
                    .mono_ms()
                    .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?,
            )
            .filter(|n| *n != 0)
            .ok_or(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded))?;
        let retry_until_ms = u64::try_from(crate::asyncrt::wall_ms())
            .map_err(|_| CellDispatchError::Invalid("binding clock"))?
            .checked_add(options.limits().max_retry_window_ms.min(60_000))
            .ok_or(CellDispatchError::Invalid("binding retry horizon"))?;
        let mut invocation = dispatcher
            .system_command(
                cell,
                CellCommandRequest {
                    id,
                    contract: binding_seal::contract(),
                    input: serde_json::to_vec(&request)
                        .map_err(|_| CellDispatchError::Invalid("binding request"))?,
                    durability: minimum,
                    retry_until_ms,
                },
                CellRoutingLimits {
                    timeout: std::time::Duration::from_millis(remaining),
                    max_attempts: 1,
                },
            )
            .await
            .map_err(|e| *e.cause)?;
        invocation
            .bind_remote_route(
                crate::raft::cell_groups::dispatch::RemoteMutationRoute {
                    group: route.history,
                    schema: route.schema_digest,
                    epoch: route.epoch.get(),
                    birth: route.birth_digest,
                },
                deadline,
            )
            .map_err(|e| *e.cause)?;
        let call = self
            .command(invocation, options)
            .map_err(|e| *e.error.cause)?;
        let response = call.response().await.map_err(|e| {
            e.cause.refusal().cloned().map_or(
                CellDispatchError::Execution(ExecutionError::Uncertain),
                |failure| CellDispatchError::Execution(ExecutionError::Refused(failure)),
            )
        })?;
        if response.result().len() != 1
            || response.result()[0] > 2
            || response.durability() < minimum
        {
            return Err(CellDispatchError::Invalid("binding retirement proof"));
        }
        Ok(response)
    }
}

pub(super) async fn next_system(
    shared: &Shared,
    dispatcher: &CellGroupDispatcher,
    options: CellSqlOptions,
    cell: CellHistory,
    route: GroupRoute,
    deadline: u64,
) -> Result<CommandId, CellDispatchError> {
    let (_, class, _) = dispatcher.host_binding()?;
    if !class.has_binding_maintenance() || class.schema_digest() != route.schema_digest {
        return Err(CellDispatchError::Registration(
            crate::application::consensus::RegistrationError::ContractMismatch,
        ));
    }
    let sql = shared
        .sql
        .upgrade()
        .ok_or(CellDispatchError::Runtime(RuntimeError::Closed))?;
    let client = crate::raft::effects::system_client(&cell)
        .map_err(|failure| CellDispatchError::Execution(ExecutionError::Refused(failure)))?;
    loop {
        let selected = async {
            let authority = shared
                .runtime
                .read_authority(
                    route.history,
                    cell.clone(),
                    CellReadConsistency::Linearizable,
                    deadline,
                )
                .await
                .map_err(CellDispatchError::Runtime)?;
            if authority.route() != &route {
                return Err(CellDispatchError::Invalid("binding maintenance route"));
            }
            // The SQL owner already bounds and joins this read. Do not acquire
            // command admission or inspect effect payloads to read one counter;
            // active business commands must be able to coexist with discovery.
            let read = sql
                .query(options.clone(), authority, |database, version| {
                    Ok(crate::raft::effects::scan::next_command(
                        database.connection(),
                        &version.history,
                    )?
                    .sequence()
                    .to_be_bytes()
                    .to_vec())
                })
                .await
                .map_err(CellDispatchError::Execution)?;
            if read.version().history != cell {
                return Err(CellDispatchError::Invalid("binding allocator history"));
            }
            let sequence = u64::from_be_bytes(
                read.result()
                    .try_into()
                    .map_err(|_| CellDispatchError::Invalid("binding allocator result"))?,
            );
            drop(read);
            CommandId::new(client, sequence)
                .map_err(|_| CellDispatchError::Invalid("binding allocator sequence"))
        }
        .await;
        match selected {
            Err(CellDispatchError::Runtime(RuntimeError::Overloaded | RuntimeError::NotReady))
            | Err(CellDispatchError::Execution(
                ExecutionError::Busy | ExecutionError::Refused(CommandFailure::Overloaded),
            )) => {
                let now = shared
                    .domain
                    .mono_ms()
                    .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?;
                if now >= deadline {
                    return Err(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded));
                }
                // Only repeat the read, within its original maintenance budget.
                // Retrying the whole turn would publish another source claim.
                shared
                    .domain
                    .sleep_until(
                        now.saturating_add(shared.limits.retry_interval_ms)
                            .min(deadline),
                    )
                    .map_err(|_| CellDispatchError::Runtime(RuntimeError::Closed))?
                    .await;
            }
            result => return result,
        }
    }
}
