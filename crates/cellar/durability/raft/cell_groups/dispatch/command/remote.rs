// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One remote attempt consumes the original route budget; loss stays ambiguous.
use super::*;
use crate::raft::query_wire::command::CellCommandCall;
use crate::raft::transport::{CellRemoteCommandResponse, RaftTransport, TransportError};
use cellar_logic::consensus::MemberId;

impl CellCommandInvocation {
    pub(crate) fn bind_remote_route(&mut self, route: super::super::RemoteMutationRoute) {
        self.remote_route = Some(route);
    }

    pub(super) async fn discover_remote_leader(
        &self,
        transport: &RaftTransport,
    ) -> Result<Option<MemberId>, CellCommandError> {
        let attempt = self.current()?;
        let candidates = match transport
            .cell_query_router_members(attempt.route(), self.session.maximum_attempts())
        {
            Ok(members) => members,
            Err(TransportError::UnknownGroup) => return Ok(None),
            Err(error) => return Err(self.error(CellDispatchError::Transport(error))),
        };
        let domain = &self.dispatcher.domain;
        for (index, member) in candidates.iter().enumerate() {
            self.session
                .validate_attempt(attempt)
                .map_err(|error| self.error(CellDispatchError::Routing(error)))?;
            let now = domain.mono_ms().map_err(|_| self.error(closed()))?;
            let remaining = self.session.deadline_ms().saturating_sub(now);
            if remaining < 2 {
                return Err(self.error(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded)));
            }
            // Reserve a share for the mutation after probing each possible peer.
            let deadline = now + (remaining / (candidates.len() - index + 1) as u64).max(1);
            match transport
                .probe_cell_command_leader(
                    attempt.route(),
                    &self.cell,
                    *member,
                    self.dispatcher.keys.clone(),
                    deadline,
                )
                .await
            {
                Ok(true) => return Ok(Some(*member)),
                Ok(false) => (),
                Err(
                    TransportError::Io(_)
                    | TransportError::Timeout
                    | TransportError::StalePeer
                    | TransportError::UnknownGroup
                    | TransportError::Overloaded,
                ) => (),
                Err(error) => return Err(self.error(CellDispatchError::Transport(error))),
            }
        }
        Err(self.error(CellDispatchError::Runtime(RuntimeError::NotReady)))
    }

    /// Execute this catalog attempt on an explicitly selected remote member.
    /// Any possible transmission makes failure ambiguous. Retry this same
    /// invocation/identity; a transport response is never a non-commit proof.
    pub async fn execute_remote(
        &mut self,
        transport: &RaftTransport,
        member: MemberId,
    ) -> Result<CellRemoteCommandResponse, CellCommandError> {
        if self.system {
            return Err(self.error(CellDispatchError::Invalid(
                "private commands require local ownership",
            )));
        }
        let attempt = self.current()?.clone();
        self.session
            .validate_attempt(&attempt)
            .map_err(|e| self.error(CellDispatchError::Routing(e)))?;
        if self.dispatched
            || self.origin.is_some()
            || self.output.is_some()
            || self.remote_route.is_some()
        {
            return Err(self.error(CellDispatchError::Invalid(
                "consumed or local command attempt",
            )));
        }
        if !transport.uses_command_domain(&self.dispatcher.domain) {
            return Err(self.error(CellDispatchError::Invalid("command transport domain")));
        }
        let now = self
            .dispatcher
            .domain
            .mono_ms()
            .map_err(|_| self.error(closed()))?;
        let timeout_ms = self
            .session
            .deadline_ms()
            .checked_sub(now)
            .filter(|n| *n > 0)
            .ok_or_else(|| {
                self.error(CellDispatchError::Runtime(RuntimeError::DeadlineExceeded))
            })?;
        let call = CellCommandCall {
            request: self.request.clone(),
            inputs: self.inputs,
            timeout_ms,
            route_epoch: attempt.route().epoch.get(),
            birth_digest: attempt.route().birth_digest,
        };
        self.session
            .mark_submitted(&attempt)
            .map_err(|e| self.error(CellDispatchError::Routing(e)))?;
        self.dispatched = true;
        self.ambiguous = true;
        let result = self
            .dispatcher
            .domain
            .run(transport.command_cell(
                attempt.route(),
                &self.cell,
                member,
                call,
                self.dispatcher.keys.clone(),
                self.session.deadline_ms(),
            ))
            .await
            .map_err(|_| self.error(closed()))?;
        let response = result.map_err(|error| {
            self.error(match error {
                TransportError::Command(failure) => {
                    CellDispatchError::Execution(ExecutionError::Refused(failure))
                }
                other => CellDispatchError::Transport(other),
            })
        })?;
        self.session
            .validate_attempt(&attempt)
            .map_err(|e| self.error(CellDispatchError::Routing(e)))?;
        Ok(response)
    }
}
