// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::raft::execution::binding_seal;
use cellar_logic::consensus::{ClientId, CommandId};

impl CellGroupDispatcher {
    pub(crate) fn binding_retirement_session(
        &self,
        cell: CellHistory,
        request: &binding_seal::Request,
        schema: [u8; 32],
        limits: CellRoutingLimits,
    ) -> Result<(CellRoutingSession, Permit, VersionKeys), CellDispatchError> {
        if !self.class.has_binding_maintenance() || self.class.schema_digest() != schema {
            return Err(CellDispatchError::Registration(
                RegistrationError::ContractMismatch,
            ));
        }
        let started = self.domain.mono_ms().map_err(|_| closed())?;
        let id = CommandId::new(
            ClientId::new(request.client)
                .map_err(|_| CellDispatchError::Invalid("binding client"))?,
            request.sequence,
        )
        .map_err(|_| CellDispatchError::Invalid("binding sequence"))?;
        let bytes = serde_json::to_vec(request)
            .map_err(|_| CellDispatchError::Invalid("binding retirement encoding"))?;
        if bytes.len() > 4096 {
            return Err(CellDispatchError::Invalid("binding retirement bounds"));
        }
        let admission = self
            .budget
            .acquire(40 * 1024)
            .ok_or(CellDispatchError::Runtime(RuntimeError::Overloaded))?;
        let session = self
            .directory
            .routing_session_in(
                self.domain.clone(),
                CellRouteRequest {
                    cell,
                    operation: CellRouteOperation::BindingRetirement(id),
                    fingerprint: Sha256::digest(&bytes).into(),
                },
                limits,
                None,
                started,
            )
            .map_err(CellDispatchError::Routing)?
            .using_cache(self.routes.clone());
        Ok((session, admission, self.keys.clone()))
    }
}
