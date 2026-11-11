// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded namespace-authorized discovery before selecting an exact cell history.

use super::*;
use wire::name::CellNameCall;

/// The returned enrollment is a name observation, not command admission, image
/// custody or durability proof. Retain transport capacity until it is consumed.
pub(crate) struct CellRemoteNameResponse {
    record: Option<EnrollmentRecord>,
    _node: Permit,
    _peer: Permit,
}

impl CellRemoteNameResponse {
    pub(crate) fn record(&self) -> Option<&EnrollmentRecord> {
        self.record.as_ref()
    }
}

impl RaftTransport {
    pub(crate) async fn lookup_cell_name(
        &self,
        route: &cellar_logic::cell_group::GroupRoute,
        candidate: &CellHistory,
        member: MemberId,
        deadline: u64,
    ) -> Result<CellRemoteNameResponse, TransportError> {
        let remaining = deadline
            .checked_sub(
                self.handle
                    .0
                    .domain
                    .mono_ms()
                    .map_err(|_| TransportError::Closed)?,
            )
            .filter(|ms| *ms != 0)
            .ok_or(TransportError::Timeout)?;
        let response = self
            .write_cell(
                route,
                candidate,
                member,
                WriteCall::Name(CellNameCall {
                    timeout_ms: remaining,
                    route_epoch: route.epoch.get(),
                    birth_digest: route.birth_digest,
                }),
                None,
                deadline,
            )
            .await?;
        let WriteValue::Name(record) = response.value else {
            return Err(TransportError::Protocol);
        };
        Ok(CellRemoteNameResponse {
            record,
            _node: response.node,
            _peer: response.peer,
        })
    }
}

impl State {
    pub(in crate::raft::transport) async fn execute_cell_name(
        &self,
        wire: &QueryRoute,
        call: CellNameCall,
        deadline: u64,
    ) -> Result<super::super::query_replies::Reply, TransportError> {
        let (_, target, candidate) = wire.decode_shared()?;
        let runtime = self.runtime.shared()?;
        self.authorize_cell_query(wire)?;
        self.check_cell_name_gateway(&candidate)?;
        let result = async {
            // Name discovery needs a namespace read grant, independently of
            // voting membership or permission to initialize/invoke the name.
            let cut = runtime
                .quorum_membership(target.scope, deadline)
                .await
                .map_err(|_| CommandFailure::Unavailable)?;
            let selected = cut.root().route();
            if selected.schema_digest != wire.schema()
                || selected.epoch.get() != call.route_epoch
                || selected.birth_digest != call.birth_digest
            {
                return Err(CommandFailure::Unavailable);
            }
            runtime
                .resolve_cell_name_at(selected.clone(), candidate.clone(), cut)
                .await
                .map_err(|_| CommandFailure::Unavailable)
        }
        .await;
        self.authorize_cell_query(wire)?;
        self.check_cell_name_gateway(&candidate)?;
        Ok(super::super::query_replies::Reply::plain(
            wire::name::encode_result(&result, self.options.max_query_response_bytes)?,
        ))
    }
}
