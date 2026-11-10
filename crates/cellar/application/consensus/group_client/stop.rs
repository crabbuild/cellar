// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exact-boot lifecycle RPCs use the node's authenticated transport. Its receiver
//! owns admitted mutations; losing the origin waiter cannot establish rollback.
use super::*;
use crate::raft::{
    fleet_catalog::FleetCatalogError,
    fleet_stop::{FleetStopAcceptance, FleetStopRequest},
    transport::fleet_stops::STOP_REQUEST_BYTES,
};

impl RaftCells {
    /// Submit the exact persisted stop/quiesce/fence request. No business cell
    /// or local group is required. Acceptance proves historical guard admission,
    /// independently of physical closure and terminal removal certificates.
    /// A lost or expired response is Unconfirmed; reconcile the same request.
    pub fn request_peer_fleet_stop(
        &self,
        request: FleetStopRequest,
        minimum_revision: u64,
        timeout: Duration,
    ) -> impl Future<Output = Result<FleetStopAcceptance, FleetCatalogError>> + Send + 'static {
        // Capture deadline and admission before returning the waiter. The
        // transport starts owned bounded work at invocation, not at first poll.
        let domain = self.handle.domain.clone();
        let admitted = (|| {
            if self.is_closed() {
                return Err(FleetCatalogError::Closed);
            }
            if self.handle.core.managed.is_none() {
                return Err(FleetCatalogError::Invalid("no managed fleet origin"));
            }
            if request.selector.validate().is_err()
                || minimum_revision == 0
                || minimum_revision >= u64::MAX - 2
                || timeout.as_millis() == 0
                || timeout > Duration::from_secs(60)
            {
                return Err(FleetCatalogError::Invalid("peer stop request"));
            }
            let deadline = domain
                .mono_ms()
                .map_err(|_| FleetCatalogError::Closed)?
                .checked_add(timeout.as_millis() as u64)
                .ok_or(FleetCatalogError::Limit)?;
            let memory = self
                .control_budget()
                .acquire(STOP_REQUEST_BYTES)
                .ok_or(FleetCatalogError::Overloaded)?;
            let delivery = self.handle.core.transport.request_fleet_stop(
                request,
                minimum_revision,
                deadline,
            )?;
            Ok((deadline, memory, delivery))
        })();
        async move {
            let (deadline, _memory, delivery) = admitted?;
            let received = delivery.receive().await?;
            if !domain.mono_ms().is_ok_and(|now| now < deadline) {
                return Err(FleetCatalogError::Unconfirmed);
            }
            Ok(*received.acceptance)
        }
    }
}
