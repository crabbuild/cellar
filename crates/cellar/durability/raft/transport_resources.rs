// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A passive observer cannot retain a transport owner or its engine.
use super::*;
use crate::application::consensus::{
    RaftBudgetUsage, RaftConnectionReservations, RaftTransportBanks, RaftTransportResources,
};
use std::sync::atomic::AtomicUsize;

#[derive(Default)]
pub(super) struct Connections {
    control: Arc<AtomicUsize>,
    data: Arc<AtomicUsize>,
    query: Arc<AtomicUsize>,
}

/// Counter metadata has no reference back to the transport or its engine.
pub(super) struct ConnectionOwner(Arc<AtomicUsize>);

impl ConnectionOwner {
    fn new(counter: &Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        Self(counter.clone())
    }
}

impl Drop for ConnectionOwner {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

impl Connections {
    pub(super) fn acquire(&self, lane: Lane) -> ConnectionOwner {
        ConnectionOwner::new(match lane {
            Lane::Control => &self.control,
            Lane::Data => &self.data,
        })
    }

    pub(super) fn query(&self) -> ConnectionOwner {
        ConnectionOwner::new(&self.query)
    }

    fn capture(&self) -> RaftConnectionReservations {
        RaftConnectionReservations {
            control: self.control.load(Ordering::Acquire),
            data: self.data.load(Ordering::Acquire),
            query: self.query.load(Ordering::Acquire),
        }
    }
}

pub(crate) struct ResourceObserver(std::sync::Weak<State>);

impl RaftTransport {
    pub(crate) fn resource_observer(&self) -> ResourceObserver {
        ResourceObserver(Arc::downgrade(&self.handle.0))
    }
}

fn banks(banks: &Banks) -> RaftTransportBanks {
    RaftTransportBanks {
        control: RaftBudgetUsage::capture(&banks.control),
        data: RaftBudgetUsage::capture(&banks.data),
        query: RaftBudgetUsage::capture(&banks.query),
    }
}

impl ResourceObserver {
    pub(crate) fn capture(&self) -> Option<RaftTransportResources> {
        let state = self.0.upgrade()?;
        let (
            tasks,
            peers,
            member_routes,
            router_routes,
            incoming_connections,
            incoming_connection_limits,
        ) = {
            // Poisoned registry data is unavailable, never a healthy zero.
            let registry = state.registry.lock().ok()?;
            // Inspection adds its reserved control slots under this same lock.
            // Capture capacity and availability together so an enabling grant
            // cannot mix an old capacity with a newly expanded semaphore.
            let ordinary = state.options.max_connections_per_lane;
            let control = ordinary.checked_add(
                usize::from(state.inspection_enabled.load(Ordering::Acquire)) * inspections::SLOTS,
            )?;
            let limits = RaftConnectionReservations {
                control,
                data: ordinary,
                query: ordinary,
            };
            let connections = RaftConnectionReservations {
                control: control.checked_sub(state.control_connections.available_permits())?,
                data: ordinary.checked_sub(state.data_connections.available_permits())?,
                query: ordinary.checked_sub(state.query_connections.available_permits())?,
            };
            (
                registry.tasks,
                registry.pools.len(),
                registry.groups.len() + registry.physical_groups.len(),
                registry.query_groups.len(),
                connections,
                limits,
            )
        };
        Some(RaftTransportResources {
            incoming: banks(&state.incoming),
            outgoing: banks(&state.outgoing),
            incoming_connections,
            outgoing_connections: state.outgoing_connections.capture(),
            incoming_connection_limits,
            snapshot_source: RaftBudgetUsage::capture(&state.snapshot_source_budget),
            snapshot_receive: RaftBudgetUsage::capture(&state.snapshot_receive_budget),
            seed_metadata: RaftBudgetUsage::capture(&state.seed_metadata),
            inspection_metadata: RaftBudgetUsage::capture(&state.inspection_metadata),
            tasks,
            peers,
            member_routes,
            router_routes,
            accepted: state.accepted.load(Ordering::Acquire),
            refused: state.refused.load(Ordering::Acquire),
            failed: state.failed.load(Ordering::Acquire),
        })
    }
}
