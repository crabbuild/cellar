//! A passive observer cannot retain a transport owner or its engine.
use super::*;
use crate::application::consensus::{RaftBudgetUsage, RaftTransportBanks, RaftTransportResources};

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
        let (tasks, peers, member_routes, router_routes) = {
            // Poisoned registry data is unavailable, never a healthy zero.
            let registry = state.registry.lock().ok()?;
            (
                registry.tasks,
                registry.pools.len(),
                registry.groups.len(),
                registry.query_groups.len(),
            )
        };
        Some(RaftTransportResources {
            incoming: banks(&state.incoming),
            outgoing: banks(&state.outgoing),
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
