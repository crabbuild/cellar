//! Fixed-size observations of actual local admission reservations.

/// These counters are sampled independently, rather than one atomic snapshot.
/// Retained request/result owners can keep reservations after the host closes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RaftBudgetUsage {
    pub bytes: usize,
    pub items: usize,
    pub max_bytes: usize,
    pub max_items: usize,
}

impl RaftBudgetUsage {
    pub(crate) fn capture(budget: &crate::raft::budget::Budget) -> Self {
        let (bytes, items) = budget.usage();
        let (max_bytes, max_items) = budget.capacity();
        Self {
            bytes,
            items,
            max_bytes,
            max_items,
        }
    }
}

/// Reservations for SDK query callbacks, acquired on the physical SQL owner
/// before opening its immutable database and released after that database drops.
/// Cancellation of a caller does not release a running callback's reservation.
/// Archive, seed, checkpoint and effect readers have their separate budgets.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RaftQueryPinUsage {
    pub in_use: usize,
    pub limit: usize,
}

/// Local host request/output and archive-capture reservations. This is not an
/// aggregate health/quorum proof or the complete transport/image/storage usage.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub struct RaftResourceUsage {
    pub business: RaftBudgetUsage,
    pub control: RaftBudgetUsage,
    pub archive_capture: RaftBudgetUsage,
    pub query_pins: RaftQueryPinUsage,
}

impl RaftResourceUsage {
    /// Fourteen fixed gauge samples. Only three constant budget labels occur;
    /// no cell, group, member, node, process or caller identity enters a label.
    pub fn prometheus(&self) -> String {
        use std::fmt::Write;
        let mut text = String::from(concat!(
            "# TYPE cellar_raft_host_budget_bytes gauge\n",
            "# TYPE cellar_raft_host_budget_items gauge\n",
            "# TYPE cellar_raft_host_budget_capacity_bytes gauge\n",
            "# TYPE cellar_raft_host_budget_capacity_items gauge\n",
            "# TYPE cellar_raft_query_pins gauge\n",
            "# TYPE cellar_raft_query_pins_capacity gauge\n",
        ));
        for (name, usage) in [
            ("business", self.business),
            ("control", self.control),
            ("archive_capture", self.archive_capture),
        ] {
            let _ = writeln!(text,
                "cellar_raft_host_budget_bytes{{budget=\"{name}\"}} {}\ncellar_raft_host_budget_items{{budget=\"{name}\"}} {}\ncellar_raft_host_budget_capacity_bytes{{budget=\"{name}\"}} {}\ncellar_raft_host_budget_capacity_items{{budget=\"{name}\"}} {}",
                usage.bytes, usage.items, usage.max_bytes, usage.max_items);
        }
        let _ = writeln!(
            text,
            "cellar_raft_query_pins {}\ncellar_raft_query_pins_capacity {}",
            self.query_pins.in_use, self.query_pins.limit
        );
        text
    }
}
