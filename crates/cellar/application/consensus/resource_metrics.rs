//! Fixed label vocabularies, independent of cells, groups and physical peers.
use super::{RaftBudgetUsage, RaftResourceUsage};
use std::collections::BTreeMap;
use std::fmt::Write;

struct Family {
    kind: &'static str,
    samples: Vec<(String, String)>,
}

#[derive(Default)]
struct Text(BTreeMap<String, Family>);

impl Text {
    fn finish(self) -> String {
        let mut output = String::new();
        for (name, family) in self.0 {
            let _ = writeln!(output, "# TYPE {name} {}", family.kind);
            for (labels, value) in family.samples {
                let _ = writeln!(output, "{name}{labels} {value}");
            }
        }
        output
    }
}

fn sample(
    text: &mut Text,
    name: &str,
    labels: &str,
    kind: &'static str,
    value: impl std::fmt::Display,
) {
    text.0
        .entry(name.into())
        .or_insert_with(|| Family {
            kind,
            samples: Vec::new(),
        })
        .samples
        .push((labels.into(), value.to_string()));
}

fn budget(text: &mut Text, prefix: &str, labels: &str, usage: RaftBudgetUsage) {
    for (suffix, value) in [
        ("bytes", usage.bytes),
        ("items", usage.items),
        ("capacity_bytes", usage.max_bytes),
        ("capacity_items", usage.max_items),
    ] {
        sample(text, &format!("{prefix}_{suffix}"), labels, "gauge", value);
    }
}

impl RaftResourceUsage {
    /// Bounded label vocabularies. Live SQL and transport sources expose 68
    /// samples. Unavailable sources expose only their availability gauge,
    /// never invented zero budgets. No identity enters a metric label.
    pub fn prometheus(&self) -> String {
        let mut text = Text::default();
        for (name, usage) in [
            ("business", self.business),
            ("control", self.control),
            ("archive_capture", self.archive_capture),
        ] {
            budget(
                &mut text,
                "cellar_raft_host_budget",
                &format!("{{budget=\"{name}\"}}"),
                usage,
            );
        }
        sample(
            &mut text,
            "cellar_raft_query_pins",
            "",
            "gauge",
            self.query_pins.in_use,
        );
        sample(
            &mut text,
            "cellar_raft_query_pins_capacity",
            "",
            "gauge",
            self.query_pins.limit,
        );
        sample(
            &mut text,
            "cellar_raft_sql_sample_available",
            "",
            "gauge",
            usize::from(self.sql.is_some()),
        );
        if let Some(sql) = self.sql {
            for (name, value) in [
                ("running_workers", sql.running_workers),
                ("worker_capacity", sql.worker_capacity),
                ("work_reservations", sql.work_reservations),
                ("work_capacity", sql.work_capacity),
                ("queue_closed", usize::from(sql.queue_closed)),
            ] {
                sample(
                    &mut text,
                    &format!("cellar_raft_sql_{name}"),
                    "",
                    "gauge",
                    value,
                );
            }
        }
        sample(
            &mut text,
            "cellar_raft_transport_sample_available",
            "",
            "gauge",
            usize::from(self.transport.is_some()),
        );
        if let Some(transport) = self.transport {
            for (direction, banks) in [
                ("incoming", transport.incoming),
                ("outgoing", transport.outgoing),
            ] {
                for (lane, usage) in [
                    ("control", banks.control),
                    ("data", banks.data),
                    ("query", banks.query),
                ] {
                    budget(
                        &mut text,
                        "cellar_raft_transport_budget",
                        &format!("{{direction=\"{direction}\",lane=\"{lane}\"}}"),
                        usage,
                    );
                }
            }
            for (name, usage) in [
                ("snapshot_source", transport.snapshot_source),
                ("snapshot_receive", transport.snapshot_receive),
                ("seed_metadata", transport.seed_metadata),
                ("inspection_metadata", transport.inspection_metadata),
            ] {
                budget(
                    &mut text,
                    "cellar_raft_transport_auxiliary_budget",
                    &format!("{{budget=\"{name}\"}}"),
                    usage,
                );
            }
            for (name, value) in [
                ("tasks", transport.tasks),
                ("peers", transport.peers),
                ("member_routes", transport.member_routes),
                ("router_routes", transport.router_routes),
            ] {
                sample(
                    &mut text,
                    &format!("cellar_raft_transport_{name}"),
                    "",
                    "gauge",
                    value,
                );
            }
            for (name, value) in [
                ("accepted", transport.accepted),
                ("refused", transport.refused),
                ("failed", transport.failed),
            ] {
                sample(
                    &mut text,
                    &format!("cellar_raft_transport_{name}_total"),
                    "",
                    "counter",
                    value,
                );
            }
        }
        text.finish()
    }
}
