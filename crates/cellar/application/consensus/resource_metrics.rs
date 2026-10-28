// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

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
    /// Bounded label vocabularies, with 111 samples when SQL, image, group and
    /// transport sources are live, plus 13 for a configured controller.
    /// A live extent inventory adds 13 samples.
    /// Unavailable sources expose only their availability gauge,
    /// never invented zero budgets. No identity enters a metric label.
    pub fn prometheus(&self) -> String {
        let mut text = Text::default();
        sample(
            &mut text,
            "cellar_raft_extents_sample_available",
            "",
            "gauge",
            usize::from(self.extents.is_some()),
        );
        if let Some(extents) = self.extents {
            for (suffix, value) in [
                ("bytes", extents.bytes),
                ("capacity_bytes", extents.max_bytes),
                ("objects", extents.objects as u64),
                ("capacity_objects", extents.max_objects as u64),
                ("roots", extents.roots as u64),
                ("capacity_roots", extents.max_roots as u64),
                ("root_extents", extents.root_extents as u64),
                ("capacity_root_extents", extents.max_root_extents as u64),
                ("workspace_bytes", extents.workspace_bytes),
                ("workspaces", extents.workspaces as u64),
                ("capacity_workspaces", extents.max_workspaces as u64),
                ("busy", u64::from(extents.busy)),
                ("quarantined", u64::from(extents.quarantined)),
            ] {
                sample(
                    &mut text,
                    &format!("cellar_raft_extents_{suffix}"),
                    "",
                    "gauge",
                    value,
                );
            }
        }
        sample(
            &mut text,
            "cellar_raft_controller_sample_available",
            "",
            "gauge",
            usize::from(self.controller.is_some()),
        );
        if let Some(controller) = self.controller {
            budget(
                &mut text,
                "cellar_raft_controller_metadata",
                "",
                controller.metadata,
            );
            for (name, value) in [
                ("enabled", controller.enabled),
                ("recovering", controller.recovering),
                ("busy", controller.busy),
            ] {
                sample(
                    &mut text,
                    &format!("cellar_raft_controller_{name}"),
                    "",
                    "gauge",
                    usize::from(value),
                );
            }
            for (name, value) in [
                ("probe_rounds", controller.probe_rounds),
                ("repair_requests", controller.repair_requests),
                ("activation_requests", controller.activation_requests),
                ("transfer_attempts", controller.transfer_attempts),
                ("completed_repairs", controller.completed_repairs),
            ] {
                sample(
                    &mut text,
                    &format!("cellar_raft_controller_{name}_total"),
                    "",
                    "counter",
                    value,
                );
            }
            sample(
                &mut text,
                "cellar_raft_controller_sample_age_ms",
                "",
                "gauge",
                controller.sample_age_ms,
            );
        }
        sample(
            &mut text,
            "cellar_raft_groups_sample_available",
            "",
            "gauge",
            usize::from(self.groups.is_some()),
        );
        if let Some(groups) = self.groups {
            for (name, value) in [
                ("durable", groups.durable_groups),
                ("attached", groups.attached_groups),
                ("parked", groups.parked_groups),
                ("leaders", groups.leaders),
                ("voters", groups.voters),
                ("learners", groups.learners),
                ("removed", groups.removed),
                ("unknown", groups.unknown),
                ("coherent_images", groups.coherent_images),
                ("historical_queries", groups.historical_queries),
                ("recent_source_queries", groups.recent_source_queries),
                ("failed", groups.failed),
                ("retired", groups.retired),
                ("sql_busy", groups.sql_busy),
                ("pending_commands", groups.pending_commands),
                ("incoming_seeds", groups.incoming_seeds),
                ("applying", groups.applying),
                ("archive_pending", groups.archive_pending),
            ] {
                sample(
                    &mut text,
                    &format!("cellar_raft_groups_{name}"),
                    "",
                    "gauge",
                    value,
                );
            }
            for (name, value) in [
                ("apply_lag_entries", groups.apply_lag_entries),
                ("max_apply_lag_entries", groups.max_apply_lag_entries),
                ("archive_lag_entries", groups.archive_lag_entries),
                ("max_archive_lag_entries", groups.max_archive_lag_entries),
                ("sample_age_ms", groups.sample_age_ms),
            ] {
                sample(
                    &mut text,
                    &format!("cellar_raft_groups_{name}"),
                    "",
                    "gauge",
                    value,
                );
            }
        }
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
            "cellar_raft_images_sample_available",
            "",
            "gauge",
            usize::from(self.images.is_some()),
        );
        if let Some(images) = self.images {
            for (name, value) in [
                ("stores", images.stores),
                ("generations", images.generations),
                ("retained", images.retained),
                ("busy", images.busy),
                ("capacity_generations", images.max_generations),
            ] {
                sample(
                    &mut text,
                    &format!("cellar_raft_images_{name}"),
                    "",
                    "gauge",
                    value,
                );
            }
            sample(
                &mut text,
                "cellar_raft_images_bytes",
                "",
                "gauge",
                images.bytes,
            );
            sample(
                &mut text,
                "cellar_raft_images_capacity_bytes",
                "",
                "gauge",
                images.max_bytes,
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
            for (direction, connections) in [
                ("incoming", transport.incoming_connections),
                ("outgoing", transport.outgoing_connections),
            ] {
                for (lane, value) in [
                    ("control", connections.control),
                    ("data", connections.data),
                    ("query", connections.query),
                ] {
                    sample(
                        &mut text,
                        "cellar_raft_transport_connections",
                        &format!("{{direction=\"{direction}\",lane=\"{lane}\"}}"),
                        "gauge",
                        value,
                    );
                }
            }
            for (lane, limit) in [
                ("control", transport.incoming_connection_limits.control),
                ("data", transport.incoming_connection_limits.data),
                ("query", transport.incoming_connection_limits.query),
            ] {
                sample(
                    &mut text,
                    "cellar_raft_transport_incoming_connection_capacity",
                    &format!("{{lane=\"{lane}\"}}"),
                    "gauge",
                    limit,
                );
            }
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
