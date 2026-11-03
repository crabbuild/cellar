// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Shared-group observations use the actual shared admissions. Missing physical
//! observers remain unavailable, including after their owners have exited.
use super::*;
use crate::application::consensus::{RaftExtentUsage, RaftImageUsage, RaftTransportResources};
use cellar_ltx::{generation::GenerationAccounting, internal::NodeExtentObserver};
use std::fmt::Write;

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct CellGroupImageUsage {
    pub materialized: Option<RaftImageUsage>,
    pub extents: Option<RaftExtentUsage>,
}

#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct CellGroupResourceUsage {
    pub host: CellGroupHostUsage,
    pub runtime: CellGroupRuntimeUsage,
    pub control: crate::application::consensus::RaftBudgetUsage,
    pub sql: Option<CellSqlUsage>,
    pub serving_images: CellGroupImageUsage,
    pub archive_verifier_images: Option<CellGroupImageUsage>,
    pub transport: Option<RaftTransportResources>,
    /// Cached manifest metadata over the bounded durable group inventory.
    /// This counts retained cell histories, not current serving authority.
    pub retained_manifest_cells: Option<u64>,
}

#[derive(Clone)]
pub(crate) struct Images {
    pub materialized: Option<GenerationAccounting>,
    pub extents: Option<NodeExtentObserver>,
}

impl Images {
    fn capture(&self) -> CellGroupImageUsage {
        CellGroupImageUsage {
            materialized: self
                .materialized
                .as_ref()
                .and_then(|v| v.snapshot())
                .map(Into::into),
            extents: self
                .extents
                .as_ref()
                .and_then(|v| v.capture())
                .map(Into::into),
        }
    }
}

pub(crate) struct Resources {
    pub(super) sql: CellGroupSqlPool,
    transport: crate::raft::transport::ResourceObserver,
    serving: Images,
    archive: Option<Images>,
}

impl Resources {
    pub(crate) fn new(
        sql: CellGroupSqlPool,
        transport: &crate::raft::transport::RaftTransport,
        serving: Images,
        archive: Option<Images>,
    ) -> Self {
        Self {
            sql,
            transport: transport.resource_observer(),
            serving,
            archive,
        }
    }

    pub(super) fn capture(&self, core: &Core) -> CellGroupResourceUsage {
        let cells = core.log.cell_groups().ok().and_then(|groups| {
            groups.iter().try_fold(0u64, |sum, group| {
                let manifest = group.group_manifest().ok()??;
                sum.checked_add(u64::from(manifest.root().cells()))
            })
        });
        CellGroupResourceUsage {
            host: core.host.usage(),
            runtime: core.runtime.usage(),
            control: crate::application::consensus::RaftBudgetUsage::capture(&core.services.memory),
            sql: self.sql.usage().ok(),
            serving_images: self.serving.capture(),
            archive_verifier_images: self.archive.as_ref().map(Images::capture),
            transport: self.transport.capture(),
            retained_manifest_cells: cells,
        }
    }
}

impl CellGroupResourceUsage {
    /// Fixed vocabularies only. Group, cell, tenant and peer identities never
    /// enter labels. Unavailable observers emit availability without zero usage.
    pub fn prometheus(&self) -> String {
        let mut out = String::new();
        let c = self.host.controller;
        for (name, value) in [
            ("namespaces", self.host.namespaces),
            ("namespace_bytes", self.host.namespace_bytes),
            ("namespace_capacity_bytes", self.host.namespace_byte_limit),
            ("registered_groups", c.groups),
            ("quarantined_groups", c.quarantined_groups),
            ("retained_calls", c.retained_commands),
            ("call_bytes", c.command_bytes),
            ("retained_s3", c.retained_s3),
            ("s3_bytes", c.s3_bytes),
            ("active_apply", c.active_apply),
            (
                "provision_configuration_bytes",
                c.provision_configuration_bytes,
            ),
            ("pending_apply", c.pending_apply),
            ("active_effects", c.active_effects),
        ] {
            sample(&mut out, name, "", value);
        }
        for (lane, usage) in [
            ("ordinary", c.ordinary),
            ("archive_settlement", c.archive_settlement),
        ] {
            let labels = format!("{{lane=\"{lane}\"}}");
            for (name, value) in [
                ("call_admission_items", usage.items),
                ("call_admission_bytes", usage.bytes),
                ("call_admission_capacity_items", usage.max_items),
                ("call_admission_capacity_bytes", usage.max_bytes),
            ] {
                sample(&mut out, name, &labels, value);
            }
        }
        for (lane, usage) in [
            ("ordinary", self.runtime.ordinary),
            ("completion", self.runtime.completions),
            ("snapshot_feedback", self.runtime.snapshot_feedback),
            ("read_pin", self.runtime.read_pins),
        ] {
            let labels = format!("{{lane=\"{lane}\"}}");
            for (name, value) in [
                ("runtime_items", usage.items),
                ("runtime_bytes", usage.bytes),
                ("runtime_capacity_items", usage.max_items),
                ("runtime_capacity_bytes", usage.max_bytes),
            ] {
                sample(&mut out, name, &labels, value);
            }
        }
        sample(
            &mut out,
            "sql_sample_available",
            "",
            usize::from(self.sql.is_some()),
        );
        if let Some(sql) = self.sql {
            for (name, value) in [
                ("sql_workers", sql.running_workers),
                ("sql_worker_capacity", sql.worker_capacity),
                ("sql_active_cells", sql.active_cells),
                ("sql_retained_operations", sql.retained_operations),
                ("sql_retained_bytes", sql.retained_bytes),
                ("sql_queued_or_running", sql.queued_or_running),
                ("sql_apply_operations", sql.apply_retained_operations),
                ("sql_apply_bytes", sql.apply_retained_bytes),
                ("sql_completion_reservations", sql.completion_reservations),
                ("sql_capacity_bytes", sql.limits.node_bytes),
                ("sql_group_capacity_bytes", sql.limits.group_bytes),
                ("sql_capacity_cells", sql.limits.max_pending_cells),
                ("sql_group_capacity_cells", sql.limits.group_pending_cells),
                ("sql_queue_closed", usize::from(sql.closing)),
            ] {
                sample(&mut out, name, "", value);
            }
        }
        for (kind, images) in [
            ("serving", Some(self.serving_images)),
            ("archive_verifier", self.archive_verifier_images),
        ] {
            let labels = format!("{{kind=\"{kind}\"}}");
            sample(
                &mut out,
                "images_sample_available",
                &labels,
                usize::from(images.and_then(|i| i.materialized).is_some()),
            );
            sample(
                &mut out,
                "extents_sample_available",
                &labels,
                usize::from(images.and_then(|i| i.extents).is_some()),
            );
            if let Some(i) = images.and_then(|i| i.materialized) {
                for (name, value) in [
                    ("image_bytes", i.bytes),
                    ("image_capacity_bytes", i.max_bytes),
                    ("image_generations", i.generations as u64),
                    ("image_retained", i.retained as u64),
                ] {
                    sample(&mut out, name, &labels, value);
                }
            }
            if let Some(i) = images.and_then(|i| i.extents) {
                for (name, value) in [
                    ("extent_bytes", i.bytes),
                    ("extent_capacity_bytes", i.max_bytes),
                    ("extent_objects", i.objects as u64),
                    ("extent_roots", i.roots as u64),
                    ("extent_workspace_bytes", i.workspace_bytes),
                    ("extent_quarantined", u64::from(i.quarantined)),
                ] {
                    sample(&mut out, name, &labels, value);
                }
            }
        }
        sample(
            &mut out,
            "manifest_cell_sample_available",
            "",
            usize::from(self.retained_manifest_cells.is_some()),
        );
        if let Some(cells) = self.retained_manifest_cells {
            sample(&mut out, "retained_manifest_cells", "", cells);
        }
        sample(
            &mut out,
            "transport_sample_available",
            "",
            usize::from(self.transport.is_some()),
        );
        out
    }
}

fn sample(out: &mut String, name: &str, labels: &str, value: impl std::fmt::Display) {
    let _ = writeln!(out, "cellar_cell_groups_{name}{labels} {value}");
}
