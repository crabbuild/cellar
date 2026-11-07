// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical host configuration and observations shared by the cell-group runtime.
//! The only production dispatcher/startup is group_host and its node controller.
#[cfg(test)]
use super::completion::*;
pub use super::host_archive::RaftArchiveOptions;
use super::{catalog::*, execution::*};
use crate::application::consensus::RaftClass;
use cellar_logic::consensus::*;
use cellar_ltx::generation::GenerationLimits;
use std::sync::Arc;

#[path = "host_extents.rs"]
mod extents;
#[path = "host_image_limits.rs"]
mod image_limits;
pub use extents::{RaftExtentCacheLimits, RaftExtentLimits, RaftExtentOptions};
#[path = "host_controller.rs"]
mod controller;
pub use controller::{RaftControllerOptions, RaftControllerStatus};
#[path = "host_directory.rs"]
mod directory;
pub use directory::RaftNodeDirectoryOptions;
#[path = "host_enrollment.rs"]
mod enrollment;
pub(crate) use enrollment::NamespaceRegistration;
pub use enrollment::{RaftManagedHostOptions, RaftNamespaceRegistration};
#[path = "host_leader_load.rs"]
mod load;
pub use load::RaftInspectionScanOptions;
#[path = "host_fleet_observation.rs"]
mod fleet_observation;
pub(crate) use fleet_observation::group_membership_digest;
pub use fleet_observation::{FleetMembershipConfiguration, FleetMembershipObservation};
#[path = "host_fleet_cut.rs"]
mod fleet_cut;
pub use super::fleet_inspection_wire::RaftPeerFleetMembershipObservation;
pub(crate) use fleet_cut::group_prefix;
pub use fleet_cut::{FleetImageState, FleetPublishedCut, FleetPublishedImage};
#[path = "host_node.rs"]
mod node;
use super::group_host::RaftCellsConfig;
pub use super::transport_options::RaftPeerNode;
pub use node::{
    RaftHostEnrollmentObservation, RaftHostObservation, RaftHostStartupBounds,
    RaftPeerNodeObservation,
};

// These storage-observation regressions remain enabled while their owning
// generic cache/archive primitives are retained. No predecessor dispatcher runs.
#[cfg(test)]
#[derive(Clone)]
struct ImageView {
    position: LogPosition,
    image: super::cache::CommittedImage,
}
#[cfg(test)]
use super::storage_fault;
#[cfg(test)]
#[path = "host_admission.rs"]
mod admission;
#[cfg(test)]
#[path = "host_archive_status.rs"]
mod archive_status;
#[cfg(test)]
#[path = "host_inspection.rs"]
mod inspection;
#[cfg(test)]
#[path = "host_readiness.rs"]
mod readiness;
#[cfg(test)]
use super::archive::ArchiveError;

#[derive(Clone, Debug)]
pub struct RaftHostOptions {
    /// Shared inventory and caches for namespaces selecting extent images.
    pub extents: Option<RaftExtentOptions>,
    pub close_report: super::operations::RaftCloseReportLimits,
    /// Explicit committed alarm/outbox scheduling. Definitions refuse activation when
    /// this capability is absent; ordinary classes do not start a scheduler.
    pub effects: Option<super::effects::RaftEffectOptions>,
    /// Opt in to bounded automatic repair and optional leadership balancing.
    pub controller: Option<RaftControllerOptions>,
    /// Bounded complete node-wide resource scans. Peer inspection still requires
    /// its independent explicit profile and grants.
    pub inspection_scan: RaftInspectionScanOptions,
    /// Explicit inbound metadata permissions. Discovery and query grants do not
    /// enable inspection. Each peer must match the configured trusted endpoint.
    pub inspection_origins: Vec<super::transport_options::NamespaceInspectionOrigin>,
    /// Enable read-only placement inspection with explicit capacity declarations.
    pub node_profile: Option<super::inspection::RaftNodeProfile>,
    /// Publish and refresh configured physical endpoints, or the complete rooted
    /// node set for managed hosts. Requires a trusted self endpoint and storage.
    pub node_directory: Option<RaftNodeDirectoryOptions>,
    /// Explicit rooted node enrollment and compiled namespace capabilities.
    /// Capacity readiness/activation and complete inventory are separate proofs.
    pub managed: Option<RaftManagedHostOptions>,
    /// Enable native snapshot recovery and its larger image admission floor.
    pub enable_snapshots: bool,
    /// Applied entries between automatic checkpoints. Shared log-cache pressure
    /// can trigger an earlier checkpoint; zero disables automatic compaction.
    pub checkpoint_entries: u64,
    /// Coalesce checkpoints of fully archived applied cuts, even below the
    /// entry threshold. Zero disables this trigger; `checkpoint_entries = 0`
    /// still disables all automatic checkpoints. Defaults to one second.
    pub archive_checkpoint_ms: u64,
    pub archive: RaftArchiveOptions,
    pub max_groups: usize,
    pub blocking_workers: usize,
    pub worker_queue: usize,
    /// Node-wide physical SDK query-reader reservations (1..=16). Each remains
    /// owned until its callback and immutable connection exit, even if cancelled.
    pub max_query_pins: usize,
    pub max_requests: usize,
    /// Independent administrative/inspection admission, including retained waiters.
    pub max_control_requests: usize,
    pub max_control_bytes: usize,
    pub max_pending_bytes: usize,
    pub max_input_bytes: usize,
    pub poll_ms: u64,
    pub stale_source_timeout_ms: u64,
    pub images: GenerationLimits,
    pub execution: ExecutionLimits,
}

impl Default for RaftHostOptions {
    fn default() -> Self {
        let images = GenerationLimits {
            apply: super::snapshot::SnapshotLimits::default().apply(),
            ..Default::default()
        };
        Self {
            extents: None,
            close_report: super::operations::RaftCloseReportLimits::default(),
            effects: None,
            controller: None,
            inspection_scan: RaftInspectionScanOptions::default(),
            inspection_origins: Vec::new(),
            node_directory: None,
            node_profile: None,
            managed: None,
            enable_snapshots: true,
            checkpoint_entries: 256,
            archive_checkpoint_ms: 1000,
            archive: RaftArchiveOptions::default(),
            max_groups: 128,
            blocking_workers: 2,
            worker_queue: 32,
            max_query_pins: 16,
            max_requests: 128,
            max_control_requests: 16,
            max_control_bytes: 16 * 1024 * 1024,
            max_pending_bytes: 64 * 1024 * 1024,
            max_input_bytes: 1024 * 1024,
            poll_ms: 25,
            stale_source_timeout_ms: 5000,
            images,
            execution: ExecutionLimits::default(),
        }
    }
}

impl RaftHostOptions {
    pub(crate) fn validate(&self) -> anyhow::Result<()> {
        self.close_report.validate()?;
        if let Some(managed) = &self.managed {
            managed.validate()?;
            anyhow::ensure!(
                self.max_control_bytes
                    >= 4 * 1024 * 1024 + super::inspection::RESPONSE_RESERVATION + 4096
                    && self.max_control_requests >= 3,
                "managed discovery requires bounded metadata and control capacity"
            );
            anyhow::ensure!(
                self.node_profile.is_some()
                    && self.node_directory.is_some()
                    && self.controller.is_none(),
                "managed enrollment requires a profile/directory; disable the legacy controller"
            );
        }
        if let Some(directory) = &self.node_directory {
            directory.validate()?;
        }
        anyhow::ensure!(
            self.inspection_origins.is_empty()
                || (self.node_profile.is_some() && self.node_directory.is_some()),
            "peer inspection requires a placement profile and physical node directory"
        );
        anyhow::ensure!(
            self.inspection_origins.len() <= 4096,
            "too many inspection grants"
        );
        self.inspection_scan.validate()?;
        if let Some(controller) = &self.controller {
            controller.validate(self)?;
        }
        if let Some(alarms) = self.effects {
            alarms.validate(self)?;
        }
        self.archive.validate()?;
        anyhow::ensure!(
            self.archive_checkpoint_ms <= 60_000,
            "invalid archive checkpoint interval"
        );
        self.execution.validate()?;
        if let Some(extents) = &self.extents {
            extents.validate(self.execution)?;
        }
        anyhow::ensure!(
            self.archive.limits.envelope == self.execution.envelope,
            "archive and execution envelope limits must agree"
        );
        // Snapshot/current/private/apply/incoming and an older source transfer
        // may coexist. Preparation reserves two copies. Additional reader or
        // archive pins may still apply bounded backpressure.
        let (generations, copies) = if self.enable_snapshots {
            (6, 8)
        } else {
            (3, 4)
        };
        anyhow::ensure!(
            self.images.max_generations >= generations
                && self.images.max_bytes >= self.execution.max_image_bytes.saturating_mul(copies),
            "image admission cannot cover configured snapshot/private/apply ownership"
        );
        let apply = self.images.apply;
        let envelope = self.execution.envelope;
        let snapshot_bound =
            cellar_ltx::ltx::full_snapshot_bound(self.execution.max_image_bytes, 512)
                .ok_or_else(|| anyhow::anyhow!("snapshot encoded bound overflow"))?;
        anyhow::ensure!(
            self.archive.limits.max_snapshot_bytes >= snapshot_bound as u64
                && self.archive.limits.max_image_bytes >= self.execution.max_image_bytes,
            "snapshot image and archive admission must cover full-LTX encoding overhead"
        );
        anyhow::ensure!(
            apply.decode.max_image_bytes >= self.execution.max_image_bytes
                && apply.max_files >= envelope.max_files
                && apply.max_total_encoded_bytes >= envelope.max_payload_bytes
                && apply.decode.max_encoded_bytes >= envelope.max_payload_bytes
                && apply.max_total_decoded_bytes >= envelope.max_decoded_bytes
                && apply.decode.max_decoded_bytes >= envelope.max_decoded_bytes
                && apply.max_total_pages as u64 >= envelope.max_pages
                && apply.decode.max_pages as u64 >= envelope.max_pages,
            "image application limits cannot cover admitted execution envelopes"
        );
        anyhow::ensure!(
            self.max_groups > 0
                && self.max_groups <= 4096
                && (1..=16).contains(&self.blocking_workers)
                && (1..=256).contains(&self.worker_queue)
                && (1..=16).contains(&self.max_query_pins)
                && (1..=4096).contains(&self.max_requests)
                && (1..=256).contains(&self.max_control_requests)
                && (4096..=64 * 1024 * 1024).contains(&self.max_control_bytes)
                && (4096..=256 * 1024 * 1024).contains(&self.max_pending_bytes)
                && self.max_input_bytes > 0
                && self.max_input_bytes <= self.execution.envelope.max_payload_bytes
                && self.max_input_bytes + 4096 <= self.max_pending_bytes
                && (1..=1000).contains(&self.poll_ms),
            "invalid Raft host limits"
        );
        anyhow::ensure!(
            (1..=3_600_000).contains(&self.stale_source_timeout_ms),
            "invalid stale-source deadline"
        );
        if let Some(profile) = &self.node_profile {
            profile.validate(self.max_groups, self.images.max_bytes)?;
            anyhow::ensure!(
                self.max_control_requests >= 2
                    && self.max_control_bytes >= super::inspection::RESPONSE_RESERVATION + 4096,
                "Raft inspection requires independent request and response capacity"
            );
        }
        Ok(())
    }
}

pub struct RaftCellsServices {
    /// Explicit infrastructure authority for resuming physically seeded fresh
    /// histories. Archive metadata alone never enables recovered startup.
    /// Requires unmanaged Resume mode and the exact retained seed/engine pins.
    /// Startup returns while quorum activation may still be pending; ordinary
    /// requests remain refused until the actual owner applies activation.
    /// Node shutdown joins any entered recovery verification/publication work.
    pub recovery: Option<
        Arc<
            dyn super::catalog::DisasterFenceAuthority<
                super::cell_groups::CellGroupDisasterFenceRequest,
            >,
        >,
    >,
    pub listeners: crate::daemon::NodeRaftListeners,
    pub tls: super::transport_options::RaftTls,
    pub store: Arc<dyn object_store::ObjectStore>,
    pub application: Arc<dyn crate::application::Application>,
    pub versions: super::version::VersionKeys,
}
