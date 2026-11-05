// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit node resources; configuration itself performs no physical work.
use super::*;
use crate::application::consensus::group_protocol::GroupEntryLimits;
use crate::raft::cell_groups::{CellGroupApplyImages, ManifestLimits, ManifestPins};
use crate::raft::{budget::Budget, transport_options::RaftPeerNode};
use std::path::PathBuf;

/// Node-shared dynamic preparation ceilings; no namespace gets its own bank.
#[derive(Clone, Copy, Debug)]
pub struct CellGroupProvisionLimits {
    pub max_preparations: usize,
    pub max_preparation_bytes: usize,
    pub max_parallel_preparations: usize,
    pub max_waiters: usize,
    pub peer_revision: u64,
}

impl CellGroupProvisionLimits {
    pub(crate) fn validate(&self, max_groups: usize) -> Result<(), CellGroupControllerError> {
        if self.max_preparations == 0
            || self.max_preparations > max_groups
            || self.max_preparation_bytes == 0
            || self.max_parallel_preparations == 0
            || self.max_parallel_preparations > self.max_preparations.min(256)
            || !(1..=65_536).contains(&self.max_waiters)
            || self.peer_revision == 0
        {
            return Err(CellGroupControllerError::Invalid(
                "group provisioning limits",
            ));
        }
        Ok(())
    }
}

/// Immutable namespace image resources. Directory identity is checked before
/// catalog reads or physical preparation; a matching name is not ownership.
pub struct CellGroupProvisionNamespace {
    pub directory: RangeDirectory,
    pub images: CellGroupApplyImages,
}

/// Resources for initial members of already assigned ranges. Managed fleet
/// admission is not inferred from this configuration. Image stores retain their
/// own quotas; page inventory and recovery work have additional bounds here.
pub struct CellGroupProvisioning {
    pub directory: PathBuf,
    pub filesystem: Arc<dyn cellar_ltx::FileSystem>,
    pub namespaces: Vec<CellGroupProvisionNamespace>,
    pub manifest: ManifestLimits,
    pub entries: GroupEntryLimits,
    pub max_effect_rows: u32,
    pub max_pages: usize,
    pub max_page_bytes: u64,
    pub max_retained_roots: usize,
    pub peers: Vec<RaftPeerNode>,
    pub peer_revision: u64,
    /// Includes recovered owners retained after a failed attachment.
    pub max_preparations: usize,
    pub max_preparation_bytes: usize,
    pub max_parallel_preparations: usize,
    /// Includes queued calls and unconsumed replies, independently of commands.
    pub max_waiters: usize,
}

pub(in super::super) struct Config {
    pub settings: CellGroupProvisioning,
    pub archives: Option<archive::Provisioning>,
    pub preparations: Arc<Budget>,
    pub waiters: Arc<Budget>,
    pub parallel: Arc<tokio::sync::Semaphore>,
    pub bytes: usize,
    pub configuration_bytes: usize,
    // Startup verified these physical owners before enabling image collection.
    // They have no controller roster or transport registration.
    pub parked: Vec<(CellGroupApplier, PreparedGroupManifest)>,
}

impl Config {
    pub fn new(
        mut settings: CellGroupProvisioning,
        shared: &Shared,
        archives: Option<archive::Provisioning>,
        parked: Vec<(CellGroupApplier, PreparedGroupManifest)>,
    ) -> Result<Self, CellGroupControllerError> {
        let invalid = || CellGroupControllerError::Invalid("group provisioning configuration");
        settings.manifest.validate().map_err(|_| invalid())?;
        settings.entries.validate().map_err(|_| invalid())?;
        if settings.namespaces.is_empty() || settings.namespaces.len() > 4096 {
            return Err(invalid());
        }
        let mut namespaces = std::collections::BTreeSet::new();
        // Trusted endpoints and fixed configuration metadata remain retained
        // even when no preparation is active. Keep them in the same total.
        let mut configuration_bytes = 2 * 1024 * 1024_usize;
        let mut image_bytes = 0;
        for profile in &mut settings.namespaces {
            let namespace = profile.directory.namespace_config();
            if !namespaces.insert((namespace.cluster, namespace.namespace))
                || !matches!(
                    (&profile.images, namespace.image_storage),
                    (
                        CellGroupApplyImages::Materialized { .. },
                        crate::raft::catalog::RaftImageStorage::Materialized
                    ) | (
                        CellGroupApplyImages::Extents { .. },
                        crate::raft::catalog::RaftImageStorage::Extents
                    )
                )
            {
                return Err(invalid());
            }
            profile.images.validate().map_err(apply_error)?;
            let mut metadata = profile
                .directory
                .cache_capacity_bytes()
                .checked_add(8192)
                .ok_or_else(invalid)?;
            if let CellGroupApplyImages::Extents {
                cache_directory, ..
            } = &mut profile.images
            {
                if !cache_directory.is_absolute()
                    || cache_directory.as_os_str().len() > 4096
                    || cache_directory
                        .components()
                        .any(|p| matches!(p, std::path::Component::ParentDir))
                {
                    return Err(invalid());
                }
                metadata = metadata
                    .checked_add(cache_directory.as_os_str().len() * 2)
                    .ok_or_else(invalid)?;
                *cache_directory = std::mem::take(cache_directory)
                    .into_boxed_path()
                    .into_path_buf();
            }
            configuration_bytes = configuration_bytes
                .checked_add(metadata)
                .ok_or_else(invalid)?;
            image_bytes = image_bytes.max(profile.images.memory_bound());
        }
        if !settings.directory.is_absolute()
            || settings.directory.as_os_str().len() > 4096
            || settings
                .directory
                .components()
                .any(|p| matches!(p, std::path::Component::ParentDir))
            || !(1..=1_000_000).contains(&settings.max_effect_rows)
            || !(1..=4_000_000).contains(&settings.max_pages)
            || !(1..=68_719_476_736).contains(&settings.max_page_bytes)
            || !(2..=4096).contains(&settings.max_retained_roots)
            || !(1..=128).contains(&settings.peers.len())
            || settings.peer_revision == 0
            || settings.max_preparations == 0
            || settings.max_preparations > shared.limits.max_groups
            || settings.max_parallel_preparations == 0
            || settings.max_parallel_preparations > settings.max_preparations.min(256)
            || !(1..=65_536).contains(&settings.max_waiters)
        {
            return Err(invalid());
        }
        let local = shared.runtime.log().limits();
        let mut nodes = std::collections::BTreeSet::new();
        let mut addresses = std::collections::BTreeSet::new();
        let mut labels = std::collections::BTreeSet::new();
        for peer in &mut settings.peers {
            if peer.label.len() > 4096
                || peer.server_name.len() > 4096
                || !nodes.insert(peer.node)
                || !labels.insert(peer.label.clone())
                || ![peer.control_address, peer.data_address, peer.query_address]
                    .into_iter()
                    .all(|a| addresses.insert(a))
            {
                return Err(invalid());
            }
            peer.validate_endpoint().map_err(|_| invalid())?;
            peer.label = std::mem::take(&mut peer.label)
                .into_boxed_str()
                .into_string();
            peer.server_name = std::mem::take(&mut peer.server_name)
                .into_boxed_str()
                .into_string();
        }
        if !settings
            .peers
            .iter()
            .any(|p| p.node == local.node && p.disk == local.disk && p.process == local.process)
        {
            return Err(invalid());
        }
        // Bound recovery pins, BTree inventory/leases, recursive page decoding,
        // image scratch and all bounded catalog/peer/request metadata. The active
        // roster retains at most max_groups stores of these configured sizes.
        let mut bytes = ManifestPins::maximum_bytes(settings.manifest.max_cells)
            .checked_add(settings.max_pages.checked_mul(256).ok_or_else(invalid)?)
            .and_then(|n| {
                n.checked_add(
                    settings.manifest.max_depth as usize
                        * crate::raft::cell_groups::page_wire::MAX_PAGE_BYTES,
                )
            })
            .and_then(|n| n.checked_add(image_bytes))
            .and_then(|n| n.checked_add(2 * 1024 * 1024))
            .ok_or_else(invalid)?;
        if let Some(archives) = &archives {
            configuration_bytes = configuration_bytes
                .checked_add(archives.metadata(&settings)?)
                .ok_or_else(invalid)?;
            // A preparation retains both serving and private verifier stores.
            // Charge both complete bounds to the existing node-wide bank.
            bytes = bytes.checked_mul(2).ok_or_else(invalid)?;
        }
        // Immutable namespace handles keep their advertised directory caches alive.
        // Charge those once against the existing total, leaving one shared bank
        // for every namespace's physical preparations. No per-namespace banks.
        let available = settings
            .max_preparation_bytes
            .checked_sub(configuration_bytes)
            .ok_or_else(invalid)?;
        if available < bytes {
            return Err(invalid());
        }
        settings.directory = settings.directory.into_boxed_path().into_path_buf();
        settings.peers = settings.peers.into_boxed_slice().into_vec();
        settings.namespaces = settings.namespaces.into_boxed_slice().into_vec();
        Ok(Self {
            preparations: Budget::new(available, settings.max_preparations),
            waiters: Budget::new(settings.max_waiters * WAITER_BYTES, settings.max_waiters),
            parallel: Arc::new(tokio::sync::Semaphore::new(
                settings.max_parallel_preparations,
            )),
            bytes,
            configuration_bytes,
            parked,
            archives,
            settings,
        })
    }

    pub fn images(
        &self,
        directory: &RangeDirectory,
    ) -> Result<CellGroupApplyImages, CellGroupControllerError> {
        self.profile(directory)
            .map(|profile| profile.images.clone())
    }

    pub fn profile(
        &self,
        directory: &RangeDirectory,
    ) -> Result<&CellGroupProvisionNamespace, CellGroupControllerError> {
        self.settings
            .namespaces
            .iter()
            .find(|profile| profile.directory.same_owner(directory))
            .ok_or(CellGroupControllerError::Invalid(
                "unconfigured namespace owner",
            ))
    }
}
