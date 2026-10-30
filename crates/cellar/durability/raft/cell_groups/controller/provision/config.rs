// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Explicit node resources; configuration itself performs no physical work.
use super::*;
use crate::application::consensus::group_protocol::GroupEntryLimits;
use crate::raft::cell_groups::{CellGroupApplyImages, ManifestLimits, ManifestPins};
use crate::raft::{budget::Budget, transport_options::RaftPeerNode};
use std::path::PathBuf;

/// Resources for initial members of already assigned ranges. Managed fleet
/// admission is not inferred from this configuration. Image stores retain their
/// own quotas; page inventory and recovery work have additional bounds here.
pub struct CellGroupProvisioning {
    pub directory: PathBuf,
    pub filesystem: Arc<dyn cellar_ltx::FileSystem>,
    pub images: CellGroupApplyImages,
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
    pub preparations: Arc<Budget>,
    pub waiters: Arc<Budget>,
    pub parallel: Arc<tokio::sync::Semaphore>,
    pub bytes: usize,
}

impl Config {
    pub fn new(
        mut settings: CellGroupProvisioning,
        shared: &Shared,
    ) -> Result<Self, CellGroupControllerError> {
        let invalid = || CellGroupControllerError::Invalid("group provisioning configuration");
        settings.manifest.validate().map_err(|_| invalid())?;
        settings.entries.validate().map_err(|_| invalid())?;
        settings.images.validate().map_err(apply_error)?;
        if let CellGroupApplyImages::Extents {
            cache_directory, ..
        } = &mut settings.images
        {
            if !cache_directory.is_absolute() || cache_directory.as_os_str().len() > 4096 {
                return Err(invalid());
            }
            *cache_directory = std::mem::take(cache_directory)
                .into_boxed_path()
                .into_path_buf();
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
        let bytes = ManifestPins::maximum_bytes(settings.manifest.max_cells)
            .checked_add(settings.max_pages.checked_mul(256).ok_or_else(invalid)?)
            .and_then(|n| {
                n.checked_add(
                    settings.manifest.max_depth as usize
                        * crate::raft::cell_groups::page_wire::MAX_PAGE_BYTES,
                )
            })
            .and_then(|n| n.checked_add(settings.images.memory_bound()))
            .and_then(|n| n.checked_add(2 * 1024 * 1024))
            .ok_or_else(invalid)?;
        if settings.max_preparation_bytes < bytes {
            return Err(invalid());
        }
        settings.directory = settings.directory.into_boxed_path().into_path_buf();
        settings.peers = settings.peers.into_boxed_slice().into_vec();
        Ok(Self {
            preparations: Budget::new(settings.max_preparation_bytes, settings.max_preparations),
            waiters: Budget::new(settings.max_waiters * WAITER_BYTES, settings.max_waiters),
            parallel: Arc::new(tokio::sync::Semaphore::new(
                settings.max_parallel_preparations,
            )),
            bytes,
            settings,
        })
    }
}
