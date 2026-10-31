// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A complete immutable source cut, independent of snapshot installation and
//! voter admission. Run capture and streaming on an admitted blocking owner.

use super::{page_wire, CellImagePin, GroupManifest, ManifestLimits, PreparedGroupManifest};
use crate::raft::{
    budget::{Budget, Permit},
    cache::GroupLog,
    member_state::GroupMembership,
};
use cellar_logic::cell_group::{CellId, GroupHistory};
use cellar_ltx::internal::{ExtentId, EXTENT_BYTES};
mod header;
mod local;
mod prepared;
mod receive;
mod receive_archive;
pub(crate) mod wire;
use header::Header;
pub use local::PreparedLocalCellGroupCheckpoint;
pub(crate) use prepared::persisted_marker;
pub use prepared::PreparedCellGroupCheckpoint;
pub use receive::CellGroupCheckpointReceiver;
pub use receive_archive::{CellGroupArchiveCheckpointReceiver, PreparedCellGroupArchiveCheckpoint};
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Validate authenticated seed metadata before any destination mapping exists.
pub(crate) fn seed_marker(
    snapshot: &raft::prelude::Snapshot,
    target: &cellar_logic::consensus::ReplicaIdentity<GroupHistory>,
    barrier: cellar_logic::consensus::LogIndex,
) -> Result<GroupManifest> {
    let header = Header::decode(&snapshot.data)?;
    let conf = header.configuration()?;
    let id = target.member.get();
    let joined = header.membership.joined(target.member)?;
    if header.snapshot()? != *snapshot
        || header.root.route().history != target.scope
        || barrier.get() == 0
        || header.root.position().index < barrier
        || header.membership.revision() > barrier
        || !conf.learners.contains(&id)
        || conf.voters.contains(&id)
        || conf.voters_outgoing.contains(&id)
        || conf.learners_next.contains(&id)
        || !header.membership.permits_group(target)
        || joined.is_none_or(|(binding, at)| {
            at > barrier || binding.node != target.node || binding.disk != target.disk
        })
    {
        return Err(CellGroupCheckpointError::Invalid("seed descriptor/member"));
    }
    Ok(header.root)
}

pub(crate) fn seed_birth(
    snapshot: &raft::prelude::Snapshot,
    route: &cellar_logic::cell_group::GroupRoute,
    initial: &raft::prelude::ConfState,
) -> Result<()> {
    let header = Header::decode(&snapshot.data)?;
    if header.root.route() != route
        || !header
            .membership
            .same_birth(&GroupMembership::initial(initial))
    {
        return Err(CellGroupCheckpointError::Invalid("seed birth"));
    }
    Ok(())
}

const HEADER_LIMIT: usize = 64 * 1024;

// Covers one verified extent, page/extent metadata encoding, serialization
// temporaries, one flat-image read, membership, traversal stack and fixed owners.
const STREAM_WORKSPACE: usize = EXTENT_BYTES as usize + 1024 * 1024;

#[derive(Debug)]
pub enum CellGroupCheckpointError {
    Authority(crate::raft::completion::RuntimeError),
    Invalid(&'static str),
    Limit,
    Busy,
    Manifest(super::ManifestError),
    Log(crate::raft::storage::LogError),
    Image(cellar_ltx::generation::GenerationError),
    Extent(cellar_ltx::internal::StoreError),
    Sink(std::io::Error),
}

impl std::fmt::Display for CellGroupCheckpointError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "group checkpoint: {self:?}")
    }
}

impl std::error::Error for CellGroupCheckpointError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Authority(error) => Some(error),
            Self::Manifest(error) => Some(error),
            Self::Log(error) => Some(error),
            Self::Image(error) => Some(error),
            Self::Extent(error) => Some(error),
            Self::Sink(error) => Some(error),
            _ => None,
        }
    }
}

impl From<super::ManifestError> for CellGroupCheckpointError {
    fn from(error: super::ManifestError) -> Self {
        Self::Manifest(error)
    }
}

impl From<crate::raft::storage::LogError> for CellGroupCheckpointError {
    fn from(error: crate::raft::storage::LogError) -> Self {
        Self::Log(error)
    }
}

impl From<cellar_ltx::generation::GenerationError> for CellGroupCheckpointError {
    fn from(error: cellar_ltx::generation::GenerationError) -> Self {
        Self::Image(error)
    }
}

impl From<cellar_ltx::internal::StoreError> for CellGroupCheckpointError {
    fn from(error: cellar_ltx::internal::StoreError) -> Self {
        Self::Extent(error)
    }
}

type Result<T> = std::result::Result<T, CellGroupCheckpointError>;

/// One pool must be shared by the node's checkpoint sources and receivers. Byte
/// accounting charges retained metadata and workspace; physical stores bound
/// disk retention. Receivers additionally share a scratch pool for image spools.
#[derive(Clone, Copy, Debug)]
pub struct CellGroupCheckpointLimits {
    pub max_sources: usize,
    pub max_retained_bytes: usize,
    pub max_cells: u32,
    pub max_transfer_bytes: u64,
    pub max_chunk_bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CellGroupCheckpointUsage {
    pub sources: usize,
    pub retained_bytes: usize,
}

#[derive(Clone)]
pub struct CellGroupCheckpointPool {
    budget: Arc<Budget>,
    limits: CellGroupCheckpointLimits,
}

impl CellGroupCheckpointPool {
    pub fn new(limits: CellGroupCheckpointLimits) -> Result<Self> {
        if !(1..=4096).contains(&limits.max_sources)
            || limits.max_retained_bytes < STREAM_WORKSPACE
            || !(1..=1_000_000).contains(&limits.max_cells)
            || limits.max_transfer_bytes < HEADER_LIMIT as u64
            || !(1..=64 * 1024).contains(&limits.max_chunk_bytes)
        {
            return Err(CellGroupCheckpointError::Limit);
        }
        Ok(Self {
            budget: Budget::new(limits.max_retained_bytes, limits.max_sources),
            limits,
        })
    }

    pub fn usage(&self) -> CellGroupCheckpointUsage {
        let (retained_bytes, sources) = self.budget.usage();
        CellGroupCheckpointUsage {
            sources,
            retained_bytes,
        }
    }

    /// Select only the exact engine-published complete root. Membership and
    /// configuration are copied under the same cache read lock as that check.
    /// A prospective, stale or foreign preparation is not a checkpoint source.
    /// O(cells) metadata admission runs off the consensus loop, without image I/O.
    pub fn capture(
        &self,
        group: &GroupLog<GroupHistory>,
        root: &PreparedGroupManifest,
    ) -> Result<CellGroupCheckpoint> {
        self.capture_root(group, root, false)
    }

    /// Pin exactly the retained checkpoint selected by Raft Storage, even after
    /// the current applied root advances. A concurrent replacement may refuse;
    /// a successfully captured source owns its immutable cut through streaming.
    pub fn capture_snapshot(
        &self,
        group: &GroupLog<GroupHistory>,
        current: &PreparedGroupManifest,
        snapshot: &raft::prelude::Snapshot,
    ) -> Result<CellGroupCheckpoint> {
        let root = current.retained_checkpoint(group)?;
        let source = self.capture_root(group, &root, true)?;
        if source.snapshot()? != *snapshot {
            return Err(CellGroupCheckpointError::Invalid(
                "retained snapshot changed",
            ));
        }
        Ok(source)
    }

    pub(super) fn capture_root(
        &self,
        group: &GroupLog<GroupHistory>,
        root: &PreparedGroupManifest,
        is_retained: bool,
    ) -> Result<CellGroupCheckpoint> {
        if !Arc::ptr_eq(&root.engine_owner, &group.owner)
            || !root
                .marker()
                .matches_group(&group.identity().scope, group.birth_digest)
            || root.pinned_images() != root.marker().root().cells() as usize
            || root.pinned_images() > self.limits.max_cells as usize
        {
            return Err(CellGroupCheckpointError::Invalid("source root/owner"));
        }
        let mut retained = STREAM_WORKSPACE
            .checked_add(root.reserved_bytes())
            .ok_or(CellGroupCheckpointError::Limit)?;
        let mut transfer = HEADER_LIMIT as u64;
        root.images.visit(&mut |_, leaf, pin, _, _| -> Result<()> {
            // Account even shared extent trees and inventory identities at a
            // conservative per-extent cost. Payloads remain in their stores.
            let extra = match pin {
                CellImagePin::Extents(pin) => {
                    let count = pin.manifest().geometry().extent_count() as usize;
                    let encoded = pin.reference().encoded_length() as usize;
                    transfer = transfer
                        .checked_add(encoded as u64)
                        .ok_or(CellGroupCheckpointError::Limit)?;
                    count
                        .checked_mul(1024)
                        .and_then(|n| n.checked_add(encoded))
                        .ok_or(CellGroupCheckpointError::Limit)?
                }
                _ => 1024,
            };
            retained = retained
                .checked_add(extra)
                .ok_or(CellGroupCheckpointError::Limit)?;
            transfer = transfer
                .checked_add(page_wire::MAX_PAGE_BYTES as u64)
                .and_then(|n| n.checked_add(leaf.geometry.length))
                .ok_or(CellGroupCheckpointError::Limit)?;
            Ok(())
        })?;
        if transfer > self.limits.max_transfer_bytes {
            return Err(CellGroupCheckpointError::Limit);
        }
        let reservation = self
            .budget
            .acquire(retained)
            .ok_or(CellGroupCheckpointError::Busy)?;
        let header = {
            let cache = group.read()?;
            let marker = root.marker();
            if is_retained {
                if cache.checkpoint_manifest.as_ref() != Some(marker) {
                    return Err(CellGroupCheckpointError::Invalid(
                        "retained checkpoint changed",
                    ));
                }
                let header = Header::decode(&cache.snapshot.data)?;
                if header.snapshot()? != cache.snapshot || &header.root != marker {
                    return Err(CellGroupCheckpointError::Invalid(
                        "retained snapshot binding",
                    ));
                }
                header
            } else {
                if cache.group_manifest.as_ref() != Some(marker)
                    || cache.applied.position != marker.position()
                    || cache.applied.image.is_some()
                    || marker.position().index.get() > cache.hard_state.commit
                    || cache.term(marker.position().index.get())
                        != Some(marker.position().term.get())
                    || marker.membership_revision() != cache.membership.revision().get()
                    || cache
                        .manifest_pins
                        .as_ref()
                        .is_some_and(|pins| pins.root() != marker.root())
                {
                    return Err(CellGroupCheckpointError::Invalid("source durable cut"));
                }
                cache
                    .membership
                    .validate(&cache.conf_state, marker.position().index.get())?;
                Header::capture(marker.clone(), &cache.conf_state, cache.membership.clone())?
            }
        };
        let encoded = header.encode()?;
        Ok(CellGroupCheckpoint {
            group: group.clone(),
            root: root.clone(),
            header: encoded,
            limits: self.limits,
            started: false,
            _reservation: reservation,
        })
    }
}

/// Borrowed part identity. Each manifest page carries the full cell leaf,
/// including empty enrollment, version, receipt-image commitment and effects.
/// Extents are emitted in logical order, including repeated content identities;
/// a receiver may reuse verified objects but must check every manifest binding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CellGroupCheckpointPart<'a> {
    Header,
    Page([u8; 32]),
    Materialized(&'a CellId),
    ExtentManifest(&'a CellId),
    Extent {
        cell: &'a CellId,
        ordinal: u32,
        id: ExtentId,
    },
}

pub struct CellGroupCheckpointChunk<'a> {
    pub part: CellGroupCheckpointPart<'a>,
    pub offset: u64,
    pub length: u64,
    pub bytes: &'a [u8],
}

/// Non-cloneable admitted source. A dropped waiter must leave this value on its
/// joined physical worker until streaming finishes. Borrows prevent parallel
/// streams or retaining a chunk without copying it into separately charged work.
/// Neither this source nor a successful stream grants install/vote authority.
pub struct CellGroupCheckpoint {
    group: GroupLog<GroupHistory>,
    root: PreparedGroupManifest,
    header: Vec<u8>,
    limits: CellGroupCheckpointLimits,
    started: bool,
    _reservation: Permit,
}

impl CellGroupCheckpoint {
    /// Metadata for this immutable source cut; it does not establish publication.
    pub fn snapshot(&self) -> Result<raft::prelude::Snapshot> {
        Header::decode(&self.header)?.snapshot()
    }

    /// Consume an unstreamed captured cut into a bounded publication capability.
    /// The owner and engine independently require that it is still current.
    pub fn prepare_publication(self) -> Result<PreparedLocalCellGroupCheckpoint> {
        local::prepare(self)
    }

    pub fn marker(&self) -> &GroupManifest {
        self.root.marker()
    }

    /// A one-shot stream. On error, previously emitted bytes are incomplete and
    /// must remain unselected on the receiver. Flat-image digest verification
    /// completes only at EOF; extent reads verify their full content first.
    /// No engine/storage mutex is held while invoking the sink.
    pub fn stream(
        &mut self,
        mut sink: impl FnMut(CellGroupCheckpointChunk<'_>) -> Result<()>,
    ) -> Result<()> {
        if self.started {
            return Err(CellGroupCheckpointError::Invalid("source already streamed"));
        }
        self.started = true;
        stream_bytes(
            &self.group,
            self.limits.max_chunk_bytes,
            CellGroupCheckpointPart::Header,
            &self.header,
            &mut sink,
        )?;
        // Reconstruct canonical pages from the immutable verified closure.
        // Exact page hashes must match; current mutable page-store state is
        // never consulted and cannot substitute a later cell at this cut.
        self.root
            .images
            .visit(&mut |reference, leaf, pin, left, right| -> Result<()> {
                let page = page_wire::Page {
                    leaf: leaf.clone(),
                    left,
                    right,
                };
                let bytes = page_wire::encode(
                    &page,
                    self.root.marker().route(),
                    ManifestLimits::default(),
                )?;
                if page_wire::digest(&bytes) != reference.digest {
                    return Err(CellGroupCheckpointError::Invalid(
                        "captured page commitment",
                    ));
                }
                stream_bytes(
                    &self.group,
                    self.limits.max_chunk_bytes,
                    CellGroupCheckpointPart::Page(reference.digest),
                    &bytes,
                    &mut sink,
                )?;
                drop(bytes);
                let cell = &leaf.version.history.cell;
                match pin {
                    CellImagePin::Empty => {}
                    CellImagePin::Materialized(pin) => {
                        let mut reader = pin.open_reader()?;
                        let mut offset = 0;
                        let mut digest = Sha256::new();
                        while offset < reader.length() {
                            self.group.cache_revision()?;
                            let size = (reader.length() - offset)
                                .min(self.limits.max_chunk_bytes as u64)
                                as usize;
                            let bytes = reader.read_exact_at(offset, size)?;
                            digest.update(&bytes);
                            sink(CellGroupCheckpointChunk {
                                part: CellGroupCheckpointPart::Materialized(cell),
                                offset,
                                length: reader.length(),
                                bytes: &bytes,
                            })?;
                            offset += size as u64;
                        }
                        if <[u8; 32]>::from(digest.finalize()) != pin.state().digest {
                            return Err(CellGroupCheckpointError::Invalid(
                                "materialized image digest",
                            ));
                        }
                    }
                    CellImagePin::Extents(pin) => {
                        let manifest = pin
                            .manifest()
                            .encode()
                            .map_err(cellar_ltx::internal::StoreError::from)?;
                        stream_bytes(
                            &self.group,
                            self.limits.max_chunk_bytes,
                            CellGroupCheckpointPart::ExtentManifest(cell),
                            &manifest,
                            &mut sink,
                        )?;
                        drop(manifest);
                        for ordinal in 0..pin.manifest().geometry().extent_count() {
                            self.group.cache_revision()?;
                            let id = pin
                                .manifest()
                                .extent(ordinal)
                                .ok_or(CellGroupCheckpointError::Invalid("extent coverage"))?;
                            let bytes = pin.view().read_extent(id)?;
                            stream_bytes(
                                &self.group,
                                self.limits.max_chunk_bytes,
                                CellGroupCheckpointPart::Extent { cell, ordinal, id },
                                &bytes,
                                &mut sink,
                            )?;
                        }
                    }
                }
                Ok(())
            })?;
        self.group.cache_revision()?;
        Ok(())
    }
}

fn stream_bytes(
    group: &GroupLog<GroupHistory>,
    chunk_bytes: usize,
    part: CellGroupCheckpointPart<'_>,
    bytes: &[u8],
    sink: &mut impl FnMut(CellGroupCheckpointChunk<'_>) -> Result<()>,
) -> Result<()> {
    for (n, chunk) in bytes.chunks(chunk_bytes).enumerate() {
        group.cache_revision()?;
        sink(CellGroupCheckpointChunk {
            part,
            offset: (n * chunk_bytes) as u64,
            length: bytes.len() as u64,
            bytes: chunk,
        })?;
    }
    Ok(())
}
