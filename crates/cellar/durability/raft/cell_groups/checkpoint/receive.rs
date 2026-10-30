// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::super::{
    apply::images::image_id, CellGroupApplier, CellImageGeometry, CellManifestLeaf, ManifestTree,
};
use super::*;
use crate::application::consensus::cell_routing_key;
use cellar_logic::cell_group::{CellImageCommitment, CellKey};
use cellar_ltx::{
    apply::ImageState,
    generation::GenerationReceive,
    internal::{ExtentManifest, ImageBinding, ImageRoot, RootView},
    stream_snapshot::ScratchPool,
    TXID,
};
use std::{collections::BTreeMap, path::PathBuf};

// Includes simultaneously received/verified extent bytes, receiver metadata
// decoding, header/page encoding, scratch verification and traversal stacks.
const RECEIVE_WORKSPACE: usize = 2 * EXTENT_BYTES as usize + 2 * 1024 * 1024;

impl CellGroupCheckpointPool {
    /// Share this node pool across both source and receiver owners. The existing
    /// max_sources setting bounds their combined count. The scratch pool must
    /// likewise be shared by all receivers using this volume.
    pub fn receive(
        &self,
        applier: CellGroupApplier,
        scratch: ScratchPool,
        directory: PathBuf,
    ) -> Result<CellGroupCheckpointReceiver> {
        if !directory.is_absolute() || directory.as_os_str().len() > 4096 {
            return Err(CellGroupCheckpointError::Invalid(
                "receive scratch directory",
            ));
        }
        let cells = self.limits.max_cells as usize;
        let extents = usize::try_from(self.limits.max_transfer_bytes.div_ceil(EXTENT_BYTES))
            .map_err(|_| CellGroupCheckpointError::Limit)?;
        let bytes = super::super::ManifestPins::maximum_bytes(self.limits.max_cells)
            .checked_mul(3)
            .and_then(|n| n.checked_add(cells.checked_mul(4096)?))
            .and_then(|n| n.checked_add(extents.checked_mul(2048)?))
            .and_then(|n| n.checked_add(RECEIVE_WORKSPACE))
            .ok_or(CellGroupCheckpointError::Limit)?;
        let reservation = self
            .budget
            .acquire(bytes)
            .ok_or(CellGroupCheckpointError::Busy)?;
        let base = applier
            .group
            .group_manifest()?
            .ok_or(CellGroupCheckpointError::Invalid("receiver base"))?;
        applier
            .storage
            .lock()
            .map_err(|_| CellGroupCheckpointError::Invalid("receiver page owner"))?
            .pages
            .require_live()?;
        Ok(CellGroupCheckpointReceiver {
            applier,
            base,
            scratch,
            directory,
            limits: self.limits,
            header: None,
            part: None,
            cell: None,
            images: BTreeMap::new(),
            pages: Vec::new(),
            last_key: None,
            transferred: 0,
            failed: false,
            reservation,
        })
    }
}

/// Owns all staged page/image pins and the full receive reservation. Appending
/// and finishing belong on the same joined blocking owner. No partial state is
/// published, and no source pin is accepted as a receiver durability proof.
pub struct CellGroupCheckpointReceiver {
    applier: CellGroupApplier,
    base: GroupManifest,
    scratch: ScratchPool,
    directory: PathBuf,
    limits: CellGroupCheckpointLimits,
    header: Option<Header>,
    part: Option<Part>,
    cell: Option<ReceivingCell>,
    images: BTreeMap<CellKey, (CellManifestLeaf, CellImagePin)>,
    pages: Vec<super::super::page_store::ManifestPagePin>,
    last_key: Option<CellKey>,
    transferred: u64,
    failed: bool,
    reservation: Permit,
}

struct ReceivingCell {
    leaf: CellManifestLeaf,
    view: Option<RootView>,
    next_extent: u32,
}

#[derive(Clone, PartialEq, Eq)]
enum Kind {
    Header,
    Page([u8; 32]),
    Materialized(CellId),
    ExtentManifest(CellId),
    Extent(CellId, u32, ExtentId),
}

impl Kind {
    fn of(part: CellGroupCheckpointPart<'_>) -> Self {
        match part {
            CellGroupCheckpointPart::Header => Self::Header,
            CellGroupCheckpointPart::Page(id) => Self::Page(id),
            CellGroupCheckpointPart::Materialized(cell) => Self::Materialized(cell.clone()),
            CellGroupCheckpointPart::ExtentManifest(cell) => Self::ExtentManifest(cell.clone()),
            CellGroupCheckpointPart::Extent { cell, ordinal, id } => {
                Self::Extent(cell.clone(), ordinal, id)
            }
        }
    }
}

struct Part {
    kind: Kind,
    length: u64,
    received: u64,
    data: Data,
}

enum Data {
    Bytes(Vec<u8>),
    Image(GenerationReceive),
}

fn invalid(message: &'static str) -> CellGroupCheckpointError {
    CellGroupCheckpointError::Invalid(message)
}

impl CellGroupCheckpointReceiver {
    pub(super) fn wire_extent(
        &self,
        cell: &CellId,
        ordinal: u32,
        digest: [u8; 32],
        length: u64,
    ) -> Result<ExtentId> {
        let pending = self.cell.as_ref().ok_or(invalid("extent without leaf"))?;
        let id = pending
            .view
            .as_ref()
            .and_then(|view| view.manifest().extent(ordinal))
            .ok_or(invalid("extent without manifest"))?;
        if &pending.leaf.version.history.cell != cell
            || ordinal != pending.next_extent
            || id.bytes() != digest
            || u64::from(id.length()) != length
        {
            return Err(invalid("wire extent binding"));
        }
        Ok(id)
    }

    pub fn append(&mut self, chunk: CellGroupCheckpointChunk<'_>) -> Result<()> {
        if self.failed {
            return Err(invalid("failed receiver"));
        }
        self.failed = true;
        self.append_inner(chunk)?;
        self.failed = false;
        Ok(())
    }

    fn append_inner(&mut self, chunk: CellGroupCheckpointChunk<'_>) -> Result<()> {
        self.applier.group.cache_revision()?;
        if chunk.bytes.is_empty() || chunk.bytes.len() > self.limits.max_chunk_bytes {
            return Err(CellGroupCheckpointError::Limit);
        }
        self.transferred = self
            .transferred
            .checked_add(chunk.bytes.len() as u64)
            .filter(|n| *n <= self.limits.max_transfer_bytes)
            .ok_or(CellGroupCheckpointError::Limit)?;
        let kind = Kind::of(chunk.part);
        if self.part.is_none() {
            if chunk.offset != 0 {
                return Err(invalid("receive part offset"));
            }
            self.part = Some(self.start_part(kind.clone(), chunk.length)?);
        }
        let part = self.part.as_mut().ok_or(invalid("receive part"))?;
        let next = chunk
            .offset
            .checked_add(chunk.bytes.len() as u64)
            .ok_or(CellGroupCheckpointError::Limit)?;
        if part.kind != kind
            || part.length != chunk.length
            || part.received != chunk.offset
            || next > part.length
        {
            return Err(invalid("mixed or noncontiguous receive part"));
        }
        match &mut part.data {
            Data::Bytes(bytes) => bytes.extend_from_slice(chunk.bytes),
            Data::Image(image) => image.append(chunk.offset, chunk.bytes)?,
        }
        part.received = next;
        if next == part.length {
            let completed = self.part.take().ok_or(invalid("receive completion"))?;
            self.complete_part(completed)?;
        }
        Ok(())
    }

    fn start_part(&self, kind: Kind, length: u64) -> Result<Part> {
        if length == 0 || length > self.limits.max_transfer_bytes {
            return Err(CellGroupCheckpointError::Limit);
        }
        let limit = match &kind {
            Kind::Header if self.header.is_none() && self.pages.is_empty() => HEADER_LIMIT as u64,
            Kind::Page(_) if self.header.is_some() && self.cell.is_none() => {
                if self.pages.len() >= self.header.as_ref().unwrap().root.root().cells() as usize {
                    return Err(invalid("extra checkpoint page"));
                }
                page_wire::MAX_PAGE_BYTES as u64
            }
            Kind::Materialized(cell) => {
                let pending = self.cell.as_ref().ok_or(invalid("image without leaf"))?;
                if &pending.leaf.version.history.cell != cell
                    || !matches!(
                        pending.leaf.version.image.commitment,
                        CellImageCommitment::Materialized(_)
                    )
                    || length != pending.leaf.geometry.length
                {
                    return Err(invalid("materialized part binding"));
                }
                let CellImageCommitment::Materialized(digest) =
                    pending.leaf.version.image.commitment
                else {
                    unreachable!()
                };
                let expected = ImageState {
                    epoch: pending.leaf.version.image.epoch,
                    txid: TXID(pending.leaf.version.image.txid),
                    checksum: pending.leaf.version.image.checksum,
                    digest,
                    length,
                    page_size: pending.leaf.geometry.page_size,
                };
                let id = image_id(
                    &pending.leaf.version.history,
                    pending.leaf.version.sequence.get(),
                    expected,
                )
                .map_err(|_| invalid("received image identity"))?;
                let storage = self
                    .applier
                    .storage
                    .lock()
                    .map_err(|_| invalid("receiver storage"))?;
                let super::super::CellGroupApplyImages::Materialized { images, .. } =
                    &storage.images
                else {
                    return Err(invalid("receiver image representation"));
                };
                return Ok(Part {
                    kind,
                    length,
                    received: 0,
                    data: Data::Image(images.receive_image(
                        id,
                        expected,
                        &self.scratch,
                        &self.directory,
                    )?),
                });
            }
            Kind::ExtentManifest(cell) => {
                let pending = self
                    .cell
                    .as_ref()
                    .ok_or(invalid("extent manifest without leaf"))?;
                let CellImageCommitment::Extents { manifest_bytes, .. } =
                    pending.leaf.version.image.commitment
                else {
                    return Err(invalid("extent manifest representation"));
                };
                if &pending.leaf.version.history.cell != cell
                    || pending.view.is_some()
                    || length != u64::from(manifest_bytes)
                {
                    return Err(invalid("extent manifest binding"));
                }
                // Includes the largest supported extent tree, while staying
                // inside the admitted metadata workspace before allocation.
                u64::from(manifest_bytes).min(1024 * 1024)
            }
            Kind::Extent(cell, ordinal, id) => {
                let pending = self.cell.as_ref().ok_or(invalid("extent without leaf"))?;
                let view = pending
                    .view
                    .as_ref()
                    .ok_or(invalid("extent without manifest"))?;
                if &pending.leaf.version.history.cell != cell
                    || *ordinal != pending.next_extent
                    || view.manifest().extent(*ordinal) != Some(*id)
                    || length != u64::from(id.length())
                {
                    return Err(invalid("extent part binding"));
                }
                EXTENT_BYTES
            }
            _ => return Err(invalid("unexpected checkpoint part")),
        };
        if length == 0 || length > limit {
            return Err(CellGroupCheckpointError::Limit);
        }
        Ok(Part {
            kind,
            length,
            received: 0,
            data: Data::Bytes(Vec::with_capacity(length as usize)),
        })
    }

    fn complete_part(&mut self, part: Part) -> Result<()> {
        match (part.kind, part.data) {
            (Kind::Header, Data::Bytes(bytes)) => {
                let header = Header::decode(&bytes)?;
                if header.root.route() != self.base.route()
                    || header.root.root().cells() > self.limits.max_cells
                    || header.root.position().index < self.base.position().index
                {
                    return Err(invalid("receiver history/range/cut"));
                }
                header
                    .membership
                    .succeeds(&self.applier.group.membership()?)?;
                self.header = Some(header);
            }
            (Kind::Page(digest), Data::Bytes(bytes)) => {
                let header = self.header.as_ref().ok_or(invalid("page without header"))?;
                if page_wire::digest(&bytes) != digest {
                    return Err(invalid("received page digest"));
                }
                let limits = ManifestLimits {
                    max_cells: self.limits.max_cells,
                    max_depth: 32,
                };
                let page = page_wire::decode(&bytes, header.root.route(), limits)?;
                page.reference(&bytes, limits)?;
                let key = cell_routing_key(
                    header.root.route().history.namespace,
                    &page.leaf.version.history.cell,
                );
                if self.last_key.as_ref().is_some_and(|last| last >= &key)
                    || page.leaf.last_mutation.index > header.root.position().index
                    || page.leaf.last_mutation.term > header.root.position().term
                {
                    return Err(invalid("checkpoint leaf order/cut"));
                }
                let mut storage = self
                    .applier
                    .storage
                    .lock()
                    .map_err(|_| invalid("receiver storage"))?;
                storage.pages.put(digest, &bytes)?;
                self.pages.push(storage.pages.pin(digest)?);
                self.last_key = Some(key.clone());
                if page.leaf.version.image.txid == 0 {
                    CellImagePin::Empty.check(&page.leaf, header.root.route().schema_digest)?;
                    self.images.insert(key, (page.leaf, CellImagePin::Empty));
                } else {
                    self.cell = Some(ReceivingCell {
                        leaf: page.leaf,
                        view: None,
                        next_extent: 0,
                    });
                }
            }
            (Kind::Materialized(_), Data::Image(image)) => {
                self.finish_cell(CellImagePin::Materialized(image.finish()?))?
            }
            (Kind::ExtentManifest(_), Data::Bytes(bytes)) => {
                let pending = self.cell.as_mut().ok_or(invalid("extent cell"))?;
                let CellImageCommitment::Extents { root, .. } =
                    pending.leaf.version.image.commitment
                else {
                    return Err(invalid("extent leaf"));
                };
                let manifest = ExtentManifest::decode(&bytes, ImageRoot::from_bytes(root))
                    .map_err(cellar_ltx::internal::StoreError::from)?;
                let image = pending.leaf.version.image;
                if manifest.binding()
                    != (ImageBinding {
                        history: super::super::cell_image_history_digest(
                            &pending.leaf.version.history,
                        ),
                        schema: self.base.route().schema_digest,
                        epoch: image.epoch,
                        txid: image.txid,
                        checksum: image.checksum,
                    })
                    || (CellImageGeometry {
                        length: manifest.geometry().length(),
                        page_size: manifest.geometry().page_size(),
                    }) != pending.leaf.geometry
                {
                    return Err(invalid("extent image binding"));
                }
                let storage = self
                    .applier
                    .storage
                    .lock()
                    .map_err(|_| invalid("receiver storage"))?;
                let super::super::CellGroupApplyImages::Extents { images, .. } = &storage.images
                else {
                    return Err(invalid("receiver image representation"));
                };
                pending.view = Some(images.stage_transient_root(manifest)?);
            }
            (Kind::Extent(_, _, id), Data::Bytes(bytes)) => {
                let pin = {
                    let pending = self.cell.as_mut().ok_or(invalid("extent cell"))?;
                    let view = pending.view.as_ref().ok_or(invalid("extent view"))?;
                    let storage = self
                        .applier
                        .storage
                        .lock()
                        .map_err(|_| invalid("receiver storage"))?;
                    let super::super::CellGroupApplyImages::Extents { images, .. } =
                        &storage.images
                    else {
                        return Err(invalid("receiver image representation"));
                    };
                    images.put_extent_bytes(id, &bytes)?;
                    drop(bytes);
                    pending.next_extent += 1;
                    if pending.next_extent == view.manifest().geometry().extent_count() {
                        Some(CellImagePin::Extents(Arc::new(images.seal_image_root(
                            view.manifest().clone(),
                            &mut vec![0; EXTENT_BYTES as usize],
                        )?)))
                    } else {
                        None
                    }
                };
                if let Some(pin) = pin {
                    self.finish_cell(pin)?;
                }
            }
            _ => return Err(invalid("receive part type")),
        }
        Ok(())
    }

    fn finish_cell(&mut self, pin: CellImagePin) -> Result<()> {
        let pending = self.cell.take().ok_or(invalid("missing receive cell"))?;
        pin.check(&pending.leaf, self.base.route().schema_digest)?;
        let key = cell_routing_key(
            self.base.route().history.namespace,
            &pending.leaf.version.history.cell,
        );
        if self.images.insert(key, (pending.leaf, pin)).is_some() {
            return Err(invalid("duplicate receive cell"));
        }
        Ok(())
    }

    /// Verify the complete authenticated tree against every independently sealed
    /// destination image and sync its page closure. This still does not install
    /// it: only the Raft owner's exact snapshot effect may publish the result.
    pub fn finish(self) -> Result<PreparedCellGroupCheckpoint> {
        if self.failed || self.part.is_some() || self.cell.is_some() {
            return Err(invalid("incomplete checkpoint"));
        }
        let header = self.header.ok_or(invalid("missing checkpoint header"))?;
        if self.images.len() != header.root.root().cells() as usize
            || self.pages.len() != self.images.len()
            || self.applier.group.group_manifest()?.as_ref() != Some(&self.base)
        {
            return Err(invalid("incomplete closure or changed receiver base"));
        }
        header
            .membership
            .succeeds(&self.applier.group.membership()?)?;
        let root = {
            let mut storage = self
                .applier
                .storage
                .lock()
                .map_err(|_| invalid("receiver storage"))?;
            let tree = ManifestTree::new(
                header.root.route().clone(),
                ManifestLimits {
                    max_cells: self.limits.max_cells,
                    max_depth: 32,
                },
            )?;
            let mut root = tree.prepare_complete(
                &self.applier.group,
                &mut storage.pages,
                header.root.root(),
                header.root.position(),
                header.root.membership_revision(),
                |leaf| {
                    let key = cell_routing_key(
                        header.root.route().history.namespace,
                        &leaf.version.history.cell,
                    );
                    self.images
                        .get(&key)
                        .filter(|(expected, _)| expected == leaf)
                        .map(|(_, pin)| pin.clone())
                        .ok_or(super::super::ManifestError::Invalid(
                            "missing received cell image",
                        ))
                },
            )?;
            root.base = Some(self.base);
            root
        };
        PreparedCellGroupCheckpoint::new(self.applier, root, header, self.reservation)
    }
}
