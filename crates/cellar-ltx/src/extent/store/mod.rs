// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One durable extent inventory per node. Cell/root handles share this owner;
//! closing them never refunds files that remain on disk. Root selection and
//! native acknowledgement authority belong to the host, not this inventory.

mod admission;
mod barrier;
mod batch;
mod image;
mod inventory;
mod publication;
mod read;
mod reclaim;
mod segment;
pub mod timing;
mod workspace;

use admission::{Key, KeyGuard};

pub use image::{ImageRootUpdate, PreparedImageRoot, RootApplyWork, SealedImageRoot};
pub use workspace::{NodeWorkspace, WorkspaceBounds};

use super::{
    file::{ExtentDownload, VerifiedBlocks},
    image::{CacheLimits, ExtentImage, ExtentSource},
    tree::{ExtentId, ExtentManifest, ImageRoot, RootError, MAX_MANIFEST_BYTES},
};
use crate::{
    host::{FileSystem, HostFile, HostFileLock},
    stream_snapshot::{self, ScratchPool},
};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex, MutexGuard, Weak},
};

#[derive(Clone, Copy, Debug)]
pub struct NodeExtentLimits {
    pub max_bytes: u64,
    /// Also bounds retained read proofs: at most 2 KiB of block hashes per
    /// object, shared with readers. Reader root pins prevent object reclamation.
    pub max_objects: usize,
    pub max_roots: usize,
    /// Total extent slots described by retained root files, not SQLite pages.
    pub max_root_extents: usize,
    pub max_references: usize,
    /// Live or quarantined disposable SQLite directories sharing this budget.
    pub max_workspaces: usize,
}

impl Default for NodeExtentLimits {
    fn default() -> Self {
        Self {
            max_bytes: 64 * 1024 * 1024 * 1024,
            max_objects: 16384,
            max_roots: 4096,
            max_root_extents: 262144,
            max_references: 16384,
            max_workspaces: 256,
        }
    }
}

impl NodeExtentLimits {
    pub fn validate(self) -> Result<(), StoreError> {
        if self.max_bytes == 0
            || self.max_objects == 0
            || self.max_roots == 0
            || self.max_root_extents == 0
            || self.max_references == 0
            || !(1..=1024).contains(&self.max_workspaces)
        {
            return Err(StoreError::Limit);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("node extent capacity is exhausted or invalid")]
    Limit,
    #[error("unrecognized node extent inventory")]
    Layout,
    #[error("extent/root is missing")]
    Missing,
    #[error("extent closure is not independently verified locally")]
    Unverified,
    #[error("complete authoritative root references have not been reconstructed")]
    Unreconciled,
    #[error("node extent mutation is already in progress")]
    Busy,
    #[error("node extent inventory is quarantined; close and recover it")]
    Quarantined,
    #[error("extent/root content differs from its identity")]
    Integrity,
    #[error("extent metadata: {0}")]
    Root(#[from] RootError),
    #[error("extent image apply: {0}")]
    Apply(#[from] crate::apply::ApplyError),
    #[error("extent bytes: {0}")]
    Extent(#[from] stream_snapshot::Error),
    #[error("extent inventory I/O: {0}")]
    Io(#[from] io::Error),
    #[error("extent operation failed ({operation}); cleanup also failed: {cleanup}")]
    Cleanup {
        operation: Box<StoreError>,
        cleanup: io::Error,
    },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NodeExtentUsage {
    pub bytes: u64,
    pub objects: usize,
    pub roots: usize,
    pub root_extents: usize,
    pub workspace_bytes: u64,
    pub workspaces: usize,
    pub busy: bool,
    pub quarantined: bool,
}

/// Exact metadata reference supplied by an authenticated native marker or
/// archive. It supplies no permission to publish, vote or serve a cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExtentRootReference {
    root: ImageRoot,
    length: u32,
}

impl ExtentRootReference {
    pub fn new(root: ImageRoot, length: u32) -> Result<Self, StoreError> {
        if !(112..=MAX_MANIFEST_BYTES as u32).contains(&length)
            || !(length - 112).is_multiple_of(32)
        {
            return Err(StoreError::Layout);
        }
        Ok(Self { root, length })
    }

    pub fn root(self) -> ImageRoot {
        self.root
    }

    pub fn encoded_length(self) -> u32 {
        self.length
    }
}

struct ObjectEntry {
    bytes: u64,
    location: Option<segment::Location>,
    publishing: bool,
    verified: bool,
    read_proof: Option<Arc<VerifiedBlocks>>,
}

struct RootData {
    manifest: ExtentManifest,
    objects: BTreeSet<ExtentId>,
}

struct RootEntry {
    bytes: u64,
    location: Option<segment::Location>,
    slots: usize,
    retained: bool,
    // This open inventory completed both immutable metadata barriers. Reopen
    // and any failed metadata read revoke the proof; cached RootData does not
    // establish it. Every subsequent seal still authenticates the file bytes.
    metadata_persisted: bool,
    token: Arc<()>,
    data: Option<Arc<RootData>>,
}

struct ReferenceEntry {
    revision: u64,
    reference: ExtentRootReference,
    active: bool,
}

#[derive(Default)]
struct State {
    objects: BTreeMap<ExtentId, ObjectEntry>,
    roots: BTreeMap<ImageRoot, RootEntry>,
    references: BTreeMap<[u8; 32], ReferenceEntry>,
    workspaces: BTreeMap<[u8; 32], u64>,
    bytes: u64,
    root_extents: usize,
    active: usize,
    exclusive: bool,
    exclusive_waiters: usize,
    keys: BTreeSet<Key>,
    quarantined: bool,
    references_reconciled: bool,
}

struct Inner {
    directory: PathBuf,
    filesystem: Arc<dyn FileSystem>,
    limits: NodeExtentLimits,
    state: Mutex<State>,
    idle: Condvar,
    serialized: bool,
    publications: usize,
    batch_directory_barriers: bool,
    barrier: barrier::DirectoryBarrier,
    segments: Option<segment::Segments>,
    _lock: HostFileLock,
}

impl Inner {
    fn state(&self) -> Result<MutexGuard<'_, State>, StoreError> {
        self.state.lock().map_err(|_| StoreError::Quarantined)
    }

    fn quarantine(&self) {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .quarantined = true;
        self.idle.notify_all();
    }

    fn checked_file(&self, path: &Path, length: u64) -> Result<HostFile, StoreError> {
        let metadata = self.filesystem.symlink_metadata(path)?;
        if !metadata.is_file || metadata.len != length {
            return Err(StoreError::Integrity);
        }
        let mut file = self.filesystem.open(path)?;
        if file.file_len()? != length {
            return Err(StoreError::Integrity);
        }
        Ok(file)
    }
}

/// One owner must be shared by all groups using this node directory/budget.
/// Its complete inventory survives cell/view closure and is rediscovered under
/// an exclusive directory lock at restart. No newest-file selection is used.
#[derive(Clone)]
pub struct NodeExtentStore {
    inner: Arc<Inner>,
}

/// Counter metadata only. Keeping an observer cannot retain the node's files
/// or exclusive inventory lock after all physical owners have exited.
#[derive(Clone)]
pub struct NodeExtentObserver(Weak<Inner>);

impl NodeExtentObserver {
    pub fn capture(&self) -> Option<(NodeExtentUsage, NodeExtentLimits)> {
        let store = NodeExtentStore {
            inner: self.0.upgrade()?,
        };
        Some((store.usage().ok()?, store.inner.limits))
    }
}

impl NodeExtentStore {
    pub fn observer(&self) -> NodeExtentObserver {
        NodeExtentObserver(Arc::downgrade(&self.inner))
    }

    /// Compare exact open inventory owners without granting access or doing I/O.
    pub fn same_inventory(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    /// Check the physical owner of a complete image seal without doing I/O.
    /// A matching root identity in another inventory is not a local seal.
    pub fn owns_image(&self, image: &SealedImageRoot) -> bool {
        Arc::ptr_eq(&self.inner, &image.view().inner)
    }
}

impl NodeExtentStore {
    pub fn usage(&self) -> Result<NodeExtentUsage, StoreError> {
        let state = self.inner.state()?;
        Ok(NodeExtentUsage {
            bytes: state.bytes,
            objects: state.objects.len(),
            roots: state.roots.len(),
            root_extents: state.root_extents,
            workspace_bytes: state.workspaces.values().sum(),
            workspaces: state.workspaces.len(),
            busy: state.active != 0,
            quarantined: state.quarantined,
        })
    }

    fn view(
        &self,
        reference: ExtentRootReference,
        data: Arc<RootData>,
    ) -> Result<RootView, StoreError> {
        let state = self.inner.state()?;
        let root = state
            .roots
            .get(&reference.root)
            .ok_or(StoreError::Missing)?;
        Ok(RootView {
            inner: self.inner.clone(),
            reference,
            data,
            _token: root.token.clone(),
        })
    }
}

/// Authenticated metadata and a pinned local byte source. Existing objects are
/// verified on demand after restart. This is deliberately not a local closure
/// seal; missing data fails the read and cannot become zero-filled SQLite data.
#[derive(Clone)]
pub struct RootView {
    inner: Arc<Inner>,
    reference: ExtentRootReference,
    data: Arc<RootData>,
    _token: Arc<()>,
}

impl RootView {
    pub fn reference(&self) -> ExtentRootReference {
        self.reference
    }

    pub fn manifest(&self) -> &ExtentManifest {
        &self.data.manifest
    }

    /// Read one complete dependency on the caller's blocking owner. The caller
    /// admits its at-most-4-MiB output before calling. No bytes escape until
    /// their full extent identity has matched; inventory membership alone is
    /// insufficient, including after this view was sealed or reopened.
    pub fn read_extent(&self, id: ExtentId) -> Result<Vec<u8>, StoreError> {
        if !self.data.objects.contains(&id) {
            return Err(StoreError::Missing);
        }
        let mut bytes = Vec::with_capacity(id.length() as usize);
        publication::read_object(&self.inner, id, |offset, chunk| {
            if offset != bytes.len() as u64
                || bytes
                    .len()
                    .checked_add(chunk.len())
                    .is_none_or(|end| end > id.length() as usize)
            {
                return Err(StoreError::Integrity);
            }
            bytes.extend_from_slice(chunk);
            Ok(())
        })?;
        Ok(bytes)
    }

    /// The caller owns this separate cache directory and scratch budget. Keep
    /// cache directories outside the durable inventory's flat namespace.
    pub fn image(
        &self,
        cache_directory: PathBuf,
        scratch: ScratchPool,
        limits: CacheLimits,
    ) -> Result<ExtentImage, StoreError> {
        Ok(ExtentImage::from_local(
            self.clone(),
            self.inner.filesystem.clone(),
            cache_directory,
            scratch,
            limits,
        )?)
    }
}

/// Returned only after the entire local object closure was verified and synced
/// and its exact metadata file/directory barriers completed. Native authority
/// and durable marker publication remain separate host operations.
pub struct SealedRoot(RootView);

impl SealedRoot {
    pub fn view(&self) -> &RootView {
        &self.0
    }
}

impl ExtentSource for RootView {
    fn read_into(
        &self,
        id: ExtentId,
        download: &mut ExtentDownload,
    ) -> Result<(), stream_snapshot::Error> {
        if !self.data.objects.contains(&id) {
            return Err(stream_snapshot::Error::Invalid(
                "extent outside pinned root",
            ));
        }
        let result = publication::read_object(&self.inner, id, |offset, bytes| {
            download.append(offset, bytes).map_err(StoreError::from)
        });
        result.map_err(|error| stream_snapshot::Error::Io(io::Error::other(error)))
    }
}
