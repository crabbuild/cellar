// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One durable extent inventory per node. Cell/root handles share this owner;
//! closing them never refunds files that remain on disk. Root selection and
//! native acknowledgement authority belong to the host, not this inventory.

mod image;
mod inventory;
mod publication;
mod reclaim;
mod workspace;

pub use image::{ImageRootUpdate, PreparedImageRoot, RootApplyWork, SealedImageRoot};
pub use workspace::{NodeWorkspace, WorkspaceBounds};

use super::{
    file::ExtentDownload,
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
    sync::{Arc, Mutex, MutexGuard},
};

#[derive(Clone, Copy, Debug)]
pub struct NodeExtentLimits {
    pub max_bytes: u64,
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
    verified: bool,
}

struct RootData {
    manifest: ExtentManifest,
    objects: BTreeSet<ExtentId>,
}

struct RootEntry {
    bytes: u64,
    slots: usize,
    retained: bool,
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
    busy: bool,
    quarantined: bool,
    references_reconciled: bool,
}

struct Inner {
    directory: PathBuf,
    filesystem: Arc<dyn FileSystem>,
    limits: NodeExtentLimits,
    state: Mutex<State>,
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

impl NodeExtentStore {
    /// Compare exact open inventory owners without granting access or doing I/O.
    pub fn same_inventory(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

struct Operation {
    inner: Arc<Inner>,
    complete: bool,
}

impl Operation {
    fn finish(mut self) -> Result<(), StoreError> {
        self.inner.state()?.busy = false;
        self.complete = true;
        Ok(())
    }
}

impl Drop for Operation {
    fn drop(&mut self) {
        if !self.complete {
            let mut state = self
                .inner
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.quarantined = true;
            state.busy = false;
        }
    }
}

impl NodeExtentStore {
    fn begin(&self) -> Result<Operation, StoreError> {
        self.start(false)
    }

    fn begin_read(&self) -> Result<Operation, StoreError> {
        self.start(true)
    }

    fn start(&self, allow_quarantine: bool) -> Result<Operation, StoreError> {
        let mut state = self.inner.state()?;
        if state.quarantined && !allow_quarantine {
            return Err(StoreError::Quarantined);
        }
        if state.busy {
            return Err(StoreError::Busy);
        }
        state.busy = true;
        Ok(Operation {
            inner: self.inner.clone(),
            complete: false,
        })
    }

    pub fn usage(&self) -> Result<NodeExtentUsage, StoreError> {
        let state = self.inner.state()?;
        Ok(NodeExtentUsage {
            bytes: state.bytes,
            objects: state.objects.len(),
            roots: state.roots.len(),
            root_extents: state.root_extents,
            workspace_bytes: state.workspaces.values().sum(),
            workspaces: state.workspaces.len(),
            busy: state.busy,
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
        Ok(ExtentImage::new(
            self.data.manifest.clone(),
            Arc::new(self.clone()),
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
