// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Admission and physical ownership of disposable SQLite directories. These
//! files supply no image or recovery authority and are discarded on restart.

use super::*;

const HOUSEKEEPING_BYTES: u64 = 16 * 1024 * 1024;
const CLEANUP_BATCHES: usize = 32;

/// File-byte ceilings enforced by the caller's SQLite VFS and bounded capture.
/// SHM/control files have separate conservative headroom; this is not a kernel
/// filesystem quota or a measurement of allocated physical blocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WorkspaceBounds {
    pub image_bytes: u64,
    pub wal_bytes: u64,
    pub capture_bytes: u64,
}

impl WorkspaceBounds {
    pub fn reservation(self) -> Result<u64, StoreError> {
        if !(512..=stream_snapshot::MAX_IMAGE_BYTES).contains(&self.image_bytes)
            || !self.image_bytes.is_multiple_of(512)
            || !(32..=512 * 1024 * 1024).contains(&self.wal_bytes)
            || !(1..=16 * 1024 * 1024 * 1024).contains(&self.capture_bytes)
        {
            return Err(StoreError::Limit);
        }
        self.image_bytes
            .checked_add(self.wal_bytes)
            .and_then(|bytes| bytes.checked_add(self.capture_bytes))
            .and_then(|bytes| bytes.checked_add(HOUSEKEEPING_BYTES))
            .ok_or(StoreError::Limit)
    }
}

struct Directory {
    inner: Arc<Inner>,
    id: [u8; 32],
    path: PathBuf,
    bounds: WorkspaceBounds,
    charge: u64,
}

struct Cleanup<'a> {
    inner: &'a Inner,
    complete: bool,
}

impl Drop for Cleanup<'_> {
    fn drop(&mut self) {
        if !self.complete {
            self.inner.quarantine();
        }
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let mut cleanup = Cleanup {
            inner: &self.inner,
            complete: false,
        };
        // This is the last physical pin. SQLite/VFS and capture users must
        // retain one until their handles close; no mutex spans filesystem I/O.
        let result = (|| {
            discard(self.inner.filesystem.as_ref(), &self.path)?;
            self.inner.filesystem.sync_all(&self.inner.directory)?;
            let mut state = self.inner.state()?;
            if state.workspaces.get(&self.id) != Some(&self.charge) || state.bytes < self.charge {
                return Err(StoreError::Quarantined);
            }
            state.workspaces.remove(&self.id);
            state.bytes -= self.charge;
            Ok(())
        })();
        if result.is_err() {
            // Keep the complete reservation after partial cleanup or failed
            // directory sync. Reopening under the exclusive store lock owns
            // recovery; another cell cannot speculate that these bytes freed.
            self.inner.quarantine();
        }
        cleanup.complete = result.is_ok();
    }
}

/// A clone retains both the directory's complete reservation and the store's
/// exclusive lock. The last pin removes files, syncs deletion, then refunds.
/// Call only on the owning blocking domain, including dropping the last pin.
#[derive(Clone)]
pub struct NodeWorkspace(Arc<Directory>);

impl NodeWorkspace {
    pub fn directory(&self) -> &Path {
        &self.0.path
    }

    pub fn bounds(&self) -> WorkspaceBounds {
        self.0.bounds
    }

    pub fn filesystem(&self) -> Arc<dyn FileSystem> {
        self.0.inner.filesystem.clone()
    }

    /// Attach to every VFS file owner so closing a cell cannot refund an open
    /// SQLite main/WAL handle. This token grants no mutation or image authority.
    pub fn retention(&self) -> Arc<dyn Send + Sync> {
        self.0.clone()
    }
}

pub(super) fn name(id: &[u8; 32]) -> String {
    format!(
        "w-{}",
        id.iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    )
}

pub(super) fn discard(filesystem: &dyn FileSystem, path: &Path) -> Result<(), StoreError> {
    for _ in 0..CLEANUP_BATCHES {
        if filesystem.remove_dir_batch(path, 256)? {
            return Ok(());
        }
    }
    Err(StoreError::Limit)
}

impl NodeExtentStore {
    /// Reserve the entire working image/WAL/capture bound before creating its
    /// directory. The caller must enforce these ceilings before actual writes.
    /// All directories live inside this exclusively locked node inventory.
    pub fn create_workspace(
        &self,
        id: [u8; 32],
        bounds: WorkspaceBounds,
    ) -> Result<NodeWorkspace, StoreError> {
        let charge = bounds.reservation()?;
        let operation = self.begin()?;
        let path = self.inner.directory.join(name(&id));
        let result = (|| {
            {
                let mut state = self.inner.state()?;
                if state.workspaces.contains_key(&id) {
                    return Err(StoreError::Busy);
                }
                let bytes = state
                    .bytes
                    .checked_add(charge)
                    .filter(|bytes| *bytes <= self.inner.limits.max_bytes)
                    .ok_or(StoreError::Limit)?;
                if state.workspaces.len() >= self.inner.limits.max_workspaces {
                    return Err(StoreError::Limit);
                }
                state.bytes = bytes;
                state.workspaces.insert(id, charge);
            }
            let created = (|| {
                match self.inner.filesystem.symlink_metadata(&path) {
                    Err(error) if error.kind() == io::ErrorKind::NotFound => (),
                    Err(error) => return Err(StoreError::Io(error)),
                    Ok(_) => return Err(StoreError::Layout),
                }
                crate::host::create_dir_all_durable(self.inner.filesystem.as_ref(), &path)?;
                Ok(())
            })();
            if let Err(error) = created {
                self.inner.quarantine();
                return Err(error);
            }
            Ok(NodeWorkspace(Arc::new(Directory {
                inner: self.inner.clone(),
                id,
                path: path.clone(),
                bounds,
                charge,
            })))
        })();
        operation.finish()?;
        result
    }

    pub fn owns_workspace(&self, workspace: &NodeWorkspace) -> bool {
        Arc::ptr_eq(&self.inner, &workspace.0.inner)
    }
}
