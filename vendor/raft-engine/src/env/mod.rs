// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::io::{Read, Result, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

mod default;
mod log_fd;
mod obfuscated;

pub use default::DefaultFileSystem;
pub use obfuscated::ObfuscatedFileSystem;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Permission {
    ReadOnly,
    ReadWrite,
}

/// Host-owned exclusive directory lock, retained for the engine lifetime.
pub type DirectoryLock = Box<dyn Send + Sync>;

pub struct DirectoryMetadata {
    pub is_dir: bool,
    pub is_file: bool,
}

pub struct DirectoryEntry {
    pub path: PathBuf,
    pub is_file: bool,
}

/// FileSystem
pub trait FileSystem: Send + Sync {
    type Handle: Send + Sync + Handle;
    type Reader: Seek + Read + Send;
    type Writer: Seek + Write + Send + WriteExt;

    // Cellar patch: directory operations share the same injectable boundary as
    // log-file I/O. Defaults preserve the upstream direct-filesystem backend.
    fn directory_metadata(&self, path: &Path) -> Result<Option<DirectoryMetadata>> {
        match std::fs::metadata(path) {
            Ok(metadata) => Ok(Some(DirectoryMetadata {
                is_dir: metadata.is_dir(),
                is_file: metadata.is_file(),
            })),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn create_directory(&self, path: &Path) -> Result<()> {
        std::fs::create_dir(path)
    }

    fn read_directory(&self, path: &Path) -> Result<Vec<DirectoryEntry>> {
        std::fs::read_dir(path)?.map(|entry| {
            let entry = entry?;
            Ok(DirectoryEntry { path: entry.path(), is_file: entry.file_type()?.is_file() })
        }).collect()
    }

    fn lock_directory(&self, path: &Path) -> Result<DirectoryLock> {
        let file = std::fs::OpenOptions::new().read(true).write(true)
            .create(true).truncate(false).open(path.join("LOCK"))?;
        fs2::FileExt::try_lock_exclusive(&file)?;
        Ok(Box::new(file))
    }

    fn sync_directory(&self, path: &Path) -> Result<()> {
        std::fs::File::open(path)?.sync_all()
    }

    fn create<P: AsRef<Path>>(&self, path: P) -> Result<Self::Handle>;

    fn open<P: AsRef<Path>>(&self, path: P, perm: Permission) -> Result<Self::Handle>;

    fn delete<P: AsRef<Path>>(&self, path: P) -> Result<()>;

    fn rename<P: AsRef<Path>>(&self, src_path: P, dst_path: P) -> Result<()>;

    /// Reuses file at `src_path` as a new file at `dst_path`. The default
    /// implementation simply renames the file.
    fn reuse<P: AsRef<Path>>(&self, src_path: P, dst_path: P) -> Result<()> {
        self.rename(src_path, dst_path)
    }

    #[inline]
    fn reuse_and_open<P: AsRef<Path>>(&self, src_path: P, dst_path: P) -> Result<Self::Handle> {
        self.reuse(src_path.as_ref(), dst_path.as_ref())?;
        self.open(dst_path, Permission::ReadWrite)
    }

    /// Deletes user implemented metadata associated with `path`. Returns
    /// `true` if any metadata is deleted.
    ///
    /// In older versions of Raft Engine, physical files are deleted without
    /// going through user implemented cleanup procedure. This method is used to
    /// detect and cleanup the user metadata that is no longer mapped to a
    /// physical file.
    fn delete_metadata<P: AsRef<Path>>(&self, _path: P) -> Result<()> {
        Ok(())
    }

    /// Returns whether there is any user metadata associated with given `path`.
    fn exists_metadata<P: AsRef<Path>>(&self, _path: P) -> bool {
        false
    }

    fn new_reader(&self, handle: Arc<Self::Handle>) -> Result<Self::Reader>;

    fn new_writer(&self, handle: Arc<Self::Handle>) -> Result<Self::Writer>;
}

pub trait Handle {
    fn truncate(&self, offset: usize) -> Result<()>;

    /// Returns the current size of this file.
    fn file_size(&self) -> Result<usize>;

    fn sync(&self) -> Result<()>;
}

/// WriteExt is writer extension api
pub trait WriteExt {
    fn truncate(&mut self, offset: usize) -> Result<()>;
    fn allocate(&mut self, offset: usize, size: usize) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::Permission;

    #[test]
    fn test_copy_permission() {
        let perm = Permission::ReadWrite;
        let perm1 = perm;
        assert_eq!(perm, Permission::ReadWrite);
        assert_eq!(perm1, Permission::ReadWrite);
    }
}
