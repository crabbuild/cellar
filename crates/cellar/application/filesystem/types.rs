// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::{Errno, Error, FilesystemLimits, Result};

pub(crate) fn validate_mode(mode: u32) -> Result<()> {
    if mode & !0o777 != 0 {
        return Err(Error::new(Errno::EOPNOTSUPP, "unsupported mode bits"));
    }
    Ok(())
}

/// Immutable logical identity policy. All calls in this capability use the
/// inode owner identity; there is no ambient host-UID or superuser bypass.
#[derive(Clone, Copy, Debug)]
pub struct Policy {
    pub creation_mask: u32,
    /// Deny application filesystem mutations without restricting local KV/SQL.
    pub read_only: bool,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            creation_mask: 0o022,
            read_only: false,
        }
    }
}

impl Policy {
    pub fn validate(&self) -> Result<()> {
        if self.creation_mask & !0o777 != 0 {
            return Err(Error::new(Errno::EINVAL, "unsupported creation mask bits"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub enum Kind {
    File,
    Directory,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stat {
    pub inode: u64,
    pub kind: Kind,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub size: u64,
    pub nlink: u64,
    /// Untracked in the initial noatime profile; always zero.
    pub atime_ns: i64,
    pub mtime_ns: i64,
    pub ctime_ns: i64,
    pub version: u64,
    /// Retained bytes, including this inode's pending truncate garbage.
    pub allocated_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirEntry {
    pub name: Vec<u8>,
    pub inode: u64,
    pub kind: Kind,
}

/// An expected path identity plus its content revision; replacement cannot
/// accidentally satisfy a check against a different inode at the same path.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileVersion {
    pub inode: u64,
    pub version: u64,
}

impl From<&Stat> for FileVersion {
    fn from(stat: &Stat) -> Self {
        Self {
            inode: stat.inode,
            version: stat.version,
        }
    }
}

/// Bounded byte-range edit. In-place patches and tail replacements touch only
/// bounded inserted data; shifting a suffix uses the whole-file helper bound.
#[derive(Clone, Debug)]
pub struct Edit {
    pub offset: u64,
    pub delete_bytes: u64,
    pub insert: Vec<u8>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadDirCursor {
    pub(crate) scope: String,
    #[serde(with = "decimal")]
    pub(crate) inode: u64,
    #[serde(with = "decimal")]
    pub(crate) position: u64,
}

mod decimal {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        value: &u64,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(value)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<u64, D::Error> {
        let value = String::deserialize(deserializer)?;
        if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(serde::de::Error::custom("invalid directory cursor integer"));
        }
        value.parse().map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Debug)]
pub struct DirectoryPage {
    pub entries: Vec<DirEntry>,
    pub next: Option<ReadDirCursor>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatVfs {
    pub logical_bytes: u64,
    pub allocated_bytes: u64,
    pub inodes: u64,
    pub available_logical_bytes: u64,
    pub available_allocated_bytes: u64,
    pub available_inodes: u64,
    pub chunk_bytes: usize,
}

#[derive(Clone, Copy, Debug)]
pub enum Seek {
    Start(u64),
    Current(i64),
    End(i64),
}

#[derive(Clone, Copy, Debug)]
pub enum Time {
    Value(i64),
    Now,
    Omit,
}

#[derive(Clone, Copy, Debug)]
pub struct Times {
    pub atime: Time,
    pub mtime: Time,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum UnlinkFlags {
    #[default]
    File,
    RemoveDirectory,
}

#[derive(Clone, Copy, Debug, Default)]
pub enum StatFlags {
    #[default]
    Follow,
    NoFollow,
}

/// Typed options avoid importing platform-specific O_* flag values.
#[derive(Clone, Copy, Debug)]
pub struct OpenOptions {
    pub(crate) readable: bool,
    pub(crate) writable: bool,
    pub(crate) create: bool,
    pub(crate) exclusive: bool,
    pub(crate) truncate: bool,
    pub(crate) append: bool,
    pub(crate) mode: u32,
}

impl OpenOptions {
    pub fn read_only() -> Self {
        Self {
            readable: true,
            writable: false,
            create: false,
            exclusive: false,
            truncate: false,
            append: false,
            mode: 0o666,
        }
    }

    pub fn write_only() -> Self {
        Self {
            readable: false,
            writable: true,
            ..Self::read_only()
        }
    }

    pub fn read_write() -> Self {
        Self {
            writable: true,
            ..Self::read_only()
        }
    }

    pub fn create(mut self, value: bool) -> Self {
        self.create = value;
        self
    }

    pub fn create_new(mut self, value: bool) -> Self {
        self.exclusive = value;
        self.create |= value;
        self
    }

    pub fn truncate(mut self, value: bool) -> Self {
        self.truncate = value;
        self
    }

    pub fn append(mut self, value: bool) -> Self {
        self.append = value;
        self
    }

    pub fn mode(mut self, mode: u32) -> Self {
        self.mode = mode;
        self
    }

    pub(crate) fn validate(self) -> Result<()> {
        if self.exclusive && !self.create {
            return Err(Error::new(
                Errno::EINVAL,
                "exclusive creation requires create",
            ));
        }
        if self.mode & !0o777 != 0 {
            return Err(Error::new(Errno::EOPNOTSUPP, "unsupported file mode bits"));
        }
        if (self.truncate || self.append) && !self.writable {
            return Err(Error::new(
                Errno::EINVAL,
                "truncate/append require write access",
            ));
        }
        Ok(())
    }
}

/// A raw byte path. Relative paths start at the cell root for root-based calls.
#[derive(Clone, Debug)]
pub struct FsPath(Vec<u8>);

impl FsPath {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8]> for FsPath {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// One byte-preserving, non-reserved directory-entry name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Name(Vec<u8>);

impl Name {
    pub fn new(value: impl AsRef<[u8]>) -> Result<Self> {
        let bytes = value.as_ref();
        if bytes.is_empty()
            || bytes.contains(&0)
            || bytes.contains(&b'/')
            || matches!(bytes, b"." | b"..")
        {
            return Err(Error::new(Errno::EINVAL, "invalid directory-entry name"));
        }
        if bytes.len() > 255 {
            return Err(Error::new(
                Errno::ENAMETOOLONG,
                "directory-entry name exceeds 255 bytes",
            ));
        }
        Ok(Self(bytes.to_vec()))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8]> for Name {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<&str> for FsPath {
    fn from(value: &str) -> Self {
        Self(value.as_bytes().to_vec())
    }
}

impl From<String> for FsPath {
    fn from(value: String) -> Self {
        Self(value.into_bytes())
    }
}

impl From<&[u8]> for FsPath {
    fn from(value: &[u8]) -> Self {
        Self(value.to_vec())
    }
}

impl From<Vec<u8>> for FsPath {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl<const N: usize> From<&[u8; N]> for FsPath {
    fn from(value: &[u8; N]) -> Self {
        Self(value.to_vec())
    }
}

impl From<&FsPath> for FsPath {
    fn from(value: &FsPath) -> Self {
        value.clone()
    }
}

pub(crate) struct Path {
    pub absolute: bool,
    pub trailing_slash: bool,
    pub components: Vec<Vec<u8>>,
}

impl Path {
    pub fn parse(path: impl AsRef<[u8]>, limits: FilesystemLimits) -> Result<Self> {
        let bytes = path.as_ref();
        if bytes.is_empty() {
            return Err(Error::new(Errno::ENOENT, "empty path"));
        }
        if bytes.contains(&0) {
            return Err(Error::new(Errno::EINVAL, "NUL in filesystem path"));
        }
        if bytes.len() > limits.max_path_bytes {
            return Err(Error::new(Errno::ENAMETOOLONG, "path limit exceeded"));
        }
        let components: Vec<_> = bytes
            .split(|byte| *byte == b'/')
            .filter(|part| !part.is_empty())
            .map(<[u8]>::to_vec)
            .collect();
        if components.len() > limits.max_path_depth
            || components.iter().any(|part| part.len() > 255)
        {
            return Err(Error::new(
                Errno::ENAMETOOLONG,
                "path component/depth limit exceeded",
            ));
        }
        Ok(Self {
            absolute: bytes[0] == b'/',
            trailing_slash: bytes.last() == Some(&b'/'),
            components,
        })
    }
}

/// Advertised independently of any operating system adapter.
#[derive(Clone, Copy, Debug, serde::Serialize)]
pub struct Capabilities {
    pub logical_uid: u32,
    pub logical_gid: u32,
    pub creation_mask: u32,
    pub read_only: bool,
    pub regular_files: bool,
    pub directories: bool,
    pub positional_io: bool,
    pub directory_relative: bool,
    pub transactions: bool,
    pub symlinks: bool,
    pub hard_links: bool,
    pub advisory_locks: bool,
    pub atime: bool,
    pub limits: FilesystemLimits,
}
