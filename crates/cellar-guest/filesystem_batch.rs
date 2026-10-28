// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use crate::filesystem::{integer, invalid, Path};
use anyhow::{ensure, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use cellar::sdk::filesystem::{Edit, Errno, FileVersion, Filesystem, FsPath};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Check {
    path: Path,
    present: bool,
    inode: Option<String>,
    version: Option<String>,
}

pub(crate) struct PreparedCheck {
    path: FsPath,
    present: bool,
    inode: Option<u64>,
    version: Option<u64>,
}

impl Check {
    pub fn prepare(self) -> Result<PreparedCheck> {
        (|| {
            ensure!(
                self.present == (self.inode.is_some() && self.version.is_some()),
                "present file checks require inode and version"
            );
            ensure!(
                self.present || (self.inode.is_none() && self.version.is_none()),
                "absent file checks cannot specify inode/version"
            );
            Ok(PreparedCheck {
                path: self.path.decode()?,
                present: self.present,
                inode: self.inode.as_deref().map(integer).transpose()?,
                version: self.version.as_deref().map(integer).transpose()?,
            })
        })()
        .map_err(invalid)
    }
}

impl PreparedCheck {
    pub fn check(&self, fs: &Filesystem) -> Result<()> {
        match fs.stat(&self.path) {
            Ok(stat)
                if self.present
                    && Some(stat.inode) == self.inode
                    && Some(stat.version) == self.version =>
            {
                Ok(())
            }
            Ok(_) => Err(crate::storage::Conflict.into()),
            Err(error) if error.errno == Errno::ENOENT => {
                if self.present {
                    Err(crate::storage::Conflict.into())
                } else {
                    Ok(())
                }
            }
            Err(error) => Err(error.into()),
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Mutation {
    WriteFile {
        path: Path,
        data_base64: String,
    },
    ReplaceFile {
        path: Path,
        data_base64: String,
    },
    EditFile {
        path: Path,
        inode: String,
        version: String,
        offset: String,
        delete_bytes: String,
        data_base64: String,
    },
    Mkdir {
        path: Path,
        mode: u32,
    },
    Unlink {
        path: Path,
    },
    Rmdir {
        path: Path,
    },
    Rename {
        old: Path,
        new: Path,
    },
    Truncate {
        path: Path,
        size: String,
    },
    Chmod {
        path: Path,
        mode: u32,
    },
}

pub(crate) enum PreparedMutation {
    WriteFile(FsPath, Vec<u8>),
    ReplaceFile(FsPath, Vec<u8>),
    EditFile(FsPath, FileVersion, Edit),
    Mkdir(FsPath, u32),
    Unlink(FsPath),
    Rmdir(FsPath),
    Rename(FsPath, FsPath),
    Truncate(FsPath, u64),
    Chmod(FsPath, u32),
}

impl Mutation {
    pub fn prepare(self, limit: usize) -> Result<PreparedMutation> {
        (|| {
            use PreparedMutation as P;
            Ok(match self {
                Self::WriteFile { path, data_base64 } => {
                    ensure!(
                        data_base64.len() <= limit.div_ceil(3).saturating_mul(4),
                        "filesystem batch payload too large"
                    );
                    let data = STANDARD.decode(data_base64)?;
                    ensure!(data.len() <= limit, "filesystem batch payload too large");
                    P::WriteFile(path.decode()?, data)
                }
                Self::ReplaceFile { path, data_base64 } => {
                    ensure!(
                        data_base64.len() <= limit.div_ceil(3).saturating_mul(4),
                        "filesystem replacement payload too large"
                    );
                    let data = STANDARD.decode(data_base64)?;
                    ensure!(
                        data.len() <= limit,
                        "filesystem replacement payload too large"
                    );
                    P::ReplaceFile(path.decode()?, data)
                }
                Self::EditFile {
                    path,
                    inode,
                    version,
                    offset,
                    delete_bytes,
                    data_base64,
                } => {
                    ensure!(
                        data_base64.len() <= limit.div_ceil(3).saturating_mul(4),
                        "filesystem edit payload too large"
                    );
                    let insert = STANDARD.decode(data_base64)?;
                    ensure!(insert.len() <= limit, "filesystem edit payload too large");
                    P::EditFile(
                        path.decode()?,
                        FileVersion {
                            inode: integer(&inode)?,
                            version: integer(&version)?,
                        },
                        Edit {
                            offset: integer(&offset)?,
                            delete_bytes: integer(&delete_bytes)?,
                            insert,
                        },
                    )
                }
                Self::Mkdir { path, mode } => P::Mkdir(path.decode()?, mode),
                Self::Unlink { path } => P::Unlink(path.decode()?),
                Self::Rmdir { path } => P::Rmdir(path.decode()?),
                Self::Rename { old, new } => P::Rename(old.decode()?, new.decode()?),
                Self::Truncate { path, size } => P::Truncate(path.decode()?, integer(&size)?),
                Self::Chmod { path, mode } => P::Chmod(path.decode()?, mode),
            })
        })()
        .map_err(invalid)
    }
}

impl PreparedMutation {
    pub fn apply(&self, fs: &Filesystem) -> Result<()> {
        match self {
            Self::WriteFile(path, data) => fs.write_file(path, data)?,
            Self::ReplaceFile(path, data) => fs.replace_file(path, data)?,
            Self::EditFile(path, version, edit) => {
                fs.edit_file(path, *version, edit)?;
            }
            Self::Mkdir(path, mode) => fs.mkdir(path, *mode)?,
            Self::Unlink(path) => fs.unlink(path)?,
            Self::Rmdir(path) => fs.rmdir(path)?,
            Self::Rename(old, new) => fs.rename(old, new)?,
            Self::Truncate(path, size) => fs.truncate(path, *size)?,
            Self::Chmod(path, mode) => fs.chmod(path, *mode)?,
        }
        Ok(())
    }
}
