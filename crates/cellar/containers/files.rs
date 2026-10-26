// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded container filesystem operations using engine archives, never a shell
//! or host extraction. Paths designate the container's own filesystem.
use super::{CellContainer, ContainerEngine};
use anyhow::{ensure, Context as _, Result};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    io::{Cursor, Read},
    sync::Arc,
};

const OVERHEAD: usize = 16384;
pub const MAX_FILE_ENTRIES: usize = 4096;

/// A canonical, absolute UTF-8 path inside the selected container. This is not
/// a host path. Relative paths, dot segments, NULs and ambiguous separators refuse.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilePath(String);

impl FilePath {
    pub fn new(path: impl Into<String>) -> Result<Self> {
        let path = path.into();
        ensure!(
            path.starts_with('/') && path.len() <= 4096 && !path.contains('\0'),
            "invalid container file path"
        );
        ensure!(
            path == "/"
                || path[1..]
                    .split('/')
                    .all(|part| !matches!(part, "" | "." | "..")),
            "container paths must be canonical and absolute"
        );
        Ok(Self(path))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn destination(&self) -> Result<(&str, &str)> {
        let (parent, name) = self
            .0
            .rsplit_once('/')
            .context("invalid container file path")?;
        ensure!(
            !name.is_empty(),
            "cannot write the container root as a file"
        );
        Ok((if parent.is_empty() { "/" } else { parent }, name))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FileKind {
    File,
    Directory,
    Symlink,
    Other,
}

/// Portable metadata. Size is bytes; mode contains permission bits only.
/// Unknown modification times and link targets are represented explicitly.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileMetadata {
    pub name: String,
    pub kind: FileKind,
    pub size: u64,
    pub mode: u32,
    pub modified_ms: Option<i64>,
    pub link_target: Option<String>,
}

#[cfg(test)]
#[path = "files_tests.rs"]
mod tests;

fn route(name: &str, path: &str, write: bool) -> String {
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    query.append_pair("path", path);
    if write {
        query.append_pair("noOverwriteDirNonDir", "true");
    }
    format!("/containers/{name}/archive?{}", query.finish())
}

fn archive_limit(maximum: usize) -> Result<usize> {
    maximum
        .checked_add(OVERHEAD)
        .context("container archive byte budget overflow")
}

fn validate_archive(bytes: &[u8]) -> Result<()> {
    // Validate raw on-disk sizes before tar's GNU/PAX handling can buffer an
    // extension. No declared entry, including an extension, may exceed the
    // already bounded archive; sparse reconstruction is unsupported.
    let mut archive = tar::Archive::new(Cursor::new(bytes));
    for entry in archive.entries()?.raw(true) {
        let entry = entry?;
        ensure!(
            entry
                .raw_file_position()
                .checked_add(entry.size())
                .is_some_and(|end| end <= bytes.len() as u64),
            "truncated or excessive container archive entry"
        );
        ensure!(
            !entry.header().entry_type().is_gnu_sparse(),
            "sparse container archives are unsupported"
        );
    }
    Ok(())
}

fn read_archive(bytes: &[u8], maximum: usize) -> Result<Vec<u8>> {
    validate_archive(bytes)?;
    let mut archive = tar::Archive::new(Cursor::new(bytes));
    let mut entries = archive.entries()?;
    let mut entry = entries.next().context("empty container file archive")??;
    ensure!(
        entry.header().entry_type().is_file(),
        "container read requires a regular file"
    );
    let size = usize::try_from(entry.size())?;
    ensure!(size <= maximum, "container file exceeds byte budget");
    let path = entry.path()?;
    ensure!(
        path.components().count() == 1 && path.file_name().is_some(),
        "invalid container file archive path"
    );
    let mut output = Vec::with_capacity(size);
    entry.read_to_end(&mut output)?;
    ensure!(output.len() == size, "truncated container file");
    drop(entry);
    ensure!(
        entries.next().is_none(),
        "container read returned multiple entries"
    );
    Ok(output)
}

fn write_archive(path: &FilePath, bytes: &[u8], mode: u32) -> Result<Vec<u8>> {
    ensure!(mode <= 0o777, "invalid container file permissions");
    let (_, name) = path.destination()?;
    let mut header = tar::Header::new_gnu();
    header.set_size(bytes.len() as u64);
    header.set_mode(mode);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_entry_type(tar::EntryType::Regular);
    header.set_cksum();
    let mut builder = tar::Builder::new(Vec::new());
    builder.append_data(&mut header, name, bytes)?;
    let output = builder.into_inner()?;
    ensure!(
        output.len() <= archive_limit(bytes.len())?,
        "container archive overhead exceeds limit"
    );
    Ok(output)
}

fn list_archive(bytes: &[u8], maximum: usize, max_entries: usize) -> Result<Vec<FileMetadata>> {
    validate_archive(bytes)?;
    let mut archive = tar::Archive::new(Cursor::new(bytes));
    let mut output = Vec::new();
    for entry in archive.entries()? {
        let entry = entry?;
        let path = entry.path()?;
        let path = path.to_str().context("container filenames must be UTF-8")?;
        let path = path
            .strip_prefix("./")
            .unwrap_or(path)
            .trim_end_matches('/');
        ensure!(
            !path.starts_with('/') && !path.split('/').any(|part| part == ".."),
            "invalid container listing path"
        );
        if path.is_empty() || path == "." || path.contains('/') {
            continue;
        }
        ensure!(
            output.len() < max_entries,
            "container listing exceeds entry budget"
        );
        let header = entry.header();
        let kind = header.entry_type();
        let kind = if kind.is_file() {
            FileKind::File
        } else if kind.is_dir() {
            FileKind::Directory
        } else if kind.is_symlink() {
            FileKind::Symlink
        } else {
            FileKind::Other
        };
        let modified_ms = Some(
            i64::try_from(header.mtime()?)?
                .checked_mul(1000)
                .context("container modification time exceeds integer range")?,
        );
        let link_target = entry
            .link_name()?
            .map(|name| {
                name.to_str()
                    .map(str::to_owned)
                    .context("container link names must be UTF-8")
            })
            .transpose()?;
        output.push(FileMetadata {
            name: path.into(),
            kind,
            size: entry.size(),
            mode: header.mode()? & 0o777,
            modified_ms,
            link_target,
        });
    }
    output.sort_by(|a, b| a.name.cmp(&b.name));
    ensure!(
        output.windows(2).all(|pair| pair[0].name != pair[1].name),
        "duplicate container listing name"
    );
    ensure!(
        serde_json::to_vec(&output)?.len() <= maximum,
        "container listing metadata exceeds byte budget"
    );
    Ok(output)
}

fn stat_header(encoded: &str) -> Result<FileMetadata> {
    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Stat {
        name: String,
        size: u64,
        mode: u32,
        mtime: Option<String>,
        link_target: Option<String>,
    }
    let stat: Stat = serde_json::from_slice(&STANDARD.decode(encoded)?)?;
    ensure!(
        stat.name.len() <= 4096
            && stat
                .link_target
                .as_ref()
                .is_none_or(|value| value.len() <= 4096),
        "container metadata exceeds path budget"
    );
    let kind = if stat.mode & (1 << 31) != 0 {
        FileKind::Directory
    } else if stat.mode & (1 << 27) != 0 {
        FileKind::Symlink
    } else if stat.mode
        & ((1 << 31) | (1 << 27) | (1 << 26) | (1 << 25) | (1 << 24) | (1 << 21) | (1 << 19))
        == 0
    {
        FileKind::File
    } else {
        FileKind::Other
    };
    let modified_ms = stat
        .mtime
        .map(|time| {
            chrono::DateTime::parse_from_rfc3339(&time).map(|value| value.timestamp_millis())
        })
        .transpose()?;
    Ok(FileMetadata {
        name: stat.name,
        kind,
        size: stat.size,
        mode: stat.mode & 0o777,
        modified_ms,
        link_target: stat.link_target.filter(|value| !value.is_empty()),
    })
}

impl ContainerEngine {
    fn file_run(&self, cell: &Arc<CellContainer>, run: u64) -> Result<()> {
        self.check_cell(cell)?;
        ensure!(
            cell.running() && cell.current_run() == run,
            "container file operation belongs to a stopped or replaced run"
        );
        Ok(())
    }

    pub async fn read_file(
        &self,
        cell: &Arc<CellContainer>,
        path: FilePath,
        maximum: usize,
    ) -> Result<Vec<u8>> {
        let run = cell.current_run();
        self.file_run(cell, run)?;
        let (reply, _) = self
            .docker
            .archive(
                "GET",
                &route(&cell.name, path.as_str(), false),
                Bytes::new(),
                archive_limit(maximum)?,
            )
            .await?;
        self.file_run(cell, run)?;
        ensure!(
            reply.status.is_success(),
            "read container file: {}",
            reply.message()
        );
        read_archive(&reply.body, maximum)
    }

    pub async fn write_file(
        &self,
        cell: &Arc<CellContainer>,
        path: FilePath,
        data: Vec<u8>,
        mode: u32,
    ) -> Result<()> {
        let (parent, _) = path.destination()?;
        let body = write_archive(&path, &data, mode)?;
        let run = cell.current_run();
        self.file_run(cell, run)?;
        let (reply, _) = self
            .docker
            .archive(
                "PUT",
                &route(&cell.name, parent, true),
                Bytes::from(body),
                OVERHEAD,
            )
            .await?;
        self.file_run(cell, run)?;
        ensure!(
            reply.status.is_success(),
            "write container file: {}",
            reply.message()
        );
        Ok(())
    }

    pub async fn stat_file(
        &self,
        cell: &Arc<CellContainer>,
        path: FilePath,
    ) -> Result<FileMetadata> {
        let run = cell.current_run();
        self.file_run(cell, run)?;
        let (reply, stat) = self
            .docker
            .archive(
                "HEAD",
                &route(&cell.name, path.as_str(), false),
                Bytes::new(),
                OVERHEAD,
            )
            .await?;
        self.file_run(cell, run)?;
        ensure!(
            reply.status.is_success(),
            "stat container file: {}",
            reply.message()
        );
        stat_header(&stat.context("container stat header missing")?)
    }

    pub async fn list_files(
        &self,
        cell: &Arc<CellContainer>,
        path: FilePath,
        maximum: usize,
        max_entries: usize,
    ) -> Result<Vec<FileMetadata>> {
        ensure!(
            (1..=MAX_FILE_ENTRIES).contains(&max_entries),
            "invalid container listing entry budget"
        );
        let run = cell.current_run();
        self.file_run(cell, run)?;
        let path = if path.as_str() == "/" {
            "/.".into()
        } else {
            format!("{}/.", path.as_str())
        };
        let (reply, _) = self
            .docker
            .archive(
                "GET",
                &route(&cell.name, &path, false),
                Bytes::new(),
                archive_limit(maximum)?,
            )
            .await?;
        self.file_run(cell, run)?;
        ensure!(
            reply.status.is_success(),
            "list container files: {}",
            reply.message()
        );
        list_archive(&reply.body, maximum, max_entries)
    }
}
