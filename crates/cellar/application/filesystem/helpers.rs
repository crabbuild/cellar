// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::*;

impl Filesystem {
    /// Return at most the caller's bound, additionally limited by class policy.
    /// Reject oversized files before allocating the output buffer.
    pub fn read_file_bounded(&self, path: impl AsRef<[u8]>, maximum: usize) -> Result<Vec<u8>> {
        operation("filesystem.read_file_bounded", || {
            let path = self.path(path)?;
            self.read(|db| {
                let inode = db.resolve(1, &path)?;
                db.require_permission(inode, 0o400)?;
                let stat = db.stat(inode)?;
                if stat.kind != Kind::File {
                    return Err(Error::new(
                        Errno::EISDIR,
                        "whole-file read requires a regular file",
                    )
                    .into());
                }
                if stat.size > maximum.min(self.limits.max_file_helper_bytes) as u64 {
                    return Err(
                        Error::new(Errno::EFBIG, "bounded whole-file read limit exceeded").into(),
                    );
                }
                self.charge(
                    0,
                    (stat.size as usize)
                        .div_ceil(CHUNK_BYTES)
                        .saturating_mul(CHUNK_BYTES),
                )?;
                let mut data = vec![0; stat.size as usize];
                let mut read = 0;
                while read < data.len() {
                    let count =
                        db.bounded_content()
                            .pread(inode, read as u64, &mut data[read..])?;
                    if count == 0 {
                        return Err(
                            Error::new(Errno::EIO, "unexpected end of bounded file read").into(),
                        );
                    }
                    read += count;
                }
                Ok(data)
            })
        })
    }

    /// Atomically publish a new inode. Existing descriptors keep the old inode
    /// until their last pin is released and maintenance reclaims it.
    pub fn replace_file(&self, path: impl AsRef<[u8]>, data: &[u8]) -> Result<()> {
        operation("filesystem.replace_file", || {
            self.scope()?;
            if data.len() > self.limits.max_file_helper_bytes {
                return Err(Error::new(
                    Errno::EFBIG,
                    "whole-file replacement exceeds helper bound",
                ));
            }
            let path = self.path(path)?;
            self.charge(
                0,
                data.len().div_ceil(CHUNK_BYTES).saturating_mul(CHUNK_BYTES),
            )?;
            self.mutate(|db| {
                let inode = db.replace(&path, 0o666, self.policy.creation_mask)?;
                let mut written = 0;
                while written < data.len() {
                    written += db
                        .bounded_content()
                        .pwrite(inode, written as u64, &data[written..], false)?
                        .0;
                }
                Ok(())
            })
        })
    }

    /// Apply one byte-range patch against an exact inode/content revision.
    /// The target must be a regular file, including for an empty patch.
    /// A conflict or any later validation/storage error rolls back all effects.
    pub fn edit_file(
        &self,
        path: impl AsRef<[u8]>,
        expected: FileVersion,
        edit: &Edit,
    ) -> Result<FileVersion> {
        operation("filesystem.edit_file", || {
            let path = self.path(path)?;
            self.mutate(|db| {
                let inode = db.resolve(1, &path)?;
                let stat = db.stat(inode)?;
                if stat.kind != Kind::File {
                    return Err(
                        Error::new(Errno::EISDIR, "file edit requires a regular file").into(),
                    );
                }
                if FileVersion::from(&stat) != expected {
                    return Err(
                        Error::new(Errno::EBUSY, "file identity/content revision changed")
                            .with_code("Conflict")
                            .into(),
                    );
                }
                db.require_permission(inode, 0o200)?;
                let end = edit
                    .offset
                    .checked_add(edit.delete_bytes)
                    .filter(|end| *end <= stat.size)
                    .ok_or_else(|| Error::new(Errno::EINVAL, "edit range is outside the file"))?;
                let size = stat
                    .size
                    .checked_sub(edit.delete_bytes)
                    .and_then(|size| size.checked_add(edit.insert.len() as u64))
                    .ok_or_else(|| Error::new(Errno::EOVERFLOW, "edit size overflow"))?;
                if edit.insert.len() > self.limits.max_file_helper_bytes {
                    return Err(
                        Error::new(Errno::EFBIG, "edit insertion exceeds helper bound").into(),
                    );
                }
                let in_place = edit.delete_bytes == edit.insert.len() as u64;
                if in_place || end == stat.size {
                    // In-place patches and tail replacements never materialize the
                    // unaffected file. This also makes a checked append suitable
                    // for a large-file effect + receipt in one guest batch.
                    if edit.delete_bytes != 0 || !edit.insert.is_empty() {
                        self.charge(
                            0,
                            edit.insert
                                .len()
                                .div_ceil(CHUNK_BYTES)
                                .saturating_add(2)
                                .saturating_mul(2)
                                .saturating_mul(CHUNK_BYTES),
                        )?;
                    }
                    if !in_place {
                        db.truncate(inode, edit.offset)?;
                    }
                    if !edit.insert.is_empty() {
                        db.bounded_content()
                            .pwrite(inode, edit.offset, &edit.insert, false)?;
                    }
                    return Ok(FileVersion::from(&db.stat(inode)?));
                }
                if stat.size > self.limits.max_file_helper_bytes as u64
                    || size > self.limits.max_file_helper_bytes as u64
                {
                    return Err(Error::new(
                        Errno::EFBIG,
                        "shifting edit exceeds whole-file helper bound; use positional I/O",
                    )
                    .into());
                }
                self.charge(
                    0,
                    (stat.size as usize)
                        .div_ceil(CHUNK_BYTES)
                        .saturating_add((size as usize).div_ceil(CHUNK_BYTES))
                        .saturating_mul(CHUNK_BYTES),
                )?;
                let mut original = vec![0; stat.size as usize];
                let mut read = 0;
                while read < original.len() {
                    let count =
                        db.bounded_content()
                            .pread(inode, read as u64, &mut original[read..])?;
                    if count == 0 {
                        return Err(Error::new(Errno::EIO, "unexpected end of edit source").into());
                    }
                    read += count;
                }
                let mut content = Vec::with_capacity(size as usize);
                content.extend_from_slice(&original[..edit.offset as usize]);
                content.extend_from_slice(&edit.insert);
                content.extend_from_slice(&original[end as usize..]);
                db.truncate(inode, 0)?;
                let mut written = 0;
                while written < content.len() {
                    written += db
                        .bounded_content()
                        .pwrite(inode, written as u64, &content[written..], false)?
                        .0;
                }
                Ok(FileVersion::from(&db.stat(inode)?))
            })
        })
    }

    /// Stateless bounded continuation. Namespace changes may affect subsequent
    /// pages; a path replaced by another directory invalidates the cursor.
    pub fn read_dir(
        &self,
        path: impl AsRef<[u8]>,
        limit: usize,
        cursor: Option<&ReadDirCursor>,
    ) -> Result<DirectoryPage> {
        operation("filesystem.read_dir", || {
            let scope = self.scope()?;
            if limit == 0 || limit > self.limits.max_directory_entries {
                return Err(Error::new(
                    Errno::EINVAL,
                    "directory page exceeds entry bound",
                ));
            }
            let path = self.path(path)?;
            self.read(|db| {
                let inode = db.resolve(1, &path)?;
                if cursor.is_some_and(|cursor| {
                    cursor.scope != scope
                        || cursor.inode != inode
                        || cursor.position > i64::MAX as u64
                }) {
                    return Err(Error::new(
                        Errno::EINVAL,
                        "directory cursor does not match this cell/directory",
                    )
                    .into());
                }
                let mut rows =
                    db.read_dir_page(inode, cursor.map_or(0, |cursor| cursor.position), limit + 1)?;
                let more = rows.len() > limit;
                rows.truncate(limit);
                let next = if more {
                    rows.last().map(|(position, _)| ReadDirCursor {
                        scope: scope.into(),
                        inode,
                        position: *position,
                    })
                } else {
                    None
                };
                Ok(DirectoryPage {
                    entries: rows.into_iter().map(|(_, entry)| entry).collect(),
                    next,
                })
            })
        })
    }
}

impl Directory {
    /// A bounded loop over readdir; failure can leave a consumed prefix.
    pub fn read_page(&self, limit: usize) -> Result<Vec<DirEntry>> {
        operation("filesystem.read_page", || {
            self.file.record.check(&self.file.fs)?;
            if limit > self.file.fs.limits.max_directory_entries {
                return Err(Error::new(
                    Errno::EINVAL,
                    "directory page exceeds entry bound",
                ));
            }
            let mut entries = Vec::new();
            for _ in 0..limit {
                match self.readdir()? {
                    Some(entry) => entries.push(entry),
                    None => break,
                }
            }
            Ok(entries)
        })
    }
}
