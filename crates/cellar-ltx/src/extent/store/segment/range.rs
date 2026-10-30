// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded, immutable payload views; ordinary extent and root verification sees
//! logical bytes only. Every read stays within the authenticated record range.
use super::*;
use crate::host::HostFileIo;

struct Range {
    inner: Arc<Inner>,
    file: HostFile,
    location: Location,
    complete: bool,
}

pub(super) fn file(
    inner: Arc<Inner>,
    file: HostFile,
    location: Location,
    complete: bool,
) -> HostFile {
    HostFile::from_io(Range {
        inner,
        file,
        location,
        complete,
    })
}

impl Range {
    fn bounds(&self, offset: u64, length: usize) -> io::Result<u64> {
        if offset
            .checked_add(length as u64)
            .is_none_or(|end| end > self.location.length)
        {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        Ok(self.location.offset + FRAME + offset)
    }
}

impl HostFileIo for Range {
    fn write_all(&mut self, _bytes: &[u8]) -> io::Result<()> {
        Err(io::ErrorKind::Unsupported.into())
    }

    fn write_all_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        if self.complete {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
        let at = self.bounds(offset, bytes.len())?;
        self.file.write_all_at(at, bytes)
    }

    fn write_all_at_window(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        if self.complete {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
        let at = self.bounds(offset, bytes.len())?;
        self.file.write_all_at_window(at, bytes)
    }

    fn read_exact_at(&mut self, offset: u64, length: usize) -> io::Result<Vec<u8>> {
        let at = self.bounds(offset, length)?;
        self.file.read_exact_at(at, length)
    }

    fn read_exact_into_at(&mut self, offset: u64, output: &mut [u8]) -> io::Result<()> {
        let at = self.bounds(offset, output.len())?;
        self.file.read_exact_into_at(at, output)
    }

    fn file_len(&mut self) -> io::Result<u64> {
        if self.complete {
            validate(&mut self.file, self.location).map_err(io::Error::other)?;
        }
        Ok(self.location.length)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        if !self.complete {
            return Err(io::ErrorKind::PermissionDenied.into());
        }
        self.inner.sync_segment(self.location)
    }
}

pub(super) fn validate(file: &mut HostFile, location: Location) -> Result<(), StoreError> {
    if file.file_len()? < location.end() {
        return Err(StoreError::Integrity);
    }
    let header = file.read_exact_at(location.offset, FRAME as usize)?;
    if codec::decode(&header, location.offset)? != (location.key, location.length)
        || file.read_exact_at(location.end() - FOOTER, FOOTER as usize)? != codec::footer(&header)
    {
        return Err(StoreError::Integrity);
    }
    Ok(())
}

impl Inner {
    pub(super) fn open_record(
        self: &Arc<Self>,
        location: Location,
    ) -> Result<HostFile, StoreError> {
        let path = self.directory.join(name(location.segment));
        let metadata = self.filesystem.symlink_metadata(&path)?;
        if !metadata.is_file || metadata.len < location.end() {
            return Err(StoreError::Integrity);
        }
        let mut owned = self.filesystem.open(&path)?;
        range::validate(&mut owned, location)?;
        Ok(file(self.clone(), owned, location, true))
    }
}
