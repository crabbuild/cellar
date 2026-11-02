// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded append segments and successful-barrier watermarks. Writers run on
//! existing blocking workers. No timer, extra thread or async task owns a file.
mod codec;
mod range;
mod reclaim;
mod recovery;

use super::*;
use codec::{FOOTER, FRAME, HEADER, OVERHEAD};
use timing::Phase;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Location {
    pub segment: u64,
    offset: u64,
    length: u64,
    key: Key,
}

impl Location {
    pub(super) fn end(self) -> u64 {
        self.offset + OVERHEAD + self.length
    }
}

struct Segment {
    length: u64,
    // Zero after reopening: framing alone is neither durable nor verified.
    durable: u64,
    namespace_persisted: bool,
    syncing: bool,
    appendable: bool,
}

#[derive(Default)]
struct State {
    files: BTreeMap<u64, Segment>,
    current: Option<u64>,
    next: u64,
    failed: bool,
}

pub(super) struct Segments {
    state: Mutex<State>,
    idle: Condvar,
    target: u64,
    group_commit: bool,
}

impl Segments {
    pub(super) fn new(target: u64, group_commit: bool) -> Result<Self, StoreError> {
        if !(4096..=64 * 1024 * 1024).contains(&target) {
            return Err(StoreError::Limit);
        }
        Ok(Self {
            state: Mutex::new(State::default()),
            idle: Condvar::new(),
            target,
            group_commit,
        })
    }

    fn state(&self) -> Result<MutexGuard<'_, State>, StoreError> {
        self.state.lock().map_err(|_| StoreError::Quarantined)
    }
}

pub(super) fn name(id: u64) -> String {
    format!("s-{id:016x}")
}

impl Inner {
    pub(super) fn storage_charge(&self, length: u64) -> u64 {
        length + if self.segments.is_some() { OVERHEAD } else { 0 }
    }

    pub(super) fn checked_object(self: &Arc<Self>, id: ExtentId) -> Result<HostFile, StoreError> {
        let location = self
            .state()?
            .objects
            .get(&id)
            .ok_or(StoreError::Missing)?
            .location;
        match location {
            Some(location) => self.open_record(location),
            None => self.checked_file(
                &self.directory.join(inventory::object_name(id)),
                u64::from(id.length()),
            ),
        }
    }

    pub(super) fn checked_root(
        self: &Arc<Self>,
        reference: ExtentRootReference,
    ) -> Result<HostFile, StoreError> {
        let location = self
            .state()?
            .roots
            .get(&reference.root)
            .ok_or(StoreError::Missing)?
            .location;
        match location {
            Some(location) => self.open_record(location),
            None => self.checked_file(
                &self.directory.join(inventory::root_name(reference.root)),
                u64::from(reference.length),
            ),
        }
    }

    pub(super) fn persist_reopened(&self, file: &mut HostFile) -> Result<(), StoreError> {
        self.sync_file(file)?;
        if self.segments.is_none() {
            self.sync_directory()?;
        }
        Ok(())
    }

    /// Append a complete framed record while holding only the append owner's
    /// mutex. Sync runs outside this lock, so other workers can fill a cohort.
    pub(super) fn append_record(
        self: &Arc<Self>,
        key: Key,
        length: u64,
        write: impl FnOnce(&mut HostFile) -> Result<(), StoreError>,
    ) -> Result<(Location, HostFile), StoreError> {
        let _panic = AppendPanic(self);
        let segments = self.segments.as_ref().ok_or(StoreError::Layout)?;
        let wait = timing::span(Phase::InventoryWait);
        let mut state = segments.state()?;
        drop(wait);
        if state.failed || self.state()?.quarantined {
            return Err(StoreError::Quarantined);
        }
        let size = length + OVERHEAD;
        let current = state.current.filter(|id| {
            state.files.get(id).is_some_and(|v| {
                v.appendable && (v.length == HEADER || v.length + size <= segments.target)
            })
        });
        let id = match current {
            Some(id) => id,
            None => {
                if state.files.len()
                    >= self
                        .limits
                        .max_objects
                        .saturating_add(self.limits.max_roots)
                {
                    return Err(StoreError::Limit);
                }
                let id = state.next.checked_add(1).ok_or(StoreError::Limit)?;
                {
                    let mut inventory = self.state()?;
                    let charged = inventory
                        .bytes
                        .checked_add(HEADER)
                        .ok_or(StoreError::Limit)?;
                    if charged > self.limits.max_bytes {
                        return Err(StoreError::Limit);
                    }
                    inventory.bytes = charged;
                }
                // Charges and sequence remain conservative after uncertain creation.
                state.next = id;
                state.current = None;
                let created = (|| {
                    let mut file = self
                        .filesystem
                        .create_read_write(&self.directory.join(name(id)))?;
                    if file.file_len()? != 0 {
                        return Err(StoreError::Integrity);
                    }
                    file.write_all_at(0, &codec::segment_header(id))?;
                    self.sync_file(&mut file)?;
                    self.sync_directory_cleanup()?;
                    if file.file_len()? != HEADER
                        || file.read_exact_at(0, HEADER as usize)? != codec::segment_header(id)
                    {
                        return Err(StoreError::Integrity);
                    }
                    Ok::<_, StoreError>(())
                })();
                if let Err(error) = created {
                    state.failed = true;
                    self.quarantine();
                    segments.idle.notify_all();
                    return Err(error);
                }
                state.files.insert(
                    id,
                    Segment {
                        length: HEADER,
                        durable: HEADER,
                        namespace_persisted: true,
                        syncing: false,
                        appendable: true,
                    },
                );
                state.current = Some(id);
                id
            }
        };
        let segment = state.files.get_mut(&id).ok_or(StoreError::Missing)?;
        let location = Location {
            segment: id,
            offset: segment.length,
            length,
            key,
        };
        let result = (|| {
            let mut file = self
                .filesystem
                .open_read_write(&self.directory.join(name(id)))?;
            if file.file_len()? != segment.length {
                return Err(StoreError::Integrity);
            }
            let header = codec::frame(key, location.offset, length);
            file.write_all_at(location.offset, &header)?;
            let mut payload = range::file(self.clone(), file, location, false);
            write(&mut payload)?;
            // Finish framing through a separate handle. A partial record poisons
            // admission; no subsequent writer may append beyond an uncertain tail.
            let mut file = self
                .filesystem
                .open_read_write(&self.directory.join(name(id)))?;
            file.write_all_at(location.offset + FRAME + length, &codec::footer(&header))?;
            if file.file_len()? != location.end() {
                return Err(StoreError::Integrity);
            }
            Ok::<_, StoreError>(())
        })();
        if let Err(error) = result {
            state.failed = true;
            self.quarantine();
            segments.idle.notify_all();
            return Err(error);
        }
        segment.length = location.end();
        drop(state);
        let file = self.open_record(location)?;
        Ok((location, file))
    }

    fn sync_segment(&self, location: Location) -> io::Result<()> {
        self.sync_segment_inner(location).map_err(io::Error::other)
    }

    pub(super) fn sync_segment_inner(&self, location: Location) -> Result<(), StoreError> {
        let segments = self.segments.as_ref().ok_or(StoreError::Layout)?;
        let wait = timing::span(Phase::BarrierWait);
        let mut state = segments.state()?;
        loop {
            if state.failed || self.state()?.quarantined {
                return Err(StoreError::Quarantined);
            }
            let segment = state
                .files
                .get_mut(&location.segment)
                .ok_or(StoreError::Missing)?;
            if location.end() > segment.length {
                return Err(StoreError::Integrity);
            }
            if segment.durable >= location.end() && segment.namespace_persisted {
                return Ok(());
            }
            if !segment.syncing {
                break;
            }
            state = segments
                .idle
                .wait(state)
                .map_err(|_| StoreError::Quarantined)?;
        }
        let segment = state
            .files
            .get_mut(&location.segment)
            .ok_or(StoreError::Missing)?;
        let covered = if segments.group_commit {
            segment.length
        } else {
            location.end()
        };
        let namespace = segment.namespace_persisted;
        segment.syncing = true;
        drop(state);
        drop(wait);
        let mut leader = SyncLeader {
            inner: self,
            complete: false,
        };
        let result = (|| {
            let mut file = self
                .filesystem
                .open(&self.directory.join(name(location.segment)))?;
            if file.file_len()? < covered {
                return Err(StoreError::Integrity);
            }
            self.sync_file(&mut file)?;
            if !namespace {
                self.sync_directory_cleanup()?;
            }
            Ok::<_, StoreError>(())
        })();
        let mut state = segments.state()?;
        let segment = state
            .files
            .get_mut(&location.segment)
            .ok_or(StoreError::Missing)?;
        segment.syncing = false;
        match result {
            Ok(()) => {
                segment.durable = covered;
                segment.namespace_persisted = true;
            }
            Err(error) => {
                state.failed = true;
                drop(state);
                self.quarantine();
                segments.idle.notify_all();
                leader.complete = true;
                return Err(error);
            }
        }
        drop(state);
        segments.idle.notify_all();
        leader.complete = true;
        Ok(())
    }
}

struct SyncLeader<'a> {
    inner: &'a Inner,
    complete: bool,
}

impl Drop for SyncLeader<'_> {
    fn drop(&mut self) {
        if !self.complete {
            if let Some(segments) = &self.inner.segments {
                segments
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .failed = true;
                segments.idle.notify_all();
            }
            self.inner.quarantine();
        }
    }
}

// Declared before the append mutex guard, so unwinding releases that guard
// before poisoning shared admission and waking every barrier waiter.
struct AppendPanic<'a>(&'a Inner);

impl Drop for AppendPanic<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            if let Some(segments) = &self.0.segments {
                segments
                    .state
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .failed = true;
                segments.idle.notify_all();
            }
            self.0.quarantine();
        }
    }
}
