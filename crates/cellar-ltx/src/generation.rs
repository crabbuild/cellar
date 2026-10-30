// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Immutable image ownership, durable file publication and bounded reclamation.
//! The host retains every durable marker/archive reference before retiring an
//! image. Physical sealing/pinning never grants Raft authority or query readiness.

use crate::apply::{self, ApplyError, ApplyLimits, ImageState, ImageUpdate};
use crate::host::{create_dir_all_durable, FileSystem, HostFile, HostFileLock};
use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

pub use crate::generation_accounting::{GenerationAccounting, GenerationResources};
pub use crate::generation_vfs::{open_immutable, ImmutableDatabase, ReadServices};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ImageId([u8; 16]);

impl ImageId {
    pub fn new(bytes: [u8; 16]) -> Result<Self, GenerationError> {
        if bytes == [0; 16] {
            return Err(GenerationError::Identity);
        }
        Ok(Self(bytes))
    }

    pub fn bytes(self) -> [u8; 16] {
        self.0
    }

    fn text(self) -> String {
        format!("{:032x}", u128::from_be_bytes(self.0))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct GenerationLimits {
    pub apply: ApplyLimits,
    pub max_generations: usize,
    pub max_bytes: u64,
}

impl Default for GenerationLimits {
    fn default() -> Self {
        Self {
            apply: ApplyLimits::default(),
            max_generations: 64,
            max_bytes: 512 * 1024 * 1024,
        }
    }
}

impl GenerationLimits {
    fn validate(&self) -> Result<(), GenerationError> {
        self.apply.validate()?;
        if self.max_generations == 0
            || self.max_generations > 4096
            || self.max_bytes == 0
            || self.max_bytes > 256 * 1024 * 1024 * 1024
        {
            return Err(GenerationError::Limit);
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GenerationError {
    #[error("invalid generation identity")]
    Identity,
    #[error("invalid or unrecognized generation directory contents")]
    Layout,
    #[error("generation admission is exhausted")]
    Limit,
    #[error("generation identity already exists")]
    Exists,
    #[error("generation is missing")]
    Missing,
    #[error("generation operation is in progress")]
    Busy,
    #[error("image store is quarantined; close and recover before admission")]
    Quarantined,
    #[error("image apply: {0}")]
    Apply(#[from] ApplyError),
    #[error("streamed snapshot: {0}")]
    Snapshot(#[from] crate::stream_snapshot::Error),
    #[error("image store I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("image operation failed ({operation}); cleanup also failed: {cleanup}")]
    Cleanup {
        operation: Box<GenerationError>,
        cleanup: std::io::Error,
    },
}

struct ImageRecord {
    id: ImageId,
    state: ImageState,
}

struct Entry {
    token: Arc<()>,
    image: Option<Arc<ImageRecord>>,
    retained: bool,
    busy: bool,
    files: Vec<(String, u64)>,
    charged: u64,
}

struct RetainedReference {
    revision: u64,
    image: ImageId,
    state: ImageState,
    active: bool,
}

struct State {
    entries: BTreeMap<ImageId, Entry>,
    references: BTreeMap<u64, RetainedReference>,
    bytes: u64,
    quarantined: bool,
    accounting: Option<crate::generation_accounting::Registration>,
}

impl State {
    fn usage(&self) -> GenerationUsage {
        GenerationUsage {
            generations: self.entries.len(),
            bytes: self.bytes,
            retained: self
                .entries
                .iter()
                .filter(|(id, entry)| {
                    entry.retained
                        || self
                            .references
                            .values()
                            .any(|reference| reference.active && reference.image == **id)
                })
                .count(),
            busy: self.entries.values().filter(|entry| entry.busy).count(),
        }
    }
}

struct StateGuard<'a>(MutexGuard<'a, State>);

impl Deref for StateGuard<'_> {
    type Target = State;

    fn deref(&self) -> &State {
        &self.0
    }
}

impl DerefMut for StateGuard<'_> {
    fn deref_mut(&mut self) -> &mut State {
        &mut self.0
    }
}

impl Drop for StateGuard<'_> {
    fn drop(&mut self) {
        if self.0.accounting.is_some() {
            let usage = self.0.usage();
            let failed = self.0.quarantined || std::thread::panicking();
            if let Some(accounting) = &mut self.0.accounting {
                if failed {
                    accounting.fail();
                } else {
                    accounting.update(usage);
                }
            }
        }
    }
}

struct Inner {
    directory: PathBuf,
    filesystem: Arc<dyn FileSystem>,
    limits: GenerationLimits,
    snapshot_scratch: crate::stream_snapshot::ScratchPool,
    state: Mutex<State>,
    _lock: HostFileLock,
}

#[derive(Clone)]
pub struct GenerationStore {
    inner: Arc<Inner>,
}

/// Pins bytes, their verified identity and the directory owner for the entire
/// reader/worker lifetime. Cloning a pin prevents reclamation, not retirement.
#[derive(Clone)]
pub struct GenerationPin {
    inner: Arc<Inner>,
    image: Arc<ImageRecord>,
}

impl GenerationPin {
    pub fn id(&self) -> ImageId {
        self.image.id
    }

    pub fn state(&self) -> ImageState {
        self.image.state
    }

    /// Select this verified image for an authoritative, monotonically revised
    /// reference. This performs no I/O. At most 64 distinct nonzero reference
    /// keys are admitted per store. A stale revision returns false; an equal
    /// revision naming different bytes is refused. Retirement cannot revoke a
    /// selected reference, even after all pins are dropped. The host reconstructs
    /// these references from durable markers before recovery reclamation.
    pub fn retain_reference(&self, reference: u64, revision: u64) -> Result<bool, GenerationError> {
        if reference == 0 || revision == 0 {
            return Err(GenerationError::Identity);
        }
        let store = GenerationStore {
            inner: self.inner.clone(),
        };
        let mut state = store.lock()?;
        let entry = state
            .entries
            .get(&self.id())
            .ok_or(GenerationError::Missing)?;
        if entry.busy {
            return Err(GenerationError::Busy);
        }
        if !entry
            .image
            .as_ref()
            .is_some_and(|image| Arc::ptr_eq(image, &self.image))
        {
            return Err(GenerationError::Identity);
        }
        if let Some(current) = state.references.get(&reference) {
            if current.revision > revision {
                return Ok(false);
            }
            if current.revision == revision
                && (current.image != self.id() || current.state != self.state())
            {
                return Err(GenerationError::Identity);
            }
        } else if state.references.len() >= 64 {
            return Err(GenerationError::Limit);
        }
        state.references.insert(
            reference,
            RetainedReference {
                revision,
                image: self.id(),
                state: self.state(),
                active: true,
            },
        );
        Ok(true)
    }

    /// Release only this exact reference revision. Its tombstone prevents a
    /// delayed release from revoking a newer selection. No I/O is performed.
    pub fn release_reference(
        &self,
        reference: u64,
        revision: u64,
    ) -> Result<bool, GenerationError> {
        if reference == 0 || revision == 0 {
            return Err(GenerationError::Identity);
        }
        let store = GenerationStore {
            inner: self.inner.clone(),
        };
        let mut state = store.lock()?;
        let current = state
            .references
            .get_mut(&reference)
            .ok_or(GenerationError::Identity)?;
        if current.revision > revision {
            return Ok(false);
        }
        if current.revision != revision
            || current.image != self.id()
            || current.state != self.state()
        {
            return Err(GenerationError::Identity);
        }
        current.active = false;
        Ok(true)
    }

    /// Create an independently verified disposable writer base. COW is used
    /// only with this pin's exact filesystem provider. Never returns a handle
    /// before destination verification, even when a copy adapter reports success.
    pub fn private_copy(
        &self,
        filesystem: &Arc<dyn FileSystem>,
        destination: &Path,
    ) -> Result<(), GenerationError> {
        let source_path = self.inner.directory.join(final_name(self.id()));
        let clone = if Arc::ptr_eq(filesystem, &self.inner.filesystem) {
            filesystem.clone_read_write(&source_path, destination)?
        } else {
            None
        };
        let mut target = match clone {
            Some(file) => file,
            None => {
                let mut target = filesystem.create_read_write(destination)?;
                let mut reader = self.inner.filesystem.open(&source_path)?;
                apply::inspect(&mut reader, &self.state(), Some(&mut target))?;
                target
            }
        };
        apply::inspect(&mut target, &self.state(), None)?;
        Ok(())
    }

    /// Encode this immutable image without retaining its pages in memory.
    /// The anonymous scratch files use this pin's injected filesystem.
    pub fn snapshot(
        &self,
        limits: crate::stream_snapshot::Limits,
    ) -> Result<crate::stream_snapshot::SnapshotFile, GenerationError> {
        self.snapshot_with_activity(limits, None)
    }

    /// The same physical snapshot contract with completed-I/O observations.
    /// Observations grant no data, persistence or publication proof.
    pub fn snapshot_with_activity(
        &self,
        limits: crate::stream_snapshot::Limits,
        activity: Option<crate::host::IoActivity>,
    ) -> Result<crate::stream_snapshot::SnapshotFile, GenerationError> {
        self.snapshot_with_io_profile(limits, crate::stream_snapshot::IoProfile::Compact, activity)
    }

    /// Encode with explicit codec memory admission supplied by the caller.
    /// Physical identity, scratch ownership and source-read bounds are unchanged.
    pub fn snapshot_with_io_profile(
        &self,
        limits: crate::stream_snapshot::Limits,
        profile: crate::stream_snapshot::IoProfile,
        activity: Option<crate::host::IoActivity>,
    ) -> Result<crate::stream_snapshot::SnapshotFile, GenerationError> {
        let mut name = self
            .inner
            .directory
            .file_name()
            .ok_or(GenerationError::Layout)?
            .to_os_string();
        name.push(".snapshot-scratch");
        let scratch = self.inner.directory.with_file_name(name);
        create_dir_all_durable(self.inner.filesystem.as_ref(), &scratch)?;
        let mut reader = self.open_reader()?;
        reader.file.set_io_activity(activity.clone());
        Ok(crate::stream_snapshot::encode_observed_in(
            self.inner.filesystem.as_ref(),
            &scratch,
            self.state(),
            limits,
            &self.inner.snapshot_scratch,
            crate::stream_snapshot::IoOptions { profile, activity },
            |offset, length| {
                reader
                    .read_exact_at(offset, length)
                    .map_err(std::io::Error::other)
            },
        )?)
    }

    pub fn open_reader(&self) -> Result<GenerationReader, GenerationError> {
        let file = self
            .inner
            .filesystem
            .open(&self.inner.directory.join(final_name(self.id())))?;
        Ok(GenerationReader {
            file,
            pin: self.clone(),
        })
    }
}

/// Synchronous reads belong on the host's owning execution/blocking domain.
pub struct GenerationReader {
    file: HostFile,
    pin: GenerationPin,
}

impl GenerationReader {
    pub fn length(&self) -> u64 {
        self.pin.state().length
    }

    pub fn read_exact_at(
        &mut self,
        offset: u64,
        length: usize,
    ) -> Result<Vec<u8>, GenerationError> {
        if offset
            .checked_add(length as u64)
            .is_none_or(|end| end > self.length())
            || length > 64 * 1024
        {
            return Err(GenerationError::Limit);
        }
        let bytes = self.file.read_exact_at(offset, length)?;
        if bytes.len() != length {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof).into());
        }
        Ok(bytes)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GenerationUsage {
    pub generations: usize,
    pub bytes: u64,
    pub retained: usize,
    pub busy: usize,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReclaimReport {
    pub files: usize,
    pub bytes: u64,
}

struct Reservation {
    inner: Arc<Inner>,
    id: ImageId,
    token: Arc<()>,
    finished: bool,
}

struct ReclaimEntry {
    reservation: Reservation,
    files: Vec<(String, u64)>,
}

/// Selected names remain busy and fully charged until their common directory
/// barrier succeeds. Dropping an unfinished selection quarantines the store.
struct Reclamation {
    entries: Vec<ReclaimEntry>,
}

impl Reclamation {
    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn remove(&self, filesystem: &dyn FileSystem, directory: &Path) -> Result<(), GenerationError> {
        for entry in &self.entries {
            for (name, _) in &entry.files {
                remove_if_present(filesystem, &directory.join(name))?;
            }
        }
        Ok(())
    }

    /// The caller must have completed the directory sync covering every removal.
    fn finish(&mut self, state: &mut State) -> Result<ReclaimReport, GenerationError> {
        let mut report = ReclaimReport::default();
        for selected in &self.entries {
            let entry = state
                .entries
                .get(&selected.reservation.id)
                .ok_or(GenerationError::Quarantined)?;
            let bytes: u64 = selected.files.iter().map(|(_, length)| length).sum();
            if !entry.busy
                || !Arc::ptr_eq(&entry.token, &selected.reservation.token)
                || !entry.files.starts_with(&selected.files)
                || entry.charged < bytes
            {
                return Err(GenerationError::Quarantined);
            }
            report.files += selected.files.len();
            report.bytes += bytes;
        }
        if state.bytes < report.bytes {
            return Err(GenerationError::Quarantined);
        }
        for selected in &mut self.entries {
            // The validation pass above and this update hold the same lock.
            let entry = state.entries.get_mut(&selected.reservation.id).unwrap();
            entry.files.drain(..selected.files.len());
            entry.charged -= selected.files.iter().map(|(_, length)| length).sum::<u64>();
            entry.busy = false;
            if entry.files.is_empty() {
                state.entries.remove(&selected.reservation.id);
            }
            selected.reservation.finished = true;
        }
        state.bytes -= report.bytes;
        Ok(report)
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if !self.finished {
            if let Ok(mut state) = self.inner.lock() {
                state.quarantined = true;
            }
        }
    }
}

impl GenerationStore {
    pub fn open(
        directory: PathBuf,
        filesystem: Arc<dyn FileSystem>,
        limits: GenerationLimits,
    ) -> Result<Self, GenerationError> {
        Self::open_inner(directory, filesystem, limits, None, None)
    }

    /// Open with physical reservation accounting. Pins keep both the directory
    /// owner and its accounting registration alive after the store handle drops.
    pub fn open_observed(
        directory: PathBuf,
        filesystem: Arc<dyn FileSystem>,
        limits: GenerationLimits,
        accounting: &GenerationAccounting,
    ) -> Result<Self, GenerationError> {
        Self::open_inner(directory, filesystem, limits, Some(accounting), None)
    }

    /// Share snapshot spool admission with other producers/receivers on the
    /// same node. Immutable generation bytes keep their separate reservation.
    pub fn open_with_scratch(
        directory: PathBuf,
        filesystem: Arc<dyn FileSystem>,
        limits: GenerationLimits,
        accounting: Option<&GenerationAccounting>,
        snapshot_scratch: crate::stream_snapshot::ScratchPool,
    ) -> Result<Self, GenerationError> {
        Self::open_inner(
            directory,
            filesystem,
            limits,
            accounting,
            Some(snapshot_scratch),
        )
    }

    fn open_inner(
        directory: PathBuf,
        filesystem: Arc<dyn FileSystem>,
        limits: GenerationLimits,
        accounting: Option<&GenerationAccounting>,
        snapshot_scratch: Option<crate::stream_snapshot::ScratchPool>,
    ) -> Result<Self, GenerationError> {
        limits.validate()?;
        let snapshot_scratch = match snapshot_scratch {
            Some(pool) => pool,
            None => crate::stream_snapshot::ScratchPool::new(Default::default())?,
        };
        create_dir_all_durable(filesystem.as_ref(), &directory)?;
        let lock = filesystem.lock_file(&directory.join("images.lock"))?;
        filesystem.sync_all(&directory.join("images.lock"))?;
        filesystem.sync_all(&directory)?;
        let mut entries = BTreeMap::<ImageId, Entry>::new();
        let mut bytes = 0_u64;
        for entry in filesystem.read_dir_iter(&directory)? {
            let entry = entry?;
            if entry.file_name == "images.lock" {
                continue;
            }
            if entry.is_dir {
                return Err(GenerationError::Layout);
            }
            let name = entry.file_name.to_str().ok_or(GenerationError::Layout)?;
            let (id, _) = parse_name(name)?;
            let metadata = filesystem.metadata(&directory.join(name))?;
            if !metadata.is_file || metadata.len > limits.apply.decode.max_image_bytes {
                return Err(GenerationError::Layout);
            }
            bytes = bytes
                .checked_add(metadata.len)
                .ok_or(GenerationError::Limit)?;
            if bytes > limits.max_bytes {
                return Err(GenerationError::Limit);
            }
            let record = entries.entry(id).or_insert_with(|| Entry {
                token: Arc::new(()),
                image: None,
                retained: true,
                busy: false,
                files: Vec::new(),
                charged: 0,
            });
            if record.files.len() >= 2 {
                return Err(GenerationError::Layout);
            }
            record.files.push((name.into(), metadata.len));
            record.charged += metadata.len;
            if entries.len() > limits.max_generations {
                return Err(GenerationError::Limit);
            }
        }
        let mut state = State {
            entries,
            references: BTreeMap::new(),
            bytes,
            quarantined: false,
            accounting: None,
        };
        state.accounting = accounting.map(|accounting| accounting.register(limits, state.usage()));
        Ok(Self {
            inner: Arc::new(Inner {
                directory,
                filesystem,
                limits,
                snapshot_scratch,
                state: Mutex::new(state),
                _lock: lock,
            }),
        })
    }

    /// Configured bounds for one sealed image's physical replay work.
    pub fn apply_limits(&self) -> ApplyLimits {
        self.inner.limits.apply
    }

    /// Whether this pin belongs to this exact open directory owner. Logical
    /// image IDs and digests alone do not establish local file ownership.
    /// This does not select the image as an authoritative retained reference.
    pub fn owns(&self, pin: &GenerationPin) -> bool {
        Arc::ptr_eq(&self.inner, &pin.inner)
    }

    /// Build a new image from an exact pinned base. The returned image is fully
    /// fsynced and immutable. The host must separately persist its Raft apply
    /// marker before publishing this pin to readers or settling a command.
    pub fn prepare(
        &self,
        id: ImageId,
        base: Option<&GenerationPin>,
        update: &ImageUpdate<'_>,
    ) -> Result<GenerationPin, GenerationError> {
        self.prepare_chain(id, base, std::slice::from_ref(update))
    }

    /// Captured physical limits; callers can partition replay without relaxing
    /// the store's aggregate decode, temporary-image or generation budgets.
    pub fn limits(&self) -> GenerationLimits {
        self.inner.limits
    }

    /// Replay a bounded contiguous chain into one new private image. Every
    /// intermediate result must pass its full digest and checksum proof before
    /// the next update runs. Only the final image is synced, sealed and pinned;
    /// no published image is mutated and no intermediate result is observable.
    /// File/page/byte limits and peak disk reservation cover the whole chain.
    /// Eligible retired files share the final directory sync. If admission
    /// requires their capacity first, reclamation completes before reservation.
    pub fn prepare_chain(
        &self,
        id: ImageId,
        base: Option<&GenerationPin>,
        updates: &[ImageUpdate<'_>],
    ) -> Result<GenerationPin, GenerationError> {
        self.prepare_chain_with_activity(id, base, updates, None)
    }

    /// The same private chain, with observations on its owned destination.
    /// All independent inspection and persistence barriers remain required.
    pub fn prepare_chain_with_activity(
        &self,
        id: ImageId,
        base: Option<&GenerationPin>,
        updates: &[ImageUpdate<'_>],
        activity: Option<crate::host::IoActivity>,
    ) -> Result<GenerationPin, GenerationError> {
        let peak = apply::peak_chain_bytes(updates, &self.inner.limits.apply)?;
        let first = updates.first().ok_or(ApplyError::Limit)?;
        if let Some(base) = base {
            if !Arc::ptr_eq(&self.inner, &base.inner) || base.state() != first.base {
                return Err(GenerationError::Identity);
            }
        } else if first.base != ImageState::empty(first.base.epoch)? {
            return Err(ApplyError::MissingBase.into());
        }
        let base_path = base.map(|p| self.inner.directory.join(final_name(p.id())));
        self.prepare_private(
            id,
            peak,
            base_path.as_deref(),
            activity,
            |destination, cloned| {
                if cloned {
                    Ok(apply::apply_private_tail(
                        destination,
                        first.base,
                        updates,
                        &self.inner.limits.apply,
                    )
                    .map_err(|error| match error {
                        ApplyError::Digest => ApplyError::BaseDigest,
                        error => error,
                    })?)
                } else {
                    Ok(apply::apply_private_chain(
                        self.inner.filesystem.as_ref(),
                        base_path.as_deref(),
                        destination,
                        updates,
                        &self.inner.limits.apply,
                    )?)
                }
            },
        )
    }

    /// Seal an independently verified, file-backed physical snapshot. The body
    /// and decoded image are bounded separately; command-delta budgets do not
    /// grow with the full image quota. The same reservation, cleanup and sync
    /// barriers as command publication protect this path.
    pub fn prepare_snapshot(
        &self,
        id: ImageId,
        snapshot: &crate::stream_snapshot::SnapshotFile,
    ) -> Result<GenerationPin, GenerationError> {
        self.prepare_snapshot_chain(id, snapshot, &[])
    }

    /// Apply a bounded tail after the file-backed snapshot while both remain
    /// private. Every intermediate image is independently inspected, but only
    /// the final generation crosses file and directory publication barriers.
    pub fn prepare_snapshot_chain(
        &self,
        id: ImageId,
        snapshot: &crate::stream_snapshot::SnapshotFile,
        updates: &[ImageUpdate<'_>],
    ) -> Result<GenerationPin, GenerationError> {
        self.prepare_snapshot_chain_with_activity(id, snapshot, updates, None)
    }

    /// Observe destination writes and independent rereads during snapshot
    /// replay without relaxing its reservation, integrity or sync contract.
    pub fn prepare_snapshot_chain_with_activity(
        &self,
        id: ImageId,
        snapshot: &crate::stream_snapshot::SnapshotFile,
        updates: &[ImageUpdate<'_>],
        activity: Option<crate::host::IoActivity>,
    ) -> Result<GenerationPin, GenerationError> {
        let expected = snapshot.metadata().image;
        expected.validate(&self.inner.limits.apply)?;
        let peak = if updates.is_empty() {
            expected.length
        } else {
            if updates[0].base != expected {
                return Err(GenerationError::Identity);
            }
            apply::peak_chain_bytes(updates, &self.inner.limits.apply)?.max(expected.length)
        };
        self.prepare_private(id, peak, None, activity, |destination, _cloned| {
            snapshot.write_image(destination)?;
            // Re-read the destination independently: valid input does not prove
            // that the storage adapter wrote the same bytes to the final file.
            Ok(apply::apply_private_tail(
                destination,
                expected,
                updates,
                &self.inner.limits.apply,
            )?)
        })
    }

    /// Import an exact physical SQLite image as private data material. A COW
    /// clone is preferred where supported; the portable fallback copies bounded
    /// buffers. The final destination is independently checked before sealing.
    /// This method grants no cell membership, Raft position or SDK output proof.
    pub fn prepare_image_file(
        &self,
        id: ImageId,
        source: &Path,
        expected: ImageState,
    ) -> Result<GenerationPin, GenerationError> {
        expected.validate(&self.inner.limits.apply)?;
        if expected.length == 0 {
            return Err(GenerationError::Identity);
        }
        self.prepare_private(
            id,
            expected.length,
            Some(source),
            None,
            |destination, cloned| {
                if !cloned {
                    let mut source = self.inner.filesystem.open(source)?;
                    apply::inspect(&mut source, &expected, Some(destination))?;
                }
                apply::inspect(destination, &expected, None)?;
                Ok(expected)
            },
        )
    }

    fn prepare_private(
        &self,
        id: ImageId,
        peak: u64,
        clone_source: Option<&Path>,
        activity: Option<crate::host::IoActivity>,
        write: impl FnOnce(&mut HostFile, bool) -> Result<ImageState, GenerationError>,
    ) -> Result<GenerationPin, GenerationError> {
        // Keep the existing conservative temporary-image admission charge.
        // Publication uses one exclusive name, but does not relax capacity.
        let charge = peak.checked_mul(2).ok_or(GenerationError::Limit)?;
        if charge > self.inner.limits.max_bytes {
            return Err(GenerationError::Limit);
        }
        let token = Arc::new(());
        let mut reclaimed_for_capacity = false;
        loop {
            let mut state = self.lock()?;
            if state.entries.contains_key(&id) {
                return Err(GenerationError::Exists);
            }
            if state.entries.len() >= self.inner.limits.max_generations
                || state
                    .bytes
                    .checked_add(charge)
                    .is_none_or(|b| b > self.inner.limits.max_bytes)
            {
                if reclaimed_for_capacity {
                    return Err(GenerationError::Limit);
                }
                drop(state);
                self.reclaim(4096)?;
                reclaimed_for_capacity = true;
                continue;
            }
            state.bytes += charge;
            state.entries.insert(
                id,
                Entry {
                    token: token.clone(),
                    image: None,
                    retained: true,
                    busy: true,
                    files: Vec::new(),
                    charged: charge,
                },
            );
            break;
        }
        let mut reservation = Reservation {
            inner: self.inner.clone(),
            id,
            token,
            finished: false,
        };
        let final_path = self.inner.directory.join(final_name(id));
        let mut final_owned = false;
        let mut reclamation = None;
        let result = (|| {
            // The exclusive name is not publication authority. The busy entry
            // prevents adoption while bytes are incomplete; no pin escapes
            // until verification and both persistence barriers succeed. Crash
            // recovery adopts only an independently authoritative exact marker.
            let cloned = clone_source
                .map(|source| self.inner.filesystem.clone_read_write(source, &final_path))
                .transpose()?
                .flatten();
            let is_cloned = cloned.is_some();
            let mut destination = match cloned {
                Some(file) => file,
                None => self.inner.filesystem.create_read_write(&final_path)?,
            };
            destination.set_io_activity(activity);
            final_owned = true;
            let image = write(&mut destination, is_cloned)?;
            destination.sync_all()?;
            drop(destination);
            let selected = self.select_reclamation(8)?;
            reclamation = Some(selected);
            if let Some(selected) = &reclamation {
                selected.remove(self.inner.filesystem.as_ref(), &self.inner.directory)?;
            }
            self.inner.filesystem.sync_all(&self.inner.directory)?;
            Ok::<_, GenerationError>(image)
        })();
        match result {
            Ok(image) => {
                let record = Arc::new(ImageRecord { id, state: image });
                let mut state = self.lock()?;
                if let Some(selected) = &mut reclamation {
                    selected.finish(&mut state)?;
                }
                let entry = state
                    .entries
                    .get_mut(&id)
                    .ok_or(GenerationError::Quarantined)?;
                if !Arc::ptr_eq(&entry.token, &reservation.token) {
                    return Err(GenerationError::Quarantined);
                }
                let old = entry.charged;
                entry.image = Some(record.clone());
                entry.busy = false;
                entry.files = vec![(final_name(id), image.length)];
                entry.charged = image.length;
                state.bytes = state.bytes - old + image.length;
                reservation.finished = true;
                Ok(GenerationPin {
                    inner: self.inner.clone(),
                    image: record,
                })
            }
            Err(operation) => {
                // Once reclamation starts, a failed removal/shared barrier has
                // an uncertain layout. Keep every reservation charged and let
                // their guards quarantine admission until recovery inventories
                // the directory. No new image pin has escaped.
                if reclamation
                    .as_ref()
                    .is_some_and(|selected| !selected.is_empty())
                {
                    return Err(operation);
                }
                // A failed exclusive creation may have created its path before
                // returning an error. Without a successful handle we cannot
                // prove ownership or refund its capacity. Reopening under the
                // directory lock reconciles every surviving name.
                if !final_owned {
                    return Err(operation);
                }
                let cleanup = (|| {
                    remove_if_present(self.inner.filesystem.as_ref(), &final_path)?;
                    self.inner.filesystem.sync_all(&self.inner.directory)
                })();
                if let Err(cleanup) = cleanup {
                    return Err(GenerationError::Cleanup {
                        operation: Box::new(operation),
                        cleanup,
                    });
                }
                let mut state = self.lock()?;
                let entry = state
                    .entries
                    .remove(&reservation.id)
                    .ok_or(GenerationError::Quarantined)?;
                state.bytes -= entry.charged;
                reservation.finished = true;
                Err(operation)
            }
        }
    }

    /// Adopt only an image named by an authoritative retained marker. Directory
    /// inventory is never interpreted as "the largest/newest image is current".
    pub fn adopt(
        &self,
        id: ImageId,
        expected: ImageState,
    ) -> Result<GenerationPin, GenerationError> {
        self.pin_existing(id, expected, true)
    }

    /// Reopen an exactly identified prepared image without changing its host
    /// retention flag. Cold images are independently verified and synced before
    /// returning a pin. This is physical preparation only: the caller must prove
    /// the expected image from its committed input and separately publish it.
    pub fn pin_prepared(
        &self,
        id: ImageId,
        expected: ImageState,
    ) -> Result<GenerationPin, GenerationError> {
        self.pin_existing(id, expected, false)
    }

    fn pin_existing(
        &self,
        id: ImageId,
        expected: ImageState,
        retain: bool,
    ) -> Result<GenerationPin, GenerationError> {
        expected.validate(&self.inner.limits.apply)?;
        let token = {
            let mut state = self.lock()?;
            let entry = state.entries.get_mut(&id).ok_or(GenerationError::Missing)?;
            if entry.busy {
                return Err(GenerationError::Busy);
            }
            if let Some(image) = &entry.image {
                if image.state != expected {
                    return Err(GenerationError::Identity);
                }
                entry.retained |= retain;
                return Ok(GenerationPin {
                    inner: self.inner.clone(),
                    image: image.clone(),
                });
            }
            if !entry.files.iter().any(|(name, _)| name == &final_name(id)) {
                return Err(GenerationError::Missing);
            }
            entry.busy = true;
            entry.retained |= retain;
            entry.token.clone()
        };
        let mut reservation = Reservation {
            inner: self.inner.clone(),
            id,
            token,
            finished: false,
        };
        let mut file = self
            .inner
            .filesystem
            .open(&self.inner.directory.join(final_name(id)))?;
        apply::inspect(&mut file, &expected, None)?;
        file.sync_all()?;
        self.inner.filesystem.sync_all(&self.inner.directory)?;
        let image = Arc::new(ImageRecord {
            id,
            state: expected,
        });
        let mut state = self.lock()?;
        let entry = state
            .entries
            .get_mut(&id)
            .ok_or(GenerationError::Quarantined)?;
        if !Arc::ptr_eq(&entry.token, &reservation.token) {
            return Err(GenerationError::Quarantined);
        }
        entry.image = Some(image.clone());
        entry.busy = false;
        reservation.finished = true;
        Ok(GenerationPin {
            inner: self.inner.clone(),
            image,
        })
    }

    /// Release the host retention reference only after all durable marker,
    /// recovery and archive references have been removed. Reader pins still win.
    pub fn retire(&self, id: ImageId) -> Result<(), GenerationError> {
        let mut state = self.lock()?;
        let entry = state.entries.get_mut(&id).ok_or(GenerationError::Missing)?;
        if entry.busy {
            return Err(GenerationError::Busy);
        }
        entry.retained = false;
        Ok(())
    }

    /// After recovery has adopted every authoritative durable reference, retire
    /// the remaining inventory. This never selects a reference from filenames.
    /// Call only with the complete marker/archive reference set and no pending
    /// publication. Live pins and monotonic references still prevent reclamation.
    pub fn retain_only(&self, references: &[ImageId]) -> Result<(), GenerationError> {
        let mut state = self.lock()?;
        if references.len() > self.inner.limits.max_generations {
            return Err(GenerationError::Limit);
        }
        if state.entries.values().any(|entry| entry.busy) {
            return Err(GenerationError::Busy);
        }
        for id in references {
            if state
                .entries
                .get(id)
                .is_none_or(|entry| entry.image.is_none())
            {
                return Err(GenerationError::Missing);
            }
        }
        for (id, entry) in &mut state.entries {
            entry.retained = references.contains(id);
        }
        Ok(())
    }

    /// Delete at most `max_files` unreferenced names. Deletion and its directory
    /// sync finish before capacity is refunded. All selected names share one
    /// directory barrier. A failure quarantines admission.
    pub fn reclaim(&self, max_files: usize) -> Result<ReclaimReport, GenerationError> {
        let mut selected = self.select_reclamation(max_files)?;
        if selected.is_empty() {
            return Ok(ReclaimReport::default());
        }
        selected.remove(self.inner.filesystem.as_ref(), &self.inner.directory)?;
        self.inner.filesystem.sync_all(&self.inner.directory)?;
        let mut state = self.lock()?;
        selected.finish(&mut state)
    }

    fn select_reclamation(&self, max_files: usize) -> Result<Reclamation, GenerationError> {
        if max_files == 0 || max_files > 4096 {
            return Err(GenerationError::Limit);
        }
        let mut state = self.lock()?;
        let mut selected = Vec::new();
        let mut remaining = max_files;
        for (id, entry) in &state.entries {
            if entry.retained
                || entry.busy
                || state
                    .references
                    .values()
                    .any(|r| r.active && r.image == *id)
                || entry
                    .image
                    .as_ref()
                    .is_some_and(|p| Arc::strong_count(p) != 1)
            {
                continue;
            }
            if entry.files.is_empty() {
                return Err(GenerationError::Layout);
            }
            let files = entry
                .files
                .iter()
                .take(remaining)
                .cloned()
                .collect::<Vec<_>>();
            remaining -= files.len();
            selected.push((*id, entry.token.clone(), files));
            if remaining == 0 {
                break;
            }
        }
        for (id, _, _) in &selected {
            // Selection and reservation hold the same lock: no new pin or
            // reference can adopt these files between the two passes.
            state.entries.get_mut(id).unwrap().busy = true;
        }
        drop(state);
        Ok(Reclamation {
            entries: selected
                .into_iter()
                .map(|(id, token, files)| ReclaimEntry {
                    reservation: Reservation {
                        inner: self.inner.clone(),
                        id,
                        token,
                        finished: false,
                    },
                    files,
                })
                .collect(),
        })
    }

    pub fn usage(&self) -> Result<GenerationUsage, GenerationError> {
        let state = self.lock()?;
        Ok(state.usage())
    }

    fn lock(&self) -> Result<StateGuard<'_>, GenerationError> {
        self.inner.lock()
    }
}

impl Inner {
    fn lock(&self) -> Result<StateGuard<'_>, GenerationError> {
        let state = match self.state.lock() {
            Ok(state) => StateGuard(state),
            Err(poisoned) => {
                if let Some(accounting) = &poisoned.into_inner().accounting {
                    accounting.fail();
                }
                return Err(GenerationError::Quarantined);
            }
        };
        if state.quarantined {
            return Err(GenerationError::Quarantined);
        }
        Ok(state)
    }
}

fn final_name(id: ImageId) -> String {
    format!("{}.sqlite", id.text())
}

fn parse_name(name: &str) -> Result<(ImageId, bool), GenerationError> {
    let (id, stage) = if let Some(id) = name.strip_suffix(".stage") {
        (id, true)
    } else if let Some(id) = name.strip_suffix(".sqlite") {
        (id, false)
    } else {
        return Err(GenerationError::Layout);
    };
    if id.len() != 32
        || !id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(GenerationError::Layout);
    }
    let value = u128::from_str_radix(id, 16).map_err(|_| GenerationError::Identity)?;
    Ok((ImageId::new(value.to_be_bytes())?, stage))
}

fn remove_if_present(fs: &dyn FileSystem, path: &Path) -> std::io::Result<()> {
    match fs.remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}
