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
            || self.max_bytes > 16 * 1024 * 1024 * 1024
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
        Self::open_inner(directory, filesystem, limits, None)
    }

    /// Open with physical reservation accounting. Pins keep both the directory
    /// owner and its accounting registration alive after the store handle drops.
    pub fn open_observed(
        directory: PathBuf,
        filesystem: Arc<dyn FileSystem>,
        limits: GenerationLimits,
        accounting: &GenerationAccounting,
    ) -> Result<Self, GenerationError> {
        Self::open_inner(directory, filesystem, limits, Some(accounting))
    }

    fn open_inner(
        directory: PathBuf,
        filesystem: Arc<dyn FileSystem>,
        limits: GenerationLimits,
        accounting: Option<&GenerationAccounting>,
    ) -> Result<Self, GenerationError> {
        limits.validate()?;
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
                state: Mutex::new(state),
                _lock: lock,
            }),
        })
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

    /// Apply a bounded sequence to one exclusively owned private image. Every
    /// intermediate result is verified before applying its successor. Only the
    /// final image is synced and sealed; no intermediate pin can escape. The
    /// normal apply byte/page/file limits cover the entire sequence, and disk
    /// admission reserves twice its largest intermediate image.
    pub fn prepare_chain(
        &self,
        id: ImageId,
        base: Option<&GenerationPin>,
        updates: &[ImageUpdate<'_>],
    ) -> Result<GenerationPin, GenerationError> {
        let peak = apply::peak_chain_bytes(updates, &self.inner.limits.apply)?;
        let update = updates.first().ok_or(ApplyError::Limit)?;
        if let Some(base) = base {
            if !Arc::ptr_eq(&self.inner, &base.inner) || base.state() != update.base {
                return Err(GenerationError::Identity);
            }
        } else if update.base != ImageState::empty(update.base.epoch)? {
            return Err(ApplyError::MissingBase.into());
        }
        // Two names can coexist before rename or after a crash. Reserving two
        // full private images conservatively bounds either recovered layout.
        let charge = peak.checked_mul(2).ok_or(GenerationError::Limit)?;
        let token = Arc::new(());
        {
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
                return Err(GenerationError::Limit);
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
        }
        let mut reservation = Reservation {
            inner: self.inner.clone(),
            id,
            token,
            finished: false,
        };
        let stage = self.inner.directory.join(stage_name(id));
        let final_path = self.inner.directory.join(final_name(id));
        let mut stage_owned = false;
        let mut final_attempted = false;
        let mut final_owned = false;
        let result = (|| {
            let mut destination = self.inner.filesystem.create_read_write(&stage)?;
            stage_owned = true;
            let base_path = base.map(|p| self.inner.directory.join(final_name(p.id())));
            let image = apply::apply_private_chain(
                self.inner.filesystem.as_ref(),
                base_path.as_deref(),
                &mut destination,
                updates,
                &self.inner.limits.apply,
            )?;
            destination.sync_all()?;
            drop(destination);
            // Atomic exclusive creation proves this final name is ours. Rename
            // replaces only this unpublished placeholder, never an old image.
            final_attempted = true;
            let placeholder = self.inner.filesystem.create_new(&final_path)?;
            final_owned = true;
            drop(placeholder);
            self.inner.filesystem.rename(&stage, &final_path)?;
            self.inner.filesystem.sync_all(&self.inner.directory)?;
            Ok::<_, GenerationError>(image)
        })();
        match result {
            Ok(image) => {
                let record = Arc::new(ImageRecord { id, state: image });
                let mut state = self.lock()?;
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
                // A failed exclusive creation may have created its path before
                // returning an error. Without a successful handle we cannot
                // prove ownership or refund its capacity. Reopening under the
                // directory lock reconciles every surviving name.
                if !stage_owned || final_attempted && !final_owned {
                    return Err(operation);
                }
                let cleanup = (|| {
                    if final_owned {
                        remove_if_present(self.inner.filesystem.as_ref(), &final_path)?;
                    }
                    remove_if_present(self.inner.filesystem.as_ref(), &stage)?;
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
                entry.retained = true;
                return Ok(GenerationPin {
                    inner: self.inner.clone(),
                    image: image.clone(),
                });
            }
            if !entry.files.iter().any(|(name, _)| name == &final_name(id)) {
                return Err(GenerationError::Missing);
            }
            entry.busy = true;
            entry.retained = true;
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
    /// sync finish before capacity is refunded. A failure quarantines admission.
    pub fn reclaim(&self, max_files: usize) -> Result<ReclaimReport, GenerationError> {
        if max_files == 0 || max_files > 4096 {
            return Err(GenerationError::Limit);
        }
        let mut report = ReclaimReport::default();
        for _ in 0..max_files {
            let selected = {
                let mut state = self.lock()?;
                let selected = state
                    .entries
                    .iter()
                    .find(|(id, e)| {
                        !e.retained
                            && !e.busy
                            && state
                                .references
                                .values()
                                .all(|reference| !reference.active || reference.image != **id)
                            && e.image.as_ref().is_none_or(|p| Arc::strong_count(p) == 1)
                    })
                    .map(|(id, _)| *id);
                let Some(id) = selected else {
                    break;
                };
                let entry = state
                    .entries
                    .get_mut(&id)
                    .ok_or(GenerationError::Quarantined)?;
                let (name, length) = entry
                    .files
                    .first()
                    .cloned()
                    .ok_or(GenerationError::Layout)?;
                entry.busy = true;
                (id, entry.token.clone(), name, length)
            };
            let (id, token, name, length) = selected;
            let mut reservation = Reservation {
                inner: self.inner.clone(),
                id,
                token,
                finished: false,
            };
            remove_if_present(
                self.inner.filesystem.as_ref(),
                &self.inner.directory.join(&name),
            )?;
            self.inner.filesystem.sync_all(&self.inner.directory)?;
            let mut state = self.lock()?;
            let entry = state
                .entries
                .get_mut(&id)
                .ok_or(GenerationError::Quarantined)?;
            if !Arc::ptr_eq(&entry.token, &reservation.token) {
                return Err(GenerationError::Quarantined);
            }
            entry.files.remove(0);
            entry.charged -= length;
            entry.busy = false;
            let empty = entry.files.is_empty();
            state.bytes -= length;
            if empty {
                state.entries.remove(&id);
            }
            reservation.finished = true;
            report.files += 1;
            report.bytes += length;
        }
        Ok(report)
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

fn stage_name(id: ImageId) -> String {
    format!("{}.stage", id.text())
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
