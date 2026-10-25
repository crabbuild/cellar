// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Committed cell archives. This module supplies recovery bytes, never votes,
//! membership authorization, leader eligibility or an SDK output permit.

use super::apply::{image_position, marker, PublishedImage};
use super::cache::GroupLog;
use super::manifest::{
    self, Image, Manifest, ObjectKind, ObjectRef, Position, PositionRange, Snapshot, Tail,
};
use crate::application::consensus::{CellUpdate, EnvelopeLimits};
use ::raft::prelude::{ConfState, Entry, EntryType};
use cellar_logic::consensus::{GroupScope, LogPosition};
use cellar_ltx::apply::{ApplyLimits, ImageState};
use cellar_ltx::generation::{GenerationError, GenerationStore, ImageId};
use cellar_ltx::{ltx, LtxHost, TXID};
use futures_util::{StreamExt, TryStreamExt};
use object_store::{path::Path, ObjectStore, PutMode, PutOptions, UpdateVersion};
use protobuf::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};

pub(crate) mod bundle;
mod coverage;
mod replay;

pub use coverage::VerifiedArchiveCoverage;

const READ_WINDOW: usize = 4;

// Logical recovery limits exclude the containing bundles. Retain an additional
// full bundle for every concurrent extraction, including cancelled workers.
pub(crate) const RECOVERY_BUNDLE_READ_BYTES: usize = READ_WINDOW * bundle::MAX_BYTES;
pub(crate) const MAX_RECOVERY_READ_BYTES: usize = 1024 * 1024 * 1024 + RECOVERY_BUNDLE_READ_BYTES;

#[derive(Debug)]
pub enum ArchiveError {
    Invalid(&'static str),
    Corrupt(&'static str),
    Conflict,
    Limit,
    Unconfirmed,
    Stopped,
    Timeout,
    Log(super::storage::LogError),
    Images(GenerationError),
    Storage(object_store::Error),
    Worker(cellar_ltx::host::HostTaskError),
}

impl fmt::Display for ArchiveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Raft archive: {self:?}")
    }
}

impl std::error::Error for ArchiveError {}

#[derive(Clone, Copy, Debug)]
pub struct ArchiveLimits {
    pub envelope: EnvelopeLimits,
    /// Logical SQLite image bound, independent of compressed object length.
    pub max_image_bytes: u64,
    pub io_timeout_ms: u64,
    pub max_capture_bytes: usize,
    pub max_manifest_bytes: usize,
    pub max_object_bytes: usize,
    pub max_set_bytes: usize,
    pub max_entries: usize,
    pub max_tails: usize,
    /// Maximum encoded tail bytes as a multiple of the current image length.
    /// Rotate earlier for small images so repeated recovery-set reads and
    /// replay do not grow to the absolute set limit. Larger values trade fewer
    /// snapshot writes for more retained-history work; other limits still apply.
    pub max_tail_image_ratio: u32,
    pub max_dependencies: usize,
}

impl Default for ArchiveLimits {
    fn default() -> Self {
        Self {
            envelope: EnvelopeLimits::default(),
            max_image_bytes: 64 * 1024 * 1024,
            io_timeout_ms: 10_000,
            max_capture_bytes: 4 * 1024 * 1024,
            max_manifest_bytes: 256 * 1024,
            max_object_bytes: 128 * 1024 * 1024,
            max_set_bytes: 256 * 1024 * 1024,
            max_entries: 1024,
            max_tails: 32,
            max_tail_image_ratio: 2,
            max_dependencies: 128,
        }
    }
}

impl ArchiveLimits {
    pub fn validate(self) -> Result<(), ArchiveError> {
        self.envelope
            .validate()
            .map_err(|_| ArchiveError::Invalid("archive envelope limits"))?;
        if !(1024..=1024 * 1024).contains(&self.max_manifest_bytes)
            || !(512..=256 * 1024 * 1024).contains(&self.max_image_bytes)
            || !(1..=60_000).contains(&self.io_timeout_ms)
            || !(1024..=self.max_object_bytes).contains(&self.max_capture_bytes)
            || self.max_manifest_bytes > self.max_object_bytes
            || !(1024..=512 * 1024 * 1024).contains(&self.max_object_bytes)
            || self.max_set_bytes < self.max_object_bytes
            || self.max_set_bytes > 1024 * 1024 * 1024
            || !(1..=4096).contains(&self.max_entries)
            || !(1..=128).contains(&self.max_tails)
            || !(1..=128).contains(&self.max_tail_image_ratio)
            || self.max_dependencies > 1024
        {
            return Err(ArchiveError::Limit);
        }
        Ok(())
    }
}

/// An immutable dependency's actual bytes, copied under the cell history's
/// content-addressed prefix. The caller must enumerate the complete closure
/// referenced by the committed image; unsupported effects must remain refused.
#[derive(Clone)]
pub struct ArchiveDependency(pub Vec<u8>);

/// One bounded scratch owner may verify many cells. The exclusively opened
/// store must initially be empty; no pin from it is exposed outside this owner.
/// A failed replay quarantines it until its directory owner performs recovery.
#[derive(Clone)]
pub struct ArchiveVerifier {
    inner: Arc<Mutex<VerifierState>>,
}

struct VerifierState {
    images: GenerationStore,
    nonce: ImageId,
    sequence: u64,
    failed: bool,
}

impl ArchiveVerifier {
    pub fn new(images: GenerationStore, nonce: ImageId) -> Result<Self, ArchiveError> {
        if images.usage().map_err(ArchiveError::Images)?.generations != 0 {
            return Err(ArchiveError::Invalid("archive verifier store is not empty"));
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(VerifierState {
                images,
                nonce,
                sequence: 0,
                failed: false,
            })),
        })
    }

    fn verify(&self, set: &ArchiveRecoverySet) -> Result<(), ArchiveError> {
        self.verify_checked(set, || Ok(()))
    }

    fn verify_checked(
        &self,
        set: &ArchiveRecoverySet,
        check: impl FnOnce() -> Result<(), ArchiveError>,
    ) -> Result<(), ArchiveError> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| ArchiveError::Invalid("archive verifier owner is poisoned"))?;
        // Lifecycle verification may have expired while another replay owned
        // the workspace. Refuse before touching scratch state in that case.
        check()?;
        if state.failed {
            return Err(ArchiveError::Invalid("archive verifier requires recovery"));
        }
        state.sequence = state.sequence.checked_add(1).ok_or(ArchiveError::Limit)?;
        let mut hash = Sha256::new();
        hash.update(b"cellar-archive-verifier-1");
        hash.update(state.nonce.bytes());
        hash.update(state.sequence.to_be_bytes());
        let digest: [u8; 32] = hash.finalize().into();
        let nonce = ImageId::new(
            digest[..16]
                .try_into()
                .map_err(|_| ArchiveError::Invalid("archive verification nonce"))?,
        )
        .map_err(ArchiveError::Images)?;
        // Set before any filesystem work. An interrupted/panicking verifier
        // cannot silently reuse partially restored scratch state.
        state.failed = true;
        if let Some(pin) = set.restore_state(&state.images, nonce)? {
            state
                .images
                .retire(pin.id())
                .map_err(ArchiveError::Images)?;
            drop(pin);
        }
        state.images.reclaim(8).map_err(ArchiveError::Images)?;
        if state
            .images
            .usage()
            .map_err(ArchiveError::Images)?
            .generations
            != 0
        {
            return Err(ArchiveError::Invalid(
                "archive verifier retained scratch images",
            ));
        }
        state.failed = false;
        Ok(())
    }
}

/// Constructed only from an exact durable engine/image pair. Holding this value
/// pins the image even if a newer image replaces it or the archive is unavailable.
#[derive(Clone)]
pub struct CommittedArchiveCut {
    image: Option<PublishedImage>,
    scope: GroupScope,
    schema_digest: [u8; 32],
    birth_digest: [u8; 32],
    position: LogPosition,
    conf_state: Vec<u8>,
    membership: super::member_state::GroupMembership,
    entries: Vec<Entry>,
    dependencies: Vec<ArchiveDependency>,
}

impl CommittedArchiveCut {
    pub fn capture(
        group: &GroupLog,
        image: PublishedImage,
        dependencies: Vec<ArchiveDependency>,
        limits: ArchiveLimits,
    ) -> Result<Self, ArchiveError> {
        let schema = image.application_digest();
        Self::capture_state(group, Some(image), schema, dependencies, limits)
    }

    /// Capture a durable configuration before any SQL image exists. This never
    /// creates SQLite state and refuses a missing publication of applied data.
    pub fn capture_empty(
        group: &GroupLog,
        schema: [u8; 32],
        limits: ArchiveLimits,
    ) -> Result<Self, ArchiveError> {
        Self::capture_state(group, None, schema, Vec::new(), limits)
    }

    fn capture_state(
        group: &GroupLog,
        image: Option<PublishedImage>,
        schema_digest: [u8; 32],
        dependencies: Vec<ArchiveDependency>,
        limits: ArchiveLimits,
    ) -> Result<Self, ArchiveError> {
        limits.validate()?;
        if image
            .as_ref()
            .is_some_and(|image| image.scope() != &group.identity().scope)
            || schema_digest == [0; 32]
            || dependencies.len() > limits.max_dependencies
        {
            return Err(ArchiveError::Invalid("archive cut scope/dependencies"));
        }
        let mut charge = 0usize;
        for dependency in &dependencies {
            charge = charge
                .checked_add(dependency.0.len())
                .ok_or(ArchiveError::Limit)?;
            if dependency.0.len() > limits.max_object_bytes || charge > limits.max_set_bytes {
                return Err(ArchiveError::Limit);
            }
        }
        let cache = group.read().map_err(ArchiveError::Log)?;
        let applied = &cache.applied;
        if applied.image != image.as_ref().map(|image| marker(&image.pin()))
            || applied.position.index.get() == 0
            || applied.position.term.get() == 0
            || applied.position.index.get() > cache.hard_state.commit
            || cache.term(applied.position.index.get()) != Some(applied.position.term.get())
            || image.as_ref().is_some_and(|image| {
                applied.position.index < image.position().index
                    || applied.position.term < image.position().term
                    || cache.term(image.position().index.get()) != Some(image.position().term.get())
            })
        {
            return Err(ArchiveError::Invalid("archive cut is not durably applied"));
        }
        // ConfState is persisted atomically with the applied marker. Protocol
        // entries may advance it while retaining the exact same image.
        let position = applied.position;
        let conf_state = manifest::encode_conf(&cache.conf_state)?;
        let mut entries = Vec::new();
        let mut bytes = charge
            .checked_add(cache.membership.reserved_bytes())
            .and_then(|bytes| bytes.checked_add(group.identity().scope.cell.len() + 128))
            .ok_or(ArchiveError::Limit)?;
        if bytes > limits.max_capture_bytes {
            return Err(ArchiveError::Limit);
        }
        for (_, entry) in
            cache
                .entries
                .range(..=position.index.get())
                .rev()
                .take(if image.is_some() {
                    limits.max_entries
                } else {
                    0
                })
        {
            let next = bytes
                .checked_add(entry.compute_size() as usize + 128)
                .ok_or(ArchiveError::Limit)?;
            if next > limits.max_capture_bytes {
                break;
            }
            bytes = next;
            entries.push(entry.clone());
        }
        entries.reverse();
        Ok(Self {
            image,
            scope: group.identity().scope.clone(),
            schema_digest,
            birth_digest: group.birth_digest,
            position,
            conf_state,
            membership: cache.membership.clone(),
            entries,
            dependencies,
        })
    }

    pub fn position(&self) -> LogPosition {
        self.position
    }

    fn image_state(&self) -> Option<Image> {
        self.image.as_ref().map(|image| image.pin().state().into())
    }

    pub(crate) fn memory_bytes(&self) -> usize {
        self.membership.reserved_bytes()
            + self.scope.cell.len()
            + 128
            + self
                .entries
                .iter()
                .map(|entry| entry.compute_size() as usize + 128)
                .sum::<usize>()
            + self
                .dependencies
                .iter()
                .map(|dependency| dependency.0.len())
                .sum::<usize>()
    }

    fn snapshot(&self, limits: ArchiveLimits) -> Result<Vec<u8>, ArchiveError> {
        let pin = self
            .image
            .as_ref()
            .ok_or(ArchiveError::Invalid(
                "empty archive has no snapshot payload",
            ))?
            .pin();
        let state = pin.state();
        Image::from(state).decode()?;
        if state.length > limits.max_image_bytes || state.length > limits.max_object_bytes as u64 {
            return Err(ArchiveError::Limit);
        }
        let mut reader = pin.open_reader().map_err(ArchiveError::Images)?;
        let mut pages = Vec::new();
        let count = state.length / u64::from(state.page_size);
        let mut hash = Sha256::new();
        for i in 0..count {
            let page = reader
                .read_exact_at(i * u64::from(state.page_size), state.page_size as usize)
                .map_err(ArchiveError::Images)?;
            hash.update(&page);
            if i + 1 != u64::from(ltx::lock_pgno(state.page_size)) {
                pages.push((i as u32 + 1, page));
            }
        }
        if <[u8; 32]>::from(hash.finalize()) != state.digest {
            return Err(ArchiveError::Corrupt("pinned archive image digest"));
        }
        let header = ltx::Header {
            version: ltx::VERSION,
            flags: if state.checksum == 0 {
                ltx::HEADER_FLAG_NO_CHECKSUM
            } else {
                0
            },
            page_size: state.page_size,
            commit: count as u32,
            min_txid: TXID(1),
            max_txid: state.txid,
            ..Default::default()
        };
        let bytes = ltx::encode_file_v0_5_2(&header, &pages, state.checksum)
            .map_err(|_| ArchiveError::Corrupt("archive snapshot encoding"))?;
        if bytes.len() > limits.max_object_bytes {
            return Err(ArchiveError::Limit);
        }
        verify_snapshot(&bytes, state)?;
        Ok(bytes)
    }
}

#[derive(Clone)]
pub struct CellArchive {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    bundle_prefix: String,
    scope: GroupScope,
    root: [u8; 32],
    birth: [u8; 32],
    initial_membership: super::member_state::GroupMembership,
    schema: [u8; 32],
    limits: ArchiveLimits,
    host: LtxHost,
    cancellation: Option<tokio::sync::watch::Receiver<bool>>,
    io_domain: Option<crate::asyncrt::DomainToken>,
    read_retention: Option<Arc<super::budget::Permit>>,
    bundle_client: Option<bundle::Client>,
    verification_deadline: Option<u64>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Head {
    manifest: ObjectRef,
    position: Position,
}

/// An immutable manifest address. This names recovery data only; it is neither
/// live-quorum authorization nor permission to initialize or attach a member.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ArchiveReference(ObjectRef);

impl ArchiveReference {
    pub fn position(&self) -> Result<LogPosition, ArchiveError> {
        if self.0.kind != ObjectKind::Manifest || self.0.bundle.is_some() {
            return Err(ArchiveError::Corrupt("archive reference domain"));
        }
        let range = self
            .0
            .range
            .ok_or(ArchiveError::Corrupt("archive reference range"))?;
        range.first.decode()?;
        let position = range.last.decode()?;
        if range.first.index > range.last.index || self.0.length == 0 || self.0.length > 1024 * 1024
        {
            return Err(ArchiveError::Limit);
        }
        Ok(position)
    }
}

/// Bounded head/manifest metadata without downloading any image, tail or Blob.
/// Payload existence and physical replay remain separate verification gates.
pub struct ArchiveMetadata {
    _retention: Option<Arc<super::budget::Permit>>,
    reference: ArchiveReference,
    scope: GroupScope,
    position: LogPosition,
    manifest: Manifest,
}

impl ArchiveMetadata {
    pub fn reference(&self) -> ArchiveReference {
        self.reference.clone()
    }

    pub fn scope(&self) -> &GroupScope {
        &self.scope
    }

    pub fn position(&self) -> LogPosition {
        self.position
    }

    pub fn schema_digest(&self) -> [u8; 32] {
        self.manifest.schema_digest
    }

    /// Descriptive selected state. None is canonical empty; this accessor
    /// supplies neither physical replay verification nor activation authority.
    pub fn image_state(&self) -> Result<Option<ImageState>, ArchiveError> {
        self.manifest.image.map(Image::decode).transpose()
    }

    pub fn configuration(&self) -> Result<ConfState, ArchiveError> {
        manifest::decode_conf(&self.manifest.snapshot.conf_state)
    }

    pub fn membership(&self) -> super::member_state::GroupMembership {
        self.manifest.snapshot.membership.clone()
    }
}

/// Fully downloaded and hash-checked recovery material. This is not a Raft
/// activation capability or an SDK acknowledgement. `restore` additionally
/// verifies physical application in an exclusively owned generation store.
pub struct ArchiveRecoverySet {
    _retention: Option<Arc<super::budget::Permit>>,
    reference: ArchiveReference,
    scope: GroupScope,
    position: LogPosition,
    manifest: Manifest,
    snapshot: Vec<u8>,
    entries: Vec<Entry>,
    dependencies: Vec<(ObjectRef, Vec<u8>)>,
    envelope: EnvelopeLimits,
    max_image_bytes: u64,
}

impl CellArchive {
    pub(crate) fn new(
        store: Arc<dyn ObjectStore>,
        archive_prefix: String,
        birth: &super::catalog::GroupBirth,
        root: [u8; 32],
        schema: [u8; 32],
        limits: ArchiveLimits,
        host: LtxHost,
    ) -> Self {
        let initial = ConfState {
            voters: birth
                .voters()
                .iter()
                .map(|member| member.member.get())
                .collect(),
            learners: birth
                .learners()
                .iter()
                .map(|member| member.member.get())
                .collect(),
            ..Default::default()
        };
        Self {
            store,
            prefix: format!(
                "{}/{}/{}",
                archive_prefix,
                hex::encode(birth.scope().group.bytes()),
                hex::encode(birth.scope().incarnation.bytes())
            ),
            bundle_prefix: format!("{archive_prefix}/bundles"),
            scope: birth.scope().clone(),
            root,
            birth: birth.digest(),
            initial_membership: super::member_state::GroupMembership::initial(&initial),
            schema,
            limits,
            host,
            cancellation: None,
            io_domain: crate::asyncrt::runtime_services().domain_token(),
            read_retention: None,
            bundle_client: None,
            verification_deadline: None,
        }
    }

    pub(crate) fn can_batch_with(&self, other: &Self) -> bool {
        self.bundle_prefix == other.bundle_prefix
            && self.root == other.root
            && self.schema == other.schema
            && self.scope.cluster == other.scope.cluster
            && self.scope.namespace == other.scope.namespace
            && Arc::ptr_eq(&self.store, &other.store)
    }

    pub(crate) fn read_memory_bound(&self) -> usize {
        self.limits.max_set_bytes
            + READ_WINDOW * bundle::MAX_BYTES.min(self.limits.max_object_bytes)
    }

    pub(crate) fn with_read_retention(mut self, retention: Arc<super::budget::Permit>) -> Self {
        self.read_retention = Some(retention);
        self
    }

    pub(crate) fn with_cancellation(
        mut self,
        cancellation: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        self.cancellation = Some(cancellation);
        self
    }

    async fn io<T>(
        &self,
        deadline: u64,
        future: impl std::future::Future<Output = object_store::Result<T>>,
    ) -> Result<T, ArchiveError> {
        let timer = self
            .io_domain
            .as_ref()
            .ok_or(ArchiveError::Stopped)?
            .sleep_until(deadline)
            .map_err(|_| ArchiveError::Stopped)?;
        let cancelled = async {
            if let Some(mut cancellation) = self.cancellation.clone() {
                loop {
                    if *cancellation.borrow_and_update() {
                        break;
                    }
                    if cancellation.changed().await.is_err() {
                        break;
                    }
                }
            } else {
                std::future::pending::<()>().await;
            }
        };
        crate::asyncrt::select_biased! { "archive shutdown refuses further object-store work before the owned producer is joined";
            _ = cancelled => Err(ArchiveError::Stopped),
            result = future => result.map_err(ArchiveError::Storage),
            _ = timer => Err(ArchiveError::Timeout),
        }
    }

    fn deadline(&self) -> Result<u64, ArchiveError> {
        let now = self
            .io_domain
            .as_ref()
            .ok_or(ArchiveError::Stopped)?
            .mono_ms()
            .map_err(|_| ArchiveError::Stopped)?;
        let deadline = now.saturating_add(self.limits.io_timeout_ms);
        match self.verification_deadline {
            Some(operation) if now >= operation => Err(ArchiveError::Timeout),
            Some(operation) => Ok(deadline.min(operation)),
            None => Ok(deadline),
        }
    }

    fn key(&self, name: &str) -> Path {
        Path::from(format!("{}/{name}", self.prefix))
    }

    /// Publish only after every object has been read back and checked. CAS
    /// failures (including lost success responses) are reconciled with exact
    /// head reads; bounded contention never falls back to unconditional writes.
    pub async fn publish(
        &self,
        cut: CommittedArchiveCut,
        verifier: &ArchiveVerifier,
    ) -> Result<ArchiveRecoverySet, ArchiveError> {
        if cut.scope != self.scope
            || cut.schema_digest != self.schema
            || cut.birth_digest != self.birth
            || !cut.membership.same_birth(&self.initial_membership)
        {
            return Err(ArchiveError::Invalid("archive producer history/schema"));
        }
        for _ in 0..8 {
            let previous = self.load_head_metadata().await?;
            if let Some((metadata, _, _)) = &previous {
                let old = metadata.manifest.position;
                if old.index >= cut.position.index.get() {
                    if old.term < cut.position.term.get()
                        || old.index == cut.position.index.get()
                            && (old.term != cut.position.term.get()
                                || metadata.manifest.image != cut.image_state()
                                || metadata.manifest.snapshot.conf_state != cut.conf_state
                                || metadata.manifest.snapshot.membership != cut.membership)
                    {
                        return Err(ArchiveError::Conflict);
                    }
                    let metadata = previous
                        .map(|(metadata, _, _)| metadata)
                        .ok_or(ArchiveError::Unconfirmed)?;
                    return self
                        .verify_set(self.load_material(metadata).await?, verifier)
                        .await;
                }
                if metadata.manifest.image.is_some() && cut.image.is_none() {
                    return Err(ArchiveError::Conflict);
                }
                if old.term > cut.position.term.get() {
                    return Err(ArchiveError::Conflict);
                }
            }
            let tail = previous.as_ref().and_then(|(metadata, _, _)| {
                // Empty configuration archives have no data base or tails.
                // The first real SQL image always establishes a full snapshot.
                let image = cut.image.as_ref()?;
                let snapshot_object = metadata.manifest.snapshot.object.as_ref()?;
                let next = metadata.manifest.position.index.checked_add(1)?;
                let entries: Vec<_> = cut
                    .entries
                    .iter()
                    .filter(|e| e.index >= next)
                    .cloned()
                    .collect();
                // Validated metadata is sufficient to plan an extension. Its
                // complete retained payloads are downloaded, decoded and
                // physically replayed below before the new head can advance.
                let count = metadata
                    .manifest
                    .tails
                    .iter()
                    .try_fold(entries.len(), |count, tail| count.checked_add(tail.entries))?;
                // Bound repeated tail reads/replay relative to the current
                // complete image as well as the absolute recovery-set budget.
                // The 44-byte framing and each length prefix match encode_tail.
                let mut tail_bytes = 44u64;
                for tail in &metadata.manifest.tails {
                    tail_bytes = tail_bytes.checked_add(tail.object.length)?;
                }
                for entry in &entries {
                    tail_bytes = tail_bytes.checked_add(u64::from(entry.compute_size()) + 4)?;
                }
                let image_budget = image
                    .pin()
                    .state()
                    .length
                    .checked_mul(u64::from(self.limits.max_tail_image_ratio))?;
                // Dependencies retained by the old base may be released only
                // by a new snapshot, whose prior material is checked below.
                let mut bytes = snapshot_object.length.checked_add(tail_bytes)?;
                for dependency in &metadata.manifest.dependencies {
                    bytes = bytes.checked_add(dependency.length)?;
                }
                for dependency in &cut.dependencies {
                    bytes = bytes.checked_add(dependency.0.len() as u64)?;
                }
                (entries.first().is_some_and(|e| e.index == next)
                    && entries
                        .last()
                        .is_some_and(|e| e.index == cut.position.index.get())
                    && entries
                        .iter()
                        .all(|e| e.get_entry_type() == EntryType::EntryNormal)
                    && metadata.manifest.snapshot.conf_state == cut.conf_state
                    && metadata.manifest.snapshot.membership == cut.membership
                    && metadata.manifest.tails.len() < self.limits.max_tails
                    && bytes <= self.limits.max_set_bytes as u64
                    && count <= self.limits.max_entries)
                    .then_some((entries, tail_bytes > image_budget))
            });
            let mode = previous
                .as_ref()
                .map_or(PutMode::Create, |(_, _, version)| {
                    PutMode::Update(version.clone())
                });
            let mut manifest = match (tail, previous) {
                (Some((entries, true)), Some((metadata, _, _))) => {
                    // A cost-based checkpoint replaces an otherwise valid
                    // extension. Verify every intermediate transition before
                    // discarding those tails, including an update overwritten
                    // by a later entry in this capture.
                    let previous = self.load_material(metadata).await?;
                    self.verify_extension(previous, entries, &cut, verifier)
                        .await?;
                    self.snapshot_manifest(&cut).await?
                }
                (Some((entries, false)), Some((metadata, _, _))) => {
                    let first = entries
                        .first()
                        .ok_or(ArchiveError::Corrupt("empty archive tail"))?;
                    let last = entries
                        .last()
                        .ok_or(ArchiveError::Corrupt("empty archive tail"))?;
                    let first = Position {
                        index: first.index,
                        term: first.term,
                    };
                    let last = Position {
                        index: last.index,
                        term: last.term,
                    };
                    let count = entries.len();
                    let limits = self.limits;
                    let body = self
                        .host
                        .run_blocking(move || encode_tail(&entries, limits))
                        .await
                        .map_err(ArchiveError::Worker)??;
                    let object = self
                        .put_object(ObjectKind::Tail, &body, Some(PositionRange { first, last }))
                        .await?;
                    let mut manifest = metadata.manifest;
                    manifest.tails.push(Tail {
                        first,
                        last,
                        entries: count,
                        object,
                    });
                    manifest.position = cut.position.into();
                    manifest.image = cut.image_state();
                    manifest.image_position =
                        cut.image.as_ref().map(|image| image.position().into());
                    manifest
                }
                (_, previous) => {
                    // A new base omits the old payloads from final verification.
                    // Preserve refusal of missing/corrupt prior material rather
                    // than silently repairing an invalid selected head here.
                    if let Some((metadata, _, _)) = previous {
                        self.load_material(metadata).await?;
                    }
                    self.snapshot_manifest(&cut).await?
                }
            };
            let mut dependencies: BTreeMap<_, _> = manifest
                .dependencies
                .iter()
                .cloned()
                .map(|r| (r.digest, r))
                .collect();
            for dependency in &cut.dependencies {
                let object = self
                    .object_reference(ObjectKind::Dependency, &dependency.0, None)
                    .await?;
                if let Some(old) = dependencies.get(&object.digest) {
                    if old != &object {
                        return Err(ArchiveError::Conflict);
                    }
                    // Final verification must read the retained object. A new
                    // PUT here could repair a missing prior dependency and
                    // hide corruption of the selected recovery history.
                    continue;
                }
                let object = self.put_referenced_object(object, &dependency.0).await?;
                dependencies.insert(object.digest, object);
            }
            manifest.dependencies = dependencies.into_values().collect();
            manifest.validate(
                &self.scope,
                self.root,
                self.birth,
                self.schema,
                self.limits,
                &self.initial_membership,
            )?;
            let bytes = manifest::encode(1, &manifest, self.limits.max_manifest_bytes)?;
            let object = self
                .put_object(
                    ObjectKind::Manifest,
                    &bytes,
                    Some(PositionRange {
                        first: manifest.snapshot.position,
                        last: manifest.position,
                    }),
                )
                .await?;
            // Do not expose a head with a missing dependency, broken coverage
            // or an invalid LTX mapping, even if all PUTs reported success.
            let set = self
                .verify_set(self.load_manifest(&object).await?, verifier)
                .await?;
            let desired = manifest::encode(
                2,
                &Head {
                    manifest: object,
                    position: manifest.position,
                },
                4096,
            )?;
            let outcome = self
                .io(
                    self.deadline()?,
                    self.store.put_opts(
                        &self.key("head"),
                        desired.clone().into(),
                        PutOptions::from(mode),
                    ),
                )
                .await;
            match self.read(&self.key("head"), 4096, None).await? {
                Some((observed, _)) if observed == desired => return Ok(set),
                Some(_) => continue,
                None => {
                    return match outcome {
                        Err(error) => Err(error),
                        Ok(_) => Err(ArchiveError::Unconfirmed),
                    }
                }
            }
        }
        Err(ArchiveError::Unconfirmed)
    }

    async fn snapshot_manifest(&self, cut: &CommittedArchiveCut) -> Result<Manifest, ArchiveError> {
        let position = cut.position.into();
        let object = if cut.image.is_some() {
            let input = cut.clone();
            let limits = self.limits;
            let body = self
                .host
                .run_blocking(move || input.snapshot(limits))
                .await
                .map_err(ArchiveError::Worker)??;
            Some(
                self.put_object(
                    ObjectKind::Snapshot,
                    &body,
                    Some(PositionRange {
                        first: position,
                        last: position,
                    }),
                )
                .await?,
            )
        } else {
            None
        };
        Ok(Manifest {
            scope: (&self.scope).into(),
            root_digest: self.root,
            birth_digest: self.birth,
            schema_digest: self.schema,
            snapshot: Snapshot {
                position: cut.position.into(),
                image: cut.image_state(),
                conf_state: cut.conf_state.clone(),
                membership: cut.membership.clone(),
                object,
            },
            tails: Vec::new(),
            position: cut.position.into(),
            image: cut.image_state(),
            image_position: cut.image.as_ref().map(|image| image.position().into()),
            dependencies: Vec::new(),
        })
    }

    async fn verify_extension(
        &self,
        mut previous: ArchiveRecoverySet,
        entries: Vec<Entry>,
        cut: &CommittedArchiveCut,
        verifier: &ArchiveVerifier,
    ) -> Result<(), ArchiveError> {
        let position = cut.position;
        let image = cut.image_state();
        let image_position = cut.image.as_ref().map(|image| image.position().into());
        let verifier = verifier.clone();
        self.host
            .run_blocking(move || {
                // This private replay input is consumed here. Its old object
                // reference is never returned as a proof for the new position.
                // The planner already bounded total encoded bytes/entries and
                // required contiguous normal entries and identical membership.
                previous.entries.extend(entries);
                previous.position = position;
                previous.manifest.position = position.into();
                previous.manifest.image = image;
                previous.manifest.image_position = image_position;
                previous.validate_payloads()?;
                verifier.verify(&previous)
            })
            .await
            .map_err(ArchiveError::Worker)?
    }

    async fn verify_set(
        &self,
        set: ArchiveRecoverySet,
        verifier: &ArchiveVerifier,
    ) -> Result<ArchiveRecoverySet, ArchiveError> {
        let verifier = verifier.clone();
        self.host
            .run_blocking(move || {
                verifier.verify(&set)?;
                Ok(set)
            })
            .await
            .map_err(ArchiveError::Worker)?
    }

    pub async fn recovery_set(&self) -> Result<Option<ArchiveRecoverySet>, ArchiveError> {
        Ok(self.load_head().await?.map(|(set, _, _)| set))
    }

    /// Select the immutable head reference without downloading its payloads.
    /// A caller authorizing later recovery must bind this exact reference into
    /// its own live-quorum operation; rereading head could select another cut.
    pub async fn recovery_metadata(&self) -> Result<Option<ArchiveMetadata>, ArchiveError> {
        Ok(self
            .load_head_metadata()
            .await?
            .map(|(metadata, _, _)| metadata))
    }

    /// Download the exact history-bound immutable set. This never falls back
    /// to a newer mutable head if the reference is missing, corrupt or stale.
    /// It supplies no voting, activation, or quorum proof.
    pub async fn recovery_set_at(
        &self,
        reference: &ArchiveReference,
    ) -> Result<ArchiveRecoverySet, ArchiveError> {
        self.load_manifest(&reference.0).await
    }

    async fn load_head(
        &self,
    ) -> Result<Option<(ArchiveRecoverySet, Vec<u8>, UpdateVersion)>, ArchiveError> {
        let Some((metadata, bytes, version)) = self.load_head_metadata().await? else {
            return Ok(None);
        };
        Ok(Some((self.load_material(metadata).await?, bytes, version)))
    }

    async fn load_head_metadata(
        &self,
    ) -> Result<Option<(ArchiveMetadata, Vec<u8>, UpdateVersion)>, ArchiveError> {
        let Some((bytes, version)) = self.read(&self.key("head"), 4096, None).await? else {
            return Ok(None);
        };
        if version.e_tag.is_none() && version.version.is_none() {
            return Err(ArchiveError::Invalid(
                "archive head lacks a conditional-write version",
            ));
        }
        let head: Head = manifest::decode(2, &bytes, 4096)?;
        let metadata = self.load_metadata(&head.manifest).await?;
        if metadata.manifest.position != head.position {
            return Err(ArchiveError::Corrupt("archive head/cut binding"));
        }
        Ok(Some((metadata, bytes, version)))
    }

    async fn load_metadata(&self, object: &ObjectRef) -> Result<ArchiveMetadata, ArchiveError> {
        if object.kind != ObjectKind::Manifest || object.bundle.is_some() {
            return Err(ArchiveError::Corrupt("archive head domain"));
        }
        let bytes = self.get_object(object).await?;
        let manifest: Manifest = manifest::decode(1, &bytes, self.limits.max_manifest_bytes)?;
        manifest.validate(
            &self.scope,
            self.root,
            self.birth,
            self.schema,
            self.limits,
            &self.initial_membership,
        )?;
        if object.range
            != Some(PositionRange {
                first: manifest.snapshot.position,
                last: manifest.position,
            })
        {
            return Err(ArchiveError::Corrupt("archive manifest object range"));
        }
        Ok(ArchiveMetadata {
            _retention: self.read_retention.clone(),
            reference: ArchiveReference(object.clone()),
            scope: self.scope.clone(),
            position: manifest.position.decode()?,
            manifest,
        })
    }

    async fn load_manifest(&self, object: &ObjectRef) -> Result<ArchiveRecoverySet, ArchiveError> {
        self.load_material(self.load_metadata(object).await?).await
    }

    async fn load_material(
        &self,
        metadata: ArchiveMetadata,
    ) -> Result<ArchiveRecoverySet, ArchiveError> {
        let ArchiveMetadata {
            reference,
            manifest,
            _retention,
            ..
        } = metadata;
        // The validated manifest bounds the sum of these exact object lengths.
        // Reads enforce each declared length before allocating its body. A
        // small ordered window overlaps I/O. The read reservation also includes
        // one full containing bundle per slot until physical extraction ends.
        let objects = manifest
            .snapshot
            .object
            .iter()
            .chain(manifest.tails.iter().map(|tail| &tail.object))
            .chain(manifest.dependencies.iter());
        let reads = objects
            .map(|object| self.get_object(object))
            .collect::<Vec<_>>();
        let mut bodies = futures_util::stream::iter(reads)
            .buffered(READ_WINDOW)
            .try_collect::<Vec<_>>()
            .await?
            .into_iter();
        let snapshot = if manifest.snapshot.object.is_some() {
            bodies
                .next()
                .ok_or(ArchiveError::Corrupt("missing archive snapshot"))?
        } else {
            Vec::new()
        };
        let tails = bodies
            .by_ref()
            .take(manifest.tails.len())
            .collect::<Vec<_>>();
        let dependencies = manifest.dependencies.iter().cloned().zip(bodies).collect();
        let scope = self.scope.clone();
        let limits = self.limits;
        self.host
            .run_blocking(move || {
                let mut entries = Vec::new();
                for (tail, body) in manifest.tails.iter().zip(tails) {
                    let decoded = decode_tail(&body, limits)?;
                    if decoded.len() != tail.entries
                        || decoded.first().map(|e| (e.index, e.term))
                            != Some((tail.first.index, tail.first.term))
                        || decoded.last().map(|e| (e.index, e.term))
                            != Some((tail.last.index, tail.last.term))
                    {
                        return Err(ArchiveError::Corrupt("archive tail/manifest binding"));
                    }
                    entries.extend(decoded);
                }
                let position = manifest.position.decode()?;
                let set = ArchiveRecoverySet {
                    _retention,
                    reference,
                    scope,
                    position,
                    manifest,
                    snapshot,
                    entries,
                    dependencies,
                    envelope: limits.envelope,
                    max_image_bytes: limits.max_image_bytes,
                };
                set.validate_payloads()?;
                Ok(set)
            })
            .await
            .map_err(ArchiveError::Worker)?
    }

    async fn put_object(
        &self,
        kind: ObjectKind,
        bytes: &[u8],
        range: Option<PositionRange>,
    ) -> Result<ObjectRef, ArchiveError> {
        let object = self.object_reference(kind, bytes, range).await?;
        self.put_referenced_object(object, bytes).await
    }

    async fn object_reference(
        &self,
        kind: ObjectKind,
        bytes: &[u8],
        range: Option<PositionRange>,
    ) -> Result<ObjectRef, ArchiveError> {
        if bytes.len() > self.limits.max_object_bytes {
            return Err(ArchiveError::Limit);
        }
        let owned = bytes.to_vec();
        self.host
            .run_blocking(move || ObjectRef::new(kind, &owned, range))
            .await
            .map_err(ArchiveError::Worker)
    }

    async fn put_referenced_object(
        &self,
        object: ObjectRef,
        bytes: &[u8],
    ) -> Result<ObjectRef, ArchiveError> {
        if matches!(object.kind, ObjectKind::Snapshot | ObjectKind::Tail) {
            if let Some(client) = &self.bundle_client {
                return client.put(self, object, bytes.to_vec()).await;
            }
        }
        self.put_direct(object, bytes.to_vec()).await
    }

    async fn put_direct(
        &self,
        object: ObjectRef,
        bytes: Vec<u8>,
    ) -> Result<ObjectRef, ArchiveError> {
        let path = self.key(&object.name());
        let result = self
            .io(
                self.deadline()?,
                self.store
                    .put_opts(&path, bytes.into(), PutOptions::from(PutMode::Create)),
            )
            .await;
        match self
            .read(&path, self.limits.max_object_bytes, Some(object.length))
            .await?
        {
            Some((observed, _)) => self
                .host
                .run_blocking(move || {
                    object.verify(&observed)?;
                    Ok(object)
                })
                .await
                .map_err(ArchiveError::Worker)?,
            None => match result {
                Err(error) => Err(error),
                Ok(_) => Err(ArchiveError::Unconfirmed),
            },
        }
    }

    async fn get_object(&self, object: &ObjectRef) -> Result<Vec<u8>, ArchiveError> {
        object.validate_location()?;
        let limit = if object.kind == ObjectKind::Manifest {
            self.limits.max_manifest_bytes
        } else {
            self.limits.max_object_bytes
        };
        if object.length > limit as u64 {
            return Err(ArchiveError::Limit);
        }
        if let Some(reference) = &object.bundle {
            if reference.length > self.limits.max_object_bytes as u64 {
                return Err(ArchiveError::Limit);
            }
            let bytes = self
                .read(
                    &self.bundle_key(reference),
                    bundle::MAX_BYTES.min(self.limits.max_object_bytes),
                    Some(reference.length),
                )
                .await?
                .ok_or(ArchiveError::Corrupt("missing archive bundle"))?
                .0;
            let archive = self.clone();
            let expected = object.clone();
            let retention = self.read_retention.clone();
            return self
                .host
                .run_blocking(move || {
                    let _retention = retention;
                    bundle::extract(&archive, &expected, &bytes)
                })
                .await
                .map_err(ArchiveError::Worker)?;
        }
        let bytes = self
            .read(&self.key(&object.name()), limit, Some(object.length))
            .await?
            .ok_or(ArchiveError::Corrupt("missing archive object"))?
            .0;
        let expected = object.clone();
        let retention = self.read_retention.clone();
        self.host
            .run_blocking(move || {
                let _retention = retention;
                expected.verify(&bytes)?;
                Ok(bytes)
            })
            .await
            .map_err(ArchiveError::Worker)?
    }

    async fn read(
        &self,
        path: &Path,
        limit: usize,
        expected_length: Option<u64>,
    ) -> Result<Option<(Vec<u8>, UpdateVersion)>, ArchiveError> {
        let deadline = self.deadline()?;
        let result = match self.io(deadline, self.store.get(path)).await {
            Ok(result) => result,
            Err(ArchiveError::Storage(object_store::Error::NotFound { .. })) => return Ok(None),
            Err(error) => return Err(error),
        };
        if result.meta.size > limit as u64
            || result.range.start != 0
            || result.range.end != result.meta.size
        {
            return Err(ArchiveError::Limit);
        }
        if expected_length.is_some_and(|length| length != result.meta.size) {
            return Err(ArchiveError::Corrupt("archive object digest/length"));
        }
        let length = result.meta.size as usize;
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let mut bytes = Vec::with_capacity(length);
        let mut stream = result.into_stream();
        let mut chunks = 0usize;
        while let Some(chunk) = self.io(deadline, stream.try_next()).await? {
            chunks += 1;
            if chunks > 16384
                || bytes
                    .len()
                    .checked_add(chunk.len())
                    .is_none_or(|n| n > length)
            {
                return Err(ArchiveError::Limit);
            }
            bytes.extend_from_slice(&chunk);
        }
        if bytes.len() != length {
            return Err(ArchiveError::Corrupt("short archive object"));
        }
        Ok(Some((bytes, version)))
    }

    fn bundle_key(&self, reference: &bundle::Reference) -> Path {
        Path::from(format!(
            "{}/{}-{}",
            self.bundle_prefix,
            reference.length,
            hex::encode(reference.digest)
        ))
    }
}

impl ArchiveRecoverySet {
    pub(crate) fn birth_digest(&self) -> [u8; 32] {
        self.manifest.birth_digest
    }

    pub fn reference(&self) -> ArchiveReference {
        self.reference.clone()
    }

    pub fn scope(&self) -> &GroupScope {
        &self.scope
    }

    pub fn position(&self) -> LogPosition {
        self.position
    }

    pub fn configuration(&self) -> Result<ConfState, ArchiveError> {
        manifest::decode_conf(&self.manifest.snapshot.conf_state)
    }

    pub fn membership(&self) -> super::member_state::GroupMembership {
        self.manifest.snapshot.membership.clone()
    }

    pub fn schema_digest(&self) -> [u8; 32] {
        self.manifest.schema_digest
    }

    /// Descriptive selected state. None is canonical empty; this accessor
    /// supplies neither physical replay verification nor activation authority.
    pub fn image_state(&self) -> Result<Option<ImageState>, ArchiveError> {
        self.manifest.image.map(Image::decode).transpose()
    }

    /// Descriptive producer publication origin, including restore aliases.
    /// Physical verification and current quorum remain separate requirements.
    pub fn image_position(&self) -> Result<Option<LogPosition>, ArchiveError> {
        self.manifest
            .image_position
            .map(Position::decode)
            .transpose()
    }

    pub fn tail_entries(&self) -> usize {
        self.entries.len()
    }

    pub fn dependencies(&self) -> impl Iterator<Item = ([u8; 32], &[u8])> {
        self.dependencies
            .iter()
            .map(|(r, bytes)| (r.digest, bytes.as_slice()))
    }

    fn validate_payloads(&self) -> Result<(), ArchiveError> {
        let Some(snapshot_image) = self.manifest.snapshot.image else {
            if self.manifest.snapshot.object.is_some()
                || self.manifest.image.is_some()
                || self.manifest.image_position.is_some()
                || !self.manifest.tails.is_empty()
                || !self.manifest.dependencies.is_empty()
                || !self.snapshot.is_empty()
                || !self.entries.is_empty()
                || !self.dependencies.is_empty()
                || self.manifest.snapshot.position != self.manifest.position
            {
                return Err(ArchiveError::Corrupt("canonical empty archive payload"));
            }
            return Ok(());
        };
        let mut state = snapshot_image.decode()?;
        verify_snapshot(&self.snapshot, state)?;
        let mut position = self.manifest.snapshot.position;
        let mut data_position = None;
        for entry in &self.entries {
            if position.index.checked_add(1) != Some(entry.index)
                || entry.term < position.term
                || entry.get_entry_type() != EntryType::EntryNormal
            {
                return Err(ArchiveError::Corrupt("archive entry coverage/domain"));
            }
            if !entry.data.is_empty() {
                let update = self.update(entry, state)?;
                state = result_state(&update)?;
                data_position = Some(Position {
                    index: entry.index,
                    term: entry.term,
                });
                if state.length > self.max_image_bytes {
                    return Err(ArchiveError::Limit);
                }
            }
            position = Position {
                index: entry.index,
                term: entry.term,
            };
        }
        if position != self.manifest.position || Some(Image::from(state)) != self.manifest.image {
            return Err(ArchiveError::Corrupt("archive final image/cut"));
        }
        let published = self.manifest.image_position.ok_or(ArchiveError::Corrupt(
            "missing archive image publication position",
        ))?;
        if data_position.is_some_and(|data| {
            published.index < data.index
                || published.term < data.term
                || published.index == data.index && published.term != data.term
        }) {
            return Err(ArchiveError::Corrupt("archive publication precedes data"));
        }
        Ok(())
    }

    fn update(&self, entry: &Entry, state: ImageState) -> Result<CellUpdate, ArchiveError> {
        let update = CellUpdate::decode(&entry.data, self.envelope)
            .map_err(|_| ArchiveError::Corrupt("archive LTX envelope"))?;
        if update.scope != self.scope
            || update.application_digest != self.manifest.schema_digest
            || update.base != image_position(state)
        {
            return Err(ArchiveError::Corrupt("archive LTX history/base/schema"));
        }
        Ok(update)
    }
}

fn result_state(update: &CellUpdate) -> Result<ImageState, ArchiveError> {
    let last = update
        .files
        .last()
        .ok_or(ArchiveError::Corrupt("empty archive LTX update"))?;
    let header = ltx::Header::parse(&last.bytes)
        .map_err(|_| ArchiveError::Corrupt("archive LTX geometry"))?;
    Image {
        epoch: update.result.epoch,
        txid: update.result.txid,
        checksum: update.result.checksum,
        page_size: header.page_size,
        length: u64::from(header.commit) * u64::from(header.page_size),
        digest: update.result.image_digest,
    }
    .decode()
}

pub(crate) fn apply_limits() -> ApplyLimits {
    super::snapshot::SnapshotLimits {
        max_image_bytes: 256 * 1024 * 1024,
        max_encoded_bytes: 512 * 1024 * 1024,
        ..Default::default()
    }
    .apply()
}

fn verify_snapshot(bytes: &[u8], state: ImageState) -> Result<(), ArchiveError> {
    let mut limits = apply_limits().decode;
    // A compressed object cannot expand beyond its manifest-bound image even
    // when the reader implementation supports larger configured profiles.
    limits.max_image_bytes = state.length;
    limits.max_decoded_bytes = state.length;
    limits.max_pages = state.length.div_ceil(512) as usize;
    let file = ltx::decode_file_bounded(bytes, limits)
        .map_err(|_| ArchiveError::Corrupt("archive snapshot LTX"))?;
    let header = file.header();
    if !header.is_snapshot()
        || header.max_txid != state.txid
        || header.page_size != state.page_size
        || u64::from(header.commit) * u64::from(header.page_size) != state.length
        || file.trailer().post_apply_checksum != state.checksum
    {
        return Err(ArchiveError::Corrupt("archive snapshot/image binding"));
    }
    let mut hash = Sha256::new();
    for (_, page) in file.pages() {
        hash.update(page);
    }
    // Archive images are bounded below SQLite's 1-GiB reserved lock page.
    if <[u8; 32]>::from(hash.finalize()) != state.digest {
        return Err(ArchiveError::Corrupt("archive snapshot image digest"));
    }
    Ok(())
}

fn encode_tail(entries: &[Entry], limits: ArchiveLimits) -> Result<Vec<u8>, ArchiveError> {
    if entries.is_empty() || entries.len() > limits.max_entries {
        return Err(ArchiveError::Limit);
    }
    let mut bytes = b"CLRTAL01".to_vec();
    bytes.extend_from_slice(&(entries.len() as u32).to_be_bytes());
    for entry in entries {
        let body = entry
            .write_to_bytes()
            .map_err(|_| ArchiveError::Corrupt("archive entry encoding"))?;
        if bytes
            .len()
            .checked_add(body.len() + 36)
            .is_none_or(|n| n > limits.max_object_bytes)
        {
            return Err(ArchiveError::Limit);
        }
        bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&body);
    }
    bytes.extend_from_slice(&manifest::digest(&bytes));
    Ok(bytes)
}

fn decode_tail(bytes: &[u8], limits: ArchiveLimits) -> Result<Vec<Entry>, ArchiveError> {
    if bytes.len() > limits.max_object_bytes {
        return Err(ArchiveError::Limit);
    }
    if bytes.len() < 44
        || &bytes[..8] != b"CLRTAL01"
        || manifest::digest(&bytes[..bytes.len() - 32])[..] != bytes[bytes.len() - 32..]
    {
        return Err(ArchiveError::Corrupt("archive tail framing/digest"));
    }
    let number = |at: usize| -> Result<usize, ArchiveError> {
        Ok(u32::from_be_bytes(
            bytes
                .get(at..at + 4)
                .ok_or(ArchiveError::Corrupt("archive tail length"))?
                .try_into()
                .map_err(|_| ArchiveError::Corrupt("archive tail length"))?,
        ) as usize)
    };
    let count = number(8)?;
    if count == 0 || count > limits.max_entries {
        return Err(ArchiveError::Limit);
    }
    let mut offset = 12usize;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let length = number(offset)?;
        offset += 4;
        let end = offset
            .checked_add(length)
            .filter(|end| *end <= bytes.len() - 32)
            .ok_or(ArchiveError::Corrupt("archive entry length"))?;
        let entry = Entry::parse_from_bytes(&bytes[offset..end])
            .map_err(|_| ArchiveError::Corrupt("archive entry protobuf"))?;
        if entry.get_unknown_fields().iter().next().is_some() {
            return Err(ArchiveError::Corrupt("unknown archive entry fields"));
        }
        entries.push(entry);
        offset = end;
    }
    if offset != bytes.len() - 32 || encode_tail(&entries, limits)? != bytes {
        return Err(ArchiveError::Corrupt("noncanonical archive tail"));
    }
    Ok(entries)
}
