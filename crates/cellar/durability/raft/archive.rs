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
use cellar_ltx::apply::{ApplyLimits, ImageState, ImageUpdate, LtxSegment};
use cellar_ltx::generation::{GenerationError, GenerationPin, GenerationStore, ImageId};
use cellar_ltx::{ltx, LtxHost, TXID};
use futures_util::TryStreamExt;
use object_store::{path::Path, ObjectStore, PutMode, PutOptions, UpdateVersion};
use protobuf::Message;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};

#[derive(Debug)]
pub enum ArchiveError {
    Invalid(&'static str),
    Corrupt(&'static str),
    Conflict,
    Limit,
    Unconfirmed,
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
    pub max_manifest_bytes: usize,
    pub max_object_bytes: usize,
    pub max_set_bytes: usize,
    pub max_entries: usize,
    pub max_tails: usize,
    pub max_dependencies: usize,
}

impl Default for ArchiveLimits {
    fn default() -> Self {
        Self {
            max_manifest_bytes: 256 * 1024,
            max_object_bytes: 64 * 1024 * 1024,
            max_set_bytes: 128 * 1024 * 1024,
            max_entries: 1024,
            max_tails: 32,
            max_dependencies: 128,
        }
    }
}

impl ArchiveLimits {
    pub fn validate(self) -> Result<(), ArchiveError> {
        if !(1024..=1024 * 1024).contains(&self.max_manifest_bytes)
            || self.max_manifest_bytes > self.max_object_bytes
            || !(1024..=64 * 1024 * 1024).contains(&self.max_object_bytes)
            || self.max_set_bytes < self.max_object_bytes
            || self.max_set_bytes > 256 * 1024 * 1024
            || !(1..=4096).contains(&self.max_entries)
            || !(1..=128).contains(&self.max_tails)
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
        let mut state = self
            .inner
            .lock()
            .map_err(|_| ArchiveError::Invalid("archive verifier owner is poisoned"))?;
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
        let pin = set.restore(&state.images, nonce)?;
        state
            .images
            .retire(pin.id())
            .map_err(ArchiveError::Images)?;
        drop(pin);
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
    image: PublishedImage,
    birth_digest: [u8; 32],
    position: LogPosition,
    conf_state: Vec<u8>,
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
        limits.validate()?;
        if image.scope() != &group.identity().scope || dependencies.len() > limits.max_dependencies
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
        if applied.image.as_ref() != Some(&marker(&image.pin()))
            || applied.position.index < image.position().index
            || applied.position.term < image.position().term
            || applied.position.index.get() > cache.hard_state.commit
            || cache.term(image.position().index.get()) != Some(image.position().term.get())
        {
            return Err(ArchiveError::Invalid("archive cut is not durably applied"));
        }
        // ConfState is persisted atomically with the applied marker. Protocol
        // entries may advance it while retaining the exact same image.
        let position = applied.position;
        let conf_state = manifest::encode_conf(&cache.conf_state)?;
        let mut entries = Vec::new();
        let mut bytes = 0usize;
        for (_, entry) in cache
            .entries
            .range(..=position.index.get())
            .rev()
            .take(limits.max_entries)
        {
            let next = bytes
                .checked_add(entry.compute_size() as usize + 4)
                .ok_or(ArchiveError::Limit)?;
            if next > limits.max_object_bytes.saturating_sub(48) {
                break;
            }
            bytes = next;
            entries.push(entry.clone());
        }
        entries.reverse();
        Ok(Self {
            image,
            birth_digest: group.birth_digest,
            position,
            conf_state,
            entries,
            dependencies,
        })
    }

    pub fn position(&self) -> LogPosition {
        self.position
    }

    fn snapshot(&self, limits: ArchiveLimits) -> Result<Vec<u8>, ArchiveError> {
        let pin = self.image.pin();
        let state = pin.state();
        Image::from(state).decode()?;
        if state.length > limits.max_object_bytes as u64 {
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
    scope: GroupScope,
    root: [u8; 32],
    birth: [u8; 32],
    schema: [u8; 32],
    limits: ArchiveLimits,
    host: LtxHost,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Head {
    manifest: ObjectRef,
    position: Position,
}

/// Fully downloaded and hash-checked recovery material. This is not a Raft
/// activation capability or an SDK acknowledgement. `restore` additionally
/// verifies physical application in an exclusively owned generation store.
pub struct ArchiveRecoverySet {
    scope: GroupScope,
    position: LogPosition,
    manifest: Manifest,
    snapshot: Vec<u8>,
    entries: Vec<Entry>,
    dependencies: Vec<(ObjectRef, Vec<u8>)>,
}

impl CellArchive {
    pub(crate) fn new(
        store: Arc<dyn ObjectStore>,
        prefix: String,
        birth: &super::catalog::GroupBirth,
        root: [u8; 32],
        schema: [u8; 32],
        limits: ArchiveLimits,
        host: LtxHost,
    ) -> Self {
        Self {
            store,
            prefix,
            scope: birth.scope().clone(),
            root,
            birth: birth.digest(),
            schema,
            limits,
            host,
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
        if cut.image.scope() != &self.scope
            || cut.image.application_digest() != self.schema
            || cut.birth_digest != self.birth
        {
            return Err(ArchiveError::Invalid("archive producer history/schema"));
        }
        for _ in 0..8 {
            let previous = self.load_head().await?;
            if let Some((set, _, _)) = &previous {
                let old = set.manifest.position;
                if old.index >= cut.position.index.get() {
                    if old.term < cut.position.term.get()
                        || old.index == cut.position.index.get()
                            && (old.term != cut.position.term.get()
                                || set.manifest.image != Image::from(cut.image.pin().state())
                                || set.manifest.snapshot.conf_state != cut.conf_state)
                    {
                        return Err(ArchiveError::Conflict);
                    }
                    let set = previous
                        .map(|(set, _, _)| set)
                        .ok_or(ArchiveError::Unconfirmed)?;
                    return self.verify_set(set, verifier).await;
                }
                if old.term > cut.position.term.get() {
                    return Err(ArchiveError::Conflict);
                }
            }
            let tail = previous.as_ref().and_then(|(set, _, _)| {
                let next = set.manifest.position.index.checked_add(1)?;
                let entries: Vec<_> = cut
                    .entries
                    .iter()
                    .filter(|e| e.index >= next)
                    .cloned()
                    .collect();
                let count = set.entries.len().checked_add(entries.len())?;
                // Rotate the base before a growing tail makes the complete
                // recovery set exceed its byte budget. Dependencies retained
                // by the old base may be released only by this new snapshot.
                let mut bytes = set.manifest.snapshot.object.length;
                for tail in &set.manifest.tails {
                    bytes = bytes.checked_add(tail.object.length)?;
                }
                for dependency in &set.manifest.dependencies {
                    bytes = bytes.checked_add(dependency.length)?;
                }
                for entry in &entries {
                    bytes = bytes.checked_add(u64::from(entry.compute_size()) + 4)?;
                }
                bytes = bytes.checked_add(44)?;
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
                    && set.manifest.snapshot.conf_state == cut.conf_state
                    && set.manifest.tails.len() < self.limits.max_tails
                    && bytes <= self.limits.max_set_bytes as u64
                    && count <= self.limits.max_entries)
                    .then_some(entries)
            });
            let mut manifest = if let (Some(entries), Some((set, _, _))) = (tail, &previous) {
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
                let mut manifest = set.manifest.clone();
                manifest.tails.push(Tail {
                    first,
                    last,
                    entries: count,
                    object,
                });
                manifest.position = cut.position.into();
                manifest.image = cut.image.pin().state().into();
                manifest
            } else {
                let input = cut.clone();
                let limits = self.limits;
                let body = self
                    .host
                    .run_blocking(move || input.snapshot(limits))
                    .await
                    .map_err(ArchiveError::Worker)??;
                let position = cut.position.into();
                let object = self
                    .put_object(
                        ObjectKind::Snapshot,
                        &body,
                        Some(PositionRange {
                            first: position,
                            last: position,
                        }),
                    )
                    .await?;
                Manifest {
                    scope: (&self.scope).into(),
                    root_digest: self.root,
                    birth_digest: self.birth,
                    schema_digest: self.schema,
                    snapshot: Snapshot {
                        position: cut.position.into(),
                        image: cut.image.pin().state().into(),
                        conf_state: cut.conf_state.clone(),
                        object,
                    },
                    tails: Vec::new(),
                    position: cut.position.into(),
                    image: cut.image.pin().state().into(),
                    dependencies: Vec::new(),
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
                    .put_object(ObjectKind::Dependency, &dependency.0, None)
                    .await?;
                if dependencies
                    .insert(object.digest, object.clone())
                    .is_some_and(|old| old != object)
                {
                    return Err(ArchiveError::Conflict);
                }
            }
            manifest.dependencies = dependencies.into_values().collect();
            manifest.validate(&self.scope, self.root, self.birth, self.schema, self.limits)?;
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
            let mode = previous
                .as_ref()
                .map_or(PutMode::Create, |(_, _, version)| {
                    PutMode::Update(version.clone())
                });
            let outcome = self
                .store
                .put_opts(
                    &self.key("head"),
                    desired.clone().into(),
                    PutOptions::from(mode),
                )
                .await;
            match self.read(&self.key("head"), 4096).await? {
                Some((observed, _)) if observed == desired => return Ok(set),
                Some(_) => continue,
                None => {
                    return match outcome {
                        Err(error) => Err(ArchiveError::Storage(error)),
                        Ok(_) => Err(ArchiveError::Unconfirmed),
                    }
                }
            }
        }
        Err(ArchiveError::Unconfirmed)
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

    async fn load_head(
        &self,
    ) -> Result<Option<(ArchiveRecoverySet, Vec<u8>, UpdateVersion)>, ArchiveError> {
        let Some((bytes, version)) = self.read(&self.key("head"), 4096).await? else {
            return Ok(None);
        };
        if version.e_tag.is_none() && version.version.is_none() {
            return Err(ArchiveError::Invalid(
                "archive head lacks a conditional-write version",
            ));
        }
        let head: Head = manifest::decode(2, &bytes, 4096)?;
        let set = self.load_manifest(&head.manifest).await?;
        if set.manifest.position != head.position {
            return Err(ArchiveError::Corrupt("archive head/cut binding"));
        }
        Ok(Some((set, bytes, version)))
    }

    async fn load_manifest(&self, object: &ObjectRef) -> Result<ArchiveRecoverySet, ArchiveError> {
        if object.kind != ObjectKind::Manifest {
            return Err(ArchiveError::Corrupt("archive head domain"));
        }
        let bytes = self.get_object(object).await?;
        let manifest: Manifest = manifest::decode(1, &bytes, self.limits.max_manifest_bytes)?;
        manifest.validate(&self.scope, self.root, self.birth, self.schema, self.limits)?;
        if object.range
            != Some(PositionRange {
                first: manifest.snapshot.position,
                last: manifest.position,
            })
        {
            return Err(ArchiveError::Corrupt("archive manifest object range"));
        }
        let snapshot = self.get_object(&manifest.snapshot.object).await?;
        let mut tails = Vec::new();
        for tail in &manifest.tails {
            let body = self.get_object(&tail.object).await?;
            tails.push(body);
        }
        let mut dependencies = Vec::new();
        for dependency in &manifest.dependencies {
            dependencies.push((dependency.clone(), self.get_object(dependency).await?));
        }
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
                    scope,
                    position,
                    manifest,
                    snapshot,
                    entries,
                    dependencies,
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
        if bytes.len() > self.limits.max_object_bytes {
            return Err(ArchiveError::Limit);
        }
        let owned = bytes.to_vec();
        let object = self
            .host
            .run_blocking(move || ObjectRef::new(kind, &owned, range))
            .await
            .map_err(ArchiveError::Worker)?;
        let path = self.key(&object.name());
        let result = self
            .store
            .put_opts(
                &path,
                bytes.to_vec().into(),
                PutOptions::from(PutMode::Create),
            )
            .await;
        match self.read(&path, self.limits.max_object_bytes).await? {
            Some((observed, _)) => self
                .host
                .run_blocking(move || {
                    object.verify(&observed)?;
                    Ok(object)
                })
                .await
                .map_err(ArchiveError::Worker)?,
            None => match result {
                Err(error) => Err(ArchiveError::Storage(error)),
                Ok(_) => Err(ArchiveError::Unconfirmed),
            },
        }
    }

    async fn get_object(&self, object: &ObjectRef) -> Result<Vec<u8>, ArchiveError> {
        let limit = if object.kind == ObjectKind::Manifest {
            self.limits.max_manifest_bytes
        } else {
            self.limits.max_object_bytes
        };
        if object.length > limit as u64 {
            return Err(ArchiveError::Limit);
        }
        let bytes = self
            .read(&self.key(&object.name()), limit)
            .await?
            .ok_or(ArchiveError::Corrupt("missing archive object"))?
            .0;
        let expected = object.clone();
        self.host
            .run_blocking(move || {
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
    ) -> Result<Option<(Vec<u8>, UpdateVersion)>, ArchiveError> {
        let result = match self.store.get(path).await {
            Ok(result) => result,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(ArchiveError::Storage(error)),
        };
        if result.meta.size > limit as u64
            || result.range.start != 0
            || result.range.end != result.meta.size
        {
            return Err(ArchiveError::Limit);
        }
        let length = result.meta.size as usize;
        let version = UpdateVersion {
            e_tag: result.meta.e_tag.clone(),
            version: result.meta.version.clone(),
        };
        let mut bytes = Vec::with_capacity(length);
        let mut stream = result.into_stream();
        let mut chunks = 0usize;
        while let Some(chunk) = stream.try_next().await.map_err(ArchiveError::Storage)? {
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
}

impl ArchiveRecoverySet {
    pub fn scope(&self) -> &GroupScope {
        &self.scope
    }

    pub fn position(&self) -> LogPosition {
        self.position
    }

    pub fn configuration(&self) -> Result<ConfState, ArchiveError> {
        manifest::decode_conf(&self.manifest.snapshot.conf_state)
    }

    pub fn schema_digest(&self) -> [u8; 32] {
        self.manifest.schema_digest
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
        let mut state = self.manifest.snapshot.image.decode()?;
        verify_snapshot(&self.snapshot, state)?;
        let mut position = self.manifest.snapshot.position;
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
            }
            position = Position {
                index: entry.index,
                term: entry.term,
            };
        }
        if position != self.manifest.position || Image::from(state) != self.manifest.image {
            return Err(ArchiveError::Corrupt("archive final image/cut"));
        }
        Ok(())
    }

    fn update(&self, entry: &Entry, state: ImageState) -> Result<CellUpdate, ArchiveError> {
        let update = CellUpdate::decode(&entry.data, EnvelopeLimits::default())
            .map_err(|_| ArchiveError::Corrupt("archive LTX envelope"))?;
        if update.scope != self.scope
            || update.application_digest != self.manifest.schema_digest
            || update.base != image_position(state)
        {
            return Err(ArchiveError::Corrupt("archive LTX history/base/schema"));
        }
        Ok(update)
    }

    /// Run on the owning blocking domain with an exclusively opened destination.
    /// A failed restore leaves no activation authority. Its retained temporary
    /// images may be inspected or removed by that directory's recovery owner.
    pub fn restore(
        &self,
        images: &GenerationStore,
        nonce: ImageId,
    ) -> Result<GenerationPin, ArchiveError> {
        let mut sequence = 0u64;
        let id = |sequence: u64| -> Result<ImageId, ArchiveError> {
            let mut hash = Sha256::new();
            hash.update(b"cellar-archive-image-1");
            hash.update(nonce.bytes());
            hash.update(sequence.to_be_bytes());
            let digest: [u8; 32] = hash.finalize().into();
            ImageId::new(
                digest[..16]
                    .try_into()
                    .map_err(|_| ArchiveError::Corrupt("archive image identity"))?,
            )
            .map_err(ArchiveError::Images)
        };
        let state = self.manifest.snapshot.image.decode()?;
        let decoded = ltx::decode_file_bounded(&self.snapshot, apply_limits().decode)
            .map_err(|_| ArchiveError::Corrupt("archive snapshot LTX"))?;
        let mut pin = images
            .prepare(
                id(sequence)?,
                None,
                &ImageUpdate {
                    base: ImageState::empty(state.epoch)
                        .map_err(|_| ArchiveError::Corrupt("archive image epoch"))?,
                    result: state,
                    segments: vec![LtxSegment {
                        bytes: &self.snapshot,
                        min_txid: TXID(1),
                        max_txid: state.txid,
                        page_count: decoded.pages().len(),
                        decoded_bytes: decoded.decoded_bytes(),
                    }],
                },
            )
            .map_err(ArchiveError::Images)?;
        drop(decoded);
        for entry in &self.entries {
            if entry.data.is_empty() {
                continue;
            }
            let update = self.update(entry, pin.state())?;
            let result = result_state(&update)?;
            let segments = update
                .files
                .iter()
                .map(|file| LtxSegment {
                    bytes: &file.bytes,
                    min_txid: TXID(file.min_txid),
                    max_txid: TXID(file.max_txid),
                    page_count: file.page_count as usize,
                    decoded_bytes: file.decoded_bytes,
                })
                .collect();
            sequence += 1;
            let next = images
                .prepare(
                    id(sequence)?,
                    Some(&pin),
                    &ImageUpdate {
                        base: pin.state(),
                        result,
                        segments,
                    },
                )
                .map_err(ArchiveError::Images)?;
            images.retire(pin.id()).map_err(ArchiveError::Images)?;
            pin = next;
            images.reclaim(8).map_err(ArchiveError::Images)?;
        }
        if Image::from(pin.state()) != self.manifest.image {
            return Err(ArchiveError::Corrupt("restored archive image digest"));
        }
        Ok(pin)
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
    let mut limits = ApplyLimits::default();
    limits.decode.max_encoded_bytes = 64 * 1024 * 1024;
    limits.max_total_encoded_bytes = 64 * 1024 * 1024;
    limits
}

fn verify_snapshot(bytes: &[u8], state: ImageState) -> Result<(), ArchiveError> {
    let file = ltx::decode_file_bounded(bytes, apply_limits().decode)
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
