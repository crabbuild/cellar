// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Reuse only a completed proof of identical physical replay inputs. Remote
//! existence, configuration, membership and activation remain separate gates.
use super::{ArchiveError, EntryType, Image, MaterializedSet};
use cellar_ltx::generation::{GenerationStore, ImageId};
use protobuf::Message as _;
use sha2::{Digest, Sha256};
use std::sync::{Arc, Mutex};

/// One bounded scratch owner may verify many cells. The exclusively opened
/// store must initially be empty; no pin from it escapes this owner. One fixed
/// digest remembers a successful physical replay, never a local file or grant.
/// A failed replay quarantines the workspace until its owner performs recovery.
#[derive(Clone)]
pub struct ArchiveVerifier {
    inner: Arc<Mutex<State>>,
}

struct State {
    images: GenerationStore,
    nonce: ImageId,
    sequence: u64,
    failed: bool,
    memo: Option<Key>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Replay {
    Performed,
    Reused,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct Key([u8; 32]);

// Issued only after the recovery-set constructor's complete byte/configuration
// validation. The full fingerprint binds metadata which intentionally does not
// belong to the physical SQL projection. A private mutation must revalidate.
pub(super) struct Validated {
    physical: Key,
    complete: [u8; 32],
}

impl Validated {
    pub(super) fn record(set: &MaterializedSet) -> Result<Self, ArchiveError> {
        let physical = Key::new(set)?;
        Ok(Self {
            physical,
            complete: complete(set, physical)?,
        })
    }

    fn checked_key(&self, set: &MaterializedSet) -> Result<Key, ArchiveError> {
        let physical = Key::new(set)?;
        if physical != self.physical || complete(set, physical)? != self.complete {
            return Err(ArchiveError::Corrupt(
                "archive material changed after validation",
            ));
        }
        Ok(physical)
    }
}

fn complete(set: &MaterializedSet, physical: Key) -> Result<[u8; 32], ArchiveError> {
    let mut hash = Sha256::new();
    hash.update(b"cellar-archive-validated-inputs-1");
    hash.update(physical.0);
    hash.update(set.position.index.get().to_be_bytes());
    hash.update(set.position.term.get().to_be_bytes());
    image(
        &mut hash,
        set.snapshot
            .as_ref()
            .map(|snapshot| snapshot.metadata().image.into()),
    );
    // Stream bounded metadata into the hash: serializing it to a fresh Vec
    // would add an unadmitted buffer. No file or socket I/O occurs here.
    serde_json::to_writer(DigestWriter(&mut hash), &set.manifest)
        .map_err(|_| ArchiveError::Corrupt("archive validation fingerprint"))?;
    serde_json::to_writer(DigestWriter(&mut hash), &set.reference.0)
        .map_err(|_| ArchiveError::Corrupt("archive validation fingerprint"))?;
    hash.update((set.entries.len() as u64).to_be_bytes());
    for entry in &set.entries {
        if entry.get_unknown_fields().iter().next().is_some() {
            return Err(ArchiveError::Corrupt("archive entry coverage/domain"));
        }
        hash.update((entry.get_entry_type() as i32).to_be_bytes());
        hash_entry(&mut hash, entry);
    }
    Ok(hash.finalize().into())
}

struct DigestWriter<'a>(&'a mut Sha256);

impl std::io::Write for DigestWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Key {
    fn new(set: &MaterializedSet) -> Result<Self, ArchiveError> {
        let mut hash = Sha256::new();
        hash.update(b"cellar-archive-physical-replay-1");
        let scope = &set.scope;
        hash.update(scope.cluster.bytes());
        hash.update(scope.namespace.bytes());
        hash.update(scope.group.bytes());
        hash.update(scope.incarnation.bytes());
        bytes(&mut hash, scope.cell.as_bytes());
        hash.update(set.manifest.root_digest);
        hash.update(set.manifest.birth_digest);
        hash.update(set.manifest.schema_digest);
        hash.update([u8::from(set.snapshot.is_some())]);
        if let Some(snapshot) = &set.snapshot {
            let metadata = snapshot.metadata();
            hash.update(metadata.encoded_length.to_be_bytes());
            hash.update(metadata.encoded_digest);
        }
        image(&mut hash, set.manifest.snapshot.image);
        image(&mut hash, set.manifest.image);
        // Policy remains explicit even though the constructors enforce it too.
        for bound in [
            set.max_image_bytes,
            set.envelope.max_header_bytes as u64,
            set.envelope.max_payload_bytes as u64,
            set.envelope.max_files as u64,
            set.envelope.max_decoded_bytes,
            set.envelope.max_pages,
        ] {
            hash.update(bound.to_be_bytes());
        }
        let data = set.entries.iter().filter(|entry| {
            entry.get_entry_type() == EntryType::EntryNormal && !entry.data.is_empty()
        });
        hash.update((data.clone().count() as u64).to_be_bytes());
        for entry in data {
            hash_entry(&mut hash, entry);
        }
        if set.dependencies.len() != set.manifest.dependencies.len() {
            return Err(ArchiveError::Corrupt("archive dependency coverage"));
        }
        hash.update((set.dependencies.len() as u64).to_be_bytes());
        for ((object, payload), expected) in set.dependencies.iter().zip(&set.manifest.dependencies)
        {
            if object != expected || object.kind != super::ObjectKind::Dependency {
                return Err(ArchiveError::Corrupt("archive dependency binding"));
            }
            // Bind actual freshly observed dependency bytes, rather than a
            // declaration that could hide changed or corrupted material.
            object.verify(payload)?;
            hash.update(object.length.to_be_bytes());
            hash.update(object.digest);
        }
        // No metadata-only cut/configuration enters the SQL projection. Those
        // changes are checked on every construction and rechecked on a hit.
        Ok(Self(hash.finalize().into()))
    }
}

fn hash_entry(hash: &mut Sha256, entry: &super::Entry) {
    hash.update(entry.index.to_be_bytes());
    hash.update(entry.term.to_be_bytes());
    hash.update([u8::from(entry.sync_log)]);
    bytes(hash, &entry.context);
    bytes(hash, &entry.data);
}

fn bytes(hash: &mut Sha256, value: &[u8]) {
    hash.update((value.len() as u64).to_be_bytes());
    hash.update(value);
}

fn image(hash: &mut Sha256, value: Option<Image>) {
    hash.update([u8::from(value.is_some())]);
    let Some(value) = value else { return };
    hash.update(value.epoch.to_be_bytes());
    hash.update(value.txid.to_be_bytes());
    hash.update(value.checksum.to_be_bytes());
    hash.update(value.page_size.to_be_bytes());
    hash.update(value.length.to_be_bytes());
    hash.update(value.digest);
}

impl ArchiveVerifier {
    pub fn new(images: GenerationStore, nonce: ImageId) -> Result<Self, ArchiveError> {
        if images.usage().map_err(ArchiveError::Images)?.generations != 0 {
            return Err(ArchiveError::Invalid("archive verifier store is not empty"));
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(State {
                images,
                nonce,
                sequence: 0,
                failed: false,
                memo: None,
            })),
        })
    }

    pub(super) fn verify(&self, set: &MaterializedSet) -> Result<Replay, ArchiveError> {
        self.verify_checked(set, || Ok(()))
    }

    pub(super) fn verify_checked(
        &self,
        set: &MaterializedSet,
        check: impl FnOnce() -> Result<(), ArchiveError>,
    ) -> Result<Replay, ArchiveError> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| ArchiveError::Invalid("archive verifier owner is poisoned"))?;
        // A queued lifecycle proof may have expired while this workspace was
        // owned by another replay. Check before using either scratch or its memo.
        check()?;
        if state.failed {
            return Err(ArchiveError::Invalid("archive verifier requires recovery"));
        }
        let key = match set
            .validated
            .as_ref()
            .ok_or(ArchiveError::Invalid(
                "archive replay input is not validated",
            ))
            .and_then(|validated| validated.checked_key(set))
        {
            Ok(key) => key,
            Err(error) => {
                state.memo = None;
                return Err(error);
            }
        };
        if state.memo == Some(key) {
            // Fresh remote reads and full source decoding issued this set's
            // validation record. Its complete fingerprint still matches, while
            // the physical projection matches an earlier successful replay.
            // No new destination or duplicate full-file scan is needed here.
            if state
                .images
                .usage()
                .map_err(ArchiveError::Images)?
                .generations
                != 0
            {
                state.failed = true;
                state.memo = None;
                return Err(ArchiveError::Invalid(
                    "archive verifier retained scratch images",
                ));
            }
            return Ok(Replay::Reused);
        }
        state.memo = None;
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
        // Set before any workspace I/O. Failure/panic cannot reuse a prior
        // proof or silently recycle a partially restored scratch generation.
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
        // Commit the memo only after replay, independent image verification,
        // retirement and physical cleanup all succeed. State stays fixed-size.
        state.memo = Some(key);
        state.failed = false;
        Ok(Replay::Performed)
    }
}

#[cfg(test)]
#[path = "verifier_tests.rs"]
mod tests;
