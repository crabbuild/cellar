// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Persistent ownership records for immutable remote roots. A registry reference
//! protects data from collection; it does not verify that data, authenticate a
//! native cut, authorize voting or permit application acknowledgements.

mod intent;
mod state;

pub use intent::{RootAllocation, RootIntent};

use super::*;
use crate::application::consensus::protocol::WireScope;
use rand::RngCore;

const KIND: u8 = 12;
const IDENTITY_KIND: u8 = 13;
const MAX_BYTES: usize = 64 * 1024;

/// Persist this exact registry identity in the authoritative owner. Opening an
/// existing identity never creates missing registry state. Fresh registries use
/// new random namespaces plus an exclusive, never-collected identity object.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RootRegistryId([u8; 32]);

/// Address of an immutable root descriptor, not proof of its dependency closure.
/// Generation names are allocated only by the registry and never reused there.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootDescriptor {
    generation: u64,
    image: [u8; 32],
    digest: [u8; 32],
    bytes: u32,
}

impl RootDescriptor {
    /// Bind independently verified descriptor bytes to their reservation. This
    /// constructor validates metadata bounds only; it performs no storage I/O.
    pub fn new(
        reservation: RootReservation,
        digest: [u8; 32],
        bytes: u32,
    ) -> Result<Self, ArchiveError> {
        let root = Self {
            generation: reservation.id,
            image: reservation.image,
            digest,
            bytes,
        };
        root.validate()?;
        Ok(root)
    }

    fn validate(self) -> Result<(), ArchiveError> {
        if self.generation < 2
            || self.generation == u64::MAX
            || self.image == [0; 32]
            || self.digest == [0; 32]
            || !(1..=256 * 1024).contains(&self.bytes)
        {
            return Err(ArchiveError::Corrupt("retained root descriptor"));
        }
        Ok(())
    }

    pub fn generation(self) -> u64 {
        self.generation
    }

    pub fn image_root(self) -> [u8; 32] {
        self.image
    }

    pub fn digest(self) -> [u8; 32] {
        self.digest
    }

    pub fn metadata_bytes(self) -> u32 {
        self.bytes
    }
}

/// A pending generation protects its own uploads and its exact retained parent.
/// Aborting removes the reservation permanently; late bind attempts then refuse.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootReservation {
    registry: [u8; 32],
    id: u64,
    image: [u8; 32],
    allocation: [u8; 32],
}

impl RootReservation {
    pub fn generation(self) -> u64 {
        self.id
    }
}

/// A durable registry binding. Dropping a handle does not release it; the owner
/// must persist retirement/fencing before explicitly releasing this reference.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RootReference {
    registry: [u8; 32],
    id: u64,
    root: RootDescriptor,
    allocation: [u8; 32],
}

impl RootReference {
    pub fn root(self) -> RootDescriptor {
        self.root
    }
}

/// One consistent mark input. Collectors must verify and expand every listed
/// immutable descriptor before deleting anything, protect each pending upload
/// generation, and exclude objects allocated after revision. This is not a
/// deletion permit and cannot be used to collect legacy archive object keys.
#[derive(Clone, Debug)]
pub struct RootProtection {
    pub registry: RootRegistryId,
    pub revision: u64,
    pub roots: Vec<RootDescriptor>,
    pub pending_generations: Vec<u64>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    registry: RootRegistryId,
    scope: WireScope,
    root: [u8; 32],
    birth: [u8; 32],
    schema: [u8; 32],
    state: state::State,
}

/// Storage adapter for one registry. It uses the archive's execution domain,
/// deadlines and cancellation. Open it again from its persisted identity after
/// restart; a missing/corrupt registry refuses, rather than resetting counters.
#[derive(Clone)]
pub struct RootRegistry {
    archive: CellArchive,
    id: RootRegistryId,
}

impl CellArchive {
    /// Allocate a fresh registry. The caller must durably select the returned
    /// identity before using its references as part of an installed-root proof.
    /// An uncertain creation may leave metadata, but never adopts an old identity.
    pub async fn create_root_registry(&self) -> Result<RootRegistry, ArchiveError> {
        if self.image_storage != super::super::catalog::RaftImageStorage::Extents {
            return Err(ArchiveError::Invalid("extent archive namespace required"));
        }
        let mut id = [0; 32];
        crate::asyncrt::rng("raft-root-registry").fill_bytes(&mut id);
        if id == [0; 32] {
            return Err(ArchiveError::Invalid("root registry identity"));
        }
        let registry = RootRegistry {
            archive: self.clone(),
            id: RootRegistryId(id),
        };
        let record = registry.record(state::State::empty());
        let identity = manifest::encode(IDENTITY_KIND, &record, MAX_BYTES)?;
        // Never interpret AlreadyExists or an unknown response as a fresh
        // allocation. The immutable identity prevents reuse after state loss.
        self.io(
            self.deadline()?,
            self.store.put_opts(
                &registry.path("identity"),
                identity.clone().into(),
                PutOptions::from(PutMode::Create),
            ),
        )
        .await?;
        registry.verify_identity().await?;
        let bytes = manifest::encode(KIND, &record, MAX_BYTES)?;
        let outcome = self
            .io(
                self.deadline()?,
                self.store.put_opts(
                    &registry.path("registry"),
                    bytes.clone().into(),
                    PutOptions::from(PutMode::Create),
                ),
            )
            .await;
        match self
            .read(&registry.path("registry"), MAX_BYTES, None)
            .await?
        {
            Some((observed, _)) if observed == bytes => Ok(registry),
            _ => outcome.and(Err(ArchiveError::Unconfirmed)),
        }
    }

    pub async fn open_root_registry(
        &self,
        id: RootRegistryId,
    ) -> Result<RootRegistry, ArchiveError> {
        let registry = RootRegistry {
            archive: self.clone(),
            id,
        };
        registry.verify_identity().await?;
        registry.load().await?;
        Ok(registry)
    }
}

impl RootRegistry {
    pub fn id(&self) -> RootRegistryId {
        self.id
    }

    fn path(&self, name: &str) -> Path {
        self.archive
            .key(&format!("retained/{}/{name}", hex::encode(self.id.0)))
    }

    fn record(&self, state: state::State) -> Record {
        Record {
            registry: self.id,
            scope: WireScope::from(&self.archive.scope),
            root: self.archive.root,
            birth: self.archive.birth,
            schema: self.archive.schema,
            state,
        }
    }

    async fn verify_identity(&self) -> Result<(), ArchiveError> {
        let expected = manifest::encode(
            IDENTITY_KIND,
            &self.record(state::State::empty()),
            MAX_BYTES,
        )?;
        match self
            .archive
            .read(&self.path("identity"), MAX_BYTES, None)
            .await?
        {
            Some((bytes, _)) if bytes == expected => Ok(()),
            _ => Err(ArchiveError::Corrupt("root registry identity")),
        }
    }

    async fn load(&self) -> Result<(state::State, Vec<u8>, UpdateVersion), ArchiveError> {
        if self.archive.image_storage != super::super::catalog::RaftImageStorage::Extents
            || self.id.0 == [0; 32]
        {
            return Err(ArchiveError::Invalid("root registry domain"));
        }
        let (bytes, version) = self
            .archive
            .read(&self.path("registry"), MAX_BYTES, None)
            .await?
            .ok_or(ArchiveError::Corrupt("missing root registry"))?;
        if version.e_tag.is_none() && version.version.is_none() {
            return Err(ArchiveError::Invalid(
                "root registry lacks a conditional version",
            ));
        }
        let record: Record = manifest::decode(KIND, &bytes, MAX_BYTES)?;
        if record.registry != self.id
            || record.scope != WireScope::from(&self.archive.scope)
            || record.root != self.archive.root
            || record.birth != self.archive.birth
            || record.schema != self.archive.schema
        {
            return Err(ArchiveError::Corrupt("root registry history/schema"));
        }
        record.state.validate(self.id.0)?;
        Ok((record.state, bytes, version))
    }

    async fn change<T>(
        &self,
        mut transition: impl FnMut(&mut state::State) -> Result<T, ArchiveError>,
    ) -> Result<T, ArchiveError> {
        for _ in 0..8 {
            let (mut state, before, version) = self.load().await?;
            let result = transition(&mut state)?;
            state.validate(self.id.0)?;
            let desired = manifest::encode(KIND, &self.record(state), MAX_BYTES)?;
            if desired == before {
                return Ok(result);
            }
            let outcome = self
                .archive
                .io(
                    self.archive.deadline()?,
                    self.archive.store.put_opts(
                        &self.path("registry"),
                        desired.clone().into(),
                        PutOptions::from(PutMode::Update(version)),
                    ),
                )
                .await;
            let (_, observed, _) = self.load().await?;
            if observed == desired {
                return Ok(result);
            }
            if matches!(
                outcome,
                Err(ArchiveError::Storage(
                    object_store::Error::Precondition { .. }
                        | object_store::Error::AlreadyExists { .. }
                ))
            ) {
                continue;
            }
            // A changed readback after success or an unknown response cannot
            // justify repeating an allocating operation. Its first result may
            // already exist. Reconciliation is an explicit owner operation.
            return Err(ArchiveError::Unconfirmed);
        }
        Err(ArchiveError::Conflict)
    }

    /// Commit exactly one caller-persisted allocation intent. Retry this same
    /// intent after an uncertain outcome; never prepare a replacement merely
    /// because a response was lost. An expired intent cannot allocate again.
    pub async fn commit(&self, intent: &RootIntent) -> Result<RootAllocation, ArchiveError> {
        if intent.registry != self.id {
            return Err(ArchiveError::Conflict);
        }
        let allocation = intent.digest()?;
        self.change(|state| state.allocate(intent, allocation))
            .await
    }

    pub async fn release(&self, reference: RootReference) -> Result<bool, ArchiveError> {
        if reference.registry != self.id.0 {
            return Err(ArchiveError::Conflict);
        }
        self.change(|state| state.release(reference)).await
    }

    pub async fn abort(&self, reservation: RootReservation) -> Result<bool, ArchiveError> {
        if reservation.registry != self.id.0 {
            return Err(ArchiveError::Conflict);
        }
        self.change(|state| state.abort(reservation)).await
    }

    pub async fn select(
        &self,
        reference: RootReference,
        position: LogPosition,
    ) -> Result<(), ArchiveError> {
        if reference.registry != self.id.0 {
            return Err(ArchiveError::Conflict);
        }
        self.change(|state| state.select(reference, position.into()))
            .await
    }

    pub async fn referenced(
        &self,
        reference: RootReference,
    ) -> Result<RootDescriptor, ArchiveError> {
        if reference.registry != self.id.0 {
            return Err(ArchiveError::Conflict);
        }
        self.load().await?.0.referenced(reference)
    }

    pub async fn protection(&self) -> Result<RootProtection, ArchiveError> {
        let (state, _, _) = self.load().await?;
        Ok(RootProtection {
            registry: self.id,
            revision: state.revision,
            roots: state.roots(),
            pending_generations: state.pending.iter().map(|entry| entry.ticket.id).collect(),
        })
    }
}
