// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Exact immutable root material. Object-store references supply data only;
//! archive-head publication and live learner authorization remain separate.

use super::*;
use crate::raft::snapshot::{
    PreparedSnapshot, RootSnapshotSet, RootSnapshotSource, SnapshotError, SnapshotLimits,
};
use cellar_ltx::internal::{ExtentId, NodeExtentStore, SealedImageRoot, EXTENT_BYTES};
use futures_util::stream::{FuturesOrdered, FuturesUnordered};

const MAGIC: &[u8; 8] = b"CLRROT01";
const HEADER_BYTES: usize = 48;
const MAX_METADATA_BYTES: usize = 256 * 1024;
const ROOT_WINDOW: usize = 2;

// A GET may temporarily retain both its response chunk and copied output.
// Include both for each entered request, including in-memory store providers.
const _: () = assert!(2 * ROOT_WINDOW * EXTENT_BYTES as usize <= physical::READ_BYTES);

pub(super) fn root_error(error: SnapshotError) -> ArchiveError {
    ArchiveError::Root(Box::new(error))
}

/// An exact immutable root snapshot address. This is neither the selected
/// archive head nor a live grant to initialize, attach or promote a member.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RootArchiveReference(ObjectRef);

impl RootArchiveReference {
    pub(super) fn metadata_bytes(&self) -> u64 {
        self.0.length
    }

    pub fn position(&self) -> Result<LogPosition, ArchiveError> {
        let object = &self.0;
        let range = object
            .range
            .ok_or(ArchiveError::Corrupt("root archive reference range"))?;
        if object.kind != ObjectKind::RootSnapshot
            || object.bundle.is_some()
            || range.first != range.last
            || !(HEADER_BYTES as u64..=MAX_METADATA_BYTES as u64).contains(&object.length)
        {
            return Err(ArchiveError::Corrupt("root archive reference domain"));
        }
        range.last.decode()
    }
}

/// A complete independently verified local closure, pinned through native
/// preparation. Only the receiving engine's ordinary ticket can install it.
pub struct RootArchiveSnapshot {
    pub(super) set: RootSnapshotSet,
    pub(super) store: NodeExtentStore,
    pub(super) image: Arc<SealedImageRoot>,
    _retention: Option<Arc<super::super::budget::Permit>>,
    // Declared last: release image/receive pins before releasing scratch.
    _verification: Option<Arc<super::verifier::ExtentLease>>,
}

impl RootArchiveSnapshot {
    pub fn snapshot(&self) -> &::raft::prelude::Snapshot {
        self.set.snapshot()
    }

    pub fn prepare(
        self,
        group: &GroupLog,
        schema: [u8; 32],
        limits: SnapshotLimits,
    ) -> Result<PreparedSnapshot, ArchiveError> {
        self.set
            .prepare_sealed(group, schema, &self.store, limits, self.image)
            .map_err(root_error)
    }
}

/// Every remote object was checked in this publication attempt, either by
/// its exact PUT read-back or by the freshly downloaded previous root. The
/// source retains the independently sealed local bytes that those checks bind.
/// This receipt supplies material only, never an archive head or native grant.
pub(super) struct ConfirmedRootSnapshot {
    reference: RootArchiveReference,
    set: RootSnapshotSet,
    source: Arc<RootSnapshotSource>,
    _retention: Option<Arc<super::super::budget::Permit>>,
}

impl ConfirmedRootSnapshot {
    pub fn reference(&self) -> &RootArchiveReference {
        &self.reference
    }

    pub async fn bind(
        self,
        expected: &RootArchiveReference,
        store: &NodeExtentStore,
        host: &LtxHost,
    ) -> Result<RootArchiveSnapshot, ArchiveError> {
        if &self.reference != expected {
            return Err(ArchiveError::Corrupt("confirmed root archive reference"));
        }
        let image = self.source.local_image(store).map_err(root_error)?;
        if self.set.reference() != image.reference()
            || self.set.marker() != &crate::raft::rooted::marker::encode(&image)
        {
            return Err(ArchiveError::Corrupt("confirmed root source binding"));
        }
        let target = store.clone();
        host.run_blocking(move || {
            // The producer's source protects all local objects while the
            // inventory rechecks its metadata, geometry and persistence. Reuse
            // avoids another remote download, not the destination seal.
            let _source = self.source;
            let _retention = self._retention;
            let image = self
                .set
                .seal(&target, &mut vec![0; EXTENT_BYTES as usize])
                .map_err(root_error)?;
            Ok(RootArchiveSnapshot {
                set: self.set,
                store: target,
                image: Arc::new(image),
                _retention,
                _verification: None,
            })
        })
        .await
        .map_err(ArchiveError::Worker)?
    }
}

impl CellArchive {
    pub(super) async fn root_index(
        &self,
        reference: &RootArchiveReference,
    ) -> Result<RootSnapshotSet, ArchiveError> {
        reference.position()?;
        let metadata = self.get_object(&reference.0).await?;
        self.decode_root_index(reference, &metadata)
    }

    fn validate_root_objects(&self, set: &RootSnapshotSet) -> Result<(), ArchiveError> {
        if set
            .extents()
            .any(|id| id.length() as usize > self.limits.max_object_bytes)
        {
            return Err(ArchiveError::Limit);
        }
        Ok(())
    }

    fn root_limits(&self) -> Result<SnapshotLimits, ArchiveError> {
        if self.image_storage != super::super::catalog::RaftImageStorage::Extents {
            return Err(ArchiveError::Invalid("extent archive namespace required"));
        }
        let limits = SnapshotLimits {
            max_image_bytes: self.limits.max_image_bytes,
            max_encoded_bytes: usize::try_from(self.limits.max_snapshot_bytes)
                .map_err(|_| ArchiveError::Limit)?,
            ..Default::default()
        };
        limits.validate().map_err(root_error)?;
        Ok(limits)
    }

    fn encode_root_index(
        &self,
        source: &RootSnapshotSource,
    ) -> Result<(RootSnapshotSet, Vec<u8>), ArchiveError> {
        let snapshot = source.snapshot();
        let set = RootSnapshotSet::for_archive(
            &self.scope,
            self.birth,
            &self.initial_membership,
            self.schema,
            &snapshot,
            source.manifest(),
            self.root_limits()?,
        )
        .map_err(root_error)?;
        self.validate_root_objects(&set)?;
        let native = snapshot
            .write_to_bytes()
            .map_err(|_| ArchiveError::Corrupt("root snapshot encoding"))?;
        let total = HEADER_BYTES
            .checked_add(native.len())
            .and_then(|length| length.checked_add(source.manifest().len()))
            .ok_or(ArchiveError::Limit)?;
        if native.len() > 64 * 1024 || total > MAX_METADATA_BYTES.min(self.limits.max_object_bytes)
        {
            return Err(ArchiveError::Limit);
        }
        let mut bytes = Vec::with_capacity(total);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&self.root);
        bytes.extend_from_slice(&(native.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&(source.manifest().len() as u32).to_be_bytes());
        bytes.extend_from_slice(&native);
        bytes.extend_from_slice(source.manifest());
        Ok((set, bytes))
    }

    fn decode_root_index(
        &self,
        reference: &RootArchiveReference,
        bytes: &[u8],
    ) -> Result<RootSnapshotSet, ArchiveError> {
        let position = reference.position()?;
        if bytes.len() < HEADER_BYTES
            || bytes.len() > MAX_METADATA_BYTES
            || &bytes[..8] != MAGIC
            || bytes[8..40] != self.root
        {
            return Err(ArchiveError::Corrupt("root archive metadata domain"));
        }
        let native_length = u32::from_be_bytes(
            bytes[40..44]
                .try_into()
                .map_err(|_| ArchiveError::Corrupt("root snapshot length"))?,
        ) as usize;
        let manifest_length = u32::from_be_bytes(
            bytes[44..48]
                .try_into()
                .map_err(|_| ArchiveError::Corrupt("root manifest length"))?,
        ) as usize;
        let manifest_at = HEADER_BYTES
            .checked_add(native_length)
            .filter(|_| native_length <= 64 * 1024)
            .ok_or(ArchiveError::Limit)?;
        if manifest_at.checked_add(manifest_length) != Some(bytes.len()) {
            return Err(ArchiveError::Corrupt("root archive metadata coverage"));
        }
        let native = bytes
            .get(HEADER_BYTES..manifest_at)
            .ok_or(ArchiveError::Corrupt("root snapshot range"))?;
        let snapshot = ::raft::prelude::Snapshot::parse_from_bytes(native)
            .map_err(|_| ArchiveError::Corrupt("root snapshot protobuf"))?;
        if snapshot
            .write_to_bytes()
            .map_err(|_| ArchiveError::Corrupt("root snapshot protobuf"))?
            != native
            || snapshot.get_metadata().index != position.index.get()
            || snapshot.get_metadata().term != position.term.get()
        {
            return Err(ArchiveError::Corrupt("root archive cut binding"));
        }
        let set = RootSnapshotSet::for_archive(
            &self.scope,
            self.birth,
            &self.initial_membership,
            self.schema,
            &snapshot,
            &bytes[manifest_at..],
            self.root_limits()?,
        )
        .map_err(root_error)?;
        self.validate_root_objects(&set)?;
        Ok(set)
    }

    fn extent_key(&self, id: ExtentId) -> Path {
        self.key(&format!(
            "root-extents/{}-{}",
            id.length(),
            hex::encode(id.bytes())
        ))
    }

    async fn get_root_extent(&self, id: ExtentId) -> Result<Vec<u8>, ArchiveError> {
        if id.length() as usize > self.limits.max_object_bytes {
            return Err(ArchiveError::Limit);
        }
        let bytes = self
            .read(
                &self.extent_key(id),
                id.length() as usize,
                Some(u64::from(id.length())),
            )
            .await?
            .ok_or(ArchiveError::Corrupt("missing root archive extent"))?
            .0;
        let retention = self.read_retention.clone();
        self.host
            .run_blocking(move || {
                let _retention = retention;
                id.verify(&bytes)
                    .map_err(|_| ArchiveError::Corrupt("root archive extent identity"))?;
                Ok(bytes)
            })
            .await
            .map_err(ArchiveError::Worker)?
    }

    async fn put_root_extent(&self, id: ExtentId, bytes: Vec<u8>) -> Result<(), ArchiveError> {
        if bytes.len() != id.length() as usize || bytes.len() > self.limits.max_object_bytes {
            return Err(ArchiveError::Limit);
        }
        let result = self
            .io(
                self.deadline()?,
                self.store.put_opts(
                    &self.extent_key(id),
                    bytes.into(),
                    PutOptions::from(PutMode::Create),
                ),
            )
            .await;
        // Neither success nor a lost PUT response proves these stored bytes.
        // Existing objects are read too; immutable corruption is never repaired.
        match self.get_root_extent(id).await {
            Ok(_) => Ok(()),
            Err(ArchiveError::Corrupt("missing root archive extent")) => {
                result.and(Err(ArchiveError::Unconfirmed))
            }
            Err(error) => Err(error),
        }
    }

    /// Store and confirm an exact immutable snapshot's complete remote closure.
    /// The caller admits read_memory_bound() working memory and retains its
    /// registered physical owner through completion/cancellation. No head,
    /// member claim, archive coverage or acknowledgement advances here.
    pub async fn store_root_snapshot(
        &self,
        source: Arc<RootSnapshotSource>,
    ) -> Result<RootArchiveReference, ArchiveError> {
        Ok(self.confirm_root_snapshot(source, None).await?.reference)
    }

    /// Reuse only the previous root freshly verified inside the caller's
    /// current head-CAS attempt. Standalone stores always pass None. A retained
    /// reference, local filename or earlier publication is not this proof.
    pub(super) async fn confirm_root_snapshot(
        &self,
        source: Arc<RootSnapshotSource>,
        previous: Option<&RootArchiveSnapshot>,
    ) -> Result<ConfirmedRootSnapshot, ArchiveError> {
        let (set, metadata) = self.encode_root_index(&source)?;
        let extents: Vec<_> = set
            .extents()
            .filter(|id| !previous.is_some_and(|base| base.set.contains_extent(*id)))
            .collect();
        let reused_objects = set.extents().count() - extents.len();
        tracing::info!(event = "raft_archive_phase", phase = "root_upload_start", cell = %self.scope.cell, index = set.snapshot().get_metadata().index, image_bytes = set.marker().length, objects = extents.len(), reused_objects);
        let mut objects = extents.into_iter();
        let mut pending = FuturesOrdered::new();
        let mut failure = None;
        loop {
            while failure.is_none() && pending.len() < ROOT_WINDOW {
                let Some(id) = objects.next() else { break };
                let source = source.clone();
                let retention = self.read_retention.clone();
                let read = self
                    .host
                    .run_blocking(move || {
                        let _retention = retention;
                        source.read_extent(id).map_err(root_error)
                    })
                    .await
                    .map_err(ArchiveError::Worker)
                    .and_then(|result| result);
                match read {
                    Ok(bytes) => pending.push_back(self.put_root_extent(id, bytes)),
                    Err(error) => failure = Some(error),
                }
            }
            let Some(result) = pending.next().await else {
                break;
            };
            if let Err(error) = result {
                if failure.is_none() {
                    failure = Some(error);
                }
            }
            // Drain every entered confirmation on failure before returning.
        }
        if let Some(error) = failure {
            return Err(error);
        }
        let metadata = self
            .put_object(
                ObjectKind::RootSnapshot,
                &metadata,
                Some(PositionRange {
                    first: Position {
                        index: set.snapshot().get_metadata().index,
                        term: set.snapshot().get_metadata().term,
                    },
                    last: Position {
                        index: set.snapshot().get_metadata().index,
                        term: set.snapshot().get_metadata().term,
                    },
                }),
            )
            .await?;
        tracing::info!(event = "raft_archive_phase", phase = "root_upload_confirmed", cell = %self.scope.cell, index = set.snapshot().get_metadata().index);
        Ok(ConfirmedRootSnapshot {
            reference: RootArchiveReference(metadata),
            set,
            source,
            _retention: self.read_retention.clone(),
        })
    }

    /// Download only this exact reference, authenticate all declared objects,
    /// and independently seal the destination. Local cache contents cannot
    /// hide missing or corrupt remote bytes. No activation authority is minted.
    pub async fn load_root_snapshot_at(
        &self,
        reference: &RootArchiveReference,
        store: &NodeExtentStore,
    ) -> Result<RootArchiveSnapshot, ArchiveError> {
        let position = reference.position()?;
        tracing::info!(event = "raft_archive_phase", phase = "root_material_read_start", cell = %self.scope.cell, index = position.index.get());
        let metadata = self.get_object(&reference.0).await?;
        let set = Arc::new(self.decode_root_index(reference, &metadata)?);
        let pin_set = set.clone();
        let inventory = store.clone();
        let retention = self.read_retention.clone();
        let verification = self.root_verification.clone();
        let receive_pin = self
            .host
            .run_blocking(move || {
                let _retention = retention;
                // The returned receive pin and every physical clone keep the
                // scratch owner until the pin itself has been released.
                pin_set
                    .pin_receive(&inventory)
                    .map(|pin| (pin, verification))
                    .map_err(root_error)
            })
            .await
            .map_err(ArchiveError::Worker)??;
        // Bound the complete GET/verify/install lifetime, rather than stopping
        // network polling whenever one extent enters its destination write.
        // Serializing only installs also supports inventories whose mutation
        // contract refuses concurrent operations instead of waiting for them.
        let install = futures_util::lock::Mutex::new(());
        let mut objects = set.extents();
        let mut pending = FuturesUnordered::new();
        let mut failure = None;
        loop {
            while failure.is_none() && pending.len() < ROOT_WINDOW {
                let Some(id) = objects.next() else { break };
                let set = set.clone();
                let store = store.clone();
                let pin = receive_pin.clone();
                let retention = self.read_retention.clone();
                let install = &install;
                pending.push(async move {
                    let bytes = self.get_root_extent(id).await?;
                    let _install = install.lock().await;
                    self.host
                        .run_blocking(move || {
                            // Cancellation may drop the async window while its
                            // entered physical job still runs. Keep both GC
                            // protection and admission through that last I/O.
                            let _pin = pin;
                            let _retention = retention;
                            set.install_extent(&store, id, &bytes).map_err(root_error)
                        })
                        .await
                        .map_err(ArchiveError::Worker)?
                });
            }
            let Some(result) = pending.next().await else {
                break;
            };
            if let Err(error) = result {
                if failure.is_none() {
                    failure = Some(error);
                }
            }
            // Stop admitting objects after the first observed error, but join
            // every entered GET and physical install before returning failure.
        }
        drop(pending);
        drop(objects);
        if let Some(error) = failure {
            return Err(error);
        }
        let set = Arc::try_unwrap(set)
            .map_err(|_| ArchiveError::Invalid("root archive physical owner retained"))?;
        let target = store.clone();
        let pin = receive_pin.clone();
        let retention = self.read_retention.clone();
        let verification = self.root_verification.clone();
        let image = self
            .host
            .run_blocking(move || {
                let _pin = pin;
                let _retention = retention;
                let image = set
                    .seal(&target, &mut vec![0; EXTENT_BYTES as usize])
                    .map_err(root_error)?;
                Ok::<_, ArchiveError>(RootArchiveSnapshot {
                    set,
                    store: target,
                    image: Arc::new(image),
                    _retention,
                    _verification: verification,
                })
            })
            .await
            .map_err(ArchiveError::Worker)??;
        drop(receive_pin);
        tracing::info!(event = "raft_archive_phase", phase = "root_material_read_done", cell = %self.scope.cell, index = position.index.get());
        Ok(image)
    }
}
