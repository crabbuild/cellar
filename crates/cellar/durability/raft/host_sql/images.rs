// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical dispatch beneath the SQL worker's existing command/read controller.
//! Snapshot, seed, archive and effect discovery share this physical owner.
//! An unsupported physical operation refuses rather than falling
//! back to a different commitment or opening a second private writer.

use super::*;
use crate::raft::rooted::{query_registered_rooted_controlled, RootImageApplier, RootQueryOptions};
use cellar_ltx::{
    generation::GenerationAccounting,
    internal::{CacheLimits, NodeExtentStore, EXTENT_BYTES},
    stream_snapshot::ScratchPool,
};
use sha2::{Digest, Sha256};

#[derive(Clone)]
pub(crate) struct RootStorage {
    // The node supplies one shared inventory and a cache outside its durable
    // namespace. Initialization/readers never admit a disposable SQL directory.
    pub store: NodeExtentStore,
    pub cache_directory: PathBuf,
    pub cache_pool: ScratchPool,
    pub cache: CacheLimits,
}

pub(super) struct RootOwnerOptions {
    storage: RootStorage,
    workspace_id: [u8; 32],
    host: LtxHost,
    query_services: ReadServices,
    application_digest: [u8; 32],
    limits: ExecutionLimits,
}

impl RootOwnerOptions {
    fn database(
        &self,
        image: &crate::raft::rooted::RootPublishedImage,
    ) -> Result<crate::storage::ImmutableQuery, ExecutionError> {
        let view = image
            .image()
            .view()
            .image(
                self.storage.cache_directory.clone(),
                self.storage.cache_pool.clone(),
                self.storage.cache,
            )
            .map_err(super::super::execution::storage)?;
        crate::storage::open_immutable_extent_image(
            view,
            crate::storage::ImmutableQueryOptions {
                services: self.query_services.clone(),
                sqlite_vec: false,
            },
        )
        .map_err(super::super::execution::storage)
    }
}

pub(super) enum ImageOwner {
    Materialized {
        options: Box<PrivateOptions>,
        applier: Box<ImageApplier>,
    },
    Rooted {
        options: Box<RootOwnerOptions>,
        applier: Box<RootImageApplier>,
    },
}

impl ImageOwner {
    pub fn capture_archive(
        &self,
        group: &GroupLog,
        class: &RaftClass,
        limits: super::super::archive::ArchiveLimits,
    ) -> Result<Option<super::super::archive::CommittedArchiveCut>, ExecutionError> {
        use super::super::{archive::CommittedArchiveCut, effects};
        match self {
            Self::Materialized { options, applier } => {
                let cut = match applier
                    .queryable()
                    .map_err(super::super::execution::storage)?
                {
                    Some(image) => {
                        let dependencies = effects::capture_dependencies(
                            &image,
                            class.blob_limits(),
                            options.query_services.clone(),
                            limits,
                        )
                        .map_err(super::super::execution::storage)?;
                        CommittedArchiveCut::capture(group, image, dependencies, limits)
                    }
                    None => {
                        CommittedArchiveCut::capture_empty(group, class.schema_digest(), limits)
                    }
                }
                .map_err(super::super::execution::storage)?;
                Ok(Some(cut))
            }
            Self::Rooted { options, applier } => {
                let Some(image) = applier
                    .queryable()
                    .map_err(super::super::execution::storage)?
                else {
                    return CommittedArchiveCut::capture_empty(
                        group,
                        class.schema_digest(),
                        limits,
                    )
                    .map(Some)
                    .map_err(super::super::execution::storage);
                };
                let dependencies = if let Some(blobs) = class.blob_limits() {
                    let database = options.database(&image)?;
                    effects::capture_connection_dependencies(database.connection(), blobs, limits)
                        .map_err(super::super::execution::storage)?
                } else {
                    vec![]
                };
                CommittedArchiveCut::capture_root(
                    group,
                    image,
                    options.storage.store.clone(),
                    dependencies,
                    limits,
                )
                .map(Some)
                .map_err(super::super::execution::storage)
            }
        }
    }

    pub fn capture_snapshot(
        &self,
        group: &GroupLog,
        schema: [u8; 32],
        limits: SnapshotLimits,
    ) -> Result<SnapshotCapture, ExecutionError> {
        let capture = match self {
            Self::Materialized { applier, .. } => match applier
                .queryable()
                .map_err(super::super::execution::storage)?
            {
                Some(image) => SnapshotCapture::capture(group, image, limits),
                None => SnapshotCapture::capture_empty(group, schema, limits),
            },
            Self::Rooted { options, applier } => match applier
                .queryable()
                .map_err(super::super::execution::storage)?
            {
                Some(image) => SnapshotCapture::capture_rooted(
                    group,
                    image,
                    options.storage.store.clone(),
                    limits,
                ),
                None => SnapshotCapture::capture_empty(group, schema, limits),
            },
        };
        capture.map_err(super::super::execution::storage)
    }

    pub fn retained_snapshot(
        &mut self,
        group: &GroupLog,
        schema: [u8; 32],
        expected: &::raft::prelude::Snapshot,
        limits: SnapshotLimits,
    ) -> Result<SnapshotCapture, ExecutionError> {
        match self {
            Self::Materialized { options, .. } => {
                SnapshotCapture::retained(group, &options.images, schema, expected, limits)
            }
            Self::Rooted { options, applier } => {
                let image = applier
                    .retained_snapshot(expected, &mut vec![0; EXTENT_BYTES as usize])
                    .map_err(super::super::execution::storage)?;
                SnapshotCapture::retained_rooted(
                    group,
                    schema,
                    expected,
                    image,
                    options.storage.store.clone(),
                    limits,
                )
            }
        }
        .map_err(super::super::execution::storage)
    }

    pub fn receive_snapshot(
        &self,
        received: ReceivedSnapshot,
    ) -> Result<(PreparedSnapshot, Permit), super::super::snapshot::SnapshotError> {
        match self {
            Self::Materialized { options, .. } => received.finish(&options.images),
            Self::Rooted { options, .. } => received.finish_root(&options.storage.store),
        }
    }

    pub fn publish_snapshot(
        &mut self,
        group: &GroupLog,
        prepared: PreparedSnapshot,
    ) -> Result<Option<PublishedExecutionImage>, ExecutionError> {
        let has_image = group
            .applied()
            .map_err(super::super::execution::storage)?
            .image
            .is_some();
        match self {
            Self::Materialized { applier, .. } => {
                if has_image {
                    Ok(Some(PublishedExecutionImage::Materialized(
                        prepared
                            .publish(applier)
                            .map_err(super::super::execution::storage)?,
                    )))
                } else {
                    prepared
                        .publish_empty(applier)
                        .map_err(super::super::execution::storage)?;
                    Ok(None)
                }
            }
            Self::Rooted { applier, .. } => {
                if has_image {
                    Ok(Some(PublishedExecutionImage::Rooted(
                        prepared
                            .publish_root(applier, &mut vec![0; EXTENT_BYTES as usize])
                            .map_err(super::super::execution::storage)?,
                    )))
                } else {
                    prepared
                        .publish_empty_root(applier)
                        .map_err(super::super::execution::storage)?;
                    Ok(None)
                }
            }
        }
    }

    pub fn recover(
        group: GroupLog,
        class: &RaftClass,
        storage: CellStorage,
        accounting: &GenerationAccounting,
    ) -> Result<Self, ExecutionError> {
        if let Some(rooted) = storage.rooted {
            storage.execution.validate()?;
            let scope = &group.identity().scope;
            let mut key = Sha256::new();
            key.update(b"cellar-private-workspace-1");
            key.update(scope.cluster.bytes());
            key.update(scope.namespace.bytes());
            key.update(scope.group.bytes());
            key.update(scope.incarnation.bytes());
            key.update((scope.cell.len() as u64).to_be_bytes());
            key.update(scope.cell.as_bytes());
            key.update(group.birth_digest);
            let workspace_id = key.finalize().into();
            let applier = RootImageApplier::recover(
                group,
                rooted.store.clone(),
                class.schema_digest(),
                storage.execution.envelope,
                storage.execution.max_image_bytes,
                &mut vec![0; EXTENT_BYTES as usize],
            )
            .map_err(super::super::execution::storage)?;
            cellar_ltx::host::create_dir_all_durable(
                storage.host.filesystem().as_ref(),
                &rooted.cache_directory,
            )
            .map_err(super::super::execution::storage)?;
            return Ok(Self::Rooted {
                options: Box::new(RootOwnerOptions {
                    storage: rooted,
                    workspace_id,
                    host: storage.host,
                    query_services: storage.services,
                    application_digest: class.schema_digest(),
                    limits: storage.execution,
                }),
                applier: Box::new(applier),
            });
        }

        let images = GenerationStore::open_with_scratch(
            storage.directory.join("images"),
            storage.host.filesystem(),
            storage.images,
            Some(accounting),
            storage.snapshot_scratch,
        )
        .map_err(super::super::execution::storage)?;
        let applier = ImageApplier::recover(
            group,
            images.clone(),
            class.schema_digest(),
            storage.execution.envelope,
        )
        .map_err(super::super::execution::storage)?;
        Ok(Self::Materialized {
            options: Box::new(PrivateOptions {
                directory: storage.directory.join("private"),
                host: storage.host,
                sqlite_vfs: None,
                images,
                query_services: storage.services,
                application_digest: class.schema_digest(),
                limits: storage.execution,
            }),
            applier: Box::new(applier),
        })
    }

    pub fn current(&self) -> Option<PublishedExecutionImage> {
        match self {
            Self::Materialized { applier, .. } => {
                applier.current().map(PublishedExecutionImage::Materialized)
            }
            Self::Rooted { applier, .. } => applier.current().map(PublishedExecutionImage::Rooted),
        }
    }

    pub fn queryable(
        &self,
    ) -> Result<Option<PublishedExecutionImage>, super::super::apply::ImageApplyError> {
        match self {
            Self::Materialized { applier, .. } => Ok(applier
                .queryable()?
                .map(PublishedExecutionImage::Materialized)),
            Self::Rooted { applier, .. } => {
                Ok(applier.queryable()?.map(PublishedExecutionImage::Rooted))
            }
        }
    }

    pub fn open_private(
        &self,
        authority: CommandAuthority,
    ) -> Result<PrivateExecutor, ExecutionError> {
        match self {
            Self::Materialized { options, applier } => {
                PrivateExecutor::open((**options).clone(), authority, applier.current())
            }
            Self::Rooted { options, applier } => {
                let workspace = options
                    .storage
                    .store
                    .create_workspace(
                        options.workspace_id,
                        RootPrivateOptions::workspace_bounds(options.limits)?,
                    )
                    .map_err(|error| match error {
                        cellar_ltx::internal::StoreError::Limit => {
                            ExecutionError::Refused(CommandFailure::Overloaded)
                        }
                        error => super::super::execution::storage(error),
                    })?;
                PrivateExecutor::open_rooted(
                    RootPrivateOptions {
                        workspace,
                        host: options.host.clone(),
                        sqlite_vfs: None,
                        images: options.storage.store.clone(),
                        query_services: options.query_services.clone(),
                        application_digest: options.application_digest,
                        limits: options.limits,
                        cache_directory: options.storage.cache_directory.clone(),
                        cache_pool: options.storage.cache_pool.clone(),
                        cache: options.storage.cache,
                    },
                    authority,
                    applier.current(),
                )
            }
        }
    }

    pub fn prepare(
        &mut self,
        ticket: ApplyTicket,
        entry: &::raft::prelude::Entry,
        image: ImageId,
        private: Option<&mut PrivateExecutor>,
    ) -> Result<ApplyCompletion, super::super::apply::ImageApplyError> {
        match self {
            Self::Materialized { applier, .. } => {
                applier.prepare_private(ticket, entry, image, private)
            }
            Self::Rooted { applier, .. } => {
                applier.prepare(ticket, entry, &mut vec![0; EXTENT_BYTES as usize])
            }
        }
    }

    pub fn publish(
        &mut self,
        ticket: &ApplyTicket,
    ) -> Result<PublishedExecutionImage, super::super::apply::ImageApplyError> {
        match self {
            Self::Materialized { applier, .. } => Ok(PublishedExecutionImage::Materialized(
                applier.publish(ticket)?,
            )),
            Self::Rooted { applier, .. } => Ok(PublishedExecutionImage::Rooted(
                applier.publish(ticket, &mut vec![0; EXTENT_BYTES as usize])?,
            )),
        }
    }

    pub fn query(
        &self,
        published: &PublishedExecutionImage,
        class: &RaftClass,
        contract: &OperationContract,
        input: &[u8],
        control: Arc<AttemptControl>,
    ) -> Result<RegisteredQueryOutput, ExecutionError> {
        match (self, published) {
            (Self::Materialized { options, .. }, PublishedExecutionImage::Materialized(image)) => {
                query_registered_controlled(
                    image,
                    class,
                    contract,
                    input,
                    options.query_services.clone(),
                    options.limits,
                    Some(control),
                )
            }
            (Self::Rooted { options, .. }, PublishedExecutionImage::Rooted(image)) => {
                query_registered_rooted_controlled(
                    image,
                    class,
                    contract,
                    input,
                    RootQueryOptions {
                        services: options.query_services.clone(),
                        execution: options.limits,
                        cache_directory: options.storage.cache_directory.clone(),
                        scratch: options.storage.cache_pool.clone(),
                        cache: options.storage.cache,
                    },
                    Some(control),
                )
            }
            _ => Err(ExecutionError::Invalid("query storage kind differs")),
        }
    }

    pub fn scan_effects(
        &self,
        class: &RaftClass,
        now: u64,
        prefer_delivery: bool,
        reservation: Permit,
    ) -> Result<Option<super::super::effects::scan::EffectScan>, ExecutionError> {
        use super::super::{effects::scan, execution::storage};
        match self {
            Self::Materialized { options, applier } => applier
                .queryable()
                .map_err(storage)?
                .map(|image| {
                    scan::scan(
                        image,
                        class,
                        options.query_services.clone(),
                        now,
                        prefer_delivery,
                        reservation,
                    )
                })
                .transpose()
                .map_err(storage),
            Self::Rooted { options, applier } => applier
                .queryable()
                .map_err(storage)?
                .map(|image| {
                    let database = options.database(&image)?;
                    scan::scan_connection(
                        database.connection(),
                        image.scope(),
                        image.position(),
                        class,
                        now,
                        prefer_delivery,
                        reservation,
                    )
                    .map_err(storage)
                })
                .transpose(),
        }
    }
}
