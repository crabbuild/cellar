// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Physical dispatch beneath the SQL worker's existing command/read controller.
//! Extent selection is internal until snapshot, seed and archive transport share
//! this owner. An unsupported physical operation refuses rather than falling
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

    pub fn materialized(&mut self) -> Result<(&PrivateOptions, &mut ImageApplier), ExecutionError> {
        match self {
            Self::Materialized { options, applier } => Ok((options, applier)),
            Self::Rooted { .. } => Err(ExecutionError::Invalid(
                "extent transport/effects are not installed",
            )),
        }
    }
}
