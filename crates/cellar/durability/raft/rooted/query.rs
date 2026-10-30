// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

use super::RootPublishedImage;
use crate::application::consensus::{CommittedVersion, OperationContract, RaftClass};
use crate::raft::execution::{
    validate_registered_query, ExecutionError, ExecutionLimits, RegisteredQueryOutput,
};
use cellar_ltx::{generation::ReadServices, internal::CacheLimits, stream_snapshot::ScratchPool};
use std::path::PathBuf;

/// The host owns and admits this cache independently of durable node extents.
pub struct RootQueryOptions {
    pub services: ReadServices,
    pub execution: ExecutionLimits,
    pub cache_directory: PathBuf,
    pub scratch: ScratchPool,
    pub cache: CacheLimits,
}

/// Run the registered query against the exact marker-selected root. Its version
/// remains pinned for the callback. A controller must authorize consistency and
/// routing before choosing this image; this physical API grants neither.
pub fn query_registered_rooted(
    published: &RootPublishedImage,
    class: &RaftClass,
    contract: &OperationContract,
    input: &[u8],
    options: RootQueryOptions,
) -> Result<RegisteredQueryOutput, ExecutionError> {
    query_registered_rooted_controlled(published, class, contract, input, options, None)
}

pub(crate) fn query_registered_rooted_controlled(
    published: &RootPublishedImage,
    class: &RaftClass,
    contract: &OperationContract,
    input: &[u8],
    options: RootQueryOptions,
    cancel: Option<std::sync::Arc<crate::application::consensus::client::AttemptControl>>,
) -> Result<RegisteredQueryOutput, ExecutionError> {
    validate_registered_query(
        class,
        contract,
        input,
        published.application_digest(),
        options.execution,
    )?;
    let image = published
        .image()
        .view()
        .image(options.cache_directory, options.scratch, options.cache)
        .map_err(|error| ExecutionError::Storage(error.into()))?;
    let database = crate::storage::open_immutable_extent_image(
        image,
        crate::storage::ImmutableQueryOptions {
            services: options.services,
            sqlite_vec: false,
        },
    )
    .map_err(ExecutionError::Storage)?;
    if let Some(cancel) = cancel {
        database
            .connection()
            .progress_handler(1000, Some(cancel.interrupt_check()))
            .map_err(|error| ExecutionError::Storage(error.into()))?;
    }
    let version = CommittedVersion {
        scope: published.scope().clone(),
        position: published.position(),
    };
    let result = class
        .invoke_group_query(contract, &version, &database, input)
        .map_err(ExecutionError::Storage)?;
    if result.len() > options.execution.max_result_bytes {
        return Err(ExecutionError::Refused(
            cellar_logic::consensus::CommandFailure::Overloaded,
        ));
    }
    Ok(RegisteredQueryOutput { result, version })
}
