// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Immutable namespace resources; private paths bind logical history, not placement.
use super::*;
use crate::application::consensus::RaftImageStorage;
use std::path::{Component, Path};

/// Namespace-level SQL/image resources. Creating a handle performs no I/O and
/// opens no SQLite connection. Each invocation derives a private history path.
#[derive(Clone)]
pub struct CellGroupNamespaceStorage {
    options: CellSqlOptions,
    bytes: usize,
}

impl CellGroupNamespaceStorage {
    pub fn new(mut options: CellSqlOptions) -> Result<Self, CellDispatchError> {
        options
            .limits()
            .validate()
            .map_err(CellDispatchError::Execution)?;
        let (path, vfs) = match &mut options {
            CellSqlOptions::Materialized(o) => (&mut o.directory, &mut o.sqlite_vfs),
            CellSqlOptions::Extents(o) => (&mut o.cache_directory, &mut o.sqlite_vfs),
        };
        validate_path(path)?;
        if vfs.as_ref().is_some_and(|name| name.len() > 128) {
            return Err(CellDispatchError::Invalid("namespace SQL VFS bounds"));
        }
        let bytes = 8192 + 2 * (path.as_os_str().len() + vfs.as_ref().map_or(0, String::len));
        *path = std::mem::take(path).into_boxed_path().into_path_buf();
        *vfs = vfs.take().map(|v| v.into_boxed_str().into_string());
        Ok(Self { options, bytes })
    }

    pub(crate) fn validate_namespace(
        &self,
        config: &crate::raft::catalog::NamespaceConfig,
    ) -> Result<(), CellDispatchError> {
        let (kind, schema) = match &self.options {
            CellSqlOptions::Materialized(o) => {
                (RaftImageStorage::Materialized, o.application_digest)
            }
            CellSqlOptions::Extents(o) => (RaftImageStorage::Extents, o.application_digest),
        };
        if config.image_storage != kind || config.schema_digest != schema {
            return Err(CellDispatchError::Invalid(
                "namespace SQL image/schema binding",
            ));
        }
        Ok(())
    }

    pub(super) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(crate) fn for_cell(&self, history: &CellHistory) -> CellSqlOptions {
        let key = hex::encode(super::super::cell_image_history_digest(history));
        let mut options = self.options.clone();
        match &mut options {
            CellSqlOptions::Materialized(o) => o.directory.push(&key),
            CellSqlOptions::Extents(o) => o.cache_directory.push(&key),
        }
        options
    }
}

fn validate_path(path: &Path) -> Result<(), CellDispatchError> {
    // Reserve a separator plus the full 64-byte history digest under the
    // existing 4096-byte invocation option bound. Never use cell IDs as paths.
    if !path.is_absolute()
        || path.as_os_str().len() > 4000
        || path.components().any(|p| matches!(p, Component::ParentDir))
    {
        return Err(CellDispatchError::Invalid("namespace SQL directory"));
    }
    Ok(())
}
