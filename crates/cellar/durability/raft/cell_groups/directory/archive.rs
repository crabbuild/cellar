// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Namespace-owned archive identity. Recovery reads the expected identity from
//! immutable catalog metadata, never by trusting a mutable registry's identity.
use super::*;
use crate::raft::cell_groups::{
    CellGroupArchive, CellGroupArchiveCreation, CellGroupArchiveError, CellGroupArchiveExecution,
    CellGroupArchivePool, CellGroupArchiveRegistry, PreparedCellGroupArchiveCreation,
};
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub enum CellGroupArchiveBindingError {
    Catalog(CatalogError),
    Archive(CellGroupArchiveError),
}

impl std::fmt::Display for CellGroupArchiveBindingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "group archive binding: {self:?}")
    }
}

impl std::error::Error for CellGroupArchiveBindingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Catalog(error) => error,
            Self::Archive(error) => error,
        })
    }
}

impl From<CatalogError> for CellGroupArchiveBindingError {
    fn from(error: CatalogError) -> Self {
        Self::Catalog(error)
    }
}

impl From<CellGroupArchiveError> for CellGroupArchiveBindingError {
    fn from(error: CellGroupArchiveError) -> Self {
        Self::Archive(error)
    }
}

type Result<T> = std::result::Result<T, CellGroupArchiveBindingError>;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    namespace: [u8; 32],
    creation: CellGroupArchiveCreation,
}

/// Persist `encode()` before commit. This one-use initializer first binds the
/// expected identity in the namespace catalog, then creates registry counters.
/// After a crash, open the existing binding; never allocate a replacement.
/// Incomplete initialization refuses admission and requires authority recovery.
pub struct PreparedCellGroupArchiveBinding {
    directory: RangeDirectory,
    birth: CellGroupBirth,
    bytes: Vec<u8>,
    creation: PreparedCellGroupArchiveCreation,
}

impl PreparedCellGroupArchiveBinding {
    pub fn encode(&self) -> &[u8] {
        &self.bytes
    }

    pub async fn commit(
        self,
        domain: &impl CellGroupArchiveExecution,
    ) -> Result<CellGroupArchiveRegistry> {
        domain
            .run(async {
                self.directory
                    .require_current_group_birth(&self.birth)
                    .await?;
                self.directory
                    .bounded(async {
                        let observed = self
                            .directory
                            .namespace
                            .inner
                            .create_and_observe_bounded(
                                &self.directory.archive_binding_key(self.birth.route()),
                                &self.bytes,
                                wire::LIMIT,
                            )
                            .await?;
                        if observed != self.bytes {
                            return Err(CatalogError::Conflict("group archive binding"));
                        }
                        Ok(())
                    })
                    .await?;
                self.directory
                    .require_current_group_birth(&self.birth)
                    .await?;
                let registry = self.creation.commit(domain).await?;
                self.directory
                    .require_current_group_birth(&self.birth)
                    .await?;
                Ok(registry)
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }
}

impl RangeDirectory {
    fn archive_binding_key(&self, route: &GroupRoute) -> Path {
        self.key(&format!(
            "archives/{}/{}/binding",
            hex::encode(route.history.group.bytes()),
            hex::encode(route.history.incarnation.bytes())
        ))
    }

    fn group_archive(
        &self,
        pool: &CellGroupArchivePool,
        route: &GroupRoute,
    ) -> Result<CellGroupArchive> {
        Ok(pool.for_group(
            self.namespace.inner.store.clone(),
            Path::from(format!(
                "{}cell-group-archives",
                self.namespace.data_prefix()
            )),
            route.history,
        )?)
    }

    /// Prepare a fresh namespace-owned archive for a selected immutable birth.
    /// Use one node-shared archive pool. No caller-selected backend or data key
    /// can substitute another group's archive. This grants no serving authority.
    pub async fn prepare_group_archive_binding(
        &self,
        domain: &impl CellGroupArchiveExecution,
        pool: &CellGroupArchivePool,
        birth: &CellGroupBirth,
    ) -> Result<PreparedCellGroupArchiveBinding> {
        domain
            .run(async {
                self.require_current_group_birth(birth).await?;
                let archive = self.group_archive(pool, birth.route())?;
                let creation = archive
                    .prepare_registry_creation(domain, birth.route().clone())
                    .await?;
                let bytes = record::encode(
                    wire::GROUP_ARCHIVE,
                    &Binding {
                        namespace: self.namespace.root_digest(),
                        creation: creation.intent().clone(),
                    },
                    wire::LIMIT,
                )?;
                Ok(PreparedCellGroupArchiveBinding {
                    directory: self.clone(),
                    birth: birth.clone(),
                    bytes,
                    creation,
                })
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }

    /// Reopen only the immutable expected identity. Missing binding, identity or
    /// counters fail closed, including after every voter disk has disappeared.
    /// This does not recreate metadata, fence old owners or authorize new voters.
    pub async fn open_group_archive(
        &self,
        domain: &impl CellGroupArchiveExecution,
        pool: &CellGroupArchivePool,
        birth: &CellGroupBirth,
    ) -> Result<CellGroupArchiveRegistry> {
        self.open_selected_archive(domain, pool, birth, None).await
    }

    pub(crate) async fn verify_group_archive_binding(
        &self,
        domain: &impl CellGroupArchiveExecution,
        pool: &CellGroupArchivePool,
        birth: &CellGroupBirth,
        intent: &[u8],
    ) -> Result<()> {
        self.open_selected_archive(domain, pool, birth, Some(intent))
            .await?;
        Ok(())
    }

    /// Observe an exact rooted intent during operator startup. Absence only
    /// delays admission. An invalid identity or binding refuses immediately.
    pub(crate) async fn observe_group_archive_binding(
        &self,
        domain: &impl CellGroupArchiveExecution,
        pool: &CellGroupArchivePool,
        birth: &CellGroupBirth,
        intent: &[u8],
    ) -> Result<bool> {
        domain
            .run(async {
                self.require_current_group_birth(birth).await?;
                let Some(binding) = self.read_archive_binding(birth, Some(intent)).await? else {
                    return Ok(false);
                };
                match self
                    .group_archive(pool, birth.route())?
                    .open_registry(domain, &binding.creation)
                    .await
                {
                    Ok(_) => (),
                    Err(CellGroupArchiveError::MissingRegistry) => return Ok(false),
                    Err(error) => return Err(error.into()),
                }
                self.require_current_group_birth(birth).await?;
                Ok(true)
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }

    async fn open_selected_archive(
        &self,
        domain: &impl CellGroupArchiveExecution,
        pool: &CellGroupArchivePool,
        birth: &CellGroupBirth,
        intent: Option<&[u8]>,
    ) -> Result<CellGroupArchiveRegistry> {
        domain
            .run(async {
                self.require_current_group_birth(birth).await?;
                let registry = self
                    .open_archive_binding(domain, pool, birth, intent)
                    .await?;
                self.require_current_group_birth(birth).await?;
                Ok(registry)
            })
            .await
            .map_err(|_| CellGroupArchiveError::Stopped)?
    }

    // The recovery owner checks the exact intent, selected source, immutable
    // birth and topology lock. Ordinary archive admission remains fenced.
    pub(super) async fn open_recovery_archive(
        &self,
        domain: &impl CellGroupArchiveExecution,
        pool: &CellGroupArchivePool,
        birth: &CellGroupBirth,
    ) -> Result<CellGroupArchiveRegistry> {
        self.open_archive_binding(domain, pool, birth, None).await
    }

    pub(super) async fn open_retired_source_archive(
        &self,
        domain: &impl CellGroupArchiveExecution,
        pool: &CellGroupArchivePool,
        route: &GroupRoute,
    ) -> Result<CellGroupArchiveRegistry> {
        let birth = self.bounded(self.open_historical_birth(route)).await?;
        self.open_archive_binding(domain, pool, &birth, None).await
    }

    async fn open_archive_binding(
        &self,
        domain: &impl CellGroupArchiveExecution,
        pool: &CellGroupArchivePool,
        birth: &CellGroupBirth,
        intent: Option<&[u8]>,
    ) -> Result<CellGroupArchiveRegistry> {
        let binding = self
            .read_archive_binding(birth, intent)
            .await?
            .ok_or(CatalogError::Corrupt("missing group archive binding"))?;
        Ok(self
            .group_archive(pool, birth.route())?
            .open_registry(domain, &binding.creation)
            .await?)
    }

    async fn read_archive_binding(
        &self,
        birth: &CellGroupBirth,
        intent: Option<&[u8]>,
    ) -> Result<Option<Binding>> {
        let Some((bytes, _)) = self
            .bounded(
                self.namespace
                    .inner
                    .read_versioned_bounded(&self.archive_binding_key(birth.route()), wire::LIMIT),
            )
            .await?
        else {
            return Ok(None);
        };
        let binding: Binding = record::decode(wire::GROUP_ARCHIVE, &bytes, wire::LIMIT)?;
        if intent.is_some_and(|intent| intent != bytes.as_slice()) {
            return Err(CatalogError::Conflict("group archive intent").into());
        }
        if binding.namespace != self.namespace.root_digest()
            || binding.creation.route() != birth.route()
        {
            return Err(CatalogError::Corrupt("group archive binding identity").into());
        }
        Ok(Some(binding))
    }

    /// Cleanup may outlive the selected birth. Reopen only the exact immutable
    /// catalog binding recorded by this engine; no initialization or serving
    /// authority is supplied by an abandoned upload intent.
    pub(in crate::raft) async fn open_seed_cleanup_archive(
        &self,
        domain: &impl CellGroupArchiveExecution,
        pool: &CellGroupArchivePool,
        expected: &CellGroupArchiveCreation,
    ) -> Result<CellGroupArchiveRegistry> {
        let route = expected.route();
        let config = self.namespace_config();
        if route.history.cluster != config.cluster || route.history.namespace != config.namespace {
            return Err(CatalogError::Corrupt("seed cleanup namespace").into());
        }
        let bytes = self
            .bounded(async {
                self.namespace
                    .inner
                    .read_versioned_bounded(&self.archive_binding_key(route), wire::LIMIT)
                    .await?
                    .map(|(bytes, _)| bytes)
                    .ok_or(CatalogError::Corrupt("missing seed archive binding"))
            })
            .await?;
        let binding: Binding = record::decode(wire::GROUP_ARCHIVE, &bytes, wire::LIMIT)?;
        if binding.namespace != self.namespace.root_digest() || &binding.creation != expected {
            return Err(CatalogError::Corrupt("seed archive binding changed").into());
        }
        Ok(self
            .group_archive(pool, route)?
            .open_registry(domain, expected)
            .await?)
    }
}
