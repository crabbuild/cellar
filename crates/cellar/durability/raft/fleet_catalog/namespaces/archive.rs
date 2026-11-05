// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Archive setup over the declared namespace and its selected live histories.
use super::super::deadline::Deadline;
use super::*;
use crate::application::consensus::RaftNamespaceRegistration;
use crate::raft::{
    catalog::{Catalog, CellGroupArchiveBindingError},
    cell_groups::{
        CellGroupArchiveLimits, CellGroupArchivePool, CellGroupBirth, DirectoryLimits,
        RangeDirectory,
    },
    group_host::RaftGroupArchiveInitialization,
};
use cellar_logic::cell_group::GroupHistory;

const FIRST_GROUP_ARCHIVE_KIND: u8 = 103;
const INTENT_LIMIT: usize = 4096;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FirstGroupArchiveIntent {
    request: FleetRequestIdentity,
    publication: [u8; 32],
    binding: String,
}

impl FirstGroupArchiveIntent {
    fn binding(&self) -> Result<Vec<u8>> {
        hex::decode(&self.binding)
            .map_err(|_| FleetCatalogError::Corrupt("first group archive intent"))
    }

    fn mutation(&self, key: &str) -> Result<FleetMutation> {
        Ok(history_mutation(
            &self.request,
            vec![FleetChange {
                table: FleetTable::Policy,
                key: key.into(),
                value: Some(encode(FIRST_GROUP_ARCHIVE_KIND, self, INTENT_LIMIT)?),
            }],
        ))
    }
}

/// Operator archive setup for one exact managed namespace. Every operation
/// checks the current fleet history and selected birth. This handle grants
/// neither local replica activation nor archive coverage.
pub struct RaftManagedArchiveProvisioning {
    fleet: FleetCatalog,
    directory: RangeDirectory,
    archives: CellGroupArchivePool,
}

impl FleetCatalog {
    /// Root one archive intent for the exact published first group before
    /// entering the one-use initializer. Replay is verification only.
    pub(crate) async fn initialize_managed_first_group_archive(
        &self,
        group: &RaftManagedFirstGroup,
        request: &FleetRequestIdentity,
        archives: &CellGroupArchivePool,
        timeout: Duration,
    ) -> Result<()> {
        validate_request(request)?;
        if request.id == group.reservation.id
            || request.operation != group.reservation.operation
            || request.principal != group.reservation.principal
            || request.expected_revision <= group.reservation.expected_revision
            || self.birth_owner()? != *group.authorization.owner()
        {
            return Err(FleetCatalogError::Invalid("first group archive request"));
        }
        let provisioning = RaftManagedArchiveProvisioning {
            fleet: self.clone(),
            directory: group.directory.clone(),
            archives: archives.clone(),
        };
        let history = *group.authorization.scope();
        let key = self.first_group_archive_key(history)?;
        let publication = group.mutation.digest();
        self.run(timeout, async {
            let snapshot = self.inner.current().await?;
            let birth = provisioning.selected_birth_at(&snapshot, history).await?;
            if let Some(intent) = self.first_group_archive_intent(&snapshot, &key).await? {
                if intent.request != *request || intent.publication != publication {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                return provisioning
                    .verify_birth_archive(&birth, &intent.binding()?)
                    .await;
            }
            if snapshot.revision() != request.expected_revision {
                return Err(FleetCatalogError::Conflict);
            }
            if let Some((prior, _)) = self
                .inner
                .read::<FleetMutation>(
                    &format!("requests/{}", hex::encode(request.id)),
                    REQUEST_KIND,
                    REQUEST_LIMIT,
                )
                .await?
            {
                // A previous owner may still be resolving its CAS. Its one-use
                // initializer cannot be reconstructed from persisted bytes.
                let [change] = prior.changes.as_slice() else {
                    return Err(FleetCatalogError::PayloadConflict);
                };
                let intent: FirstGroupArchiveIntent = decode(
                    FIRST_GROUP_ARCHIVE_KIND,
                    change
                        .value
                        .as_deref()
                        .ok_or(FleetCatalogError::PayloadConflict)?,
                    INTENT_LIMIT,
                )
                .map_err(|_| FleetCatalogError::PayloadConflict)?;
                return Err(
                    if intent.request == *request
                        && intent.publication == publication
                        && prior == intent.mutation(&key)?
                    {
                        FleetCatalogError::Unconfirmed
                    } else {
                        FleetCatalogError::PayloadConflict
                    },
                );
            }
            let domain = &self.inner.domain;
            let prepared = provisioning
                .directory
                .prepare_group_archive_binding(domain, archives, &birth)
                .await
                .map_err(binding_error)?;
            let intent = FirstGroupArchiveIntent {
                request: request.clone(),
                publication,
                binding: hex::encode(prepared.encode()),
            };
            let mutation = intent.mutation(&key)?;
            self.commit_inner(&snapshot, &mutation).await?;
            prepared.commit(domain).await.map_err(binding_error)?;
            Ok(())
        })
        .await
    }

    fn first_group_archive_key(&self, history: GroupHistory) -> Result<String> {
        self.binding(&history)?
            .key()
            .map(|key| format!("first-group-archives/{key}"))
            .map_err(|_| FleetCatalogError::Invalid("archive history binding"))
    }

    async fn first_group_archive_intent(
        &self,
        snapshot: &FleetSnapshot,
        key: &str,
    ) -> Result<Option<FirstGroupArchiveIntent>> {
        let Some(bytes) = self
            .inner
            .lookup(snapshot.head.tables[FleetTable::Policy.slot()], key)
            .await?
        else {
            return Ok(None);
        };
        let intent: FirstGroupArchiveIntent =
            decode(FIRST_GROUP_ARCHIVE_KIND, &bytes, INTENT_LIMIT)?;
        let mutation = intent.mutation(key)?;
        let digest = hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?);
        if self
            .inner
            .committed(&snapshot.head, &hex::encode(mutation.id), digest)
            .await?
            .is_none()
        {
            return Err(FleetCatalogError::Corrupt("first group archive journal"));
        }
        Ok(Some(intent))
    }

    /// Open the existing, declared namespace for explicit archive setup.
    pub async fn open_managed_archive_provisioning(
        &self,
        catalog: &Catalog,
        registration: &RaftNamespaceRegistration,
        directory_limits: DirectoryLimits,
        archive_limits: CellGroupArchiveLimits,
        timeout: Duration,
    ) -> Result<RaftManagedArchiveProvisioning> {
        let archives = CellGroupArchivePool::new(archive_limits)?;
        let deadline = Deadline::new(self, timeout)?;
        let directory = self
            .open_managed_directory(catalog, registration, directory_limits, &deadline)
            .await?;
        Ok(RaftManagedArchiveProvisioning {
            fleet: self.clone(),
            directory,
            archives,
        })
    }
}

impl RaftManagedArchiveProvisioning {
    /// Prepare the one-use initializer for a confirmed managed group. Persist
    /// and sync the encoded intent before committing it.
    pub async fn prepare_archive(
        &self,
        history: GroupHistory,
        timeout: Duration,
    ) -> Result<RaftGroupArchiveInitialization> {
        self.fleet
            .run(timeout, async {
                let domain = &self.fleet.inner.domain;
                let birth = self.selected_birth(history).await?;
                let prepared = self
                    .directory
                    .prepare_group_archive_binding(domain, &self.archives, &birth)
                    .await
                    .map_err(binding_error)?;
                Ok(RaftGroupArchiveInitialization { prepared })
            })
            .await
    }

    /// Verify the exact persisted intent after a restart or uncertain commit.
    /// Missing binding, identity or counters refuse without creating metadata.
    pub async fn verify_archive(
        &self,
        history: GroupHistory,
        intent: &[u8],
        timeout: Duration,
    ) -> Result<()> {
        self.fleet
            .run(timeout, async {
                let birth = self.selected_birth(history).await?;
                self.verify_birth_archive(&birth, intent).await
            })
            .await
    }

    /// Verify the operator-owned archive against its rooted intent. A missing
    /// intent or incomplete initialization refuses startup without allocating.
    pub async fn verify_first_group_archive(
        &self,
        history: GroupHistory,
        timeout: Duration,
    ) -> Result<()> {
        let key = self.fleet.first_group_archive_key(history)?;
        self.fleet
            .run(timeout, async {
                let snapshot = self.fleet.inner.current().await?;
                let birth = self.selected_birth_at(&snapshot, history).await?;
                let intent = self
                    .fleet
                    .first_group_archive_intent(&snapshot, &key)
                    .await?
                    .ok_or(FleetCatalogError::Conflict)?;
                self.verify_birth_archive(&birth, &intent.binding()?).await
            })
            .await
    }

    async fn verify_birth_archive(&self, birth: &CellGroupBirth, intent: &[u8]) -> Result<()> {
        self.directory
            .verify_group_archive_binding(&self.fleet.inner.domain, &self.archives, birth, intent)
            .await
            .map_err(|error| match error {
                CellGroupArchiveBindingError::Archive(
                    crate::raft::cell_groups::CellGroupArchiveError::Invalid(reason),
                ) => FleetCatalogError::Corrupt(reason),
                error => binding_error(error),
            })
    }

    async fn selected_birth(&self, history: GroupHistory) -> Result<CellGroupBirth> {
        let snapshot = self.fleet.inner.current().await?;
        self.selected_birth_at(&snapshot, history).await
    }

    async fn selected_birth_at(
        &self,
        snapshot: &FleetSnapshot,
        history: GroupHistory,
    ) -> Result<CellGroupBirth> {
        let binding = self.fleet.binding(&history)?;
        let key = binding
            .key()
            .map_err(|_| FleetCatalogError::Invalid("archive history binding"))?;
        let authority = self
            .fleet
            .authorize_live_group(snapshot, binding, &key)
            .await?;
        let owner = self.fleet.birth_owner()?;
        let birth = self
            .fleet
            .inner
            .domain
            .run(
                self.directory
                    .open_managed_selected_history(history, &owner),
            )
            .await
            .map_err(|_| FleetCatalogError::Closed)?
            .map_err(catalog_error)?;
        if !authority.check_birth(&self.directory, &birth) {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(birth)
    }
}

fn binding_error(error: CellGroupArchiveBindingError) -> FleetCatalogError {
    match error {
        CellGroupArchiveBindingError::Catalog(error) => catalog_error(error),
        CellGroupArchiveBindingError::Archive(error) => error.into(),
    }
}
