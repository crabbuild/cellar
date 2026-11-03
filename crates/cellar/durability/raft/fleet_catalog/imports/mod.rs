//! Resumable directory import. The fleet root owns progress; directory effects
//! retain frozen references until every canonical history has a committed guard.
use super::histories::{catalog_error, history_mutation, validate_request};
use super::*;
use crate::raft::catalog::{
    CellGroupImportCut, FrozenCellGroupInventory, ManagedBirthOwner, VoterPolicy,
};
use crate::raft::cell_groups::{CellGroupBirthRequest, CellGroupQuorumMembership, RangeDirectory};
use cellar_logic::consensus::{NamespaceId, ProposalId};

mod coverage;
mod inventory;
mod records;
pub use records::FleetImportPhase;
use records::Record;

/// Rooted import progress, not a current membership or node-absence proof.
#[derive(Clone, Debug)]
pub struct FleetNamespaceImport {
    record: Record,
}

impl FleetNamespaceImport {
    pub fn phase(&self) -> FleetImportPhase {
        self.record.phase
    }

    pub fn stopped(&self) -> bool {
        self.record.stopped
    }

    pub fn intents(&self) -> u64 {
        self.record.cut.count
    }

    pub fn inventoried(&self) -> u64 {
        self.record.inventoried
    }

    pub fn covered(&self) -> u64 {
        self.record.covered
    }

    pub fn operation(&self) -> [u8; 16] {
        self.record.cut.operation
    }

    pub fn namespace_root(&self) -> [u8; 32] {
        self.record.cut.root
    }

    pub fn cut_digest(&self) -> Result<[u8; 32]> {
        self.record.cut.digest().map_err(catalog_error)
    }
}

fn key(directory: NamespaceId) -> String {
    format!("namespace/{}", hex::encode(directory.bytes()))
}

impl FleetCatalog {
    /// Root the exact frozen cut before attempting its exclusive directory
    /// claim. Preparing/stopped imports grant no managed birth or maintenance.
    pub async fn begin_namespace_import(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        directory: &RangeDirectory,
        frozen: &FrozenCellGroupInventory,
        timeout: Duration,
    ) -> Result<FleetCommit> {
        self.check_snapshot(snapshot)?;
        validate_request(request)?;
        let cut = frozen.import_cut(directory).map_err(catalog_error)?;
        if request.operation != cut.operation
            || cut.count > HISTORY_LIMIT
            || directory.namespace_config().cluster.bytes() != self.inner.cluster
            || directory.namespace_config().voters != VoterPolicy::ThreeVoters
        {
            return Err(FleetCatalogError::Invalid("directory import cut/scope"));
        }
        let record = Record {
            namespace: directory.namespace_config().namespace.bytes(),
            schema: directory.namespace_config().schema_digest,
            catalog: self.birth_owner()?.catalog,
            cut,
            phase: FleetImportPhase::Preparing,
            stopped: false,
            inventoried: 0,
            covered: 0,
            cursor: None,
            coverage: None,
        };
        record.validate(self, directory)?;
        self.run(timeout, async {
            directory
                .import_directory_view(&record.cut)
                .await
                .map_err(catalog_error)?;
            self.require_namespace_declaration(snapshot, directory.namespace(), None)
                .await?;
            let mutation = history_mutation(
                request,
                vec![FleetChange {
                    table: FleetTable::Policy,
                    key: key(directory.namespace_config().namespace),
                    value: Some(record.encode()?),
                }],
            );
            mutation.validate()?;
            let current = self.inner.current().await?;
            let digest = hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?);
            if self
                .inner
                .committed(&current.head, &hex::encode(request.id), digest)
                .await?
                .is_none()
                && self
                    .inner
                    .lookup(
                        snapshot.head.tables[FleetTable::Policy.slot()],
                        &key(directory.namespace_config().namespace),
                    )
                    .await?
                    .is_some()
            {
                return Err(FleetCatalogError::Conflict);
            }
            self.commit_inner(snapshot, &mutation).await
        })
        .await
    }

    pub async fn namespace_import(
        &self,
        snapshot: &FleetSnapshot,
        directory: &RangeDirectory,
        timeout: Duration,
    ) -> Result<FleetNamespaceImport> {
        self.check_snapshot(snapshot)?;
        self.run(timeout, async {
            Ok(FleetNamespaceImport {
                record: self.import_record(snapshot, directory).await?,
            })
        })
        .await
    }

    /// Resolve the next canonical intent from rooted progress. There is no
    /// caller manifest or caller cursor that can skip an offline history.
    pub async fn next_namespace_import_birth(
        &self,
        snapshot: &FleetSnapshot,
        directory: &RangeDirectory,
        timeout: Duration,
    ) -> Result<Option<CellGroupBirthRequest>> {
        self.check_snapshot(snapshot)?;
        self.run_import(
            timeout,
            Box::pin(async {
                let record = self.import_record(snapshot, directory).await?;
                if record.stopped {
                    return Err(FleetCatalogError::Conflict);
                }
                if !matches!(
                    record.phase,
                    FleetImportPhase::Inventory | FleetImportPhase::Coverage
                ) {
                    return Ok(None);
                }
                Ok(directory
                    .namespace()
                    .cell_group_import_step(
                        directory,
                        &record.cut,
                        &self.birth_owner()?,
                        record.cursor.as_deref(),
                    )
                    .await
                    .map_err(catalog_error)?
                    .entry)
            }),
        )
        .await
    }

    /// One bounded resumable step. Inventory settles a pending birth at its
    /// immutable key; coverage requires this exact live group's opaque quorum
    /// observation. Handoff is admitted in the root before changing ownership.
    pub async fn advance_namespace_import(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        directory: &RangeDirectory,
        observation: Option<&CellGroupQuorumMembership>,
        timeout: Duration,
    ) -> Result<FleetCommit> {
        self.check_snapshot(snapshot)?;
        validate_request(request)?;
        self.run_import(
            timeout,
            Box::pin(async {
                let mut record = self.import_record(snapshot, directory).await?;
                if request.operation != record.cut.operation || record.stopped {
                    return Err(FleetCatalogError::Conflict);
                }
                if observation.is_some() && record.phase != FleetImportPhase::Coverage {
                    return Err(FleetCatalogError::Invalid("unexpected import observation"));
                }
                let changes = match record.phase {
                    FleetImportPhase::Preparing => {
                        directory
                            .namespace()
                            .change_cell_group_import(&record.cut, &self.birth_owner()?, false)
                            .await
                            .map_err(catalog_error)?;
                        record.phase = FleetImportPhase::Inventory;
                        Vec::new()
                    }
                    FleetImportPhase::Inventory => {
                        self.import_inventory_step(snapshot, request, directory, &mut record)
                            .await?
                    }
                    FleetImportPhase::Coverage => {
                        self.import_coverage_step(snapshot, directory, &mut record, observation)
                            .await?
                    }
                    FleetImportPhase::Handoff => {
                        self.verify_complete_import(snapshot, directory, &record)
                            .await?;
                        directory
                            .finish_group_import(&record.cut, &self.birth_owner()?)
                            .await
                            .map_err(catalog_error)?;
                        record.phase = FleetImportPhase::Complete;
                        Vec::new()
                    }
                    FleetImportPhase::Complete => return Err(FleetCatalogError::Conflict),
                };
                self.commit_import_record(snapshot, request, directory, record, changes)
                    .await
            }),
        )
        .await
    }

    /// Stop/resume the same operation while retaining its claim and references.
    /// Once Handoff wins, finish it; cancellation cannot race an ownership
    /// effect with a reopened legacy birth path or release unjoined work.
    pub async fn set_namespace_import_stopped(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        directory: &RangeDirectory,
        stopped: bool,
        timeout: Duration,
    ) -> Result<FleetCommit> {
        self.check_snapshot(snapshot)?;
        validate_request(request)?;
        self.run(timeout, async {
            let mut record = self.import_record(snapshot, directory).await?;
            if request.operation != record.cut.operation
                || matches!(
                    record.phase,
                    FleetImportPhase::Handoff | FleetImportPhase::Complete
                )
            {
                return Err(FleetCatalogError::Conflict);
            }
            record.stopped = stopped;
            self.commit_import_record(snapshot, request, directory, record, Vec::new())
                .await
        })
        .await
    }

    /// Replay the exact persisted candidate after a lost reply/reopen, without
    /// inventing a new ID or reconstructing an ephemeral observation. Missing
    /// candidate/journal state never reports successful admission.
    pub async fn retry_namespace_import_request(
        &self,
        request: &FleetRequestIdentity,
        directory: &RangeDirectory,
        timeout: Duration,
    ) -> Result<FleetCommit> {
        validate_request(request)?;
        self.run_import(
            timeout,
            Box::pin(async {
                let (mutation, _) = self
                    .inner
                    .read::<FleetMutation>(
                        &format!("requests/{}", hex::encode(request.id)),
                        REQUEST_KIND,
                        REQUEST_LIMIT,
                    )
                    .await?
                    .ok_or(FleetCatalogError::Unconfirmed)?;
                mutation.validate()?;
                if mutation.id != request.id
                    || mutation.operation != request.operation
                    || mutation.principal != request.principal
                    || mutation.expected_revision != request.expected_revision
                {
                    return Err(FleetCatalogError::PayloadConflict);
                }
                let last = mutation
                    .changes
                    .last()
                    .ok_or(FleetCatalogError::Corrupt("empty import retry"))?;
                if last.table != FleetTable::Policy
                    || last.key != key(directory.namespace_config().namespace)
                {
                    return Err(FleetCatalogError::Conflict);
                }
                let record: Record = decode(
                    records::KIND,
                    last.value.as_deref().ok_or(FleetCatalogError::Conflict)?,
                    4096,
                )?;
                record.validate(self, directory)?;
                if record.cut.operation != request.operation
                    || mutation.changes.len() > 4
                    || mutation
                        .changes
                        .iter()
                        .any(|c| !matches!(c.table, FleetTable::Policy | FleetTable::Histories))
                {
                    return Err(FleetCatalogError::Conflict);
                }
                let current = self.inner.current().await?;
                let digest = hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?);
                if record.phase == FleetImportPhase::Complete
                    && self
                        .inner
                        .committed(&current.head, &hex::encode(request.id), digest)
                        .await?
                        .is_none()
                {
                    self.verify_complete_import(&current, directory, &record)
                        .await?;
                }
                self.commit_inner(&current, &mutation).await
            }),
        )
        .await
    }

    async fn import_record(
        &self,
        snapshot: &FleetSnapshot,
        directory: &RangeDirectory,
    ) -> Result<Record> {
        let bytes = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Policy.slot()],
                &key(directory.namespace_config().namespace),
            )
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        let record: Record = decode(records::KIND, &bytes, 4096)?;
        record.validate(self, directory)?;
        Ok(record)
    }

    async fn commit_import_record(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        directory: &RangeDirectory,
        record: Record,
        mut changes: Vec<FleetChange>,
    ) -> Result<FleetCommit> {
        record.validate(self, directory)?;
        changes.push(FleetChange {
            table: FleetTable::Policy,
            key: key(directory.namespace_config().namespace),
            value: Some(record.encode()?),
        });
        let mutation = history_mutation(request, changes);
        mutation.validate()?;
        self.commit_inner(snapshot, &mutation).await
    }

    // Keep the large management state machine behind one owned allocation as
    // it crosses catalog/deadline/domain adapters. It must fit ordinary task
    // stacks; increasing executor stack sizes is not an admission strategy.
    async fn run_import<T>(
        &self,
        timeout: Duration,
        work: futures_util::future::BoxFuture<'_, Result<T>>,
    ) -> Result<T> {
        self.run(timeout, async {
            self.inner
                .domain
                .run(work)
                .await
                .map_err(|_| FleetCatalogError::Closed)?
        })
        .await
    }

    pub(super) async fn managed_owner_for_birth(
        &self,
        snapshot: &FleetSnapshot,
        directory: &RangeDirectory,
    ) -> Result<ManagedBirthOwner> {
        let owner = self.birth_owner()?;
        if directory.namespace_config().managed_fleet.is_some() {
            if self.bind_namespace(directory.namespace_config().clone())?
                != *directory.namespace_config()
            {
                return Err(FleetCatalogError::Conflict);
            }
        } else {
            let record = self.import_record(snapshot, directory).await?;
            if record.phase != FleetImportPhase::Complete || record.stopped {
                return Err(FleetCatalogError::Conflict);
            }
            directory
                .check_group_import(&record.cut, &owner, true)
                .await
                .map_err(catalog_error)?;
        }
        Ok(owner)
    }
}
