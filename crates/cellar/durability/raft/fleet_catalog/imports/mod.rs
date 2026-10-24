//! Resumable namespace import. The fleet root owns progress; namespace effects
//! retain frozen references until every canonical history has a committed guard.
use super::histories::{catalog_error, history_mutation, validate_request};
use super::*;
use crate::raft::catalog::{
    BirthImportCut, BirthInventoryEntry, FrozenBirthInventory, ManagedBirthOwner, NamespaceCatalog,
    VoterPolicy,
};
use crate::raft::host::FleetMembershipObservation;
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

fn key(namespace: NamespaceId) -> String {
    format!("namespace/{}", hex::encode(namespace.bytes()))
}

impl FleetCatalog {
    /// Root the exact frozen cut before attempting its exclusive namespace
    /// claim. Preparing/stopped imports grant no managed birth or maintenance.
    pub async fn begin_namespace_import(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        namespace: &NamespaceCatalog,
        frozen: &FrozenBirthInventory,
        timeout: Duration,
    ) -> Result<FleetCommit> {
        self.check_snapshot(snapshot)?;
        validate_request(request)?;
        let cut = frozen.import_cut(namespace).map_err(catalog_error)?;
        if request.operation != cut.operation
            || cut.count > HISTORY_LIMIT
            || namespace.config().cluster.bytes() != self.inner.cluster
            || namespace.config().voters != VoterPolicy::ThreeVoters
        {
            return Err(FleetCatalogError::Invalid("namespace import cut/scope"));
        }
        let record = Record {
            namespace: namespace.config().namespace.bytes(),
            schema: namespace.config().schema_digest,
            catalog: self.birth_owner()?.catalog,
            cut,
            phase: FleetImportPhase::Preparing,
            stopped: false,
            inventoried: 0,
            covered: 0,
            cursor: None,
            coverage: None,
        };
        record.validate(self, namespace)?;
        self.run(timeout, async {
            let mutation = history_mutation(
                request,
                vec![FleetChange {
                    table: FleetTable::Policy,
                    key: key(namespace.config().namespace),
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
                        &key(namespace.config().namespace),
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
        namespace: &NamespaceCatalog,
        timeout: Duration,
    ) -> Result<FleetNamespaceImport> {
        self.check_snapshot(snapshot)?;
        self.run(timeout, async {
            Ok(FleetNamespaceImport {
                record: self.import_record(snapshot, namespace).await?,
            })
        })
        .await
    }

    /// Resolve the next canonical intent from rooted progress. There is no
    /// caller manifest or caller cursor that can skip an offline history.
    pub async fn next_namespace_import_birth(
        &self,
        snapshot: &FleetSnapshot,
        namespace: &NamespaceCatalog,
        timeout: Duration,
    ) -> Result<Option<BirthInventoryEntry>> {
        self.check_snapshot(snapshot)?;
        self.run_import(
            timeout,
            Box::pin(async {
                let record = self.import_record(snapshot, namespace).await?;
                if record.stopped {
                    return Err(FleetCatalogError::Conflict);
                }
                if !matches!(
                    record.phase,
                    FleetImportPhase::Inventory | FleetImportPhase::Coverage
                ) {
                    return Ok(None);
                }
                Ok(namespace
                    .import_birth_step(&record.cut, &self.birth_owner()?, record.cursor.as_deref())
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
        namespace: &NamespaceCatalog,
        observation: Option<&FleetMembershipObservation>,
        timeout: Duration,
    ) -> Result<FleetCommit> {
        self.check_snapshot(snapshot)?;
        validate_request(request)?;
        self.run_import(
            timeout,
            Box::pin(async {
                let mut record = self.import_record(snapshot, namespace).await?;
                if request.operation != record.cut.operation || record.stopped {
                    return Err(FleetCatalogError::Conflict);
                }
                if observation.is_some() && record.phase != FleetImportPhase::Coverage {
                    return Err(FleetCatalogError::Invalid("unexpected import observation"));
                }
                let changes = match record.phase {
                    FleetImportPhase::Preparing => {
                        namespace
                            .claim_birth_import(&record.cut, &self.birth_owner()?)
                            .await
                            .map_err(catalog_error)?;
                        record.phase = FleetImportPhase::Inventory;
                        Vec::new()
                    }
                    FleetImportPhase::Inventory => {
                        self.import_inventory_step(
                            snapshot,
                            request,
                            namespace,
                            &mut record,
                            timeout,
                        )
                        .await?
                    }
                    FleetImportPhase::Coverage => {
                        self.import_coverage_step(snapshot, namespace, &mut record, observation)
                            .await?
                    }
                    FleetImportPhase::Handoff => {
                        self.verify_complete_import(snapshot, namespace, &record)
                            .await?;
                        namespace
                            .finish_birth_import(&record.cut, &self.birth_owner()?)
                            .await
                            .map_err(catalog_error)?;
                        record.phase = FleetImportPhase::Complete;
                        Vec::new()
                    }
                    FleetImportPhase::Complete => return Err(FleetCatalogError::Conflict),
                };
                self.commit_import_record(snapshot, request, namespace, record, changes)
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
        namespace: &NamespaceCatalog,
        stopped: bool,
        timeout: Duration,
    ) -> Result<FleetCommit> {
        self.check_snapshot(snapshot)?;
        validate_request(request)?;
        self.run(timeout, async {
            let mut record = self.import_record(snapshot, namespace).await?;
            if request.operation != record.cut.operation
                || matches!(
                    record.phase,
                    FleetImportPhase::Handoff | FleetImportPhase::Complete
                )
            {
                return Err(FleetCatalogError::Conflict);
            }
            record.stopped = stopped;
            self.commit_import_record(snapshot, request, namespace, record, Vec::new())
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
        namespace: &NamespaceCatalog,
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
                if last.table != FleetTable::Policy || last.key != key(namespace.config().namespace)
                {
                    return Err(FleetCatalogError::Conflict);
                }
                let record: Record = decode(
                    records::KIND,
                    last.value.as_deref().ok_or(FleetCatalogError::Conflict)?,
                    4096,
                )?;
                record.validate(self, namespace)?;
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
                    self.verify_complete_import(&current, namespace, &record)
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
        namespace: &NamespaceCatalog,
    ) -> Result<Record> {
        let bytes = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Policy.slot()],
                &key(namespace.config().namespace),
            )
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        let record: Record = decode(records::KIND, &bytes, 4096)?;
        record.validate(self, namespace)?;
        Ok(record)
    }

    async fn commit_import_record(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        namespace: &NamespaceCatalog,
        record: Record,
        mut changes: Vec<FleetChange>,
    ) -> Result<FleetCommit> {
        record.validate(self, namespace)?;
        changes.push(FleetChange {
            table: FleetTable::Policy,
            key: key(namespace.config().namespace),
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
        namespace: &NamespaceCatalog,
    ) -> Result<ManagedBirthOwner> {
        let owner = self.birth_owner()?;
        if namespace.config().managed_fleet.is_some() {
            if self.bind_namespace(namespace.config().clone())? != *namespace.config() {
                return Err(FleetCatalogError::Conflict);
            }
        } else {
            let record = self.import_record(snapshot, namespace).await?;
            if record.phase != FleetImportPhase::Complete || record.stopped {
                return Err(FleetCatalogError::Conflict);
            }
            namespace
                .check_birth_import(&record.cut, &owner, true)
                .await
                .map_err(catalog_error)?;
        }
        Ok(owner)
    }
}
