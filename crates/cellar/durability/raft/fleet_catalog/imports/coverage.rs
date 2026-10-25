use super::*;
use crate::raft::catalog::BirthDisposition;
use crate::raft::member_state::FleetMembershipPermit;
use records::{Decision, Voter, Witness};
use std::collections::BTreeSet;

impl FleetCatalog {
    /// Confirm that every retained proof and history is still readable before
    /// the irreversible namespace effect. Missing pages are never empty sets.
    pub(super) async fn verify_complete_import(
        &self,
        snapshot: &FleetSnapshot,
        namespace: &NamespaceCatalog,
        record: &Record,
    ) -> Result<()> {
        self.verify_import_coverage(snapshot, namespace, record, true)
            .await
    }

    pub(in crate::raft::fleet_catalog) async fn verify_imported_inventory(
        &self,
        snapshot: &FleetSnapshot,
        namespace: &NamespaceCatalog,
    ) -> Result<()> {
        let record = self
            .import_record(snapshot, namespace)
            .await
            .map_err(|e| match e {
                FleetCatalogError::Conflict => FleetCatalogError::InventoryIncomplete,
                other => other,
            })?;
        if record.phase != records::FleetImportPhase::Complete || record.stopped {
            return Err(FleetCatalogError::InventoryIncomplete);
        }
        namespace
            .check_birth_import(&record.cut, &self.birth_owner()?, true)
            .await
            .map_err(catalog_error)?;
        self.verify_import_coverage(snapshot, namespace, &record, false)
            .await
    }

    async fn verify_import_coverage(
        &self,
        snapshot: &FleetSnapshot,
        namespace: &NamespaceCatalog,
        record: &Record,
        handoff: bool,
    ) -> Result<()> {
        let mut after = None;
        let mut seen = 0u64;
        loop {
            let page = self
                .inner
                .scan(record.coverage, after.as_ref(), 128)
                .await?;
            for value in page.entries {
                seen = seen
                    .checked_add(1)
                    .filter(|n| *n <= record.cut.count)
                    .ok_or(FleetCatalogError::Corrupt("extra import coverage"))?;
                let witness: Witness = decode(records::WITNESS_KIND, &value.value, 4096)?;
                let entry = namespace
                    .import_birth_entry(&record.cut, &self.birth_owner()?, &value.key)
                    .await
                    .map_err(catalog_error)?;
                let scope = &entry.proposal().scope;
                let binding = self.binding(scope)?;
                let history_key = binding
                    .key()
                    .map_err(|_| FleetCatalogError::Corrupt("import witness binding"))?;
                let history = self.history_inner(snapshot, &history_key).await?;
                if witness.history != history_key
                    || witness.birth != entry.expected_digest()
                    || history.birth_digest != witness.birth
                    || history.namespace_root != record.cut.root
                    || history.schema_digest != record.schema
                {
                    return Err(FleetCatalogError::Corrupt("import witness history"));
                }
                match (
                    witness.decision,
                    namespace
                        .birth_disposition(&scope.cell)
                        .await
                        .map_err(catalog_error)?,
                ) {
                    (
                        Decision::Cancelled { operation },
                        Some(BirthDisposition::Cancelled {
                            scope: actual,
                            operation: op,
                        }),
                    ) => {
                        if actual != *scope
                            || op.bytes() != operation
                            || history.state != (FleetHistoryState::Cancelled { operation })
                        {
                            return Err(FleetCatalogError::Corrupt("import witness disposition"));
                        }
                    }
                    (
                        Decision::Guarded {
                            node,
                            disk,
                            process,
                            member,
                            barrier,
                            applied,
                            term,
                            membership_revision,
                            consumed_sequence,
                            phase,
                            voters,
                        },
                        Some(BirthDisposition::Live(birth)),
                    ) if history.state == FleetHistoryState::Live
                        && history.check_birth(namespace, &birth) =>
                    {
                        let valid_id = |id| cellar_logic::consensus::IncarnationId::new(id).is_ok();
                        if !valid_id(node)
                            || !valid_id(disk)
                            || !valid_id(process)
                            || member == 0
                            || barrier == 0
                            || applied < barrier
                            || term == 0
                            || membership_revision == 0
                            || membership_revision > applied
                            || consumed_sequence >= u64::MAX - 2
                            || phase.is_some_and(|p| p.active())
                            || ((consumed_sequence == 0) != phase.is_none())
                            || voters.len() != 3
                        {
                            return Err(FleetCatalogError::Corrupt("import quorum witness"));
                        }
                        let mut members = BTreeSet::new();
                        let mut nodes = BTreeSet::new();
                        for voter in &voters {
                            if voter.member == 0
                                || !valid_id(voter.node)
                                || !valid_id(voter.disk)
                                || !valid_id(voter.instance)
                                || !members.insert(voter.member)
                                || !nodes.insert(voter.node)
                            {
                                return Err(FleetCatalogError::Corrupt("import witness voters"));
                            }
                            let record = self
                                .node_records(snapshot)
                                .await?
                                .into_iter()
                                .find(|n| n.node == voter.node)
                                .ok_or(FleetCatalogError::Conflict)?;
                            let host = self.host_record(snapshot, record)?;
                            if host.disk().bytes() != voter.disk
                                || host.storage_instance().map(|id| id.bytes())
                                    != Some(voter.instance)
                                || handoff
                                    && matches!(
                                        host.state(),
                                        FleetNodeState::Removed | FleetNodeState::Evacuated
                                    )
                            {
                                return Err(FleetCatalogError::Conflict);
                            }
                            namespace
                                .verify_fleet_storage_pin(&host)
                                .await
                                .map_err(catalog_error)?;
                            namespace
                                .verify_import_member_claim(
                                    &birth,
                                    cellar_logic::consensus::MemberId::new(voter.member).map_err(
                                        |_| FleetCatalogError::Corrupt("import witness member"),
                                    )?,
                                    cellar_logic::consensus::DiskLineage::new(voter.disk).map_err(
                                        |_| FleetCatalogError::Corrupt("import witness disk"),
                                    )?,
                                )
                                .await
                                .map_err(catalog_error)?;
                        }
                        if !voters
                            .iter()
                            .any(|v| v.member == member && v.node == node && v.disk == disk)
                        {
                            return Err(FleetCatalogError::Corrupt("import witness source"));
                        }
                    }
                    _ => return Err(FleetCatalogError::Corrupt("import witness disposition")),
                }
            }
            after = page.next;
            if after.is_none() {
                break;
            }
        }
        if seen != record.cut.count {
            return Err(FleetCatalogError::Corrupt("missing import coverage"));
        }
        if handoff {
            self.no_namespace_admissions(snapshot, namespace.config().namespace)
                .await
        } else {
            Ok(())
        }
    }

    pub(super) async fn import_coverage_step(
        &self,
        snapshot: &FleetSnapshot,
        namespace: &NamespaceCatalog,
        record: &mut Record,
        observation: Option<&FleetMembershipObservation>,
    ) -> Result<Vec<FleetChange>> {
        let step = namespace
            .import_birth_step(&record.cut, &self.birth_owner()?, record.cursor.as_deref())
            .await
            .map_err(catalog_error)?;
        if let Some(entry) = step.entry {
            let proposal = entry.proposal();
            let binding = self.binding(&proposal.scope)?;
            let history_key = binding
                .key()
                .map_err(|_| FleetCatalogError::Corrupt("import binding key"))?;
            let history = self.history_inner(snapshot, &history_key).await?;
            if history.namespace_root != record.cut.root
                || history.schema_digest != record.schema
                || history.birth_digest != entry.expected_digest()
            {
                return Err(FleetCatalogError::Conflict);
            }
            let decision = match namespace
                .birth_disposition(&proposal.scope.cell)
                .await
                .map_err(catalog_error)?
            {
                Some(BirthDisposition::Cancelled { scope, operation })
                    if scope == proposal.scope
                        && history.state
                            == (FleetHistoryState::Cancelled {
                                operation: operation.bytes(),
                            })
                        && observation.is_none() =>
                {
                    Decision::Cancelled {
                        operation: operation.bytes(),
                    }
                }
                Some(BirthDisposition::Live(birth))
                    if history.state == FleetHistoryState::Live
                        && history.check_birth(namespace, &birth) =>
                {
                    let observed = observation.ok_or(FleetCatalogError::Invalid(
                        "missing live import observation",
                    ))?;
                    let status = observed.status();
                    let conf = observed.configuration();
                    if observed.replica().scope != proposal.scope
                        || status.binding != binding
                        || observed.birth_digest() != history.birth_digest
                        || observed.schema_digest() != record.schema
                        || observed.barrier().get() == 0
                        || observed.applied().index < observed.barrier()
                        || observed.membership_revision() > observed.applied().index
                        || observed.membership_revision() < status.changed_at
                        || conf.voters().len() != 3
                        || !conf.outgoing_voters().is_empty()
                        || !conf.learners().is_empty()
                        || !conf.next_learners().is_empty()
                        || conf.auto_leave()
                        || status.phase.is_some_and(|phase| phase.active())
                        || observed
                            .repair_status()
                            .map_err(|_| FleetCatalogError::Corrupt("import repair status"))?
                            .is_some_and(|repair| repair.phase.active())
                        || !conf.voters().contains(&observed.replica().member)
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                    let mut voters = Vec::new();
                    let mut nodes = BTreeSet::new();
                    for member in conf.voters() {
                        let (node, disk) = if let Some((joined, _)) = observed
                            .joined_member(*member)
                            .map_err(|_| FleetCatalogError::Corrupt("import joined member"))?
                        {
                            (joined.node, Some(joined.disk))
                        } else {
                            let initial = proposal
                                .voters
                                .iter()
                                .chain(&proposal.learners)
                                .find(|m| m.member == *member)
                                .ok_or(FleetCatalogError::Conflict)?;
                            (initial.node, None)
                        };
                        if !nodes.insert(node) {
                            return Err(FleetCatalogError::Conflict);
                        }
                        let host = self.host_from(snapshot, node).await?;
                        if matches!(
                            host.state(),
                            FleetNodeState::Removed | FleetNodeState::Evacuated
                        ) || host.storage_instance().is_none()
                            || disk.is_some_and(|disk| disk != host.disk())
                        {
                            return Err(FleetCatalogError::Conflict);
                        }
                        namespace
                            .verify_fleet_storage_pin(&host)
                            .await
                            .map_err(catalog_error)?;
                        namespace
                            .verify_import_member_claim(&birth, *member, host.disk())
                            .await
                            .map_err(catalog_error)?;
                        voters.push(Voter {
                            member: member.get(),
                            node: node.bytes(),
                            disk: host.disk().bytes(),
                            instance: host
                                .storage_instance()
                                .ok_or(FleetCatalogError::Conflict)?
                                .bytes(),
                        });
                    }
                    let source = self.host_from(snapshot, observed.replica().node).await?;
                    if source.disk() != observed.replica().disk
                        || !voters.iter().any(|v| {
                            v.member == observed.replica().member.get()
                                && v.node == observed.replica().node.bytes()
                                && v.disk == observed.replica().disk.bytes()
                        })
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                    Decision::Guarded {
                        node: observed.replica().node.bytes(),
                        disk: observed.replica().disk.bytes(),
                        process: observed.replica().process.bytes(),
                        member: observed.replica().member.get(),
                        barrier: observed.barrier().get(),
                        applied: observed.applied().index.get(),
                        term: observed.applied().term.get(),
                        membership_revision: observed.membership_revision().get(),
                        consumed_sequence: status.consumed_sequence,
                        phase: status.phase,
                        voters,
                    }
                }
                _ => return Err(FleetCatalogError::Conflict),
            };
            let name = hex::encode(hash(proposal.scope.cell.as_bytes()));
            if self.inner.lookup(record.coverage, &name).await?.is_some() {
                return Err(FleetCatalogError::Corrupt("duplicate import coverage"));
            }
            let bytes = encode(
                records::WITNESS_KIND,
                &Witness {
                    history: history_key,
                    birth: entry.expected_digest(),
                    decision,
                },
                4096,
            )?;
            record.coverage = self
                .inner
                .update(record.coverage, &name, Some(bytes), 0)
                .await?;
            record.covered = record
                .covered
                .checked_add(1)
                .ok_or(FleetCatalogError::Limit)?;
            if record.covered > record.cut.count
                || (step.next.is_none() != (record.covered == record.cut.count))
            {
                return Err(FleetCatalogError::Corrupt(
                    "import coverage count/continuation",
                ));
            }
            record.cursor = step.next;
        } else if record.cut.count != 0
            || record.covered != 0
            || step.next.is_some()
            || observation.is_some()
        {
            return Err(FleetCatalogError::Corrupt("incomplete import coverage"));
        }
        if record.covered == record.cut.count {
            self.no_namespace_admissions(snapshot, namespace.config().namespace)
                .await?;
            record.phase = FleetImportPhase::Handoff;
        }
        Ok(Vec::new())
    }

    async fn no_namespace_admissions(
        &self,
        snapshot: &FleetSnapshot,
        namespace: NamespaceId,
    ) -> Result<()> {
        let index = snapshot.head.tables[FleetTable::Admissions.slot()];
        let mut after = None;
        let mut seen = 0u64;
        loop {
            let page = self.inner.scan(index, after.as_ref(), 128).await?;
            for entry in page.entries {
                seen = seen
                    .checked_add(1)
                    .filter(|n| *n <= HISTORY_LIMIT)
                    .ok_or(FleetCatalogError::Limit)?;
                let permit = FleetMembershipPermit::decode(&entry.value)
                    .map_err(|_| FleetCatalogError::Corrupt("import admission permit"))?;
                let scope = permit
                    .binding()
                    .scope()
                    .map_err(|_| FleetCatalogError::Corrupt("import permit scope"))?;
                if scope.namespace == namespace {
                    return Err(FleetCatalogError::Conflict);
                }
            }
            after = page.next;
            if after.is_none() {
                return Ok(());
            }
        }
    }
}
