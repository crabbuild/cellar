use super::*;
use crate::raft::catalog::{BirthDisposition, GroupBirth};
use crate::raft::fleet_catalog::histories::{Member, Origin};

impl FleetCatalog {
    pub(super) async fn import_inventory_step(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        namespace: &NamespaceCatalog,
        record: &mut Record,
        timeout: Duration,
    ) -> Result<Vec<FleetChange>> {
        let step = namespace
            .import_birth_step(&record.cut, &self.birth_owner()?, record.cursor.as_deref())
            .await
            .map_err(catalog_error)?;
        let Some(entry) = step.entry else {
            if record.cut.count != 0 || record.inventoried != 0 || step.next.is_some() {
                return Err(FleetCatalogError::Corrupt("incomplete import inventory"));
            }
            record.phase = FleetImportPhase::Coverage;
            return Ok(Vec::new());
        };
        let proposal = entry.proposal();
        let decision = match namespace
            .birth_disposition(&proposal.scope.cell)
            .await
            .map_err(catalog_error)?
        {
            Some(decision) => decision,
            None => namespace
                .cancel_birth(
                    proposal.clone(),
                    ProposalId::new(record.cut.operation)
                        .map_err(|_| FleetCatalogError::Corrupt("import cancellation"))?,
                    timeout,
                )
                .await
                .map_err(catalog_error)?,
        };
        let history = match &decision {
            BirthDisposition::Live(birth)
                if birth.scope() == &proposal.scope
                    && birth.digest() == entry.expected_digest() =>
            {
                FleetHistory::observed(
                    self.binding(birth.scope())?,
                    namespace,
                    birth,
                    ProposalId::new(request.id)
                        .map_err(|_| FleetCatalogError::Invalid("import admission"))?,
                    ProposalId::new(request.operation)
                        .map_err(|_| FleetCatalogError::Invalid("import operation"))?,
                )?
            }
            BirthDisposition::Cancelled { scope, operation } if scope == &proposal.scope => {
                FleetHistory {
                    binding: self.binding(scope)?,
                    schema_digest: namespace.config().schema_digest,
                    namespace_root: namespace.root_digest(),
                    birth_digest: entry.expected_digest(),
                    admission: request.id,
                    operation: request.operation,
                    voters: proposal.voters.iter().copied().map(Member::from).collect(),
                    learners: proposal
                        .learners
                        .iter()
                        .copied()
                        .map(Member::from)
                        .collect(),
                    state: FleetHistoryState::Cancelled {
                        operation: operation.bytes(),
                    },
                    origin: Origin::Observed,
                }
            }
            _ => return Err(FleetCatalogError::Conflict),
        };
        history.validate()?;
        let changes = self
            .import_history_changes(
                snapshot,
                namespace,
                &history,
                match &decision {
                    BirthDisposition::Live(birth) => Some(birth),
                    _ => None,
                },
            )
            .await?;
        record.inventoried = record
            .inventoried
            .checked_add(1)
            .ok_or(FleetCatalogError::Limit)?;
        if record.inventoried > record.cut.count
            || (step.next.is_none() != (record.inventoried == record.cut.count))
        {
            return Err(FleetCatalogError::Corrupt(
                "import inventory count/continuation",
            ));
        }
        record.cursor = step.next;
        if record.inventoried == record.cut.count {
            record.phase = FleetImportPhase::Coverage;
        }
        Ok(changes)
    }

    async fn import_history_changes(
        &self,
        snapshot: &FleetSnapshot,
        namespace: &NamespaceCatalog,
        history: &FleetHistory,
        birth: Option<&GroupBirth>,
    ) -> Result<Vec<FleetChange>> {
        let binding_key = history
            .binding
            .key()
            .map_err(|_| FleetCatalogError::Corrupt("import history key"))?;
        let names = [history.name_key()?, history.group_key()?];
        if let Some(bytes) = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Histories.slot()],
                &binding_key,
            )
            .await?
        {
            let saved = FleetHistory::decode(&bytes)?;
            if saved.binding != history.binding
                || saved.namespace_root != history.namespace_root
                || saved.schema_digest != history.schema_digest
                || saved.birth_digest != history.birth_digest
                || saved.voters != history.voters
                || saved.learners != history.learners
                || saved.state != history.state
                || birth.is_some_and(|birth| !saved.check_birth(namespace, birth))
            {
                return Err(FleetCatalogError::Conflict);
            }
            for name in &names {
                if self
                    .inner
                    .lookup(snapshot.head.tables[FleetTable::Policy.slot()], name)
                    .await?
                    .as_deref()
                    != Some(binding_key.as_bytes())
                {
                    return Err(FleetCatalogError::Corrupt("import history identity index"));
                }
            }
            return Ok(Vec::new());
        }
        if snapshot.head.history_count >= HISTORY_LIMIT {
            return Err(FleetCatalogError::Limit);
        }
        for name in &names {
            if self
                .inner
                .lookup(snapshot.head.tables[FleetTable::Policy.slot()], name)
                .await?
                .is_some()
            {
                return Err(FleetCatalogError::Conflict);
            }
        }
        Ok(vec![
            FleetChange {
                table: FleetTable::Histories,
                key: binding_key.clone(),
                value: Some(history.encode()?),
            },
            FleetChange {
                table: FleetTable::Policy,
                key: names[0].clone(),
                value: Some(binding_key.as_bytes().to_vec()),
            },
            FleetChange {
                table: FleetTable::Policy,
                key: names[1].clone(),
                value: Some(binding_key.into_bytes()),
            },
        ])
    }
}
