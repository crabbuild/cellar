// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One rooted physical intent per step, including cancelled pending groups.
use super::*;
use crate::raft::cell_groups::{CellGroupBirth, CellGroupBirthDisposition};

impl FleetCatalog {
    pub(super) async fn import_inventory_step(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
        directory: &RangeDirectory,
        record: &mut Record,
    ) -> Result<Vec<FleetChange>> {
        directory
            .check_group_import(&record.cut, &self.birth_owner()?, false)
            .await
            .map_err(catalog_error)?;
        let step = directory
            .namespace()
            .cell_group_import_step(
                directory,
                &record.cut,
                &self.birth_owner()?,
                record.cursor.as_deref(),
            )
            .await
            .map_err(catalog_error)?;
        let Some(proposal) = step.entry else {
            if record.cut.count != 0 || record.inventoried != 0 || step.next.is_some() {
                return Err(FleetCatalogError::Corrupt(
                    "incomplete group import inventory",
                ));
            }
            record.phase = FleetImportPhase::Coverage;
            return Ok(Vec::new());
        };
        self.require_namespace_declaration(snapshot, directory.namespace(), None)
            .await?;
        let decision = directory
            .settle_import_group(&proposal, &record.cut, &self.birth_owner()?)
            .await
            .map_err(catalog_error)?;
        let admission = ProposalId::new(request.id)
            .map_err(|_| FleetCatalogError::Invalid("group import admission"))?;
        let operation = ProposalId::new(request.operation)
            .map_err(|_| FleetCatalogError::Invalid("group import operation"))?;
        let history = match &decision {
            CellGroupBirthDisposition::Live(birth) if birth.route() == proposal.route() => {
                FleetHistory::observed(
                    self.binding(&proposal.route().history)?,
                    directory,
                    birth,
                    admission,
                    operation,
                )?
            }
            CellGroupBirthDisposition::Cancelled {
                history,
                operation: cancellation,
            } if *history == proposal.route().history => FleetHistory::cancelled_group(
                self.binding(history)?,
                directory,
                &proposal,
                admission,
                operation,
                *cancellation,
            )?,
            _ => return Err(FleetCatalogError::Conflict),
        };
        let changes = self
            .import_history_changes(
                snapshot,
                directory,
                &history,
                match &decision {
                    CellGroupBirthDisposition::Live(birth) => Some(birth),
                    _ => None,
                },
            )
            .await?;
        record.inventoried = record
            .inventoried
            .checked_add(1)
            .ok_or(FleetCatalogError::Limit)?;
        if record.inventoried > record.cut.count
            || step.next.is_none() != (record.inventoried == record.cut.count)
        {
            return Err(FleetCatalogError::Corrupt(
                "group import count/continuation",
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
        directory: &RangeDirectory,
        history: &FleetHistory,
        birth: Option<&CellGroupBirth>,
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
                || birth.is_some_and(|birth| !saved.check_birth(directory, birth))
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
