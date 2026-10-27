// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Actual exclusive engine ownership consumes return consent before endpoints.
use super::super::super::{
    boots::Boot,
    nodes::{FleetHostAuthorization, NODE_KIND},
};
use super::*;
use futures_util::future::BoxFuture;
use serde::Deserialize;

const KIND: u8 = 89;
pub(super) const PREFIX: &str = "restart-return-boot/";

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReturnedBoot {
    format: u8,
    request: FleetRequestIdentity,
    input_root: [u8; 32],
    parent: [u8; 16],
    parent_digest: [u8; 32],
    node: NodeRecord,
}

impl ReturnedBoot {
    fn key(operation: [u8; 16]) -> String {
        format!("{PREFIX}{}", hex::encode(operation))
    }

    fn validate(&self, catalog: &FleetCatalog, consent: &Return, gate: &Gate) -> Result<()> {
        validate_request(&self.request)?;
        consent.validate(catalog, gate)?;
        self.node.validate(&catalog.inner)?;
        let boot = self
            .node
            .boot
            .as_ref()
            .ok_or(FleetCatalogError::Corrupt("returned boot"))?;
        let mut expected = gate.node.clone();
        expected.epoch = expected
            .epoch
            .checked_add(1)
            .ok_or(FleetCatalogError::Limit)?;
        expected.boot = Some(boot.clone());
        if self.format != 1
            || self.input_root == [0; 32]
            || self.parent != consent.request.id
            || self.parent_digest != consent.digest()?
            || self.request.operation != consent.request.operation
            || self.request.principal != "fleet-maintenance-boot"
            || self.request.expected_revision <= consent.request.expected_revision
            || self.request.id != boot.id
            || boot.process != consent.process
            || boot.id == gate.node.boot_binding(gate.preparation.catalog)?.boot
            || boot.epoch != expected.epoch
            || self.node != expected
        {
            return Err(FleetCatalogError::Corrupt("returned engine boot"));
        }
        self.quiescence(gate)?;
        Ok(())
    }

    fn quiescence(&self, gate: &Gate) -> Result<StopSelector> {
        let selector = StopSelector {
            purpose: LifecyclePurpose::Maintenance,
            boot: self.node.boot_binding(gate.preparation.catalog)?,
            epoch: self
                .node
                .epoch
                .checked_add(1)
                .ok_or(FleetCatalogError::Limit)?,
            operation: self.request.operation,
            finalization: self.parent,
            request: self.request.id,
        };
        selector
            .validate()
            .map_err(|_| FleetCatalogError::Corrupt("return quiescence"))?;
        Ok(selector)
    }

    fn mutation(&self) -> Result<FleetMutation> {
        Ok(FleetMutation {
            id: self.request.id,
            operation: self.request.operation,
            principal: self.request.principal.clone(),
            expected_revision: self.request.expected_revision,
            changes: vec![
                FleetChange {
                    table: FleetTable::Nodes,
                    key: hex::encode(self.node.node),
                    value: Some(encode(NODE_KIND, &self.node, LIMIT)?),
                },
                FleetChange {
                    table: FleetTable::Operations,
                    key: Self::key(self.request.operation),
                    value: Some(encode(KIND, self, LIMIT)?),
                },
            ],
        })
    }
}

impl FleetHostAuthorization {
    /// Only the physical startup path has the exclusive log capability. No
    /// serialized return receipt can initialize a new or replacement engine.
    pub(crate) fn bind_restart_boot<'a>(
        &'a self,
        log: &'a ReplicaLog,
        id: ProposalId,
        join_key: [u8; 32],
        deadline: u64,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<(Self, LifecycleUpdate)>> {
        // Return has its own allocation and poll boundary. Ordinary startup calls
        // bind_boot directly and never polls this protocol.
        Box::pin(async move {
            let instance = log
                .storage_instance()
                .map_err(|_| FleetCatalogError::Conflict)?;
            let process = log.limits().process;
            if self.state() != FleetNodeState::RestartPending {
                return Err(FleetCatalogError::Conflict);
            }
            self.catalog
                .run(timeout, async {
                    let snapshot = self.catalog.inner.current().await?;
                    let current = self.catalog.host_from(&snapshot, self.node()).await?;
                    self.require_successor(&current)?;
                    let operation = current
                        .record
                        .operation()
                        .ok_or(FleetCatalogError::Conflict)?;
                    let (consent, stop, gate) = self
                        .catalog
                        .checked_restart_return(&snapshot, operation)
                        .await?;
                    if consent.process != process.bytes()
                        || current.storage_instance() != Some(instance)
                        || log.limits().node != self.node()
                        || log.limits().disk != self.disk()
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                    let prior = self
                        .catalog
                        .returned_boot(&snapshot, operation, &consent, &gate)
                        .await?;
                    let (record, committed) = if let Some(record) = prior {
                        // An unknown startup CAS keeps its original physical boot nonce.
                        // The exclusive log must still contain the predecessor's stop
                        // or this exact returned quiescence before any endpoint opens.
                        if current.record != record.node {
                            return Err(FleetCatalogError::Conflict);
                        }
                        (record, snapshot)
                    } else {
                        self.catalog.require_return_origin(&snapshot, &gate).await?;
                        let mut node = gate.node.clone();
                        node.epoch = node.epoch.checked_add(1).ok_or(FleetCatalogError::Limit)?;
                        node.boot = Some(Boot {
                            id: id.bytes(),
                            process: process.bytes(),
                            epoch: node.epoch,
                            join_key,
                        });
                        let record = ReturnedBoot {
                            format: 1,
                            request: FleetRequestIdentity {
                                id: id.bytes(),
                                operation,
                                principal: "fleet-maintenance-boot".into(),
                                expected_revision: snapshot.revision(),
                            },
                            input_root: hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?),
                            parent: consent.request.id,
                            parent_digest: consent.digest()?,
                            node,
                        };
                        record.validate(&self.catalog, &consent, &gate)?;
                        check_log(log, &stop, &record.quiescence(&gate)?, join_key)?;
                        let mutation = record.mutation()?;
                        let committed = self
                            .catalog
                            .commit_with_disruption(
                                &snapshot,
                                &mutation,
                                None,
                                Authority::RestartReturn,
                                || check_log(log, &stop, &record.quiescence(&gate)?, join_key),
                            )
                            .await?;
                        (record, committed.snapshot)
                    };
                    record.validate(&self.catalog, &consent, &gate)?;
                    let quiescence = record.quiescence(&gate)?;
                    check_log(log, &stop, &quiescence, join_key)?;
                    let authorization = self.catalog.host_from(&committed, self.node()).await?;
                    self.require_successor(&authorization)?;
                    self.catalog
                        .confirm_restart_boot(
                            &committed,
                            &authorization.record,
                            &record.mutation()?,
                        )
                        .await?;
                    Ok((
                        authorization,
                        LifecycleUpdate::MaintenanceBoot {
                            stop: Box::new(stop),
                            quiescence: Box::new(quiescence),
                            deadline,
                        },
                    ))
                })
                .await
        })
    }
}

fn check_log(
    log: &ReplicaLog,
    stop: &StopSelector,
    next: &StopSelector,
    join_key: [u8; 32],
) -> Result<()> {
    let guard = log
        .lifecycle_guard()
        .map_err(|_| FleetCatalogError::Closed)?
        .ok_or(FleetCatalogError::Conflict)?;
    if next.boot.process != log.limits().process.bytes()
        || next.boot.join_key != join_key
        || next.boot.engine
            != log
                .storage_instance()
                .map_err(|_| FleetCatalogError::Closed)?
                .bytes()
        || !(guard.accepted() == Some(stop)
            || guard.boot == next.boot && guard.quiescing() == Some(next))
    {
        return Err(FleetCatalogError::Conflict);
    }
    Ok(())
}

impl FleetCatalog {
    async fn returned_boot(
        &self,
        snapshot: &FleetSnapshot,
        operation: [u8; 16],
        consent: &Return,
        gate: &Gate,
    ) -> Result<Option<ReturnedBoot>> {
        let Some(bytes) = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &ReturnedBoot::key(operation),
            )
            .await?
        else {
            return Ok(None);
        };
        let record: ReturnedBoot = decode(KIND, &bytes, LIMIT)?;
        record.validate(self, consent, gate)?;
        self.confirm_restart_boot(snapshot, &record.node, &record.mutation()?)
            .await?;
        Ok(Some(record))
    }

    pub(in crate::raft::fleet_catalog) fn confirm_restart_boot<'a>(
        &'a self,
        snapshot: &'a FleetSnapshot,
        node: &'a NodeRecord,
        mutation: &'a FleetMutation,
    ) -> BoxFuture<'a, Result<()>> {
        // Ordinary boot confirmation must not embed this entire maintenance
        // protocol in every capacity/finalization future.
        Box::pin(async move {
            mutation.validate()?;
            let bytes = mutation
                .changes
                .iter()
                .find(|c| {
                    c.table == FleetTable::Operations
                        && c.key == ReturnedBoot::key(mutation.operation)
                })
                .and_then(|c| c.value.as_deref())
                .ok_or(FleetCatalogError::Corrupt("return boot scope"))?;
            let record: ReturnedBoot = decode(KIND, bytes, LIMIT)?;
            let (consent, _, gate) = self
                .checked_restart_return(snapshot, mutation.operation)
                .await?;
            record.validate(self, &consent, &gate)?;
            if record.mutation()? != *mutation || !node.boot_origin_matches(&record.node) {
                return Err(FleetCatalogError::Corrupt("return boot projection"));
            }
            let key = hex::encode(mutation.id);
            let stored = self
                .inner
                .read::<FleetMutation>(&format!("requests/{key}"), REQUEST_KIND, REQUEST_LIMIT)
                .await?
                .ok_or(FleetCatalogError::Corrupt("missing return boot request"))?
                .0;
            let applied = self
                .inner
                .committed(
                    &snapshot.head,
                    &key,
                    hash(&encode(REQUEST_KIND, mutation, REQUEST_LIMIT)?),
                )
                .await?
                .ok_or(FleetCatalogError::Corrupt("unadmitted return boot"))?;
            if stored != *mutation || mutation.expected_revision.checked_add(1) != Some(applied) {
                return Err(FleetCatalogError::Corrupt("return boot journal"));
            }
            Ok(())
        })
    }
}
