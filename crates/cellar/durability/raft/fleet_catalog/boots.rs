// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Root-admitted physical boots order against a node's no-new-boot fence.
use super::nodes::{FleetHostAuthorization, NodeRecord, NODE_KIND};
use super::*;
use cellar_logic::consensus::{EngineInstanceId, ProcessGeneration, ProposalId};
use serde::Deserialize;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Boot {
    pub(super) id: [u8; 16],
    pub(super) process: [u8; 16],
    pub(super) epoch: u64,
}

impl Boot {
    pub(super) fn validate(&self, epoch: u64) -> Result<()> {
        ProposalId::new(self.id).map_err(|_| FleetCatalogError::Corrupt("boot request"))?;
        ProcessGeneration::new(self.process)
            .map_err(|_| FleetCatalogError::Corrupt("boot process"))?;
        if self.epoch == 0 || self.epoch > epoch || self.epoch >= u64::MAX - 2 {
            return Err(FleetCatalogError::Corrupt("boot epoch"));
        }
        Ok(())
    }
}

impl FleetHostAuthorization {
    /// The exclusive engine owner claims one boot before publishing endpoints
    /// or starting groups. Its nonce is fixed across unknown replies, including
    /// an already pinned engine. This CAS orders against Finalizing: an earlier
    /// boot is inventoried; a later attempt cannot pass the rooted boot fence.
    pub(crate) async fn bind_boot(
        &self,
        instance: EngineInstanceId,
        process: ProcessGeneration,
        id: ProposalId,
        timeout: Duration,
    ) -> Result<Self> {
        self.catalog
            .run(timeout, async {
                let snapshot = self.catalog.inner.current().await?;
                let mut current = self.catalog.host_from(&snapshot, self.node()).await?;
                self.require_successor(&current)?;
                if current
                    .storage_instance()
                    .is_some_and(|pinned| pinned != instance)
                {
                    return Err(FleetCatalogError::Conflict);
                }
                if let Some(boot) = &current.record.boot {
                    if boot.id == id.bytes() {
                        if boot.process != process.bytes()
                            || current.storage_instance() != Some(instance)
                        {
                            return Err(FleetCatalogError::PayloadConflict);
                        }
                        self.catalog
                            .confirm_boot(&snapshot, &current.record)
                            .await?;
                        return Ok(current);
                    }
                }
                // An earlier candidate with this identity retains its original
                // expected revision. Never rewrite it after a clean/unknown conflict.
                if self
                    .catalog
                    .inner
                    .read::<FleetMutation>(
                        &format!("requests/{}", hex::encode(id.bytes())),
                        REQUEST_KIND,
                        REQUEST_LIMIT,
                    )
                    .await?
                    .is_some()
                {
                    return Err(FleetCatalogError::Conflict);
                }
                current.record.storage_instance = Some(instance.bytes());
                current.record.epoch = current
                    .record
                    .epoch
                    .checked_add(1)
                    .ok_or(FleetCatalogError::Limit)?;
                current.record.boot = Some(Boot {
                    id: id.bytes(),
                    process: process.bytes(),
                    epoch: current.record.epoch,
                });
                current.record.validate(&self.catalog.inner)?;
                let mutation = FleetMutation {
                    id: id.bytes(),
                    operation: current.record.registration_operation,
                    principal: "fleet-host-boot".into(),
                    expected_revision: snapshot.revision(),
                    changes: vec![FleetChange {
                        table: FleetTable::Nodes,
                        key: hex::encode(current.record.node),
                        value: Some(encode(NODE_KIND, &current.record, 4096)?),
                    }],
                };
                mutation.validate()?;
                let committed = self.catalog.commit_inner(&snapshot, &mutation).await?;
                let observed = self
                    .catalog
                    .host_from(&committed.snapshot, self.node())
                    .await?;
                self.require_successor(&observed)?;
                if observed.record.boot != current.record.boot
                    || observed.storage_instance() != Some(instance)
                {
                    return Err(FleetCatalogError::Conflict);
                }
                self.catalog
                    .confirm_boot(&committed.snapshot, &observed.record)
                    .await?;
                Ok(observed)
            })
            .await
    }
}

impl FleetCatalog {
    /// Called inside the owning bounded turn. A retained boot is accepted only
    /// with its exact original node mutation and permanent journal link.
    pub(super) async fn confirm_boot(
        &self,
        snapshot: &FleetSnapshot,
        node: &NodeRecord,
    ) -> Result<()> {
        let boot = node
            .boot
            .as_ref()
            .ok_or(FleetCatalogError::Corrupt("missing physical boot"))?;
        boot.validate(node.epoch)?;
        let key = hex::encode(boot.id);
        let (mutation, _) = self
            .inner
            .read::<FleetMutation>(&format!("requests/{key}"), REQUEST_KIND, REQUEST_LIMIT)
            .await?
            .ok_or(FleetCatalogError::Corrupt("missing boot request"))?;
        mutation.validate()?;
        if mutation.id != boot.id
            || mutation.operation != node.registration_operation
            || mutation.principal != "fleet-host-boot"
            || mutation.changes.len() != 1
            || mutation.changes[0].table != FleetTable::Nodes
            || mutation.changes[0].key != hex::encode(node.node)
        {
            return Err(FleetCatalogError::Corrupt("boot request scope"));
        }
        let origin: NodeRecord = decode(
            NODE_KIND,
            mutation.changes[0]
                .value
                .as_deref()
                .ok_or(FleetCatalogError::Corrupt("boot node deletion"))?,
            4096,
        )?;
        origin.validate(&self.inner)?;
        if origin.boot.as_ref() != Some(boot)
            || origin.epoch != boot.epoch
            || !node.boot_origin_matches(&origin)
        {
            return Err(FleetCatalogError::Corrupt("boot node projection"));
        }
        let digest = hash(&encode(REQUEST_KIND, &mutation, REQUEST_LIMIT)?);
        let applied = self
            .inner
            .committed(&snapshot.head, &key, digest)
            .await?
            .ok_or(FleetCatalogError::Corrupt("unadmitted boot projection"))?;
        if mutation.expected_revision.checked_add(1) != Some(applied) {
            return Err(FleetCatalogError::Corrupt("boot applied revision"));
        }
        Ok(())
    }
}
