// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! A rooted floor on Raft segment file lengths. The latest number is the
//! only replayable settlement. The storage worker retains its corresponding
//! receipt until Fleet accepts it and authorizes the next number.
mod charges;

use super::*;
use crate::raft::fleet_catalog::deadline::Deadline;
use crate::raft::storage::{
    LogError, RaftSegmentInventory, RaftSegmentPurgeReceipt, ReplicaLog, SegmentPurgeSchedule,
};
use charges::Charges;

const PREFIX: &str = "segment-floor/";
const KIND: u8 = 241;
const LIMIT: usize = 4096;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Floor {
    format: u8,
    boot: BootBinding,
    sequence: u64,
    number: u64,
    charges: [u8; 32],
    bytes: u64,
    receipt: Option<[u8; 32]>,
}

impl Floor {
    fn check(&self, boot: &BootBinding) -> Result<()> {
        if self.format != 1
            || self.sequence == 0
            || self.charges == [0; 32]
            || self.receipt == Some([0; 32])
            || (self.number == 0) != self.receipt.is_none()
        {
            return Err(FleetCatalogError::Corrupt("segment floor"));
        }
        self.boot
            .validate()
            .map_err(|_| FleetCatalogError::Corrupt("segment floor boot"))?;
        if self.boot != *boot && !boot.is_retained_successor_of(&self.boot) {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(())
    }
}

enum PurgeTurn {
    Explicit(u64),
    Periodic(SegmentPurgeSchedule),
}

impl FleetHostAuthorization {
    pub(crate) async fn settle_raft_segment_purge(
        &self,
        log: &ReplicaLog,
        number: u64,
        timeout: Duration,
    ) -> Result<u64> {
        if number == 0 {
            return Err(FleetCatalogError::Invalid("segment purge number"));
        }
        let deadline = Deadline::new(&self.catalog, timeout)?;
        self.segment_purge_turn(log, PurgeTurn::Explicit(number), &deadline)
            .await
            .map(|(_, bytes)| bytes)
    }

    pub(crate) async fn settle_due_raft_segment_purge(
        &self,
        log: &ReplicaLog,
        settled: u64,
        timeout: Duration,
    ) -> Result<u64> {
        let deadline = Deadline::new(&self.catalog, timeout)?;
        let schedule = log.segment_purge_schedule().await.map_err(storage_error)?;
        if !schedule.due && schedule.latest <= settled {
            return Ok(settled);
        }
        self.segment_purge_turn(log, PurgeTurn::Periodic(schedule), &deadline)
            .await
            .map(|(number, _)| number)
    }

    async fn segment_purge_turn(
        &self,
        log: &ReplicaLog,
        turn: PurgeTurn,
        deadline: &Deadline<'_>,
    ) -> Result<(u64, u64)> {
        let catalog = &self.catalog;
        let periodic = matches!(&turn, PurgeTurn::Periodic(_));
        let (mut snapshot, boot, old, qualified) = catalog
            .run(deadline.remaining()?, async {
                let snapshot = catalog.inner.current().await?;
                let current = catalog.host_from(&snapshot, self.node()).await?;
                self.require_running_successor(&current)?;
                let boot = current.boot_binding()?;
                let floor = catalog.segment_floor(&snapshot, &boot).await?;
                let qualified = if periodic
                    && matches!(
                        current.state(),
                        FleetNodeState::Active | FleetNodeState::Draining
                    ) {
                    match catalog.capacity_from(&snapshot, self.node()).await {
                        Ok(capacity) => capacity.attestation().boot == boot,
                        Err(FleetCatalogError::Conflict) => false,
                        Err(error) => return Err(error),
                    }
                } else {
                    !periodic
                };
                Ok((snapshot, boot, floor, qualified))
            })
            .await?;
        require_log_boot(log, &boot)?;
        let number = match turn {
            PurgeTurn::Explicit(number) => number,
            PurgeTurn::Periodic(schedule) => {
                let rooted = old.as_ref().map_or(0, |floor| floor.number);
                let next = rooted.checked_add(1).ok_or(FleetCatalogError::Limit)?;
                if !(rooted..=next).contains(&schedule.latest) {
                    return Err(FleetCatalogError::Conflict);
                }
                if !qualified || !schedule.due && schedule.latest == rooted {
                    return Ok((rooted, old.map_or(0, |floor| floor.bytes)));
                }
                next
            }
        };
        let old = match old {
            Some(old) if old.number == number => return Ok((number, old.bytes)),
            Some(old) if old.number.checked_add(1) == Some(number) => old,
            None if number == 1 => {
                let started = catalog.resource_now()?;
                let initial = log.segment_inventory().await.map_err(storage_error)?;
                require_sample(&boot, &initial)?;
                let charges = Charges::observe(&initial)?;
                let (commit, floor) = catalog
                    .run(deadline.remaining()?, async {
                        catalog
                            .commit_segment_floor(
                                &snapshot, &boot, &initial, &charges, None, started,
                            )
                            .await
                    })
                    .await?;
                if commit.snapshot.revision() != commit.applied_revision {
                    return Err(FleetCatalogError::Conflict);
                }
                snapshot = commit.snapshot;
                floor
            }
            _ => return Err(FleetCatalogError::Conflict),
        };
        let before = catalog
            .run(deadline.remaining()?, Charges::read(catalog, old.charges))
            .await?;
        if before.floor()? != old.bytes {
            return Err(FleetCatalogError::Corrupt("segment charge total"));
        }
        require_log_boot(log, &boot)?;
        deadline.remaining()?;
        let receipt = log.purge_segments(number).await.map_err(storage_error)?;
        let started = catalog.resource_now()?;
        let after = log.segment_inventory().await.map_err(storage_error)?;
        require_sample(&boot, &after)?;
        if receipt.number != number
            || receipt.node != after.node
            || receipt.disk != after.disk
            || receipt.engine != after.engine
            || receipt.prepared_sequence <= old.sequence
            || receipt.completed_sequence <= receipt.prepared_sequence
            || after.durable_sequence < receipt.completed_sequence
        {
            return Err(FleetCatalogError::Conflict);
        }
        let charges = before.settle(&receipt, &after)?;
        require_log_boot(log, &boot)?;
        catalog
            .run(deadline.remaining()?, async {
                let (_, floor) = catalog
                    .commit_segment_floor(
                        &snapshot,
                        &boot,
                        &after,
                        &charges,
                        Some(&receipt),
                        started,
                    )
                    .await?;
                Ok((number, floor.bytes))
            })
            .await
    }
}

impl FleetCatalog {
    pub(in crate::raft::fleet_catalog) async fn segment_floor_bytes(
        &self,
        snapshot: &FleetSnapshot,
        boot: &BootBinding,
    ) -> Result<u64> {
        Ok(self
            .segment_floor(snapshot, boot)
            .await?
            .map_or(0, |floor| floor.bytes))
    }

    async fn segment_floor(
        &self,
        snapshot: &FleetSnapshot,
        boot: &BootBinding,
    ) -> Result<Option<Floor>> {
        let Some(value) = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &key(boot.node),
            )
            .await?
        else {
            return Ok(None);
        };
        let floor: Floor = decode(KIND, &value, LIMIT)?;
        floor.check(boot)?;
        Ok(Some(floor))
    }

    async fn commit_segment_floor(
        &self,
        snapshot: &FleetSnapshot,
        boot: &BootBinding,
        sample: &RaftSegmentInventory,
        charges: &Charges,
        receipt: Option<&RaftSegmentPurgeReceipt>,
        started: u64,
    ) -> Result<(FleetCommit, Floor)> {
        self.require_resource_age(started)?;
        let floor = Floor {
            format: 1,
            boot: boot.clone(),
            sequence: sample.durable_sequence,
            number: receipt.map_or(0, |receipt| receipt.number),
            charges: charges.write(self).await?,
            bytes: charges.floor()?,
            receipt: receipt.map(charges::receipt_digest).transpose()?,
        };
        floor.check(boot)?;
        let digest = hash(&encode(KIND, &(snapshot.revision(), &floor), LIMIT)?);
        let mut id = [0; 16];
        id.copy_from_slice(&digest[..16]);
        id[0] |= 1;
        let mutation = FleetMutation {
            id,
            operation: id,
            expected_revision: snapshot.revision(),
            principal: "raft-segment-settlement".into(),
            changes: vec![FleetChange {
                table: FleetTable::Operations,
                key: key(boot.node),
                value: Some(encode(KIND, &floor, LIMIT)?),
            }],
        };
        let commit = self
            .commit_with_disruption(
                snapshot,
                &mutation,
                None,
                Authority::SegmentSettlement,
                || self.require_resource_age(started),
            )
            .await?;
        Ok((commit, floor))
    }
}

pub(super) fn check_mutation(mutation: &FleetMutation, authority: Authority) -> Result<bool> {
    let protected = mutation
        .changes
        .iter()
        .any(|c| c.table == FleetTable::Operations && c.key.starts_with(PREFIX));
    if !protected {
        return Ok(false);
    }
    if authority != Authority::SegmentSettlement || mutation.changes.len() != 1 {
        return Err(FleetCatalogError::Invalid("segment settlement authority"));
    }
    Ok(true)
}

fn key(node: [u8; 16]) -> String {
    format!("{PREFIX}{}", hex::encode(node))
}

fn require_sample(boot: &BootBinding, sample: &RaftSegmentInventory) -> Result<()> {
    if sample.node.bytes() != boot.node
        || sample.disk.bytes() != boot.disk
        || sample.engine.bytes() != boot.engine
        || sample.process.bytes() != boot.process
        || sample.durable_sequence == 0
    {
        return Err(FleetCatalogError::Conflict);
    }
    Ok(())
}

fn require_log_boot(log: &ReplicaLog, boot: &BootBinding) -> Result<()> {
    if log
        .lifecycle_guard()
        .map_err(storage_error)?
        .is_none_or(|guard| !guard.admits_group_start(boot))
    {
        return Err(FleetCatalogError::Conflict);
    }
    Ok(())
}

fn storage_error(error: LogError) -> FleetCatalogError {
    match error {
        LogError::Closed => FleetCatalogError::Closed,
        LogError::Overloaded => FleetCatalogError::Overloaded,
        LogError::Invalid(_) | LogError::WrongIdentity => FleetCatalogError::Conflict,
        _ => FleetCatalogError::Storage(error.into()),
    }
}
