// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Spend a joined local engine receipt once; unknown occupancy remains charged.
use super::*;
use crate::raft::fleet_catalog::{deadline::Deadline, managed_source_settlement::SlotRelease};
use crate::{application::consensus::RaftCells, raft::storage::MemberReclaimAck};

const KIND: u8 = 237;
const LIMIT: usize = 4096;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Settlement {
    format: u8,
    reservation: [u8; 16],
    fence: [u8; 32],
    member: u64,
    high_water: u64,
    birth: [u8; 32],
    before: u32,
    floor: u32,
    bytes_per_group: u64,
    physical: FleetResourceBounds,
    attestation: FleetResourceAttestation,
}

fn key(reservation: [u8; 16]) -> String {
    format!("member-cleanup/{}", hex::encode(reservation))
}

fn index_key(group: &str, node: [u8; 16]) -> String {
    format!("member-cleanup/index/{group}/{}", hex::encode(node))
}

fn invalid() -> FleetCatalogError {
    FleetCatalogError::Invalid("member settlement")
}

impl Settlement {
    fn validate(&self) -> Result<()> {
        self.attestation.validate()?;
        if self.format != 1
            || self.reservation == [0; 16]
            || self.fence == [0; 32]
            || self.member == 0
            || self.high_water < self.member
            || self.birth == [0; 32]
            || self.before == 0
            || self.bytes_per_group == 0
            || self
                .bytes_per_group
                .checked_mul(u64::from(self.before))
                .is_none()
            || self.attestation.used.engine_slots != 0
        {
            return Err(invalid());
        }
        Ok(())
    }

    async fn floor_change(
        &self,
        catalog: &FleetCatalog,
        snapshot: &FleetSnapshot,
    ) -> Result<FleetChange> {
        let old = catalog.resource_floor(snapshot, &self.attestation).await?;
        let mut change = catalog
            .source_slot_floor_change(snapshot, &self.attestation, &self.release())
            .await?;
        let mut physical = old
            .high_water(self.attestation.used)
            .high_water(self.physical);
        physical.engine_slots = if self.before >= old.engine_slots {
            old.engine_slots
                .saturating_sub(1)
                .max(self.physical.engine_slots)
        } else {
            old.engine_slots.max(self.physical.engine_slots)
        };
        physical.durable_bytes = released_disk_floor(
            old.durable_bytes,
            self.attestation.used.durable_bytes,
            self.physical.durable_bytes,
            self.before,
            self.bytes_per_group,
        );
        change.value = Some(
            completion::Floor {
                format: 1,
                boot: self.attestation.boot.clone(),
                physical,
            }
            .encode()?,
        );
        Ok(change)
    }

    fn release(&self) -> SlotRelease {
        SlotRelease {
            before: self.before,
            floor: self.floor,
            removed: 1,
            physical: self.physical,
        }
    }
}

impl FleetHostAuthorization {
    pub(crate) async fn settle_member_reclamation(
        &self,
        proof: &MemberReclaimAck,
        observer: &RaftCells,
        timeout: Duration,
    ) -> Result<()> {
        let catalog = &self.catalog;
        let deadline = Deadline::new(catalog, timeout)?;
        let record = proof.record();
        let permit = record.member.authorization().map_err(|_| invalid())?;
        let fence = hash(&encode(KIND, record, LIMIT)?);
        if !record.complete
            || record.node != self.node().bytes()
            || observer.identity().node != self.node()
        {
            return Err(FleetCatalogError::Conflict);
        }
        let prepared = catalog
            .run(deadline.remaining()?, async {
                let snapshot = catalog.inner.current().await?;
                let current = catalog.host_from_record(&snapshot, self.node()).await?;
                self.require_running_successor(&current)?;
                let boot = current.boot_binding()?;
                if boot.disk != record.disk || boot.engine != record.engine {
                    return Err(FleetCatalogError::Conflict);
                }
                if let Some(bytes) = catalog
                    .inner
                    .lookup(
                        snapshot.head.tables[FleetTable::Operations.slot()],
                        &key(permit.id()),
                    )
                    .await?
                {
                    let previous: Settlement = decode(KIND, &bytes, LIMIT)?;
                    previous.validate()?;
                    if previous.fence != fence
                        || previous.reservation != permit.id()
                        || previous.attestation.boot != boot
                    {
                        return Err(FleetCatalogError::Conflict);
                    }
                    return Ok(None);
                }
                // No older unresolved reservation may later spend a stale captured
                // floor. Concurrent new admission changes the root and loses this CAS.
                if !catalog.resource_book(&snapshot).await?.rows.is_empty() {
                    return Err(FleetCatalogError::SettlementInProgress {
                        admission: permit.id(),
                    });
                }
                let capacity = catalog.capacity_from(&snapshot, self.node()).await?;
                if capacity.attestation().boot != boot {
                    return Err(FleetCatalogError::Conflict);
                }
                let capacity = capacity.attestation();
                let attestation = FleetResourceAttestation {
                    boot,
                    profile_revision: capacity.profile_revision,
                    used: FleetResourceBounds {
                        engine_slots: 0,
                        durable_bytes: capacity.used_durable_bytes,
                        memory_bytes: capacity.used_memory_bytes,
                    },
                    evidence_digest: capacity.evidence_digest,
                    evidence_reference: capacity.evidence_reference.clone(),
                };
                let floor = catalog
                    .resource_floor(&snapshot, &attestation)
                    .await?
                    .engine_slots;
                Ok(Some((snapshot, attestation, floor)))
            })
            .await?;
        let Some((snapshot, attestation, floor)) = prepared else {
            return Ok(());
        };
        let _memory = observer
            .control_budget()
            .acquire(2 * crate::raft::physical_inventory::RESPONSE_BYTES)
            .ok_or(FleetCatalogError::Overloaded)?;
        let sample = observer
            .inspect_peer_physical_inventory(
                self.node(),
                snapshot.revision(),
                deadline.remaining()?,
            )
            .await
            .map_err(FleetCatalogError::Observation)?;
        let host = catalog
            .run(
                deadline.remaining()?,
                catalog.host_from(&snapshot, self.node()),
            )
            .await?;
        let capacity = catalog
            .run(
                deadline.remaining()?,
                catalog.capacity_from(&snapshot, self.node()),
            )
            .await?;
        capacity::check_observation(&host, capacity.attestation(), &sample)?;
        let inventory = sample
            .inventory
            .as_ref()
            .ok_or(FleetCatalogError::InventoryIncomplete)?;
        let history = record.member.scope().map_err(|_| invalid())?;
        if sample.view.node.profile.bytes_per_group != record.bytes_per_group
            || inventory.initializing().is_some()
            || inventory.histories().len() != sample.view.node.groups as usize
            || inventory.group_history(&history).is_some()
        {
            return Err(FleetCatalogError::InventoryIncomplete);
        }
        let started = catalog
            .resource_now()?
            .checked_sub(sample.sample_age_ms)
            .ok_or(FleetCatalogError::Unconfirmed)?;
        let settlement = Settlement {
            format: 1,
            reservation: permit.id(),
            fence,
            member: record.member.member,
            high_water: record.member.high_water,
            birth: record.member.birth,
            before: record.before_groups,
            floor,
            bytes_per_group: record.bytes_per_group,
            physical: FleetResourceBounds {
                engine_slots: sample.view.node.groups,
                durable_bytes: sample.view.node.reserved_bytes,
                memory_bytes: sample.view.startup_bounds.bounded_buffer_bytes,
            },
            attestation,
        };
        settlement.validate()?;
        catalog
            .run(deadline.remaining()?, async {
                let floor = settlement.floor_change(catalog, &snapshot).await?;
                let group = permit.binding().key().map_err(|_| invalid())?;
                let encoded = encode(KIND, &settlement, LIMIT)?;
                let changes = vec![
                    FleetChange {
                        table: FleetTable::Operations,
                        key: key(permit.id()),
                        value: Some(encoded.clone()),
                    },
                    floor,
                    FleetChange {
                        table: FleetTable::Operations,
                        key: index_key(&group, settlement.attestation.boot.node),
                        value: Some(encoded),
                    },
                ];
                let digest = hash(&encode(
                    KIND,
                    &(snapshot.revision(), &changes),
                    REQUEST_LIMIT,
                )?);
                let mut id = [0; 16];
                id.copy_from_slice(&digest[..16]);
                id[0] |= 1;
                let mutation = FleetMutation {
                    id,
                    operation: permit.operation(),
                    principal: "member-reclamation".into(),
                    expected_revision: snapshot.revision(),
                    changes,
                };
                catalog
                    .commit_with_disruption(
                        &snapshot,
                        &mutation,
                        None,
                        Authority::MemberSettlement,
                        || catalog.require_resource_age(started),
                    )
                    .await
                    .map(|_| ())
            })
            .await
    }
}

impl FleetCatalog {
    /// A selected root receipt closes only the collected member prefix on this
    /// exact retained engine. An empty physical inventory alone proves no GC.
    pub(in crate::raft::fleet_catalog) async fn collected_member_prefix(
        &self,
        snapshot: &FleetSnapshot,
        group: &str,
        birth: [u8; 32],
        host: &FleetHostAuthorization,
        member: cellar_logic::consensus::MemberId,
    ) -> Result<bool> {
        let Some(bytes) = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &index_key(group, host.node().bytes()),
            )
            .await?
        else {
            return Ok(false);
        };
        let receipt: Settlement = decode(KIND, &bytes, LIMIT)?;
        receipt.validate()?;
        let current = host.boot_binding()?;
        let previous = &receipt.attestation.boot;
        Ok(receipt.birth == birth
            && receipt.high_water >= member.get()
            && previous.catalog == current.catalog
            && previous.fleet == current.fleet
            && previous.node == current.node
            && previous.disk == current.disk
            && previous.engine == current.engine)
    }

    pub(in crate::raft::fleet_catalog) async fn check_member_settlement_mutation(
        &self,
        snapshot: &FleetSnapshot,
        mutation: &FleetMutation,
    ) -> Result<()> {
        if mutation.changes.len() != 3 {
            return Err(invalid());
        }
        let first = &mutation.changes[0];
        let settlement: Settlement =
            decode(KIND, first.value.as_deref().ok_or_else(invalid)?, LIMIT)?;
        settlement.validate()?;
        if first.table != FleetTable::Operations
            || first.key != key(settlement.reservation)
            || self
                .inner
                .lookup(
                    snapshot.head.tables[FleetTable::Operations.slot()],
                    &first.key,
                )
                .await?
                .is_some()
            || !self.resource_book(snapshot).await?.rows.is_empty()
        {
            return Err(FleetCatalogError::Conflict);
        }
        let row = self
            .admitted_resource(snapshot, settlement.reservation)
            .await?;
        let permit = row.permit.as_ref().ok_or_else(invalid)?;
        let plan = row.request.movement.as_ref().ok_or_else(invalid)?.plan()?;
        let current = self
            .host_from(
                snapshot,
                NodeId::new(settlement.attestation.boot.node).map_err(|_| invalid())?,
            )
            .await?;
        let owner = row
            .seed_owners
            .as_ref()
            .ok_or_else(invalid)?
            .iter()
            .find(|owner| {
                owner.member == settlement.member && owner.node == settlement.attestation.boot.node
            })
            .ok_or(FleetCatalogError::Conflict)?;
        if !matches!(
            self.resource_settlement(snapshot, &row).await?,
            Some((completion::Settlement::Complete, _))
        ) || permit.operation() != mutation.operation
            || plan.obsolete.get() != settlement.member
            || current.boot_binding()? != settlement.attestation.boot
            || !owner.matches(&settlement.attestation.boot)?
        {
            return Err(FleetCatalogError::Conflict);
        }
        let history = self.history_inner(snapshot, &row.request.group).await?;
        if settlement.birth != history.birth_digest() {
            return Err(FleetCatalogError::Conflict);
        }
        let index = &mutation.changes[2];
        if index.table != FleetTable::Operations
            || index.key != index_key(&row.request.group, settlement.attestation.boot.node)
            || index.value != first.value
        {
            return Err(invalid());
        }
        if let Some(bytes) = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &index.key,
            )
            .await?
        {
            let previous: Settlement = decode(KIND, &bytes, LIMIT)?;
            previous.validate()?;
            if previous.birth != settlement.birth
                || previous.attestation.boot.disk != settlement.attestation.boot.disk
                || previous.attestation.boot.engine != settlement.attestation.boot.engine
                || previous.high_water >= settlement.member
            {
                return Err(FleetCatalogError::Conflict);
            }
        }
        let expected = settlement.floor_change(self, snapshot).await?;
        if mutation.changes[1] != expected {
            return Err(invalid());
        }
        Ok(())
    }
}

// This is a logical placement reservation, not a claim about engine file sizes.
// Only a floor fully covered by the receipt's captured physical group count can
// lose one group reservation. Qualified baseline and post-GC occupancy survive.
fn released_disk_floor(old: u64, baseline: u64, observed: u64, before: u32, per_group: u64) -> u64 {
    let reduced = if per_group
        .checked_mul(u64::from(before))
        .is_some_and(|known| known >= old)
    {
        old.saturating_sub(per_group)
    } else {
        old
    };
    reduced.max(baseline).max(observed)
}

#[cfg(test)]
mod tests {
    use super::released_disk_floor;

    #[test]
    fn disk_release_preserves_unattributed_debt_and_observed_baselines() {
        assert_eq!(released_disk_floor(256, 1, 0, 1, 256), 1);
        assert_eq!(released_disk_floor(512, 1, 256, 2, 256), 256);
        assert_eq!(released_disk_floor(513, 1, 0, 2, 256), 513);
        assert_eq!(released_disk_floor(256, 300, 0, 1, 256), 300);
        assert_eq!(released_disk_floor(256, 1, 400, 1, 256), 400);
        assert_eq!(released_disk_floor(u64::MAX, 0, 0, 2, u64::MAX), u64::MAX);
    }
}
