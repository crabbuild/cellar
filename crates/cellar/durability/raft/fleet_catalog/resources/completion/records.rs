// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Permanent completed attribution and independent monotonic physical floors.
use super::*;

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Floor {
    pub format: u8,
    pub boot: BootBinding,
    pub physical: FleetResourceBounds,
}

impl Floor {
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode(FLOOR_KIND, self, 4096)
    }

    fn decode(catalog: &FleetCatalog, bytes: &[u8]) -> Result<Self> {
        let floor: Self = decode(FLOOR_KIND, bytes, 4096)?;
        floor.boot.validate().map_err(|_| invalid())?;
        if floor.format != 1
            || floor.boot.catalog != catalog.birth_owner()?.catalog
            || floor.boot.fleet != catalog.inner.incarnation
        {
            return Err(invalid());
        }
        Ok(floor)
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Completion {
    pub format: u8,
    pub settlement: Settlement,
    pub request: FleetMovementCompletionRequest,
    pub input_root: [u8; 32],
    pub resource: [u8; 32],
    pub permit: [u8; 32],
    pub target_inventory: [u8; 32],
    pub source_inventory: [u8; 32],
    pub isolated_sources: Vec<[u8; 16]>,
    pub retained_target: bool,
    pub membership_revision: u64,
    pub barrier: u64,
    pub physical: FleetResourceBounds,
}

impl Completion {
    pub fn encode(&self) -> Result<Vec<u8>> {
        encode(COMPLETE_KIND, self, 8192)
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        let value: Self = decode(COMPLETE_KIND, bytes, 8192)?;
        value.request.validate()?;
        if value.format != 1
            || [
                value.input_root,
                value.resource,
                value.permit,
                value.target_inventory,
                value.source_inventory,
            ]
            .contains(&[0; 32])
            || value.membership_revision == 0
            || value.isolated_sources.len() > 1
            || value.settlement != Settlement::Complete && !value.isolated_sources.is_empty()
            || value.settlement == Settlement::Complete && !value.retained_target
            || value.settlement == Settlement::Fenced && value.retained_target
            || value.isolated_sources.contains(&[0; 16])
            || value.barrier < value.membership_revision
            || value.physical.high_water(value.request.attestation.used) != value.physical
        {
            return Err(FleetCatalogError::Corrupt("resource completion"));
        }
        Ok(value)
    }

    fn matches(&self, row: &Record) -> Result<()> {
        let obsolete = row
            .request
            .movement
            .as_ref()
            .ok_or_else(invalid)?
            .plan()?
            .obsolete
            .get();
        if self.request.reservation != row.request.identity.id
            || self.isolated_sources.iter().any(|node| {
                row.seed_owners.as_ref().is_none_or(|owners| {
                    !owners
                        .iter()
                        .any(|owner| owner.node == *node && owner.member == obsolete)
                })
            })
            || self.request.identity.operation != row.request.identity.operation
            || self.request.attestation.boot != row.request.attestation.boot
            || self.request.attestation.profile_revision != row.request.attestation.profile_revision
            || self.resource != hash(&row.encode()?)
            || self.permit
                != hash(
                    &row.permit
                        .as_ref()
                        .ok_or_else(invalid)?
                        .encode()
                        .map_err(|_| invalid())?,
                )
            || self.physical.high_water(row.physical) != self.physical
        {
            return Err(FleetCatalogError::Corrupt("resource completion binding"));
        }
        Ok(())
    }
}

impl FleetCatalog {
    pub(in super::super) async fn admitted_resource(
        &self,
        snapshot: &FleetSnapshot,
        id: [u8; 16],
    ) -> Result<Record> {
        let bytes = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &row_key(id),
            )
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        let row = Record::decode(self, &bytes)?;
        let (mutation, original) = self
            .resource_request(&row.request)
            .await?
            .ok_or(FleetCatalogError::Unconfirmed)?;
        if row.request.identity.id != id
            || original.encode()? != bytes
            || self.resource_applied(snapshot, &mutation).await?.is_none()
        {
            return Err(FleetCatalogError::Corrupt("unadmitted resource row"));
        }
        Ok(row)
    }

    pub(in super::super) async fn resource_floor(
        &self,
        snapshot: &FleetSnapshot,
        attestation: &FleetResourceAttestation,
    ) -> Result<FleetResourceBounds> {
        let Some(bytes) = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &floor_key(attestation.boot.node),
            )
            .await?
        else {
            return Ok(FleetResourceBounds::default());
        };
        let floor = Floor::decode(self, &bytes)?;
        if floor.boot.node != attestation.boot.node
            || floor.boot.disk != attestation.boot.disk
            || floor.boot.engine != attestation.boot.engine
        {
            return Err(FleetCatalogError::Corrupt("resource floor owner"));
        }
        Ok(floor.physical)
    }

    pub(in super::super) async fn resource_is_completed(
        &self,
        snapshot: &FleetSnapshot,
        row: &Record,
    ) -> Result<bool> {
        Ok(self.resource_settlement(snapshot, row).await?.is_some())
    }

    pub(in super::super) async fn resource_retained_target(
        &self,
        snapshot: &FleetSnapshot,
        row: &Record,
    ) -> Result<bool> {
        let Some((kind, _)) = self.resource_settlement(snapshot, row).await? else {
            return Ok(false);
        };
        if kind == Settlement::Fenced {
            return Ok(false);
        }
        let bytes = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &completion_key(row.request.identity.id),
            )
            .await?
            .ok_or(FleetCatalogError::Corrupt("missing binding completion"))?;
        Ok(Completion::decode(&bytes)?.retained_target)
    }

    pub(in super::super) async fn resource_settlement(
        &self,
        snapshot: &FleetSnapshot,
        row: &Record,
    ) -> Result<Option<(Settlement, u64)>> {
        let Some(bytes) = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &completion_key(row.request.identity.id),
            )
            .await?
        else {
            return Ok(None);
        };
        let completion = Completion::decode(&bytes)?;
        completion.matches(row)?;
        let (mutation, original) = self
            .completion_request(&completion.request, completion.settlement)
            .await?
            .ok_or(FleetCatalogError::Unconfirmed)?;
        let applied = self.resource_applied(snapshot, &mutation).await?;
        if original.encode()? != bytes || applied.is_none() {
            return Err(FleetCatalogError::Corrupt("unadmitted completion"));
        }
        Ok(applied.map(|revision| (completion.settlement, revision)))
    }

    pub(super) async fn completion_request(
        &self,
        request: &FleetMovementCompletionRequest,
        settlement: Settlement,
    ) -> Result<Option<(FleetMutation, Completion)>> {
        let Some((mutation, _)) = self
            .inner
            .read::<FleetMutation>(
                &format!("requests/{}", hex::encode(request.identity.id)),
                REQUEST_KIND,
                REQUEST_LIMIT,
            )
            .await?
        else {
            return Ok(None);
        };
        mutation.validate()?;
        if mutation.id != request.identity.id
            || mutation.operation != request.identity.operation
            || mutation.principal != request.identity.principal
            || mutation.expected_revision != request.identity.expected_revision
            || mutation.changes.len() != 3
            || mutation.changes[0].table != FleetTable::Operations
            || mutation.changes[0].key != completion_key(request.reservation)
            || mutation.changes[1].table != FleetTable::Operations
            || mutation.changes[1].key != floor_key(request.attestation.boot.node)
            || mutation.changes[2].table != FleetTable::Policy
            || mutation.changes[2].key != BOOK_KEY
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        let completion =
            Completion::decode(mutation.changes[0].value.as_deref().ok_or_else(invalid)?)?;
        if completion.request != *request || completion.settlement != settlement {
            return Err(FleetCatalogError::PayloadConflict);
        }
        let floor = Floor::decode(
            self,
            mutation.changes[1].value.as_deref().ok_or_else(invalid)?,
        )?;
        if floor.boot != request.attestation.boot || floor.physical != completion.physical {
            return Err(invalid());
        }
        Book::decode(
            self,
            mutation.changes[2].value.as_deref().ok_or_else(invalid)?,
        )?;
        Ok(Some((mutation, completion)))
    }

    pub(in super::super::super) async fn check_resource_completion_mutation(
        &self,
        snapshot: &FleetSnapshot,
        mutation: &FleetMutation,
        authority: Authority,
    ) -> Result<bool> {
        if !mutation.changes.iter().any(|c| {
            c.table == FleetTable::Operations
                && (c.key.starts_with(COMPLETE_PREFIX) || c.key.starts_with(FLOOR_PREFIX))
        }) {
            return Ok(false);
        }
        if authority != Authority::Completion || mutation.changes.len() != 3 {
            return Err(FleetCatalogError::Invalid("resource completion authority"));
        }
        let completion =
            Completion::decode(mutation.changes[0].value.as_deref().ok_or_else(invalid)?)?;
        let request = &completion.request;
        let expected = FleetRequestIdentity {
            id: mutation.id,
            operation: mutation.operation,
            principal: mutation.principal.clone(),
            expected_revision: mutation.expected_revision,
        };
        if request.identity != expected
            || completion.input_root != hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?)
            || mutation.changes[0].table != FleetTable::Operations
            || mutation.changes[0].key != completion_key(request.reservation)
            || mutation.changes[1].table != FleetTable::Operations
            || mutation.changes[1].key != floor_key(request.attestation.boot.node)
            || mutation.changes[2].table != FleetTable::Policy
            || mutation.changes[2].key != BOOK_KEY
        {
            return Err(invalid());
        }
        let row = self
            .admitted_resource(snapshot, request.reservation)
            .await?;
        completion.matches(&row)?;
        if self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &completion_key(request.reservation),
            )
            .await?
            .is_some()
        {
            return Err(FleetCatalogError::Conflict);
        }
        let mut expected = self.resource_book(snapshot).await?;
        let reference = Reference {
            id: request.reservation,
            digest: hash(&row.encode()?),
        };
        let index = expected
            .rows
            .iter()
            .position(|r| r == &reference)
            .ok_or(FleetCatalogError::Conflict)?;
        expected.rows.remove(index);
        if mutation.changes[2].value.as_deref() != Some(expected.encode()?.as_slice()) {
            return Err(invalid());
        }
        let floor = Floor::decode(
            self,
            mutation.changes[1].value.as_deref().ok_or_else(invalid)?,
        )?;
        if floor.boot != request.attestation.boot
            || floor.physical != completion.physical
            || floor
                .physical
                .high_water(self.resource_floor(snapshot, &request.attestation).await?)
                != floor.physical
        {
            return Err(invalid());
        }
        Ok(true)
    }
}
