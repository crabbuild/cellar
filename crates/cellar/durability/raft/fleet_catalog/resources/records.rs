// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Bounded append-only active book and exact journal-linked debit records.
use super::movement::{Movement, MovementOwner};
use super::*;
use crate::raft::fleet_catalog::histories::validate_request;
use crate::raft::member_state::FleetMembershipPermit;

pub(super) const ROW_KIND: u8 = 61;
const BOOK_KIND: u8 = 62;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Request {
    pub identity: FleetRequestIdentity,
    pub group: String,
    pub attestation: FleetResourceAttestation,
    pub peak: FleetResourceBounds,
    #[serde(deserialize_with = "required_nullable")]
    pub movement: Option<Movement>,
}

impl Request {
    pub fn new(catalog: &FleetCatalog, request: &FleetResourceRequest) -> Result<Self> {
        validate_request(&request.identity)?;
        request.attestation.validate()?;
        let group = catalog
            .binding(&request.group)?
            .key()
            .map_err(|_| FleetCatalogError::Invalid("resource group"))?;
        let value = Self {
            identity: request.identity.clone(),
            group,
            attestation: request.attestation.clone(),
            peak: request.peak,
            movement: None,
        };
        value.validate(catalog)?;
        Ok(value)
    }

    pub fn validate(&self, catalog: &FleetCatalog) -> Result<()> {
        validate_request(&self.identity)?;
        self.attestation.validate()?;
        super::super::records::validate_key(&self.group)?;
        if self.attestation.boot.catalog != catalog.birth_owner()?.catalog
            || self.attestation.boot.fleet != catalog.inner.incarnation
            || self.peak.engine_slots != 1
            || self.peak.durable_bytes == 0
            || self.peak.memory_bytes == 0
        {
            return Err(FleetCatalogError::Invalid("resource request"));
        }
        if let Some(movement) = &self.movement {
            movement.validate()?;
            let plan = movement.plan()?;
            if plan.replacement.node.bytes() != self.attestation.boot.node
                || plan.replacement.disk.bytes() != self.attestation.boot.disk
            {
                return Err(FleetCatalogError::Invalid("resource target mismatch"));
            }
        }
        Ok(())
    }
}

fn required_nullable<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(
    decoder: D,
) -> std::result::Result<Option<T>, D::Error> {
    Option::<T>::deserialize(decoder)
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRecord {
    format: u8,
    request: Request,
    input_root: [u8; 32],
    capacity_epoch: u64,
    physical: FleetResourceBounds,
    inventory_digest: [u8; 32],
    #[serde(deserialize_with = "required_nullable")]
    permit: Option<FleetMembershipPermit>,
    #[serde(deserialize_with = "required_nullable")]
    seed_owners: Option<Vec<MovementOwner>>,
}

#[derive(Clone)]
pub(super) struct Record {
    pub request: Request,
    pub input_root: [u8; 32],
    pub capacity_epoch: u64,
    pub physical: FleetResourceBounds,
    pub inventory_digest: [u8; 32],
    pub node: NodeId,
    pub permit: Option<FleetMembershipPermit>,
    pub seed_owners: Option<Vec<MovementOwner>>,
}

impl Record {
    pub fn new(
        catalog: &FleetCatalog,
        request: Request,
        snapshot: &FleetSnapshot,
        capacity_epoch: u64,
        physical: FleetResourceBounds,
        inventory_digest: [u8; 32],
        movement: Option<super::movement::MovementProof>,
    ) -> Result<Self> {
        let (permit, seed_owners) = match movement {
            Some(proof) => (Some(proof.permit), Some(proof.owners)),
            None => (None, None),
        };
        let node = NodeId::new(request.attestation.boot.node)
            .map_err(|_| FleetCatalogError::Invalid("resource node"))?;
        let record = Self {
            request,
            input_root: hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?),
            capacity_epoch,
            physical,
            inventory_digest,
            node,
            permit,
            seed_owners,
        };
        record.validate(catalog)?;
        Ok(record)
    }

    fn validate(&self, catalog: &FleetCatalog) -> Result<()> {
        self.request.validate(catalog)?;
        match (&self.request.movement, &self.permit, &self.seed_owners) {
            (None, None, None) => (),
            (Some(movement), Some(permit), Some(owners)) => {
                let mut nodes = std::collections::BTreeSet::new();
                let source = movement.source()?;
                if owners.len() != 3
                    || owners.windows(2).any(|w| w[0].member >= w[1].member)
                    || !owners.iter().any(|o| o.boot.node == source.bytes())
                {
                    return Err(FleetCatalogError::Corrupt("resource seed owners"));
                }
                for owner in owners {
                    cellar_logic::consensus::MemberId::new(owner.member)
                        .map_err(|_| FleetCatalogError::Corrupt("resource source member"))?;
                    owner
                        .boot
                        .validate()
                        .map_err(|_| FleetCatalogError::Corrupt("resource source boot"))?;
                    if owner.boot.catalog != self.request.attestation.boot.catalog
                        || owner.boot.fleet != self.request.attestation.boot.fleet
                        || owner.boot.node == self.request.attestation.boot.node
                        || !nodes.insert(owner.boot.node)
                    {
                        return Err(FleetCatalogError::Corrupt("resource source owner"));
                    }
                }
                permit
                    .check_plan(&movement.plan()?)
                    .map_err(|_| FleetCatalogError::Corrupt("resource permit plan"))?;
                if permit.id() != self.request.identity.id
                    || permit.operation() != self.request.identity.operation
                    || permit
                        .binding()
                        .key()
                        .map_err(|_| FleetCatalogError::Corrupt("resource permit scope"))?
                        != self.request.group
                    || permit.binding().catalog() != catalog.birth_owner()?.catalog
                    || permit.binding().fleet() != catalog.inner.incarnation
                {
                    return Err(FleetCatalogError::Corrupt("resource permit identity"));
                }
            }
            _ => return Err(FleetCatalogError::Corrupt("resource permit pairing")),
        }
        if self.input_root == [0; 32]
            || self.inventory_digest == [0; 32]
            || self.node.bytes() != self.request.attestation.boot.node
            || self.capacity_epoch <= self.request.attestation.boot.epoch
            || self.capacity_epoch >= u64::MAX - 2
            || self.physical.high_water(self.request.attestation.used) != self.physical
        {
            return Err(FleetCatalogError::Corrupt("resource record"));
        }
        Ok(())
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        encode(
            ROW_KIND,
            &WireRecord {
                format: 1,
                request: self.request.clone(),
                input_root: self.input_root,
                capacity_epoch: self.capacity_epoch,
                physical: self.physical,
                inventory_digest: self.inventory_digest,
                permit: self.permit.clone(),
                seed_owners: self.seed_owners.clone(),
            },
            8192,
        )
    }

    pub fn decode(catalog: &FleetCatalog, bytes: &[u8]) -> Result<Self> {
        let wire: WireRecord = decode(ROW_KIND, bytes, 8192)?;
        if wire.format != 1 {
            return Err(FleetCatalogError::Corrupt("resource format"));
        }
        let node = NodeId::new(wire.request.attestation.boot.node)
            .map_err(|_| FleetCatalogError::Corrupt("resource node"))?;
        let record = Self {
            request: wire.request,
            input_root: wire.input_root,
            capacity_epoch: wire.capacity_epoch,
            physical: wire.physical,
            inventory_digest: wire.inventory_digest,
            node,
            permit: wire.permit,
            seed_owners: wire.seed_owners,
        };
        record.validate(catalog)?;
        Ok(record)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Reference {
    pub id: [u8; 16],
    pub digest: [u8; 32],
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Book {
    format: u8,
    catalog: [u8; 32],
    pub rows: Vec<Reference>,
}

impl Book {
    pub fn empty(catalog: &FleetCatalog) -> Result<Self> {
        Ok(Self {
            format: 1,
            catalog: catalog.birth_owner()?.catalog,
            rows: vec![],
        })
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        encode(BOOK_KIND, self, 4096)
    }

    pub(super) fn decode(catalog: &FleetCatalog, bytes: &[u8]) -> Result<Self> {
        let book: Self = decode(BOOK_KIND, bytes, 4096)?;
        if book.format != 1
            || book.catalog != catalog.birth_owner()?.catalog
            || book.rows.len() > MAX_MOVES
            || book
                .rows
                .iter()
                .any(|r| r.id == [0; 16] || r.digest == [0; 32])
            || book.rows.windows(2).any(|w| w[0].id >= w[1].id)
        {
            return Err(FleetCatalogError::Corrupt("resource book"));
        }
        Ok(book)
    }
}

impl FleetCatalog {
    pub(in super::super) async fn check_resource_mutation(
        &self,
        snapshot: &FleetSnapshot,
        mutation: &FleetMutation,
        movement: Authority,
    ) -> Result<()> {
        if self
            .check_resource_completion_mutation(snapshot, mutation, movement)
            .await?
        {
            return Ok(());
        }
        if mutation
            .changes
            .iter()
            .any(|c| c.table == FleetTable::Admissions)
        {
            let book = self.resource_book(snapshot).await?;
            let rows = self.resource_rows(snapshot, &book).await?;
            for row in &rows {
                if let Some(permit) = &row.permit {
                    let expected = permit
                        .encode()
                        .map_err(|_| FleetCatalogError::Corrupt("resource permit"))?;
                    if mutation.changes.iter().any(|c| {
                        c.table == FleetTable::Admissions
                            && c.key == row.request.group
                            && c.value.as_deref() != Some(expected.as_slice())
                    }) {
                        return Err(FleetCatalogError::Invalid(
                            "resource-linked permit replacement",
                        ));
                    }
                }
            }
        }
        if !mutation.changes.iter().any(|c| {
            c.table == FleetTable::Policy && c.key == BOOK_KEY
                || c.table == FleetTable::Operations && c.key.starts_with(ROW_PREFIX)
        }) {
            return Ok(());
        }
        if !(2..=3).contains(&mutation.changes.len()) {
            return Err(FleetCatalogError::Invalid("resource mutation"));
        }
        let row = &mutation.changes[0];
        let book = &mutation.changes[1];
        if row.table != FleetTable::Operations
            || row.key != row_key(mutation.id)
            || book.table != FleetTable::Policy
            || book.key != BOOK_KEY
        {
            return Err(FleetCatalogError::Invalid("resource mutation keys"));
        }
        let row_bytes = row
            .value
            .as_deref()
            .ok_or(FleetCatalogError::Invalid("resource deletion"))?;
        let record = Record::decode(self, row_bytes)?;
        match &record.permit {
            None if mutation.changes.len() == 2 => (),
            Some(permit) if movement == Authority::Admission && mutation.changes.len() == 3 => {
                let change = &mutation.changes[2];
                if change.table != FleetTable::Admissions
                    || change.key != record.request.group
                    || change.value.as_deref()
                        != Some(
                            permit
                                .encode()
                                .map_err(|_| FleetCatalogError::Corrupt("resource permit"))?
                                .as_slice(),
                        )
                {
                    return Err(FleetCatalogError::Invalid("resource permit mutation"));
                }
            }
            _ => return Err(FleetCatalogError::Invalid("resource permit authority")),
        }
        if record.request.identity.id != mutation.id
            || record.request.identity.operation != mutation.operation
            || record.request.identity.principal != mutation.principal
            || record.request.identity.expected_revision != mutation.expected_revision
            || record.input_root != hash(&encode(HEAD_KIND, &snapshot.head, ROOT_LIMIT)?)
        {
            return Err(FleetCatalogError::Invalid("resource mutation identity"));
        }
        let next = Book::decode(
            self,
            book.value
                .as_deref()
                .ok_or(FleetCatalogError::Invalid("resource book deletion"))?,
        )?;
        let previous = self.resource_book(snapshot).await?;
        let added = Reference {
            id: mutation.id,
            digest: hash(row_bytes),
        };
        if next.rows.len() != previous.rows.len() + 1
            || !next.rows.contains(&added)
            || previous
                .rows
                .iter()
                .any(|r| r.id == added.id || !next.rows.contains(r))
        {
            return Err(FleetCatalogError::Invalid(
                "resource book regression/replacement",
            ));
        }
        Ok(())
    }

    pub(super) async fn check_resource_permits(
        &self,
        snapshot: &FleetSnapshot,
        rows: &[Record],
    ) -> Result<()> {
        let mut cursor = None;
        let mut count = 0u64;
        loop {
            let page = self
                .inner
                .scan(
                    snapshot.head.tables[FleetTable::Admissions.slot()],
                    cursor.as_ref(),
                    128,
                )
                .await?;
            for entry in page.entries {
                count = count
                    .checked_add(1)
                    .filter(|count| *count <= HISTORY_LIMIT)
                    .ok_or(FleetCatalogError::Limit)?;
                let mut attributed = false;
                for row in rows {
                    if row.request.group == entry.key {
                        if let Some(permit) = &row.permit {
                            attributed = permit
                                .encode()
                                .map_err(|_| FleetCatalogError::Corrupt("resource permit"))?
                                == entry.value;
                        }
                    }
                }
                if !attributed {
                    let permit = FleetMembershipPermit::decode(&entry.value)
                        .map_err(|_| resource_error(LedgerError::Incomplete))?;
                    let row = self
                        .admitted_resource(snapshot, permit.id())
                        .await
                        .map_err(|_| resource_error(LedgerError::Incomplete))?;
                    if row.request.group != entry.key
                        || row.permit.as_ref() != Some(&permit)
                        || !self.resource_is_completed(snapshot, &row).await?
                    {
                        return Err(resource_error(LedgerError::Incomplete));
                    }
                }
            }
            cursor = page.next;
            if cursor.is_none() {
                return Ok(());
            }
        }
    }

    pub(super) async fn resource_book(&self, snapshot: &FleetSnapshot) -> Result<Book> {
        match self
            .inner
            .lookup(snapshot.head.tables[FleetTable::Policy.slot()], BOOK_KEY)
            .await?
        {
            Some(bytes) => Book::decode(self, &bytes),
            None => Book::empty(self),
        }
    }

    pub(super) async fn resource_request(
        &self,
        request: &Request,
    ) -> Result<Option<(FleetMutation, Record)>> {
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
            || mutation.changes.len() != if request.movement.is_some() { 3 } else { 2 }
            || mutation.changes[0].table != FleetTable::Operations
            || mutation.changes[0].key != row_key(request.identity.id)
            || mutation.changes[1].table != FleetTable::Policy
            || mutation.changes[1].key != BOOK_KEY
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        let bytes = mutation.changes[0]
            .value
            .as_deref()
            .ok_or(FleetCatalogError::Corrupt("resource request deletion"))?;
        let record = Record::decode(self, bytes)?;
        if record.request != *request {
            return Err(FleetCatalogError::PayloadConflict);
        }
        if let Some(permit) = &record.permit {
            let change = &mutation.changes[2];
            if change.table != FleetTable::Admissions
                || change.key != request.group
                || change.value.as_deref()
                    != Some(
                        permit
                            .encode()
                            .map_err(|_| FleetCatalogError::Corrupt("resource permit"))?
                            .as_slice(),
                    )
            {
                return Err(FleetCatalogError::Corrupt("resource request permit"));
            }
        }
        let book = Book::decode(
            self,
            mutation.changes[1]
                .value
                .as_deref()
                .ok_or(FleetCatalogError::Corrupt("resource request book"))?,
        )?;
        if !book.rows.contains(&Reference {
            id: request.identity.id,
            digest: hash(bytes),
        }) {
            return Err(FleetCatalogError::Corrupt("resource request reference"));
        }
        Ok(Some((mutation, record)))
    }

    pub(super) async fn resource_applied(
        &self,
        snapshot: &FleetSnapshot,
        mutation: &FleetMutation,
    ) -> Result<Option<u64>> {
        let digest = hash(&encode(REQUEST_KIND, mutation, REQUEST_LIMIT)?);
        let applied = self
            .inner
            .committed(&snapshot.head, &hex::encode(mutation.id), digest)
            .await?;
        if applied.is_some_and(|r| mutation.expected_revision.checked_add(1) != Some(r)) {
            return Err(FleetCatalogError::Corrupt("resource applied revision"));
        }
        Ok(applied)
    }

    pub(super) async fn resource_rows(
        &self,
        snapshot: &FleetSnapshot,
        book: &Book,
    ) -> Result<Vec<Record>> {
        let mut records = Vec::with_capacity(book.rows.len());
        for reference in &book.rows {
            let bytes = self
                .inner
                .lookup(
                    snapshot.head.tables[FleetTable::Operations.slot()],
                    &row_key(reference.id),
                )
                .await?
                .ok_or(FleetCatalogError::Corrupt("missing resource debit"))?;
            if hash(&bytes) != reference.digest {
                return Err(FleetCatalogError::Corrupt("resource debit digest"));
            }
            let record = Record::decode(self, &bytes)?;
            let (mutation, original) = self
                .resource_request(&record.request)
                .await?
                .ok_or(FleetCatalogError::Corrupt("missing resource request"))?;
            if record.request.identity.id != reference.id
                || original.encode()? != bytes
                || self.resource_applied(snapshot, &mutation).await?.is_none()
            {
                return Err(FleetCatalogError::Corrupt("unadmitted resource debit"));
            }
            records.push(record);
        }
        Ok(records)
    }
}
