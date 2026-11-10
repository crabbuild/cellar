// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Root-selected, node-targeted cleanup work. Completion enqueues and the
//! opaque physical settlement dequeues in the same CAS as their proof records.
use super::records::Record;
use super::*;

const KIND: u8 = 238;
const LIMIT: usize = 4096;
const MAX_PENDING: usize = 32;
pub(in crate::raft::fleet_catalog) const PREFIX: &str = "member-cleanup/work/";

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Queue {
    format: u8,
    reservations: Vec<[u8; 16]>,
}

impl Queue {
    fn validate(&self) -> Result<()> {
        if self.format != 1
            || self.reservations.len() > MAX_PENDING
            || self.reservations.contains(&[0; 16])
            || self.reservations.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(FleetCatalogError::Corrupt("member cleanup queue"));
        }
        Ok(())
    }

    fn change(&self, node: NodeId) -> Result<FleetChange> {
        self.validate()?;
        Ok(FleetChange {
            table: FleetTable::Operations,
            key: format!("{PREFIX}{}", hex::encode(node.bytes())),
            value: Some(encode(KIND, self, LIMIT)?),
        })
    }
}

fn obsolete_node(row: &Record) -> Result<Option<NodeId>> {
    let Some(movement) = &row.request.movement else {
        return Ok(None);
    };
    let obsolete = movement.plan()?.obsolete.get();
    let owner = row
        .seed_owners
        .as_ref()
        .and_then(|owners| owners.iter().find(|owner| owner.member == obsolete))
        .ok_or(FleetCatalogError::Corrupt("member cleanup owner"))?;
    NodeId::new(owner.node)
        .map(Some)
        .map_err(|_| FleetCatalogError::Corrupt("member cleanup node"))
}

impl FleetCatalog {
    async fn member_cleanup_queue(&self, snapshot: &FleetSnapshot, node: NodeId) -> Result<Queue> {
        let bytes = self
            .inner
            .lookup(
                snapshot.head.tables[FleetTable::Operations.slot()],
                &format!("{PREFIX}{}", hex::encode(node.bytes())),
            )
            .await?;
        let queue = match bytes {
            Some(bytes) => decode(KIND, &bytes, LIMIT)?,
            None => Queue {
                format: 1,
                reservations: Vec::new(),
            },
        };
        queue.validate()?;
        Ok(queue)
    }

    pub(super) async fn check_member_cleanup_room(
        &self,
        snapshot: &FleetSnapshot,
        row: &Record,
        pending: &[Record],
    ) -> Result<()> {
        // A new member must not occupy this history on the same engine until
        // the prior cleanup receipt/floor/index/dequeue have committed together.
        for id in self
            .member_cleanup_queue(snapshot, row.node)
            .await?
            .reservations
        {
            let previous = self.admitted_resource(snapshot, id).await?;
            if previous.request.group == row.request.group {
                return Err(FleetCatalogError::SettlementInProgress { admission: id });
            }
        }
        let Some(node) = obsolete_node(row)? else {
            return Ok(());
        };
        let source = self.host_from_record(snapshot, node).await?;
        if source.record.isolation.is_some() {
            let proof = self
                .confirm_node_isolation(snapshot, &source.record)
                .await?;
            let obsolete = row
                .request
                .movement
                .as_ref()
                .ok_or(FleetCatalogError::Conflict)?
                .plan()?
                .obsolete
                .get();
            let owner = row
                .seed_owners
                .as_ref()
                .and_then(|owners| owners.iter().find(|owner| owner.member == obsolete))
                .ok_or(FleetCatalogError::Conflict)?;
            if !owner.matches(&proof.boot)? {
                return Err(FleetCatalogError::Conflict);
            }
            // Permanent isolation has its own offline retirement owner. Never
            // require a fenced process to restart merely to drain this queue.
            return Ok(());
        }
        let queue = self.member_cleanup_queue(snapshot, node).await?;
        let mut reserved = queue.reservations.len();
        for previous in pending {
            if obsolete_node(previous)? == Some(node) {
                reserved += 1;
            }
        }
        // Reserve queue room before membership starts; completion cannot be
        // trapped behind a full queue while cleanup waits on active debits.
        if reserved >= MAX_PENDING {
            return Err(FleetCatalogError::Overloaded);
        }
        Ok(())
    }

    pub(super) async fn enqueue_member_cleanup(
        &self,
        snapshot: &FleetSnapshot,
        row: &Record,
    ) -> Result<FleetChange> {
        let node =
            obsolete_node(row)?.ok_or(FleetCatalogError::Invalid("member cleanup movement"))?;
        let mut queue = self.member_cleanup_queue(snapshot, node).await?;
        let id = row.request.identity.id;
        match queue.reservations.binary_search(&id) {
            Ok(_) => return Err(FleetCatalogError::Conflict),
            Err(index) => queue.reservations.insert(index, id),
        }
        queue.change(node)
    }

    pub(super) async fn dequeue_member_cleanup(
        &self,
        snapshot: &FleetSnapshot,
        node: NodeId,
        reservation: [u8; 16],
    ) -> Result<FleetChange> {
        let row = self.admitted_resource(snapshot, reservation).await?;
        if obsolete_node(&row)? != Some(node) {
            return Err(FleetCatalogError::Conflict);
        }
        let mut queue = self.member_cleanup_queue(snapshot, node).await?;
        let index = queue
            .reservations
            .binary_search(&reservation)
            .map_err(|_| FleetCatalogError::Conflict)?;
        queue.reservations.remove(index);
        queue.change(node)
    }

    pub(in crate::raft::fleet_catalog) async fn pending_member_cleanup(
        &self,
        snapshot: &FleetSnapshot,
        node: NodeId,
    ) -> Result<Vec<[u8; 16]>> {
        // One bounded point read; never enumerate fleet histories or object keys.
        Ok(self
            .member_cleanup_queue(snapshot, node)
            .await?
            .reservations)
    }

    pub(super) async fn node_has_resource_reservations(
        &self,
        snapshot: &FleetSnapshot,
        node: NodeId,
    ) -> Result<bool> {
        let book = self.resource_book(snapshot).await?;
        Ok(self
            .resource_rows(snapshot, &book)
            .await?
            .iter()
            .any(|row| row.node == node))
    }
}
