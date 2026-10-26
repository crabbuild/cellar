// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! Append-only per-operation movement index, rooted with the debit and permit.
use super::super::records::{Record, Reference};
use super::*;

impl FleetCatalog {
    /// The existence of a canonical prospective request is an unresolved
    /// decision, never permission to execute it. A current-root fence resolves
    /// it if its old admission has not already advanced the head.
    pub(in super::super) async fn topology_candidate(
        &self,
        snapshot: &FleetSnapshot,
        request: &FleetRequestIdentity,
    ) -> Result<bool> {
        let Some((change, _)) = self
            .inner
            .read::<FleetMutation>(
                &format!("requests/{}", hex::encode(request.id)),
                super::super::super::REQUEST_KIND,
                REQUEST_LIMIT,
            )
            .await?
        else {
            return Ok(false);
        };
        change.validate()?;
        let first = change
            .changes
            .first()
            .ok_or(FleetCatalogError::Corrupt("empty topology candidate"))?;
        let row = Record::decode(
            self,
            first
                .value
                .as_deref()
                .ok_or(FleetCatalogError::Corrupt("deleted topology candidate"))?,
        )?;
        if row.request.identity != *request
            || row.request.movement.is_none()
            || change.changes.len() != 5
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        let (canonical, original) = self
            .resource_request(&row.request)
            .await?
            .ok_or(FleetCatalogError::Unconfirmed)?;
        if canonical != change || original.encode()? != row.encode()? {
            return Err(FleetCatalogError::Corrupt("topology candidate projection"));
        }
        if self.resource_applied(snapshot, &change).await?.is_some() {
            return Err(FleetCatalogError::Conflict);
        }
        Ok(true)
    }

    pub(super) async fn next_topology_movement(
        &self,
        snapshot: &FleetSnapshot,
        operation: &IntentRecord,
        row: &Record,
    ) -> Result<IntentRecord> {
        if operation.phase != FleetTopologyPhase::Running
            || row.request.identity.operation != operation.request.operation
            || row.request.identity.expected_revision != snapshot.revision()
            || row.permit.is_none()
        {
            return Err(FleetCatalogError::Conflict);
        }
        let key = hex::encode(row.request.identity.id);
        if self.inner.lookup(operation.moves, &key).await?.is_some() {
            return Err(FleetCatalogError::PayloadConflict);
        }
        let reference = Reference {
            id: row.request.identity.id,
            digest: hash(&row.encode()?),
        };
        let mut next = operation.clone();
        next.transition = row.request.identity.clone();
        next.count = next.count.checked_add(1).ok_or(FleetCatalogError::Limit)?;
        next.moves = self
            .inner
            .update(
                operation.moves,
                &key,
                Some(encode(MOVE_KIND, &reference, 4096)?),
                0,
            )
            .await?;
        next.validate(self)?;
        Ok(next)
    }

    pub(in super::super) async fn append_topology_movement(
        &self,
        snapshot: &FleetSnapshot,
        row: &Record,
        change: &mut FleetMutation,
    ) -> Result<()> {
        let operation = self
            .active_intent(snapshot)
            .await?
            .ok_or(FleetCatalogError::Conflict)?;
        change.changes.push(
            self.next_topology_movement(snapshot, &operation, row)
                .await?
                .change()?,
        );
        Ok(())
    }

    /// Canonical prospective bytes are not authority. The admitted-resource
    /// caller separately confirms their permanent root journal after this check.
    pub(in super::super) async fn check_topology_resource_projection(
        &self,
        change: &FleetMutation,
        row: &Record,
    ) -> Result<()> {
        let projected = change
            .changes
            .get(4)
            .ok_or(FleetCatalogError::Corrupt("missing topology movement"))?;
        let operation: IntentRecord = decode(
            ROW_KIND,
            projected
                .value
                .as_deref()
                .ok_or(FleetCatalogError::Corrupt("deleted topology movement"))?,
            4096,
        )?;
        operation.validate(self)?;
        let reference = Reference {
            id: row.request.identity.id,
            digest: hash(&row.encode()?),
        };
        if projected.table != FleetTable::Operations
            || projected.key != row_key(row.request.identity.operation)
            || operation.request.operation != row.request.identity.operation
            || operation.transition != row.request.identity
            || operation.phase != FleetTopologyPhase::Running
            || operation.count == 0
            || self
                .inner
                .lookup(operation.moves, &hex::encode(reference.id))
                .await?
                .as_deref()
                != Some(encode(MOVE_KIND, &reference, 4096)?.as_slice())
        {
            return Err(FleetCatalogError::Corrupt("topology resource projection"));
        }
        Ok(())
    }

    pub async fn topology_movements(
        &self,
        snapshot: &FleetSnapshot,
        operation: ProposalId,
        after: Option<&FleetCursor>,
        limit: usize,
        timeout: Duration,
    ) -> Result<FleetTopologyMovementPage> {
        self.check_snapshot(snapshot)?;
        if !(1..=128).contains(&limit) {
            return Err(FleetCatalogError::Limit);
        }
        self.run(
            timeout,
            Box::pin(async {
                let record = self.intent_record(snapshot, operation.bytes()).await?;
                if after.is_some_and(|c| c.index != record.moves) {
                    return Err(FleetCatalogError::Invalid("topology movement cursor"));
                }
                let page = self.inner.scan(record.moves, after, limit).await?;
                let mut entries = Vec::with_capacity(page.entries.len());
                for entry in page.entries {
                    let reference: Reference = decode(MOVE_KIND, &entry.value, 4096)?;
                    let row = self.admitted_resource(snapshot, reference.id).await?;
                    let movement = row
                        .request
                        .movement
                        .as_ref()
                        .ok_or(FleetCatalogError::Corrupt("unlinked topology movement"))?;
                    if entry.key != hex::encode(reference.id)
                        || reference.digest != hash(&row.encode()?)
                        || row.request.identity.operation != operation.bytes()
                    {
                        return Err(FleetCatalogError::Corrupt("topology movement entry"));
                    }
                    entries.push(FleetTopologyMovementEntry {
                        reservation: reference.id,
                        group: row
                            .permit
                            .as_ref()
                            .ok_or(FleetCatalogError::Corrupt("missing topology permit"))?
                            .binding()
                            .scope()
                            .map_err(|_| FleetCatalogError::Corrupt("topology movement scope"))?,
                        source: movement.source()?,
                        target: row.node,
                        admitted_revision: row
                            .request
                            .identity
                            .expected_revision
                            .checked_add(1)
                            .ok_or(FleetCatalogError::Corrupt("topology admission revision"))?,
                    });
                }
                Ok(FleetTopologyMovementPage {
                    entries,
                    next: page.next,
                })
            }),
        )
        .await
    }

    /// Consume the first move of a current opaque preview for the active
    /// operation. The debit, permit and operation progress share one root CAS.
    pub async fn reserve_topology_movement(
        &self,
        preview: &FleetTopologyPreview,
        identity: FleetRequestIdentity,
        observer: &RaftCells,
        namespaces: &[NamespaceCatalog],
        timeout: Duration,
    ) -> Result<Option<FleetMovementAdmission>> {
        self.check_snapshot(&preview.snapshot)?;
        let deadline = super::super::super::deadline::Deadline::new(self, timeout)?;
        self.run(
            deadline.remaining()?,
            Box::pin(async {
                let record = self
                    .active_intent(&preview.snapshot)
                    .await?
                    .ok_or(FleetCatalogError::Conflict)?;
                if record.request.operation != identity.operation
                    || record.phase != FleetTopologyPhase::Running
                {
                    return Err(FleetCatalogError::Conflict);
                }
                self.require_intent_preview(&preview.snapshot, &record, preview)
                    .await
            }),
        )
        .await?;
        let Some(request) = preview.first_movement(identity)? else {
            return Ok(None);
        };
        self.reserve_movement_inner(
            &preview.snapshot,
            &request,
            observer,
            namespaces,
            true,
            deadline.remaining()?,
        )
        .await
        .map(Some)
    }
}
