// Copyright 2026 Cellar Project Authors. Licensed under Apache-2.0.

//! One durable acquire allocation for an exact verification basis and fixed cut.
//! A timeout leaves the allocation untouched. These holds outlive local handles;
//! terminal cleanup must fence all users and join physical work before release.
use super::*;
use crate::raft::cell_groups::{
    CellGroupArchiveAcquireOutcome, CellGroupArchiveCreation, CellGroupArchiveError,
    CellGroupArchiveIntent,
};
use receipts::Publication;
use serde::Deserialize;

const KIND: u8 = 101;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    basis: Basis,
    fixed: Publication,
    registry: CellGroupArchiveCreation,
    attempt: u64,
    intent: CellGroupArchiveIntent,
}

impl Record {
    fn check(
        &self,
        basis: &Basis,
        fixed: &FleetPublishedCut,
        registry: &CellGroupArchiveRegistry,
    ) -> Result<()> {
        if &self.basis != basis || &self.registry != registry.creation() || self.attempt == u64::MAX
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        if self.fixed != Publication::from(fixed) {
            return Err(FleetCatalogError::Conflict);
        }
        check_marker(self.intent.acquire_marker()?, fixed)
    }
}

fn check_marker(
    marker: &crate::raft::cell_groups::GroupManifest,
    fixed: &FleetPublishedCut,
) -> Result<()> {
    if !crate::raft::host::group_prefix(marker, fixed.root())
        .map_err(|_| FleetCatalogError::Conflict)?
    {
        return Err(FleetCatalogError::Unconfirmed);
    }
    Ok(())
}

impl FleetEvacuationVerification {
    fn archive_hold_key(&self) -> Result<String> {
        let pending = &self
            .pending
            .as_ref()
            .ok_or(FleetCatalogError::InventoryIncomplete)?
            .0;
        Ok(format!(
            "removal-archive-acquires/{}/{}",
            hex::encode(hash(&encode(KIND, &self.book.basis, PAGE_LIMIT)?)),
            pending.key
        ))
    }

    /// Recover a fixed cut even if the last controller disappeared between
    /// acquiring retention and appending a live receipt. A new observation must
    /// cover the stored cut; it cannot silently select a new acquire journal.
    pub(super) async fn archive_fixed(
        &self,
        current: &FleetPublishedCut,
    ) -> Result<Option<FleetPublishedCut>> {
        let Some((record, _)) = self
            .catalog
            .inner
            .read::<Record>(&self.archive_hold_key()?, KIND, PAGE_LIMIT)
            .await?
        else {
            return Ok(None);
        };
        let fixed = record.fixed.restore(current)?;
        if record.basis != self.book.basis
            || record.registry.route() != fixed.root().route()
            || record.attempt == u64::MAX
        {
            return Err(FleetCatalogError::PayloadConflict);
        }
        check_marker(record.intent.acquire_marker()?, &fixed)?;
        Ok(Some(fixed))
    }

    /// Acquire this walk's own remote retention. Call freeze_live first. The
    /// exact intent is stored in the fleet catalog before any registry mutation.
    /// Unknown outcomes reconcile the same allocation after controller loss.
    /// No publisher-owned hold, current-health lease or terminal authority is
    /// borrowed. Local cancellation/drop never retires this persistent owner.
    pub async fn acquire_archive_hold(
        &mut self,
        registry: &CellGroupArchiveRegistry,
        timeout: Duration,
    ) -> Result<CellGroupArchiveHold> {
        let catalog = &self.catalog;
        let deadline = Deadline::new(catalog, timeout)?;
        let fixed = &self
            .frozen
            .as_ref()
            .ok_or(FleetCatalogError::Unconfirmed)?
            .fixed;
        let basis = &self.book.basis;
        if registry.route() != fixed.root().route() {
            return Err(FleetCatalogError::Conflict);
        }
        let key = self.archive_hold_key()?;
        let result = catalog
            .run(deadline.remaining()?, async {
                basis.check_current(catalog).await?;
                let (record, token) =
                    match catalog.inner.read::<Record>(&key, KIND, PAGE_LIMIT).await? {
                        Some(stored) => stored,
                        None => {
                            let candidate = prepare(catalog, registry, basis, fixed, 0).await?;
                            persist(catalog, &key, &candidate, None).await?
                        }
                    };
                record.check(basis, fixed, registry)?;
                basis.check_current(catalog).await?;
                match registry
                    .acquire(&catalog.inner.domain, &record.intent)
                    .await
                {
                    Ok(hold) => Ok(hold),
                    Err(
                        error @ (CellGroupArchiveError::Expired | CellGroupArchiveError::Conflict),
                    ) => {
                        match registry
                            .acquire_outcome(&catalog.inner.domain, &record.intent)
                            .await?
                        {
                            CellGroupArchiveAcquireOutcome::Held(hold) => Ok(*hold),
                            CellGroupArchiveAcquireOutcome::Pending => Err(error.into()),
                            CellGroupArchiveAcquireOutcome::Superseded => {
                                // A fresh consistent registry read proved that the
                                // old revision is consumed and its nonce owns no
                                // hold. Its delayed conditional write cannot win.
                                let next = record
                                    .attempt
                                    .checked_add(1)
                                    .ok_or(FleetCatalogError::Limit)?;
                                let candidate =
                                    prepare(catalog, registry, basis, fixed, next).await?;
                                basis.check_current(catalog).await?;
                                let (winner, _) =
                                    persist(catalog, &key, &candidate, Some(&token)).await?;
                                winner.check(basis, fixed, registry)?;
                                // Reconcile this persisted winner on a later turn;
                                // never hide another allocation retry in this call.
                                Err(FleetCatalogError::Unconfirmed)
                            }
                        }
                    }
                    Err(error) => Err(error.into()),
                }
            })
            .await;
        let hold = match result {
            Ok(hold) => hold,
            Err(error) => {
                // Another controller may have persisted an earlier fixed cut
                // since this one's quorum capture. Re-enter fresh quorum proof
                // and restore that winner; do not move its journal or nonce.
                self.frozen = None;
                return Err(error);
            }
        };
        check_marker(hold.marker(), fixed)?;
        catalog
            .run(deadline.remaining()?, basis.check_current(catalog))
            .await?;
        deadline.remaining()?;
        Ok(hold)
    }
}

async fn prepare(
    catalog: &FleetCatalog,
    registry: &CellGroupArchiveRegistry,
    basis: &Basis,
    fixed: &FleetPublishedCut,
    attempt: u64,
) -> Result<Record> {
    let intent = registry.prepare_acquire(&catalog.inner.domain).await?;
    let record = Record {
        basis: basis.clone(),
        fixed: fixed.into(),
        registry: registry.creation().clone(),
        attempt,
        intent,
    };
    record.check(basis, fixed, registry)?;
    Ok(record)
}

/// Conditional journal storage chooses one candidate even when simultaneous
/// controllers prepare different nonces. Only the observed winner may be sent.
async fn persist(
    catalog: &FleetCatalog,
    key: &str,
    candidate: &Record,
    token: Option<&str>,
) -> Result<(Record, String)> {
    let bytes = encode(KIND, candidate, PAGE_LIMIT)?;
    let outcome = catalog
        .inner
        .bucket
        .put_cas(&catalog.inner.key(key), bytes, token)
        .await;
    match catalog.inner.read(key, KIND, PAGE_LIMIT).await? {
        Some(stored) => Ok(stored),
        None => match outcome {
            Err(error) => Err(FleetCatalogError::Storage(error)),
            Ok(_) => Err(FleetCatalogError::Unconfirmed),
        },
    }
}
